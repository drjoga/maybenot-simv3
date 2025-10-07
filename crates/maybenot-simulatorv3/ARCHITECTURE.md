# maybenot-simulatorv3 Architecture

This document provides a detailed overview of the maybenot-simulatorv3 architecture, focusing on the key structs and their relationships, with special attention to the `sim_advanced()` function.

## Overview

The v3 simulator is a **fast and flexible** network simulation framework for testing Maybenot defenses. Unlike v2, it supports:
- Complex network topologies (not just simple client-server)
- Trace-based link throughput modeling (high and standard resolution)
- Parallel simulation runs for link traces
- Integration delay modeling (user space ↔ kernel space)
- Configurable network nodes via TOML

## Core Simulation Flow

```
1. Traffic Trace → parse_trace() → (SimInfo, SimQueue)
2. Topology Config → build_topology_from_config() → (NetworkTopology, NetworkLinkState)
3. sim_advanced(machines, topology, linkstate, si, sq, args) → Vec<SimEvent>
```

## Primary Entry Point: `sim_advanced()`

### Function Signature
```rust
pub fn sim_advanced(
    machines_client: &[Machine],      // Client-side defense machines
    machines_server: &[Machine],      // Server-side defense machines
    topology: &NetworkTopology,       // Network structure and routing
    link_state: &mut NetworkLinkState,// Link throughput states (mutable!)
    si: &SimInfo,                     // Timing baselines and dependencies
    sq: &mut SimQueue,                // Event priority queue (consumed!)
    args: &SimulatorArgs,             // Advanced configuration
) -> Vec<SimEvent>
```

### Critical Notes
- **`sq` is consumed** - clone it if you need to reuse
- **`link_state` is mutated** - link throughput tracking updates state
- **Order matters** - arguments follow a logical flow: machines → topology → state → queue → config

---

## Argument Structs for `sim_advanced()`

### 1. `SimulatorArgs` - Configuration Parameters

**Location**: `src/lib.rs:397`

Controls simulation behavior, termination, and Maybenot framework parameters.

```rust
pub struct SimulatorArgs {
    // Termination conditions (ANY met → stop)
    pub max_trace_length: usize,         // Max events in output (0 = unlimited)
    pub max_sim_iterations: usize,       // Max processing loops (0 = unlimited)
    pub continue_after_all_normal_packets_processed: bool,  // Keep running after traffic done?

    // Output filtering
    pub only_client_events: bool,        // Filter to client perspective only
    pub only_network_activity: bool,     // Only TunnelSent/TunnelRecv events

    // Maybenot framework limits (per side)
    pub max_padding_frac_client: f64,    // Max padding overhead (0.0-1.0)
    pub max_blocking_frac_client: f64,   // Max blocking overhead (0.0-1.0)
    pub max_padding_frac_server: f64,
    pub max_blocking_frac_server: f64,

    // Blocking behavior
    pub drain_blocked_by_time: bool,     // true = drain by timestamp, false = normal first, then padding

    // RNG control
    pub insecure_rng_seed: Option<u64>,  // Some(seed) = deterministic, None = secure random

    // Integration delays
    pub client_integration: Option<Integration>,  // User/kernel space delays
    pub server_integration: Option<Integration>,
}
```

**Key Method**: `SimulatorArgs::new(max_trace_length, only_network_activity)`

**Termination Logic**: Simulation stops when **any** condition is met:
1. Output trace reaches `max_trace_length`
2. Processing reaches `max_sim_iterations`
3. All normal packets processed (if `continue_after_all_normal_packets_processed = false`)

### 2. `NetworkTopology` - Network Structure

**Location**: `src/topology.rs:46`

Defines the complete network graph: nodes, routing rules, and special node indices.

```rust
pub struct NetworkTopology {
    pub network_config: NetworkConfig,        // Original TOML config
    pub nodes: Vec<NodeType>,                 // All nodes (indexed)
    pub routes: Vec<Vec<Option<usize>>>,      // routes[node_id][inlink] = Some(outlink)

    // Special node indices
    pub client: usize,                        // Traffic source node
    pub traffic_server: usize,                // Traffic destination node
    pub has_mb: bool,                         // Are Maybenot nodes present?
    pub mb_client: usize,                     // Client MBN node index
    pub mb_server: usize,                     // Server/relay MBN node index
}
```

**Key Methods**:
- `get_outlink(node_id, in_link)` - Routing lookup
- `get_mbn_client()` / `get_mbn_server()` - Access Maybenot nodes as trait objects

**Node Types** (see `src/nodes.rs:11`):
- `ClientBasic` - Simple traffic source
- `RouterBasic` - Packet forwarding
- `TrafficServerBasic` - Simple traffic destination
- `ClientMBN` - Client with Maybenot framework
- `RelayMBN` - Relay with Maybenot framework
- `RelayMBNtserver` - Combined relay+traffic-server with Maybenot

**Routing**: `routes[node_id][incoming_link_id] = Some(outgoing_link_id)` defines forwarding rules.

### 3. `NetworkLinkState` - Link Throughput State

**Location**: `src/topology.rs:7`

Holds **mutable state** for all network links. Updated during simulation to track link busy times.

```rust
pub struct NetworkLinkState {
    pub links: Vec<LinkType>,  // All link instances (indexed by link_id)
}
```

**Link Types** (see `src/links.rs:7`):
- `FixedTput` - Constant throughput, fixed delay
- `HiTraceTput` - High-resolution trace-based throughput (precomputed busy_to matrix)
- `StdTraceTput` - Standard-resolution trace-based throughput

**Key Operations**:
- `get_link_mut(link_id)` - Get mutable link for state updates
- `link.sample(current_duration)` - Calculate packet transmission delay + queuing

**Link State Mutation**: Each link tracks `next_busy_to_duration` to model queuing delays from throughput limits.

### 4. `SimInfo` - Timing Baselines & Dependencies

**Location**: `src/lib.rs:183`

Stores timing reference points and packet dependency information (request-response patterns).

```rust
pub struct SimInfo {
    pub zero_instant: Instant,              // Relative time zero for trace
    pub earliest_event_instant: Instant,    // Earliest event in queue (may be < zero)
    pub(crate) dependent_tx: Vec<Vec<(usize, i64, EventKind)>>,  // dependent_tx[packet_id] = [(dep_pkt, delta_ns, kind)]
}
```

**Dependency Analysis**: `dependent_tx` maps client sends → triggered server responses. Created by `traffic_trace_prepare()` by analyzing request-response timing patterns.

**Example**: If packet 5 triggers packet 10 after 50ms, then `dependent_tx[5] = [(10, 50_000_000, CliSend)]`.

### 5. `SimQueue` - Event Priority Queue

**Location**: `src/lib.rs:211`

Priority queue for simulation events. **Consumed during simulation** - clone if you need to reuse.

```rust
pub struct SimQueue {
    pub heap: BinaryHeap<SimEvent>,    // Min-heap by time (earliest first)
    next_q_sequence_nr: u64,           // Deterministic tie-breaking
}
```

**Event Ordering** (see `event_to_usize()` in `src/lib.rs:290`):
1. **Primary**: timestamp (earliest first)
2. **Secondary**: event type priority (Tunnel > Normal > Padding > Blocking > Timer)
3. **Tertiary**: `q_sequence_nr` (insertion order)

**Key Method**: `no_normal_packets(topology)` - Checks if only padding/control events remain.

### 6. `SimEvent` - Single Network Event

**Location**: `src/lib.rs:41`

Fundamental unit of simulation - represents packets, padding, blocking, and timer events.

```rust
pub struct SimEvent {
    // Core event data
    pub event: TriggerEvent,           // Maybenot event type
    pub time: Instant,                 // When event occurs
    pub packet_id: usize,              // usize::MAX for non-traffic events

    // Routing information
    pub node_id: usize,                // Node processing this event
    pub link_id: usize,                // Link for transmission

    // Simulation metadata
    pub q_sequence_nr: u64,            // Deterministic ordering
    pub contains_padding: bool,        // Padding vs normal traffic
    bypass: bool,                      // Bypass blocking?
    replace: bool,                     // Replace with normal traffic?

    #[cfg(debug_assertions)]
    pub debug_note: Option<String>,
}
```

**Event Types** (from maybenot crate `TriggerEvent`):
- `NormalSent` / `NormalRecv` - Application traffic
- `TunnelSent` / `TunnelRecv` - Network layer view
- `PaddingSent{machine}` / `PaddingRecv` - Defense padding
- `BlockingBegin{machine}` / `BlockingEnd` - Traffic blocking
- `TimerBegin{machine}` / `TimerEnd{machine}` - Machine internal timers

---

## Supporting Structs

### Integration Delays

**`Integration`** (`src/integration.rs:8`) - Models user/kernel space latency:
```rust
pub struct Integration {
    pub action_delay: BinDist,     // Time to execute action (send padding)
    pub reporting_delay: BinDist,  // Time to report event to Maybenot
    pub trigger_delay: BinDist,    // Time to trigger scheduled action
}
```

**`BinDist`** (`src/integration.rs:53`) - Delay distribution from JSON histogram:
```rust
pub struct BinDist {
    bins: Vec<(f64, f64)>,              // (min, max) millisecond ranges
    cumulative_probabilities: Vec<f64>, // For efficient sampling
}
```

### Maybenot Node State

**`MbnState`** (`src/mbn_nodes.rs:59`) - Per-node Maybenot framework state:
```rust
pub struct MbnState<M, R> {
    pub framework: Framework<M, R>,             // Maybenot framework instance
    pub scheduled_action: Vec<Option<ScheduledAction>>,   // Per-machine action timers
    pub scheduled_internal_timer: Vec<Option<Instant>>,   // Per-machine internal timers
    pub blocking_until: Option<Instant>,        // Active blocking end time
    pub blocking_bypassable: bool,              // Can padding bypass?
    pub drain_blocked_by_time: bool,            // Drain order for blocked packets
    pub integration: Option<Integration>,       // Integration delays
}
```

**`ScheduledAction`** (`src/mbn_nodes.rs:52`) - Pending defense action:
```rust
pub struct ScheduledAction {
    pub action: TriggerAction,  // SendPadding, BlockOutgoing, etc.
    pub time: Instant,          // When to execute
}
```

**Traffic Queues**: MBN nodes maintain blocked packet queues:
- `queue_normal: RefCell<VecDeque<SimEvent>>` - Blocked normal traffic
- `queue_padding: RefCell<VecDeque<SimEvent>>` - Blocked padding traffic

### Trace-Based Links

**`LinkTrace`** (`src/linktrace.rs:15`) - Throughput evolution for a link:
```rust
pub struct LinkTrace {
    traceinput: String,                    // Filename or trace data
    pub bw_trace: Vec<i32>,                // Throughput samples (bytes/ms)
    pub is_tput_trace_high_res: bool,      // High vs standard resolution
    sizebin_lookuptable: SizebinLookupTable,  // Packet size → bin mapping
    busy_to_mtx: Array2<i32>,              // Precomputed busy_to lookup (high-res only)
}
```

**`LinkBundle`** (`src/linkbundle.rs:31`) - Collection of link traces:
```rust
pub struct LinkBundle {
    pub bundleinfo: String,
    pub linktraces: Vec<Arc<LinkTrace>>,   // Shared traces
    pub tracefilenames: Vec<String>,
}
```

Used for parallel simulation runs with different trace combinations.

### Topology Configuration

**`NetworkConfig`** (`src/topology_parse.rs:16`) - TOML-based topology definition:
```rust
pub struct NetworkConfig {
    pub nodes: Vec<NodeConfig>,
    pub links: Vec<LinkConfig>,
    pub routes: Vec<RouteConfig>,
    pub fixed_propagation: bool,           // Fixed vs time-varying link delays
}
```

**`NodeConfig`** (`src/topology_parse.rs:26`) - Node specification:
```rust
pub struct NodeConfig {
    pub id: usize,
    pub node_type: String,                 // "ClientMBN", "RelayMBN", etc.
    pub ts_to_relay_extra_us: Option<u64>, // Extra delay for packet generation
}
```

**`LinkConfig`** (`src/topology_parse.rs:38`) - Link specification:
```rust
pub struct LinkConfig {
    pub id: usize,
    pub from_node: usize,
    pub to_node: usize,
    pub prop_us: u64,                      // Propagation delay (microseconds)
    pub tput_bps: Option<u64>,             // Fixed throughput (bits/sec)
    pub linktrace: Option<String>,         // Trace file path
    pub prop_us_vec: Option<Vec<u64>>,     // Time-varying propagation
}
```

---

## Simulation Algorithm

### Main Loop (`sim_advanced()` in `src/lib.rs:490`)

```
1. Initialize MBN nodes (if present) with framework instances
2. Set current_time = earliest_event_instant

LOOP while pick_next() returns event:
    3. Advance current_time to event.time
    4. nodes[event.node_id].handle_event(event, ...)
       - Routes packet through topology
       - Updates link states
       - Queues dependent packets
    5. IF event at MBN node: trigger_update(event, ...)
       - framework.trigger_events() → actions
       - Schedule actions (padding, blocking, timers)
       - Update MBN state
    6. IF output filter passes: append event to trace
    7. Check termination conditions
```

### Event Selection (`pick_next_mbn()` in `src/lib.rs:658`)

Chooses next event from **4 concurrent sources** (when Maybenot nodes present):

1. **Blocking expiry** - `blocking_until` from MBN nodes
2. **Queue events** - `sq.heap` (network packets)
3. **Internal timers** - `scheduled_internal_timer` from MBN nodes
4. **Scheduled actions** - `scheduled_action` from MBN nodes (padding, blocking)

**Priority**: Earliest timestamp wins. Tie-breaking favors blocking > queue > timers > actions.

### Node Event Handling

**Basic Nodes** (`ClientBasic`, `RouterBasic`, `TrafficServerBasic`):
- Receive packet on incoming link
- Route to outgoing link via `topology.routes`
- Call `make_network_receive_from_sent()` to schedule receive event
- Check `dependent_tx` for triggered packets

**MBN Nodes** (`ClientMBN`, `RelayMBN`, `RelayMBNtserver`):
- All basic node functionality
- **Plus**: Maybenot framework integration
- Blocking queue management
- `trigger_update()` → framework → actions → scheduled events

---

## Key Relationships

### Data Flow
```
Traffic Trace (CSV)
  ↓ parse_trace()
(SimInfo, SimQueue)
  ↓
SimEvent → SimQueue.heap
  ↓ pick_next()
SimEvent → NodeType.handle_event()
  ↓
SimEvent → Link.sample() → mutates NetworkLinkState
  ↓
SimEvent → dependent_tx lookup → new SimEvents → SimQueue
  ↓ (if MBN node)
TriggerEvent → Framework.trigger_events()
  ↓
TriggerAction → ScheduledAction → MbnState
  ↓ pick_next()
SimEvent → output trace
```

### Type Dependencies
```
sim_advanced() requires:
  ├─ machines_client/server: &[Machine]
  │    └─ from maybenot crate (serialized defense state machines)
  ├─ topology: &NetworkTopology
  │    ├─ nodes: Vec<NodeType>
  │    │    ├─ Basic nodes (ClientBasic, RouterBasic, TrafficServerBasic)
  │    │    └─ MBN nodes (ClientMBN, RelayMBN, RelayMBNtserver)
  │    │         └─ contains: RefCell<MbnState>
  │    │              └─ framework: Framework<Vec<Machine>, RngSource>
  │    └─ Built from: NetworkConfig (TOML)
  ├─ link_state: &mut NetworkLinkState
  │    └─ links: Vec<LinkType>
  │         ├─ FixedTputLink
  │         ├─ HiTraceTputLink (uses LinkTrace)
  │         └─ StdTraceTputLink (uses LinkTrace)
  ├─ si: &SimInfo
  │    └─ dependent_tx: Vec<Vec<(usize, i64, EventKind)>>
  ├─ sq: &mut SimQueue
  │    └─ heap: BinaryHeap<SimEvent>
  └─ args: &SimulatorArgs
       └─ client/server_integration: Option<Integration>
            └─ delays: BinDist
```

---

## Common Patterns

### Creating a Simulation

```rust
// 1. Parse traffic trace
let (si, mut sq) = parse_trace(trace_str, &topology, network_delay);

// 2. Load topology from TOML
let topology = load_topology_from_file("config.toml")?;
let mut linkstate = topology.network_config.to_linkstate();

// 3. Configure simulation
let mut args = SimulatorArgs::new(10000, true);
args.max_padding_frac_client = 0.3;
args.insecure_rng_seed = Some(42);  // Deterministic

// 4. Run simulation
let trace = sim_advanced(
    &client_machines,
    &server_machines,
    &topology,
    &mut linkstate,
    &si,
    &mut sq,
    &args,
);
```

### Parallel Simulations (LinkBundle)

```rust
let bundle = load_linkbundle_from_file("traces.linkbundle.gz")?;

// Run parallel simulations with different trace combinations
let results: Vec<_> = (0..bundle.linktraces.len())
    .into_par_iter()  // rayon parallel iterator
    .map(|i| {
        let mut linkstate = topology.new_from_config();
        linkstate.set_link_traces(bundle.get_index_trace(i));
        sim_advanced(...)
    })
    .collect();
```

### Deterministic Testing

```rust
let mut args = SimulatorArgs::new(5000, false);
args.insecure_rng_seed = Some(42);  // Fixed seed
// Note: Server uses seed + 1 automatically
```

---

## Performance Characteristics

### Bottlenecks
1. **Event queue operations** - O(log N) per pop/push on BinaryHeap
2. **Framework trigger_events()** - O(machines × states) per event
3. **Link throughput calculation** - Trace lookup for high-res links

### Optimizations
- **Precomputed busy_to matrix** for `HiTraceTputLink` (trades memory for speed)
- **Enum dispatch** for nodes/links (no vtable overhead)
- **RefCell interior mutability** for MBN state (avoids clone-modify-replace)
- **Arc<LinkTrace>** for shared traces across parallel runs

### Memory
- `LinkTrace.busy_to_mtx`: O(num_bins × trace_length) for high-res traces
- `SimQueue.heap`: Grows with pending events (padding machines can generate infinite events)
- `dependent_tx`: O(num_packets) but each packet typically has 0-2 dependents

---

## Testing Notes

### Required Setup
Before running tests, **you must** generate trace data:
```bash
cargo test --test trace_setup  # Run FIRST
cargo test                      # Then run other tests
```

This creates fixtures needed by integration tests.

### Debug Output
Enable rich debug logging:
```bash
RUST_LOG=debug cargo test test_name
```

Shows event selection, framework triggers, and state transitions.

---

## Further Reading

- **Core Maybenot**: See `crates/maybenot/README.md` for framework details
- **TOML configs**: Example files in crate root (`basic_test.toml`, `mbn_test.toml`)
- **Traffic traces**: See `traffic_parse::traffic_trace_prepare()` for dependency analysis algorithm
