pub mod nodes;
pub mod mbn_nodes;
pub mod mbn_helpers;
pub mod links;
pub mod network;
pub mod linktrace;
pub mod linkbundle;
pub mod integration;
pub mod traffic_parse;

// Re-export traffic parsing types and functions for backward compatibility
pub use traffic_parse::{
    parse_trace, traffic_trace_prepare, fill_simq, event_schedule_print,
    PacketEvent, EventKind, TrafficTraceData
};

use std::{
    collections::{HashMap,BinaryHeap},
    cmp::Ordering,
    time::{Duration, Instant},
};


use log::debug;
use network::{NetworkTopology, NetworkLinkstate};

use maybenot::{Framework, Machine,  TriggerAction, TriggerEvent};
use rand::{rngs::ThreadRng, RngCore};
use rand_xoshiro::rand_core::SeedableRng;
use rand_xoshiro::Xoshiro256StarStar;
use mbn_helpers::initialize_mbn_sim_states;



// Enum to encapsulate different RngCore sources: in the Maybenot Framework, the
// RngCore trait is not ?Sized (unnecessary overhead for the framework), so we
// have to work around this by using an enum to support selecting rng source as
// a simulation option.
#[derive(Debug)]
pub enum RngSource {
    Thread(ThreadRng),
    Xoshiro(Xoshiro256StarStar),
}

impl RngCore for RngSource {
    fn next_u32(&mut self) -> u32 {
        match self {
            RngSource::Thread(rng) => rng.next_u32(),
            RngSource::Xoshiro(rng) => rng.next_u32(),
        }
    }

    fn next_u64(&mut self) -> u64 {
        match self {
            RngSource::Thread(rng) => rng.next_u64(),
            RngSource::Xoshiro(rng) => rng.next_u64(),
        }
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        match self {
            RngSource::Thread(rng) => rng.fill_bytes(dest),
            RngSource::Xoshiro(rng) => rng.fill_bytes(dest),
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        match self {
            RngSource::Thread(rng) => rng.try_fill_bytes(dest),
            RngSource::Xoshiro(rng) => rng.try_fill_bytes(dest),
        }
    }
}



/// SimulEvent represents an event in the v3 simulator. It is used internally to
/// represent events that are to be processed by the simulator (in SimulQueue) and
/// events that are produced by the simulator (the resulting trace).
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub struct SimulEvent {
    /// the actual event
    pub event: TriggerEvent,
    /// the time of the event taking place
    pub time: Instant,
    /// Packet ID for triggering dependent tx events
    pub packet_idx: usize,
    /// Node index and link index for the event
    pub node_idx: usize,
    pub link_idx: usize,
    /// sequence number for deterministic insertion ordering when timestamp is identical
    pub q_sequence_nr: u64,
    // Start of MaybeNot specific fields
    /// flag to track padding or normal packet
    pub contains_padding: bool,
    /// internal flag to mark event as bypass
    bypass: bool,
    /// internal flag to mark event as replace
    replace: bool,
    // debug note
    #[cfg(debug_assertions)]
    pub debug_note: Option<String>,
}

impl SimulEvent {
    /// Format event as compact string for column alignment
    fn format_event_compact(&self) -> String {
        match &self.event {
            TriggerEvent::NormalSent => "NormalSent".to_string(),
            TriggerEvent::NormalRecv => "NormalRecv".to_string(),
            TriggerEvent::TunnelSent => "TunnelSent".to_string(),
            TriggerEvent::TunnelRecv => "TunnelRecv".to_string(),
            TriggerEvent::PaddingSent { machine } => format!("PadSent-M{}", machine.into_raw()),
            TriggerEvent::PaddingRecv => "PaddingRecv".to_string(),
            TriggerEvent::BlockingBegin { machine } => format!("BlockBeg-M{}", machine.into_raw()),
            TriggerEvent::BlockingEnd => "BlockingEnd".to_string(),
            TriggerEvent::TimerBegin { machine } => format!("TimerBeg-M{}", machine.into_raw()),
            TriggerEvent::TimerEnd { machine } => format!("TimerEnd-M{}", machine.into_raw()),
        }
    }

    /// Display SimulEvent with time as microseconds since sq.zero_instant
    pub fn display_relative(&self, sq: &SimulQueue) -> String {
        let time_since_zero = if self.time >= sq.zero_instant {
            self.time.duration_since(sq.zero_instant).as_micros() as i64
        } else {
            -(sq.zero_instant.duration_since(self.time).as_micros() as i64)
        };
        format!(
            "{:?} at {}μs (pkt {}, node {}, link {}) P:{} B:{} R:{}",
            self.event, time_since_zero, self.packet_idx, 
            if self.packet_idx == usize::MAX { "MAX".to_string() } else {self.packet_idx.to_string() },
            self.link_idx,
            if self.contains_padding { "T" } else { "F" },
            if self.bypass { "T" } else { "F" },
            if self.replace { "T" } else { "F" }
        )
    }
    /// Display SimulEvent as display_relative but with shortform of nodetype string printed for each node,
    /// from - to nodeid for each link
    pub fn display_full(&self, sq: &SimulQueue, topology: &NetworkTopology, linkstate: &NetworkLinkstate) -> String {
        let time_since_zero = if self.time >= sq.zero_instant {
            self.time.duration_since(sq.zero_instant).as_micros() as i64
        } else {
            -(sq.zero_instant.duration_since(self.time).as_micros() as i64)
        };
        let link = linkstate.get_link(self.link_idx).unwrap();
        // Adjust formatting so field lengths are appropriate for example line below
        // NormalSent at 25 μs (pkt 5, node 2 TrafficServerBasic, link 0 n2->n1) P:F B:F R:F
        format!(
            "{:<12} at{:>8} μs (pkt {:<5} node {:<2} {:<20} link {:<2} n{:<2}->n{:<2})   P:{} B:{} R:{}",
            self.format_event_compact(),
            time_since_zero,
            if self.packet_idx == usize::MAX { "MAX".to_string() } else {self.packet_idx.to_string() },
            self.node_idx,
            topology.nodes[self.node_idx].type_name(),
            self.link_idx,
            link.from_node(),
            link.to_node(),
            if self.contains_padding { "T" } else { "F" },
            if self.bypass { "T" } else { "F" },
            if self.replace { "T" } else { "F" }
        ) }
}


// A display fmt for SimulEvent that shows the event type, time, and packet index as one line
// and has P:T B:F R:T according to the booleans 
impl std::fmt::Display for SimulEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} at {:?} (pkt {}, node {}, link {}) P:{} B:{} R:{}",
            self.event, self.time, self.packet_idx, self.node_idx, self.link_idx,
            if self.contains_padding { "T" } else { "F" },
            if self.bypass { "T" } else { "F" },
            if self.replace { "T" } else { "F" }
        )
    }
}


// for SimulEvent, implement Ord and PartialOrd to allow for sorting by time
impl Ord for SimulEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order to get the smallest time first
        self.time
            .cmp(&other.time)
            .then_with(|| event_to_usize(&self.event).cmp(&event_to_usize(&other.event)))
            .then_with(|| self.q_sequence_nr.cmp(&other.q_sequence_nr))
            .reverse()
    }
}

impl PartialOrd for SimulEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}



#[derive(Clone, Debug)]
pub struct SimulQueue {
    pub zero_instant: Instant,
    pub earliest_event_instant: Instant,
    pub heap: BinaryHeap<SimulEvent>,
    pub(crate) dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>>,
    next_q_sequence_nr: u64,
    pub highest_depend_tx: usize,
}

impl SimulQueue {
    pub fn new() -> Self {
        let now_time = Instant::now();
        Self {
            // sq.zero_instant holds the time instant which is used to represent relative
            // time zero in the traffic trace
            zero_instant: now_time,
            // earliest_event_instant is the earliest event time in the queue, used to
            // calculate relative time in the trace. May be earlier than zero_instant.
            earliest_event_instant: now_time,
            heap: BinaryHeap::new(),
            dependent_tx: HashMap::new(),
            next_q_sequence_nr: 0,
            highest_depend_tx: 0,
        }
    }

    pub fn push(&mut self, mut s_event: SimulEvent) {
        s_event.q_sequence_nr = self.next_q_sequence_nr;
        self.next_q_sequence_nr += 1;
        self.heap.push(s_event);
    }

    pub fn pop(&mut self) -> Option<SimulEvent> {
        self.heap.pop()
    }

    pub fn peek(&self) -> Option<&SimulEvent> {
        self.heap.peek()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    // This function is called for every processed event if continue_after_all_normal_packets_processed is false
    // Although 
    pub fn no_normal_packets(&self, topology: &network::NetworkTopology) -> bool {
        // Check main simulation queue, see if any of traffic trace packer are in it. 
        if self.heap.iter().any(|e| {e.packet_idx < usize::MAX}) {
            return false;
        }
        
        // Check MBN node blocking queues if they exist
        if topology.has_mb {
            let client_mbn = topology.get_mbn_client();
            if !client_mbn.get_queue_normal().borrow().is_empty() {
                return false;
            }
            
            let relay_mbn = topology.get_mbn_server();
            if !relay_mbn.get_queue_normal().borrow().is_empty() {
                return false;
            }
        }
        
        true
    }

}




/// Helper function to convert a TriggerEvent to a usize for sorting purposes.
fn event_to_usize(e: &TriggerEvent) -> usize {
    match e {
        // tunnel before normal before padding
        TriggerEvent::TunnelSent => 0,
        TriggerEvent::NormalSent => 1,
        TriggerEvent::PaddingSent { .. } => 2,
        TriggerEvent::TunnelRecv => 3,
        TriggerEvent::NormalRecv => 4,
        TriggerEvent::PaddingRecv => 5,
        // begin before end
        TriggerEvent::BlockingBegin { .. } => 6,
        TriggerEvent::BlockingEnd => 7,
        TriggerEvent::TimerBegin { .. } => 8,
        TriggerEvent::TimerEnd { .. } => 9,
    }
}





/// ScheduledAction represents an action that is scheduled to be executed at a
/// certain time.
#[derive(PartialEq, Clone, Debug)]
pub struct ScheduledAction {
    action: TriggerAction,
    time: Instant,
}

/// The state of the client, or relay in the simulator.
#[derive(Debug)]
pub struct SimState<M, R> {
    /// an instance of the Maybenot framework
    framework: Framework<M, R>,
    /// scheduled action timers
    scheduled_action: Vec<Option<ScheduledAction>>,
    /// scheduled internal timers
    scheduled_internal_timer: Vec<Option<Instant>>,
    /// blocking until time, active is set
    blocking_until: Option<Instant>,
    /// whether the active blocking bypassable or not
    blocking_bypassable: bool,
    //// integration aspects for this state
    //integration: Option<Integration>,
}

impl<M> SimState<M, RngSource>
where
    M: AsRef<[Machine]>,
{
    pub fn new(
        machines: M,
        current_time: Instant,
        max_padding_frac: f64,
        max_blocking_frac: f64,
        //integration: Option<Integration>,
        insecure_rng_seed: Option<u64>,
    ) -> Self {
        let rng = match insecure_rng_seed {
            // deterministic, insecure RNG
            Some(seed) => RngSource::Xoshiro(Xoshiro256StarStar::seed_from_u64(seed)),
            // secure RNG, default
            None => RngSource::Thread(rand::thread_rng()),
        };

        let num_machines = machines.as_ref().len();

        Self {
            framework: Framework::new(
                machines,
                max_padding_frac,
                max_blocking_frac,
                current_time,
                rng,
            )
            .unwrap(),
            scheduled_action: vec![None; num_machines],
            scheduled_internal_timer: vec![None; num_machines],
            blocking_until: None,
            blocking_bypassable: false,
            //integration,
        }
    }

    /* 
    pub fn reporting_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(|i| i.reporting_delay())
            .unwrap_or(Duration::from_micros(0))
    }

    pub fn action_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(|i| i.action_delay())
            .unwrap_or(Duration::from_micros(0))
    }

    pub fn trigger_delay(&self) -> Duration {
        self.integration
            .as_ref()
            .map(|i| i.trigger_delay())
            .unwrap_or(Duration::from_micros(0))
    }
    */
}






/// The main simulator function.
///
/// Zero or more machines can concurrently be run on the client and server. The
/// machines can be different. The framework is designed to support many
/// machines.
///
/// The queue MUST have been created by [`parse_trace`] with the same delay. The
/// queue is modified by the simulator and should be re-created for each run of
/// the simulator or cloned.
///
/// If max_trace_length is > 0, the simulator will stop after max_trace_length
/// events have been *simulated* by the simulator and added to the simulating
/// output trace. Note that some machines may schedule infinite actions (e.g.,
/// schedule new padding after sending padding), so the simulator may never
/// stop. Use [`sim_advanced`] to set the maximum number of iterations to run
/// the simulator for and other advanced settings.
///
/// If only_network_activity is true, the simulator will only append events that
/// are related to network activity (i.e., packets sent and received) to the
/// output trace. This is recommended if you want to use the output trace for
/// traffic analysis without further (recursive) simulation.
pub fn sim(
    machines_client: &[Machine],
    machines_server: &[Machine],
    sq: &mut SimulQueue,
    topology: &NetworkTopology,
    linkstate: &mut NetworkLinkstate,
    max_trace_length: usize,
    only_network_activity: bool,
) -> Vec<SimulEvent> {
    let args = SimulatorArgs::new(max_trace_length, only_network_activity);
    simul_advanced(machines_client, machines_server, topology, linkstate, sq, &args)
}





/// Arguments for [`sim_advanced`].
#[derive(Clone, Debug)]
pub struct SimulatorArgs {
    /// The maximum number of events to simulate.
    pub max_trace_length: usize,
    /// The maximum number of iterations to run the simulator for. If 0, the
    /// simulator will run until it stops.
    pub max_sim_iterations: usize,
    /// If true, the simulator will continue after all normal packets have been
    /// processed.
    pub continue_after_all_normal_packets_processed: bool,
    /// If true, only client events are returned in the output trace.
    pub only_client_events: bool,
    /// If true, only events that represent network packets are returned in the
    /// output trace.
    pub only_network_activity: bool,
    /// The maximum fraction of padding for the client's instance of the
    /// Maybenot framework.
    pub max_padding_frac_client: f64,
    /// The maximum fraction of blocking for the client's instance of the
    /// Maybenot framework.
    pub max_blocking_frac_client: f64,
    /// The maximum fraction of padding for the server's instance of the
    /// Maybenot framework.
    pub max_padding_frac_server: f64,
    /// The maximum fraction of blocking for the server's instance of the
    /// Maybenot framework.
    pub max_blocking_frac_server: f64,
    /// The seed for the deterministic (insecure) Xoshiro256StarStar RNG. If
    /// None, the simulator will use the cryptographically secure thread_rng().
    pub insecure_rng_seed: Option<u64>,
    ///// Optional client integration delays.
    //pub client_integration: Option<Integration>,
    ///// Optional server integration delays.
    //pub server_integration: Option<Integration>,
}


impl SimulatorArgs {
    pub fn new(max_trace_length: usize, only_network_activity: bool) -> Self {
        Self {
            max_trace_length,
            max_sim_iterations: 0,
            //This bool has different impact in v3 , should be noted
            continue_after_all_normal_packets_processed: true,
            only_client_events: false,
            only_network_activity,
            max_padding_frac_client: 0.0,
            max_blocking_frac_client: 0.0,
            max_padding_frac_server: 0.0,
            max_blocking_frac_server: 0.0,
            insecure_rng_seed: None,
            //client_integration: None,
            //server_integration: None,
        }
    }
}

/// Like [`sim`], but allows to (i) set the maximum padding and blocking
/// fractions for the client and server, (ii) specify the maximum number of
/// iterations to run the simulator for, and (iii) only returning client events.
pub fn simul_advanced(
    machines_client: &[Machine],
    machines_server: &[Machine],
    topology: &NetworkTopology,
    linkstate: &mut NetworkLinkstate,
    sq: &mut SimulQueue,
    args: &SimulatorArgs,
) -> Vec<SimulEvent> {
    // the resulting simulated trace
    let expected_trace_len = if args.max_trace_length > 0 {
        args.max_trace_length
    } else {
        // a rough estimate of the number of events in the trace
        sq.len() * 5
    };
    let mut trace: Vec<SimulEvent> = Vec::with_capacity(expected_trace_len);

    // put the mocked current time at the first event
    let mut current_time = sq.earliest_event_instant;

    // Initialize MBN nodes with SimState if they exist
    if topology.has_mb {
        initialize_mbn_sim_states(topology, machines_client, machines_server, current_time, args);
    }

    let client_mbn = if topology.has_mb { Some(topology.get_mbn_client()) } else { None };
    let relay_mbn = if topology.has_mb { Some(topology.get_mbn_server()) } else { None };


    debug!("sim(): client machines {}", machines_client.len());
    debug!("sim(): server machines {}", machines_server.len());

    let mut sim_iterations = 0;
    while let Some(next) = pick_next(sq, topology, current_time) {
        debug!("#########################################################");
        debug!("sim(): main loop start");

        // move time forward?
        match next.time.cmp(&current_time) {
            Ordering::Less => {
                debug!("sim(): {:#?}", current_time);
                debug!("sim(): {:#?}", next.time);
                panic!("BUG: next event moves time backwards");
            }
            Ordering::Greater => {
                debug!("sim(): time moved forward {:#?}", next.time - current_time);
                current_time = next.time;
            }
            _ => {}
        }

        // Debug blocking status from nodes
        if client_mbn.is_some() && relay_mbn.is_some(){
            if let Some(blocking_until) = client_mbn.unwrap().get_sim_state().borrow().blocking_until {
                debug!("sim(): client is blocked until time {:#?}",
                    blocking_until.duration_since(sq.zero_instant)
                );
            }        
            if let Some(blocking_until) = relay_mbn.unwrap().get_sim_state().borrow().blocking_until {
                debug!("sim(): server is blocked until time {:#?}",
                    blocking_until.duration_since(sq.zero_instant)
                );
            }
        }


        debug!("sim(): next event: {}", next.display_relative(sq));

        // Handle event at node
        topology.nodes[next.node_idx]
            .handle_event(&next, &topology, linkstate, sq);

        // Call trigger_update on MBN nodes after handling the event
        if topology.has_mb {
            if next.node_idx == topology.mb_client {
                debug!("sim(): trigger @client framework {:?}", next.event);
                client_mbn.unwrap().trigger_update(&next, &current_time, sq, topology);
            } else if next.node_idx == topology.mb_server {
                debug!("sim(): trigger @server framework {:?}", next.event);
                relay_mbn.unwrap().trigger_update(&next, &current_time, sq, topology);
            }
        }

        // conditional save to resulting trace: only on network activity if set
        // in fn arg, and only on client activity if set in fn arg
        if (!args.only_client_events || next.node_idx == topology.client) &&
            (!args.only_network_activity || next.event == TriggerEvent::TunnelRecv ||
             next.event == TriggerEvent::TunnelSent) 
        {
            trace.push(next);            
        }

        if args.max_trace_length > 0 && trace.len() >= args.max_trace_length {
            debug!(
                "sim(): we done, reached max trace length {}",
                args.max_trace_length
            );
            break;
        }

        // check if we should stop
        sim_iterations += 1;
        if args.max_sim_iterations > 0 && sim_iterations >= args.max_sim_iterations {
            debug!(
                "sim(): we done, reached max sim iterations {}",
                args.max_sim_iterations
            );
            break;
        }

        // check if we should stop after all normal packets have been processed
        if !args.continue_after_all_normal_packets_processed && sq.no_normal_packets(topology) {
            debug!("sim(): we done, all normal packets processed");
            debug!("Highest dependent tx: {}", sq.highest_depend_tx);
            debug!(" Heap: {:?}", sq.heap);
            break;
        }

        debug!("sim(): main loop end, more work?");
        debug!("#########################################################");
    }

    // sort the trace by time
    // TIME-TEST: 3.6 -> 4.7 ms when sorting the trace
    trace.sort_by(|a, b| a.time.cmp(&b.time));

    trace
}


fn pick_next(
    sq: &mut SimulQueue,
    topology: &NetworkTopology,
    current_time: Instant,
) -> Option<SimulEvent> {
    if topology.has_mb {
        pick_next_mbn(sq, topology, current_time)
    } else {
        sq.pop()
    }
}


// MaybeNot node-based version of pick_next that queries nodes directly instead of using global SimState
fn pick_next_mbn(
    sq: &mut SimulQueue,
    topology: &NetworkTopology,
    current_time: Instant,
) -> Option<SimulEvent> {

    let client_mbn = topology.get_mbn_client();
    let relay_mbn = topology.get_mbn_server();

    // Collect scheduled actions and internal timers from MBN nodes
    let mut min_scheduled_action = Duration::MAX;
    let mut action_node = client_mbn; 
    let mut min_internal_timer = Duration::MAX;
    let mut timer_node = client_mbn;
    let client_blocking_until: Option<Instant>;
    let server_blocking_until: Option<Instant>;

    // Check client MBN node
    let state = client_mbn.get_sim_state().borrow();
    
    // Check scheduled actions
    for action in state.scheduled_action.iter().flatten() {
        if action.time >= current_time {
            let duration = action.time.duration_since(current_time);
            if duration < min_scheduled_action {
                min_scheduled_action = duration;
            }
        }
    }
    
    // Check internal timers
    for timer in state.scheduled_internal_timer.iter().flatten() {
        if *timer >= current_time {
            let duration = timer.duration_since(current_time);
            if duration < min_internal_timer {
                min_internal_timer = duration;
            }
        }
    }
    client_blocking_until = state.blocking_until;
    drop(state);

    // Check server MBN node
    let state = relay_mbn.get_sim_state().borrow();
    
    // Check scheduled actions
    for action in state.scheduled_action.iter().flatten() {
        if action.time >= current_time {
            let duration = action.time.duration_since(current_time);
            if duration < min_scheduled_action {
                min_scheduled_action = duration;
                action_node = relay_mbn;
            }
        }
    }
    
    // Check internal timers
    for timer in state.scheduled_internal_timer.iter().flatten() {
        if *timer >= current_time {
            let duration = timer.duration_since(current_time);
            if duration < min_internal_timer {
                min_internal_timer = duration;
                timer_node = relay_mbn;
            }
        }
    }    
    server_blocking_until = state.blocking_until;
    drop(state);
    

    // Check blocking expiry
    let (min_blocking, blocking_is_client) = match (client_blocking_until, server_blocking_until) {
        (Some(c), Some(s)) => {
            if c < s {
                (c.duration_since(current_time), true)
            } else {
                (s.duration_since(current_time), false)
            }
        }
        (Some(c), None) => (c.duration_since(current_time), true),
        (None, Some(s)) => (s.duration_since(current_time), false),
        (None, None) => (Duration::MAX, true),
    };

    // Check queue
    let queue_next = sq.peek();
    let queue_duration = match queue_next {
        Some(event) => event.time.duration_since(current_time),
        None => Duration::MAX,
    };

    // Debug output
    if min_scheduled_action == Duration::MAX {
        debug!("\tpick_next(): peek_scheduled_action = None");
    } else {
        debug!("\tpick_next(): peek_scheduled_action = {:?}", min_scheduled_action);
    }

    if min_internal_timer == Duration::MAX {
        debug!("\tpick_next(): peek_scheduled_internal_timer = None");
    } else {
        debug!("\tpick_next(): peek_scheduled_internal_timer = {:?}", min_internal_timer);
    }

    if min_blocking == Duration::MAX {
        debug!("\tpick_next(): peek_blocked_exp = None");
    } else {
        debug!("\tpick_next(): peek_blocked_exp = {:?}", min_blocking);
    }

    if queue_duration == Duration::MAX {
        debug!("\tpick_next(): peek_queue = None");
    } else {
        debug!("\tpick_next(): peek_queue = {}", queue_next.unwrap().display_relative(sq));
    }

    // No next event?
    if min_scheduled_action == Duration::MAX
        && min_internal_timer == Duration::MAX
        && min_blocking == Duration::MAX
        && queue_duration == Duration::MAX
    {
        return None;
    }

    // Pick the earliest event
    
    // Blocking expiry is earliest
    if min_blocking <= min_scheduled_action && min_blocking <= min_internal_timer && min_blocking <= queue_duration {
        debug!("\tpick_next(): picked blocking");
        
        // Clear blocking state from the appropriate node
        if blocking_is_client {
            client_mbn.get_sim_state().borrow_mut().blocking_until = None;
        } else {
            relay_mbn.get_sim_state().borrow_mut().blocking_until = None;
        }

        let e = SimulEvent {
            event: TriggerEvent::BlockingEnd,
            time: current_time + min_blocking,
            packet_idx: usize::MAX,
            node_idx: if blocking_is_client {
                topology.mb_client
            } else {
                topology.mb_server
            },
            link_idx: if blocking_is_client {
                topology.nodes[topology.mb_client].get_coreside_out_id()
            } else {
                topology.nodes[topology.mb_server].get_edgeside_out_id()
            },
            bypass: false,
            replace: false,
            contains_padding: false,
            q_sequence_nr: 0,
            #[cfg(debug_assertions)]
            debug_note: None,
        };
        return Some(e);
    }

    // Queue is next
    if queue_duration <= min_scheduled_action && queue_duration <= min_internal_timer {
        debug!("\tpick_next(): picked queue");
        return sq.pop();
    }

    // Internal timer is next
    if min_internal_timer <= min_scheduled_action  {
        debug!("\tpick_next(): picked internal timer");
        let target_time = current_time + min_internal_timer;
        
        if let Some(event) = timer_node.do_internal_timer(target_time) {
            return Some(event);
        }
    }

    // Scheduled action is last
    debug!("\tpick_next(): picked scheduled action");
    let target_time = current_time + min_scheduled_action;
    
    if let Some(event) = action_node.do_scheduled_action(target_time) {
        return Some(event);
    }
    None

}






