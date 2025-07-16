
pub mod events;
pub mod nodes;
pub mod links;
pub mod network;
pub mod queue;
pub mod queue_event;
pub mod queue_peek;
pub mod linktrace;
pub mod linkbundle;
pub mod integration;

use std::{
    collections::{HashMap,BinaryHeap},
    cmp::Ordering,
    time::{Duration, Instant},
};

use maybenot::TriggerEvent;

use log::{debug, warn};
use network::{NetworkTopology, NetworkLinkstate};

use maybenot::{Framework, Machine, TriggerAction};
use rand::{rngs::ThreadRng, RngCore};
use rand_xoshiro::rand_core::SeedableRng;
use rand_xoshiro::Xoshiro256StarStar;


use crate::{
    queue_peek::{
        peek_scheduled_action, peek_scheduled_internal_timer,
    },
};



// Enum to encapsulate different RngCore sources: in the Maybenot Framework, the
// RngCore trait is not ?Sized (unnecessary overhead for the framework), so we
// have to work around this by using an enum to support selecting rng source as
// a simulation option.
#[derive(Debug)]
enum RngSource {
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

// for SimulEvent, implement Ord and PartialOrd to allow for sorting by time
impl Ord for SimulEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order to get the smallest time first
        self.time
            .cmp(&other.time)
            .then_with(|| event_to_usize(&self.event).cmp(&event_to_usize(&other.event)))
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
    heap: BinaryHeap<SimulEvent>,
    pub(crate) dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>>,
}

impl SimulQueue {
    pub fn new() -> Self {
        let now_time = Instant::now();
        Self {
            zero_instant: now_time,
            earliest_event_instant: now_time,
            heap: BinaryHeap::new(),
            dependent_tx: HashMap::new(),
        }
    }

    pub fn push(&mut self, event: SimulEvent) {
        self.heap.push(event);
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

    /// get the first time of the queue: should only be used for the
    /// simulator's current time at startup
    pub fn get_first_event_time(&self) -> Option<Instant> {
        self.peek().map(|e| e.time)
    }

}



/// SimEvent represents an event in the v1 simulator. It is used internally to
/// represent events that are to be processed by the simulator (in SimQueue) and
/// events that are produced by the simulator (the resulting trace).
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub struct SimEvent {
    /// the actual event
    pub event: TriggerEvent,
    /// the time of the event taking place
    pub time: Instant,
    /// Packet ID for triggering dependent tx events
    pub packet_idx: usize,
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

// for SimEvent, implement Ord and PartialOrd to allow for sorting by time
impl Ord for SimEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order to get the smallest time first
        self.time
            .cmp(&other.time)
            .then_with(|| event_to_usize(&self.event).cmp(&event_to_usize(&other.event)))
            .reverse()
    }
}

impl PartialOrd for SimEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}




/// ScheduledAction represents an action that is scheduled to be executed at a
/// certain time.
#[derive(PartialEq, Clone, Debug)]
pub struct ScheduledAction {
    action: TriggerAction,
    time: Instant,
}

/// The state of the client, or server, or webserver in the simulator.
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
            //This bool has different impact in v3 , should be removed
            continue_after_all_normal_packets_processed: true,
            // TIME-TEST: 4.7 -> 7.5 ms when sonly tracing client events !! StarNGE 64k - 21K events is slower...
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
        sq.len() * 2
    };
    let mut trace: Vec<SimulEvent> = Vec::with_capacity(expected_trace_len);

    // put the mocked current time at the first event
    let mut current_time = sq.get_first_event_time().unwrap();

    let mut client = SimState::new(
        machines_client,
        current_time,
        args.max_padding_frac_client,
        args.max_blocking_frac_client,
        //args.clone().client_integration,
        args.insecure_rng_seed,
    );
    let mut server = SimState::new(
        machines_server,
        current_time,
        args.max_padding_frac_server,
        args.max_blocking_frac_server,
        //args.clone().server_integration,
        // if we have an insecure seed, we use the next number in the sequence
        // to avoid the same seed for both client and server
        args.insecure_rng_seed.map(|seed| seed.wrapping_add(1)),
    );
    //debug!("sim(): client machines {}", machines_client.len());
    //debug!("sim(): server machines {}", machines_server.len());

    let mut sim_iterations = 0;
    let _start_time = current_time;
    while let Some(next) = pick_next(sq, &mut client, &mut server, topology, linkstate, current_time) {
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

        debug!("sim(): next event: {:#?}", next);

        topology.nodes[next.node_idx]
            .handle_event(&next, &topology, linkstate, sq);
        
        // Add any response events to the simulation queue
        //for response_event in response_events {
        //    sq.push(response_event);
        //} 

        // Call the .handle function on the handler appropriate for the node type of the node having the event.

        // get actions, update scheduled actions
        debug!("sim(): trigger framework {:?}", next.event);

        /* 
        // conditional save to resulting trace: only on network activity if set
        // in fn arg, and only on client activity if set in fn arg
        if (!args.only_network_activity || network_activity)
            && (!args.only_client_events || next.node_idx == network.client)
        {
            // this should be a network trace: adjust timestamps based on any
            // integration delays
            let mut n = next.clone();
            match next.event {
                TriggerEvent::NormalSent => {
                    // remove the reporting delay
                    //n.time -= n.integration_delay;
                }
                TriggerEvent::PaddingSent { .. } => {
                    // padding packet adds the action delay
                    //n.time += n.integration_delay;
                }
                TriggerEvent::TunnelSent => {
                    if n.contains_padding {
                        // padding packet adds the action delay
                        //n.time += n.integration_delay;
                    } else {
                        // normal packet removes the reporting delay
                        //n.time -= n.integration_delay;
                    }
                }
                TriggerEvent::TunnelRecv | TriggerEvent::PaddingRecv | TriggerEvent::NormalRecv => {
                    // remove the reporting delay
                    //n.time -= n.integration_delay;
                }

                _ => {}
            }

            trace.push(n);
        } */


        // conditional save to resulting trace: only on network activity if set
        // in fn arg, and only on client activity if set in fn arg
        if !args.only_client_events || next.node_idx == topology.client
        {
            trace.push(next.clone());
            
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
        //if !args.continue_after_all_normal_packets_processed && sq.no_normal_packets() {
        //    debug!("sim(): we done, all normal packets processed");
        //    break;
        //}

        debug!("sim(): main loop end, more work?");
        debug!("#########################################################");
    }

    // sort the trace by time
    // TIME-TEST: 3.6 -> 4.7 ms when sorting the trac
    trace.sort_by(|a, b| a.time.cmp(&b.time));

    trace
}

fn pick_next<M: AsRef<[Machine]>>(
    sq: &mut SimulQueue,
    client: &mut SimState<M, RngSource>,
    server: &mut SimState<M, RngSource>,
    _topology: &NetworkTopology,
    _linkstate: &mut NetworkLinkstate,
    current_time: Instant,
) -> Option<SimulEvent> {
    // find the earliest scheduled action, internal timer, block expiry,
    // aggregate delay, and queued events to determine the next event
    let s = peek_scheduled_action(
        &client.scheduled_action,
        &server.scheduled_action,
        current_time,
    );
    debug!("\tpick_next(): peek_scheduled_action = {:?}", s);

    let i = peek_scheduled_internal_timer(
        &client.scheduled_internal_timer,
        &server.scheduled_internal_timer,
        current_time,
    );
    debug!("\tpick_next(): peek_scheduled_internal_timer = {:?}", i);

    let q = sq.peek();
    let qt = match q {
        Some(event) => event.time - current_time,
        None => Duration::MAX,
    };
    debug!("\tpick_next(): peek_queue = {:?}", q);

    // no next?
    if s == Duration::MAX
        && i == Duration::MAX
        && qt == Duration::MAX
    {
        return None;
    }

    // We prioritize the queue next: in general, stuff happens faster outside
    // the framework than inside it. On overload, the user of the framework will
    // bulk trigger events in the framework.
    if qt <= s && qt <= i {
        debug!(
            "\tpick_next(): picked queue",
        );
        let mut tmp = sq.pop().unwrap();
        debug!("\tpick_next(): popped from queue {:?}", tmp);
        // check if blocking moves the event forward in time
        if current_time + qt > tmp.time {
            // move the event forward in time
            tmp.time = current_time + qt;
        }

        return Some(tmp);
    }

    return None;

    /* 
    // next we pick internal events, which should be faster than scheduled
    // actions due to less work
    if i <= s {
        debug!("\tpick_next(): picked internal timer");
        let target = current_time + i;
        let act = do_internal_timer(client, server, target);
        if let Some(a) = act {
            sq.push_sim(a.clone());
        }
        return pick_next(sq, client, server, network, current_time);
    }

    // what's left is scheduled actions: find the action act on the action,
    // putting the event into the sim queue, and then recurse
    debug!("\tpick_next(): picked scheduled action");
    let target = current_time + s;
    let act = do_scheduled_action(client, server, target);
    if let Some(a) = act {
        sq.push_sim(a.clone());
    }
    */
    // No wasteful recursion
    // pick_next(sq, client, server, network, current_time)
    
}



/// Parse a trace into a [`SimQueue`] for use with [`sim`].
///
/// The trace should contain one or more lines of the form
/// "time,direction,size\n", where time is in nanoseconds relative to the first
/// line, direction is either "s" for sent or "r" for received, and size is the
/// number of bytes sent or received. The delay is used to model the network
/// delay between the client and server. Returns a SimQueue with the events in
/// the trace for use with [`sim`].
pub fn parse_trace(trace: &str, topology: &NetworkTopology, ttrace_ts_to_c_delay: Duration) -> SimulQueue {
    let mut sq = SimulQueue::new();    

    // sq.zero_instamt holds the time instant which is used to represent relative 
    // time zero in the treffic trace
    let starting_time = sq.zero_instant;

    let mut oneline = String::new();

    for l in trace.lines() {
        let parts: Vec<&str> = l.split(',').collect();
        if parts.len() >= 2 {
            // Time in traffic trace is in nanoseconds... 
            let timestamp =
                parts[0].trim().parse::<u64>().unwrap();

            match parts[1] {
                "s" | "sn" => {
                    oneline.push_str(&format!("{},s ", timestamp));
                }
                "r" | "rn" => {
                    oneline.push_str(&format!("{},r ", timestamp));
                }
                "sp" | "rp" => {
                    // TODO: figure out of ignoring is the right thing to do
                }
                _ => {
                    panic!("invalid direction")
                }
            }
        }
    }

    let traffic_events = traffic_trace_prepare(&oneline, sq.zero_instant, ttrace_ts_to_c_delay.as_nanos() as i64);

    // print out events if there are not a lot. Current printinout function is slow for large traces.
    if traffic_events.dependent_tx.len() < 200{
        event_schedule_print(&traffic_events, ttrace_ts_to_c_delay.as_nanos() as i64);
    }
    fill_simq(&traffic_events, &topology, &mut sq);
    let total_dependent_events: usize = traffic_events.dependent_tx.values().map(|v| v.len()).sum();
    println!("SimQ length: {:?}   oneline length: {:?} tx_dpend length: {:?} tx_dpend events: {:?}", sq.len(), oneline.len(), traffic_events.dependent_tx.len(), total_dependent_events);
    sq
}



//// Code for reading in traffic trace, create depndent_tx, and prefill SimulQueue 

#[derive(Debug, Clone, Copy)]
pub struct PacketEvent {
    pub packet_idx: usize,
    pub time_ns: i64,
    pub kind: EventKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EventKind {
    CliSend,
    CliReceive,
}

#[derive(Debug, Clone)]
pub struct TrafficTraceData {
    /// Client send events that did not depend on any prior receive.
    pub client_simq_push: Vec<PacketEvent>,
    /// Client receive events that did not have a qualifying client send dependency.
    pub webserver_simq_push: Vec<PacketEvent>,
    /// Dictionary mapping each receive packet_idx to a list of dependet events: (dependent packet_idx, delta, client EventKind)
    pub dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>>,
}

/// Parses the input string (e.g. "0,s 18,s 25,r 25,r 30,s 35,r") and the given delay,
/// then builds a TrafficTraceData struct:
/// - For client sends, if there is no preceding receive, the event is a simQ_push; otherwise, it is recorded
///   as a dependency of the most recent receive.
/// - For each receive event, we search among client send events for the most recent candidate whose timestamp
///   is at or before (recv time - 4×delay). If found (and the time difference is at least 4×delay), that dependency
///   is recorded; otherwise, the receive event is treated as a webserver simQ_push event.
pub fn traffic_trace_prepare(s: &String, zero_instant: Instant, ttrace_ts_to_c_delay_ns: i64) -> TrafficTraceData {
    let mut pkt_events: Vec<PacketEvent> = Vec::new();

    // Parse input string into ordered PacketEvents.
    for (packet_idx , token) in s.split_whitespace().enumerate() {
        let parts: Vec<&str> = token.split(',').collect();
        if parts.len() != 2 {
            eprintln!("Skipping malformed entry: {}", token);
            continue;
        }
        let time_ns: i64 = match parts[0].parse() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Invalid timestamp: {}", parts[0]);
                continue;
            }
        };
        let kind = match parts[1] {
            "s" | "sn" => EventKind::CliSend,
            "r" | "rn" => EventKind::CliReceive,
            _ => {
                eprintln!("Unknown kind '{}'", parts[1]);
                continue;
            }
        };
        pkt_events.push(PacketEvent { packet_idx , time_ns, kind });
    }


    // Process client send events: for each send event, if there is a preceding receive, record a dependency;
    // otherwise, mark it as an initial simQ push.
    let mut client_simq_push = Vec::new();
    let mut dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>> = HashMap::new();
    let mut last_recv: Option<&PacketEvent> = None;
    for pkt_event in &pkt_events {
        if pkt_event.kind == EventKind::CliReceive {
            last_recv = Some(pkt_event);
        } else if pkt_event.kind == EventKind::CliSend {
            if let Some(prev_recv) = last_recv {
                let delta = pkt_event.time_ns - prev_recv.time_ns;
                dependent_tx.entry(prev_recv.packet_idx).or_default().push((pkt_event.packet_idx, delta, pkt_event.kind));
            } else {
                client_simq_push.push(pkt_event.clone());
            }
        }
    }

    // Process webserver events: for each client receive event, try to find the most recent client send event
    // that occurred at or before (recv time - 2 * ttrace_ts_to_c_delay_ns). If found,
    // record that as a dependency; otherwise, mark the receive as a simQ push for webserver.
    let client_sends: Vec<&PacketEvent> = pkt_events.iter().filter(|e| e.kind == EventKind::CliSend).collect();
    let mut webserver_simq_push = Vec::new();
    for pkt_event in &pkt_events {
        if pkt_event.kind == EventKind::CliReceive {
            let boundary = pkt_event.time_ns - (2 * ttrace_ts_to_c_delay_ns);
            let candidate = client_sends
                .iter()
                .filter(|&&e| e.time_ns <= boundary)
                .max_by_key(|&&e| e.time_ns);
            if let Some(&client_send) = candidate {
                if pkt_event.time_ns - client_send.time_ns >= 2 * ttrace_ts_to_c_delay_ns {
                    let delta = (pkt_event.time_ns - client_send.time_ns) - 2 * ttrace_ts_to_c_delay_ns;
                    dependent_tx.entry(client_send.packet_idx).or_default().push((pkt_event.packet_idx, delta, pkt_event.kind));
                } else {
                    let mut adjusted_event = pkt_event.clone();
                    adjusted_event.time_ns -= ttrace_ts_to_c_delay_ns;
                    webserver_simq_push.push(adjusted_event);
                } 
            } else {
                // Fix since some traces start with 0,r or time < which is messy, 
                let mut adjusted_event = pkt_event.clone();
                adjusted_event.time_ns -= ttrace_ts_to_c_delay_ns;
                webserver_simq_push.push(adjusted_event);
                //panic!("Receive event {} is too early to be a server simQ push", event.packet_idx);
            }   
        }
    }

    /* 
    // For debugging, print first few lines of dependent_tx sorted on key  
    println!("\nFirst 8 lines of dependent_tx:");
    let mut sorted_dependent_tx: Vec<_> = dependent_tx.iter().map(|(key, value)| (*key, value)).collect();
    sorted_dependent_tx.sort_by_key(|(key, _)| *key);

    for (i, (key, value)) in sorted_dependent_tx.iter().take(8).enumerate() {
        println!("  {}: Key: {}, Value: {:?}", i + 1, key, value);
    }
    */

    debug!("{:#?}\n{:#?}\n{:#?}\n", client_simq_push, webserver_simq_push, dependent_tx);
    TrafficTraceData {
        client_simq_push,
        webserver_simq_push,
        dependent_tx,
    }
    
}


/// Print the reconstructed traffic trace based on the TrafficTraceData struct.
/// Also prints the SimQ prefill vectors and the dependency hashmap.
pub fn event_schedule_print(traffic: &TrafficTraceData, ttrace_ts_to_c_delay_ns: i64) {
    let mut pkt_events: Vec<PacketEvent> = traffic.client_simq_push.clone();
    pkt_events.extend(
        traffic
            .webserver_simq_push
            .clone()
            .into_iter()
            .map(|mut pkt_event| {
                pkt_event.time_ns += ttrace_ts_to_c_delay_ns;
                pkt_event
            }),
    );
    let mut event_output: Vec<(usize, String)> = Vec::new();

    println!("Reconstructed client-side traffic trace events, ttrace_ts_to_c_delay_ns : {}:  ",
             ttrace_ts_to_c_delay_ns);
    for pkt_event in &pkt_events {
        let kind_str = match pkt_event.kind {
            EventKind::CliSend => "cli_send",
            EventKind::CliReceive => "cli_recv",
        };
        let dep_txt = format!("#{:5},  {:7}, {}  :  simQ_push", pkt_event.packet_idx, pkt_event.time_ns, kind_str);
        event_output.push((pkt_event.packet_idx, dep_txt));
    }

    // Create a copy of the dependency map to drain
    let mut remaining_dependencies = traffic.dependent_tx.clone();
    
    
    while !remaining_dependencies.is_empty() {
        let mut made_progress = false;
        
        // Collect keys to avoid borrowing issues during iteration
        let recv_indices: Vec<usize> = remaining_dependencies.keys().cloned().collect();
        
        for recv_idx in recv_indices {
            // Check if the receive event exists in events
            if let Some(recv_event) = pkt_events.clone().iter().find(|e| e.packet_idx == recv_idx) {
                // We found the receive event, process its dependencies
                if let Some(deps) = remaining_dependencies.remove(&recv_idx) {
                    made_progress = true;
                    
                    for (dep_idx, delta, event_kind) in deps {
                        let send_time;
                        let dep_txt;
                        if event_kind == EventKind::CliSend {
                            send_time = recv_event.time_ns + delta;
                            dep_txt = format!(
                                "#{:5},  {:7}, cli_send  :  depends_on cli_recv                [#{:5} @{:7}]         [Δt = {:6}]",
                                dep_idx,
                                send_time,
                                recv_event.packet_idx,
                                recv_event.time_ns,
                                delta
                            );
                        } else {
                            send_time = recv_event.time_ns + 2 * (ttrace_ts_to_c_delay_ns) + delta;
                            dep_txt = format!(
                                "#{:5},  {:7}, cli_recv  :  webserver_send depends_on cli_send [#{:5} @{:7}] [ws send Δt = {:6}]",
                                dep_idx,
                                send_time,
                                recv_event.packet_idx,
                                recv_event.time_ns,
                                delta
                            );
                        }
                        
                        // Store the dependency info in the output vector
                        event_output.push((dep_idx, dep_txt));
                        
                        pkt_events.push(PacketEvent {
                            packet_idx: dep_idx,
                            time_ns: send_time,
                            kind: event_kind,
                        });
                    }
                }
            }
        }

        if !made_progress {
            eprintln!("Warning: Could not process remaining {} dependencies due to missing events", 
                      remaining_dependencies.len());
            break;
        }
    }
    
    event_output.sort_by_key(|(idx, _)| *idx);
    for (_, text) in event_output {
        println!("{}", text);
    }
        
    let mut tx_event_output: Vec<(usize, usize, String)> = Vec::new();

    println!("\nInitial client simQ push events:");
    for event in &traffic.client_simq_push {
        println!("cli_send  [#{:5}  @{:7}]   simQ_push", event.packet_idx, event.time_ns);
    }

    println!("\nInitial webserver simQ push events:");
    for event in &traffic.webserver_simq_push {
        println!("cli_recv  [#{:5}  @{:7}]   webserver_send simQ_push", event.packet_idx, event.time_ns);
    }

    println!("\nTX dependency mapping (recv_id -> [dependent_id, Δt]):");
    for (recv_idx, deps) in &traffic.dependent_tx {
        for (dep_idx, delta, event_kind) in deps {
            let dep_txt = if *event_kind == EventKind::CliSend {
                format!("cli_recv [#{:5}] triggers cli_send [#{:5}] with Δt = {:5}", recv_idx, dep_idx, delta)
            } else {
                format!("ws_recv  [#{:5}] triggers ws_send  [#{:5}] with Δt = {:5}", recv_idx, dep_idx, delta)
            };
            tx_event_output.push((*recv_idx, *dep_idx, dep_txt));
        }
    } 
    // Sort by recv_idx and dep_idx
    tx_event_output.sort_by_key(|(recv_idx, dep_idx, _)| (*recv_idx, *dep_idx));
    for (_, _, text) in tx_event_output {
        println!("{}", text);
    }
}


/// Helper function to get the event instant based on the zero_instant and the relative time
/// in the trace,  with the trace is in nanoseconds. 
fn get_event_instant(sq: &mut SimulQueue, pkt_event: &PacketEvent) -> Instant {
    if pkt_event.time_ns >= 0 {
        sq.zero_instant + Duration::from_nanos(pkt_event.time_ns as u64)
    } else {
        // Negative offsets can occur due to client receiving at 0,r as in some tests, or it may
        // come from trafserv_to_client_delay being configured too low compared to the actual real delay
        // when the traffic trace was collected.
        let early_instant = sq.zero_instant.checked_sub(Duration::from_nanos(-pkt_event.time_ns as u64)).expect("Underflow for Instant");
        if sq.earliest_event_instant == sq.zero_instant {
            // print out notification that trafser to client delay is too low
            warn!("Note: Negative offset in traffic trace event: {}. This may indicate that trafserv_to_client_delay is too low compared to the actual delay when the traffic trace was collected.", pkt_event.packet_idx);
        } 
        if early_instant < sq.earliest_event_instant {
            sq.earliest_event_instant = early_instant;
        }                 
        early_instant
    }
}



pub fn fill_simq(traffic_events: &TrafficTraceData, topology: &NetworkTopology, sq: &mut SimulQueue) {

    for event in &traffic_events.client_simq_push {
        let event_instant = get_event_instant(sq, event);
        let simul_event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_idx: event.packet_idx,
            node_idx: topology.client, // Client node index
            link_idx: topology.nodes[topology.client].get_coreside_linkid(), // Client->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            #[cfg(debug_assertions)]
            debug_note: Some("Client initial send".to_string()),
        };
        sq.push(simul_event);
    }
    
    for event in &traffic_events.webserver_simq_push {
        let event_instant = get_event_instant(sq, event);
        let simul_event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_idx: event.packet_idx,
            node_idx: topology.traffic_server, // TrafficServer node index
            link_idx: topology.nodes[topology.traffic_server].get_edgeside_linkid(), // TrafficServer->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            #[cfg(debug_assertions)]
            debug_note: Some("WebServer initial send".to_string()),
        };
        sq.push(simul_event);
    }
     
    sq.dependent_tx = traffic_events.dependent_tx.clone();
}





