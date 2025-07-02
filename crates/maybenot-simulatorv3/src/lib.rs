
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

use linktrace::mk_start_instant;
use log::debug;
use network::Network;

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


pub struct SimulQueue {
    heap: BinaryHeap<SimulEvent>,
    pub(crate) dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>>,
}

impl SimulQueue {
    pub fn new() -> Self {
        Self {
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
    network: Network,
    max_trace_length: usize,
    only_network_activity: bool,
) -> Vec<SimulEvent> {
    let args = SimulatorArgs::new(network, max_trace_length, only_network_activity);
    simul_advanced(machines_client, machines_server, sq, &args)
}





/// Arguments for [`sim_advanced`].
#[derive(Clone, Debug)]
pub struct SimulatorArgs {
    /// The network model for simulating the network between the client and the
    /// server.
    pub network: Network,
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
    ///// Optional simulated network type specification.
    //pub simulated_network_type: Option<ExtendedNetworkLabels>,
}


impl SimulatorArgs {
    pub fn new(network: Network, max_trace_length: usize, only_network_activity: bool) -> Self {
        Self {
            network,
            max_trace_length,
            max_sim_iterations: 0,
            continue_after_all_normal_packets_processed: false,
            only_client_events: false,
            only_network_activity,
            max_padding_frac_client: 0.0,
            max_blocking_frac_client: 0.0,
            max_padding_frac_server: 0.0,
            max_blocking_frac_server: 0.0,
            insecure_rng_seed: None,
            //client_integration: None,
            //server_integration: None,
            //simulated_network_type: None,
        }
    }
}

/// Like [`sim`], but allows to (i) set the maximum padding and blocking
/// fractions for the client and server, (ii) specify the maximum number of
/// iterations to run the simulator for, and (iii) only returning client events.
pub fn simul_advanced(
    machines_client: &[Machine],
    machines_server: &[Machine],
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

    let mut network = args.network.clone();
    let mut sim_iterations = 0;
    let _start_time = current_time;
    while let Some(next) = pick_next(sq, &mut client, &mut server, &mut network, current_time) {
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

        let _response_events = network.nodes[next.node_idx]
            .handle_event(&next, &network, sq)
            .unwrap_or_else(|e| {
                panic!(
                    "BUG: node {} failed to handle event {:?}: {}",
                    next.node_idx, next.event, e
                )
            });
        
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
        }

        */
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
    trace.sort_by(|a, b| a.time.cmp(&b.time));

    trace
}

fn pick_next<M: AsRef<[Machine]>>(
    sq: &mut SimulQueue,
    client: &mut SimState<M, RngSource>,
    server: &mut SimState<M, RngSource>,
    _network: &mut Network,
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
    let qt = q.unwrap().time - current_time;
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
pub fn parse_trace(trace: &str, _network: Network, ttrace_ts_to_c_delay: Duration) -> SimulQueue {
    

    // we just need a random starting time to make sure that we don't start from
    // absolute 0
    //let starting_time = Instant::now();

    // Introduce mitigation as mk_start_instant and network.delay() will fall
    // on a ms or us boundary, and small initialization timing variations can cause
    // initial current_time to be placed on either side. If unmitigated, this behavior
    // can cause some randomness in output results, eg when ethernet burst_interval=2.
    let boundary_jitter_mitigation = Duration::from_nanos(500500);
    // Use a common starting time for simqueue and linktrace indexing.
    // Adjust it to the subtraction of network delay made below to ensure
    // no negative indexes
    let starting_time = mk_start_instant() + ttrace_ts_to_c_delay + boundary_jitter_mitigation;

    let mut oneline = String::new();

    for l in trace.lines() {
        let parts: Vec<&str> = l.split(',').collect();
        if parts.len() >= 2 {
            // Time in traffic trace is in nanoseconds... 
            let timestamp =
                (parts[0].trim().parse::<u64>().unwrap()) / 1000;

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

    let traffic_events = traffic_trace_prepare(&oneline, ttrace_ts_to_c_delay.as_micros() as i64);
    let mut sq = SimulQueue::new();
    // print out events if there are not a lot. Current printinout function is slow for large traces.
    if traffic_events.dependent_tx.len() < 200{
        event_schedule_print(&traffic_events, ttrace_ts_to_c_delay.as_micros() as i64);
    }
    fill_simq(&traffic_events, &mut sq, starting_time, false);
    println!("SimQ length: {:?} oneline length: {:?} tx_dpend length: {:?}", sq.len(), oneline.len(), traffic_events.dependent_tx.len());
    sq
}





//// Code for reading in traffic trace, create depndent_tx, and prefill SimulQueue 

#[derive(Debug, Clone)]
pub struct PacketEvent {
    pub packet_idx: usize,
    pub time: i64,
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
pub fn traffic_trace_prepare(s: &String, ttrace_ts_to_c_delay_us: i64) -> TrafficTraceData {
    let mut events: Vec<PacketEvent> = Vec::new();

    // Parse input string into ordered PacketEvents.
    for (packet_idx , token) in s.split_whitespace().enumerate() {
        let parts: Vec<&str> = token.split(',').collect();
        if parts.len() != 2 {
            eprintln!("Skipping malformed entry: {}", token);
            continue;
        }
        let time: i64 = match parts[0].parse() {
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
        events.push(PacketEvent { packet_idx , time, kind });
    }

    // Check for client receive events that would be too early to be valid given the specified ttrace delay.
    // This can happen if the trace starts with a receive event at time 0, and the ttrace delay is larger
    // than what would be resonable according to the specific trace. This should be handled by setting the
    // ttrace delay to a vlaue that reflects the actual trafserver to client delay when the traffictrace was collected. 
    // For now, to alllow backwards compatibility, shift the events so that all offsets from the starting point
    // will be positive, to ensure that the linktrace timeslot indexes are always positive. 
    let adjust_time_us = 0;
    for event in &mut events {
        //let mut adjusted_event = event.clone();
        if event.kind == EventKind::CliReceive  {
            if event.time < ttrace_ts_to_c_delay_us {
                event.time -= adjust_time_us;
            }
        } else {
            break; // No need to adjust events
        }
    }
    if adjust_time_us != 0 {
        for event in &mut events {
            event.time -= adjust_time_us;
        }
    }



    // Process client send events: for each send event, if there is a preceding receive, record a dependency;
    // otherwise, mark it as an initial simQ push.
    let mut client_simq_push = Vec::new();
    let mut dependent_tx: HashMap<usize, Vec<(usize, i64, EventKind)>> = HashMap::new();
    let mut last_recv: Option<&PacketEvent> = None;
    for event in &events {
        if event.kind == EventKind::CliReceive {
            last_recv = Some(event);
        } else if event.kind == EventKind::CliSend {
            if let Some(prev_recv) = last_recv {
                let delta = event.time - prev_recv.time;
                dependent_tx.entry(prev_recv.packet_idx).or_default().push((event.packet_idx, delta, event.kind));
            } else {
                client_simq_push.push(event.clone());
            }
        }
    }

    // Process webserver events: for each client receive event, try to find the most recent client send event
    // that occurred at or before (recv time - 4×delay). If found (and the difference is at least 4×delay),
    // record that as a dependency; otherwise, mark the receive as a simQ push for webserver.
    let client_sends: Vec<&PacketEvent> = events.iter().filter(|e| e.kind == EventKind::CliSend).collect();
    let mut webserver_simq_push = Vec::new();
    for event in &events {
        if event.kind == EventKind::CliReceive {
            let boundary = event.time.saturating_sub(2 * ttrace_ts_to_c_delay_us);
            let candidate = client_sends
                .iter()
                .filter(|&&e| e.time <= boundary)
                .max_by_key(|&&e| e.time);
            if let Some(&client_send) = candidate {
                if event.time - client_send.time >= 2 * ttrace_ts_to_c_delay_us {
                    let delta = (event.time - client_send.time) - 2 * ttrace_ts_to_c_delay_us;
                    dependent_tx.entry(client_send.packet_idx).or_default().push((event.packet_idx, delta, event.kind));
                } else {
                    let mut adjusted_event = event.clone();
                    adjusted_event.time -= ttrace_ts_to_c_delay_us;
                    webserver_simq_push.push(adjusted_event);
                } /*else {
                // Fix since some traces start with 0,r which is messy
                adjusted_event.time -= s_c_delay_us;
                server_simq_push.push(adjusted_event);
                //panic!("Receive event {} is too early to be a server simQ push", event.packet_idx);
            }   */
            }
        }
    }
    debug!("{:#?}\n{:#?}\n{:#?}\n", client_simq_push, webserver_simq_push, dependent_tx);
    TrafficTraceData {
        client_simq_push,
        webserver_simq_push,
        dependent_tx,
    }
}

/// Print the reconstructed traffic trace based on the TrafficTraceData struct.
/// Also prints the SimQ prefill vectors and the dependency hashmap.
pub fn event_schedule_print(traffic: &TrafficTraceData, ttrace_ts_to_c_delay_us: i64) {
    let mut events: Vec<PacketEvent> = traffic.client_simq_push.clone();
    events.extend(
        traffic
            .webserver_simq_push
            .clone()
            .into_iter()
            .map(|mut event| {
                event.time += ttrace_ts_to_c_delay_us;
                event
            }),
    );
    let mut event_output: Vec<(usize, String)> = Vec::new();

    println!("Reconstructed client-side traffic trace events, ttrace_ts_to_c_delay_us : {}:  ",
             ttrace_ts_to_c_delay_us);
    for event in &events {
        let kind_str = match event.kind {
            EventKind::CliSend => "cli_send",
            EventKind::CliReceive => "cli_recv",
        };
        let dep_txt = format!("#{:5},  {:7}, {}  :  simQ_push", event.packet_idx, event.time, kind_str);
        event_output.push((event.packet_idx, dep_txt));
    }

    // Create a copy of the dependency map to drain
    let mut remaining_dependencies = traffic.dependent_tx.clone();
    
    
    while !remaining_dependencies.is_empty() {
        let mut made_progress = false;
        
        // Collect keys to avoid borrowing issues during iteration
        let recv_indices: Vec<usize> = remaining_dependencies.keys().cloned().collect();
        
        for recv_idx in recv_indices {
            // Check if the receive event exists in events
            if let Some(recv_event) = events.clone().iter().find(|e| e.packet_idx == recv_idx) {
                // We found the receive event, process its dependencies
                if let Some(deps) = remaining_dependencies.remove(&recv_idx) {
                    made_progress = true;
                    
                    for (dep_idx, delta, event_kind) in deps {
                        let send_time;
                        let dep_txt;
                        if event_kind == EventKind::CliSend {
                            send_time = recv_event.time + delta;
                            dep_txt = format!(
                                "#{:5},  {:7}, cli_send  :  depends_on cli_recv                [#{:5} @{:7}]         [Δt = {:6}]",
                                dep_idx,
                                send_time,
                                recv_event.packet_idx,
                                recv_event.time,
                                delta
                            );
                        } else {
                            send_time = recv_event.time + 2 * (ttrace_ts_to_c_delay_us) + delta;
                            dep_txt = format!(
                                "#{:5},  {:7}, cli_recv  :  webserver_send depends_on cli_send [#{:5} @{:7}] [ws send Δt = {:6}]",
                                dep_idx,
                                send_time,
                                recv_event.packet_idx,
                                recv_event.time,
                                delta
                            );
                        }
                        
                        // Store the dependency info in the output vector
                        event_output.push((dep_idx, dep_txt));
                        
                        events.push(PacketEvent {
                            packet_idx: dep_idx,
                            time: send_time,
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
        println!("cli_send  [#{:5}  @{:7}]   simQ_push", event.packet_idx, event.time);
    }

    println!("\nInitial webserver simQ push events:");
    for event in &traffic.webserver_simq_push {
        println!("cli_recv  [#{:5}  @{:7}]   webserver_send simQ_push", event.packet_idx, event.time);
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


fn get_event_instant(event: &PacketEvent, starting_time: Instant, as_ms: bool) -> Instant {
    let offset_us = if as_ms {
        event.time * 1000
    } else {
        event.time
    };
    if offset_us >= 0 {
        starting_time + Duration::from_micros(offset_us as u64)
    } else {
         starting_time - Duration::from_micros((-offset_us) as u64)
    }
}



pub fn fill_simq(traffic_events: &TrafficTraceData, sq: &mut SimulQueue, starting_time: Instant, as_ms: bool) {

    for event in &traffic_events.client_simq_push {
        let event_instant = get_event_instant(event, starting_time, as_ms);
        let simul_event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_idx: event.packet_idx,
            node_idx: 0, // Client node index
            link_idx: 2, // Client->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            debug_note: Some("Client initial send".to_string()),
        };
        sq.push(simul_event);
    }
    
    for event in &traffic_events.webserver_simq_push {
        let event_instant = get_event_instant(event, starting_time, as_ms);
        let simul_event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_idx: event.packet_idx,
            node_idx: 2, // TrafficServer node index
            link_idx: 0, // TrafficServer->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            debug_note: Some("WebServer initial send".to_string()),
        };
        sq.push(simul_event);
    }
     
    sq.dependent_tx = traffic_events.dependent_tx.clone();
    if as_ms {
        for (_, deps) in sq.dependent_tx.iter_mut() {
            for (_, delta, _) in deps.iter_mut() {
                *delta *= 1000;
            }
        }
    }
}





