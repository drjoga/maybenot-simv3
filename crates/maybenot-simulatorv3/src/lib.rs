
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
    collections::HashMap,
    cmp::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

use events::{Event, EventQueue};
use queue::SimQueue;
use maybenot::TriggerEvent;
use integration::Integration;

use linktrace::{mk_start_instant, LinkTrace};
use log::debug;
use links::{ExtendedNetwork, ExtendedNetworkLabels, WindowCount};

use maybenot::{Framework, Machine, MachineId, Timer, TriggerAction};
use rand::{rngs::ThreadRng, RngCore};
use rand_xoshiro::rand_core::SeedableRng;
use rand_xoshiro::Xoshiro256StarStar;

use crate::{
    queue_peek::{
        peek_blocked_exp, peek_queue, peek_scheduled_action, peek_scheduled_internal_timer,
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




/// SimEvent represents an event in the simulator. It is used internally to
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
    /// integration aspects for this state
    integration: Option<Integration>,
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
        integration: Option<Integration>,
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
            integration,
        }
    }

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
}

















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
    /// Receive events that did not have a qualifying client send dependency.
    pub webserver_simq_push: Vec<PacketEvent>,
    /// Receive events which were so early that they need to be createed at server instead of webserver.
    pub server_simq_push: Vec<PacketEvent>,
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
pub fn traffic_trace_prepare(s: &String, s_c_delay_us: i64, ws_s_delay_us: i64) -> TrafficTraceData {
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

    // Process webserver events: for each receive event, try to find the most recent client send event
    // that occurred at or before (recv time - 4×delay). If found (and the difference is at least 4×delay),
    // record that as a dependency; otherwise, mark the receive as a simQ push for webserver.
    let client_sends: Vec<&PacketEvent> = events.iter().filter(|e| e.kind == EventKind::CliSend).collect();
    let mut webserver_simq_push = Vec::new();
    let mut server_simq_push = Vec::new();
    for event in &events {
        let mut adjusted_event = event.clone();
        if event.kind == EventKind::CliReceive {
            let boundary = event.time.saturating_sub(2 * (ws_s_delay_us + s_c_delay_us));
            let candidate = client_sends
                .iter()
                .filter(|&&e| e.time <= boundary)
                .max_by_key(|&&e| e.time);
            if let Some(&client_send) = candidate {
                if event.time - client_send.time >= 2 * (ws_s_delay_us + s_c_delay_us) {
                    let delta = (event.time - client_send.time) - 2 * (ws_s_delay_us + s_c_delay_us);
                    dependent_tx.entry(client_send.packet_idx).or_default().push((event.packet_idx, delta, event.kind));
                } else if event.time - client_send.time >=  s_c_delay_us {
                    adjusted_event.time -= s_c_delay_us;
                    server_simq_push.push(adjusted_event);
                } else {
                    // Fix since some traces start with 0,r which is messy
                    adjusted_event.time -= s_c_delay_us;
                    server_simq_push.push(adjusted_event);
                    //panic!("Receive event {} is too early to be a server simQ push", event.packet_idx);
                }
            } else if event.time >= ws_s_delay_us + s_c_delay_us {
                adjusted_event.time -= ws_s_delay_us + s_c_delay_us;
                webserver_simq_push.push(adjusted_event);
            } else if event.time >=  s_c_delay_us {
                adjusted_event.time -= s_c_delay_us;
                server_simq_push.push(adjusted_event);
            } else {
                // Fix since some traces start with 0,r which is messy
                adjusted_event.time -= s_c_delay_us;
                server_simq_push.push(adjusted_event);
                //panic!("Receive event {} is too early to be a server simQ push", event.packet_idx);
            }   
        }
    }
    debug!("{:#?}\n{:#?}\n{:#?}\n{:#?}\n", client_simq_push, webserver_simq_push, server_simq_push, dependent_tx);
    TrafficTraceData {
        client_simq_push,
        webserver_simq_push,
        server_simq_push,
        dependent_tx,
    }
}

/// Print the reconstructed traffic trace based on the TrafficTraceData struct.
/// Also prints the SimQ prefill vectors and the dependency hashmap.
pub fn event_schedule_print(traffic: &TrafficTraceData, s_c_delay_us: i64, ws_s_delay_us: i64) {
    let mut events: Vec<PacketEvent> = traffic.client_simq_push.clone();
    events.extend(
        traffic
            .webserver_simq_push
            .clone()
            .into_iter()
            .map(|mut event| {
                event.time += ws_s_delay_us + s_c_delay_us;
                event
            }),
    );
    events.extend(
        traffic
            .server_simq_push
            .clone()
            .into_iter()
            .map(|mut event| {
                event.time += s_c_delay_us;
                event
            }),
    );
    let mut event_output: Vec<(usize, String)> = Vec::new();

    println!("Reconstructed client-side traffic trace events, s_c_delay_us : {},  ws_s_delay_us : {}:  ",
             s_c_delay_us, ws_s_delay_us);
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
                            send_time = recv_event.time + 2 * (ws_s_delay_us + s_c_delay_us) + delta;
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

    println!("\nInitial server simQ push events:");
    for event in &traffic.server_simq_push {
        println!("cli_recv  [#{:5}  @{:7}]   server_send simQ_push", event.packet_idx, event.time);
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


pub fn fill_simq(traffic_events: &TrafficTraceData, sq: &mut SimQueue, starting_time: Instant, as_ms: bool) {

    for event in &traffic_events.client_simq_push {
        let event_instant = get_event_instant(event, starting_time, as_ms);
        sq.push(
            TriggerEvent::NormalSent,
            true,
            false,
            false,
            event.packet_idx,
            false,
            event_instant,
            Duration::from_micros(0),
        );
    }
    for event in &traffic_events.webserver_simq_push {
        let event_instant = get_event_instant(event, starting_time, as_ms);
        sq.push(
            TriggerEvent::NormalSent,
            false,
            true,
            false,
            event.packet_idx,
            false,
            event_instant,
            Duration::from_micros(0),
        );
    }
    for event in &traffic_events.server_simq_push {
        let event_instant = get_event_instant(event, starting_time, as_ms);
        sq.push(
            TriggerEvent::NormalSent,
            false,
            false,
            false,
            event.packet_idx,
            false,
            event_instant,
            Duration::from_micros(0),
        );
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










pub struct DiscreteEventSimulator {
    event_queue: EventQueue,
    current_time: Duration,
}

impl DiscreteEventSimulator {
    pub fn new() -> Self {
        Self {
            event_queue: EventQueue::new(),
            current_time: Duration::ZERO,
        }
    }

    pub fn schedule_event(&mut self, event: Event) {
        self.event_queue.push(event);
    }

    pub fn run_until(&mut self, end_time: Duration) {
        while let Some(event) = self.event_queue.pop() {
            if event.time > end_time {
                self.event_queue.push(event);
                break;
            }
            self.current_time = event.time;
        }
    }

    pub fn current_time(&self) -> Duration {
        self.current_time
    }
}

impl Default for DiscreteEventSimulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulator_creation() {
        let simulator = DiscreteEventSimulator::new();
        assert_eq!(simulator.current_time(), Duration::ZERO);
    }
}