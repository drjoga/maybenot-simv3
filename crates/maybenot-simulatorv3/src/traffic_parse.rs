use log::{debug, warn};
use std::time::{Duration, Instant};

use crate::topology::NetworkTopology;
use crate::{SimEvent, SimInfo, SimQueue};
use maybenot::TriggerEvent;

/// Errors that can occur during traffic trace parsing.
#[derive(Debug, Clone)]
pub enum TraceParseError {
    /// Invalid timestamp in trace
    InvalidTimestamp(String),
    /// Invalid direction field in trace
    InvalidDirection(String),
    /// Malformed trace entry
    MalformedEntry(String),
    /// Instant underflow when calculating event time
    InstantUnderflow(i64),
}

impl std::fmt::Display for TraceParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceParseError::InvalidTimestamp(s) => write!(f, "Invalid timestamp: {}", s),
            TraceParseError::InvalidDirection(s) => write!(f, "Invalid direction: {}", s),
            TraceParseError::MalformedEntry(s) => write!(f, "Malformed trace entry: {}", s),
            TraceParseError::InstantUnderflow(ns) => {
                write!(f, "Instant underflow for time_ns: {}", ns)
            }
        }
    }
}

impl std::error::Error for TraceParseError {}

/// Parses a network traffic trace into simulation events.
///
/// This function converts raw network traces into [`SimInfo`] and [`SimQueue`]
/// objects ready for simulation. It performs dependency analysis to model
/// client-server request-response patterns.
///
/// # Traffic Trace Format
///
/// The trace should contain line-separated entries:
/// `"<time>,<direction>,<size>\n<time>,<direction>,<size>\n..."` where:
/// - **time**: nanoseconds relative to trace start (0-based)
/// - **direction**: `"s"` (sent by client) or `"r"` (received by client)
/// - **size**: packet size in bytes (currently unused, can be omitted)
///
/// # Arguments
///
/// * `trace` - Raw trace string in the format described above
/// * `topology` - Network topology for node/link mapping  
/// * `ttrace_ts_to_c_delay` - Network delay between client and server when the
///   traces was captured, used to determine which packets are dependent on
///   others.
///
/// # Returns
///
/// * `Ok((SimInfo, SimQueue))` - Timing baselines and priority queue on success
/// * `Err(TraceParseError)` - Error if trace parsing fails
///
/// # Errors
///
/// Returns `TraceParseError` if:
/// - Timestamp cannot be parsed as u64
/// - Invalid direction field in trace entry
/// - Instant underflow occurs during time calculation
///
/// # Dependency Analysis
///
/// The parser automatically identifies request-response patterns:
/// - Client sends followed by receives become dependent events
/// - Server responses are triggered by client requests with appropriate delays
///
/// # See Also
///
/// - [`traffic_trace_prepare`] for the core dependency analysis algorithm
/// - [`fill_simq`] for event queue population logic
pub fn parse_trace(
    trace: &str,
    topology: &NetworkTopology,
    ttrace_ts_to_c_delay: Duration,
) -> Result<(SimInfo, SimQueue), TraceParseError> {
    let mut si = SimInfo::new();
    let mut sq = SimQueue::new();

    let mut oneline = String::new();

    for l in trace.lines() {
        let parts: Vec<&str> = l.split(',').collect();
        if parts.len() >= 2 {
            // Time in traffic trace is in nanoseconds...
            let timestamp = parts[0]
                .trim()
                .parse::<u64>()
                .map_err(|_| TraceParseError::InvalidTimestamp(parts[0].to_string()))?;

            match parts[1] {
                "s" | "sn" => {
                    oneline.push_str(&format!("{},s ", timestamp));
                }
                "r" | "rn" => {
                    oneline.push_str(&format!("{},r ", timestamp));
                }
                "sp" | "rp" => {
                    // TODO: figure out of ignoring is the right thing to do in
                    // all cases, we might want to support recursive use of the
                    // simulator
                }
                _ => {
                    return Err(TraceParseError::InvalidDirection(parts[1].to_string()));
                }
            }
        }
    }
    let traffic_events = traffic_trace_prepare(&oneline, ttrace_ts_to_c_delay.as_nanos() as i64);

    fill_simq(&traffic_events, topology, &mut si, &mut sq)?;

    Ok((si, sq))
}

/// Code for reading in traffic trace, create depndent_tx, and prefill SimQueue

#[derive(Debug, Clone, Copy)]
pub struct PacketEvent {
    pub packet_id: usize,
    pub time_ns: i64,
    pub kind: EventKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EventKind {
    CliSend,
    CliReceive,
}

/// Result of traffic trace dependency analysis.
///
/// This struct represents the parsed and analyzed traffic trace, separating
/// events into independent initial events and dependent request-response
/// chains.
///
/// # Structure
///
/// - **Independent events** go directly into simulation queue at trace start
/// - **Dependent events** are triggered by other events during simulation
/// - **Dependencies** are stored as `(packet_id, delay, kind)` tuples
///
/// # Usage in Simulation
///
/// 1. `client_simq_push` and `endpoint_simq_push` events seed the simulation
/// 2. When a receive event processes, it triggers its `dependent_tx` events
/// 3. Dependent events are scheduled with appropriate delays from their
///    triggers
#[derive(Debug, Clone)]
pub struct TrafficTraceData {
    /// Client send events that did not depend on any prior receive. These
    /// represent initial client requests that start new communication flows.
    pub client_simq_push: Vec<PacketEvent>,

    /// Client receive events that did not have a qualifying client send
    /// dependency. These represent server-initiated communications (pushes,
    /// notifications, etc.).
    pub endpoint_simq_push: Vec<PacketEvent>,

    /// Dependency mapping: `dependent_tx[recv_packet_id]` contains all events
    /// triggered by that receive. Each tuple is `(dependent_packet_id,
    /// time_delta_ns, event_kind)`.
    pub dependent_tx: Vec<Vec<(usize, i64, EventKind)>>,
}

/// Performs traffic dependency analysis for client-server communication.
///
/// This function implements the core algorithm that transforms a raw traffic
/// trace into a dependency graph modeling realistic client-server
/// request-response patterns.
///
/// # Algorithm Overview
///
/// ## Client Send Analysis
/// For each client send event:
/// - **No prior receive**: Classified as initial request → goes to
///   `client_simq_push`
/// - **Has prior receive**: Classified as response-triggered → recorded as
///   dependency
///
/// ## Server Response Analysis  
/// For each client receive event:
/// - **Find matching send**: Search for client send ≥
///   `2×ttrace_ts_to_c_delay_ns` before receive time
/// - **Match found**: Server response depends on that client send → recorded as
///   dependency  
/// - **No match**: Server-initiated event → goes to `destination_simq_push`
///
/// # Arguments
///
/// * `s` - Space-separated trace string: `"0,s 18,s 25,r 25,r 30,s 35,r"`
/// * `ttrace_ts_to_c_delay` - Network delay between client and server when the
///   traces was captured, used to determine which packets are dependent on
///   others.
///
/// # Returns
///
/// [`TrafficTraceData`] containing:
/// - `client_simq_push`: Initial client requests (no dependencies)  
/// - `endpoint_simq_push`: Server-initiated events (no dependencies)
/// - `dependent_tx`: Dependency mapping `[recv_id] → [(send_id, delay, kind)]`
///
pub fn traffic_trace_prepare(s: &str, ttrace_ts_to_c_delay_ns: i64) -> TrafficTraceData {
    let mut pkt_events: Vec<PacketEvent> = Vec::new();

    // Parse input string into ordered PacketEvents.
    for (packet_id, token) in s.split_whitespace().enumerate() {
        let parts: Vec<&str> = token.split(',').collect();
        if parts.len() != 2 {
            warn!("Skipping malformed entry: {}", token);
            continue;
        }
        let time_ns: i64 = match parts[0].parse() {
            Ok(v) => v,
            Err(_) => {
                warn!("Invalid timestamp: {}", parts[0]);
                continue;
            }
        };
        let kind = match parts[1] {
            "s" | "sn" => EventKind::CliSend,
            "r" | "rn" => EventKind::CliReceive,
            _ => {
                warn!("Unknown kind '{}'", parts[1]);
                continue;
            }
        };
        pkt_events.push(PacketEvent {
            packet_id,
            time_ns,
            kind,
        });
    }

    // Process client send events: for each send event, if there is a preceding
    // receive, record a dependency; otherwise, mark it as an initial simQ push.
    let mut client_simq_push = Vec::new();
    let mut dependent_tx = vec![Vec::new(); s.split_whitespace().count()];
    let mut last_recv: Option<&PacketEvent> = None;
    for pkt_event in &pkt_events {
        if pkt_event.kind == EventKind::CliReceive {
            last_recv = Some(pkt_event);
        } else if pkt_event.kind == EventKind::CliSend {
            if let Some(prev_recv) = last_recv {
                let delta = pkt_event.time_ns - prev_recv.time_ns;
                dependent_tx[prev_recv.packet_id].push((
                    pkt_event.packet_id,
                    delta,
                    pkt_event.kind,
                ));
            } else {
                client_simq_push.push(*pkt_event);
            }
        }
    }

    // Process endpoint events: for each client receive event, try to find
    // the most recent client send event that occurred at or before (recv time -
    // 2 * ttrace_ts_to_c_delay_ns). If found, record that as a dependency;
    // otherwise, mark the receive as a simQ push for endpoint.
    let client_sends: Vec<&PacketEvent> = pkt_events
        .iter()
        .filter(|e| e.kind == EventKind::CliSend)
        .collect();
    let mut endpoint_simq_push = Vec::new();
    for pkt_event in &pkt_events {
        if pkt_event.kind == EventKind::CliReceive {
            let boundary = pkt_event.time_ns - (2 * ttrace_ts_to_c_delay_ns);
            let candidate = client_sends
                .iter()
                .filter(|&&e| e.time_ns <= boundary)
                .max_by_key(|&&e| e.time_ns);
            if let Some(&client_send) = candidate {
                if pkt_event.time_ns - client_send.time_ns >= 2 * ttrace_ts_to_c_delay_ns {
                    let delta =
                        (pkt_event.time_ns - client_send.time_ns) - 2 * ttrace_ts_to_c_delay_ns;
                    dependent_tx[client_send.packet_id].push((
                        pkt_event.packet_id,
                        delta,
                        pkt_event.kind,
                    ));
                } else {
                    let mut adjusted_event = *pkt_event;
                    adjusted_event.time_ns -= ttrace_ts_to_c_delay_ns;
                    endpoint_simq_push.push(adjusted_event);
                }
            } else {
                // Fix since some traces start with 0,r or time < which is messy,
                let mut adjusted_event = *pkt_event;
                adjusted_event.time_ns -= ttrace_ts_to_c_delay_ns;
                endpoint_simq_push.push(adjusted_event);
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

    debug!(
        "{:#?}\n{:#?}\n{:#?}\n",
        client_simq_push, endpoint_simq_push, dependent_tx
    );
    TrafficTraceData {
        client_simq_push,
        endpoint_simq_push,
        dependent_tx,
    }
}

/// Print the reconstructed traffic trace based on the TrafficTraceData struct.
/// Also prints the SimQ prefill vectors and the dependency hashmap.
pub fn event_schedule_print(traffic: &TrafficTraceData, ttrace_ts_to_c_delay_ns: i64) {
    let mut pkt_events: Vec<PacketEvent> = traffic.client_simq_push.clone();
    pkt_events.extend(
        traffic
            .endpoint_simq_push
            .clone()
            .into_iter()
            .map(|mut pkt_event| {
                pkt_event.time_ns += ttrace_ts_to_c_delay_ns;
                pkt_event
            }),
    );
    let mut event_output: Vec<(usize, String)> = Vec::new();

    println!(
        "Reconstructed client-side traffic trace events, ttrace_ts_to_c_delay_ns : {}:  ",
        ttrace_ts_to_c_delay_ns
    );
    for pkt_event in &pkt_events {
        let kind_str = match pkt_event.kind {
            EventKind::CliSend => "cli_send",
            EventKind::CliReceive => "cli_recv",
        };
        let dep_txt = format!(
            "#{:5},  {:7}, {}  :  simQ_push",
            pkt_event.packet_id, pkt_event.time_ns, kind_str
        );
        event_output.push((pkt_event.packet_id, dep_txt));
    }

    // Create a copy of the dependency vector to drain
    let mut remaining_dependencies = traffic.dependent_tx.clone();

    while remaining_dependencies.iter().any(|deps| !deps.is_empty()) {
        let mut made_progress = false;

        // Iterate through indices to avoid borrowing issues
        for (recv_id, dependency_vector) in remaining_dependencies.iter_mut().enumerate() {
            if dependency_vector.is_empty() {
                continue;
            }

            // Check if the receive event exists in events
            if let Some(recv_event) = pkt_events.clone().iter().find(|e| e.packet_id == recv_id) {
                // We found the receive event, process its dependencies
                let deps = std::mem::take(dependency_vector);
                made_progress = true;

                for (dep_id, delta, event_kind) in deps {
                    let send_time;
                    let dep_txt;
                    if event_kind == EventKind::CliSend {
                        send_time = recv_event.time_ns + delta;
                        dep_txt = format!(
                            "#{:5},  {:7}, cli_send  :  depends_on cli_recv                [#{:5} @{:7}]         [Δt = {:6}]",
                            dep_id, send_time, recv_event.packet_id, recv_event.time_ns, delta
                        );
                    } else {
                        send_time = recv_event.time_ns + 2 * (ttrace_ts_to_c_delay_ns) + delta;
                        dep_txt = format!(
                            "#{:5},  {:7}, cli_recv  :  webserver_send depends_on cli_send [#{:5} @{:7}] [ws send Δt = {:6}]",
                            dep_id, send_time, recv_event.packet_id, recv_event.time_ns, delta
                        );
                    }

                    // Store the dependency info in the output vector
                    event_output.push((dep_id, dep_txt));

                    pkt_events.push(PacketEvent {
                        packet_id: dep_id,
                        time_ns: send_time,
                        kind: event_kind,
                    });
                }
            }
        }

        if !made_progress {
            let remaining_count: usize = remaining_dependencies.iter().map(Vec::len).sum();
            warn!(
                "Could not process remaining {} dependencies due to missing events",
                remaining_count
            );
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
        println!(
            "cli_send  [#{:5}  @{:7}]   simQ_push",
            event.packet_id, event.time_ns
        );
    }

    println!("\nInitial webserver simQ push events:");
    for event in &traffic.endpoint_simq_push {
        println!(
            "cli_recv  [#{:5}  @{:7}]   webserver_send simQ_push",
            event.packet_id, event.time_ns
        );
    }

    println!("\nTX dependency mapping (recv_id -> [dependent_id, Δt]):");
    for (recv_id, deps) in traffic.dependent_tx.iter().enumerate() {
        for (dep_id, delta, event_kind) in deps {
            let dep_txt = if *event_kind == EventKind::CliSend {
                format!(
                    "cli_recv [#{:5}] triggers cli_send [#{:5}] with Δt = {:5}",
                    recv_id, dep_id, delta
                )
            } else {
                format!(
                    "ws_recv  [#{:5}] triggers ws_send  [#{:5}] with Δt = {:5}",
                    recv_id, dep_id, delta
                )
            };
            tx_event_output.push((recv_id, *dep_id, dep_txt));
        }
    }
    // Sort by recv_id and dep_id
    tx_event_output.sort_by_key(|(recv_id, dep_id, _)| (*recv_id, *dep_id));
    for (_, _, text) in tx_event_output {
        println!("{}", text);
    }
}

/// Helper function to get the event instant based on the zero_instant and the
/// relative time in the trace, with the trace is in nanoseconds.
fn get_event_instant(
    si: &mut SimInfo,
    pkt_event: &PacketEvent,
) -> Result<Instant, TraceParseError> {
    if pkt_event.time_ns >= 0 {
        Ok(si.zero_instant + Duration::from_nanos(pkt_event.time_ns as u64))
    } else {
        // Negative offsets can occur due to client receiving at 0,r as in some
        // tests, or it may come from trafserv_to_client_delay being configured
        // too low compared to the actual real delay when the traffic trace was
        // collected.
        let early_instant = si
            .zero_instant
            .checked_sub(Duration::from_nanos(-pkt_event.time_ns as u64))
            .ok_or(TraceParseError::InstantUnderflow(pkt_event.time_ns))?;
        if si.earliest_event_instant == si.zero_instant {
            // print out notification that transfer to client delay is too low
            warn!(
                "Note: Negative offset in traffic trace event: {}. This may indicate that trafserv_to_client_delay is too low compared to the actual delay when the traffic trace was collected.",
                pkt_event.packet_id
            );
        }
        if early_instant < si.earliest_event_instant {
            si.earliest_event_instant = early_instant;
        }
        Ok(early_instant)
    }
}

pub fn fill_simq(
    traffic_events: &TrafficTraceData,
    topology: &NetworkTopology,
    si: &mut SimInfo,
    sq: &mut SimQueue,
) -> Result<(), TraceParseError> {
    for event in &traffic_events.client_simq_push {
        let event_instant = get_event_instant(si, event)?;
        let simul_event = SimEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_id: event.packet_id,
            node_id: topology.client, // Client node index
            link_id: topology.nodes[topology.client].get_coreside_out_id(), // Client->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            q_sequence_nr: 0, // Will be overwritten by push()
            #[cfg(debug_assertions)]
            debug_note: Some("Client initial send".to_string()),
        };
        sq.push(simul_event);
    }

    for event in &traffic_events.endpoint_simq_push {
        let event_instant = get_event_instant(si, event)?;
        let simul_event = SimEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_id: event.packet_id,
            node_id: topology.endpoint, // Endpoint node index
            link_id: topology.nodes[topology.endpoint].get_edgeside_out_id(), // Endpoint->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            q_sequence_nr: 0, // Will be overwritten by push()
            #[cfg(debug_assertions)]
            debug_note: Some("WebServer initial send".to_string()),
        };
        sq.push(simul_event);
    }

    si.dependent_tx = traffic_events.dependent_tx.clone();
    Ok(())
}
