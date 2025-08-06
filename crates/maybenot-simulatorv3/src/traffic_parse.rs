
use std::collections::HashMap;
use std::time::{Duration, Instant};
use log::{debug, warn};

use crate::{SimulEvent, SimulQueue};
use crate::network::NetworkTopology;
use maybenot::TriggerEvent;

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

    sq.highest_depend_tx = oneline.split_whitespace().count();

    let traffic_events = traffic_trace_prepare(&oneline, ttrace_ts_to_c_delay.as_nanos() as i64);

    fill_simq(&traffic_events, &topology, &mut sq);
    let total_dependent_events: usize = traffic_events.dependent_tx.values().map(|v| v.len()).sum();
    println!("SimQ length: {:?}   oneline events: {:?} tx_dpend length: {:?} tx_dpend events: {:?}", sq.len(), sq.highest_depend_tx, traffic_events.dependent_tx.len(), total_dependent_events);
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
    pub trafficserver_simq_push: Vec<PacketEvent>,
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
pub fn traffic_trace_prepare(s: &String, ttrace_ts_to_c_delay_ns: i64) -> TrafficTraceData {
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

    // Process trafficserver events: for each client receive event, try to find the most recent client send event
    // that occurred at or before (recv time - 2 * ttrace_ts_to_c_delay_ns). If found,
    // record that as a dependency; otherwise, mark the receive as a simQ push for trafficserver.
    let client_sends: Vec<&PacketEvent> = pkt_events.iter().filter(|e| e.kind == EventKind::CliSend).collect();
    let mut trafficserver_simq_push = Vec::new();
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
                    trafficserver_simq_push.push(adjusted_event);
                } 
            } else {
                // Fix since some traces start with 0,r or time < which is messy, 
                let mut adjusted_event = pkt_event.clone();
                adjusted_event.time_ns -= ttrace_ts_to_c_delay_ns;
                trafficserver_simq_push.push(adjusted_event);
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

    debug!("{:#?}\n{:#?}\n{:#?}\n", client_simq_push, trafficserver_simq_push, dependent_tx);
    TrafficTraceData {
        client_simq_push,
        trafficserver_simq_push,
        dependent_tx,
    }
    
}


/// Print the reconstructed traffic trace based on the TrafficTraceData struct.
/// Also prints the SimQ prefill vectors and the dependency hashmap.
pub fn event_schedule_print(traffic: &TrafficTraceData, ttrace_ts_to_c_delay_ns: i64) {
    let mut pkt_events: Vec<PacketEvent> = traffic.client_simq_push.clone();
    pkt_events.extend(
        traffic
            .trafficserver_simq_push
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
    for event in &traffic.trafficserver_simq_push {
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
            link_idx: topology.nodes[topology.client].get_coreside_out_id(), // Client->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            q_sequence_nr: 0, // Will be overwritten by push()
            #[cfg(debug_assertions)]
            debug_note: Some("Client initial send".to_string()),
        };
        sq.push(simul_event);
    }
    
    for event in &traffic_events.trafficserver_simq_push {
        let event_instant = get_event_instant(sq, event);
        let simul_event = SimulEvent {
            event: TriggerEvent::NormalSent,
            time: event_instant,
            packet_idx: event.packet_idx,
            node_idx: topology.traffic_server, // TrafficServer node index
            link_idx: topology.nodes[topology.traffic_server].get_edgeside_out_id(), // TrafficServer->Relay link
            contains_padding: false,
            bypass: false,
            replace: false,
            q_sequence_nr: 0, // Will be overwritten by push()
            #[cfg(debug_assertions)]
            debug_note: Some("WebServer initial send".to_string()),
        };
        sq.push(simul_event);
    }
     
    sq.dependent_tx = traffic_events.dependent_tx.clone();
}



