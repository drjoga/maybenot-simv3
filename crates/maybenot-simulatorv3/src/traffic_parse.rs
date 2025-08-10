
use std::time::{Duration, Instant};
use log::{debug, warn};

use crate::{SimulEvent, SimulInfo, SimulQueue};
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
pub fn parse_trace(trace: &str, topology: &NetworkTopology, ttrace_ts_to_c_delay: Duration) -> (SimulInfo, SimulQueue) {
    let mut si = SimulInfo::new();
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

    let traffic_events = traffic_trace_prepare(&oneline, ttrace_ts_to_c_delay.as_nanos() as i64);

    fill_simq(&traffic_events, topology, &mut si, &mut sq);
    //let total_dependent_events: usize = traffic_events.dependent_tx.values().map(|v| v.len()).sum();
    //println!(" Online events: {:?}   SimQ length: {:?}   tx_dpend length: {:?} tx_dpend events: {:?}", oneline.split_whitespace().count(), sq.len(), traffic_events.dependent_tx.len(), total_dependent_events);
    (si, sq)
}




/// Code for reading in traffic trace, create depndent_tx, and prefill SimulQueue 

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
    pub dependent_tx: Vec<Vec<(usize, i64, EventKind)>>,
}

/// Parses the input string (e.g. "0,s 18,s 25,r 25,r 30,s 35,r") and the given delay,
/// then builds a TrafficTraceData struct:
/// - For client sends, if there is no preceding receive, the event is a simQ_push; otherwise, it is recorded
///   as a dependency of the most recent receive.
/// - For each receive event, we search among client send events for the most recent candidate whose timestamp
///   is at or before (recv time - 4×delay). If found (and the time difference is at least 4×delay), that dependency
///   is recorded; otherwise, the receive event is treated as a webserver simQ_push event.
pub fn traffic_trace_prepare(s: &str, ttrace_ts_to_c_delay_ns: i64) -> TrafficTraceData {
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
    let mut dependent_tx = vec![Vec::new(); s.split_whitespace().count()];
    let mut last_recv: Option<&PacketEvent> = None;
    for pkt_event in &pkt_events {
        if pkt_event.kind == EventKind::CliReceive {
            last_recv = Some(pkt_event);
        } else if pkt_event.kind == EventKind::CliSend {
            if let Some(prev_recv) = last_recv {
                let delta = pkt_event.time_ns - prev_recv.time_ns;
                dependent_tx[prev_recv.packet_idx].push((pkt_event.packet_idx, delta, pkt_event.kind));
            } else {
                client_simq_push.push(*pkt_event);
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
                    dependent_tx[client_send.packet_idx].push((pkt_event.packet_idx, delta, pkt_event.kind));
                } else {
                    let mut adjusted_event = *pkt_event;
                    adjusted_event.time_ns -= ttrace_ts_to_c_delay_ns;
                    trafficserver_simq_push.push(adjusted_event);
                } 
            } else {
                // Fix since some traces start with 0,r or time < which is messy, 
                let mut adjusted_event = *pkt_event;
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

    // Create a copy of the dependency vector to drain
    let mut remaining_dependencies = traffic.dependent_tx.clone();
    
    
    while remaining_dependencies.iter().any(|deps| !deps.is_empty()) {
        let mut made_progress = false;
        
        // Iterate through indices to avoid borrowing issues
        for (recv_idx, dependency_vector) in remaining_dependencies.iter_mut().enumerate() {
            if dependency_vector.is_empty() {
                continue;
            }
            
            // Check if the receive event exists in events
            if let Some(recv_event) = pkt_events.clone().iter().find(|e| e.packet_idx == recv_idx) {
                // We found the receive event, process its dependencies
                let deps = std::mem::take(dependency_vector);
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

        if !made_progress {
            let remaining_count: usize = remaining_dependencies.iter().map(|deps| deps.len()).sum();
            eprintln!("Warning: Could not process remaining {} dependencies due to missing events", remaining_count);
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
    for (recv_idx, deps) in traffic.dependent_tx.iter().enumerate() {
        for (dep_idx, delta, event_kind) in deps {
            let dep_txt = if *event_kind == EventKind::CliSend {
                format!("cli_recv [#{:5}] triggers cli_send [#{:5}] with Δt = {:5}", recv_idx, dep_idx, delta)
            } else {
                format!("ws_recv  [#{:5}] triggers ws_send  [#{:5}] with Δt = {:5}", recv_idx, dep_idx, delta)
            };
            tx_event_output.push((recv_idx, *dep_idx, dep_txt));
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
fn get_event_instant(si: &mut SimulInfo, pkt_event: &PacketEvent) -> Instant {
    if pkt_event.time_ns >= 0 {
        si.zero_instant + Duration::from_nanos(pkt_event.time_ns as u64)
    } else {
        // Negative offsets can occur due to client receiving at 0,r as in some tests, or it may
        // come from trafserv_to_client_delay being configured too low compared to the actual real delay
        // when the traffic trace was collected.
        let early_instant = si.zero_instant.checked_sub(Duration::from_nanos(-pkt_event.time_ns as u64)).expect("Underflow for Instant");
        if si.earliest_event_instant == si.zero_instant {
            // print out notification that trafser to client delay is too low
            warn!("Note: Negative offset in traffic trace event: {}. This may indicate that trafserv_to_client_delay is too low compared to the actual delay when the traffic trace was collected.", pkt_event.packet_idx);
        } 
        if early_instant < si.earliest_event_instant {
            si.earliest_event_instant = early_instant;
        }                 
        early_instant
    }
}



pub fn fill_simq(traffic_events: &TrafficTraceData, topology: &NetworkTopology, si: &mut SimulInfo, sq: &mut SimulQueue) {

    for event in &traffic_events.client_simq_push {
        let event_instant = get_event_instant(si, event);
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
        let event_instant = get_event_instant(si, event);
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
     
    si.dependent_tx = traffic_events.dependent_tx.clone();
}

/// Modify TOML configuration by applying parameter changes specified in modifier string.
/// 
/// # Arguments
/// * `toml_in` - Input TOML configuration string
/// * `modifier_string` - Modifications in format: "SectionType:ID::param1:value1::param2:value2\n..."
///                      Supported SectionTypes: "Node", "Link"
/// 
/// # Example
/// ```
/// let modifications = "Link:0::prop_us:5000::tput_bps:50000000\nNode:1::ts_prop_us:10000";
/// let modified_toml = modify_toml(&original_toml, modifications)?;
/// ```
pub fn modify_toml(toml_in: &str, modifier_string: &str) -> Result<String, String> {
    // Parse input TOML into a mutable value
    let mut toml_value: toml::Value = toml::from_str(toml_in)
        .map_err(|e| format!("Failed to parse input TOML: {}", e))?;
    
    // Get the root table
    let root_table = toml_value.as_table_mut()
        .ok_or("TOML root is not a table")?;
    
    // Process each modification line
    for line in modifier_string.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        
        // Parse line format: "SectionType:ID::param1:value1::param2:value2"
        let parts: Vec<&str> = line.split("::").collect();
        if parts.is_empty() {
            return Err("Empty modification line".to_string());
        }
        
        // Parse section type and ID from first part
        let section_parts: Vec<&str> = parts[0].split(':').collect();
        if section_parts.len() != 2 {
            return Err(format!("Invalid section format in line: {}", line));
        }
        
        let section_type = section_parts[0];
        let section_id: usize = section_parts[1].parse()
            .map_err(|_| format!("Invalid section ID in line: {}", line))?;
        
        // Find the appropriate section array
        let section_array = match section_type {
            "Node" => root_table.get_mut("Node"),
            "Link" => root_table.get_mut("Link"),
            _ => return Err(format!("Unsupported section type: {}", section_type)),
        };
        
        let section_array = section_array
            .and_then(|v| v.as_array_mut())
            .ok_or(format!("Section {} is not an array", section_type))?;
        
        // Find the specific section by ID
        let target_section = section_array.iter_mut()
            .find(|entry| {
                entry.as_table()
                    .and_then(|table| table.get("id"))
                    .and_then(|id| id.as_integer())
                    .map(|id| id == section_id as i64)
                    .unwrap_or(false)
            })
            .ok_or(format!("Section {} with ID {} not found", section_type, section_id))?;
        
        let target_table = target_section.as_table_mut()
            .ok_or("Section entry is not a table".to_string())?;
        
        // Apply parameter modifications from remaining parts
        for param_part in &parts[1..] {
            let param_kv: Vec<&str> = param_part.split(':').collect();
            if param_kv.len() != 2 {
                return Err(format!("Invalid parameter format in: {}", param_part));
            }
            
            let param_name = param_kv[0];
            let param_value_str = param_kv[1];
            
            // Convert value to appropriate TOML type
            let param_value = if let Ok(int_val) = param_value_str.parse::<i64>() {
                toml::Value::Integer(int_val)
            } else if let Ok(float_val) = param_value_str.parse::<f64>() {
                toml::Value::Float(float_val)
            } else if let Ok(bool_val) = param_value_str.parse::<bool>() {
                toml::Value::Boolean(bool_val)
            } else {
                toml::Value::String(param_value_str.to_string())
            };
            
            // Update the parameter in the target section
            target_table.insert(param_name.to_string(), param_value);
        }
    }
    
    // Serialize back to TOML string
    toml::to_string_pretty(&toml_value)
        .map_err(|e| format!("Failed to serialize TOML: {}", e))
}



