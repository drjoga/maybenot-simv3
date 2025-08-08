use std::env;
use std::time::{Duration, Instant};

use log::debug;
use maybenot::{action::Action, state::State, Machine, TriggerEvent};
use maybenot_simulatorv3::{
    event_schedule_print, 
    SimulEvent,
    network::{Network, NetworkTopology},
    simul_advanced, traffic_trace_prepare, fill_simq, SimulatorArgs, SimulInfo, SimulQueue
};
use once_cell::sync::Lazy;

#[allow(clippy::too_many_arguments)]
pub fn run_test_sim(
    input: &str,
    output: &str,
    propagation_delay: Duration,
    machines_client: &[Machine],
    machines_server: &[Machine],
    client: bool,
    max_trace_length: usize,
    only_packets: bool,
    as_ms: bool,
) {
    let config_files = [
        "tests/mbn_baseline_test.toml",
        "tests/mbn_fast_test.toml",
        "tests/mbn_complex_test.toml"
    ];

    for config_file in config_files.iter() {
        run_test_sim_toml(
            input,
            output,
            propagation_delay,
            machines_client,
            machines_server,
            client,
            max_trace_length,
            only_packets,
            as_ms,
            config_file,
        );
    }
}


#[allow(clippy::too_many_arguments)]
pub fn run_test_sim_toml(
    input: &str,
    output: &str,
    propagation_delay: Duration,
    machines_client: &[Machine],
    machines_server: &[Machine],
    client: bool,
    max_trace_length: usize,
    only_packets: bool,
    as_ms: bool,
    config_file: &str,

) {
    //Read in config path to toml_str
    let toml_str = std::fs::read_to_string(config_file)
        .expect("Failed to read TOML configuration file");
    // Create Topology and linkstate from the TOML string   
    let (topology, mut linkstate) = Network::from_toml_str(&toml_str)
        .expect("Failed to parse the network configuration from TOML string");
    // The additional events in more complex topologies require increasing the max length compared to what is specced in old tests
    let max_trace_length = 3 * max_trace_length;
    let mut args = SimulatorArgs::new(max_trace_length, only_packets);
    args.continue_after_all_normal_packets_processed = false;
    // The test cases assume the timing from netsimv1, where the client <--> relay/server <--> trafficserver
    // have two occurences of the link delay, so create that to apply when parsing the trace.
    let adjusted_delay =  propagation_delay * 2;
    let (si,mut sq) = make_si_sq(input.to_string(), &topology, adjusted_delay, as_ms);
    // Check if the topology has a short-circuiting relay mbn tserver
    // If so, we need to adjust the delay for the trafficserver SimQ events
    // TODO: Should be generalized away by separating trace_ts_client_delay and sim_ts_client_delay
    if matches!(topology.nodes[topology.mb_server],
          maybenot_simulatorv3::nodes::NodeType::RelayMBNtserver(_)) {
            // Iterate over the SimulEvents in the queue and adjust the time for trafficserver events
            let mut events: Vec<_> = sq.heap.drain().collect();
            for event in events.iter_mut() {
                if event.node_idx == topology.mb_server && event.event == TriggerEvent::NormalSent {
                    // Adjust the time by adding the propagation delay trafserv <--> relay/server
                    event.time += propagation_delay;
                }
            }
            sq.heap.extend(events);
    }

    let trace = simul_advanced(machines_client, machines_server, &topology, &mut linkstate, &si, &mut sq, &args);
    if *SHOW_EVENTS {
        for event in &trace {
            println!("{}", event.display_full(&si,&topology,&linkstate));
        }
    }
    let mut fmt = fmt_trace(trace.as_slice(), client, only_packets, as_ms, topology, &si);
    if fmt.len() > output.len() {
        fmt = fmt.get(0..output.len()).unwrap().to_string();
    }
    debug!("input: {}", input);
    assert_eq!(output, fmt);
}

#[allow(non_camel_case_types)]
pub enum TraceSpec {
    ether100M,
    ether100M_10M_assym,
}

pub fn adjust_toml_string(
    mut toml_str: String,
    use_network: String,
    tracespec: TraceSpec,
) -> String {
    match (use_network.as_str(), tracespec) {
        ("hires", TraceSpec::ether100M) => {
            // Replace FixedTput links with HiTraceTput and add trace file
            toml_str = toml_str.replace(
                r#"type = "FixedTput"
tput_bps = 100_000_000_000_000"#,
                r#"type = "HiTraceTput"
trace_file = "tests/ether100M_synth5M.ltbin.gz""#
            );
            toml_str
        }
        ("hires", TraceSpec::ether100M_10M_assym) => {
            // For asymmetric, use different trace files for different links
            // Note: In simplex architecture, we use ether10M for one direction, ether100M for other
            toml_str = toml_str.replace(
                r#"type = "FixedTput"
tput_bps = 100_000_000_000_000"#,
                r#"type = "HiTraceTput"
trace_file = "tests/ether10M_synth5M.ltbin.gz""#
            );
            // Replace only the first two occurrences with ether100M for the return path
            let mut count = 0;
            let parts: Vec<&str> = toml_str.split(r#"trace_file = "tests/ether10M_synth5M.ltbin.gz""#).collect();
            let mut result = String::new();
            for (i, part) in parts.iter().enumerate() {
                result.push_str(part);
                if i < parts.len() - 1 {
                    if count < 2 {
                        result.push_str(r#"trace_file = "tests/ether100M_synth5M.ltbin.gz""#);
                        count += 1;
                    } else {
                        result.push_str(r#"trace_file = "tests/ether10M_synth5M.ltbin.gz""#);
                    }
                }
            }
            result
        }
        ("stdres", TraceSpec::ether100M) => {
            // Replace FixedTput links with StdTraceTput and add trace file
            toml_str = toml_str.replace(
                r#"type = "FixedTput"
tput_bps = 100_000_000_000_000"#,
                r#"type = "StdTraceTput"
trace_file = "tests/ether100M_synth10K_std.ltbin.gz""#
            );
            toml_str
        }
        ("stdres", TraceSpec::ether100M_10M_assym) => {
            // For asymmetric, use different trace files for different links
            toml_str = toml_str.replace(
                r#"type = "FixedTput"
tput_bps = 100_000_000_000_000"#,
                r#"type = "StdTraceTput"
trace_file = "tests/ether10M_synth10K_std.ltbin.gz""#
            );
            // Replace only the first two occurrences with ether100M for the return path
            let mut count = 0;
            let parts: Vec<&str> = toml_str.split(r#"trace_file = "tests/ether10M_synth10K_std.ltbin.gz""#).collect();
            let mut result = String::new();
            for (i, part) in parts.iter().enumerate() {
                result.push_str(part);
                if i < parts.len() - 1 {
                    if count < 2 {
                        result.push_str(r#"trace_file = "tests/ether100M_synth10K_std.ltbin.gz""#);
                        count += 1;
                    } else {
                        result.push_str(r#"trace_file = "tests/ether10M_synth10K_std.ltbin.gz""#);
                    }
                }
            }
            result
        }
        ("fixed", TraceSpec::ether100M) => {
            // Keep FixedTput but set appropriate throughput values
            toml_str = toml_str.replace(
                "tput_bps = 100_000_000_000_000",
                "tput_bps = 100_000_000"
            );
            toml_str
        }
        ("fixed", TraceSpec::ether100M_10M_assym) => {
            // Set asymmetric throughput values
            let mut result = toml_str.clone();
            let mut count = 0;
            while let Some(pos) = result.find("tput_bps = 100_000_000_000_000") {
                let replacement = if count < 2 {
                    "tput_bps = 100_000_000"  // First two links get 100M
                } else {
                    "tput_bps = 10_000_000"   // Other links get 10M
                };
                result.replace_range(pos..pos + "tput_bps = 100_000_000_000_000".len(), replacement);
                count += 1;
            }
            result
        }
        ("bneck", _) => {
            // Keep the original TOML for bottleneck testing
            toml_str
        }
        (other, _) => panic!(
            "Invalid USE_NETWORK value: {}. Expected either 'hires', 'stdres', 'fixed', 'bneck'.",
            other
        ),
    }
}


fn fmt_trace(trace: &[SimulEvent], client: bool, only_packets: bool, ms: bool, topology: NetworkTopology, si: &SimulInfo) -> String {
    fn fmt_event(e: &SimulEvent, base: Instant, ms: bool) -> String {
        let time_value = if e.time >= base {
            // Event is at or after base time
            match ms {
                true => e.time.duration_since(base).as_millis() as i64,
                false => e.time.duration_since(base).as_micros() as i64,
            }
        } else {
            // Event is before base time (negative time)
            let duration = base.duration_since(e.time);
            match ms {
                true => -(duration.as_millis() as i64),
                false => -(duration.as_micros() as i64),
            }
        };
        
        format!("{},{}", time_value, e.event)
    }

    let base = si.zero_instant;
    let mut s: String = "".to_string();
    for s_event in trace {
        if only_packets && s_event.event != TriggerEvent::TunnelSent && s_event.event != TriggerEvent::TunnelRecv {
            continue; // Skip non-tunnel events
        }
        if client {
            if s_event.node_idx == topology.client {
                s = format!("{} {}", s, fmt_event(s_event, base, ms));
            }
        } else {
            // Only show events on the servers "interface" towards client
            let edgeside_out  = topology.nodes[topology.mb_server].get_edgeside_out_id();
            let edgeside_in = topology.nodes[topology.mb_server].get_edgeside_in_id();
            if s_event.node_idx == topology.mb_server && 
            (s_event.link_idx == edgeside_out  || s_event.link_idx == edgeside_in) {
                s = format!("{} {}", s, fmt_event(s_event, base, ms));
            }
        }
    }
    s.trim().to_string()
}


pub fn make_si_sq(s: String, topology: &NetworkTopology, delay: Duration, as_ms: bool) -> (SimulInfo, SimulQueue) {
    let mut si = SimulInfo::new();
    let mut sq = SimulQueue::new();
    let to_ns_factor = match as_ms {
        true => 1_000_000 ,
        false => 1_000 
        
    };
    let ttrace_ts_to_c_delay_ns =  delay.as_micros() as i64 * 1_000;
    //traffic_trace_prepare now expects the delay in nanoseconds, 
    //so loop throough the string and convert to nanoseconds
    // nonwithstanding the as_ms flag
    let s = s.split_whitespace()
        .map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 2 {
                let time = parts[0].parse::<i64>().unwrap() * to_ns_factor; // Convert to nanoseconds
                let direction = parts[1].to_string();
                format!("{},{}", time, direction)
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    let traffic_events = traffic_trace_prepare(&s, ttrace_ts_to_c_delay_ns);
    if *SHOW_PARSING {
        print!("----- Parsing -----------------------------\n");
        event_schedule_print(&traffic_events, ttrace_ts_to_c_delay_ns);
        print!("----------------------------------\n");
    }

    fill_simq(&traffic_events, topology, &mut si, &mut sq);
    (si, sq)

}



pub fn set_bypass(s: &mut State, value: bool) {
    if let Some(ref mut a) = s.action {
        match a {
            Action::BlockOutgoing { bypass, .. } => {
                *bypass = value;
            }
            Action::SendPadding { bypass, .. } => {
                *bypass = value;
            }
            _ => {}
        }
    }
}

pub fn set_replace(s: &mut State, value: bool) {
    if let Some(ref mut a) = s.action {
        match a {
            Action::BlockOutgoing { replace, .. } => {
                *replace = value;
            }
            Action::SendPadding { replace, .. } => {
                *replace = value;
            }
            _ => {}
        }
    }
}

/// If the
/// environment variable `SAVE_TRACE` is set to "1", writes the formatted result
/// to the specified filename.
/// eg.   $SHOW_TRACE=1 cargo test
static SHOW_EVENTS: Lazy<bool> = Lazy::new(|| match env::var("SHOW_EVENTS").as_deref() {
    Ok("0") => false,
    Ok("1") => true,
    Ok(v) => panic!("Invalid SHOW_EVENTS value: {}. Expected 0 or 1.", v),
    Err(_) => false,
});

static SHOW_PARSING: Lazy<bool> = Lazy::new(|| match env::var("SHOW_PARSING").as_deref() {
    Ok("0") => false,
    Ok("1") => true,
    Ok(v) => panic!("Invalid SHOW_PARSING value: {}. Expected 0 or 1.", v),
    Err(_) => false,
});

