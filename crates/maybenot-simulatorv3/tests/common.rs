use std::env;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};

use log::debug;
use maybenot::{action::Action, state::State, Machine, TriggerEvent};
use maybenot_simulatorv3::{
    event_schedule_print, SimulEvent,
    network::{Network, NetworkTopology},
    simul_advanced, traffic_trace_prepare, fill_simq, SimulatorArgs, SimulQueue
};
use once_cell::sync::Lazy;

#[allow(clippy::too_many_arguments)]
pub fn run_test_sim(
    input: &str,
    output: &str,
    delay: Duration,
    machines_client: &[Machine],
    machines_server: &[Machine],
    client: bool,
    max_trace_length: usize,
    only_packets: bool,
    as_ms: bool,
) {
    //let config_path = "basic_test.toml";
    let config_path = "mbn_test.toml";
    //Read in config path to toml_str
    let mut toml_str = std::fs::read_to_string(config_path)
        .expect("Failed to read the configuration file");
    //Go through toml_str and change all values for 'prop_us =' in toml_str to be the value of delay variable
    toml_str = toml_str.lines()
        .map(|line| {
            if line.trim_start().starts_with("prop_us") {
                format!("prop_us = {}", delay.as_micros())
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let (topology, mut linkstate) = Network::from_toml_str(&toml_str)
        .expect("Failed to parse the network configuration from TOML string");
    print!("toml string: {}\n", toml_str);
    // The trafficserver events require incresing the max length compared to what is specced in old tests
    let max_trace_length = 2 * max_trace_length;
    let mut args = SimulatorArgs::new(max_trace_length, only_packets);
    args.continue_after_all_normal_packets_processed = false;
    let mut sq = make_sq(input.to_string(), &topology, delay, as_ms);
    let trace = simul_advanced(machines_client, machines_server, &topology, &mut linkstate, &mut sq, &args);
    //print!("{:?}\n\n", trace);
    // Loop over all events in trace and print them
    for event in &trace {
        println!("{}", event.display_full(&sq,&topology,&linkstate));
    }
    let mut fmt = fmt_trace(trace.as_slice(), client, only_packets, as_ms, topology, &sq);
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

pub fn run_test_sim_trace(
    input: &str,
    output: &str,
    delay: Duration,
    machines_client: &[Machine],
    machines_server: &[Machine],
    client: bool,
    max_trace_length: usize,
    only_packets: bool,
    as_ms: bool,
    description: &str,
    use_network: &str,
    skip_asserts: bool,
) {

    let config_path = "../crates/maybenot-simulatorv3/basic_test.toml";
    //Read in config path to toml_str
    let mut toml_str = std::fs::read_to_string(config_path)
        .expect("Failed to read the configuration file");
    //Go through toml_str and change all prop_us in toml_str to the value of delay variable
    toml_str = toml_str.replace("prop_us", &format!("{}", delay.as_micros()));
    toml_str = adjust_toml_string(toml_str, use_network.to_string(), TraceSpec::ether100M);
    
    let (topology, mut linkstate) = Network::from_toml_str(&toml_str)
        .expect("Failed to parse the network configuration from TOML string");



    // The trafficserver events require incresing the max length compared to what is specced in tests
    let max_trace_length = 4 * max_trace_length;
    let mut args = SimulatorArgs::new(max_trace_length, only_packets);
    args.continue_after_all_normal_packets_processed = true;
    let tracefilename = format!("{}__{}.simtrace", description, use_network);

    let mut sq = make_sq(input.to_string(), &topology, delay, as_ms);
    let trace = run_and_save_trace(&tracefilename, || {
        simul_advanced(machines_client, machines_server, &topology, &mut linkstate, &mut sq, &args)
    });

    let mut fmt = fmt_trace(trace.as_slice(), client, only_packets, as_ms, topology, &sq);
    if fmt.len() > output.len() {
        fmt = fmt.get(0..output.len()).unwrap().to_string();
    }
    debug!("input: {}", input);
    if !skip_asserts {
        assert_eq!(output, fmt);
    }
}

fn fmt_trace(trace: &[SimulEvent], client: bool, only_packets: bool, ms: bool, topology: NetworkTopology, sq: &SimulQueue) -> String {
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

    let base = sq.zero_instant;
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
            if s_event.node_idx == topology.mb_server && 
            (s_event.link_idx == topology.nodes[topology.mb_server].get_edgeside_linkid()  || 
            // FIXME: Remove hardcoding!!
            s_event.link_idx == 2) {
                s = format!("{} {}", s, fmt_event(s_event, base, ms));
            }
        }
    }
    s.trim().to_string()
}


pub fn make_sq(s: String, topology: &NetworkTopology, delay: Duration, as_ms: bool) -> SimulQueue {
    let mut sq = SimulQueue::new();
    let to_ns_factor = match as_ms {
        true => 1_000_000 ,
        false => 1_000 
        
    };
    let ttrace_ts_to_c_delay_ns =  delay.as_micros() as i64 * 1_000 * 2;
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
 /* 
    let s = s.lines()
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
*/

    sq.highest_depend_tx = s.split_whitespace().count();
    let traffic_events = traffic_trace_prepare(&s, ttrace_ts_to_c_delay_ns);
    print!("----------------------------------\n");
    event_schedule_print(&traffic_events, ttrace_ts_to_c_delay_ns);
    print!("----------------------------------\n");

    fill_simq(&traffic_events, topology, &mut sq);        
    sq

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

/// Runs the closure `f` to produce a result (e.g. the trace), and if the
/// environment variable `SAVE_TRACE` is set to "1", writes the formatted result
/// to the specified filename.
/// eg.   $SAVE_TRACE=1 cargo test
static SAVE_TRACE: Lazy<bool> = Lazy::new(|| match env::var("SAVE_TRACE").as_deref() {
    Ok("0") => false,
    Ok("1") => true,
    Ok(v) => panic!("Invalid SAVE_TRACE value: {}. Expected 0 or 1.", v),
    Err(_) => false,
});

pub fn run_and_save_trace<T, F>(filename: &str, f: F) -> T
where
    F: FnOnce() -> T,
    T: std::fmt::Debug,
{
    let result = f();

    if *SAVE_TRACE {
        let mut file = File::create(filename).expect("Failed to create trace output file");
        write!(file, "{:#?}", result).expect("Failed to write trace to file");
        println!("Trace saved to {}", filename);
    }
    result
}
