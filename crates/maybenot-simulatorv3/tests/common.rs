use std::env;
use std::time::{Duration, Instant};

use log::debug;
use maybenot::{Machine, TriggerEvent, action::Action, state::State};
use maybenot_simulatorv3::{
    SimEvent, SimInfo, SimQueue, SimulatorArgs, event_schedule_print, fill_simq,
    load_topology_from_str, sim_advanced, topology::NetworkTopology, traffic_trace_prepare,
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
        "tests/cfg/maybenot_baseline_test.toml",
        "tests/cfg/maybenot_fast_test.toml",
        "tests/cfg/maybenot_complex_test.toml",
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
    let toml_str =
        std::fs::read_to_string(config_file).expect("Failed to read TOML configuration file");
    // Create Topology and linkstate from the TOML string
    let (topology, mut linkstate) = load_topology_from_str(&toml_str)
        .expect("Failed to parse the network configuration from TOML string");
    // The additional events in more complex topologies require increasing the max length compared to what is specced in old tests
    let max_trace_length = 3 * max_trace_length;
    let mut args = SimulatorArgs::new(max_trace_length, only_packets);
    args.continue_after_all_normal_packets_processed = false;
    // The test cases assume the timing from netsimv1, where the client <--> relay/server <--> destination
    // have two occurences of the link delay, so create that to apply when parsing the trace.
    let adjusted_delay = propagation_delay * 2;
    let (si, mut sq) = make_si_sq(input.to_string(), &topology, adjusted_delay, as_ms);
    // Check if the topology has a short-circuiting relay maybenot destination
    // If so, we need to adjust the delay for the destination SimQ events
    // TODO: Should be generalized away by separating trace_destination_client_delay and sim_destination_client_delay
    if matches!(
        topology.nodes[topology.mb_server],
        maybenot_simulatorv3::nodes::NodeType::RelayMaybenotDestination(_)
    ) {
        // Iterate over the SimEvents in the queue and adjust the time for
        // destination events
        let mut events: Vec<_> = sq.heap.drain().collect();
        for event in events.iter_mut() {
            if event.node_id == topology.mb_server && event.event == TriggerEvent::NormalSent {
                // Adjust the time by adding the propagation delay destination <--> relay/server
                event.time += propagation_delay;
            }
        }
        sq.heap.extend(events);
    }

    let trace = sim_advanced(
        machines_client,
        machines_server,
        &topology,
        &mut linkstate,
        &si,
        &mut sq,
        &args,
    );
    if *SHOW_EVENTS {
        for event in &trace {
            println!("{}", event.display_full(&si, &topology, &linkstate));
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

fn fmt_trace(
    trace: &[SimEvent],
    client: bool,
    only_packets: bool,
    ms: bool,
    topology: NetworkTopology,
    si: &SimInfo,
) -> String {
    fn fmt_event(e: &SimEvent, base: Instant, ms: bool) -> String {
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
        if only_packets
            && s_event.event != TriggerEvent::TunnelSent
            && s_event.event != TriggerEvent::TunnelRecv
        {
            continue; // Skip non-tunnel events
        }
        if client {
            if s_event.node_id == topology.client {
                s = format!("{} {}", s, fmt_event(s_event, base, ms));
            }
        } else {
            // Only show events on the servers "interface" towards client
            let edgeside_out = topology.nodes[topology.mb_server].get_edgeside_out_id();
            let edgeside_in = topology.nodes[topology.mb_server].get_edgeside_in_id();
            if s_event.node_id == topology.mb_server
                && (s_event.link_id == edgeside_out || s_event.link_id == edgeside_in)
            {
                s = format!("{} {}", s, fmt_event(s_event, base, ms));
            }
        }
    }
    s.trim().to_string()
}

pub fn make_si_sq(
    s: String,
    topology: &NetworkTopology,
    delay: Duration,
    as_ms: bool,
) -> (SimInfo, SimQueue) {
    let mut si = SimInfo::new();
    let mut sq = SimQueue::new();
    let to_ns_factor = match as_ms {
        true => 1_000_000,
        false => 1_000,
    };
    let ttrace_ts_to_c_delay_ns = delay.as_micros() as i64 * 1_000;
    //traffic_trace_prepare now expects the delay in nanoseconds,
    //so loop throough the string and convert to nanoseconds
    // nonwithstanding the as_ms flag
    let s = s
        .split_whitespace()
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
        println!("----- Parsing -----------------------------");
        event_schedule_print(&traffic_events, ttrace_ts_to_c_delay_ns);
        println!("----------------------------------");
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

/// If the environment variable `SHOW_EVENTS` is set to "1", writes the formatted events
/// eg.   $SHOW_EVENTS=1 cargo test  --test simulator test_both_block_machine  -- --nocapture
static SHOW_EVENTS: Lazy<bool> = Lazy::new(|| match env::var("SHOW_EVENTS").as_deref() {
    Ok("0") => false,
    Ok("1") => true,
    Ok(v) => panic!("Invalid SHOW_EVENTS value: {}. Expected 0 or 1.", v),
    Err(_) => false,
});

/// If the environment variable `SHOW_PARSING` is set to "1", writes the parsed traffic trace
/// eg.   $SHOW_PARSING=1 cargo test  --test simulator test_both_block_machine  -- --nocapture
static SHOW_PARSING: Lazy<bool> = Lazy::new(|| match env::var("SHOW_PARSING").as_deref() {
    Ok("0") => false,
    Ok("1") => true,
    Ok(v) => panic!("Invalid SHOW_PARSING value: {}. Expected 0 or 1.", v),
    Err(_) => false,
});

// Trace setup for trace-dependent tests
#[cfg(feature = "trace-tests")]
use std::process::Command;
#[cfg(feature = "trace-tests")]
use std::sync::Once;

#[cfg(feature = "trace-tests")]
static TRACE_INIT: Once = Once::new();

/// Helper function to ensure traces exist before running trace-dependent tests.
/// Call this at the start of any test that needs generated trace files.
#[cfg(feature = "trace-tests")]
pub fn setup_traces() {
    let manifest_root = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let tests_dir = std::path::Path::new(&manifest_root).join("tests");
    ensure_traces_exist(&tests_dir);
}

#[cfg(feature = "trace-tests")]
fn ensure_traces_exist(tests_dir: &std::path::Path) {
    TRACE_INIT.call_once(|| {
        let required_trace_files = [
            "ether100M_synth10K_std.ltbin.gz",
            "ether100M_synth5K.tr",
            "ether10M_synth10K_std.ltbin.gz",
            "test100K_synth2M_std.ltbin.gz",
            "ether100M_synth5K.ltbin.gz",
            "ether100M_synth5M.ltbin.gz",
            "ether100M_synth5M_21bins.ltbin.gz",
            "ether100M_synth10M.ltbin.gz",
            "ether10M_synth5M.ltbin.gz",
            "ether100M_synth40M.ltbin.gz",
        ];

        let data_dir = tests_dir.join("data");
        let missing_files: Vec<_> = required_trace_files
            .iter()
            .filter(|&file| !data_dir.join(file).exists())
            .collect();

        if missing_files.is_empty() {
            return;
        }

        println!(
            "Missing {} trace files, attempting to generate them...",
            missing_files.len()
        );

        let script_path = tests_dir.join("create_testlinktraces.sh");
        if !script_path.exists() {
            panic!("Trace generation script not found: {:?}", script_path);
        }

        let info_cmd = "echo '\\n\\nSTARTING TRACE GENERATION FOR TESTS, \
            WILL TAKE SOME TIME, RUNS ONCE.\\n\\n'";
        let _ = Command::new("bash")
            .arg("-c")
            .arg(info_cmd)
            .current_dir(tests_dir)
            .status();

        let script_result = Command::new("bash")
            .arg(&script_path)
            .current_dir(tests_dir)
            .status();

        match script_result {
            Ok(status) if status.success() => {
                println!("Successfully generated trace files");
            }
            Ok(status) => {
                panic!(
                    "Failed to run trace generation script: exit code {}",
                    status
                );
            }
            Err(e) => {
                panic!("Failed to execute trace generation script: {}", e);
            }
        }

        let still_missing: Vec<_> = required_trace_files
            .iter()
            .filter(|&file| !data_dir.join(file).exists())
            .collect();

        if !still_missing.is_empty() {
            panic!(
                "Failed to generate all required trace files. Still missing: {:?}",
                still_missing
            );
        }
    });
}
