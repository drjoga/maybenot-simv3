use maybenot::{Machine, TriggerEvent};
use maybenot_simulatorv3::{load_topology_from_file, parse_trace, sim, modify_toml, load_topology_from_str};
use std::{str::FromStr, time::Duration};
use maybenot_simulatorv3::{SimulatorArgs, simul_advanced};
use std::fs;

#[test_log::test]
fn full_trace_compare() {
    // Load the EARLY_TEST_TRACE file
    const EARLY_TRACE: &str = include_str!("EARLY_TEST_TRACE.log");

    // Use the same network configuration as the bench
    //let (topology, mut linkstate) = Network::from_toml_file("basic_test.toml").unwrap();
    let (topology, mut linkstate) = load_topology_from_file("mbn_test.toml").unwrap();

    // Parse the trace with the same parameters as the bench
    let trafserv_to_client_delay = Duration::from_millis(20);
    let (si, mut sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);

    // 30097 gives 10000 client events, with basic toml to be used in benching to get comparable times
    //let output_trace = sim(&[], &[], &mut input_trace, &mut sim_network, 30097, true);
    // 56829 gives 10000 client events, with basic toml to be used in benching to get comparable times
    //let output_trace = sim(&[], &[], &mut input_trace, &mut sim_network, 56829, true);

    let output_trace = sim(
        &[],
        &[],
        &si,
        &mut sq,
        &topology,
        &mut linkstate,
        50000,
        true,
    );
    // print length of output trace
    println!("Output trace length: {}", output_trace.len());

    // Print the first 15 events in output trace for debugging
    println!("First 15 events in output trace:");
    for event in output_trace.iter().take(15) {
        println!("{}", event.display_full(&si, &topology, &linkstate));
    }

    // Convert output trace to EARLY_TEST_TRACE format (time,direction) - ignoring size
    let starting_time = si.zero_instant;
    let mut formatted_output = Vec::new();

    for event in output_trace.iter().filter(|e| e.node_id == 0) {
        // Client perspective only
        let relative_time = (event.time - starting_time).as_nanos();
        let direction = match event.event {
            //TriggerEvent::NormalSent | TriggerEvent::PaddingSent { .. } | TriggerEvent::TunnelSent => "s",
            //TriggerEvent::NormalRecv | TriggerEvent::PaddingRecv | TriggerEvent::TunnelRecv => "r",
            TriggerEvent::TunnelSent => "s",
            TriggerEvent::TunnelRecv => "r",
            _ => continue, // Skip other event types
        };
        // Only compare time and direction, ignore packet size
        formatted_output.push(format!("{},{}", relative_time, direction));
    }

    // Parse the expected trace and extract only time and direction
    let expected_lines: Vec<String> = EARLY_TRACE
        .trim()
        .lines()
        .map(|line| {
            let parts: Vec<&str> = line.trim().split(',').collect();
            if parts.len() >= 2 {
                format!("{},{}", parts[0], parts[1]) // Only time and direction
            } else {
                line.trim().to_string()
            }
        })
        .collect();

    // Compare line by line
    println!(
        "Comparing {} expected lines with {} output lines",
        expected_lines.len(),
        formatted_output.len()
    );

    let max_lines = std::cmp::min(expected_lines.len(), formatted_output.len());
    let mut differences = 0;

    for i in 0..max_lines {
        let expected = &expected_lines[i];
        let actual = &formatted_output[i];

        if expected != actual {
            differences += 1;
            if differences <= 10 {
                // Only show first 10 differences
                println!("Line {}: Expected '{}', Got '{}'", i + 1, expected, actual);
            }
        }
    }

    if expected_lines.len() != formatted_output.len() {
        println!(
            "Length mismatch: Expected {} lines, got {}",
            expected_lines.len(),
            formatted_output.len()
        );
    }

    if differences == 0 && expected_lines.len() == formatted_output.len() {
        println!("✓ All lines match perfectly!");
    } else {
        println!(
            "✗ Found {} differences out of {} lines",
            differences, max_lines
        );
    }

    // For debugging, print first few lines of each
    println!("\nFirst 5 expected lines (time,direction only):");
    for (i, line) in expected_lines.iter().take(5).enumerate() {
        println!("  {}: {}", i + 1, line);
    }

    println!("\nFirst 5 output lines (time,direction only):");
    for (i, line) in formatted_output.iter().take(5).enumerate() {
        println!("  {}: {}", i + 1, line);
    }
}

#[test_log::test]
fn simulator_example_use() {
    // The first ten packets of a network trace from the client's perspective
    // when visiting google.com. The format is: "time,direction\n". The
    // direction is either "s" (sent) or "r" (received). The time is in
    // nanoseconds since the start of the trace.
    let raw_trace = "0,s
    19714282,r
    183976147,s
    243699564,r
    1696037773,s
    2047985926,s
    2055955094,r
    9401039609,s
    9401094589,s
    9420892765,r";

    // The network model for simulating the network between the client and the
    // server. Currently just a delay.
    let (topology, mut linkstate) = load_topology_from_file("mbn_test.toml").unwrap();

    //let network = Network::new(Duration::from_millis(10), None);

    // Parse the raw trace into a queue of events for the simulator. This uses
    // the delay to generate a queue of events at the client and server in such
    // a way that the client is ensured to get the packets in the same order and
    // at the same time as in the raw trace.
    let trafserv_to_client_delay = Duration::from_millis(20);
    let (si, mut sq) = parse_trace(raw_trace, &topology, trafserv_to_client_delay);

    // A simple machine that sends one padding packet 20 milliseconds after the
    // first normal packet is sent.
    let m = "02eNp1ibEJAEAIA5Nf7B3N0v1cSESwEL0m5A6YvBqSgP7WeXfM5UoBW7ICYg==";
    let m = Machine::from_str(m).unwrap();

    // Run the simulator with the machine at the client. Run the simulation up
    // until 100 packets have been recorded (total, client and server).
    let trace = sim(
        &[m],
        &[],
        &si,
        &mut sq,
        &topology,
        &mut linkstate,
        100,
        true,
    );


    // print packets from the client's perspective
    for event in trace.iter().filter(|p| p.node_id == 0) {
        println!("{}", event.display_full(&si, &topology, &linkstate));
    }


    //Force error to be able to get debug output
    //assert_eq!(10, 1000);

    // Output:
    // sent a normal packet at 0 ms
    // received a normal packet at 19 ms
    // sent a padding packet at 20 ms
    // sent a normal packet at 183 ms
    // received a normal packet at 243 ms
    // sent a normal packet at 1696 ms
    // sent a normal packet at 2047 ms
    // received a normal packet at 2055 ms
    // sent a normal packet at 9401 ms
    // sent a normal packet at 9401 ms
    // received a normal packet at 9420 ms
}


use std::time::Instant;

//const SIM_EVENT_COUNTS: [usize; 3] = [5_000, 10_000, 20_000];
const SIM_EVENT_COUNTS: [usize; 1] = [10_000, ];
const CONFIG_FILES: [&str; 3] = [
    "/tests/mbn_baseline_test.toml",
    "/tests/mbn_fast_test.toml",
    "/tests/mbn_complex_test.toml"
];
const LINK_TYPES: [(&str, &str, &str); 1] = [
    //("FixedTput", "Link:0::type:FixedTput::tput_bps:100000000", ""),
    ("HiTraceTput", "Link:0::type:HiTraceTput::trace_file:tests/ether100M_synth40M.ltbin.gz", "/tests/ether100M_synth40M.ltbin.gz"),
    //("StdTraceTput", "Link:0::type:StdTraceTput::trace_file:tests/ether100M_synth10K_std.ltbin.gz", "/tests/ether100M_synth10K_std.ltbin.gz"),
];

#[test_log::test]
fn v3_multi_run_like() {
    const EARLY_TRACE: &str =
        include_str!("EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    for sim_event_count in SIM_EVENT_COUNTS.iter() {
        for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES.iter() {
            for config_file in CONFIG_FILES.iter() {
                let config_path = toml_path.clone() + config_file;
                let config_name = config_file.split('_').nth(1).unwrap();
                
                println!("Running simulation with config: {}", config_path);
                // Read TOML file content
                let toml_content = fs::read_to_string(&config_path).unwrap();
                
                // Modify TOML to change Link 0's type
                let modified_toml = modify_toml(&toml_content, toml_edit_string).unwrap();
                
                // Use load_topology_from_str instead of load_topology_from_file
                let (topology, linkstate) = load_topology_from_str(&modified_toml).unwrap();
                let trafserv_to_client_delay= Duration::from_millis(20);
                let (si, sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
                let mut output_len = 0;
                let bench_name = format!("v3_{:?}K_{}_{},", sim_event_count/1000, config_name, link_name);
                //c.bench_function(bench_name.as_str(), |b| {
                    //b.iter(|| {

                let start = Instant::now();     
                // Can use 1000 when running test with --release
                //for _ in 0..1000 {
                for _ in 0..5 {
                        let mut args = SimulatorArgs::new(*sim_event_count, true);
                        args.only_client_events = true;
                        args.continue_after_all_normal_packets_processed = false;
                        let trace = simul_advanced(&[], &[], &topology, &mut linkstate.clone(), &si, &mut sq.clone(), &args);
                        //let trace = simul_advanced(&[ratio3_machine()], &[], &topology, &mut linkstate.clone(), &si, &mut sq.clone(), &args);

                        output_len = trace.len();
                        print!("x");
                }
                    //});
                //});
                let duration = start.elapsed(); 
                println!("\n{}   Loop took {:.3} seconds", bench_name, duration.as_secs_f64());
                print!("Length of output trace: {}\n", output_len );
            }
        }
    }
}



use maybenot::{
    action::Action,
    constants::MAX_SAMPLED_BLOCK_DURATION,
    dist::{Dist, DistType},
    event::Event,
    state::{State, Trans},    
};
use enum_map::enum_map;



fn _ratio3_machine() -> Machine {
    let n = 3;
    let mut states = vec![];

    // start state 0
    let start_state = State::new(enum_map! {
       Event::TunnelSent | Event::TunnelRecv => vec![Trans(1, 1.0)],
       _ => vec![],
    });
    states.push(start_state);

    // blocking state 1
    let mut blocking_state = State::new(enum_map! {
        Event::BlockingBegin => vec![Trans(2, 1.0)],
        _ => vec![],
    });
    blocking_state.action = Some(Action::BlockOutgoing {
        bypass: true,
        replace: true,
        timeout: Dist {
            dist: DistType::Uniform {
                low: 0.0,
                high: 0.0,
            },
            start: 0.0,
            max: 0.0,
        },
        duration: Dist {
            dist: DistType::Uniform {
                low: 0.0,
                high: 0.0,
            },
            start: MAX_SAMPLED_BLOCK_DURATION,
            max: 0.0,
        },
        limit: None,
    });
    states.push(blocking_state);

    // recv states 2..n+2
    for i in 0..n {
        states.push(State::new(enum_map! {
           // to the next state
           Event::TunnelRecv => vec![Trans(3+i, 1.0)],
           // something else let traffic through, back to counting
           //Event::TunnelSent => vec![Trans(2, 1.0)],
           _ => vec![],
        }));
    }

    // padding state n+2
    let mut padding_state = State::new(enum_map! {
        Event::PaddingSent => vec![Trans(2, 1.0)],
        _ => vec![],
    });
    padding_state.action = Some(Action::SendPadding {
        bypass: true,
        replace: true,
        timeout: Dist {
            dist: DistType::Uniform {
                low: 0.0,
                high: 0.0,
            },
            start: 0.0,
            max: 0.0,
        },
        limit: None,
    });
    states.push(padding_state);

    Machine::new(u64::MAX, 0.0, u64::MAX, 0.0, states).unwrap()
}



