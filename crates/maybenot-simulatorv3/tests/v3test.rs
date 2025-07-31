use maybenot::{Machine, TriggerEvent};
use maybenot_simulatorv3::{network::Network, parse_trace, sim};
use std::{str::FromStr, time::Duration};


#[test_log::test]
fn full_trace_compare() {
    // Load the EARLY_TEST_TRACE file
    const EARLY_TRACE: &str = include_str!("EARLY_TEST_TRACE.log");
    
    // Use the same network configuration as the bench
    //let (topology, mut linkstate) = Network::from_toml_file("basic_test.toml").unwrap();
    let (topology, mut linkstate) = Network::from_toml_file("mbn_test.toml").unwrap();
    
    // Parse the trace with the same parameters as the bench
    let trafserv_to_client_delay = Duration::from_millis(20);
    let mut input_trace = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
    
    // 30097 gives 10000 client events, with basic toml to be used in benching to get comparable times
    //let output_trace = sim(&[], &[], &mut input_trace, &mut sim_network, 30097, true);
    // 56829 gives 10000 client events, with basic toml to be used in benching to get comparable times
    //let output_trace = sim(&[], &[], &mut input_trace, &mut sim_network, 56829, true);
    
    let output_trace = sim(&[], &[], &mut input_trace, &topology, &mut linkstate, 56829, true);
    // print length of output trace
    println!("Output trace length: {}", output_trace.len());
    
    // Print the first 5 events in output trace for debugging
    println!("First 5 events in output trace:");
    for event in output_trace.iter().take(5) {
        println!("{}", event);
    }

    // Convert output trace to EARLY_TEST_TRACE format (time,direction) - ignoring size
    let starting_time = input_trace.zero_instant;
    let mut formatted_output = Vec::new();
    
    for event in output_trace.iter().filter(|e| e.node_idx == 0) { // Client perspective only
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
    let expected_lines: Vec<String> = EARLY_TRACE.trim().lines()
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
    println!("Comparing {} expected lines with {} output lines", expected_lines.len(), formatted_output.len());
    
    let max_lines = std::cmp::min(expected_lines.len(), formatted_output.len());
    let mut differences = 0;
    
    for i in 0..max_lines {
        let expected = &expected_lines[i];
        let actual = &formatted_output[i];
        
        if expected != actual {
            differences += 1;
            if differences <= 10 { // Only show first 10 differences
                println!("Line {}: Expected '{}', Got '{}'", i + 1, expected, actual);
            }
        }
    }
    
    if expected_lines.len() != formatted_output.len() {
        println!("Length mismatch: Expected {} lines, got {}", expected_lines.len(), formatted_output.len());
    }
    
    if differences == 0 && expected_lines.len() == formatted_output.len() {
        println!("✓ All lines match perfectly!");
    } else {
        println!("✗ Found {} differences out of {} lines", differences, max_lines);
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
    49714282,r
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
    let (topology, mut linkstate) = Network::from_toml_file("basic_test.toml").unwrap();

    //let network = Network::new(Duration::from_millis(10), None);

    // Parse the raw trace into a queue of events for the simulator. This uses
    // the delay to generate a queue of events at the client and server in such
    // a way that the client is ensured to get the packets in the same order and
    // at the same time as in the raw trace.
    let trafserv_to_client_delay= Duration::from_millis(20);
    let mut input_trace = parse_trace(raw_trace, &topology, trafserv_to_client_delay);

    // A simple machine that sends one padding packet 20 milliseconds after the
    // first normal packet is sent.
    let m = "02eNp1ibEJAEAIA5Nf7B3N0v1cSESwEL0m5A6YvBqSgP7WeXfM5UoBW7ICYg==";
    let m = Machine::from_str(m).unwrap();

    // Run the simulator with the machine at the client. Run the simulation up
    // until 100 packets have been recorded (total, client and server).
    let trace = sim(&[m], &[], &mut input_trace, &topology, &mut linkstate, 100, true);

    // print packets from the client's perspective
    println!("{:#?}",&trace.clone());

    let starting_time = trace[0].time;
    trace
        .into_iter()
        .filter(|p| p.node_idx==0)
        .for_each(|p| match p.event {
            TriggerEvent::NormalSent => {
                if p.contains_padding {
                    println!(
                        "sent a padding packet at {} ms",
                        (p.time - starting_time).as_millis()
                    );
                } else {
                    println!(
                        "sent a normal packet at {} ms",
                        (p.time - starting_time).as_millis()
                    );
                }
            }
            TriggerEvent::NormalRecv => {
                if p.contains_padding {
                    println!(
                        "received a padding packet at {} ms",
                        (p.time - starting_time).as_millis()
                    );
                } else {
                    println!(
                        "received a normal packet at {} ms",
                        (p.time - starting_time).as_millis()
                    );
                }
            }
            _ => {}
        });
    //Force error
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
