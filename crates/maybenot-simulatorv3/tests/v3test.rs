use enum_map::enum_map;
use maybenot::{
    action::Action,
    dist::{Dist, DistType},
    event::Event,
    state::{State, Trans},
    Machine, TriggerEvent,
};
use maybenot_simulatorv3::{network::Network, parse_trace, sim};
use std::{str::FromStr, time::Duration};


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
    let network = Network::from_toml_file("basic_test.toml").unwrap();

    //let network = Network::new(Duration::from_millis(10), None);

    // Parse the raw trace into a queue of events for the simulator. This uses
    // the delay to generate a queue of events at the client and server in such
    // a way that the client is ensured to get the packets in the same order and
    // at the same time as in the raw trace.
    let trafserv_to_client_delay= Duration::from_millis(20);
    let mut input_trace = parse_trace(raw_trace, network.clone(), trafserv_to_client_delay);

    // A simple machine that sends one padding packet 20 milliseconds after the
    // first normal packet is sent.
    let m = "02eNp1ibEJAEAIA5Nf7B3N0v1cSESwEL0m5A6YvBqSgP7WeXfM5UoBW7ICYg==";
    let m = Machine::from_str(m).unwrap();

    // Run the simulator with the machine at the client. Run the simulation up
    // until 100 packets have been recorded (total, client and server).
    let trace = sim(&[m], &[], &mut input_trace, network, 100, true);

    // print packets from the client's perspective
    let starting_time = trace[0].time;
    trace
        .into_iter()
        .filter(|p| p.node_idx==0)
        .for_each(|p| match p.event {
            TriggerEvent::TunnelSent => {
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
            TriggerEvent::TunnelRecv => {
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
