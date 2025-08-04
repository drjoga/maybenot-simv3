use std::time::Duration;


use maybenot_simulatorv3::{network::Network, parse_trace,simul_advanced, SimulatorArgs};


use criterion::{criterion_group, criterion_main, Criterion};




fn v3_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        //include_str!("../../.../tests/EARLY_TEST_TRACE.log");
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    //let config_path = "../crates/maybenot-simulatorv3/basic_test.toml";
    let config_path = "../crates/maybenot-simulatorv3/mbn_test.toml";
    let (topology, linkstate) = Network::from_toml_file(config_path).unwrap();

    println!("Config path: {}", config_path);
    let trafserv_to_client_delay= Duration::from_millis(20);
    let input_trace = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
    println!("Input trace length: {}", input_trace.len());
    let mut output_len = 0;
    c.bench_function("v3 network simulation run", |b| {
        b.iter(|| {
            let mut linkstate2 = linkstate.clone(); 
            let mut input_trace2 = input_trace.clone();
            let nr_sim_events = 56829;   // 30097 with basic tom, 56829 with mbn toml, gives 10000 client events to be comparable

            let mut args = SimulatorArgs::new(0, true);
            args.max_sim_iterations = nr_sim_events;
            args.only_client_events = true;
            args.continue_after_all_normal_packets_processed = false;
            let trace = simul_advanced(&[], &[], &topology, &mut linkstate2, &mut input_trace2, &args);

            output_len = trace.len();
        });
    });
    print!("Length of output trace: {}\n", output_len );
}



criterion_group!(
    benches,
    v3_simulator_run,
);

criterion_main!(benches);
