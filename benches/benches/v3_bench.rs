use std::time::Duration;


use maybenot_simulatorv3::{network::Network, parse_trace, sim};


use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ndarray::Array2;
use rand::Rng;
use rayon::prelude::*;




fn v3_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        //include_str!("../../.../tests/EARLY_TEST_TRACE.log");
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    //let config_path = concat!(env!("CARGO_MANIFEST_DIR"), "/basic_test.toml");

    let config_path = "../crates/maybenot-simulatorv3/basic_test.toml";

    //let network = Network::from_toml_file("../../basic_test.toml").unwrap();
    let network = Network::from_toml_file(config_path).unwrap();

    println!("Config path: {}", config_path);
    let trafserv_to_client_delay= Duration::from_millis(20);
    let mut input_trace = parse_trace(EARLY_TRACE, network.clone(), trafserv_to_client_delay);
    let mut current_time = input_trace.get_first_event_time().unwrap();
    println!("Input trace length: {}", input_trace.len());
    let mut t2_network = network.clone();
    c.bench_function("v3 network simulation run", |b| {
        b.iter(|| {
            //black_box(t2_network = network2.clone()); 
            let mut network2 = Network::from_toml_file(config_path).unwrap();
            let mut input_trace2 = input_trace.clone();
            black_box(sim(&[], &[], &mut input_trace2, &mut network2, 10000, true));
        });
    });
}



/*
    c.bench_function("v3 network simulation run", |b| {
        b.iter(|| {
            let t_network = network.clone(); 
            black_box(sim(&[], &[], &mut input_trace, t_network, 40000, true));
        });
    });

*/



criterion_group!(
    benches,
    v3_simulator_run,
);

criterion_main!(benches);
