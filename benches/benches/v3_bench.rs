use std::time::Duration;


use maybenot_simulatorv3::{network::Network, parse_trace,simul_advanced, SimulatorArgs};


use criterion::{criterion_group, black_box, criterion_main, Criterion};

use rayon::prelude::*;

//const SIM_EVENT_COUNTS: [usize; 3] = [5_000, 10_000, 20_000];
const SIM_EVENT_COUNTS: [usize; 1] = [10_000, ];


fn v3_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        //include_str!("../../.../tests/EARLY_TEST_TRACE.log");
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    //let config_path = "../crates/maybenot-simulatorv3/basic_test.toml";
    //let config_path = "../crates/maybenot-simulatorv3/mbn_test.toml";
    let config_path = "../crates/maybenot-simulatorv3/mbnfast.toml";
    //let config_path = "../crates/maybenot-simulatorv3/mbn_complex.toml";
    let (topology, linkstate) = Network::from_toml_file(config_path).unwrap();

    println!("Config path: {}", config_path);
    let trafserv_to_client_delay= Duration::from_millis(20);
    let (si, mut sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
    let mut output_len = 0;
    c.bench_function("v3_10K_baseline", |b| {
        b.iter(|| {
            let mut linkstate2 = linkstate.clone(); 
            let mut sq2 = sq.clone();

            let mut args = SimulatorArgs::new(10000, true);
            args.only_client_events = true;
            args.continue_after_all_normal_packets_processed = false;
            let trace = simul_advanced(&[], &[], &topology, &mut linkstate2, &si, &mut sq2, &args);

            output_len = trace.len();
        });
    });
    print!("Length of output trace: {}\n", output_len );
}



fn v3_multi_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    let config_files = [
        "/benches/mbn_baseline_bench.toml",
        "/benches/mbn_fast_bench.toml",
        "/benches/mbn_complex_bench.toml"
    ];

    for sim_event_count in SIM_EVENT_COUNTS.iter() {

        for config_file in config_files.iter() {
            let config_path = toml_path.clone() + config_file;
            let config_name = config_file.split('_').nth(1).unwrap();
            let (topology, linkstate) = Network::from_toml_file(config_path.clone()).unwrap();
            let trafserv_to_client_delay= Duration::from_millis(20);
            let (si, mut sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
            let mut output_len = 0;
            let bench_name = format!("v3_{:?}K_{},", sim_event_count/1000,config_name);
            c.bench_function(bench_name.as_str(), |b| {
                b.iter(|| {
                    let mut args = SimulatorArgs::new(*sim_event_count, true);
                    args.only_client_events = true;
                    args.continue_after_all_normal_packets_processed = false;
                    let trace = simul_advanced(&[], &[], &topology, &mut linkstate.clone(), &si, &mut sq.clone(), &args);

                    output_len = trace.len();
                });
            });
            print!("Length of output trace: {}\n", output_len );
        }
    }
}




fn v3_components(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();   
    let config_file = "/benches/mbn_complex_bench.toml";

    let config_path = toml_path.clone() + config_file;

    c.bench_function("SimulatorArgs.new", |b| {
        b.iter(|| {
            black_box( SimulatorArgs::new(10_000, true));
        });
    });


    c.bench_function("TopologyRead", |b| {
        b.iter(|| {
            let _ = black_box(Network::from_toml_file(config_path.clone()));
        });
    });

    let (topology, _linkstate) = Network::from_toml_file(config_path).unwrap();
    let trafserv_to_client_delay= Duration::from_millis(20);

    c.bench_function("parse_trace", |b| {
        b.iter(|| {
            black_box(parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay));
        });
    });



}




fn v3_multi_ratio3(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();
    
    let config_files = [
        "/benches/mbn_baseline_bench.toml",
        "/benches/mbn_fast_bench.toml",
        "/benches/mbn_complex_bench.toml"
    ];

    for sim_event_count in SIM_EVENT_COUNTS.iter() {

        for config_file in config_files.iter() {
            let config_path = toml_path.clone() + config_file;
            let config_name = config_file.split('_').nth(1).unwrap();

            let (topology, linkstate) = Network::from_toml_file(config_path).unwrap();

            let trafserv_to_client_delay= Duration::from_millis(20);
            let (si, mut sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
            let mut output_len = 0;
            let bench_name = format!("v3_{:?}K_{}_ClientRatio3,", sim_event_count/1000,config_name);
            c.bench_function(bench_name.as_str(), |b| {
                b.iter(|| {
                    let mut args = SimulatorArgs::new(*sim_event_count, true);
                    args.only_client_events = true;
                    args.continue_after_all_normal_packets_processed = false;
                    let trace = simul_advanced(&[ratio3_machine()], &[], &topology, &mut linkstate.clone(), &si, &mut sq.clone(), &args);
                    output_len = trace.len();
                });
            });
            print!("Length of output trace: {}\n", output_len );
        }
    }
}


 

fn v3_multi_run_parallel(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();
    
    let config_files = [
        "/benches/mbn_baseline_bench.toml",
        "/benches/mbn_fast_bench.toml",
        "/benches/mbn_complex_bench.toml"
    ];

    for sim_event_count in SIM_EVENT_COUNTS.iter() {

        for config_file in config_files.iter() {
            let config_path = toml_path.clone() + config_file;
            let config_name = config_file.split('_').nth(1).unwrap();

            let (topology, _linkstate) = Network::from_toml_file(config_path.clone()).unwrap();

            let trafserv_to_client_delay= Duration::from_millis(20);
            let (si, sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
            let mut args = SimulatorArgs::new(*sim_event_count, true);
            args.only_client_events = true;
            args.continue_after_all_normal_packets_processed = false;

            let bench_name = format!("v3_{:?}K_{}_100para,", sim_event_count/1000,config_name);
            c.bench_function(bench_name.as_str(), |b| {
                b.iter(|| {
                    (0..100).into_par_iter().for_each(|_| {
                        let (topology, mut linkstate) = Network::from_toml_file(config_path.clone()).unwrap();
                        black_box(simul_advanced(&[], &[], &topology, &mut linkstate, &si, &mut sq.clone(), &args.clone()));
                    });
                });
            });
        }   
    }
}



use maybenot::{
    action::Action,
    constants::MAX_SAMPLED_BLOCK_DURATION,
    dist::{Dist, DistType},
    event::Event,
    state::{State, Trans},
    Machine,
};
use enum_map::enum_map;



fn ratio3_machine() -> Machine {
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








criterion_group!(
    benches,
    v3_multi_run,
    v3_multi_ratio3,
    v3_multi_run_parallel,
);

criterion_group!(
    benches2,
    v3_simulator_run,
    v3_multi_run,
    v3_multi_ratio3,
    v3_multi_run_parallel,
    v3_components,
);



criterion_main!(benches);
