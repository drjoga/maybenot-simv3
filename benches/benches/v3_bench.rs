use std::fs;
use std::time::Duration;

use maybenot_simulatorv3::{
    SimulatorArgs, build_network_topology_from_config, load_topology_from_file,
    load_topology_from_str, modify_toml, parse_trace, sim_advanced,
};

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use rayon::prelude::*;

//const SIM_EVENT_COUNTS: [usize; 3] = [5_000, 10_000, 20_000];
const SIM_EVENT_COUNTS: [usize; 1] = [10_000];

const CONFIG_FILES: [&str; 3] = [
    "/benches/topologies/maybenot_baseline_bench.toml",
    "/benches/topologies/maybenot_fast_bench.toml",
    "/benches/topologies/maybenot_complex_bench.toml",
];
const LINK_TYPES: [(&str, &str, &str); 3] = [
    (
        "FixedTput",
        "Link:0::type:FixedTput::tput_bps:100000000",
        "",
    ),
    (
        "HiTraceTput",
        "Link:0::type:HiTraceTput::trace_file:../crates/maybenot-simulatorv3/tests/ether100M_synth40M.ltbin.gz",
        "../crates/maybenot-simulatorv3/tests/ether100M_synth40M.ltbin.gz",
    ),
    (
        "StdTraceTput",
        "Link:0::type:StdTraceTput::trace_file:../crates/maybenot-simulatorv3/tests/ether100M_synth10K_std.ltbin.gz",
        "../crates/maybenot-simulatorv3/tests/ether100M_synth10K_std.ltbin.gz",
    ),
];

fn v3_single_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    let sim_event_count = &SIM_EVENT_COUNTS[0];
    let config_file = CONFIG_FILES[0];
    let config_path = toml_path.clone() + config_file;
    let config_name = config_file.split('_').nth(1).unwrap();
    let (topology, linkstate) = load_topology_from_file(config_path.clone()).unwrap();
    let trafserv_to_client_delay = Duration::from_millis(20);
    let (si, sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay).unwrap();
    let mut output_len = 0;
    let bench_name = format!("v3_{:?}K_{}_single,", sim_event_count / 1000, config_name);
    c.bench_function(bench_name.as_str(), |b| {
        b.iter(|| {
            let mut args = SimulatorArgs::new(*sim_event_count, true);
            args.only_client_events = true;
            args.continue_after_all_normal_packets_processed = false;
            let trace = sim_advanced(
                &[],
                &[],
                &topology,
                &mut linkstate.clone(),
                &si,
                &mut sq.clone(),
                &args,
            );
            output_len = trace.len();
        });
    });
    println!("Length of output trace: {}", output_len);
}

fn v3_multi_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    for sim_event_count in SIM_EVENT_COUNTS.iter() {
        for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES.iter() {
            for config_file in CONFIG_FILES.iter() {
                let config_path = toml_path.clone() + config_file;
                let config_name = config_file.split('_').nth(1).unwrap();

                // Read TOML file content
                let toml_content = fs::read_to_string(&config_path).unwrap();

                // Modify TOML to change Link 0's type
                let modified_toml = modify_toml(&toml_content, toml_edit_string).unwrap();

                // Use load_topology_from_str instead of load_topology_from_file
                let (topology, linkstate) = load_topology_from_str(&modified_toml).unwrap();
                let trafserv_to_client_delay = Duration::from_millis(20);
                let (si, sq) =
                    parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay).unwrap();
                let mut output_len = 0;
                let bench_name = format!(
                    "v3_{:?}K_{}_{},",
                    sim_event_count / 1000,
                    config_name,
                    link_name
                );
                c.bench_function(bench_name.as_str(), |b| {
                    b.iter(|| {
                        let mut args = SimulatorArgs::new(*sim_event_count, true);
                        args.only_client_events = true;
                        args.continue_after_all_normal_packets_processed = false;
                        let trace = sim_advanced(
                            &[],
                            &[],
                            &topology,
                            &mut linkstate.clone(),
                            &si,
                            &mut sq.clone(),
                            &args,
                        );
                        output_len = trace.len();
                    });
                });
                println!("Length of output trace: {}", output_len);
            }
        }
    }
}

// NOTE: Ratio3 does not run with tracefile linktypes due to Ratio3 leading to 1 day long blockings
// causing timeslot overflows trace driven links ....
fn v3_multi_ratio3(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    for sim_event_count in SIM_EVENT_COUNTS.iter() {
        //for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES.iter() {
        for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES[0..1].iter() {
            for config_file in CONFIG_FILES.iter() {
                let config_path = toml_path.clone() + config_file;
                let config_name = config_file.split('_').nth(1).unwrap();

                // Read TOML file content
                let toml_content = fs::read_to_string(&config_path).unwrap();

                // Modify TOML to change Link 0's type
                let modified_toml = modify_toml(&toml_content, toml_edit_string).unwrap();

                // Use load_topology_from_str instead of load_topology_from_file
                let (topology, linkstate) = load_topology_from_str(&modified_toml).unwrap();

                let trafserv_to_client_delay = Duration::from_millis(20);
                let (si, sq) =
                    parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay).unwrap();
                let mut out_trace = Vec::new();
                let mut output_len = 0;
                let bench_name = format!(
                    "v3_{:?}K_{}_{}_ClientRatio3,",
                    sim_event_count / 1000,
                    config_name,
                    link_name
                );
                c.bench_function(bench_name.as_str(), |b| {
                    b.iter(|| {
                        let mut args = SimulatorArgs::new(*sim_event_count, true);
                        args.only_client_events = true;
                        args.continue_after_all_normal_packets_processed = false;
                        out_trace = sim_advanced(
                            &[ratio3_machine()],
                            &[],
                            &topology,
                            &mut linkstate.clone(),
                            &si,
                            &mut sq.clone(),
                            &args,
                        );
                        output_len = out_trace.len();
                    });
                });
                println!("Length of output trace: {}\n", output_len);
                println!("First 5 events in output trace:");
                for event in out_trace.iter().take(5) {
                    println!("{}", event.display_full(&si, &topology, &linkstate));
                }
                println!("Last 5 events in output trace:");
                for event in out_trace[out_trace.len().saturating_sub(5)..].iter() {
                    println!("{}", event.display_full(&si, &topology, &linkstate));
                }
            }
        }
    }
}

fn v3_multi_run_parallel(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    for sim_event_count in SIM_EVENT_COUNTS.iter() {
        for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES[0..1].iter() {
            for config_file in CONFIG_FILES.iter() {
                let config_path = toml_path.clone() + config_file;
                let config_name = config_file.split('_').nth(1).unwrap();

                // Read TOML file content
                let toml_content = fs::read_to_string(&config_path).unwrap();

                // Modify TOML to change Link 0's type
                let modified_toml = modify_toml(&toml_content, toml_edit_string).unwrap();

                // Use load_topology_from_str instead of load_topology_from_file
                let (topology, linkstate) = load_topology_from_str(&modified_toml).unwrap();
                let network_config = topology.network_config.clone();

                let trafserv_to_client_delay = Duration::from_millis(20);
                let (si, sq) =
                    parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay).unwrap();
                let mut args = SimulatorArgs::new(*sim_event_count, true);
                args.only_client_events = true;
                args.continue_after_all_normal_packets_processed = false;

                let bench_name = format!(
                    "v3_{:?}K_{}_{}_100para,",
                    sim_event_count / 1000,
                    config_name,
                    link_name
                );
                c.bench_function(bench_name.as_str(), |b| {
                    b.iter(|| {
                        (0..100).into_par_iter().for_each(|_| {
                            let thread_topology =
                                build_network_topology_from_config(&network_config).unwrap();
                            black_box(sim_advanced(
                                &[],
                                &[],
                                &thread_topology,
                                &mut linkstate.clone(),
                                &si,
                                &mut sq.clone(),
                                &args.clone(),
                            ));
                        });
                    });
                });
            }
        }
    }
}

// NOTE: Ratio3 does not run with tracefile linktypes due to Ratio3 leading to 1 day long blockings
// causing timeslot overflows trace driven links ....
fn v3_multi_run_parallel_ratio3(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    for sim_event_count in SIM_EVENT_COUNTS.iter() {
        //for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES.iter() {
        for (link_name, toml_edit_string, _pattern_file_path) in LINK_TYPES[0..1].iter() {
            for config_file in CONFIG_FILES.iter() {
                let config_path = toml_path.clone() + config_file;
                let config_name = config_file.split('_').nth(1).unwrap();

                // Read TOML file content
                let toml_content = fs::read_to_string(&config_path).unwrap();

                // Modify TOML to change Link 0's type
                let modified_toml = modify_toml(&toml_content, toml_edit_string).unwrap();

                // Use load_topology_from_str instead of load_topology_from_file
                let (topology, linkstate) = load_topology_from_str(&modified_toml).unwrap();
                let network_config = topology.network_config.clone();

                let trafserv_to_client_delay = Duration::from_millis(20);
                let (si, sq) =
                    parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay).unwrap();
                let mut args = SimulatorArgs::new(*sim_event_count, true);
                args.only_client_events = true;
                args.continue_after_all_normal_packets_processed = false;

                let bench_name = format!(
                    "v3_{:?}K_{}_{}_100paraRatio3,",
                    sim_event_count / 1000,
                    config_name,
                    link_name
                );
                c.bench_function(bench_name.as_str(), |b| {
                    b.iter(|| {
                        (0..100).into_par_iter().for_each(|_| {
                            let thread_topology =
                                build_network_topology_from_config(&network_config).unwrap();
                            black_box(sim_advanced(
                                &[],
                                &[],
                                &thread_topology,
                                &mut linkstate.clone(),
                                &si,
                                &mut sq.clone(),
                                &args.clone(),
                            ));
                        });
                    });
                });
            }
        }
    }
}

use enum_map::enum_map;
use maybenot::{
    Machine,
    action::Action,
    constants::MAX_SAMPLED_BLOCK_DURATION,
    dist::{Dist, DistType},
    event::Event,
    state::{State, Trans},
};

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
    all_sim_benches,
    v3_single_simulator_run,
    v3_multi_run,
    v3_multi_ratio3,
    v3_multi_run_parallel,
    v3_multi_run_parallel_ratio3,
);

criterion_group!(
    overview_sim_benches,
    v3_multi_run,
    v3_multi_ratio3,
    v3_multi_run_parallel,
    v3_multi_run_parallel_ratio3,
);

criterion_group!(
    parallell_testing,
    v3_multi_run_parallel,
    v3_multi_run_parallel_ratio3,
);

//criterion_main!(all_sim_benches);
criterion_main!(overview_sim_benches);
//criterion_main!(parallell_testing);
