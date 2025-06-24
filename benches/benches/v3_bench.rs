use std::time::Duration;

use maybenot_simulatorv3::{
    linktrace::{load_linktrace_from_file, mk_start_instant},
    links::{ExtendedNetworkLabels, NetworkBottleneck, NetworkLinktrace},
    parse_trace, sim, sim_advanced, SimulatorArgs,
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ndarray::Array2;
use rand::Rng;
use rayon::prelude::*;

// Evaluate lookup performace for different sizes of traces. Larger traces will be slower
// because of less cache locality. Note that the random lookup here is conservative as the
// actual use would be sequentially increasing within a limited range.
pub fn benchmark_busy_to(c: &mut Criterion) {
    // List of linktrace files to benchmark
    let linktrace_files = vec![
        "../crates/maybenot-simulator/tests/ether100M_synth5K.ltbin.gz",
        "../crates/maybenot-simulator/tests/ether100M_synth5M.ltbin.gz",
        "../crates/maybenot-simulator/tests/ether100M_synth40M.ltbin.gz",
    ];

    let nr_samples = 10_000;
    let mut rng = rand::thread_rng();

    for file in linktrace_files {
        // Load the LinkTrace instance from the file
        let linksim_trace = load_linktrace_from_file(file).expect(&format!(
            "Failed to load LinkTrace ltbin from file: {}",
            file
        ));

        // Generate random time slot values between 0 and trace length
        let nr_time_slots = linksim_trace.get_nr_timeslots() as usize;
        let time_slots: Vec<usize> = (0..nr_samples)
            .map(|_| rng.gen_range(0..nr_time_slots))
            .collect();

        // Generate random packet size values between 40 and 1500
        let pkt_sizes: Vec<i32> = (0..nr_samples).map(|_| rng.gen_range(40..=1500)).collect();

        // Benchmark the get_dl_busy_to function for the current linktrace file
        c.bench_function(&format!("get_dl_busy_to_  {}", file), |b| {
            b.iter(|| {
                for i in 0..nr_samples {
                    // Use black_box to prevent the compiler from optimizing away the call
                    black_box(linksim_trace.get_dl_busy_to(time_slots[i], pkt_sizes[i]));
                }
            })
        });
    }
}

fn simulator_network_sample(c: &mut Criterion) {
    //How many packets to process
    let nr_iter = 10_000;

    // Initialize the vector with the first Instant
    let mut instants = Vec::with_capacity(nr_iter);
    let mut rng = rand::thread_rng();

    // Start with the defined  Instant
    let mut current_instant = mk_start_instant();
    instants.push(current_instant + Duration::from_micros(1));

    // Total duration target
    let target_duration = 4_000_000;
    let mut accumulated_duration = 0;

    // Generate more Instants
    for _ in 1..nr_iter {
        // Calculate remaining microseconds and divide by remaining instants to get average step size
        let remaining_steps = nr_iter - instants.len();
        let remaining_duration = target_duration - accumulated_duration;
        let average_step = remaining_duration / remaining_steps;

        // Generate a random step, allowing some variation around the average step
        let step_micros: u64 = rng.gen_range(average_step / 2..=average_step * 2) as u64;

        // Update the accumulated duration
        accumulated_duration += step_micros as usize;

        // Add the random step to the current Instant
        current_instant += Duration::from_micros(step_micros);

        // Push the new Instant into the vector
        instants.push(current_instant);
    }

    // Initalize network, start with a reasonable 10ms delay
    let linktrace =
        load_linktrace_from_file("../crates/maybenot-simulator/tests/ether100M_synth5M.ltbin.gz")
            .expect("Failed to load LinkTrace ltbin from file");
    let mut network_lt = NetworkLinktrace::new_linktrace(linktrace);

    c.bench_function("Linktrace HiRes network.sample", |b| {
        b.iter(|| {
            for i in 0..nr_iter {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_lt.sample(&instants[i], true));
                network_lt.reset_linktrace();
            }
        })
    });

    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulator/tests/ether100M_synth10K_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");
    let mut network_lt = NetworkLinktrace::new_linktrace(linktrace);

    c.bench_function("Linktrace StdRes eth 100Mbps network.sample", |b| {
        b.iter(|| {
            for i in 0..nr_iter {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_lt.sample(&instants[i], true));
                network_lt.reset_linktrace();
            }
        })
    });

    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulator/tests/test100K_synth2M_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");
    let mut network_lt = NetworkLinktrace::new_linktrace(linktrace);

    c.bench_function("Linktrace StdRes slow 100Kbps network.sample", |b| {
        b.iter(|| {
            for i in 0..nr_iter {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_lt.sample(&instants[i], true));
                network_lt.reset_linktrace();
            }
        })
    });

    let mut network_bneck =
        NetworkBottleneck::new(Duration::from_millis(1000), Some(1000));

    c.bench_function("Bottleneck network.sample", |b| {
        b.iter(|| {
            for i in 0..nr_iter {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_bneck.sample(&instants[i], true));
                // TODO: Find out why memory consumption goes haywaire without the line below...
                network_lt.reset_linktrace();
            }
        })
    });

    let mut network_ftput = NetworkLinktrace::new_fixed(10_000_000, 100_000_000);

    c.bench_function("FixedTput network.sample", |b| {
        b.iter(|| {
            for i in 0..nr_iter {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_ftput.sample(&instants[i], true));
                // TODO: Find out why memory consumption goes haywaire without the line below...
                network_lt.reset_linktrace();
            }
        })
    });

    let mut network_bneck = NetworkBottleneck::new(Duration::from_millis(1000), Some(100));

    c.bench_function("Bottleneck network.sample queue_pps 100", |b| {
        b.iter(|| {
            for i in 0..nr_iter {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_bneck.sample(&instants[i], true));
                network_lt.reset_linktrace();
            }
        })
    });

    //sim(&[], &[], &mut pq.clone(), network.delay, 1000, true);
}

fn simple_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    c.bench_function("Simple network simulation run", |b| {
        b.iter(|| {
            let network = Network::new(Duration::from_millis(10), None);
            let pq = parse_trace(EARLY_TRACE, network);
            black_box(sim(&[], &[], &mut pq.clone(), network.delay, 10000, true));
        });
    });
}

fn bottleneck_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    c.bench_function("Bottleneck network simulation run", |b| {
        b.iter(|| {
            let network = Network::new(Duration::from_millis(10), None);
            let sq = parse_trace(EARLY_TRACE, network);
            let args = SimulatorArgs::new(network, 10000, true);
            black_box(sim_advanced(&[], &[], &mut sq.clone(), &args));
        });
    });
}

fn fixedtput_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    c.bench_function("FixedTput network simulation run", |b| {
        b.iter(|| {
            let network = Network::new(Duration::from_millis(10), None);
            let sq = parse_trace(EARLY_TRACE, network);
            let args = SimulatorArgs::new(network, 10000, true);
            let fixedtput_args = SimulatorArgs {
                simulated_network_type: Some(ExtendedNetworkLabels::FixedTput),
                client_tput: Some(10_000_000),
                server_tput: Some(100_000_000),
                ..args
            };
            black_box(sim_advanced(&[], &[], &mut sq.clone(), &fixedtput_args));
        });
    });
}

fn linktrace_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    let linktrace =
        load_linktrace_from_file("../crates/maybenot-simulator/tests/ether100M_synth40M.ltbin.gz")
            .expect("Failed to load LinkTrace ltbin from file");

    c.bench_function("Linktrace network simulation run", |b| {
        b.iter(|| {
            let network = Network::new(Duration::from_millis(10), None);
            let sq = parse_trace(EARLY_TRACE, network);
            let args = SimulatorArgs::new(network, 10000, true);
            let linktrace_args = SimulatorArgs {
                simulated_network_type: Some(ExtendedNetworkLabels::Linktrace),
                linktrace: Some(linktrace.clone()),
                ..args
            };

            black_box(sim_advanced(&[], &[], &mut sq.clone(), &linktrace_args));
        });
    });
}

fn linktrace_std100m_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulator/tests/ether100M_synth10K_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");

    c.bench_function("Linktrace StdRes eth 100Mbps network simulation run", |b| {
        b.iter(|| {
            let network = Network::new(Duration::from_millis(10), None);
            let sq = parse_trace(EARLY_TRACE, network);
            let args = SimulatorArgs::new(network, 10000, true);
            let linktrace_args = SimulatorArgs {
                simulated_network_type: Some(ExtendedNetworkLabels::Linktrace),
                linktrace: Some(linktrace.clone()),
                ..args
            };

            black_box(sim_advanced(&[], &[], &mut sq.clone(), &linktrace_args));
        });
    });
}

fn linktrace_std100k_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulator/tests/test100K_synth2M_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");

    c.bench_function("Linktrace StdRes slow 100K network simulation run", |b| {
        b.iter(|| {
            let network = Network::new(Duration::from_millis(10), None);
            let sq = parse_trace(EARLY_TRACE, network);
            let args = SimulatorArgs::new(network, 10000, true);
            let linktrace_args = SimulatorArgs {
                simulated_network_type: Some(ExtendedNetworkLabels::Linktrace),
                linktrace: Some(linktrace.clone()),
                ..args
            };

            black_box(sim_advanced(&[], &[], &mut sq.clone(), &linktrace_args));
        });
    });
}

fn bottleneck_parallel_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    c.bench_function("Bottleneck parallel simulation run", |b| {
        b.iter(|| {
            (0..100).into_par_iter().for_each(|_| {
                let network = Network::new(Duration::from_millis(10), None);
                let sq = parse_trace(EARLY_TRACE, network);
                let args = SimulatorArgs::new(network, 10000, true);
                black_box(sim_advanced(&[], &[], &mut sq.clone(), &args));
            });
        });
    });
}

//To verify that parallellization works fine on linktraces which are huge.
fn linktrace_parallel_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    let linktrace =
        load_linktrace_from_file("../crates/maybenot-simulator/tests/ether100M_synth40M.ltbin.gz")
            .expect("Failed to load LinkTrace ltbin from file");

    c.bench_function("Linktrace HiRes parallel simulation run", |b| {
        b.iter(|| {
            (0..100).into_par_iter().for_each(|_| {
                let network = Network::new(Duration::from_millis(10), None);
                let sq = parse_trace(EARLY_TRACE, network);
                let args = SimulatorArgs::new(network, 10000, true);
                let linktrace_args = SimulatorArgs {
                    simulated_network_type: Some(ExtendedNetworkLabels::Linktrace),
                    linktrace: Some(linktrace.clone()),
                    ..args
                };
                black_box(sim_advanced(&[], &[], &mut sq.clone(), &linktrace_args));
            });
        });
    });
}

fn linktrace_std100m_parallel_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulator/tests/ether100M_synth10K_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");

    c.bench_function(
        "Linktrace StdRes eth 100Mbps parallel simulation run",
        |b| {
            b.iter(|| {
                (0..100).into_par_iter().for_each(|_| {
                    let network = Network::new(Duration::from_millis(10), None);
                    let sq = parse_trace(EARLY_TRACE, network);
                    let args = SimulatorArgs::new(network, 10000, true);
                    let linktrace_args = SimulatorArgs {
                        simulated_network_type: Some(ExtendedNetworkLabels::Linktrace),
                        linktrace: Some(linktrace.clone()),
                        ..args
                    };
                    black_box(sim_advanced(&[], &[], &mut sq.clone(), &linktrace_args));
                });
            });
        },
    );
}

fn linktrace_std100k_parallel_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulator/tests/test100K_synth2M_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");

    c.bench_function(
        "Linktrace StdRes slow 100Kbps parallel simulation run",
        |b| {
            b.iter(|| {
                (0..100).into_par_iter().for_each(|_| {
                    let network = Network::new(Duration::from_millis(10), None);
                    let sq = parse_trace(EARLY_TRACE, network);
                    let args = SimulatorArgs::new(network, 10000, true);
                    let linktrace_args = SimulatorArgs {
                        simulated_network_type: Some(ExtendedNetworkLabels::Linktrace),
                        linktrace: Some(linktrace.clone()),
                        ..args
                    };
                    black_box(sim_advanced(&[], &[], &mut sq.clone(), &linktrace_args));
                });
            });
        },
    );
}

fn fixedtput_parallel_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulator/tests/EARLY_TEST_TRACE.log");

    c.bench_function("FixedTput parallel simulation run", |b| {
        b.iter(|| {
            (0..100).into_par_iter().for_each(|_| {
                let network = Network::new(Duration::from_millis(10), None);
                let sq = parse_trace(EARLY_TRACE, network);
                let args = SimulatorArgs::new(network, 10000, true);
                let fixedtput_args = SimulatorArgs {
                    simulated_network_type: Some(ExtendedNetworkLabels::FixedTput),
                    client_tput: Some(10_000_000),
                    server_tput: Some(100_000_000),
                    ..args
                };
                black_box(sim_advanced(&[], &[], &mut sq.clone(), &fixedtput_args));
            });
        });
    });
}

//criterion_group!(benches, benchmark_flat_vector, benchmark_ndarray);
//criterion_group!(benches, benchmark_busy_to, simulator_network_sample);
//criterion_group!(benches, simple_simulator_run, bottleneck_simulator_run, linktrace_simulator_run);
//criterion_group!(benches, simple_simulator_run);
//criterion_group!(benches, linktrace_simulator_run);
//criterion_group!(benches, linktrace_parallel_run);
criterion_group!(
    benches,
    benchmark_busy_to,
    simulator_network_sample,
    simple_simulator_run,
    bottleneck_simulator_run,
    fixedtput_simulator_run,
    linktrace_simulator_run,
    linktrace_std100m_simulator_run,
    linktrace_std100k_simulator_run,
    bottleneck_parallel_run,
    fixedtput_parallel_run,
    linktrace_parallel_run,
    linktrace_std100m_parallel_run,
    linktrace_std100k_parallel_run,
);

criterion_main!(benches);
