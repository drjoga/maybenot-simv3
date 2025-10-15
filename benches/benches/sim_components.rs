use std::time::Duration;

use maybenot_simulatorv3::{
    FixedTputLink, HiTraceTputLink, LinkType, SimulatorArgs, StdTraceTputLink,
    load_linktrace_from_file, load_topology_from_file, parse_trace, sim_advanced,
};

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use ndarray::Array2;
use rand::Rng;

//const SIM_EVENT_COUNTS: [usize; 3] = [5_000, 10_000, 20_000];
const SIM_EVENT_COUNTS: [usize; 1] = [10_000];
const CONFIG_FILES: [&str; 3] = [
    "/benches/topologies/maybenot_baseline_bench.toml",
    "/benches/topologies/maybenot_fast_bench.toml",
    "/benches/topologies/maybenot_complex_bench.toml",
];

fn v3_single_simulator_run(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();

    let sim_event_count = &SIM_EVENT_COUNTS[0];
    let config_file = CONFIG_FILES[1];
    let config_path = toml_path.clone() + config_file;
    let config_name = config_file.split('_').nth(1).unwrap();
    let (topology, linkstate) = load_topology_from_file(config_path.clone()).unwrap();
    let trafserv_to_client_delay = Duration::from_millis(20);
    let (si, sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
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
        for config_file in CONFIG_FILES.iter() {
            let config_path = toml_path.clone() + config_file;
            let config_name = config_file.split('_').nth(1).unwrap();
            let (topology, linkstate) = load_topology_from_file(config_path.clone()).unwrap();
            let trafserv_to_client_delay = Duration::from_millis(20);
            let (si, sq) = parse_trace(EARLY_TRACE, &topology, trafserv_to_client_delay);
            let mut output_len = 0;
            let bench_name = format!("v3_{:?}K_{},", sim_event_count / 1000, config_name);
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

fn sim_initialization_components(c: &mut Criterion) {
    const EARLY_TRACE: &str =
        include_str!("../../crates/maybenot-simulatorv3/tests/EARLY_TEST_TRACE.log");

    let toml_path = env!("CARGO_MANIFEST_DIR").to_string();
    let config_file = CONFIG_FILES[1];

    let config_path = toml_path.clone() + config_file;

    c.bench_function("SimulatorArgs.new", |b| {
        b.iter(|| {
            black_box(SimulatorArgs::new(10_000, true));
        });
    });

    c.bench_function("TopologyRead", |b| {
        b.iter(|| {
            let _ = black_box(load_topology_from_file(config_path.clone()));
        });
    });

    let (topology, _linkstate) = load_topology_from_file(config_path).unwrap();
    let trafserv_to_client_delay = Duration::from_millis(20);

    c.bench_function("parse_trace", |b| {
        b.iter(|| {
            black_box(parse_trace(
                EARLY_TRACE,
                &topology,
                trafserv_to_client_delay,
            ));
        });
    });
}

// Evaluate lookup performance of different data structures
fn initialize_flat_vector(rows: usize, cols: usize) -> (Vec<u32>, Vec<(usize, usize)>) {
    let mut array: Vec<u32> = vec![0; rows * cols];
    let mut rng = rand::rng();

    // Initialize the array with random numbers
    for row in 0..rows {
        for col in 0..cols {
            array[row * cols + col] = rng.random_range(0..10000);
        }
    }

    // Generate 10,000 random indices for lookup
    let indices: Vec<(usize, usize)> = (0..10_000)
        .map(|_| (rng.random_range(0..rows), rng.random_range(0..cols)))
        .collect();

    (array, indices)
}

fn initialize_ndarray(rows: usize, cols: usize) -> (Array2<u32>, Vec<(usize, usize)>) {
    let mut array = Array2::<u32>::zeros((rows, cols));
    let mut rng = rand::rng();

    // Initialize the array with random numbers
    for row in 0..rows {
        for col in 0..cols {
            array[[row, col]] = rng.random_range(0..10000);
        }
    }

    // Generate 10,000 random indices for lookup
    let indices: Vec<(usize, usize)> = (0..10_000)
        .map(|_| (rng.random_range(0..rows), rng.random_range(0..cols)))
        .collect();

    (array, indices)
}

fn flat_vector_lookup(array: &[u32], indices: &[(usize, usize)], cols: usize) -> u64 {
    let mut sum: u64 = 0;

    // Perform lookups using pre-generated indices
    for &(row, col) in indices {
        sum += array[row * cols + col] as u64;
    }

    black_box(sum) // Prevents the compiler from optimizing away the computation
}

fn ndarray_lookup(array: &Array2<u32>, indices: &[(usize, usize)]) -> u64 {
    let mut sum: u64 = 0;

    // Perform lookups using pre-generated indices
    for &(row, col) in indices {
        sum += array[[row, col]] as u64;
    }

    black_box(sum) // Prevents the compiler from optimizing away the computation
}

pub fn benchmark_flat_vector(c: &mut Criterion) {
    let rows = 22;
    let cols = 5_000_000;
    let (array, indices) = initialize_flat_vector(rows, cols); // Initialize once

    c.bench_function("Flat Vector Lookup", |b| {
        b.iter(|| flat_vector_lookup(&array, &indices, cols))
    });
}

pub fn benchmark_ndarray(c: &mut Criterion) {
    let rows = 22;
    let cols = 5_000_000;
    let (array, indices) = initialize_ndarray(rows, cols); // Initialize once

    c.bench_function("Ndarray Lookup", |b| {
        b.iter(|| ndarray_lookup(&array, &indices))
    });
}

// Evaluate lookup performace for different sizes of traces. Larger traces will be slower
// because of less cache locality. Note that the random lookup here is conservative as the
// actual use would be sequentially increasing within a limited range.
pub fn benchmark_busy_to(c: &mut Criterion) {
    // List of linktrace files to benchmark
    let linktrace_files = vec![
        "../crates/maybenot-simulatorv3/tests/ether100M_synth5K.ltbin.gz",
        "../crates/maybenot-simulatorv3/tests/ether100M_synth5M.ltbin.gz",
        "../crates/maybenot-simulatorv3/tests/ether100M_synth40M.ltbin.gz",
    ];

    let nr_samples = 10_000;
    let mut rng = rand::rng();

    for file in linktrace_files {
        // Load the LinkTrace instance from the file
        let linksim_trace = load_linktrace_from_file(file)
            .unwrap_or_else(|_| panic!("Failed to load LinkTrace ltbin from file: {}", file));

        // Generate random time slot values between 0 and trace length
        let nr_time_slots = linksim_trace.get_nr_timeslots() as usize;
        let time_slots: Vec<usize> = (0..nr_samples)
            .map(|_| rng.random_range(0..nr_time_slots))
            .collect();

        // Generate random packet size values between 40 and 1500
        let pkt_sizes: Vec<i32> = (0..nr_samples)
            .map(|_| rng.random_range(40..=1500))
            .collect();

        // Benchmark the get_dl_busy_to function for the current linktrace file
        c.bench_function(&format!("get_dl_busy_to_  {}", file), |b| {
            b.iter(|| {
                for i in 0..nr_samples {
                    // Use black_box to prevent the compiler from optimizing away the call
                    black_box(linksim_trace.get_busy_to(time_slots[i], pkt_sizes[i]));
                }
            })
        });
    }
}

fn simulator_network_sample(c: &mut Criterion) {
    //How many packets to process
    let nr_iter = 10_000;

    // Initialize the vector with Duration values instead of Instant
    let mut durations = Vec::with_capacity(nr_iter);
    let mut rng = rand::rng();

    // Start with Duration::ZERO
    let mut current_duration = Duration::from_micros(1);
    durations.push(current_duration);

    // Total duration target (4 seconds in microseconds)
    let target_duration = 4_000_000;
    let mut accumulated_duration = 1; // Starting from 1 microsecond

    // Generate more Duration values
    for _ in 1..nr_iter {
        // Calculate remaining microseconds and divide by remaining durations to get average step size
        let remaining_steps = nr_iter - durations.len();
        let remaining_duration = target_duration - accumulated_duration;
        let average_step = remaining_duration / remaining_steps;

        // Generate a random step, allowing some variation around the average step
        let step_micros: u64 = rng.random_range(average_step / 2..=average_step * 2) as u64;

        // Update the accumulated duration
        accumulated_duration += step_micros as usize;

        // Add the random step to the current Duration
        current_duration += Duration::from_micros(step_micros);

        // Push the new Duration into the vector
        durations.push(current_duration);
    }

    // Create HiTraceTput link with loaded trace
    let linktrace =
        load_linktrace_from_file("../crates/maybenot-simulatorv3/tests/ether100M_synth5M.ltbin.gz")
            .expect("Failed to load LinkTrace ltbin from file");
    let mut network_lt = LinkType::HiTraceTput(HiTraceTputLink::new(
        0,                         // id
        0,                         // from node
        1,                         // to node
        Duration::from_millis(10), // propagation delay
        linktrace,
        true,   // fixed propagation
        vec![], // prop_us_vec (empty for fixed propagation)
    ));

    c.bench_function("Linktrace HiRes network.sample", |b| {
        b.iter(|| {
            for duration in durations.iter().take(nr_iter) {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_lt.sample(*duration));
                network_lt.reset();
            }
        })
    });

    // Create StdTraceTput link with loaded trace
    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulatorv3/tests/ether100M_synth10K_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");
    let mut network_lt_std = LinkType::StdTraceTput(StdTraceTputLink::new(
        1,                         // id
        0,                         // from node
        1,                         // to node
        Duration::from_millis(10), // propagation delay
        linktrace,
        true,   // fixed propagation
        vec![], // prop_us_vec (empty for fixed propagation)
    ));

    c.bench_function("Linktrace StdRes eth 100Mbps network.sample", |b| {
        b.iter(|| {
            for duration in durations.iter().take(nr_iter) {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_lt_std.sample(*duration));
                network_lt_std.reset();
            }
        })
    });

    // Create slow 100Kbps StdTraceTput link
    let linktrace = load_linktrace_from_file(
        "../crates/maybenot-simulatorv3/tests/test100K_synth2M_std.ltbin.gz",
    )
    .expect("Failed to load LinkTrace ltbin from file");
    let mut network_lt_slow = LinkType::StdTraceTput(StdTraceTputLink::new(
        2,                         // id
        0,                         // from node
        1,                         // to node
        Duration::from_millis(10), // propagation delay
        linktrace,
        true,   // fixed propagation
        vec![], // prop_us_vec (empty for fixed propagation)
    ));

    c.bench_function("Linktrace StdRes slow 100Kbps network.sample", |b| {
        b.iter(|| {
            for duration in durations.iter().take(nr_iter) {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_lt_slow.sample(*duration));
                network_lt_slow.reset();
            }
        })
    });

    // Create FixedTput link (100 Mbps)
    let mut network_ftput = LinkType::FixedTput(FixedTputLink::new(
        3,                         // id
        0,                         // from node
        1,                         // to node
        Duration::from_millis(10), // propagation delay
        100_000_000,               // throughput in bps (100 Mbps)
        true,                      // fixed propagation
        vec![],                    // prop_us_vec (empty for fixed propagation)
    ));

    c.bench_function("FixedTput 100Mbps network.sample", |b| {
        b.iter(|| {
            for duration in durations.iter().take(nr_iter) {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_ftput.sample(*duration));
            }
        })
    });

    // Create another FixedTput link (10 Mbps)
    let mut network_ftput_slow = LinkType::FixedTput(FixedTputLink::new(
        4,                         // id
        0,                         // from node
        1,                         // to node
        Duration::from_millis(10), // propagation delay
        10_000_000,                // throughput in bps (10 Mbps)
        true,                      // fixed propagation
        vec![],                    // prop_us_vec (empty for fixed propagation)
    ));

    c.bench_function("FixedTput 10Mbps network.sample", |b| {
        b.iter(|| {
            for duration in durations.iter().take(nr_iter) {
                // Use black_box to prevent the compiler from optimizing away the call
                black_box(network_ftput_slow.sample(*duration));
            }
        })
    });

    //sim(&[], &[], &mut pq.clone(), network.delay, 1000, true);
}

criterion_group!(
    sim_components,
    sim_initialization_components,
    benchmark_flat_vector,
    benchmark_ndarray,
    benchmark_busy_to,
    simulator_network_sample,
    v3_single_simulator_run,
    v3_multi_run,
);

criterion_main!(sim_components);
