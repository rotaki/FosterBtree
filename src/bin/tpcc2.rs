#[allow(unused_imports)]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

use clap::Parser;
use fbtree::{
    affinity::{get_total_cpus, with_affinity},
    bp::{get_test_bp_clock, MemPool},
    prelude::PAGE_SIZE,
    print_cfg_flags,
    tpcc2::TpccBenchmark,
};

/// Configuration settings parsed from command-line arguments for TPC-C2.
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct TpccConfig {
    /// BP size in GB. 0 means 1GB per warehouse.
    #[arg(short = 'b', long, default_value_t = 0)]
    pub bp_size: usize,

    /// Number of warehouses.
    #[arg(short = 'w', long, default_value_t = 1)]
    pub num_warehouses: u16,

    /// Number of threads.
    #[arg(short = 't', long, default_value_t = 1)]
    pub num_threads: usize,

    /// Warmup duration in seconds.
    #[arg(short = 'd', long, default_value_t = 3)]
    pub warmup_time: u64,

    /// Test duration in seconds.
    #[arg(short = 'D', long, default_value_t = 10)]
    pub exec_time: u64,
}

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    print_cfg_flags::print_cfg_flags();

    let config = TpccConfig::parse();
    println!("config: {:?}", config);

    let num_cores = get_total_cpus();
    if config.num_threads + 1 > num_cores {
        panic!(
            "Number of worker threads + main thread {} exceeds number of cores {}",
            config.num_threads, num_cores
        );
    }

    // Set frames for 1GB memory per warehouse if not specified
    let num_frames = if config.bp_size > 0 {
        config.bp_size * 1024 * 1024 * 1024 / PAGE_SIZE
    } else {
        config.num_warehouses as usize * 1024 * 1024 * 1024 / PAGE_SIZE
    };
    println!(
        "BP size: {} GB",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024)
    );

    let bp = get_test_bp_clock::<64>(num_frames);

    // Create the TPC-C2 benchmark
    println!(
        "Initializing TPC-C2 benchmark with {} warehouses...",
        config.num_warehouses
    );
    let benchmark = TpccBenchmark::new(bp.clone(), config.num_warehouses);

    println!("BP stats after load: \n{}", unsafe { bp.stats() });

    // Print thread affinity information
    for i in 0..config.num_threads {
        println!("Thread {} pinned to CPU {}", i, i);
    }
    println!(
        "{} warehouse(s), {} thread(s), {} second(s)",
        config.num_warehouses, config.num_threads, config.exec_time
    );

    let result = with_affinity(get_total_cpus() - 1, || {
        if config.warmup_time > 0 {
            println!("Running warmup for {} seconds...", config.warmup_time);
            let warmup_result =
                benchmark.run_benchmark(config.num_threads, config.warmup_time, false);
            println!(
                "Warmup completed. Throughput: {:.2} txns/sec",
                warmup_result.throughput
            );
            println!("BP stats after warmup: \n{}", unsafe { bp.stats() });
        }

        if config.exec_time == 0 {
            panic!("Execution time is 0. Please specify a non-zero execution time.");
        }

        // Run the actual benchmark
        benchmark.run_benchmark(config.num_threads, config.exec_time, false)
    })
    .unwrap();

    // Print results
    result.print(true);

    println!("\nFinal BP stats: \n{}", unsafe { bp.stats() });
    bp.clear_dirty_flags().unwrap();
}
