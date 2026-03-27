use clap::Parser;
use fbtree::{
    affinity::{get_current_cpu, get_total_cpus, with_affinity},
    bp::{get_bp, MemPool},
    container::ContainerManager,
    prelude::{TPCCConfig, PAGE_SIZE},
    print_cfg_flags,
    tpcc::TpccBenchmark,
    txn_storage::NoWaitTxnStorage,
};
use std::sync::Arc;

// #[global_allocator]
// static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

// use mimalloc::MiMalloc;
//
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    print_cfg_flags::print_cfg_flags();

    let mut config = TPCCConfig::parse();
    config.fixed_warehouse_per_thread = false;
    if config.bp_size == 0 {
        // Set frames with 1GB memory per warehouse if the size is not specified
        config.bp_size = config.num_warehouses as usize;
    }
    println!("config: {:?}", config);
    println!("Fixed warehouse per thread is set to false for external TPCC benchmark");

    let num_cores = get_total_cpus();
    if config.num_threads + 1 > num_cores {
        panic!(
            "Number of worker threads + main thread {} exceeds number of cores {}",
            config.num_threads, num_cores
        );
    }

    // Otherwise, set frames equal to the specified size
    let num_frames = config.bp_size * 1024 * 1024 * 1024 / PAGE_SIZE;
    println!(
        "BP size: {} GB ({} frames)",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024),
        num_frames
    );

    let base_dir = format!("tpcc_db_w{}", config.num_warehouses);
    // Check if the directory exists
    if !std::path::Path::new(&base_dir).exists() {
        panic!(
            "Directory {} does not exist. Please run tpcc_db_gen first.",
            base_dir
        );
    }
    println!("Reading from directory: {}", base_dir);
    let cm = Arc::new(ContainerManager::new(base_dir, true, false).unwrap());

    println!("Allocating buffer pool");
    let start = std::time::Instant::now();
    let bp = get_bp(num_frames, cm);
    let elapsed = start.elapsed();
    println!("Done allocating buffer pool: {:?}", elapsed);

    // Load the database from the directory.
    let txn_storage = NoWaitTxnStorage::load(&bp);
    let tpcc_bench = TpccBenchmark::load(txn_storage, config.num_warehouses);

    let result = with_affinity(get_total_cpus() - 1, || {
        let current_cpu = get_current_cpu();
        println!("Main thread pinned to CPU {}", current_cpu);

        // Warmup
        if config.warmup_time > 0 {
            let _ = tpcc_bench.run_benchmark(
                config.num_threads,
                config.warmup_time,
                !config.fixed_warehouse_per_thread,
            );
            println!("BP stats after warmup: \n{}", bp.stats());
        } else {
            println!("Warm up skipped");
        }

        // Run the benchmark
        if config.exec_time == 0 {
            panic!("Execution time is 0. Please specify a non-zero execution time.");
        }
        tpcc_bench.run_benchmark(
            config.num_threads,
            config.exec_time,
            !config.fixed_warehouse_per_thread,
        )
    })
    .unwrap();

    result.print(true);

    println!("BP stats: \n{}", bp.stats());
    bp.clear_dirty_flags().unwrap();
}
