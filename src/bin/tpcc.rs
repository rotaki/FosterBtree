#[allow(unused_imports)]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

use clap::Parser;
use fbtree::{
    affinity::{get_current_cpu, get_total_cpus, with_affinity},
    bp::{get_test_bp, MemPool},
    prelude::{
        print_tpcc_stats, run_tpcc, tpcc_gen_all_tables, tpcc_show_table_stats, TPCCConfig,
        PAGE_SIZE,
    },
    print_cfg_flags,
    txn_storage::NoWaitTxnStorage,
};

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    print_cfg_flags::print_cfg_flags();

    let config = TPCCConfig::parse();
    println!("config: {:?}", config);

    let num_cores = get_total_cpus();
    if config.num_threads + 1 > num_cores {
        panic!(
            "Number of worker threads + main thread {} exceeds number of cores {}",
            config.num_threads, num_cores
        );
    }

    // Set frames for 1GB memory per warehouses
    let num_frames = config.num_warehouses as usize * 1024 * 1024 * 1024 / PAGE_SIZE;
    println!(
        "BP size: {} GB",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024)
    );

    let bp = get_test_bp(num_frames);

    let txn_storage = NoWaitTxnStorage::new(&bp);
    let tbl_info = tpcc_gen_all_tables(&txn_storage, config.num_warehouses);
    tpcc_show_table_stats(&txn_storage, &tbl_info);
    println!("BP stats after load: \n{}", unsafe { bp.stats() });

    let stats_and_outs = with_affinity(get_total_cpus() - 1, || {
        let current_cpu = get_current_cpu();
        println!("Main thread pinned to CPU {}", current_cpu);

        if config.warmup_time > 0 {
            let _ = run_tpcc(true, &config, &txn_storage, &tbl_info);
            println!("BP stats after warmup: \n{}", unsafe { bp.stats() });
        } else {
            println!("No warmup time specified, skipping warmup");
        }

        if config.exec_time == 0 {
            panic!("Execution time is 0. Please specify a non-zero execution time.");
        }

        run_tpcc(false, &config, &txn_storage, &tbl_info)
    })
    .unwrap();

    print_tpcc_stats(
        config.num_warehouses,
        config.num_threads,
        config.exec_time,
        stats_and_outs,
    );

    println!("BP stats: \n{}", unsafe { bp.stats() });
    bp.clear_dirty_flags().unwrap();
}
