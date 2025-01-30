use std::sync::atomic::{AtomicBool, Ordering};

use clap::Parser;
use fbtree::{
    bp::{get_test_bp, MemPool},
    prelude::{
        run_ycsb_for_thread, ycsb_load_all_tables, ycsb_show_table_stats, ReadTxn, TxnStorageTrait,
        UpdateTxn, YCSBConfig, YCSBOutput, YCSBStat, YCSBTableInfo, YCSBTxnProfile,
        YCSBTxnProfileID, PAGE_SIZE,
    },
    txn_storage::NoWaitTxnStorage,
};

// #[global_allocator]
// static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    #[cfg(feature = "no_tree_hint")]
    {
        println!("Tree hint disabled");
    }
    #[cfg(not(feature = "no_tree_hint"))]
    {
        println!("Tree hint enabled");
    }
    #[cfg(feature = "no_bp_hint")]
    {
        println!("BP hint disabled");
    }
    #[cfg(not(feature = "no_bp_hint"))]
    {
        println!("BP hint enabled");
    }
    let config = YCSBConfig::parse();
    println!("config: {:?}", config);

    // Set frames for 1GB memory per warehouses
    // Compute the expected size of the tree.
    // There are two indices, primary and secondary, and each entry is a key-value pair.
    // To account for the internal nodes and extra space for each entry, we multiply by 4 and add 20 bytes per entry.
    let size = 2 * 4 * config.num_keys * (20 + config.key_size + config.value_size);
    let num_frames = size / PAGE_SIZE;
    println!("num_frames: {}", num_frames);
    println!(
        "expected size in gigabytes: {}",
        size as f64 / 1024.0 / 1024.0 / 1024.0
    );
    let bp = get_test_bp(num_frames);
    let txn_storage = NoWaitTxnStorage::new(&bp);
    let tbl_info = ycsb_load_all_tables(&txn_storage, &config);
    ycsb_show_table_stats(&txn_storage, &tbl_info);

    println!("BP stats after load: \n{}", bp.stats());

    // ycsb_preliminary_secondary_scan(&txn_storage, &tbl_info);

    /*
    // Test all transactions
    let mut stat = YCSBStat::new();
    let mut out = YCSBOutput::new();
    test_all_transactions(&config, &txn_storage, &tbl_info, &mut stat, &mut out);
    println!("{}", stat);
    */

    let mut stats_and_outs = Vec::with_capacity(config.num_threads);
    let flag = AtomicBool::new(true); // while flag is true, keep running the benchmark

    // Warmup
    std::thread::scope(|s| {
        for i in 0..config.num_threads {
            let thread_id = i;
            let config_ref = &config;
            let txn_storage_ref = &txn_storage;
            let tbl_info_ref = &tbl_info;
            let flag_ref = &flag;
            s.spawn(move || {
                run_ycsb_for_thread(
                    thread_id,
                    config_ref,
                    txn_storage_ref,
                    tbl_info_ref,
                    flag_ref,
                );
            });
        }
        // Start timer for config duration
        std::thread::sleep(std::time::Duration::from_secs(config.warmup_time));
        flag.store(false, Ordering::Release);

        // Automatically join all threads
    });

    println!("BP stats after warmup: \n{}", bp.stats());

    flag.store(true, Ordering::Release);
    // Run the benchmark
    std::thread::scope(|s| {
        let mut handlers = Vec::with_capacity(config.num_threads);
        for i in 0..config.num_threads {
            let thread_id = i;
            let config_ref = &config;
            let txn_storage_ref = &txn_storage;
            let tbl_info_ref = &tbl_info;
            let flag_ref = &flag;
            let handler = s.spawn(move || {
                run_ycsb_for_thread(
                    thread_id,
                    config_ref,
                    txn_storage_ref,
                    tbl_info_ref,
                    flag_ref,
                )
            });
            handlers.push(handler);
        }
        // Start timer for config duration
        std::thread::sleep(std::time::Duration::from_secs(config.exec_time));
        flag.store(false, Ordering::Release);

        for handler in handlers {
            stats_and_outs.push(handler.join().unwrap());
        }
    });

    let mut final_stat = YCSBStat::new();
    for (stat, _) in stats_and_outs {
        final_stat.add(&stat);
    }
    let agg_stat = final_stat.aggregate_perf();

    let num_keys = config.num_keys;
    let key_size = config.key_size;
    let value_size = config.value_size;
    let num_threads = config.num_threads;
    let seconds = config.exec_time;

    println!(
        "{} keys(s), {} byte(s) key, {} byte(s) value, {} thread(s), {} second(s)",
        num_keys, key_size, value_size, num_threads, seconds
    );
    println!("    commits: {}", agg_stat.num_commits);
    println!("    sys_aborts: {}", agg_stat.num_sys_aborts);
    println!(
        "Throughput: {} txns/s",
        agg_stat.num_commits as f64 / seconds as f64
    );

    println!("\nDetails:");

    for p in 0..YCSBTxnProfileID::Max as u8 {
        let p = YCSBTxnProfileID::from(p);
        let profile = format!("{}", p);
        let tries = final_stat[p].num_commits + final_stat[p].num_sys_aborts;
        println!(
        "    {:<15} c[{:6.2}%]:{:8}({:6.2}%)  sa:{:8}({:6.2}%)  avgl:{:8.0}  minl:{:8}  maxl:{:8}",
        profile,
        (final_stat[p].num_commits as f64) / (agg_stat.num_commits as f64) * 100.0,
        final_stat[p].num_commits,
        (final_stat[p].num_commits as f64) / (tries as f64) * 100.0,
        final_stat[p].num_sys_aborts,
        (final_stat[p].num_sys_aborts as f64) / (tries as f64) * 100.0,
        (final_stat[p].total_latency as f64) / (final_stat[p].num_commits as f64),
        if final_stat[p].min_latency == u64::MAX {
            0
        } else {
            final_stat[p].min_latency
        },
        final_stat[p].max_latency
    );
    }

    println!("\nStat Details:");
    println!("{}", final_stat);

    println!("BP stats: \n{}", bp.stats());
}

pub fn test_all_transactions<T: TxnStorageTrait>(
    config: &YCSBConfig,
    txn_storage: &T,
    tbl_info: &YCSBTableInfo,
    stat: &mut YCSBStat,
    out: &mut YCSBOutput,
) {
    let count = 10000;

    println!("Running {} read transactions", count);
    for _ in 0..count {
        let txn = ReadTxn::new(config);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }

    println!("Running {} update transactions", count);
    for _ in 0..count {
        let txn = UpdateTxn::new(config);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }
}
