use std::sync::atomic::{AtomicBool, Ordering};

use clap::Parser;
use fbtree::{
    bp::{get_test_bp, MemPool},
    prelude::{
        load_all_tables, run_benchmark_for_thread, DeliveryTxn, NewOrderTxn, OrderStatusTxn,
        PaymentTxn, StockLevelTxn, TPCCConfig, TPCCOutput, TPCCStat, TableInfo, TxnProfile,
        TxnProfileID, TxnStorageTrait, PAGE_SIZE,
    },
    txn_storage::NoWaitTxnStorage,
};

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

pub fn main() {
    let config = TPCCConfig::parse();

    // Set frames for 4GB memory
    let num_frames = 4 * 1024 * 1024 * 1024 / PAGE_SIZE;
    let bp = get_test_bp(num_frames);
    let txn_storage = NoWaitTxnStorage::new(&bp);
    let tbl_info = load_all_tables(&txn_storage, &config);

    println!("BP stats after load: \n{}", bp.stats());

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
                run_benchmark_for_thread(
                    thread_id,
                    config_ref,
                    txn_storage_ref,
                    tbl_info_ref,
                    flag_ref,
                );
            });
        }
        // Start timer for config duration
        std::thread::sleep(std::time::Duration::from_secs(config.warmup));
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
                run_benchmark_for_thread(
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
        std::thread::sleep(std::time::Duration::from_secs(config.duration));
        flag.store(false, Ordering::Release);

        for handler in handlers {
            stats_and_outs.push(handler.join().unwrap());
        }
    });

    let mut final_stat = TPCCStat::new();
    for (stat, _) in stats_and_outs {
        final_stat.add(&stat);
    }
    let agg_stat = final_stat.aggregate_perf();

    let num_warehouses = config.num_warehouses;
    let num_threads = config.num_threads;
    let seconds = config.duration;

    println!(
        "{} warehouse(s), {} thread(s), {} second(s)",
        num_warehouses, num_threads, seconds
    );
    println!("    commits: {}", agg_stat.num_commits);
    println!("    usr_aborts: {}", agg_stat.num_usr_aborts);
    println!("    sys_aborts: {}", agg_stat.num_sys_aborts);
    println!(
        "Throughput: {} txns/s",
        agg_stat.num_commits as f64 / seconds as f64
    );

    println!("\nDetails:");

    for p in 0..TxnProfileID::Max as u8 {
        let p = TxnProfileID::from(p);
        let profile = format!("{}", p);
        let tries =
            final_stat[p].num_commits + final_stat[p].num_usr_aborts + final_stat[p].num_sys_aborts;
        println!(
        "    {:<15} c[{:6.2}%]:{:8}({:6.2}%)   ua:{:8}({:6.2}%)  sa:{:8}({:6.2}%)  avgl:{:8.0}  minl:{:8}  maxl:{:8}",
        profile,
        (final_stat[p].num_commits as f64) / (agg_stat.num_commits as f64) * 100.0,
        final_stat[p].num_commits,
        (final_stat[p].num_commits as f64) / (tries as f64) * 100.0,
        final_stat[p].num_usr_aborts,
        (final_stat[p].num_usr_aborts as f64) / (tries as f64) * 100.0,
        final_stat[p].num_sys_aborts,
        (final_stat[p].num_sys_aborts as f64) / (tries as f64) * 100.0,
        (final_stat[p].total_latency as f64) / (final_stat[p].num_commits as f64),
        if final_stat[p].min_latency == std::u64::MAX {
            0
        } else {
            final_stat[p].min_latency
        },
        final_stat[p].max_latency
    );
    }

    println!("\nSystem Abort Details:");

    NewOrderTxn::print_abort_details(&final_stat[TxnProfileID::NewOrderTxn].abort_details);
    PaymentTxn::print_abort_details(&final_stat[TxnProfileID::PaymentTxn].abort_details);
    OrderStatusTxn::print_abort_details(&final_stat[TxnProfileID::OrderStatusTxn].abort_details);
    DeliveryTxn::print_abort_details(&final_stat[TxnProfileID::DeliveryTxn].abort_details);
    StockLevelTxn::print_abort_details(&final_stat[TxnProfileID::StockLevelTxn].abort_details);

    println!("BP stats: \n{}", bp.stats());
}

pub fn test_all_transactions<T: TxnStorageTrait>(
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TableInfo,
    stat: &mut TPCCStat,
    out: &mut TPCCOutput,
    w_id: u16,
) {
    for _ in 0..10000 {
        let txn = PaymentTxn::new(config, w_id);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }

    for _ in 0..10000 {
        let txn = NewOrderTxn::new(config, w_id);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }

    for _ in 0..10000 {
        let txn = OrderStatusTxn::new(config, w_id);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }

    for _ in 0..10000 {
        let txn = DeliveryTxn::new(config, w_id);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }

    for _ in 0..10000 {
        let txn = StockLevelTxn::new(config, w_id);
        txn.run(config, txn_storage, tbl_info, stat, out);
    }
}
