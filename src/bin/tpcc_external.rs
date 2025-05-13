use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

use clap::Parser;
use fbtree::{
    affinity::{get_current_cpu, get_num_cores, set_affinity},
    bp::MemPool,
    container::ContainerManager,
    prelude::{
        run_tpcc_for_thread, tpcc_load_schema, DeliveryTxn, NewOrderTxn, OrderStatusTxn,
        PaymentTxn, StockLevelTxn, TPCCConfig, TPCCOutput, TPCCStat, TPCCTableInfo, TPCCTxnProfile,
        TPCCTxnProfileID, TxnStorageTrait, PAGE_SIZE,
    },
    txn_storage::NoWaitTxnStorage,
};

// #[global_allocator]
// static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    let mut config = TPCCConfig::parse();
    config.fixed_warehouse_per_thread = false;
    config.warmup_time = 0;
    println!("config: {:?}", config);
    println!("Fixed warehouse per thread is set to false for external TPCC benchmark");
    println!("Warmup time is set to 0 for external TPCC benchmark");

    let num_cores = get_num_cores();
    if config.num_threads + 1 > num_cores {
        panic!(
            "Number of worker threads + main thread {} exceeds number of cores {}",
            config.num_threads, num_cores
        );
    }

    // Set frames for 32GB of BP (assume that PAGE_SIZE is 16KB)
    let num_frames = 2 * 1024 * 1024;
    println!(
        "BP size: {} GB",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024)
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
    #[cfg(feature = "use_vmc_tpcc")]
    let bp = {
        use fbtree::bp::VMCachePool;
        Arc::new(VMCachePool::<false, 64>::new(num_frames, cm).unwrap())
    };
    #[cfg(not(feature = "use_vmc_tpcc"))]
    let bp = {
        use fbtree::bp::BufferPoolClock;
        Arc::new(BufferPoolClock::<64>::new(num_frames, cm).unwrap())
    };
    let elapsed = start.elapsed();
    println!("Done allocating buffer pool: {:?}", elapsed);

    // Load the database from the directory.
    let txn_storage = NoWaitTxnStorage::load(&bp);
    let tbl_info = tpcc_load_schema(&txn_storage);

    set_affinity(get_num_cores() - 1).unwrap();
    let current_cpu = get_current_cpu();
    println!("Main thread pinned to CPU {}", current_cpu);

    let mut stats_and_outs = Vec::with_capacity(config.num_threads);
    let flag = AtomicBool::new(true); // while flag is true, keep running the benchmark

    // Warmup
    // let warmup_barrier = Arc::new(Barrier::new(config.num_threads + 1));
    // std::thread::scope(|s| {
    //     for i in 0..config.num_threads {
    //         let thread_id = i;
    //         let warmup_barrier = warmup_barrier.clone();
    //         let config_ref = &config;
    //         let txn_storage_ref = &txn_storage;
    //         let tbl_info_ref = &tbl_info;
    //         let flag_ref = &flag;
    //         s.spawn(move || {
    //             run_tpcc_for_thread(
    //                 true,
    //                 thread_id,
    //                 true,
    //                 warmup_barrier,
    //                 config_ref,
    //                 txn_storage_ref,
    //                 tbl_info_ref,
    //                 flag_ref,
    //             );
    //         });
    //     }
    //     // Start timer for config duration
    //     warmup_barrier.wait();
    //     std::thread::sleep(std::time::Duration::from_secs(config.warmup_time));
    //     flag.store(false, Ordering::Release);
    //     // Automatically join all threads
    // });
    // println!("BP stats after warmup: \n{}", unsafe { bp.stats() });
    println!("Warm up skipped in external TPCC benchmark");

    flag.store(true, Ordering::Release);
    // Run the benchmark
    let run_barrier = Arc::new(Barrier::new(config.num_threads + 1));
    std::thread::scope(|s| {
        let mut handlers = Vec::with_capacity(config.num_threads);
        for i in 0..config.num_threads {
            let thread_id = i;
            let run_barrier = run_barrier.clone();
            let config_ref = &config;
            let txn_storage_ref = &txn_storage;
            let tbl_info_ref = &tbl_info;
            let flag_ref = &flag;
            let handler = s.spawn(move || {
                run_tpcc_for_thread(
                    false,
                    thread_id,
                    true,
                    run_barrier,
                    config_ref,
                    txn_storage_ref,
                    tbl_info_ref,
                    flag_ref,
                )
            });
            handlers.push(handler);
        }
        // Start timer for config duration
        run_barrier.wait();
        std::thread::sleep(std::time::Duration::from_secs(config.exec_time));
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
    let seconds = config.exec_time;

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

    for p in 0..TPCCTxnProfileID::Max as u8 {
        let p = TPCCTxnProfileID::from(p);
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
        if final_stat[p].min_latency == u64::MAX {
            0
        } else {
            final_stat[p].min_latency
        },
        final_stat[p].max_latency
    );
    }

    println!("\nSystem Abort Details:");

    NewOrderTxn::print_abort_details(&final_stat[TPCCTxnProfileID::NewOrderTxn].abort_details);
    PaymentTxn::print_abort_details(&final_stat[TPCCTxnProfileID::PaymentTxn].abort_details);
    OrderStatusTxn::print_abort_details(
        &final_stat[TPCCTxnProfileID::OrderStatusTxn].abort_details,
    );
    DeliveryTxn::print_abort_details(&final_stat[TPCCTxnProfileID::DeliveryTxn].abort_details);
    StockLevelTxn::print_abort_details(&final_stat[TPCCTxnProfileID::StockLevelTxn].abort_details);

    println!("BP stats: \n{}", unsafe { bp.stats() });
    bp.clear_dirty_flags().unwrap();
}

pub fn test_all_transactions<T: TxnStorageTrait>(
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
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
