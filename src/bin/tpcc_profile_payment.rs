/// Profiling binary for TPC-C Payment transaction.
/// Runs the FULL TPC-C workload mix (45% NewOrder, 43% Payment, 4% OrderStatus,
/// 4% Delivery, 4% StockLevel) but instruments only Payment with nanosecond-precision
/// per-operation timing. This ensures realistic contention patterns.
///
/// Usage:
///   cargo run --release --bin tpcc_profile_payment --features bp_clock -- -w 2 -t 4
///   cargo run --release --bin tpcc_profile_payment --features bp_pt -- -w 2 -t 4
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};
use std::time::{Duration, Instant};

use clap::Parser;
use fbtree::{
    affinity::{get_current_cpu, get_total_cpus, with_affinity},
    bp::{reset_macro_profile, MemPool},
    prelude::*,
    print_cfg_flags,
    random::gen_truncated_randomized_exponential_backoff,
    txn_storage::NoWaitTxnStorage,
};

// -- Per-operation timing accumulator ----------------------------------------

#[derive(Debug, Clone, Default)]
struct OpTiming {
    total_ns: u64,
    min_ns: u64,
    max_ns: u64,
    count: u64,
}

impl OpTiming {
    fn new() -> Self {
        OpTiming {
            total_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            count: 0,
        }
    }

    fn record(&mut self, ns: u64) {
        self.total_ns += ns;
        if ns < self.min_ns {
            self.min_ns = ns;
        }
        if ns > self.max_ns {
            self.max_ns = ns;
        }
        self.count += 1;
    }

    fn avg_ns(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_ns as f64 / self.count as f64
        }
    }

    fn merge(&mut self, other: &OpTiming) {
        self.total_ns += other.total_ns;
        self.count += other.count;
        if other.count > 0 {
            if other.min_ns < self.min_ns {
                self.min_ns = other.min_ns;
            }
            if other.max_ns > self.max_ns {
                self.max_ns = other.max_ns;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct PaymentProfile {
    total: OpTiming,
    begin_txn: OpTiming,
    update_warehouse: OpTiming,
    update_district: OpTiming,
    get_customer: OpTiming,
    commit: OpTiming,
    num_commits: u64,
    num_aborts: u64,
}

impl PaymentProfile {
    fn new() -> Self {
        PaymentProfile {
            total: OpTiming::new(),
            begin_txn: OpTiming::new(),
            update_warehouse: OpTiming::new(),
            update_district: OpTiming::new(),
            get_customer: OpTiming::new(),
            commit: OpTiming::new(),
            num_commits: 0,
            num_aborts: 0,
        }
    }

    fn merge(&mut self, other: &PaymentProfile) {
        self.total.merge(&other.total);
        self.begin_txn.merge(&other.begin_txn);
        self.update_warehouse.merge(&other.update_warehouse);
        self.update_district.merge(&other.update_district);
        self.get_customer.merge(&other.get_customer);
        self.commit.merge(&other.commit);
        self.num_commits += other.num_commits;
        self.num_aborts += other.num_aborts;
    }
}

// -- Formatting helpers ------------------------------------------------------

fn format_ns(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:>10.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:>10.2} \u{00b5}s", ns / 1_000.0)
    } else {
        format!("{:>10.0} ns", ns)
    }
}

fn print_op(prefix: &str, name: &str, op: &OpTiming, total_avg: f64) {
    if op.count == 0 {
        return;
    }
    let avg = op.avg_ns();
    let pct = if total_avg > 0.0 {
        avg / total_avg * 100.0
    } else {
        0.0
    };
    println!(
        "{}{:<28} avg: {}   min: {}   max: {}   cnt: {:>8}   [{:>5.1}%]",
        prefix,
        name,
        format_ns(avg),
        format_ns(op.min_ns as f64),
        format_ns(op.max_ns as f64),
        op.count,
        pct,
    );
}

fn print_tree(profile: &PaymentProfile) {
    let total_avg = profile.total.avg_ns();
    println!();
    println!(
        "Payment Transaction Profile (commits: {}, aborts: {})",
        profile.num_commits, profile.num_aborts
    );
    println!("================================================================================");
    print_op("", "Total Transaction", &profile.total, total_avg);
    println!("\u{2502}");
    print_op(
        "\u{251c}\u{2500}\u{2500} ",
        "BeginTxn",
        &profile.begin_txn,
        total_avg,
    );
    print_op(
        "\u{251c}\u{2500}\u{2500} ",
        "UpdateWarehouse",
        &profile.update_warehouse,
        total_avg,
    );
    print_op(
        "\u{251c}\u{2500}\u{2500} ",
        "UpdateDistrict",
        &profile.update_district,
        total_avg,
    );
    print_op(
        "\u{251c}\u{2500}\u{2500} ",
        "GetCustomer",
        &profile.get_customer,
        total_avg,
    );
    println!("\u{2502}");
    print_op(
        "\u{2514}\u{2500}\u{2500} ",
        "Commit",
        &profile.commit,
        total_avg,
    );
    println!("================================================================================");
}

// -- Instrumented Payment execution ------------------------------------------

macro_rules! time_op {
    ($profile:expr, $field:ident, $body:expr) => {{
        let __start = Instant::now();
        let __result = $body;
        let __elapsed = __start.elapsed().as_nanos() as u64;
        $profile.$field.record(__elapsed);
        __result
    }};
}

fn run_profiled_payment<T: TxnStorageTrait>(
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    profile: &mut PaymentProfile,
    w_id: u16,
) {
    let input = PaymentTxnInput::new(config, w_id);
    let txn_start = Instant::now();

    // Begin transaction
    let txn = time_op!(profile, begin_txn, {
        txn_storage.begin_txn(0, TxnOptions::default()).unwrap()
    });

    let w_id = input.w_id;
    let d_id = input.d_id;
    let mut c_id = input.c_id;
    let c_w_id = input.c_w_id;
    let c_d_id = input.c_d_id;
    let h_amount = input.h_amount;
    let c_last = &input.c_last;
    let by_last_name = input.by_last_name;

    // UpdateWarehouse
    let w_res = time_op!(profile, update_warehouse, {
        let w_key = WarehouseKey::create_key(w_id);
        txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::Warehouse],
            w_key.into_bytes(),
            |bytes| {
                let w = Warehouse::from_bytes_mut(bytes);
                w.w_ytd += h_amount;
            },
        )
    });
    if w_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // UpdateDistrict
    let d_res = time_op!(profile, update_district, {
        let d_key = DistrictKey::create_key(w_id, d_id);
        txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::District],
            d_key.into_bytes(),
            |bytes| {
                let d = unsafe { District::from_bytes_mut(bytes) };
                d.d_ytd += h_amount;
            },
        )
    });
    if d_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // GetCustomer (includes scan-by-last-name path + update)
    let gc_start = Instant::now();

    let c_key_res: Result<CustomerKey, ()> = if by_last_name {
        let mut customer_recs = Vec::new();
        let sec_key_start = CustomerSecondaryKey::create_key(c_w_id, c_d_id, c_last, 1);
        let sec_key_start_bytes = sec_key_start.into_bytes();
        let sec_key_end = CustomerSecondaryKey::create_key(c_w_id, c_d_id, c_last, u32::MAX);
        let sec_key_end_bytes = sec_key_end.into_bytes();
        let scan_res = txn_storage.scan_range(
            &txn,
            tbl_info[TPCCTable::CustomerSecondary],
            ScanOptions {
                lower_inc: sec_key_start_bytes.to_vec(),
                upper_exc: sec_key_end_bytes.to_vec(),
            },
        );
        if scan_res.is_err() {
            Err(())
        } else {
            let iter = scan_res.unwrap();
            let mut scan_failed = false;
            loop {
                match txn_storage.iter_next(&txn, &iter) {
                    Ok(Some((_, p_value))) => {
                        customer_recs.push(p_value);
                    }
                    Ok(None) => break,
                    Err(_) => {
                        scan_failed = true;
                        break;
                    }
                }
            }
            drop(iter);

            if scan_failed || customer_recs.is_empty() {
                Err(())
            } else {
                customer_recs.sort_by(|a, b| {
                    let a = unsafe { Customer::from_bytes(a) };
                    let b = unsafe { Customer::from_bytes(b) };
                    a.c_first.cmp(&b.c_first)
                });

                let c = unsafe {
                    Customer::from_bytes(
                        customer_recs[customer_recs.len().div_ceil(2) - 1].as_slice(),
                    )
                };
                Ok(CustomerKey::create_key_from_customer(c))
            }
        }
    } else {
        Ok(CustomerKey::create_key(c_w_id, c_d_id, c_id))
    };

    if c_key_res.is_err() {
        let gc_elapsed = gc_start.elapsed().as_nanos() as u64;
        profile.get_customer.record(gc_elapsed);
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    let c_key = c_key_res.unwrap();
    let c_res = txn_storage.update_value_with_func(
        &txn,
        tbl_info[TPCCTable::Customer],
        c_key.into_bytes(),
        |bytes| {
            let c = Customer::from_bytes_mut(bytes);
            c_id = c.c_id;
            c.c_balance -= h_amount;
            c.c_ytd_payment += h_amount;
            c.c_payment_cnt += 1;
            if c.c_credit.starts_with(b"BC") {
                // Modify customer data inline
                let new_data = format!(
                    "| {:4} {:2} {:4} {:2} {:4} ${:7.2}",
                    c.c_id, c_d_id, c_w_id, d_id, w_id, h_amount
                );
                let new_data_bytes = new_data.as_bytes();
                let len = new_data_bytes.len();
                c.c_data.copy_within(0..Customer::MAX_DATA - len, len);
                c.c_data[..len].copy_from_slice(new_data_bytes);
            }
        },
    );

    let gc_elapsed = gc_start.elapsed().as_nanos() as u64;
    profile.get_customer.record(gc_elapsed);

    if c_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // Commit
    let commit_res = time_op!(profile, commit, { txn_storage.commit_txn(&txn, false) });
    match commit_res {
        Ok(_) => {
            let total_elapsed = txn_start.elapsed().as_nanos() as u64;
            profile.total.record(total_elapsed);
            profile.num_commits += 1;
        }
        Err(_) => {
            txn_storage.abort_txn(&txn).unwrap();
            profile.num_aborts += 1;
        }
    }
}

// -- Run non-Payment transactions (standard TPC-C mix) -----------------------

fn run_other_txn<T: TxnStorageTrait, P: TPCCTxnProfile>(
    thread_id: usize,
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    stat: &mut TPCCStat,
    out: &mut TPCCOutput,
) {
    let w_id = if config.fixed_warehouse_per_thread {
        ((thread_id % config.num_warehouses as usize) + 1) as u16
    } else {
        urand_int(1, config.num_warehouses as u64) as u16
    };
    let mut attempts = 0usize;
    loop {
        let p = P::new(config, w_id);
        let status = p.run(config, txn_storage, tbl_info, stat, out);
        match status {
            TPCCStatus::Success | TPCCStatus::UserAbort | TPCCStatus::Bug(_) => break,
            TPCCStatus::SystemAbort => {
                std::thread::sleep(Duration::from_nanos(
                    gen_truncated_randomized_exponential_backoff(attempts),
                ));
                attempts += 1;
            }
        }
    }
}

// -- BP setup (same as tpcc.rs) ----------------------------------------------

pub fn get_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        use fbtree::bp::get_test_vmcache;
        get_test_vmcache::<false, 64>(num_frames)
    }
    #[cfg(feature = "bp_clock")]
    {
        use fbtree::bp::get_test_bp_clock;
        get_test_bp_clock::<64>(num_frames)
    }
    #[cfg(feature = "bp_pt2")]
    {
        use fbtree::bp::get_test_pt_two_hash;
        get_test_pt_two_hash(num_frames)
    }
    #[cfg(feature = "bp_pt2_bucket")]
    {
        use fbtree::bp::get_test_pt_two_hash_bucket_validate;
        get_test_pt_two_hash_bucket_validate(num_frames)
    }
    #[cfg(feature = "bp_pt4_bucket")]
    {
        use fbtree::bp::get_test_pt_fp_four_hash;
        return get_test_pt_fp_four_hash(num_frames);
    }
    #[cfg(feature = "bp_predicache")]
    {
        use fbtree::bp::get_test_predicache;
        get_test_predicache(num_frames)
    }
    #[cfg(feature = "bp_overflow")]
    {
        use fbtree::bp::get_test_overflow_bp;
        get_test_overflow_bp(num_frames)
    }
    #[cfg(not(any(
        feature = "vmcache",
        feature = "bp_clock",
        feature = "bp_predicache",
        feature = "bp_pt2",
        feature = "bp_pt2_bucket",
        feature = "bp_pt4_bucket",
        feature = "bp_overflow",
    )))]
    {
        use fbtree::bp::get_test_bp;
        get_test_bp(num_frames)
    }
}

// -- Main --------------------------------------------------------------------

pub fn main() {
    println!("=== TPC-C Payment Profiler ===");
    println!("Page size: {}", PAGE_SIZE);
    print_cfg_flags::print_cfg_flags();

    let config = TPCCConfig::parse();
    println!("Config: {:?}", config);

    let num_cores = get_total_cpus();
    if config.num_threads + 1 > num_cores {
        panic!(
            "Number of worker threads + main thread {} exceeds number of cores {}",
            config.num_threads, num_cores
        );
    }

    let num_frames = config.num_warehouses as usize * 1024 * 1024 * 1024 / PAGE_SIZE;
    println!(
        "BP size: {} GB",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024)
    );

    let bp = get_bp(num_frames);
    let txn_storage = NoWaitTxnStorage::new(&bp);
    let tbl_info = tpcc_gen_all_tables(&txn_storage, config.num_warehouses);
    tpcc_show_table_stats(&txn_storage, &tbl_info);
    println!("BP stats after load:\n{}", unsafe { bp.stats() });

    // Warmup
    if config.warmup_time > 0 {
        println!("\nWarming up for {} seconds...", config.warmup_time);
        let _ = with_affinity(get_total_cpus() - 1, || {
            run_tpcc(true, &config, &txn_storage, &tbl_info)
        })
        .unwrap();
        println!("BP stats after warmup:\n{}", unsafe { bp.stats() });
    }

    reset_macro_profile();

    // Run profiled Payment transactions
    println!(
        "\nProfiling Payment for {} seconds with {} threads, {} warehouses...",
        config.exec_time, config.num_threads, config.num_warehouses
    );

    let flag = AtomicBool::new(true);
    let run_barrier = Arc::new(Barrier::new(config.num_threads + 1));

    let profiles: Vec<PaymentProfile> = with_affinity(get_total_cpus() - 1, || {
        let current_cpu = get_current_cpu();
        println!("Main thread pinned to CPU {}", current_cpu);

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
                    let num_cores_minus_one = get_total_cpus() - 1;
                    if thread_id >= num_cores_minus_one {
                        panic!("Thread ID {} exceeds available cores", thread_id);
                    }
                    with_affinity(thread_id, || {
                        println!(
                            "Profiler thread {} pinned to CPU {}",
                            thread_id,
                            get_current_cpu()
                        );
                        let mut profile = PaymentProfile::new();
                        let mut stat = TPCCStat::new();
                        let mut out = TPCCOutput::new();

                        run_barrier.wait();

                        while flag_ref.load(Ordering::Acquire) {
                            let w_id = if config_ref.fixed_warehouse_per_thread {
                                ((thread_id % config_ref.num_warehouses as usize) + 1) as u16
                            } else {
                                urand_int(1, config_ref.num_warehouses as u64) as u16
                            };

                            let x = urand_int(1, 100);
                            if x <= 4 {
                                // StockLevel (4%)
                                run_other_txn::<_, StockLevelTxn>(
                                    thread_id,
                                    config_ref,
                                    txn_storage_ref,
                                    tbl_info_ref,
                                    &mut stat,
                                    &mut out,
                                );
                            } else if x <= 8 {
                                // Delivery (4%)
                                run_other_txn::<_, DeliveryTxn>(
                                    thread_id,
                                    config_ref,
                                    txn_storage_ref,
                                    tbl_info_ref,
                                    &mut stat,
                                    &mut out,
                                );
                            } else if x <= 12 {
                                // OrderStatus (4%)
                                run_other_txn::<_, OrderStatusTxn>(
                                    thread_id,
                                    config_ref,
                                    txn_storage_ref,
                                    tbl_info_ref,
                                    &mut stat,
                                    &mut out,
                                );
                            } else if x <= 55 {
                                // Payment (43%) -- profiled with retry
                                let mut attempts = 0usize;
                                loop {
                                    let before_commits = profile.num_commits;
                                    let before_aborts = profile.num_aborts;
                                    run_profiled_payment(
                                        config_ref,
                                        txn_storage_ref,
                                        tbl_info_ref,
                                        &mut profile,
                                        w_id,
                                    );
                                    if profile.num_commits > before_commits {
                                        break; // committed successfully
                                    }
                                    if profile.num_aborts == before_aborts {
                                        break; // no abort recorded -- should not happen for Payment but handle gracefully
                                    }
                                    // system abort -- backoff and retry
                                    std::thread::sleep(Duration::from_nanos(
                                        gen_truncated_randomized_exponential_backoff(attempts),
                                    ));
                                    attempts += 1;
                                }
                            } else {
                                // NewOrder (45%)
                                run_other_txn::<_, NewOrderTxn>(
                                    thread_id,
                                    config_ref,
                                    txn_storage_ref,
                                    tbl_info_ref,
                                    &mut stat,
                                    &mut out,
                                );
                            }
                        }
                        profile
                    })
                    .unwrap()
                });
                handlers.push(handler);
            }

            run_barrier.wait();
            std::thread::sleep(std::time::Duration::from_secs(config.exec_time));
            flag.store(false, Ordering::Release);

            handlers
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        })
    })
    .unwrap();

    // Merge all thread profiles
    let mut merged = PaymentProfile::new();
    for p in &profiles {
        merged.merge(p);
    }

    // Print per-thread summary
    println!("\n--- Per-Thread Summary ---");
    for (i, p) in profiles.iter().enumerate() {
        println!(
            "  Thread {}: commits={}, aborts={}, avg_txn={}",
            i,
            p.num_commits,
            p.num_aborts,
            format_ns(p.total.avg_ns()),
        );
    }

    // Print merged tree
    print_tree(&merged);

    println!("\nBP stats:\n{}", unsafe { bp.stats() });

    bp.print_profile();

    bp.clear_dirty_flags().unwrap();
}
