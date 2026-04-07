/// Profiling binary for TPC-C NewOrder transaction.
/// Runs the FULL TPC-C workload mix (45% NewOrder, 43% Payment, 4% OrderStatus,
/// 4% Delivery, 4% StockLevel) but instruments only NewOrder with nanosecond-precision
/// per-operation timing. This ensures realistic contention patterns.
///
/// Usage:
///   cargo run --release --bin tpcc_profile_neworder --features bp_clock -- -w 2 -t 4
///   cargo run --release --bin tpcc_profile_neworder --features bp_dashmap -- -w 2 -t 4
///   cargo run --release --bin tpcc_profile_neworder --features bp_pt -- -w 2 -t 4
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
use memchr::memmem;

// ── Per-operation timing accumulator ────────────────────────────────────────

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
struct NewOrderProfile {
    total: OpTiming,
    begin_txn: OpTiming,
    get_warehouse: OpTiming,
    update_district: OpTiming,
    get_customer: OpTiming,
    insert_neworder: OpTiming,
    insert_order: OpTiming,
    insert_order_secondary: OpTiming,
    get_item: OpTiming,
    update_stock: OpTiming,
    insert_orderline: OpTiming,
    commit: OpTiming,
    // Aggregate for the full order-line loop
    orderline_loop: OpTiming,
    num_commits: u64,
    num_aborts: u64,
}

impl NewOrderProfile {
    fn new() -> Self {
        NewOrderProfile {
            total: OpTiming::new(),
            begin_txn: OpTiming::new(),
            get_warehouse: OpTiming::new(),
            update_district: OpTiming::new(),
            get_customer: OpTiming::new(),
            insert_neworder: OpTiming::new(),
            insert_order: OpTiming::new(),
            insert_order_secondary: OpTiming::new(),
            get_item: OpTiming::new(),
            update_stock: OpTiming::new(),
            insert_orderline: OpTiming::new(),
            commit: OpTiming::new(),
            orderline_loop: OpTiming::new(),
            num_commits: 0,
            num_aborts: 0,
        }
    }

    fn merge(&mut self, other: &NewOrderProfile) {
        self.total.merge(&other.total);
        self.begin_txn.merge(&other.begin_txn);
        self.get_warehouse.merge(&other.get_warehouse);
        self.update_district.merge(&other.update_district);
        self.get_customer.merge(&other.get_customer);
        self.insert_neworder.merge(&other.insert_neworder);
        self.insert_order.merge(&other.insert_order);
        self.insert_order_secondary
            .merge(&other.insert_order_secondary);
        self.get_item.merge(&other.get_item);
        self.update_stock.merge(&other.update_stock);
        self.insert_orderline.merge(&other.insert_orderline);
        self.commit.merge(&other.commit);
        self.orderline_loop.merge(&other.orderline_loop);
        self.num_commits += other.num_commits;
        self.num_aborts += other.num_aborts;
    }
}

// ── Formatting helpers ──────────────────────────────────────────────────────

fn format_ns(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:>10.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:>10.2} µs", ns / 1_000.0)
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

fn print_tree(profile: &NewOrderProfile) {
    let total_avg = profile.total.avg_ns();
    println!();
    println!(
        "NewOrder Transaction Profile (commits: {}, aborts: {})",
        profile.num_commits, profile.num_aborts
    );
    println!("================================================================================");
    print_op("", "Total Transaction", &profile.total, total_avg);
    println!("│");
    print_op("├── ", "BeginTxn", &profile.begin_txn, total_avg);
    print_op("├── ", "GetWarehouse", &profile.get_warehouse, total_avg);
    print_op(
        "├── ",
        "UpdateDistrict",
        &profile.update_district,
        total_avg,
    );
    print_op("├── ", "GetCustomer", &profile.get_customer, total_avg);
    print_op(
        "├── ",
        "InsertNewOrder",
        &profile.insert_neworder,
        total_avg,
    );
    print_op("├── ", "InsertOrder", &profile.insert_order, total_avg);
    print_op(
        "├── ",
        "InsertOrderSecondary",
        &profile.insert_order_secondary,
        total_avg,
    );
    println!("│");
    print_op(
        "├── ",
        "OrderLineLoop (total)",
        &profile.orderline_loop,
        total_avg,
    );
    println!("│   │");
    print_op("│   ├── ", "GetItem", &profile.get_item, total_avg);
    print_op("│   ├── ", "UpdateStock", &profile.update_stock, total_avg);
    print_op(
        "│   └── ",
        "InsertOrderLine",
        &profile.insert_orderline,
        total_avg,
    );
    println!("│");
    print_op("└── ", "Commit", &profile.commit, total_avg);
    println!("================================================================================");
}

// ── Instrumented NewOrder execution ─────────────────────────────────────────

macro_rules! time_op {
    ($profile:expr, $field:ident, $body:expr) => {{
        let __start = Instant::now();
        let __result = $body;
        let __elapsed = __start.elapsed().as_nanos() as u64;
        $profile.$field.record(__elapsed);
        __result
    }};
}

fn run_profiled_neworder<T: TxnStorageTrait>(
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    profile: &mut NewOrderProfile,
    w_id: u16,
) {
    let input = NewOrderTxnInput::new(config, w_id);
    let txn_start = Instant::now();

    // Begin transaction
    let txn = time_op!(profile, begin_txn, {
        txn_storage.begin_txn(0, TxnOptions::default()).unwrap()
    });

    let w_id = input.w_id;
    let d_id = input.d_id;
    let c_id = input.c_id;
    let ol_cnt = input.ol_cnt;
    let is_remote = input.is_remote;

    // GetWarehouse
    let w_res = time_op!(profile, get_warehouse, {
        let w_key = WarehouseKey::create_key(w_id);
        txn_storage.get_value(&txn, tbl_info[TPCCTable::Warehouse], w_key.into_bytes())
    });
    if w_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }
    let mut w_bytes = w_res.unwrap();
    let w = Warehouse::from_bytes_mut(&mut w_bytes);
    let w_tax = w.w_tax;

    // UpdateDistrict
    let mut o_id = 0u32;
    let mut d_tax = 0.0f64;
    let d_res = time_op!(profile, update_district, {
        let d_key = DistrictKey::create_key(w_id, d_id);
        txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::District],
            d_key.into_bytes(),
            |bytes| {
                let d = unsafe { District::from_bytes_mut(bytes) };
                o_id = d.d_next_o_id;
                d.d_next_o_id += 1;
                d_tax = d.d_tax;
            },
        )
    });
    if d_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // GetCustomer
    let c_res = time_op!(profile, get_customer, {
        let c_key = CustomerKey::create_key(w_id, d_id, c_id);
        txn_storage.get_value(&txn, tbl_info[TPCCTable::Customer], c_key.into_bytes())
    });
    if c_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }
    let c_bytes = c_res.unwrap();
    let c = unsafe { Customer::from_bytes(&c_bytes) };
    let c_discount = c.c_discount;

    // InsertNewOrder
    let no_res = time_op!(profile, insert_neworder, {
        let no_key = NewOrderKey::create_key(w_id, d_id, o_id);
        let mut no = NewOrder::new();
        no.no_w_id = w_id;
        no.no_d_id = d_id;
        no.no_o_id = o_id;
        txn_storage.insert_value(
            &txn,
            tbl_info[TPCCTable::NewOrder],
            no_key.into_bytes().to_vec(),
            no.as_bytes().to_vec(),
        )
    });
    if no_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // InsertOrder
    let o_key = OrderKey::create_key(w_id, d_id, o_id);
    let o_res = time_op!(profile, insert_order, {
        let mut o_rec = Order::new();
        o_rec.o_w_id = w_id;
        o_rec.o_d_id = d_id;
        o_rec.o_c_id = c_id;
        o_rec.o_id = o_id;
        o_rec.o_carrier_id = 0;
        o_rec.o_ol_cnt = ol_cnt;
        o_rec.o_all_local = !is_remote as u8;
        o_rec.o_entry_d = get_timestamp();
        txn_storage.insert_value(
            &txn,
            tbl_info[TPCCTable::Order],
            o_key.into_bytes().to_vec(),
            o_rec.as_bytes().to_vec(),
        )
    });
    if o_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // InsertOrderSecondary
    let os_res = time_op!(profile, insert_order_secondary, {
        let o_idx_key = OrderSecondaryKey::create_key(w_id, d_id, c_id, o_id);
        txn_storage.insert_value(
            &txn,
            tbl_info[TPCCTable::OrderSecondary],
            o_idx_key.into_bytes().to_vec(),
            o_key.into_bytes().to_vec(),
        )
    });
    if os_res.is_err() {
        txn_storage.abort_txn(&txn).unwrap();
        profile.num_aborts += 1;
        return;
    }

    // OrderLine loop
    let ol_loop_start = Instant::now();
    let mut _total = 0.0f64;

    for ol_num_idx in 0..input.items.len() {
        let ol_num = (ol_num_idx + 1) as u8;
        let item = &input.items[ol_num_idx];
        let ol_supply_w_id = item.ol_supply_w_id;
        let ol_i_id = item.ol_i_id;
        let ol_quantity = item.ol_quantity;

        if ol_i_id == Item::UNUSED_ID {
            txn_storage.abort_txn(&txn).unwrap();
            // User abort (1% rollback) - don't count as system abort
            return;
        }

        // GetItem
        let i_res = time_op!(profile, get_item, {
            let i_key = ItemKey::create_key(ol_i_id);
            txn_storage.get_value(&txn, tbl_info[TPCCTable::Item], i_key.into_bytes())
        });
        if i_res.is_err() {
            txn_storage.abort_txn(&txn).unwrap();
            profile.num_aborts += 1;
            return;
        }
        let i_bytes = i_res.unwrap();
        let i_rec = unsafe { Item::from_bytes(&i_bytes) };

        let ol_amount = ol_quantity as f64 * i_rec.i_price;
        _total += ol_amount;

        // UpdateStock
        let mut ol = OrderLine::new();
        let s_res = time_op!(profile, update_stock, {
            let s_key = StockKey::create_key(ol_supply_w_id, ol_i_id);
            txn_storage.update_value_with_func(
                &txn,
                tbl_info[TPCCTable::Stock],
                s_key.into_bytes(),
                |bytes| {
                    let s = unsafe { Stock::from_bytes_mut(bytes) };
                    if memmem::find(&s.s_data, b"ORIGINAL").is_some()
                        && memmem::find(&i_rec.i_data, b"ORIGINAL").is_some()
                    {
                        // brand_generic = 'B';
                    }
                    // modify stock
                    if s.s_quantity > ol_quantity as i16 + 10 {
                        s.s_quantity -= ol_quantity as i16;
                    } else {
                        s.s_quantity = s.s_quantity - ol_quantity as i16 + 91;
                    }
                    s.s_ytd += ol_quantity as u32;
                    s.s_order_cnt += 1;
                    if is_remote {
                        s.s_remote_cnt += 1;
                    }
                    // create orderline
                    ol.ol_w_id = w_id;
                    ol.ol_d_id = d_id;
                    ol.ol_o_id = o_id;
                    ol.ol_number = ol_num;
                    ol.ol_i_id = ol_i_id;
                    ol.ol_supply_w_id = ol_supply_w_id;
                    ol.ol_delivery_d = 0;
                    ol.ol_quantity = ol_quantity;
                    ol.ol_amount = ol_amount;
                    ol.ol_dist_info
                        .copy_from_slice(&s.s_dist[(d_id - 1) as usize]);
                },
            )
        });
        if s_res.is_err() {
            txn_storage.abort_txn(&txn).unwrap();
            profile.num_aborts += 1;
            return;
        }

        // InsertOrderLine
        let ol_res = time_op!(profile, insert_orderline, {
            let ol_key = OrderLineKey::create_key(w_id, d_id, o_id, ol_num);
            txn_storage.insert_value(
                &txn,
                tbl_info[TPCCTable::OrderLine],
                ol_key.into_bytes().to_vec(),
                ol.as_bytes().to_vec(),
            )
        });
        if ol_res.is_err() {
            txn_storage.abort_txn(&txn).unwrap();
            profile.num_aborts += 1;
            return;
        }
    }

    let ol_loop_elapsed = ol_loop_start.elapsed().as_nanos() as u64;
    profile.orderline_loop.record(ol_loop_elapsed);

    _total *= (1.0 - c_discount) * (1.0 + w_tax + d_tax);

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

// ── Run non-NewOrder transactions (standard TPC-C mix) ──────────────────────

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

// ── BP setup (same as tpcc.rs) ──────────────────────────────────────────────

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
    #[cfg(feature = "bp_pt_bucket")]
    {
        use fbtree::bp::get_test_pt_bucket_validate;
        get_test_pt_bucket_validate(num_frames)
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
    #[cfg(feature = "bp_pt")]
    {
        use fbtree::bp::get_test_pt;
        get_test_pt(num_frames)
    }
    #[cfg(feature = "bp_overflow")]
    {
        use fbtree::bp::get_test_overflow_bp;
        get_test_overflow_bp(num_frames)
    }
    #[cfg(feature = "bp_open_addressing")]
    {
        use fbtree::bp::get_test_open_addressing_bp;
        get_test_open_addressing_bp(num_frames)
    }
    #[cfg(feature = "bp_dashmap")]
    {
        use fbtree::bp::get_test_dashmap_bp;
        get_test_dashmap_bp(num_frames)
    }
    #[cfg(feature = "bp_hashmap")]
    {
        use fbtree::bp::get_test_hashmap_bp;
        get_test_hashmap_bp(num_frames)
    }
    #[cfg(not(any(
        feature = "vmcache",
        feature = "bp_clock",
        feature = "bp_pt",
        feature = "bp_pt2",
        feature = "bp_pt_bucket",
        feature = "bp_pt2_bucket",
        feature = "bp_pt4_bucket",
        feature = "bp_overflow",
        feature = "bp_open_addressing",
        feature = "bp_dashmap",
        feature = "bp_hashmap"
    )))]
    {
        use fbtree::bp::get_test_bp;
        get_test_bp(num_frames)
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

pub fn main() {
    println!("=== TPC-C NewOrder Profiler ===");
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

    let num_frames = if config.bp_size > 0 {
        config.bp_size * 1024 * 1024 * 1024 / PAGE_SIZE
    } else {
        config.num_warehouses as usize * 1024 * 1024 * 1024 / PAGE_SIZE
    };
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

    // Run profiled NewOrder transactions
    println!(
        "\nProfiling NewOrder for {} seconds with {} threads, {} warehouses...",
        config.exec_time, config.num_threads, config.num_warehouses
    );

    let flag = AtomicBool::new(true);
    let run_barrier = Arc::new(Barrier::new(config.num_threads + 1));

    let profiles: Vec<NewOrderProfile> = with_affinity(get_total_cpus() - 1, || {
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
                        let mut profile = NewOrderProfile::new();
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
                                // Payment (43%)
                                run_other_txn::<_, PaymentTxn>(
                                    thread_id,
                                    config_ref,
                                    txn_storage_ref,
                                    tbl_info_ref,
                                    &mut stat,
                                    &mut out,
                                );
                            } else {
                                // NewOrder (45%) — profiled with retry
                                let mut attempts = 0usize;
                                loop {
                                    let before_commits = profile.num_commits;
                                    let before_aborts = profile.num_aborts;
                                    run_profiled_neworder(
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
                                        break; // user abort (1% rollback) — no retry
                                    }
                                    // system abort — backoff and retry
                                    std::thread::sleep(Duration::from_nanos(
                                        gen_truncated_randomized_exponential_backoff(attempts),
                                    ));
                                    attempts += 1;
                                }
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
    let mut merged = NewOrderProfile::new();
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
