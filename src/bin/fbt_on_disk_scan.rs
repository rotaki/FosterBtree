use clap::Parser;
use fbtree::{
    access_method::{fbt::FosterBtreeCursor, *},
    bench_utils::*,
    bp::MemPool,
    random::RandomKVs,
};
use rand::{rngs::SmallRng, RngCore, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn main() {
    let mut insert_params = BenchParams::parse();
    insert_params.ops_ratio = "1:0:0:0".to_string(); // Only insertions are done in this snipp

    let mut get_params = insert_params.clone();
    get_params.ops_ratio = "0:0:0:1".to_string(); // Only gets are done in this snippet

    println!("{}", insert_params);

    let bp_size = insert_params.bp_size;
    let tree = gen_foster_btree_on_disk(bp_size);

    let kvs = RandomKVs::new(
        insert_params.unique_keys,
        false,
        insert_params.num_threads,
        insert_params.num_keys,
        insert_params.key_size,
        insert_params.val_min_size,
        insert_params.val_max_size,
    );

    run_bench(insert_params, kvs, tree.clone());

    println!("After Insertion\n{}", unsafe { tree.mem_pool.stats() });
    tree.mem_pool.flush_all().unwrap();
    println!("After Flushing\n{}", unsafe { tree.mem_pool.stats() });
    unsafe { tree.mem_pool.reset_stats() };
    println!("After Resetting Stats\n{}", unsafe {
        tree.mem_pool.stats()
    });

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", tree.mem_pool.eviction_stats());
        println!("File stats: ");
        println!("{}", tree.mem_pool.file_stats());
        println!("Btree op stats: ");
        println!("{}", tree.op_stats());
        // println!("Btree page stats: ");
        // println!("{}", tree.page_stats(false));
    }

    // Random-start bounded range scan: each thread loops { pick random start
    // key, scan forward SCAN_SIZE records, drop cursor }, repeating for
    // EXEC_SECS. Uses cursor.for_each (no-copy, prefetched) to avoid per-tuple
    // Vec<u8> allocations.
    let scan_size: u64 = std::env::var("SCAN_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let exec_secs: u64 = std::env::var("EXEC_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);

    let total_kvs = AtomicU64::new(0);
    let total_scans = AtomicU64::new(0);
    let total_len = AtomicU64::new(0);
    let flag = AtomicBool::new(true);
    let n_scan_threads = get_params.num_threads.max(1);
    let key_size = get_params.key_size;
    let num_keys = get_params.num_keys;

    println!(
        "Starting random-start range-scan workload: threads={} scan_size={} exec_secs={}",
        n_scan_threads, scan_size, exec_secs
    );

    let scan_start = Instant::now();
    std::thread::scope(|s| {
        for tid in 0..n_scan_threads {
            let tree = tree.clone();
            let total_kvs = &total_kvs;
            let total_scans = &total_scans;
            let total_len = &total_len;
            let flag = &flag;
            s.spawn(move || {
                let mut rng = SmallRng::seed_from_u64(0x1234_5678 ^ tid as u64);
                let mut local_kvs: u64 = 0;
                let mut local_scans: u64 = 0;
                let mut local_len: u64 = 0;
                let mut start_key = vec![0u8; key_size];
                while flag.load(Ordering::Relaxed) {
                    // Fresh random start key matching the inserted-key layout
                    // (RandomKVs::new uses `usize.to_be_bytes() + zero padding`,
                    // so we generate the same shape with a random index in
                    // [0, num_keys)).
                    let key_idx = (rng.next_u64() as usize) % num_keys.max(1);
                    let idx_bytes = key_idx.to_be_bytes();
                    for b in start_key.iter_mut() {
                        *b = 0;
                    }
                    let take = idx_bytes.len().min(key_size);
                    start_key[..take].copy_from_slice(&idx_bytes[..take]);
                    let mut cursor = FosterBtreeCursor::new(&tree, &start_key, &[]);
                    let mut count: u64 = 0;
                    cursor.for_each(|k, v| {
                        count += 1;
                        local_len += (k.len() + v.len()) as u64;
                        // black_box the first byte of value so the read isn't
                        // optimized out.
                        std::hint::black_box(v.first().copied().unwrap_or(0));
                        count < scan_size
                    });
                    local_kvs += count;
                    local_scans += 1;
                }
                total_kvs.fetch_add(local_kvs, Ordering::Relaxed);
                total_scans.fetch_add(local_scans, Ordering::Relaxed);
                total_len.fetch_add(local_len, Ordering::Relaxed);
            });
        }
        std::thread::sleep(Duration::from_secs(exec_secs));
        flag.store(false, Ordering::Relaxed);
    });
    let scan_elapsed = scan_start.elapsed();
    let total_kvs = total_kvs.load(Ordering::Relaxed);
    let total_scans = total_scans.load(Ordering::Relaxed);
    let total_len = total_len.load(Ordering::Relaxed) as usize;
    let kvs_per_sec = total_kvs as f64 / scan_elapsed.as_secs_f64();
    let scans_per_sec = total_scans as f64 / scan_elapsed.as_secs_f64();
    println!(
        "BENCH_SCAN_RESULT total_kvs={} total_scans={} elapsed_s={:.3} kvs_per_sec={:.0} ({:.2} M kvs/s) scans_per_sec={:.0} ns_per_kv={:.1} threads={} scan_size={}",
        total_kvs,
        total_scans,
        scan_elapsed.as_secs_f64(),
        kvs_per_sec,
        kvs_per_sec / 1_000_000.0,
        scans_per_sec,
        scan_elapsed.as_nanos() as f64 / total_kvs.max(1) as f64,
        n_scan_threads,
        scan_size,
    );

    println!("After Scan\n{}", unsafe { tree.mem_pool.stats() });
    tree.mem_pool.flush_all().unwrap();
    println!("After Flushing\n{}", unsafe { tree.mem_pool.stats() });
    unsafe { tree.mem_pool.reset_stats() };
    println!("After Resetting Stats\n{}", unsafe {
        tree.mem_pool.stats()
    });

    println!("Total length of keys and values: {}", total_len);

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", tree.mem_pool.eviction_stats());
        println!("File stats: ");
        println!("{}", tree.mem_pool.file_stats());
        println!("Btree op stats: ");
        println!("{}", tree.op_stats());
        println!("Btree page stats: ");
        println!("{}", tree.page_stats(false));
    }
}
