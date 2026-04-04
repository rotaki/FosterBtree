/// Benchmark: secondary-index scan via transactional storage layer.
///
/// Loads a primary + secondary index pair with N entries, then measures
/// scan throughput through the full txn path (locking, rwset, primary
/// dereference, hint repair).
///
/// Compares two paths:
///   1. `iter_next`      — per-key cursor iteration (old path)
///   2. `iter_for_each`  — page-at-a-time with batch prefetch + hint repair
///
/// Also measures a "warm hints" run (second scan after repair) to quantify
/// the benefit of hint correction.
///
/// Usage:
///   cargo run --release --bin secondary_index_scan_bench -- -n 100000 -r 20 -s 100
///   cargo run --release --bin secondary_index_scan_bench --features bp_pt -- -n 500000
use std::time::Instant;

use clap::Parser;
use fbtree::{bp::get_test_bp, prelude::*, txn_storage::NoWaitTxnStorage};

#[derive(Parser, Debug)]
struct Args {
    /// Total number of entries to load
    #[arg(short = 'n', long, default_value_t = 100_000)]
    num_entries: u32,

    /// Number of scan repetitions (separate txn each)
    #[arg(short = 'r', long, default_value_t = 10)]
    repetitions: u32,

    /// Scan selectivity: number of entries per range scan
    #[arg(short = 's', long, default_value_t = 0)]
    scan_size: u32,

    /// BP size in pages
    #[arg(short = 'b', long, default_value_t = 50_000)]
    bp_pages: usize,

    /// Value size in bytes for primary records
    #[arg(short = 'v', long, default_value_t = 200)]
    value_size: usize,
}

// ── Formatting ──────────────────────────────────────────────────────────────

fn format_ns(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:>10.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:>10.2} µs", ns / 1_000.0)
    } else {
        format!("{:>10.0} ns", ns)
    }
}

fn format_throughput(count: u64, elapsed_ns: u64) -> String {
    if elapsed_ns == 0 {
        return "N/A".to_string();
    }
    let per_sec = count as f64 / (elapsed_ns as f64 / 1_000_000_000.0);
    if per_sec >= 1_000_000.0 {
        format!("{:.2} M ops/s", per_sec / 1_000_000.0)
    } else if per_sec >= 1_000.0 {
        format!("{:.2} K ops/s", per_sec / 1_000.0)
    } else {
        format!("{:.0} ops/s", per_sec)
    }
}

// ── Benchmark routines ──────────────────────────────────────────────────────

fn bench_iter_next<M: fbtree::bp::MemPool>(
    storage: &NoWaitTxnStorage<M>,
    db_id: u16,
    sec_cid: u16,
    lower: &[u8],
    upper: &[u8],
) -> (u64, u64) {
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let iter = storage
        .scan_range(
            &txn,
            sec_cid,
            ScanOptions {
                lower_inc: lower.to_vec(),
                upper_exc: upper.to_vec(),
            },
        )
        .unwrap();

    let start = Instant::now();
    let mut count = 0u64;
    loop {
        match storage.iter_next(&txn, &iter) {
            Ok(Some((_key, _value))) => {
                count += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    drop(iter);
    storage.commit_txn(&txn, false).unwrap();
    (count, elapsed_ns)
}

fn bench_iter_for_each<M: fbtree::bp::MemPool>(
    storage: &NoWaitTxnStorage<M>,
    db_id: u16,
    sec_cid: u16,
    lower: &[u8],
    upper: &[u8],
) -> (u64, u64) {
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let iter = storage
        .scan_range(
            &txn,
            sec_cid,
            ScanOptions {
                lower_inc: lower.to_vec(),
                upper_exc: upper.to_vec(),
            },
        )
        .unwrap();

    let start = Instant::now();
    let mut count = 0u64;
    let total = storage
        .iter_for_each(&txn, &iter, &mut |_key, _value| {
            count += 1;
            true
        })
        .unwrap();
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    drop(iter);
    storage.commit_txn(&txn, false).unwrap();
    assert_eq!(count, total);
    (count, elapsed_ns)
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();
    let n = args.num_entries;
    let reps = args.repetitions;
    // 0 means full scan
    let scan_size = if args.scan_size == 0 {
        n
    } else {
        args.scan_size
    };

    println!("=== Secondary Index Scan Benchmark ===");
    println!(
        "  entries: {}   scan_size: {}   reps: {}   value_size: {}B   bp_pages: {}",
        n, scan_size, reps, args.value_size, args.bp_pages
    );
    println!();

    // ── Setup ────────────────────────────────────────────────────────────
    let bp = get_test_bp(args.bp_pages);
    let storage = NoWaitTxnStorage::new(&bp);
    let db_id = storage.open_db(DBOptions::new("bench")).unwrap();
    let pri_cid = storage
        .create_container(
            db_id,
            ContainerOptions::primary("primary", ContainerDS::BTree),
        )
        .unwrap();
    let sec_cid = storage
        .create_container(
            db_id,
            ContainerOptions::secondary("secondary", ContainerDS::BTree, pri_cid),
        )
        .unwrap();

    // ── Load ─────────────────────────────────────────────────────────────
    print!("Loading {} entries... ", n);
    let load_start = Instant::now();
    {
        // Batch inserts in chunks to avoid enormous rwsets.
        let chunk = 5000u32;
        let mut loaded = 0u32;
        while loaded < n {
            let end = (loaded + chunk).min(n);
            let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            for i in loaded..end {
                let pk = format!("pk{:08}", i).into_bytes();
                // Value = fixed-size payload
                let mut val = format!("val{:08}", i).into_bytes();
                val.resize(args.value_size, b'x');
                let sk = format!("sk{:08}", i).into_bytes();
                storage
                    .insert_value(&txn, pri_cid, pk.clone(), val)
                    .unwrap();
                storage.insert_value(&txn, sec_cid, sk, pk).unwrap();
            }
            storage.commit_txn(&txn, false).unwrap();
            loaded = end;
        }
    }
    let load_elapsed = load_start.elapsed();
    println!("done in {:.2}s", load_elapsed.as_secs_f64());

    // ── Compute scan bounds ──────────────────────────────────────────────
    // Scan a window of `scan_size` entries starting from a random offset.
    // Using the middle of the key space for reproducibility.
    let start_idx = (n / 2).saturating_sub(scan_size / 2);
    let end_idx = (start_idx + scan_size).min(n);
    let lower = format!("sk{:08}", start_idx).into_bytes();
    let upper = format!("sk{:08}", end_idx).into_bytes();
    let expected = (end_idx - start_idx) as u64;

    println!(
        "Scan range: sk{:08}..sk{:08}  (expect {} entries)",
        start_idx, end_idx, expected
    );
    println!();

    // ── Benchmark: iter_next (cold hints) ────────────────────────────────
    println!("--- iter_next (per-key cursor) ---");
    let mut total_ns = 0u64;
    let mut total_count = 0u64;
    for i in 0..reps {
        let (count, ns) = bench_iter_next(&storage, db_id, sec_cid, &lower, &upper);
        if i == 0 {
            assert_eq!(
                count, expected,
                "iter_next returned {} rows, expected {}",
                count, expected
            );
        }
        total_count += count;
        total_ns += ns;
    }
    let avg_ns = total_ns as f64 / reps as f64;
    let per_row = if expected > 0 {
        avg_ns / expected as f64
    } else {
        0.0
    };
    println!(
        "  avg/scan: {}   avg/row: {}   throughput: {}",
        format_ns(avg_ns),
        format_ns(per_row),
        format_throughput(total_count, total_ns),
    );

    // ── Benchmark: iter_for_each — first run repairs hints ───────────────
    println!("\n--- iter_for_each (batch, cold hints) ---");
    let mut total_ns = 0u64;
    let mut total_count = 0u64;
    for i in 0..reps {
        let (count, ns) = bench_iter_for_each(&storage, db_id, sec_cid, &lower, &upper);
        if i == 0 {
            assert_eq!(
                count, expected,
                "iter_for_each returned {} rows, expected {}",
                count, expected
            );
        }
        total_count += count;
        total_ns += ns;
    }
    let avg_ns = total_ns as f64 / reps as f64;
    let per_row = if expected > 0 {
        avg_ns / expected as f64
    } else {
        0.0
    };
    println!(
        "  avg/scan: {}   avg/row: {}   throughput: {}",
        format_ns(avg_ns),
        format_ns(per_row),
        format_throughput(total_count, total_ns),
    );

    // ── Benchmark: iter_for_each — second run (hints should be warm) ────
    println!("\n--- iter_for_each (batch, warm hints) ---");
    let mut total_ns = 0u64;
    let mut total_count = 0u64;
    for _ in 0..reps {
        let (count, ns) = bench_iter_for_each(&storage, db_id, sec_cid, &lower, &upper);
        total_count += count;
        total_ns += ns;
    }
    let avg_ns = total_ns as f64 / reps as f64;
    let per_row = if expected > 0 {
        avg_ns / expected as f64
    } else {
        0.0
    };
    println!(
        "  avg/scan: {}   avg/row: {}   throughput: {}",
        format_ns(avg_ns),
        format_ns(per_row),
        format_throughput(total_count, total_ns),
    );

    // ── Benchmark: iter_next — warm hints (benefit from prior repair) ────
    println!("\n--- iter_next (per-key cursor, warm hints) ---");
    let mut total_ns = 0u64;
    let mut total_count = 0u64;
    for _ in 0..reps {
        let (count, ns) = bench_iter_next(&storage, db_id, sec_cid, &lower, &upper);
        total_count += count;
        total_ns += ns;
    }
    let avg_ns = total_ns as f64 / reps as f64;
    let per_row = if expected > 0 {
        avg_ns / expected as f64
    } else {
        0.0
    };
    println!(
        "  avg/scan: {}   avg/row: {}   throughput: {}",
        format_ns(avg_ns),
        format_ns(per_row),
        format_throughput(total_count, total_ns),
    );

    println!("\n=== Done ===");
}
