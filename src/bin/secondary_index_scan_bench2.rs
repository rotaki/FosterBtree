/// Benchmark: secondary-index scan via txn_storage2 (FieldLeveLStorageTrait).
///
/// Uses TransactionalStorage with the same schema as tpcc2 (order secondary
/// index). Loads N orders across warehouses/districts, then measures scan
/// throughput of `iter_for_each_fields` on the secondary index.
///
/// Compares:
///   1. `iter_for_each`        — raw byte-level iteration (no field deserialization)
///   2. `iter_for_each_fields` — field-level with prefetch + hint repair (secondary path)
///
/// Also measures warm hints (second scan after repair).
///
/// Usage:
///   cargo run --release --bin secondary_index_scan_bench2 -- -n 100000 -r 20 -s 100
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use fbtree::{
    bp::get_test_bp,
    txn_storage2::{
        field::{DataType, Field, Record, RecordPointer},
        field_level_storage_trait::{
            ContainerDS, ContainerOptions, DBOptions, FieldLeveLStorageTrait, ScanOptions,
            TxnOptions,
        },
        schema::Schema,
        transactional_storage::TransactionalStorage,
    },
};

#[derive(Parser, Debug)]
struct Args {
    /// Total number of entries to load
    #[arg(short = 'n', long, default_value_t = 100_000)]
    num_entries: u32,

    /// Number of scan repetitions (separate txn each)
    #[arg(short = 'r', long, default_value_t = 10)]
    repetitions: u32,

    /// Scan selectivity: number of entries per range scan (0 = full)
    #[arg(short = 's', long, default_value_t = 0)]
    scan_size: u32,

    /// BP size in pages
    #[arg(short = 'b', long, default_value_t = 50_000)]
    bp_pages: usize,

    /// Number of value columns in primary table (controls record width)
    #[arg(short = 'c', long, default_value_t = 5)]
    num_value_cols: usize,
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

// ── Schemas ─────────────────────────────────────────────────────────────────

/// Primary table: (pk: Uint32) + N value columns (Float64 each).
fn primary_schema(num_value_cols: usize) -> Schema {
    let mut cols = vec![(false, DataType::Uint32)]; // pk
    for _ in 0..num_value_cols {
        cols.push((false, DataType::Float64));
    }
    Schema::with_primary_key(cols, vec![0])
}

/// Secondary index: (sk_prefix: Uint16, sk_suffix: Uint32, pk: Uint32, pointer)
/// Key columns = [0,1,2], primary_key_col_indices = [2]
fn secondary_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Uint16),  // sk_prefix (e.g. warehouse/district)
            (false, DataType::Uint32),  // sk_suffix (e.g. customer_id — non-unique part)
            (false, DataType::Uint32),  // pk (order_id — makes it unique)
            (false, DataType::Pointer), // pointer to primary record
        ],
        vec![0, 1, 2], // all three form the secondary key
    )
}

// ── Benchmark routines ──────────────────────────────────────────────────────

fn bench_iter_for_each_raw<M: fbtree::bp::MemPool>(
    storage: &TransactionalStorage<M>,
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
                cols: vec![],
            },
        )
        .unwrap();

    let start = Instant::now();
    let mut count = 0u64;
    let _total = storage
        .iter_for_each(&txn, &iter, &mut |_key, _value, _hint| {
            count += 1;
            true
        })
        .unwrap();
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    storage.drop_iterator_handle(iter).unwrap();
    storage.commit_txn(&txn, false).unwrap();
    (count, elapsed_ns)
}

/// Secondary scan with inline primary dereference.
/// cols refers to primary schema columns — the new tpcc2 path.
fn bench_iter_for_each_fields_with_deref<M: fbtree::bp::MemPool>(
    storage: &TransactionalStorage<M>,
    db_id: u16,
    sec_cid: u16,
    lower: &[u8],
    upper: &[u8],
    deref_cols: &[usize],
) -> (u64, u64) {
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let iter = storage
        .scan_range(
            &txn,
            sec_cid,
            ScanOptions {
                lower_inc: lower.to_vec(),
                upper_exc: upper.to_vec(),
                cols: deref_cols.to_vec(),
            },
        )
        .unwrap();

    let start = Instant::now();
    let mut count = 0u64;
    let _total = storage
        .iter_for_each_fields(&txn, &iter, &mut |_key_fields, _val_fields, _hint| {
            count += 1;
            true
        })
        .unwrap();
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    storage.drop_iterator_handle(iter).unwrap();
    storage.commit_txn(&txn, false).unwrap();
    (count, elapsed_ns)
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();
    let n = args.num_entries;
    let reps = args.repetitions;
    let scan_size = if args.scan_size == 0 {
        n
    } else {
        args.scan_size
    };

    println!("=== Secondary Index Scan Benchmark (txn_storage2) ===");
    println!(
        "  entries: {}   scan_size: {}   reps: {}   value_cols: {}   bp_pages: {}",
        n, scan_size, reps, args.num_value_cols, args.bp_pages
    );
    println!();

    // ── Setup ────────────────────────────────────────────────────────────
    let bp = get_test_bp(args.bp_pages);
    let storage = TransactionalStorage::new(bp.clone());
    let db_id = storage.open_db(DBOptions::new("bench")).unwrap();

    let pri_schema = primary_schema(args.num_value_cols);
    let sec_schema = secondary_schema();

    let pri_cid = storage
        .create_container(
            db_id,
            ContainerOptions::new("primary", ContainerDS::BTree, pri_schema),
        )
        .unwrap();
    let sec_cid = storage
        .create_container(
            db_id,
            ContainerOptions::secondary(
                "secondary",
                ContainerDS::BTree,
                sec_schema,
                pri_cid,
                vec![2], // primary_key_col_indices: pk is at index 2 in secondary schema
            ),
        )
        .unwrap();

    // ── Load ─────────────────────────────────────────────────────────────
    print!("Loading {} entries... ", n);
    let load_start = Instant::now();
    {
        let chunk = 5000u32;
        let mut loaded = 0u32;
        while loaded < n {
            let end = (loaded + chunk).min(n);
            // Use raw_insert for loading (no txn overhead)
            for i in loaded..end {
                // Primary record: pk=i, then value columns
                let mut fields = vec![Field::Uint32(Some(i))];
                for c in 0..args.num_value_cols {
                    fields.push(Field::Float64(Some(i as f64 + c as f64 * 0.1)));
                }
                let pri_hint = storage
                    .raw_insert_record(db_id, pri_cid, Record { fields })
                    .unwrap();

                // Secondary record: (sk_prefix=1, sk_suffix=i/10, pk=i, pointer)
                let sk_prefix = 1u16; // single "warehouse"
                let sk_suffix = i / 10; // groups of 10
                let sec_record = Record {
                    fields: vec![
                        Field::Uint16(Some(sk_prefix)),
                        Field::Uint32(Some(sk_suffix)),
                        Field::Uint32(Some(i)),
                        Field::Pointer(Some(pri_hint)),
                    ],
                };
                storage
                    .raw_insert_record(db_id, sec_cid, sec_record)
                    .unwrap();
            }
            loaded = end;
        }
    }
    let load_elapsed = load_start.elapsed();
    println!("done in {:.2}s", load_elapsed.as_secs_f64());

    // ── Compute scan bounds ──────────────────────────────────────────────
    let start_idx = (n / 2).saturating_sub(scan_size / 2);
    let end_idx = (start_idx + scan_size).min(n);
    let expected = (end_idx - start_idx) as u64;

    // Build normalized key bounds using Field + ScanOptions::with_bounds
    let lower_fields = vec![
        Field::Uint16(Some(1)),
        Field::Uint32(Some(start_idx / 10)),
        Field::Uint32(Some(start_idx)),
    ];
    let upper_fields = vec![
        Field::Uint16(Some(1)),
        Field::Uint32(Some((end_idx - 1) / 10)),
        Field::Uint32(Some(end_idx)),
    ];

    let scan_opts = ScanOptions::new(&[3]).with_bounds(lower_fields, upper_fields);
    let lower = scan_opts.lower_inc.clone();
    let upper = scan_opts.upper_exc.clone();

    println!(
        "Scan range: idx {}..{}  (expect {} entries)",
        start_idx, end_idx, expected
    );
    println!();

    // Deref column indices: all value columns
    let deref_cols: Vec<usize> = (1..=args.num_value_cols).collect();

    // ── Benchmark: iter_for_each (raw bytes) ───────────────────────────
    println!("--- iter_for_each (raw bytes, no field deser, no primary deref) ---");
    let (total_count, total_ns) = run_bench(
        reps,
        expected,
        |lower, upper| bench_iter_for_each_raw(&storage, db_id, sec_cid, lower, upper),
        &lower,
        &upper,
    );
    print_stats("iter_for_each raw", reps, expected, total_count, total_ns);

    // ── Benchmark: iter_for_each_fields with inline deref (cold hints) ──
    println!("\n--- iter_for_each_fields + inline primary deref (cold hints) ---");
    let (total_count, total_ns) = run_bench(
        reps,
        expected,
        |lower, upper| {
            bench_iter_for_each_fields_with_deref(
                &storage,
                db_id,
                sec_cid,
                lower,
                upper,
                &deref_cols,
            )
        },
        &lower,
        &upper,
    );
    print_stats("inline deref cold", reps, expected, total_count, total_ns);

    // ── Benchmark: iter_for_each_fields with inline deref (warm hints) ──
    println!("\n--- iter_for_each_fields + inline primary deref (warm hints) ---");
    let (total_count, total_ns) = run_bench(
        reps,
        expected,
        |lower, upper| {
            bench_iter_for_each_fields_with_deref(
                &storage,
                db_id,
                sec_cid,
                lower,
                upper,
                &deref_cols,
            )
        },
        &lower,
        &upper,
    );
    print_stats("inline deref warm", reps, expected, total_count, total_ns);

    println!("\n=== Done ===");
}

fn run_bench(
    reps: u32,
    expected: u64,
    f: impl Fn(&[u8], &[u8]) -> (u64, u64),
    lower: &[u8],
    upper: &[u8],
) -> (u64, u64) {
    let mut total_ns = 0u64;
    let mut total_count = 0u64;
    for i in 0..reps {
        let (count, ns) = f(lower, upper);
        if i == 0 && expected > 0 {
            assert_eq!(
                count, expected,
                "scan returned {} rows, expected {}",
                count, expected
            );
        }
        total_count += count;
        total_ns += ns;
    }
    (total_count, total_ns)
}

fn print_stats(label: &str, reps: u32, expected: u64, total_count: u64, total_ns: u64) {
    let avg_ns = total_ns as f64 / reps as f64;
    let per_row = if expected > 0 {
        avg_ns / expected as f64
    } else {
        0.0
    };
    println!(
        "  [{}] avg/scan: {}   avg/row: {}   throughput: {}",
        label,
        format_ns(avg_ns),
        format_ns(per_row),
        format_throughput(total_count, total_ns),
    );
}
