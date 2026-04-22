//! Fast-path coverage / scarcity micro-benchmark.
//!
//! See plan: PT strength/weakness study, Part B2.
//!
//! We claim that PT(FP)'s fast path is a scarce hash-placement privilege:
//! only one of k hot pages mapping to the same preferred slot can enjoy the
//! zero-translation fast path at a time, while TLB/LIPAH-style designs let
//! many hot pages simultaneously retain a stable fast path.
//!
//! To make the weakness visible without it being drowned out by a noisy
//! p=1/512 promotion coin, we measure **coverage** (fast-path hits /
//! total hot-path touches) rather than throughput.
//!
//! Workload
//!   1. Create `--num-pages` pages (single container).
//!   2. Pre-compute the **PT preferred slot** (pt_preferred_slot) of every
//!      created page id. Group page ids by slot.
//!   3. Two modes:
//!      * default (conflict): pick `--hot-sets` slots each owning ≥ k pages,
//!        and take the first k pages of each slot. Hot set size = k * hot_sets.
//!        Access pattern is strict round-robin over the hot set so the PT
//!        fast path is forced to pick a winner per slot.
//!      * `--no-conflict`: pick `k * hot_sets` pages from `k * hot_sets`
//!        distinct slots. Same size, no contention — PT(FP), TLB, LIPAH
//!        should all land on their fast path.
//!   4. N worker threads round-robin the hot set (each starts at a different
//!      offset). Each op records latency into an hdr histogram (when enabled).
//!
//! The coverage line printed by each BP (`fast_path_coverage: X.YYYY ...`) is
//! parsed by bench_pt_strength_weakness.sh.
//!
//! Usage:
//!   cargo run --release --bin pt_fastpath_coverage \
//!     --features "bp_pt_bucket_v2,pt_counts" \
//!     -- --num-frames 200000 --num-pages 800000 \
//!        --collision-width 4 --hot-sets 256 --threads 12 --seconds 10

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use clap::Parser;
use fbtree::bp::{pt_preferred_slot, ContainerKey, MemPool, PageFrameKey};
use hdrhistogram::Histogram;

#[derive(Parser, Debug)]
struct Args {
    /// Buffer-pool frame count.
    #[arg(long, default_value_t = 200_000)]
    num_frames: usize,

    /// Pages pre-created. Needs to be several × num_frames so we can find
    /// enough slots with `>= collision_width` pages mapping to them.
    #[arg(long, default_value_t = 800_000)]
    num_pages: usize,

    /// Collision width `k`: number of pages per hot slot (for the conflict
    /// mode) or per-distinct-slot (for no-conflict). k=1 recovers the
    /// uncontended case.
    #[arg(long, default_value_t = 4)]
    collision_width: usize,

    /// Number of hot slots to use. Total hot set = collision_width * hot_sets.
    #[arg(long, default_value_t = 256)]
    hot_sets: usize,

    /// Worker threads.
    #[arg(long, default_value_t = 12)]
    threads: usize,

    /// Measurement window (seconds).
    #[arg(long, default_value_t = 10)]
    seconds: u64,

    /// Warmup window (seconds).
    #[arg(long, default_value_t = 3)]
    warmup: u64,

    /// Pick `k * hot_sets` pages from distinct slots (no PT collisions). Keeps
    /// hot set size the same as the conflict run for a clean control.
    #[arg(long, default_value_t = false)]
    no_conflict: bool,

    /// Record per-op latency histogram. Adds ~20ns overhead per op.
    #[arg(long, default_value_t = false)]
    latency: bool,
}

fn get_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "bp_clock_v2")]
    {
        use fbtree::bp::get_test_bp_clock_v2;
        return get_test_bp_clock_v2::<64>(num_frames);
    }
    #[cfg(feature = "bp_pt_bucket_v2_ophash")]
    {
        use fbtree::bp::get_test_pt_bucket_validate_v2_ophash;
        return get_test_pt_bucket_validate_v2_ophash(num_frames);
    }
    #[cfg(all(feature = "bp_pt_bucket_v2", not(feature = "bp_pt_bucket_v2_ophash")))]
    {
        use fbtree::bp::get_test_pt_bucket_validate_v2;
        return get_test_pt_bucket_validate_v2(num_frames);
    }
    #[cfg(feature = "bp_pt_v2_ophash")]
    {
        use fbtree::bp::get_test_pt_v2_ophash;
        return get_test_pt_v2_ophash(num_frames);
    }
    #[cfg(all(feature = "bp_pt_v2", not(feature = "bp_pt_v2_ophash")))]
    {
        use fbtree::bp::get_test_pt_v2;
        return get_test_pt_v2(num_frames);
    }
    #[cfg(feature = "bp_tlb_v2")]
    {
        use fbtree::bp::get_test_tlb_bp_v2;
        return get_test_tlb_bp_v2(num_frames);
    }
    #[cfg(not(any(
        feature = "bp_clock_v2",
        feature = "bp_pt_v2",
        feature = "bp_pt_v2_ophash",
        feature = "bp_pt_bucket_v2",
        feature = "bp_pt_bucket_v2_ophash",
        feature = "bp_tlb_v2",
    )))]
    {
        use fbtree::bp::get_test_bp_clock_v2;
        get_test_bp_clock_v2::<64>(num_frames)
    }
}

fn main() {
    let args = Args::parse();
    let c_key = ContainerKey::new(0, 0);

    println!("=== PT Fast-Path Coverage Benchmark ===");
    println!(
        "num_frames={} num_pages={} k={} hot_sets={} threads={} seconds={} warmup={} no_conflict={}",
        args.num_frames,
        args.num_pages,
        args.collision_width,
        args.hot_sets,
        args.threads,
        args.seconds,
        args.warmup,
        args.no_conflict,
    );

    let bp = get_bp(args.num_frames);

    // Create pages. Each create returns the assigned PageFrameKey; page ids are
    // monotonically assigned by the BP, so `keys[i].p_key().page_id == i` in
    // practice, but we don't rely on that — we key everything off the actual
    // returned PageFrameKey.
    let mut keys: Vec<PageFrameKey> = Vec::with_capacity(args.num_pages);
    for _ in 0..args.num_pages {
        let g = bp.create_new_page_for_write(c_key).unwrap();
        keys.push(g.page_frame_key().unwrap());
    }
    println!("Created {} pages", keys.len());

    // Bucket page ids by their PT preferred slot.
    let nf64 = args.num_frames as u64;
    let mut by_slot: Vec<Vec<usize>> = vec![Vec::new(); args.num_frames];
    for (i, k) in keys.iter().enumerate() {
        let slot = pt_preferred_slot(c_key.as_u32(), k.p_key().page_id, nf64) as usize;
        by_slot[slot].push(i);
    }

    // Build the hot set.
    let hot_indices: Vec<usize> = if args.no_conflict {
        // Pick distinct slots, one page each, then repeat until we have
        // k * hot_sets distinct pages from k * hot_sets distinct slots.
        let want = args.collision_width * args.hot_sets;
        let mut out = Vec::with_capacity(want);
        for (slot_id, pages) in by_slot.iter().enumerate() {
            if pages.is_empty() {
                continue;
            }
            let _ = slot_id;
            out.push(pages[0]);
            if out.len() == want {
                break;
            }
        }
        assert!(
            out.len() == want,
            "no-conflict mode: only found {} usable slots; \
             need {} (increase --num-pages or reduce k*hot_sets)",
            out.len(),
            want,
        );
        out
    } else {
        // Pick hot_sets slots each owning ≥ k pages; take the first k pages.
        let k = args.collision_width;
        let want_sets = args.hot_sets;
        let mut picked_sets = 0usize;
        let mut out = Vec::with_capacity(k * want_sets);
        for pages in by_slot.iter() {
            if pages.len() < k {
                continue;
            }
            out.extend(pages.iter().take(k).copied());
            picked_sets += 1;
            if picked_sets == want_sets {
                break;
            }
        }
        assert!(
            picked_sets == want_sets,
            "conflict mode: only found {} slots with ≥ {} collisions; \
             need {} (increase --num-pages / --num-frames ratio)",
            picked_sets,
            k,
            want_sets,
        );
        out
    };

    // Before warmup, refresh every hot page's stored frame_id so LIPAH's
    // hint-based fast path has accurate data. Page IDs in `hot_indices` are
    // stable, but each page may have been evicted/re-placed during the
    // 40000-page create loop above, so the create-time frame_id is stale.
    // Reading each hot page once via `PageFrameKey::new` (no hint) forces the
    // slow path, and the returned guard has the current frame id.
    let mut hot_keys_vec: Vec<PageFrameKey> = Vec::with_capacity(hot_indices.len());
    for &i in &hot_indices {
        let pid = keys[i].p_key().page_id;
        let k = PageFrameKey::new(c_key, pid);
        let g = bp.get_page_for_read(k).expect("failed to refresh hot page");
        hot_keys_vec.push(g.page_frame_key().unwrap());
    }
    let hot_keys: Arc<Vec<PageFrameKey>> = Arc::new(hot_keys_vec);

    println!(
        "Hot set built: {} pages ({} distinct preferred slots{})",
        hot_keys.len(),
        if args.no_conflict {
            hot_keys.len()
        } else {
            args.hot_sets
        },
        if args.no_conflict {
            ", no-conflict control"
        } else {
            ", with collisions"
        },
    );

    // Drop the full `keys` vec — we only need the hot subset below.
    drop(keys);

    // Reset coverage counters after setup so warmup/measurement are clean.
    // The BPs don't expose a "clear coverage" method, but the *creation* +
    // refresh loop only contributes a tiny fraction (~40k + 128 ops) compared
    // to the measurement loop (10s × tens of Mops), so we tolerate it.

    // Warmup. Bring hot pages into residency and drive PT's promotion coin.
    if args.warmup > 0 {
        let flag = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(args.threads + 1));
        std::thread::scope(|s| {
            for tid in 0..args.threads {
                let bp = &bp;
                let hot = &hot_keys;
                let flag = &flag;
                let barrier = &barrier;
                let start = (tid * hot.len()) / args.threads.max(1);
                s.spawn(move || {
                    let mut i = start;
                    barrier.wait();
                    while flag.load(Ordering::Relaxed) {
                        let k = hot[i];
                        if let Ok(g) = bp.get_page_for_read(k) {
                            std::hint::black_box(&g[0]);
                        }
                        i = if i + 1 == hot.len() { 0 } else { i + 1 };
                    }
                });
            }
            barrier.wait();
            std::thread::sleep(std::time::Duration::from_secs(args.warmup));
            flag.store(false, Ordering::Relaxed);
        });
        println!("Warmup done");
    }

    // Main measurement loop.
    let flag = Arc::new(AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(args.threads + 1));

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(args.threads);
        for tid in 0..args.threads {
            let bp = &bp;
            let hot = &hot_keys;
            let flag = &flag;
            let barrier = &barrier;
            let record_latency = args.latency;
            let start = (tid * hot.len()) / args.threads.max(1);
            let h = s.spawn(move || {
                let mut ops: u64 = 0;
                let mut checksum: u64 = 0;
                let mut i = start;
                let mut hist: Option<Histogram<u64>> = if record_latency {
                    Some(Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap())
                } else {
                    None
                };
                barrier.wait();
                while flag.load(Ordering::Relaxed) {
                    let k = hot[i];
                    let t0 = if record_latency {
                        Some(Instant::now())
                    } else {
                        None
                    };
                    match bp.get_page_for_read(k) {
                        Ok(g) => {
                            checksum = checksum.wrapping_add(g[0] as u64);
                        }
                        Err(_) => continue,
                    }
                    if let (Some(h), Some(t0)) = (hist.as_mut(), t0) {
                        let _ = h.record(t0.elapsed().as_nanos() as u64);
                    }
                    ops += 1;
                    i = if i + 1 == hot.len() { 0 } else { i + 1 };
                }
                std::hint::black_box(checksum);
                (ops, hist)
            });
            handles.push(h);
        }

        barrier.wait();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_secs(args.seconds));
        flag.store(false, Ordering::Relaxed);
        let elapsed = start.elapsed();

        let per_thread: Vec<(u64, Option<Histogram<u64>>)> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        let total_ops: u64 = per_thread.iter().map(|(o, _)| *o).sum();
        let throughput = total_ops as f64 / elapsed.as_secs_f64();

        println!();
        println!("=== Results ===");
        println!("Duration:    {:.2}s", elapsed.as_secs_f64());
        println!("Total ops:   {}", total_ops);
        println!(
            "Throughput:  {:.0} ops/s ({:.2} Mops/s)",
            throughput,
            throughput / 1_000_000.0
        );

        if args.latency {
            let mut merged: Histogram<u64> =
                Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap();
            for (_, h) in per_thread.iter() {
                if let Some(h) = h {
                    merged.add(h).unwrap();
                }
            }
            let fmt = |v: u64| {
                if v >= 1_000_000 {
                    format!("{:.2} ms", v as f64 / 1_000_000.0)
                } else if v >= 1_000 {
                    format!("{:.2} µs", v as f64 / 1_000.0)
                } else {
                    format!("{} ns", v)
                }
            };
            println!();
            println!(
                "=== Latency (per-op, across all threads, n={}) ===",
                merged.len()
            );
            println!("  p50    : {}", fmt(merged.value_at_quantile(0.50)));
            println!("  p95    : {}", fmt(merged.value_at_quantile(0.95)));
            println!("  p99    : {}", fmt(merged.value_at_quantile(0.99)));
            println!("  p99.9  : {}", fmt(merged.value_at_quantile(0.999)));
            println!("  max    : {}", fmt(merged.max()));
            println!("  mean   : {:.1} ns", merged.mean());
        }
    });

    // Emit the unified coverage line.
    bp.print_profile();
}
