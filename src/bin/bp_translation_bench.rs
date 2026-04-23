//! Micro-benchmark: pure page translation cost.
//!
//! Default workload is **pointer chasing**: every page stores the
//! `PageFrameKey` of the next page in the chain at its last 12 bytes. Each op
//! reads the current page, decodes the next key from the page tail, and jumps.
//! This creates a dependency chain — the next `get_page_for_read` can't start
//! until the previous page's bytes are loaded — so translation-path latency
//! isn't hidden by OoO execution or the HW prefetcher.
//!
//! Chain layout:
//!   * `--sequential`:   page i → page (i+1) % N  (linear, prefetcher-friendly)
//!   * default (random): one big random cycle covering all pages (derangement
//!                       via shuffled cyclic permutation — defeats prefetcher)
//!
//! Chain payload (last 8 bytes of each page, big-endian):
//!   * bytes[-8..-4] = `page_id` (u32) of the next page in the chain
//!   * bytes[-4..]   = `frame_id` (u32) hint for that page at setup time
//!
//! All pages live in a single container, so the `ContainerKey` is implicit
//! and never stored on-page. On the read hot path:
//!   * **LIPAH variants** (`bp_clock*`) read 8 bytes and use
//!     `PageFrameKey::new_with_frame_id` so the fast-path frame-id hint
//!     short-circuits the DashMap lookup.
//!   * **Non-hint variants** (PT / TLB) read only the 4-byte `page_id` and
//!     construct `PageFrameKey::new`, since `frame_id` is unused by them.
//!
//! Legacy modes (`--theta > 0` zipf, `--phase-shift`) still use
//! pre-computed index arrays.
//!
//! Usage:
//!   cargo run --release --bin bp_translation_bench --features bp_clock \
//!     -- --num-pages 100000 --num-frames 200000 --threads 1 --seconds 5 --theta 0.8

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use clap::Parser;
use fbtree::bp::{ContainerKey, MemPool, PageFrameKey};
use fbtree::random::small_thread_rng;
use fbtree::zipfan::FastZipf;
use hdrhistogram::Histogram;
use rand::RngCore;

/// Whether the compiled BP variant consults the `frame_id` field of
/// `PageFrameKey` as a fast-path hint. If false, the `--no-frame-hint` flag
/// is a no-op.
#[cfg(any(
    feature = "bp_clock",
    feature = "bp_clock_v2",
    feature = "vmcache",
    feature = "bp_dashmap",
    feature = "bp_hashmap",
    feature = "bp_overflow",
))]
const BP_USES_HINT: bool = true;

#[cfg(not(any(
    feature = "bp_clock",
    feature = "bp_clock_v2",
    feature = "vmcache",
    feature = "bp_dashmap",
    feature = "bp_hashmap",
    feature = "bp_overflow",
)))]
const BP_USES_HINT: bool = false;

#[derive(Parser, Debug)]
struct Args {
    /// Number of pages to pre-create.
    #[arg(short = 'n', long, default_value_t = 100_000)]
    num_pages: usize,

    /// Number of buffer pool frames.
    #[arg(short = 'f', long, default_value_t = 200_000)]
    num_frames: usize,

    /// Number of worker threads.
    #[arg(short = 't', long, default_value_t = 1)]
    threads: usize,

    /// Benchmark duration in seconds.
    #[arg(short = 's', long, default_value_t = 5)]
    seconds: u64,

    /// Zipfian theta (0 = uniform, 0.99 = highly skewed). 0 uses uniform.
    #[arg(long, default_value_t = 0.0)]
    theta: f64,

    /// Warmup seconds.
    #[arg(short = 'w', long, default_value_t = 2)]
    warmup: u64,

    /// Use sequential access pattern (scan simulation).
    #[arg(long, default_value_t = false)]
    sequential: bool,

    /// Workload phase shift: split benchmark into two halves, each accessing
    /// a different half of the pages. Simulates hot-set migration.
    #[arg(long, default_value_t = false)]
    phase_shift: bool,

    /// Force LIPAH-style buffer pools to ignore the embedded `frame_id` hint
    /// at chain-traversal time, so every access goes through full
    /// translation. Makes LIPAH vs PT/TLB a clean "translation mechanism"
    /// comparison. No-op for BPs that don't use the hint anyway.
    #[arg(long, default_value_t = false)]
    no_frame_hint: bool,

    /// Multi-hotspot mode: place N concurrent hot clusters across the page
    /// range. Each op picks a hotspot uniformly at random, then samples a
    /// page within it using `--hotspot-theta` (0.0 = uniform-within-cluster).
    /// Disables chain traversal. Set to 0 to disable hotspot mode.
    #[arg(long, default_value_t = 0)]
    hotspots: usize,

    /// Pages per hotspot (only used when `--hotspots > 0`).
    #[arg(long, default_value_t = 1000)]
    hotspot_size: usize,

    /// Zipfian theta for sampling within a hotspot.
    #[arg(long, default_value_t = 0.99)]
    hotspot_theta: f64,

    /// Skip the full-page byte fold in the chain hot path. When set, each op
    /// only reads the last cacheline (for the next-page pointer) plus one
    /// byte for the checksum. Exposes per-op translation/swap overhead
    /// without page-size-dependent memory bandwidth masking it.
    /// No effect on non-chain workloads (zipf, phase-shift, hotspots) which
    /// always fold the page.
    #[arg(long, default_value_t = false)]
    no_page_fold: bool,

    /// Record per-op latency into a hdr-histogram and print percentiles
    /// (p50/p95/p99/p99.9/max) at the end. Adds ~20 ns per op of timing
    /// overhead, so leave off for pure throughput runs.
    #[arg(long, default_value_t = false)]
    latency: bool,

    /// Refresh frame_id hints after warmup (LIPAH "hot" mode). By default,
    /// hints are written once at setup and become stale after evictions
    /// (LIPAH "cold" mode). With this flag, hints are updated to current
    /// frame positions after warmup, giving LIPAH the best-case fast-path
    /// hit rate. Only affects chain traversal mode with hint-using BPs.
    #[arg(long, default_value_t = false)]
    refresh_hints: bool,
}

fn get_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "bp_clock")]
    {
        use fbtree::bp::get_test_bp_clock;
        return get_test_bp_clock::<64>(num_frames);
    }
    #[cfg(feature = "bp_clock_v2")]
    {
        use fbtree::bp::get_test_bp_clock_v2;
        return get_test_bp_clock_v2::<64>(num_frames);
    }
    #[cfg(feature = "bp_pt_bucket")]
    {
        use fbtree::bp::get_test_pt_bucket_validate;
        return get_test_pt_bucket_validate(num_frames);
    }
    // Note: order matters — `bp_pt_bucket_v2_ophash` implies `bp_pt_bucket_v2`,
    // and `bp_pt_v2_ophash` implies `bp_pt_v2`, so the ophash arms are matched
    // first. The inner type is identical; only `preferred_frame` changes
    // (gated by `pt_op_hash` in predictive_translation_v2.rs).
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
    #[cfg(feature = "bp_pt_tlb")]
    {
        use fbtree::bp::get_test_pt_fp_tlb;
        return get_test_pt_fp_tlb(num_frames);
    }
    #[cfg(feature = "bp_pt_tlb_only")]
    {
        use fbtree::bp::get_test_pt_tlb_only;
        return get_test_pt_tlb_only(num_frames);
    }
    #[cfg(feature = "bp_pt_tlb_only_keys")]
    {
        use fbtree::bp::get_test_pt_tlb_only_keys;
        return get_test_pt_tlb_only_keys(num_frames);
    }
    #[cfg(feature = "bp_tlb")]
    {
        use fbtree::bp::get_test_tlb_bp;
        return get_test_tlb_bp(num_frames);
    }
    #[cfg(feature = "bp_tlb_v2")]
    {
        use fbtree::bp::get_test_tlb_bp_v2;
        return get_test_tlb_bp_v2(num_frames);
    }
    #[cfg(feature = "bp_pt")]
    {
        use fbtree::bp::get_test_pt;
        return get_test_pt(num_frames);
    }
    #[cfg(not(any(
        feature = "bp_clock",
        feature = "bp_clock_v2",
        feature = "bp_pt_bucket",
        feature = "bp_pt_bucket_v2",
        feature = "bp_pt_bucket_v2_ophash",
        feature = "bp_pt_v2",
        feature = "bp_pt_v2_ophash",
        feature = "bp_pt_tlb",
        feature = "bp_pt_tlb_only",
        feature = "bp_pt_tlb_only_keys",
        feature = "bp_tlb",
        feature = "bp_tlb_v2",
        feature = "bp_pt",
    )))]
    {
        use fbtree::bp::get_test_bp_clock;
        get_test_bp_clock::<64>(num_frames)
    }
}

fn main() {
    let args = Args::parse();
    let c_key = ContainerKey::new(0, 0);

    // Runtime resolution: only honor the compile-time hint if the user hasn't
    // forced it off from the CLI. A non-hint BP ignores both.
    let use_frame_hint = BP_USES_HINT && !args.no_frame_hint;

    println!("=== BP Translation Micro-Benchmark ===");
    println!(
        "pages={} frames={} threads={} seconds={} theta={} warmup={} sequential={} phase_shift={} frame_hint={} hotspots={} hotspot_size={} hotspot_theta={}",
        args.num_pages,
        args.num_frames,
        args.threads,
        args.seconds,
        args.theta,
        args.warmup,
        args.sequential,
        args.phase_shift,
        use_frame_hint,
        args.hotspots,
        args.hotspot_size,
        args.hotspot_theta,
    );

    // Create BP and pre-populate pages.
    let bp = get_bp(args.num_frames);
    let mut keys: Vec<PageFrameKey> = Vec::with_capacity(args.num_pages);
    for _ in 0..args.num_pages {
        let g = bp.create_new_page_for_write(c_key).unwrap();
        keys.push(g.page_frame_key().unwrap());
    }
    let num_pages = args.num_pages;
    println!("Created {} pages", num_pages);

    // Chain traversal (default) — pick which next-pointer each page gets.
    // `use_chain` = false when the user opted into zipf, phase-shift, or
    // multi-hotspot mode.
    let use_chain = args.theta == 0.0 && !args.phase_shift && args.hotspots == 0;
    let mut next_idx: Vec<usize> = Vec::new();  // Keep outside for hint refresh
    if use_chain {
        // Build per-page next-index.
        next_idx = vec![0; num_pages];
        if args.sequential {
            for i in 0..num_pages {
                next_idx[i] = (i + 1) % num_pages;
            }
        } else {
            // Single random cycle: shuffle a permutation, then
            // next_idx[perm[k]] = perm[(k+1) % N].
            let mut perm: Vec<usize> = (0..num_pages).collect();
            let mut rng = small_thread_rng();
            // Fisher-Yates shuffle
            for i in (1..num_pages).rev() {
                let j = (rng.next_u64() as usize) % (i + 1);
                perm.swap(i, j);
            }
            for k in 0..num_pages {
                next_idx[perm[k]] = perm[(k + 1) % num_pages];
            }
        }

        // Write each page's next-link tail: [page_id BE | frame_id BE].
        for i in 0..num_pages {
            let next = keys[next_idx[i]];
            let page_id_bytes = next.p_key().page_id.to_be_bytes();
            let frame_id_bytes = next.frame_id().to_be_bytes();
            let mut g = bp.get_page_for_write(keys[i]).unwrap();
            let page: &mut [u8] = &mut *g;
            let len = page.len();
            page[len - 8..len - 4].copy_from_slice(&page_id_bytes);
            page[len - 4..].copy_from_slice(&frame_id_bytes);
        }
        println!(
            "Chain installed ({} links, {}, frame-hint={})",
            num_pages,
            if args.sequential {
                "linear"
            } else {
                "random cycle"
            },
            use_frame_hint,
        );
    }

    let keys = Arc::new(keys);

    // Warmup
    if args.warmup > 0 {
        println!("Warming up for {}s...", args.warmup);
        let flag = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(args.threads + 1));

        std::thread::scope(|s| {
            for tid in 0..args.threads {
                let bp = &bp;
                let keys = &keys;
                let flag = &flag;
                let barrier = &barrier;
                let sequential = args.sequential;
                let use_chain = use_chain;
                let use_frame_hint = use_frame_hint;
                // Spread warmup starts evenly so threads touch different
                // pages under chain mode.
                let start_idx = tid * num_pages / args.threads.max(1);
                s.spawn(move || {
                    let mut rng = small_thread_rng();
                    let mut seq_idx: usize = 0;
                    let mut current: PageFrameKey = keys[start_idx];
                    barrier.wait();
                    while flag.load(Ordering::Relaxed) {
                        if use_chain {
                            let g = match bp.get_page_for_read(current) {
                                Ok(g) => g,
                                Err(_) => {
                                    current = keys[start_idx];
                                    continue;
                                }
                            };
                            let page: &[u8] = &*g;
                            let len = page.len();
                            let page_id =
                                u32::from_be_bytes(page[len - 8..len - 4].try_into().unwrap());
                            current = if use_frame_hint {
                                let frame_id =
                                    u32::from_be_bytes(page[len - 4..].try_into().unwrap());
                                PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
                            } else {
                                PageFrameKey::new(c_key, page_id)
                            };
                        } else {
                            let idx = if sequential {
                                let i = seq_idx;
                                seq_idx = (seq_idx + 1) % num_pages;
                                i
                            } else {
                                (rng.next_u64() as usize) % num_pages
                            };
                            let _ = bp.get_page_for_read(keys[idx]);
                        }
                    }
                });
            }
            barrier.wait();
            std::thread::sleep(std::time::Duration::from_secs(args.warmup));
            flag.store(false, Ordering::Relaxed);
        });
        println!("Warmup done");
    }

    // Refresh hints after warmup if requested (LIPAH "hot" mode).
    if use_chain && use_frame_hint && args.refresh_hints {
        println!("Refreshing frame_id hints to current positions...");
        for i in 0..num_pages {
            let next = keys[next_idx[i]];
            // Read current page to get updated next page info
            let next_key = PageFrameKey::new(c_key, next.p_key().page_id);
            match bp.get_page_for_read(next_key) {
                Ok(guard) => {
                    // Get current frame_id from the guard
                    if let Some(current_key) = guard.page_frame_key() {
                        let frame_id_bytes = current_key.frame_id().to_be_bytes();
                        // Update the hint in the current page
                        let mut g = bp.get_page_for_write(keys[i]).unwrap();
                        let page: &mut [u8] = &mut *g;
                        let len = page.len();
                        // Update only the frame_id portion (last 4 bytes)
                        page[len - 4..].copy_from_slice(&frame_id_bytes);
                    }
                }
                Err(_) => {
                    // Page was evicted, best effort - keep old hint
                    continue;
                }
            }
        }
        println!("Hints refreshed");
    }

    // Benchmark
    let flag = Arc::new(AtomicBool::new(true));
    // Phase: 0 = first half of pages, 1 = second half. Only used with --phase-shift.
    let phase = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(args.threads + 1));

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for tid in 0..args.threads {
            let bp = &bp;
            let keys = &keys;
            let flag = &flag;
            let phase = &phase;
            let barrier = &barrier;
            let theta = args.theta;
            let sequential = args.sequential;
            let phase_shift = args.phase_shift;
            let half = num_pages / 2;
            let use_chain = use_chain;
            let use_frame_hint = use_frame_hint;
            let num_hotspots = args.hotspots;
            let hotspot_size = args.hotspot_size.min(num_pages);
            let hotspot_theta = args.hotspot_theta;
            let no_page_fold = args.no_page_fold;
            let record_latency = args.latency;
            // Per-thread start position so threads don't convoy on the same
            // chain pointer in lock-step.
            let start_idx = tid * num_pages / args.threads.max(1);

            let h = s.spawn(move || {
                let rng = small_thread_rng();
                let mut zipf = if theta > 0.0 {
                    Some(FastZipf::new(
                        rng,
                        theta,
                        if phase_shift { half } else { num_pages },
                    ))
                } else {
                    None
                };
                // Dedicated zipfian for sampling within a hotspot — each
                // thread gets its own so contention on RNG state doesn't
                // muddy the picture.
                let mut hot_zipf = if num_hotspots > 0 && hotspot_theta > 0.0 {
                    Some(FastZipf::new(
                        small_thread_rng(),
                        hotspot_theta,
                        hotspot_size,
                    ))
                } else {
                    None
                };
                let mut uniform_rng = small_thread_rng();
                let mut seq_idx: usize = 0;
                let mut ops: u64 = 0;
                // Running checksum so the compiler cannot DCE the per-op work.
                // For the chain path we keep the checksum even though the
                // decoded next-key already forces a load of the last cacheline:
                // `black_box` on the final accumulator is belt-and-braces.
                let mut checksum: u64 = 0;
                let mut current: PageFrameKey = keys[start_idx];
                // Latency histogram (1 ns .. 60 s, 3 sig figs). None when
                // recording is disabled so the hot path pays no cost.
                let mut hist: Option<Histogram<u64>> = if record_latency {
                    Some(Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap())
                } else {
                    None
                };

                barrier.wait();

                while flag.load(Ordering::Relaxed) {
                    if use_chain {
                        // Pointer-chase. The dependency chain from `current`
                        // → page load → tail bytes → next `current` means
                        // each translation must complete before the next can
                        // begin; OoO execution cannot hide latency.
                        let t0 = if record_latency {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        let g = match bp.get_page_for_read(current) {
                            Ok(g) => g,
                            Err(_) => {
                                current = keys[start_idx];
                                continue;
                            }
                        };
                        let page: &[u8] = &*g;
                        let len = page.len();
                        let page_id =
                            u32::from_be_bytes(page[len - 8..len - 4].try_into().unwrap());
                        let next = if use_frame_hint {
                            let frame_id = u32::from_be_bytes(page[len - 4..].try_into().unwrap());
                            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
                        } else {
                            PageFrameKey::new(c_key, page_id)
                        };
                        // Mix one byte into the checksum so the load of the
                        // non-tail portion of the page isn't DCE'd.
                        checksum = checksum.wrapping_add(page[0] as u64);
                        drop(g);
                        if let (Some(h), Some(t0)) = (hist.as_mut(), t0) {
                            let _ = h.record(t0.elapsed().as_nanos() as u64);
                        }
                        current = next;
                        ops += 1;
                        continue;
                    }

                    let idx = if num_hotspots > 0 {
                        // Multi-hotspot: pick a hotspot uniformly, then
                        // sample within it (zipf or uniform). Hotspots are
                        // placed contiguously at the start of the page
                        // range; layout: hot[h] = [h*size, (h+1)*size).
                        let h = (uniform_rng.next_u64() as usize) % num_hotspots;
                        let base = (h * hotspot_size) % num_pages.max(1);
                        let within = if let Some(ref mut hz) = hot_zipf {
                            hz.sample()
                        } else {
                            (uniform_rng.next_u64() as usize) % hotspot_size.max(1)
                        };
                        (base + within) % num_pages.max(1)
                    } else {
                        let base = if phase_shift {
                            phase.load(Ordering::Relaxed) * half
                        } else {
                            0
                        };
                        let range = if phase_shift { half } else { num_pages };

                        if sequential {
                            let i = seq_idx;
                            seq_idx = (seq_idx + 1) % range;
                            base + i
                        } else if let Some(ref mut z) = zipf {
                            base + z.sample()
                        } else {
                            base + (uniform_rng.next_u64() as usize) % range
                        }
                    };
                    let t0 = if record_latency {
                        Some(Instant::now())
                    } else {
                        None
                    };
                    let g = match bp.get_page_for_read(keys[idx]) {
                        Ok(g) => g,
                        Err(_) => continue, // retry on eviction failure / latch contention
                    };
                    let page: &[u8] = &*g;
                    if no_page_fold {
                        // Minimal touch: one byte, prevents DCE. Isolates
                        // translation / swap cost from page-size-dependent
                        // memory bandwidth.
                        checksum = checksum.wrapping_add(page[0] as u64);
                    } else {
                        // Fold every byte of the page (Page derefs to &[u8]).
                        let page = std::hint::black_box(page);
                        let mut acc: u64 = 0;
                        for &b in page {
                            acc = acc.wrapping_add(b as u64);
                        }
                        checksum ^= acc;
                    }
                    drop(g);
                    if let (Some(h), Some(t0)) = (hist.as_mut(), t0) {
                        let _ = h.record(t0.elapsed().as_nanos() as u64);
                    }
                    ops += 1;
                }
                std::hint::black_box(checksum);
                (ops, hist)
            });
            handles.push(h);
        }

        barrier.wait();
        let start = Instant::now();
        if args.phase_shift {
            // Phase 1: first half of pages
            std::thread::sleep(std::time::Duration::from_secs(args.seconds / 2));
            println!("--- Phase shift: switching to second half of pages ---");
            phase.store(1, Ordering::Relaxed);
            // Phase 2: second half of pages
            std::thread::sleep(std::time::Duration::from_secs(
                args.seconds - args.seconds / 2,
            ));
        } else {
            std::thread::sleep(std::time::Duration::from_secs(args.seconds));
        }
        flag.store(false, Ordering::Relaxed);
        let elapsed = start.elapsed();

        let per_thread: Vec<(u64, Option<Histogram<u64>>)> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        let total_ops: u64 = per_thread.iter().map(|(o, _)| *o).sum();
        let throughput = total_ops as f64 / elapsed.as_secs_f64();
        let avg_ns = elapsed.as_nanos() as f64 / (total_ops as f64 / args.threads as f64);

        println!();
        println!("=== Results ===");
        println!("Duration:    {:.2}s", elapsed.as_secs_f64());
        println!("Total ops:   {}", total_ops);
        println!(
            "Throughput:  {:.0} ops/s ({:.2} Mops/s)",
            throughput,
            throughput / 1_000_000.0
        );
        println!("Avg latency: {:.1} ns/op (per thread)", avg_ns);

        for (i, (o, _)) in per_thread.iter().enumerate() {
            println!(
                "  Thread {}: {} ops ({:.2} Mops/s)",
                i,
                o,
                *o as f64 / elapsed.as_secs_f64() / 1_000_000.0
            );
        }

        // Aggregate latency histograms if recording was enabled.
        if args.latency {
            let mut merged: Histogram<u64> =
                Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap();
            for (_, h) in per_thread.iter() {
                if let Some(h) = h {
                    merged.add(h).unwrap();
                }
            }
            let fmt_ns = |v: u64| {
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
            println!("  min    : {}", fmt_ns(merged.min()));
            println!("  p50    : {}", fmt_ns(merged.value_at_quantile(0.50)));
            println!("  p90    : {}", fmt_ns(merged.value_at_quantile(0.90)));
            println!("  p95    : {}", fmt_ns(merged.value_at_quantile(0.95)));
            println!("  p99    : {}", fmt_ns(merged.value_at_quantile(0.99)));
            println!("  p99.9  : {}", fmt_ns(merged.value_at_quantile(0.999)));
            println!("  p99.99 : {}", fmt_ns(merged.value_at_quantile(0.9999)));
            println!("  max    : {}", fmt_ns(merged.max()));
            println!("  mean   : {:.1} ns", merged.mean());
            println!("  stddev : {:.1} ns", merged.stdev());
        }
    });

    // Dump per-BP profile counters (no-op unless the BP was built with
    // `pt_counts` or `pt_profile`).
    bp.print_profile();
}
