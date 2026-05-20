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
    feature = "vmcache",
    feature = "bp_overflow",
))]
const BP_USES_HINT: bool = true;

#[cfg(not(any(
    feature = "bp_clock",
    feature = "vmcache",
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

    /// Number of containers to distribute pages across. With ophash, multiple
    /// containers create collisions where container ranges overlap in the frame
    /// space, reducing coverage. Default: 1 (single container, ophash gets 100%).
    #[arg(long, default_value_t = 1)]
    num_containers: usize,

    /// Fold the first N bytes of each page into the checksum on every chain
    /// hop. N=0 (default) preserves the translation-only behavior (1 byte
    /// from page[0] + the 12-byte tail). Larger N simulates per-page payload
    /// work like tuple decode or aggregation scans. Bytes are folded
    /// sequentially from page[0..N] — prefetcher's best case.
    #[arg(long, default_value_t = 0)]
    payload_bytes: usize,

    /// Disable the chain-traversal mode. Each access picks a uniformly random
    /// page index and looks it up via keys[idx], with no dependency between
    /// successive accesses. Models OLTP point-lookup workloads (each access
    /// independent, OoO can hide some translation latency).
    #[arg(long, default_value_t = false)]
    no_chain: bool,

    /// Physically scramble page→frame placement before the benchmark runs.
    /// After page creation and chain installation, calls flush_all_and_reset
    /// then re-faults every page in shuffled order. The new positions are
    /// uncorrelated with creation order, so:
    ///   - LIPAH: hints written at creation now point at the wrong frames
    ///     (LIPAH issues a hint check, misses, falls back to DashMap).
    ///   - PT/Congee: pages are at random positions wrt their preferred
    ///     frames, so the FP fast path's meta(pref).key() check misses and
    ///     every access lands in the overflow slow path.
    /// This is the proper "stale predictions" workload — measures the cost
    /// when the predictor is wrong, not when it's been short-circuited.
    #[arg(long, default_value_t = false)]
    scramble: bool,

    /// Use the callback read path (`read_page_with`) instead of the
    /// guard-returning `get_page_for_read`. For PrediCache this exercises
    /// the speculative try-latch-at-pref path, which bypasses the
    /// OptimisticPageMap on hits. Other BPs fall back to the default
    /// trait impl, which is just a guard + callback wrapper.
    #[arg(long, default_value_t = false)]
    callback_path: bool,

    /// Control experiment: with probability F sample from pages currently at
    /// their preferred frame, else from displaced pages. Negative = disabled
    /// (default). Forces uniform-random access (implies `--no-chain`) since
    /// the classification only makes sense for independent point lookups.
    /// Requires the BP to implement `preferred_frame_for` (currently
    /// PrediCache only). After classification we also force-set the
    /// promotion probabilities to `u32::MAX` so the classified sets don't
    /// drift mid-run.
    #[arg(long, default_value_t = -1.0)]
    prefer_prob: f64,

    /// Balance the preferred / displaced sets to exactly this many pages each
    /// (working_set = 2 * prefer_cap). Default 10000 keeps the working set
    /// small enough to fit in L3 at moderate payload, so cache footprint
    /// doesn't confound the bypass-vs-always-probe comparison. Set to 0 to
    /// use `min(|preferred|, |displaced|)` instead (no fixed cap).
    #[arg(long, default_value_t = 10000)]
    prefer_cap: usize,
}

fn get_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "bp_clock")]
    {
        use fbtree::bp::get_test_bp_clock;
        return get_test_bp_clock::<64>(num_frames);
    }
    #[cfg(feature = "bp_pt_bucket_v2")]
    {
        use fbtree::bp::get_test_pt_bucket_validate_v2;
        return get_test_pt_bucket_validate_v2(num_frames);
    }
    #[cfg(feature = "bp_lapt")]
    {
        use fbtree::bp::get_test_lapt;
        return get_test_lapt(num_frames);
    }
    #[cfg(feature = "bp_lapt2")]
    {
        use fbtree::bp::get_test_lapt2;
        return get_test_lapt2(num_frames);
    }
    #[cfg(feature = "bp_lapt3")]
    {
        use fbtree::bp::get_test_lapt3;
        return get_test_lapt3(num_frames);
    }
    #[cfg(feature = "bp_pt_v2")]
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
    #[cfg(feature = "bp_predicache")]
    {
        use fbtree::bp::get_test_predicache;
        return get_test_predicache(num_frames);
    }
    #[cfg(not(any(
        feature = "bp_clock",
        feature = "bp_pt_bucket_v2",
        feature = "bp_lapt",
        feature = "bp_lapt2",
        feature = "bp_lapt3",
        feature = "bp_pt_v2",
        feature = "bp_pt_tlb",
        feature = "bp_pt_tlb_only",
        feature = "bp_pt_tlb_only_keys",
        feature = "bp_tlb",
        feature = "bp_predicache",
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
        "pages={} frames={} threads={} seconds={} theta={} warmup={} sequential={} phase_shift={} frame_hint={} hotspots={} hotspot_size={} hotspot_theta={} callback_path={}",
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
        args.callback_path,
    );

    // Create BP and pre-populate pages across multiple containers.
    let bp = get_bp(args.num_frames);
    let mut keys: Vec<PageFrameKey> = Vec::with_capacity(args.num_pages);
    let pages_per_container = args.num_pages / args.num_containers;

    for c_idx in 0..args.num_containers {
        let container_key = if args.num_containers == 1 {
            c_key  // Use default c_key for single container (backward compat)
        } else {
            ContainerKey::new(c_idx as u16, 0)
        };
        let count = if c_idx == args.num_containers - 1 {
            // Last container gets remainder
            args.num_pages - (pages_per_container * (args.num_containers - 1))
        } else {
            pages_per_container
        };
        for _ in 0..count {
            let g = bp.create_new_page_for_write(container_key).unwrap();
            keys.push(g.page_frame_key().unwrap());
        }
    }
    let num_pages = args.num_pages;
    if args.num_containers > 1 {
        println!("Created {} pages across {} containers", num_pages, args.num_containers);
    } else {
        println!("Created {} pages", num_pages);
    }

    // Chain traversal (default) — pick which next-pointer each page gets.
    // `use_chain` = false when the user opted into zipf, phase-shift, or
    // multi-hotspot mode.
    // --prefer-prob implies random access (no chain) because the
    // classification only makes sense for independent point lookups.
    let use_chain = args.theta == 0.0
        && !args.phase_shift
        && args.hotspots == 0
        && !args.no_chain
        && args.prefer_prob < 0.0;
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

        // Write each page's next-link tail: [c_key BE | page_id BE | frame_id BE].
        // c_key encoded so chains crossing containers (--num-containers > 1)
        // resolve correctly on read.
        for i in 0..num_pages {
            let next = keys[next_idx[i]];
            let c_key_bytes = next.p_key().c_key.as_u32().to_be_bytes();
            let page_id_bytes = next.p_key().page_id.to_be_bytes();
            let frame_id_bytes = next.frame_id().to_be_bytes();
            let mut g = bp.get_page_for_write(keys[i]).unwrap();
            let page: &mut [u8] = &mut *g;
            let len = page.len();
            page[len - 12..len - 8].copy_from_slice(&c_key_bytes);
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

    // Scramble: physically randomize page→frame placement so creation-order
    // assumptions break. flush_all_and_reset returns every frame to the free
    // queue; re-faulting in a shuffled permutation maps shuffled[i] → next
    // free frame, decoupling page_id from frame_id.
    //
    // After scramble, the chain tail still encodes the OLD frame_ids written
    // at creation. We deliberately don't rewrite them — that's the whole
    // point: LIPAH reads the stale hint, attempts the wrong frame, falls
    // back to DashMap. For PT/Congee the chain frame_id is unread anyway.
    if args.scramble {
        println!("Scrambling page→frame placement...");
        bp.flush_all_and_reset()
            .expect("flush_all_and_reset failed during scramble");
        let mut perm: Vec<usize> = (0..num_pages).collect();
        let mut rng = small_thread_rng();
        for i in (1..num_pages).rev() {
            let j = (rng.next_u64() as usize) % (i + 1);
            perm.swap(i, j);
        }
        for &i in &perm {
            // Re-fault by page_key only (no hint). The BP picks the next
            // free frame from its queue, which is in eviction order — the
            // shuffled access order is what randomizes the placement.
            let pk = keys[i].p_key();
            let lookup_key = PageFrameKey::new(pk.c_key, pk.page_id);
            let _ = bp.get_page_for_read(lookup_key).unwrap();
        }
        println!("Scramble done ({} pages re-faulted in shuffled order)", num_pages);
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
                            let next_c_key = ContainerKey::from_u32(u32::from_be_bytes(
                                page[len - 12..len - 8].try_into().unwrap(),
                            ));
                            let page_id =
                                u32::from_be_bytes(page[len - 8..len - 4].try_into().unwrap());
                            current = if use_frame_hint {
                                let frame_id =
                                    u32::from_be_bytes(page[len - 4..].try_into().unwrap());
                                PageFrameKey::new_with_frame_id(next_c_key, page_id, frame_id)
                            } else {
                                PageFrameKey::new(next_c_key, page_id)
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
            let next_key = PageFrameKey::new(next.p_key().c_key, next.p_key().page_id);
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

    // --prefer-prob: classify pages into preferred-at vs displaced sets.
    // Done after warmup so the BP has stabilized. The Arc<Vec<usize>>s are
    // empty when --prefer-prob is disabled and the workload loop falls back
    // to the existing uniform random index pick.
    let prefer_enabled = args.prefer_prob >= 0.0;
    let (preferred_indices, displaced_indices): (Vec<usize>, Vec<usize>) = if prefer_enabled {
        // Lock both promotion knobs to u32::MAX *before* the classification
        // loop. Each `get_page_for_read` we issue below would otherwise be
        // a chance to trigger probabilistic promotion (1/50 default on
        // no-demote, 1/512 on demote), which swaps pages between preferred
        // and displaced frames mid-classification and invalidates earlier
        // entries in our sets.
        #[cfg(feature = "bp_predicache")]
        fbtree::bp::pt_set_promote_probs(u32::MAX, u32::MAX);

        let mut pref = Vec::new();
        let mut displ = Vec::new();
        let mut unsupported = false;
        for i in 0..num_pages {
            let k = keys[i];
            let pref_frame = match bp.preferred_frame_for(k.p_key()) {
                Some(p) => p,
                None => {
                    unsupported = true;
                    break;
                }
            };
            let g = match bp.get_page_for_read(k) {
                Ok(g) => g,
                Err(_) => continue,
            };
            let cur_frame = g.page_frame_key().map(|pfk| pfk.frame_id()).unwrap_or(u32::MAX);
            drop(g);
            if cur_frame == pref_frame {
                pref.push(i);
            } else {
                displ.push(i);
            }
        }
        if unsupported {
            eprintln!(
                "--prefer-prob: BP does not implement preferred_frame_for; \
                 disabling the control experiment"
            );
            (Vec::new(), Vec::new())
        } else {
            let observed = pref.len() as f64 / num_pages as f64;
            let n_pref_full = pref.len();
            let n_displ_full = displ.len();
            // Balance both sets to the same fixed cap so working-set footprint
            // is identical regardless of prefer_prob (otherwise the smaller
            // set wins on cache footprint and confounds the lookup-cost
            // comparison). Default 10000 each (configurable via
            // `--prefer-cap`). Set cap to 0 to use min(|pref|,|displ|).
            let cap = if args.prefer_cap == 0 {
                n_pref_full.min(n_displ_full)
            } else {
                args.prefer_cap.min(n_pref_full).min(n_displ_full)
            };
            let mut rng = small_thread_rng();
            for v in [&mut pref, &mut displ].iter_mut() {
                for i in (1..v.len()).rev() {
                    let j = (rng.next_u64() as usize) % (i + 1);
                    v.swap(i, j);
                }
                v.truncate(cap);
            }
            println!(
                "--prefer-prob classification: natural_hit_rate = {:.3} \
                 ({} preferred, {} displaced); balanced to {} each \
                 (working_set = {} pages)",
                observed, n_pref_full, n_displ_full, cap, cap * 2,
            );
            println!(
                "  target prefer_prob = {:.3}",
                args.prefer_prob,
            );
            // Promotion was already locked before the classification loop
            // (see above), so the classified sets are stable through the
            // timed window. Nothing more to do here.
            (pref, displ)
        }
    } else {
        (Vec::new(), Vec::new())
    };
    let preferred_indices = Arc::new(preferred_indices);
    let displaced_indices = Arc::new(displaced_indices);
    let prefer_active =
        prefer_enabled && !preferred_indices.is_empty() && !displaced_indices.is_empty();

    // For the --prefer-prob control experiment, zero out the BP's profile
    // counters so `fast_path_coverage` reflects ONLY the timed window
    // (otherwise warmup + classification reads dilute it toward the
    // natural hit rate). Safe to call unconditionally — non-pt_counts
    // builds have an empty profile so `clear()` is effectively a no-op.
    if prefer_active {
        unsafe { bp.reset_stats() };
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
            let payload_bytes = args.payload_bytes;
            let record_latency = args.latency;
            let callback_path = args.callback_path;
            let prefer_active = prefer_active;
            let prefer_prob = args.prefer_prob;
            let preferred_indices = preferred_indices.clone();
            let displaced_indices = displaced_indices.clone();
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
                        if callback_path {
                            // Closure does the same work as the guard arm; the
                            // BP releases the latch on return. PrediCache's
                            // override of `read_page_with` runs this body
                            // under its speculative latch-at-pref path.
                            let outcome = bp.read_page_with(current, |page| {
                                let bytes: &[u8] = page;
                                let len = bytes.len();
                                let next_c_key = ContainerKey::from_u32(u32::from_be_bytes(
                                    bytes[len - 12..len - 8].try_into().unwrap(),
                                ));
                                let page_id = u32::from_be_bytes(
                                    bytes[len - 8..len - 4].try_into().unwrap(),
                                );
                                let next = if use_frame_hint {
                                    let frame_id = u32::from_be_bytes(
                                        bytes[len - 4..].try_into().unwrap(),
                                    );
                                    PageFrameKey::new_with_frame_id(next_c_key, page_id, frame_id)
                                } else {
                                    PageFrameKey::new(next_c_key, page_id)
                                };
                                let cs_delta: u64 = if payload_bytes == 0 {
                                    bytes[0] as u64
                                } else {
                                    let n = payload_bytes.min(len);
                                    let payload = std::hint::black_box(&bytes[..n]);
                                    let mut acc: u64 = 0;
                                    for &b in payload {
                                        acc = acc.wrapping_add(b as u64);
                                    }
                                    acc
                                };
                                (next, cs_delta)
                            });
                            let (next, cs_delta) = match outcome {
                                Ok(v) => v,
                                Err(_) => {
                                    current = keys[start_idx];
                                    continue;
                                }
                            };
                            if payload_bytes == 0 {
                                checksum = checksum.wrapping_add(cs_delta);
                            } else {
                                checksum ^= cs_delta;
                            }
                            if let (Some(h), Some(t0)) = (hist.as_mut(), t0) {
                                let _ = h.record(t0.elapsed().as_nanos() as u64);
                            }
                            current = next;
                            ops += 1;
                            continue;
                        }
                        let g = match bp.get_page_for_read(current) {
                            Ok(g) => g,
                            Err(_) => {
                                current = keys[start_idx];
                                continue;
                            }
                        };
                        let page: &[u8] = &*g;
                        let len = page.len();
                        let next_c_key = ContainerKey::from_u32(u32::from_be_bytes(
                            page[len - 12..len - 8].try_into().unwrap(),
                        ));
                        let page_id =
                            u32::from_be_bytes(page[len - 8..len - 4].try_into().unwrap());
                        let next = if use_frame_hint {
                            let frame_id = u32::from_be_bytes(page[len - 4..].try_into().unwrap());
                            PageFrameKey::new_with_frame_id(next_c_key, page_id, frame_id)
                        } else {
                            PageFrameKey::new(next_c_key, page_id)
                        };
                        // Payload work: fold the first `payload_bytes` of the
                        // page into the checksum. Default 0 means just one
                        // byte (page[0]) — pure translation cost.
                        if payload_bytes == 0 {
                            checksum = checksum.wrapping_add(page[0] as u64);
                        } else {
                            let n = payload_bytes.min(len);
                            let payload = std::hint::black_box(&page[..n]);
                            let mut acc: u64 = 0;
                            for &b in payload {
                                acc = acc.wrapping_add(b as u64);
                            }
                            checksum ^= acc;
                        }
                        drop(g);
                        if let (Some(h), Some(t0)) = (hist.as_mut(), t0) {
                            let _ = h.record(t0.elapsed().as_nanos() as u64);
                        }
                        current = next;
                        ops += 1;
                        continue;
                    }

                    let idx = if prefer_active {
                        // --prefer-prob: with probability `prefer_prob` sample
                        // from pages currently at their preferred frame, else
                        // from displaced pages. Two cached vector loads + one
                        // rng call — same per-access bookkeeping as a uniform
                        // pick over `keys[]`.
                        let r = (uniform_rng.next_u64() as f64) / (u64::MAX as f64);
                        if r < prefer_prob {
                            let pos = (uniform_rng.next_u64() as usize)
                                % preferred_indices.len();
                            preferred_indices[pos]
                        } else {
                            let pos = (uniform_rng.next_u64() as usize)
                                % displaced_indices.len();
                            displaced_indices[pos]
                        }
                    } else if num_hotspots > 0 {
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
                    if callback_path {
                        // Respect `--payload-bytes` on the random-access path
                        // too — was previously only honored on the chain path,
                        // so `--no-chain --payload-bytes 1024` was silently
                        // reading the full page (e.g. 16 KiB).
                        let outcome = bp.read_page_with(keys[idx], |page| {
                            let bytes: &[u8] = page;
                            let len = bytes.len();
                            if no_page_fold {
                                (true, bytes[0] as u64)
                            } else if payload_bytes == 0 {
                                let page = std::hint::black_box(bytes);
                                let mut acc: u64 = 0;
                                for &b in page {
                                    acc = acc.wrapping_add(b as u64);
                                }
                                (false, acc)
                            } else {
                                let n = payload_bytes.min(len);
                                let payload = std::hint::black_box(&bytes[..n]);
                                let mut acc: u64 = 0;
                                for &b in payload {
                                    acc = acc.wrapping_add(b as u64);
                                }
                                (false, acc)
                            }
                        });
                        match outcome {
                            Ok((is_byte, v)) => {
                                if is_byte {
                                    checksum = checksum.wrapping_add(v);
                                } else {
                                    checksum ^= v;
                                }
                            }
                            Err(_) => continue,
                        }
                        if let (Some(h), Some(t0)) = (hist.as_mut(), t0) {
                            let _ = h.record(t0.elapsed().as_nanos() as u64);
                        }
                        ops += 1;
                        continue;
                    }
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
                    } else if payload_bytes == 0 {
                        // Fold every byte of the page (Page derefs to &[u8]).
                        let page = std::hint::black_box(page);
                        let mut acc: u64 = 0;
                        for &b in page {
                            acc = acc.wrapping_add(b as u64);
                        }
                        checksum ^= acc;
                    } else {
                        // Honor `--payload-bytes` on the guard random-access
                        // path too (parallels the callback path above).
                        let n = payload_bytes.min(page.len());
                        let payload = std::hint::black_box(&page[..n]);
                        let mut acc: u64 = 0;
                        for &b in payload {
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
