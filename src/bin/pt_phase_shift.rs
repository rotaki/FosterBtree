//! Phase-shift coverage trace — adaptation-lag microbenchmark.
//!
//! See plan: PT strength/weakness study, Part B4.
//!
//! The headline claim: PT(FP)'s fast path is tied to hash placement, so when
//! the hot set shifts, PT has to *re-win* preferred slots from the previous
//! owners. With promotion probability p = 1/512 the "win" is a slow random
//! coin, so coverage ramps back up only over many thousands of accesses per
//! hot page. LIPAH and TLB don't have this bottleneck because their fast
//! path is not a single-winner slot — every page has its own hint/TLB entry.
//!
//! Workload:
//!   1. Create `--num-pages` pages (default 100k) in a single container.
//!   2. Split page space into `--num-phases` disjoint hot sets of size
//!      `--phase-hot-size`. Every `--phase-interval` seconds rotate to the
//!      next hot set.
//!   3. N workers round-robin the *current* hot set.
//!   4. A separate sampler thread snapshots the unified coverage counter
//!      (`MemPool::sample_coverage`) every `--sample-interval-ms` and prints
//!      a TSV line: `t_ms<tab>phase<tab>delta_hits<tab>delta_total<tab>coverage_window`.
//!      `coverage_window` = delta_hits / delta_total for this window, giving
//!      a per-window view that's easy to plot against (time since phase change).
//!
//! Usage:
//!   cargo run --release --bin pt_phase_shift \
//!     --features "bp_pt_bucket_v2,pt_counts" \
//!     -- --num-pages 100000 --num-frames 200000 \
//!        --phase-hot-size 256 --num-phases 4 --phase-interval 5 \
//!        --threads 12

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use clap::Parser;
use fbtree::bp::{ContainerKey, MemPool, PageFrameKey};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 100_000)]
    num_pages: usize,

    #[arg(long, default_value_t = 200_000)]
    num_frames: usize,

    /// Number of disjoint hot sets to cycle through.
    #[arg(long, default_value_t = 4)]
    num_phases: usize,

    /// Pages per hot set.
    #[arg(long, default_value_t = 256)]
    phase_hot_size: usize,

    /// Seconds spent in each phase before rotating to the next.
    #[arg(long, default_value_t = 5)]
    phase_interval: u64,

    /// Sampling interval for the coverage trace in milliseconds.
    #[arg(long, default_value_t = 500)]
    sample_interval_ms: u64,

    #[arg(long, default_value_t = 12)]
    threads: usize,

    /// Warmup seconds on the first hot set before we start the trace.
    #[arg(long, default_value_t = 3)]
    warmup: u64,
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

    assert!(
        args.phase_hot_size * args.num_phases <= args.num_pages,
        "need num_pages >= phase_hot_size * num_phases; got {} < {} * {}",
        args.num_pages,
        args.phase_hot_size,
        args.num_phases,
    );

    println!("=== PT Phase-Shift Coverage Trace ===");
    println!(
        "num_pages={} num_frames={} phases={} phase_hot_size={} phase_interval={}s sample_interval={}ms threads={}",
        args.num_pages,
        args.num_frames,
        args.num_phases,
        args.phase_hot_size,
        args.phase_interval,
        args.sample_interval_ms,
        args.threads,
    );

    let bp = get_bp(args.num_frames);

    let mut keys: Vec<PageFrameKey> = Vec::with_capacity(args.num_pages);
    for _ in 0..args.num_pages {
        let g = bp.create_new_page_for_write(c_key).unwrap();
        keys.push(g.page_frame_key().unwrap());
    }
    println!("Created {} pages", keys.len());

    // Build disjoint hot sets. Phase `p` owns pages [p*H, (p+1)*H).
    let hot_sets: Vec<Arc<Vec<PageFrameKey>>> = (0..args.num_phases)
        .map(|p| {
            let base = p * args.phase_hot_size;
            let mut v = Vec::with_capacity(args.phase_hot_size);
            for i in 0..args.phase_hot_size {
                // Refresh the frame_id so LIPAH's hint starts accurate.
                let pid = keys[base + i].p_key().page_id;
                let k = PageFrameKey::new(c_key, pid);
                let g = bp.get_page_for_read(k).unwrap();
                v.push(g.page_frame_key().unwrap());
            }
            Arc::new(v)
        })
        .collect();

    drop(keys);

    // Drive the current phase via an AtomicUsize that workers read every op.
    let phase = Arc::new(AtomicUsize::new(0));
    let flag = Arc::new(AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(args.threads + 1));

    // Warmup on phase 0.
    {
        let warm_flag = Arc::new(AtomicBool::new(true));
        let warm_bar = Arc::new(Barrier::new(args.threads + 1));
        std::thread::scope(|s| {
            for tid in 0..args.threads {
                let bp = &bp;
                let hot = hot_sets[0].clone();
                let warm_flag = &warm_flag;
                let warm_bar = &warm_bar;
                let start = (tid * hot.len()) / args.threads.max(1);
                s.spawn(move || {
                    let mut i = start;
                    warm_bar.wait();
                    while warm_flag.load(Ordering::Relaxed) {
                        if let Ok(g) = bp.get_page_for_read(hot[i]) {
                            std::hint::black_box(&g[0]);
                        }
                        i = if i + 1 == hot.len() { 0 } else { i + 1 };
                    }
                });
            }
            warm_bar.wait();
            std::thread::sleep(Duration::from_secs(args.warmup));
            warm_flag.store(false, Ordering::Relaxed);
        });
        println!("Warmup done");
    }

    // Baseline coverage snapshot — we report *windowed* deltas so the
    // steady-state before phase 1 doesn't poison the post-shift signal.
    let (base_hits, base_total) = bp.sample_coverage();

    std::thread::scope(|s| {
        // Workers. Each worker keeps a thread-local copy of every phase's
        // hot set so it can refresh its own frame_id hint after each access
        // — otherwise LIPAH (and PT(FP)) see stale hints when a page gets
        // evicted + re-placed elsewhere, which would artificially depress
        // their coverage after a phase shift.
        for tid in 0..args.threads {
            let bp = &bp;
            let hot_sets = &hot_sets;
            let phase = &phase;
            let flag = &flag;
            let barrier = &barrier;
            let threads = args.threads;
            s.spawn(move || {
                let mut local: Vec<Vec<PageFrameKey>> =
                    hot_sets.iter().map(|v| v.to_vec()).collect();
                let mut i = tid;
                barrier.wait();
                while flag.load(Ordering::Relaxed) {
                    let p = phase.load(Ordering::Relaxed);
                    let hot = &mut local[p];
                    let idx = i % hot.len();
                    if let Ok(g) = bp.get_page_for_read(hot[idx]) {
                        std::hint::black_box(&g[0]);
                        if let Some(pfk) = g.page_frame_key() {
                            hot[idx] = pfk;
                        }
                    }
                    i = i.wrapping_add(threads);
                }
            });
        }

        // Sampler thread: prints one TSV line per window. Not gated by the
        // start barrier — it may emit a couple of near-zero windows before
        // workers ramp up, which is fine (deltas are still correct).
        let sampler = {
            let bp = bp.clone();
            let phase = phase.clone();
            let flag = flag.clone();
            let interval = Duration::from_millis(args.sample_interval_ms);
            s.spawn(move || {
                let t0 = Instant::now();
                let mut prev_hits = base_hits;
                let mut prev_total = base_total;
                println!("# t_ms\tphase\tdelta_hits\tdelta_total\tcoverage_window");
                while flag.load(Ordering::Relaxed) {
                    std::thread::sleep(interval);
                    let (hits, total) = bp.sample_coverage();
                    let dh = hits.saturating_sub(prev_hits);
                    let dt = total.saturating_sub(prev_total);
                    let cov = if dt == 0 { 0.0 } else { dh as f64 / dt as f64 };
                    println!(
                        "{}\t{}\t{}\t{}\t{:.4}",
                        t0.elapsed().as_millis(),
                        phase.load(Ordering::Relaxed),
                        dh,
                        dt,
                        cov
                    );
                    prev_hits = hits;
                    prev_total = total;
                }
            })
        };

        barrier.wait();
        // Rotate through phases.
        for p in 0..args.num_phases {
            phase.store(p, Ordering::Relaxed);
            eprintln!("=== phase={} ===", p);
            std::thread::sleep(Duration::from_secs(args.phase_interval));
        }
        flag.store(false, Ordering::Relaxed);
        sampler.join().unwrap();
    });

    bp.print_profile();
}
