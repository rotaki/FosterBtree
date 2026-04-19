//! Micro-benchmark: pure page translation cost.
//!
//! Isolates the cost of `get_page_for_read` by doing bare random page lookups
//! with no B-tree traversal, no transaction logic, no commits. Each operation
//! is a single page access, so translation cost is a large fraction of total.
//!
//! Supports zipfian (skewed) and uniform access distributions.
//!
//! Usage:
//!   cargo run --release --bin bp_translation_bench --features bp_clock \
//!     -- --num-pages 100000 --num-frames 200000 --threads 1 --seconds 5 --theta 0.8

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use clap::Parser;
use fbtree::bp::{ContainerKey, MemPool, PageFrameKey};
use fbtree::random::small_thread_rng;
use fbtree::zipfan::FastZipf;
use rand::RngCore;

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
}

fn get_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "bp_clock")]
    {
        use fbtree::bp::get_test_bp_clock;
        return get_test_bp_clock::<64>(num_frames);
    }
    #[cfg(feature = "bp_pt_bucket")]
    {
        use fbtree::bp::get_test_pt_bucket_validate;
        return get_test_pt_bucket_validate(num_frames);
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
    #[cfg(feature = "bp_pt")]
    {
        use fbtree::bp::get_test_pt;
        return get_test_pt(num_frames);
    }
    #[cfg(not(any(
        feature = "bp_clock",
        feature = "bp_pt_bucket",
        feature = "bp_pt_tlb",
        feature = "bp_pt_tlb_only",
        feature = "bp_pt_tlb_only_keys",
        feature = "bp_tlb",
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

    println!("=== BP Translation Micro-Benchmark ===");
    println!(
        "pages={} frames={} threads={} seconds={} theta={} warmup={} sequential={}",
        args.num_pages, args.num_frames, args.threads, args.seconds, args.theta, args.warmup, args.sequential
    );

    // Create BP and pre-populate pages.
    let bp = get_bp(args.num_frames);
    let mut keys: Vec<PageFrameKey> = Vec::with_capacity(args.num_pages);
    for _ in 0..args.num_pages {
        let g = bp.create_new_page_for_write(c_key).unwrap();
        keys.push(g.page_frame_key().unwrap());
    }
    let keys = Arc::new(keys);
    let num_pages = args.num_pages;
    println!("Created {} pages", num_pages);

    // Warmup
    if args.warmup > 0 {
        println!("Warming up for {}s...", args.warmup);
        let flag = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(args.threads + 1));

        std::thread::scope(|s| {
            for _ in 0..args.threads {
                let bp = &bp;
                let keys = &keys;
                let flag = &flag;
                let barrier = &barrier;
                let sequential = args.sequential;
                s.spawn(move || {
                    let mut rng = small_thread_rng();
                    let mut seq_idx: usize = 0;
                    barrier.wait();
                    while flag.load(Ordering::Relaxed) {
                        let idx = if sequential {
                            let i = seq_idx;
                            seq_idx = (seq_idx + 1) % num_pages;
                            i
                        } else {
                            (rng.next_u64() as usize) % num_pages
                        };
                        let _ = bp.get_page_for_read(keys[idx]);
                    }
                });
            }
            barrier.wait();
            std::thread::sleep(std::time::Duration::from_secs(args.warmup));
            flag.store(false, Ordering::Relaxed);
        });
        println!("Warmup done");
    }

    // Benchmark
    let flag = Arc::new(AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(args.threads + 1));

    let results: Vec<u64> = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..args.threads {
            let bp = &bp;
            let keys = &keys;
            let flag = &flag;
            let barrier = &barrier;
            let theta = args.theta;
            let sequential = args.sequential;

            let h = s.spawn(move || {
                let rng = small_thread_rng();
                let mut zipf = if theta > 0.0 {
                    Some(FastZipf::new(rng, theta, num_pages))
                } else {
                    None
                };
                let mut uniform_rng = small_thread_rng();
                let mut seq_idx: usize = 0;
                let mut ops: u64 = 0;

                barrier.wait();

                while flag.load(Ordering::Relaxed) {
                    let idx = if sequential {
                        let i = seq_idx;
                        seq_idx = (seq_idx + 1) % num_pages;
                        i
                    } else if let Some(ref mut z) = zipf {
                        z.sample()
                    } else {
                        (uniform_rng.next_u64() as usize) % num_pages
                    };
                    let _ = bp.get_page_for_read(keys[idx]);
                    ops += 1;
                }
                ops
            });
            handles.push(h);
        }

        barrier.wait();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_secs(args.seconds));
        flag.store(false, Ordering::Relaxed);
        let elapsed = start.elapsed();

        let ops: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let total_ops: u64 = ops.iter().sum();
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

        for (i, &o) in ops.iter().enumerate() {
            println!(
                "  Thread {}: {} ops ({:.2} Mops/s)",
                i,
                o,
                o as f64 / elapsed.as_secs_f64() / 1_000_000.0
            );
        }

        ops
    });

    let _ = results;
}
