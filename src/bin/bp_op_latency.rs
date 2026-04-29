//! Micro-benchmark: per-operation latency for Clock vs PT vs PT-FP buffer pools.
//!
//! Measures create_new_page_for_write, get_page_for_read, and get_page_for_write
//! on BufferPoolClock (LIPAH), PrediCache.
//!
//! Usage:
//!   cargo run --release --bin bp_op_latency [-- --num-pages <N> --num-frames <F> --iters <I>]

use std::sync::Arc;
use std::time::Instant;

use fbtree::bp::{
    get_test_bp_clock, get_test_predicache, ContainerKey, MemPool, PageFrameKey,
};

struct BenchResult {
    create_new_ns: f64,
    get_read_ns: f64,
    get_write_ns: f64,
}

/// Run the benchmark on any MemPool implementation.
///
/// 1. Creates `num_pages` pages (timed).
/// 2. Reads them `iters` times in a cycle (timed).
/// 3. Writes them `iters` times in a cycle (timed).
///
/// All pages fit in the buffer pool (no eviction during reads/writes).
fn bench<T: MemPool>(bp: &Arc<T>, num_pages: usize, iters: usize) -> BenchResult {
    let c_key = ContainerKey::new(0, 0);

    // --- create_new_page_for_write ---
    let mut frame_ids = Vec::with_capacity(num_pages);

    // Warmup: create a few pages first
    let warmup_creates = 100.min(num_pages);
    for _ in 0..warmup_creates {
        let g = bp.create_new_page_for_write(c_key).unwrap();
        frame_ids.push(g.frame_id());
        drop(g);
    }

    // Timed creates for the remaining pages
    let create_count = num_pages - warmup_creates;
    let start = Instant::now();
    for _ in 0..create_count {
        let g = bp.create_new_page_for_write(c_key).unwrap();
        frame_ids.push(g.frame_id());
        drop(g);
    }
    let create_elapsed = start.elapsed();

    // --- Build keys with frame_id hints ---
    let keys: Vec<PageFrameKey> = (0..num_pages as u32)
        .map(|pid| PageFrameKey::new_with_frame_id(c_key, pid, frame_ids[pid as usize]))
        .collect();

    // --- get_page_for_read ---
    // Warmup reads
    for i in 0..100.min(num_pages) {
        let _g = bp.get_page_for_read(keys[i]).unwrap();
    }

    // Timed reads — cycle through all pages
    let start = Instant::now();
    for i in 0..iters {
        let idx = i % num_pages;
        let _g = bp.get_page_for_read(keys[idx]).unwrap();
    }
    let read_elapsed = start.elapsed();

    // --- get_page_for_write ---
    // Warmup writes
    for i in 0..100.min(num_pages) {
        let _g = bp.get_page_for_write(keys[i]).unwrap();
    }

    // Timed writes
    let start = Instant::now();
    for i in 0..iters {
        let idx = i % num_pages;
        let _g = bp.get_page_for_write(keys[idx]).unwrap();
    }
    let write_elapsed = start.elapsed();

    BenchResult {
        create_new_ns: if create_count > 0 {
            create_elapsed.as_nanos() as f64 / create_count as f64
        } else {
            0.0
        },
        get_read_ns: read_elapsed.as_nanos() as f64 / iters as f64,
        get_write_ns: write_elapsed.as_nanos() as f64 / iters as f64,
    }
}

fn print_result(name: &str, r: &BenchResult) {
    println!("--- {} ---", name);
    println!("  create_new_page_for_write : {:.1} ns/op", r.create_new_ns);
    println!("  get_page_for_read         : {:.1} ns/op", r.get_read_ns);
    println!("  get_page_for_write        : {:.1} ns/op", r.get_write_ns);
    println!();
}

fn main() {
    let mut num_pages: usize = 5000;
    let mut num_frames: usize = 10000;
    let mut iters: usize = 2_000_000;

    // Simple arg parsing
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--num-pages" => {
                num_pages = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--num-frames" => {
                num_frames = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--iters" => {
                iters = args[i + 1].parse().unwrap();
                i += 2;
            }
            _ => {
                eprintln!("Unknown arg: {}", args[i]);
                i += 1;
            }
        }
    }

    // Ensure enough frames so all pages stay resident (no eviction noise)
    let effective_frames = num_frames.max(num_pages + 500);

    println!("=== Buffer Pool Operation Latency Benchmark ===");
    println!("  num_pages  = {}", num_pages);
    println!("  num_frames = {}", effective_frames);
    println!("  iters      = {}", iters);
    println!();

    // --- Clock (LIPAH) ---
    {
        let bp = get_test_bp_clock::<64>(effective_frames);
        let r = bench(&bp, num_pages, iters);
        print_result("Clock (LIPAH)", &r);
    }

}
