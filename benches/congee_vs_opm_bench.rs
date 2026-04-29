/// Microbenchmark: Congee vs OptimisticPageMap
///
/// Tests raw lookup performance of:
/// 1. OptimisticPageMap (used by PT - custom buckets + chaining with OLC)
/// 2. CongeeRawU32 (used by TLB-BP - adaptive radix tree)
///
/// This tells us if switching PT's overflow table to Congee would be faster.

use congee::CongeeRawU32;
use fbtree::bp::mem_pool_trait::{ContainerKey, PageKey};
use fbtree::bp::optimistic_page_map::OptimisticPageMap;
use rand::Rng;
use std::sync::Arc;
use std::time::Instant;

// Simulate frame IDs
type FrameId = u32;

// Pack PageKey into usize (same as TLB-BP)
#[inline(always)]
fn pack_page_key(key: &PageKey) -> usize {
    ((key.c_key.as_u32() as usize) << 32) | key.page_id as usize
}

fn bench_optimistic_page_map(
    name: &str,
    num_pages: usize,
    num_accesses: usize,
    threads: usize,
) -> f64 {
    println!("\n=== {} ===", name);
    println!(
        "Pages: {}, Accesses: {}, Threads: {}",
        num_pages, num_accesses, threads
    );

    // Create overflow table (PT uses 2× num_frames buckets)
    let num_buckets = num_pages * 2;
    let table = Arc::new(OptimisticPageMap::new(num_buckets));

    // Populate with pages
    println!("Populating OptimisticPageMap ({} buckets)...", num_buckets);
    for i in 0..num_pages {
        let page_key = PageKey::new(ContainerKey::from_u32(0), i as u32);
        table.insert(page_key, i as u32);
    }

    // Warmup
    println!("Warming up...");
    let mut rng = rand::thread_rng();
    for _ in 0..10000 {
        let page_id = rng.gen_range(0..num_pages) as u32;
        let page_key = PageKey::new(ContainerKey::from_u32(0), page_id);
        let bucket_idx = table.bucket_index_pub(&page_key);
        let _ = table.lookup_with_bucket(&page_key, bucket_idx);
    }

    // Benchmark parallel access
    println!("Running benchmark...");
    let start = Instant::now();

    std::thread::scope(|s| {
        for _ in 0..threads {
            let table = Arc::clone(&table);
            s.spawn(move || {
                let mut rng = rand::thread_rng();
                let accesses_per_thread = num_accesses / threads;

                for _ in 0..accesses_per_thread {
                    let page_id = rng.gen_range(0..num_pages) as u32;
                    let page_key = PageKey::new(ContainerKey::from_u32(0), page_id);

                    // Simulate what PT does: bucket lookup
                    let bucket_idx = table.bucket_index_pub(&page_key);
                    let _result = table.lookup_with_bucket(&page_key, bucket_idx);
                }
            });
        }
    });

    let elapsed = start.elapsed();
    let throughput = num_accesses as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("Duration: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.2} Mops/s", throughput);
    println!("Latency: {:.0} ns/op", elapsed.as_nanos() as f64 / num_accesses as f64);

    throughput
}

fn bench_congee(
    name: &str,
    num_pages: usize,
    num_accesses: usize,
    threads: usize,
) -> f64 {
    println!("\n=== {} ===", name);
    println!(
        "Pages: {}, Accesses: {}, Threads: {}",
        num_pages, num_accesses, threads
    );

    // Create Congee table (adaptive radix tree, keyed by packed usize)
    let table: Arc<CongeeRawU32<usize>> = Arc::new(CongeeRawU32::default());

    // Populate with pages
    println!("Populating CongeeRawU32...");
    for i in 0..num_pages {
        let page_key = PageKey::new(ContainerKey::from_u32(0), i as u32);
        let guard = crossbeam_epoch::pin();
        let _ = table.insert(pack_page_key(&page_key), i as u32, &guard);
    }

    // Warmup
    println!("Warming up...");
    let mut rng = rand::thread_rng();
    for _ in 0..10000 {
        let page_id = rng.gen_range(0..num_pages) as u32;
        let page_key = PageKey::new(ContainerKey::from_u32(0), page_id);
        let guard = crossbeam_epoch::pin();
        let _ = table.get(&pack_page_key(&page_key), &guard);
    }

    // Benchmark parallel access
    println!("Running benchmark...");
    let start = Instant::now();

    std::thread::scope(|s| {
        for _ in 0..threads {
            let table = Arc::clone(&table);
            s.spawn(move || {
                let mut rng = rand::thread_rng();
                let accesses_per_thread = num_accesses / threads;

                for _ in 0..accesses_per_thread {
                    let page_id = rng.gen_range(0..num_pages) as u32;
                    let page_key = PageKey::new(ContainerKey::from_u32(0), page_id);

                    // Direct Congee lookup (what TLB-BP does)
                    let guard = crossbeam_epoch::pin();
                    let _result = table.get(&pack_page_key(&page_key), &guard);
                }
            });
        }
    });

    let elapsed = start.elapsed();
    let throughput = num_accesses as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("Duration: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.2} Mops/s", throughput);
    println!("Latency: {:.0} ns/op", elapsed.as_nanos() as f64 / num_accesses as f64);

    throughput
}

fn main() {
    println!("=== Congee vs PT Overflow Table Microbenchmark ===\n");

    let configs = vec![
        ("Small (10k pages)", 10_000, 10_000_000, 12),
        ("Medium (100k pages)", 100_000, 10_000_000, 12),
        ("Large (1M pages)", 1_000_000, 10_000_000, 12),
    ];

    let mut results = Vec::new();

    for (name, num_pages, num_accesses, threads) in configs {
        println!("\n{}", "=".repeat(60));
        println!("Configuration: {}", name);
        println!("{}", "=".repeat(60));

        let overflow_throughput =
            bench_optimistic_page_map(&format!("OptimisticPageMap - {}", name), num_pages, num_accesses, threads);

        let congee_throughput =
            bench_congee(&format!("CongeeRawU32 - {}", name), num_pages, num_accesses, threads);

        let speedup = congee_throughput / overflow_throughput;
        results.push((name, overflow_throughput, congee_throughput, speedup));

        println!("\n--- Comparison ---");
        println!("PT Overflow: {:.2} Mops/s", overflow_throughput);
        println!("Pure Congee: {:.2} Mops/s", congee_throughput);
        println!("Speedup: {:.2}x", speedup);
    }

    println!("\n\n{}", "=".repeat(60));
    println!("SUMMARY");
    println!("{}\n", "=".repeat(60));

    println!("{:<25} {:>15} {:>15} {:>10}", "Configuration", "OptimisticPageMap", "CongeeRawU32", "Speedup");
    println!("{:-<70}", "");
    for (name, overflow, congee, speedup) in results {
        println!(
            "{:<25} {:>12.2} Mops/s {:>12.2} Mops/s {:>9.2}x",
            name, overflow, congee, speedup
        );
    }

    println!("\nInterpretation:");
    println!("- Speedup > 1.0: Congee is faster → PT should switch to Congee");
    println!("- Speedup < 1.0: OptimisticPageMap is faster → PT should keep current");
    println!("- Speedup ≈ 1.0: No significant difference");
    println!("\nContext:");
    println!("- OptimisticPageMap: PT's custom buckets + chaining (OLC versioning)");
    println!("- CongeeRawU32: TLB-BP's adaptive radix tree (epoch-based GC)");
}
