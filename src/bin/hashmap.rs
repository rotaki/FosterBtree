use std::time::Instant;

use clap::Parser;
use fbtree::{bench_utils::*, random::RandomKVs};
use std::process::Command;

fn main() {
    let mut bench_params = BenchParams::parse();
    bench_params.bp_size = 100_000;
    bench_params.ops_ratio = "0:0:0:1".to_string();
    bench_params.num_keys = 200_000;
    println!("{}", bench_params);

    let bp_size = bench_params.bp_size;
    // let phm_in_mem = gen_paged_hash_map_in_mem();
    let phm_on_disk = gen_paged_hash_map_on_disk(bp_size);
    let rhm = gen_rust_hash_map();
    // let phm = gen_paged_hash_map_on_disk_with_hash_eviction_policy(bp_size);

    let kvs = RandomKVs::new(
        bench_params.unique_keys,
        false,
        bench_params.num_threads,
        bench_params.num_keys,
        bench_params.key_size,
        bench_params.val_min_size,
        bench_params.val_max_size,
    );

    // insert_into_paged_hash_map(&phm_in_mem, &kvs);
    insert_into_paged_hash_map(&phm_on_disk, &kvs);
    insert_into_rust_hash_map(&rhm, &kvs);

    let iterations = 20;
    // let mut in_mem_paged_hash_map_times = vec![];
    let mut on_disk_paged_hash_map_times = vec![];
    let mut rust_hash_map_times = vec![];

    for _ in 0..iterations {
        // clear_cache();

        // let start_time = Instant::now();
        // get_from_paged_hash_map(&phm_in_mem, &kvs);
        // let elapsed_time = start_time.elapsed();
        // in_mem_paged_hash_map_times.push(elapsed_time.as_millis());

        clear_cache();

        let start_time = Instant::now();
        get_from_paged_hash_map(&phm_on_disk, &kvs);
        let elapsed_time = start_time.elapsed();
        on_disk_paged_hash_map_times.push(elapsed_time.as_millis());

        clear_cache();

        let start_time = Instant::now();
        get_from_rust_hash_map(&rhm, &kvs);
        let elapsed_time = start_time.elapsed();
        rust_hash_map_times.push(elapsed_time.as_millis());
    }

    fn calculate_stats(times: &[u128]) -> (f64, f64, u128, u128) {
        let mean = {
            let sum = times.iter().sum::<u128>() as f64;
            sum / times.len() as f64
        };
        let stddev = {
            let sum = times
                .iter()
                .map(|&time| (time as f64 - mean).powi(2))
                .sum::<f64>();
            (sum / times.len() as f64).sqrt()
        };
        let min = *times.iter().min().unwrap();
        let max = *times.iter().max().unwrap();
        (mean, stddev, min, max)
    }

    // let (mean, stddev, min, max) = calculate_stats(&in_mem_paged_hash_map_times);
    // println!(
    //     "In-Memory Paged Hash Map - Mean: {:.2} ms, StdDev: {:.2} ms, Min: {} ms, Max: {} ms",
    //     mean, stddev, min, max
    // );

    let (mean, stddev, min, max) = calculate_stats(&on_disk_paged_hash_map_times);
    println!(
        "On-Disk Paged Hash Map - Mean: {:.2} ms, StdDev: {:.2} ms, Min: {} ms, Max: {} ms",
        mean, stddev, min, max
    );

    let (mean, stddev, min, max) = calculate_stats(&rust_hash_map_times);
    println!(
        "Rust Hash Map - Mean: {:.2} ms, StdDev: {:.2} ms, Min: {} ms, Max: {} ms\n",
        mean, stddev, min, max
    );

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm_on_disk.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm_on_disk.bp.file_stats());
        println!("PagedHashMap stats: ");
        println!("{}", phm_on_disk.stats());
    }
}

fn clear_cache() {
    // Unix-based system cache clearing
    let _ = Command::new("sync").status();
    let _ = Command::new("echo 3 > /proc/sys/vm/drop_caches").status();
}
