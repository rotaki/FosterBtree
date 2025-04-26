use clap::Parser;
use fbtree::{bench_utils::*, bp::MemPool, random::RandomKVs};

fn main() {
    let bench_params = BenchParams::parse();
    println!("{}", bench_params);

    let bp_size = bench_params.bp_size;
    let phm = gen_paged_hash_map_on_disk(bp_size);

    let kvs = RandomKVs::new(
        bench_params.unique_keys,
        false,
        bench_params.num_threads,
        bench_params.num_keys,
        bench_params.key_size,
        bench_params.val_min_size,
        bench_params.val_max_size,
    );

    run_bench_for_paged_hash_map(bench_params, kvs, &phm);

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm.bp.file_stats());
    }
    println!("After Insertion\n{}", unsafe { phm.bp.stats() });
    phm.bp.flush_all().unwrap();
    println!("After Flushing\n{}", unsafe { phm.bp.stats() });
    unsafe { phm.bp.reset_stats() };
    println!("After Resetting Stats\n{}", unsafe { phm.bp.stats() });

    let mut total_len = 0;
    for (k, v) in phm.iter() {
        total_len += k.len() + v.len();
    }

    println!("After Scan\n{}", unsafe { phm.bp.stats() });
    phm.bp.flush_all().unwrap();
    println!("After Flushing\n{}", unsafe { phm.bp.stats() });
    unsafe { phm.bp.reset_stats() };
    println!("After Resetting Stats\n{}", unsafe { phm.bp.stats() });
    println!("Total length of keys and values: {}", total_len);
}
