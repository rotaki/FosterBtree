use clap::Parser;
use fbtree::{
    bench_utils::*,
    bp::{BufferPool, LRUEvictionPolicy},
    prelude::PagedHashMap,
    random::RandomKVs,
};
use std::{sync::Arc, time::Instant};

fn main() {
    let mut insert_params = BenchParams::parse();
    insert_params.ops_ratio = "1:0:0:0".to_string(); // Only insertions are done in this snipp
                                                     // insert_params.bp_size = 20_000;
                                                     // insert_params.bucket_num = 800;
                                                     // insert_params.num_keys = 1_000_000;
                                                     // insert_params.unique_keys = true;

    let insert_kvs = RandomKVs::new(
        insert_params.unique_keys,
        false,
        insert_params.num_threads,
        insert_params.num_keys,
        insert_params.key_size,
        insert_params.val_min_size,
        insert_params.val_max_size,
    );

    let mut get_params = insert_params.clone();
    get_params.ops_ratio = "0:0:0:1".to_string(); // Only gets are done in this snipp

    let get_kvs = RandomKVs::new(
        get_params.unique_keys,
        false,
        get_params.num_threads,
        get_params.num_keys,
        get_params.key_size,
        get_params.val_min_size,
        get_params.val_max_size,
    );

    let bp_size = insert_params.bp_size;
    let bucket_num = insert_params.bucket_num;

    let phm_no_ps = gen_paged_hash_map_on_disk_without_pointer_swizzling(bp_size, bucket_num);
    let phm_shared_ps = gen_paged_hash_map_on_disk(bp_size, bucket_num);
    let phm_unsafe_ps =
        gen_paged_hash_map_on_disk_with_unsafe_pointer_swizzling(bp_size, bucket_num);
    let phm_local_ps = gen_paged_hash_map_on_disk_with_local_pointer_swizzling(bp_size, bucket_num);

    // Helper function to run benchmarks and display results
    fn run_and_display_bench(
        insert_params: BenchParams,
        insert_kvs: Vec<RandomKVs>,
        get_params: BenchParams,
        get_kvs: Vec<RandomKVs>,
        phm: Arc<PagedHashMap<LRUEvictionPolicy, BufferPool<LRUEvictionPolicy>>>,
        description: &str,
    ) -> (std::time::Duration, std::time::Duration) {
        println!(
            "------------------------------------------------------- {}\n",
            description
        );

        let ipc = insert_params.clone();
        let ikc = insert_kvs.clone();
        // Measure insertion time
        let start_insert = Instant::now();
        run_bench_for_paged_hash_map(ipc, ikc, &phm);
        let duration_insert = start_insert.elapsed();
        println!("Insertion Time: {:?}\n", duration_insert);

        #[cfg(feature = "stat")]
        {
            println!("After Insertion\n{}\n", phm.bp.stats());
            phm.bp.flush_all().unwrap();
            println!("After Flushing\n{}\n", phm.bp.stats());
            phm.bp.reset_stats();
            println!("After Resetting Stats\n{}\n", phm.bp.stats());
            println!("BP stats: ");
            println!("{}", phm.bp.eviction_stats());
            println!("File stats: ");
            println!("{}", phm.bp.file_stats());
        }

        let gpc = get_params.clone();
        let gkc = get_kvs.clone();
        // Measure get time
        let start_get = Instant::now();
        run_bench_for_paged_hash_map(gpc, gkc, &phm);
        let duration_get = start_get.elapsed();
        println!("Get Time: {:?}\n", duration_get);

        #[cfg(feature = "stat")]
        {
            println!("After Get\n{}\n", phm.bp.stats());
            phm.bp.flush_all().unwrap();
            println!("After Flushing\n{}\n", phm.bp.stats());
            phm.bp.reset_stats();
            println!("After Resetting Stats\n{}\n", phm.bp.stats());
            println!("BP stats: ");
            println!("{}", phm.bp.eviction_stats());
            println!("File stats: ");
            println!("{}", phm.bp.file_stats());
            println!("PagedHashMap stats: ");
            println!("{}", phm.stats());
            phm.reset_stats();
        }
        (duration_insert, duration_get)
    }

    println!("Insert Params:\n{}", insert_params);
    println!("Get Params:\n{}", get_params);

    let (insert_duration_no_ps, get_duration_no_ps) = run_and_display_bench(
        insert_params.clone(),
        insert_kvs.clone(),
        get_params.clone(),
        get_kvs.clone(),
        phm_no_ps,
        "Pointer Swizzling OFF (disabled)",
    );

    let (insert_duration_shared_ps, get_duration_shared_ps) = run_and_display_bench(
        insert_params.clone(),
        insert_kvs.clone(),
        get_params.clone(),
        get_kvs.clone(),
        phm_shared_ps.clone(),
        "Pointer Swizzling Shared by Thread with Atomic",
    );

    let (insert_duration_unsafe_ps, get_duration_unsafe_ps) = run_and_display_bench(
        insert_params.clone(),
        insert_kvs.clone(),
        get_params.clone(),
        get_kvs.clone(),
        phm_unsafe_ps,
        "Pointer Swizzling Shared by Thread with Unsafe",
    );

    let (insert_duration_local_ps, get_duration_local_ps) = run_and_display_bench(
        insert_params.clone(),
        insert_kvs.clone(),
        get_params.clone(),
        get_kvs.clone(),
        phm_local_ps,
        "Pointer Swizzling Local Thread Unsafe",
    );

    println!("------------------------------------------------------- Summary\n");

    println!("Insert number of keys: {}", insert_params.num_keys);
    println!("BufferPool size: {}", bp_size);
    println!("Bucket number: {}", bucket_num);
    println!("Thread count: {}\n", insert_params.num_threads);

    println!("Chain stats: {}", phm_shared_ps.get_chain_stats());

    println!(
        "Duration Insert without Pointer Swizzling: {:?}",
        insert_duration_no_ps
    );
    println!(
        "Duration Insert with Pointer Swizzling (Atomic Shared): {:?}",
        insert_duration_shared_ps
    );
    println!(
        "Duration Insert with Pointer Swizzling (Unsafe Shared): {:?}",
        insert_duration_unsafe_ps
    );
    println!(
        "Duration Insert with Pointer Swizzling (Thread Local Unsafe): {:?}\n",
        insert_duration_local_ps
    );

    println!(
        "Duration Get without Pointer Swizzling: {:?}",
        get_duration_no_ps
    );
    println!(
        "Duration Get with Pointer Swizzling (Atomic Shared): {:?}",
        get_duration_shared_ps
    );
    println!(
        "Duration Get with Pointer Swizzling (Unsafe Shared): {:?}",
        get_duration_unsafe_ps
    );
    println!(
        "Duration Get with Pointer Swizzling (Thread Local Unsafe): {:?}",
        get_duration_local_ps
    );
}
