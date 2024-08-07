use clap::Parser;
use fbtree::{bench_utils::*, random::RandomKVs};
use std::time::Instant;

fn main() {
    let mut insert_params = BenchParams::parse();
    insert_params.ops_ratio = "1:0:0:0".to_string(); // Only insertions are done in this snipp
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

    println!("Insert Params:\n{}", insert_params);
    println!("Get Params:\n{}", get_params);

    println!("------------------------------------------------------- Pointer Swizzling OFF (disabled)\n");
    let phm_no_ps = gen_paged_hash_map_on_disk_without_pointer_swizzling(bp_size, bucket_num);

    let ipc = insert_params.clone();
    let ikc = insert_kvs.clone();
    // clear_cache();
    // Measure insertion time without pointer swizzling
    let start_insert_no_ps = Instant::now();
    run_bench_for_paged_hash_map(ipc, ikc, &phm_no_ps);
    let duration_insert_no_ps = start_insert_no_ps.elapsed();
    println!(
        "Insertion Time without Pointer Swizzling: {:?}\n",
        duration_insert_no_ps
    );

    println!("After Insertion\n{:?}\n", phm_no_ps.bp.stats());
    phm_no_ps.bp.flush_all().unwrap();
    println!("After Flushing\n{:?}\n", phm_no_ps.bp.stats());
    phm_no_ps.bp.reset_stats();
    println!("After Resetting Stats\n{:?}\n", phm_no_ps.bp.stats());

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm_no_ps.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm_no_ps.bp.file_stats());
    }
    // Done inserting

    let gpc = get_params.clone();
    let gkc = get_kvs.clone();
    // clear_cache();
    // Measure get time without pointer swizzling
    let start_get_no_ps = Instant::now();
    run_bench_for_paged_hash_map(gpc, gkc, &phm_no_ps);
    let duration_get_no_ps = start_get_no_ps.elapsed();
    println!(
        "Get Time without Pointer Swizzling: {:?}\n",
        duration_get_no_ps
    );

    println!("After Get\n{:?}\n", phm_no_ps.bp.stats());
    phm_no_ps.bp.flush_all().unwrap();
    println!("After Flushing\n{:?}\n", phm_no_ps.bp.stats());
    phm_no_ps.bp.reset_stats();
    println!("After Resetting Stats\n{:?}\n", phm_no_ps.bp.stats());
    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm_no_ps.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm_no_ps.bp.file_stats());
        println!("PagedHashMap stats: ");
        println!("{}", phm_no_ps.stats());
        phm_no_ps.reset_stats();
        // println!("PagedHashMap stats after clear(): ");
        // println!("{}", phm_no_ps.stats());
    }

    println!(
        "------------------------------------------------------- Pointer swizzling ON (enabled)\n"
    );
    let phm = gen_paged_hash_map_on_disk(bp_size, bucket_num);

    let ipc = insert_params.clone();
    let ikc = insert_kvs.clone();
    // clear_cache();
    // Measure insertion time
    let start_insert = Instant::now();
    run_bench_for_paged_hash_map(ipc, ikc, &phm);
    let duration_insert = start_insert.elapsed();
    println!(
        "Insertion Time with Pointer Swizzling: {:?}\n",
        duration_insert
    );

    println!("After Insertion\n{:?}\n", phm.bp.stats());
    phm.bp.flush_all().unwrap();
    println!("After Flushing\n{:?}\n", phm.bp.stats());
    phm.bp.reset_stats();
    println!("After Resetting Stats\n{:?}\n", phm.bp.stats());

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm.bp.file_stats());
    }
    // Done inserting

    let gpc = get_params.clone();
    let gkc = get_kvs.clone();
    // clear_cache();
    // Measure get time
    let start_get = Instant::now();
    run_bench_for_paged_hash_map(gpc, gkc, &phm);
    let duration_get = start_get.elapsed();
    println!("Get Time with Pointer Swizzling: {:?}\n", duration_get);

    println!("After Get\n{:?}\n", phm.bp.stats());
    phm.bp.flush_all().unwrap();
    println!("After Flushing\n{:?}\n", phm.bp.stats());
    phm.bp.reset_stats();
    println!("After Resetting Stats\n{:?}\n", phm.bp.stats());
    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm.bp.file_stats());
        println!("PagedHashMap stats: ");
        println!("{}", phm.stats());
        phm.reset_stats();
        // println!("PagedHashMap stats after clear(): ");
        // println!("{}", phm.stats());
    }

    println!("------------------------------------------------------- Summary\n");

    println!("BufferPool size: {}", bp_size);
    println!("Bucket number: {}", bucket_num);
    println!("Thread count: {}\n", insert_params.num_threads);

    println!("Chain stats: {}", phm.get_chain_stats());

    println!(
        "Duration Insert without Pointer Swizzling: {:?}",
        duration_insert_no_ps
    );
    println!(
        "Duration Insert with Pointer Swizzling: {:?}",
        duration_insert
    );

    println!(
        "Duration Get without Pointer Swizzling: {:?}",
        duration_get_no_ps
    );
    println!("Duration Get with Pointer Swizzling: {:?}", duration_get);
}
