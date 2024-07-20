use clap::Parser;
use fbtree::{bench_utils::*, bp::MemPool, random::RandomKVs};
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
    get_params.ops_ratio = "0:0:0:1".to_string(); // Only gets are done in this snippet
    let get_kvs = RandomKVs::new(
        get_params.unique_keys,
        false,
        get_params.num_threads,
        get_params.num_keys,
        get_params.key_size,
        get_params.val_min_size,
        get_params.val_max_size,
    );
    
    println!("Insert Params:\n{}", insert_params);
    println!("Get Params:\n{}", get_params);

    let bp_size = insert_params.bp_size;

    let phm = gen_paged_hash_map_on_disk(bp_size);
    run_bench_for_paged_hash_map(insert_params.clone(), insert_kvs.clone(), &phm);
    println!("After Insertion\n{}", phm.bp.stats());
    phm.bp.flush_all().unwrap();
    println!("After Flushing\n{}", phm.bp.stats());
    phm.bp.reset_stats();
    println!("After Resetting Stats\n{}", phm.bp.stats());
    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm.bp.file_stats());
    }
    // Done inserting
    run_bench_for_paged_hash_map(get_params.clone(), get_kvs.clone(), &phm);
    println!("After Get\n{}", phm.bp.stats());
    phm.bp.flush_all().unwrap();
    println!("After Flushing\n{}", phm.bp.stats());
    phm.bp.reset_stats();
    println!("After Resetting Stats\n{}", phm.bp.stats());
    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm.bp.file_stats());
    }

    println!("Now, No Pointer Swizzling\n");
    let phm_no_ps = gen_paged_hash_map_on_disk_without_ponter_swizzling(bp_size);

    run_bench_for_paged_hash_map(insert_params, insert_kvs, &phm_no_ps);
    println!("After Insertion\n{}", phm_no_ps.bp.stats());
    phm_no_ps.bp.flush_all().unwrap();
    println!("After Flushing\n{}", phm_no_ps.bp.stats());
    phm_no_ps.bp.reset_stats();
    println!("After Resetting Stats\n{}", phm_no_ps.bp.stats());
    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm_no_ps.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm_no_ps.bp.file_stats());
    }
    // Done inserting
    run_bench_for_paged_hash_map(get_params, get_kvs, &phm_no_ps);
    println!("After Get\n{}", phm_no_ps.bp.stats());
    phm_no_ps.bp.flush_all().unwrap();
    println!("After Flushing\n{}", phm_no_ps.bp.stats());
    phm_no_ps.bp.reset_stats();
    println!("After Resetting Stats\n{}", phm_no_ps.bp.stats());
    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", phm_no_ps.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm_no_ps.bp.file_stats());
    }
}
