use clap::Parser;
use fbtree::{bench_utils::*, bp::MemPool, random::RandomKVs};

fn main() {
    let mut insert_params = BenchParams::parse();
    insert_params.ops_ratio = "1:0:0:0".to_string(); // Only insertions are done in this snipp

    let mut get_params = insert_params.clone();
    get_params.ops_ratio = "0:0:0:1".to_string(); // Only gets are done in this snippet

    println!("{}", insert_params);

    let bp_size = insert_params.bp_size;
    let tree = gen_foster_btree_on_disk(bp_size);

    let kvs = RandomKVs::new(
        insert_params.unique_keys,
        false,
        insert_params.num_threads,
        insert_params.num_keys,
        insert_params.key_size,
        insert_params.val_min_size,
        insert_params.val_max_size,
    );

    run_bench(insert_params, kvs, tree.clone());

    println!("After Insertion\n{}", tree.mem_pool.stats());

    println!("Btree page stats: ");
    println!("{}", tree.page_stats(false));

    tree.mem_pool.flush_all().unwrap();
    println!("After Flushing\n{}", tree.mem_pool.stats());
    tree.mem_pool.reset_stats();
    println!("After Resetting Stats\n{}", tree.mem_pool.stats());

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", tree.mem_pool.eviction_stats());
        println!("File stats: ");
        println!("{}", tree.mem_pool.file_stats());
        println!("Btree op stats: ");
        println!("{}", tree.op_stats());
        // println!("Btree page stats: ");
        // println!("{}", tree.page_stats(false));
    }

    let kvs = RandomKVs::new(
        get_params.unique_keys,
        false,
        get_params.num_threads,
        get_params.num_keys,
        get_params.key_size,
        get_params.val_min_size,
        get_params.val_max_size,
    );

    // Done inserting
    run_bench(get_params, kvs, tree.clone());

    println!("After Get\n{}", tree.mem_pool.stats());
    tree.mem_pool.flush_all().unwrap();
    println!("After Flushing\n{}", tree.mem_pool.stats());
    tree.mem_pool.reset_stats();
    println!("After Resetting Stats\n{}", tree.mem_pool.stats());

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", tree.mem_pool.eviction_stats());
        println!("File stats: ");
        println!("{}", tree.mem_pool.file_stats());
        println!("Btree op stats: ");
        println!("{}", tree.op_stats());
        println!("Btree page stats: ");
        println!("{}", tree.page_stats(false));
    }
}
