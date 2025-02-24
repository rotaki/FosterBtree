use clap::Parser;
use fbtree::{
    bench_utils::*,
    bp::{get_test_bp, ContainerKey, MemPool},
    prelude::{FosterBtree, UniqueKeyIndex},
    random::RandomKVs,
};
use std::{collections::BTreeMap, sync::Arc};

fn measure_time(title: &str, f: impl FnOnce()) {
    let start = std::time::Instant::now();
    f();
    let elapsed = start.elapsed();
    println!("{}: {:?}", title, elapsed);
}

#[derive(Debug, Parser, Clone)]
pub struct Params {
    #[clap(short = 't', long, default_value = "3")]
    pub num_threads: usize,
    #[clap(short = 'n', long, default_value = "1000000")]
    pub num_keys: usize,
    #[clap(long, default_value = "100")]
    pub key_size: usize,
    #[clap(long, default_value = "50")]
    pub val_min_size: usize,
    #[clap(long, default_value = "100")]
    pub val_max_size: usize,
}

fn main() {
    #[cfg(feature = "stat")]
    println!("Stat is enabled");
    #[cfg(not(feature = "stat"))]
    println!("Stat is disabled");

    let params = Params::parse();
    println!("Params: {:?}", params);

    let kvs = RandomKVs::new(
        true,
        false,
        params.num_threads,
        params.num_keys,
        params.key_size,
        params.val_min_size,
        params.val_max_size,
    );
    let bp_size = 10000;

    measure_time("Non-BP Foster BTree Insertion", || {
        insert_into_foster_tree(gen_foster_btree_in_mem(), &kvs)
    });

    measure_time("Non-BP Foster BTree Insertion Parallel", || {
        insert_into_foster_tree_parallel(gen_foster_btree_in_mem(), &kvs)
    });

    measure_time("BP Foster BTree Insertion", || {
        insert_into_foster_tree(gen_foster_btree_on_disk(bp_size), &kvs)
    });

    measure_time("BP Foster BTree Insertion Parallel", || {
        insert_into_foster_tree_parallel(gen_foster_btree_on_disk(bp_size), &kvs)
    });

    measure_time("BTreeMap Insertion", || {
        insert_into_btree_map(BTreeMap::new(), &kvs)
    });

    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let bp = get_test_bp(bp_size);
    let btree = Arc::new(FosterBtree::new(c_key, bp.clone()));

    let start = std::time::Instant::now();
    std::thread::scope(|s| {
        for partition in kvs.iter() {
            let btree = btree.clone();
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    btree.insert(k, v).unwrap();
                }
            });
        }
    });
    let elapsed = start.elapsed();
    println!("BP Foster BTree Insertion Parallel: {:?}", elapsed);

    // BP stats
    let stats = bp.stats();
    println!("BP Stats: {}", stats);
    bp.clear_dirty_flags().unwrap();
}
