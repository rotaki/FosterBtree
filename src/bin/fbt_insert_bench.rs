use clap::{Parser, ValueEnum};
use fbtree::{
    bp::{get_test_bp, get_test_bp_clock, get_test_vmcache, ContainerKey, MemPool},
    prelude::{FosterBtree, UniqueKeyIndex},
    random::RandomKVs,
};
use std::sync::Arc;

#[allow(dead_code)]
fn measure_time(title: &str, f: impl FnOnce()) {
    let start = std::time::Instant::now();
    f();
    let elapsed = start.elapsed();
    println!("{}: {:?}", title, elapsed);
}

/// Which buffer‑pool implementation to use.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum BPType {
    BPLRU,
    BPClock,
    VMCache,
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
    /// Buffer‑pool type
    #[arg(long, value_enum, default_value_t = BPType::BPLRU)]
    pub bp_type: BPType,
}

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let bp_size = 100000;
    match params.bp_type {
        BPType::BPLRU => {
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
            let stats = unsafe { bp.stats() };
            println!("BP Stats: {}", stats);
            bp.clear_dirty_flags().unwrap();
        }
        BPType::BPClock => {
            let bp = get_test_bp_clock::<128>(bp_size);
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
            let stats = unsafe { bp.stats() };
            println!("BP Stats: {}", stats);
            bp.clear_dirty_flags().unwrap();
        }
        BPType::VMCache => {
            let bp = get_test_vmcache::<false, 128>(bp_size);
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
            let stats = unsafe { bp.stats() };
            println!("BP Stats: {}", stats);
            bp.clear_dirty_flags().unwrap();
        }
    };
}
