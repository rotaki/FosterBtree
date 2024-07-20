use std::{collections::BTreeMap, sync::Arc, thread};

use clap::Parser;

use crate::{
    access_method::{fbt::FosterBtree, hashindex::prelude::*},
    bp::{
        get_in_mem_pool, get_test_bp,
        prelude::{
            ContainerKey, DummyEvictionPolicy, EvictionPolicy, InMemPool, LRUEvictionPolicy,
            MemPool,
        },
        BufferPool,
    },
    random::{RandomKVs, RandomOp},
};

#[derive(Debug, Parser, Clone)]
pub struct BenchParams {
    /// Number of threads.
    #[clap(short = 't', long = "num_threads", default_value = "1")]
    pub num_threads: usize,
    /// Unique keys.
    #[clap(short = 'u', long = "unique_keys")]
    pub unique_keys: bool,
    /// Number of keys.
    #[clap(short = 'n', long = "num_keys", default_value = "500000")]
    pub num_keys: usize,
    /// Key size.
    #[clap(short = 'k', long = "key_size", default_value = "100")]
    pub key_size: usize,
    /// Minimum size of the payload.
    #[clap(short = 'i', long = "val_min_size", default_value = "50")]
    pub val_min_size: usize,
    /// Maximum size of the payload.
    #[clap(short = 'a', long = "val_max_size", default_value = "100")]
    pub val_max_size: usize,
    /// Buffer pool size. (Only for on_disk)
    #[clap(short = 'b', long = "bp_size", default_value = "10000")]
    pub bp_size: usize,
    /// Operations ratio: insert:update:delete:get
    #[clap(short = 'r', long = "ops_ratio", default_value = "1:0:0:0")]
    pub ops_ratio: String,
}

#[derive(Debug, Clone, Copy)]
pub enum TreeOperation {
    Insert,
    Update,
    Delete,
    Get,
}

impl BenchParams {
    /// Parses the `ops_ratio` string into a tuple of four usize values representing the ratio of insert, update, delete, and get operations.
    pub fn parse_ops_ratio(&self) -> Vec<(TreeOperation, f64)> {
        let ratios: Vec<&str> = self.ops_ratio.split(':').collect();
        if ratios.len() != 4 {
            panic!("Operations ratio must be in the format insert:update:delete:get");
        }

        let mut ratio = Vec::with_capacity(4);
        for (i, r) in ratios.iter().enumerate() {
            let op = match i {
                0 => TreeOperation::Insert,
                1 => TreeOperation::Update,
                2 => TreeOperation::Delete,
                3 => TreeOperation::Get,
                _ => unreachable!(),
            };
            let r = r.parse::<f64>().unwrap();
            ratio.push((op, r));
        }
        ratio
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        num_threads: usize,
        unique_keys: bool,
        num_keys: usize,
        key_size: usize,
        val_min_size: usize,
        val_max_size: usize,
        bp_size: usize,
        ops_ratio: String,
    ) -> Self {
        Self {
            num_threads,
            unique_keys,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
            bp_size,
            ops_ratio,
        }
    }
}

impl std::fmt::Display for BenchParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        result.push_str(&format!("{:<20}: {}\n", "num_threads", self.num_threads));
        result.push_str(&format!("{:<20}: {}\n", "unique_keys", self.unique_keys));
        result.push_str(&format!("{:<20}: {}\n", "num_keys", self.num_keys));
        result.push_str(&format!("{:<20}: {}\n", "key_size", self.key_size));
        result.push_str(&format!("{:<20}: {}\n", "val_min_size", self.val_min_size));
        result.push_str(&format!("{:<20}: {}\n", "val_max_size", self.val_max_size));
        result.push_str(&format!("{:<20}: {}\n", "bp_size", self.bp_size));
        result.push_str(&format!("{:<20}: {}\n", "ops_ratio", self.ops_ratio));
        write!(f, "{}", result)
    }
}

pub fn gen_foster_btree_in_mem(
) -> Arc<FosterBtree<DummyEvictionPolicy, InMemPool<DummyEvictionPolicy>>> {
    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let btree = FosterBtree::new(c_key, get_in_mem_pool());
    Arc::new(btree)
}

pub fn gen_foster_btree_on_disk(
    bp_size: usize,
) -> Arc<FosterBtree<LRUEvictionPolicy, BufferPool<LRUEvictionPolicy>>> {
    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let btree = FosterBtree::new(c_key, get_test_bp(bp_size));
    Arc::new(btree)
}

pub fn insert_into_foster_tree<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &[RandomKVs],
) {
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            btree.insert(k, v).unwrap();
        }
    }
}

pub fn insert_into_foster_tree_parallel<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &[RandomKVs],
) {
    // Scopeed threads
    thread::scope(|s| {
        for partition in kvs.iter() {
            let btree = btree.clone();
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    btree.insert(k, v).unwrap();
                }
            });
        }
    })
}

pub fn insert_into_btree_map(mut btree: BTreeMap<Vec<u8>, Vec<u8>>, kvs: &[RandomKVs]) {
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            btree.insert(k.clone(), v.clone());
        }
    }
}

pub fn run_bench<E: EvictionPolicy, M: MemPool<E>>(
    bench_params: BenchParams,
    kvs: Vec<RandomKVs>,
    btree: Arc<FosterBtree<E, M>>,
) {
    let ops_ratio = bench_params.parse_ops_ratio();
    thread::scope(|s| {
        for partition in kvs.iter() {
            let btree = btree.clone();
            let rand = RandomOp::new(ops_ratio.clone());
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    let op = rand.get();
                    match op {
                        TreeOperation::Insert => {
                            let _ = btree.insert(k, v);
                        }
                        TreeOperation::Update => {
                            let _ = btree.update(k, v);
                        }
                        TreeOperation::Delete => {
                            let _ = btree.delete(k);
                        }
                        TreeOperation::Get => {
                            let _ = btree.get(k);
                        }
                    };
                }
            });
        }
    })
}

pub fn run_bench_for_paged_hash_map<E: EvictionPolicy, M: MemPool<E>>(
    bench_params: BenchParams,
    kvs: Vec<RandomKVs>,
    phm: &Arc<PagedHashMap<E, M>>,
) {
    // simple function to merge two values, not Box
    // let func = |old: &[u8], new: &[u8]| new.to_vec();

    let ops_ratio = bench_params.parse_ops_ratio();
    thread::scope(|s| {
        for partition in kvs.iter() {
            let phm = phm.clone();
            let rand = RandomOp::new(ops_ratio.clone());
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    let op = rand.get();
                    match op {
                        TreeOperation::Insert => {
                            // let _ = phm.upsert_with_merge(k, v, func);
                            let _ = phm.insert(k, v);
                        }
                        TreeOperation::Update => {
                            panic!("Update not supported")
                            // let _ = phm.update(k, v);
                        }
                        TreeOperation::Delete => {
                            panic!("Delete not supported")
                            // let _ = phm.delete(k);
                        }
                        TreeOperation::Get => {
                            let _ = phm.get(k);
                        }
                    };
                }
            });
        }
    })
}

pub fn run_bench_for_rust_hash_map(
    bench_params: BenchParams,
    kvs: Vec<RandomKVs>,
    rhm: &Arc<RustHashMap>,
) {
    let ops_ratio = bench_params.parse_ops_ratio();
    thread::scope(|s| {
        for partition in kvs.iter() {
            let rhm = rhm.clone();
            let rand = RandomOp::new(ops_ratio.clone());
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    let op = rand.get();
                    match op {
                        TreeOperation::Insert => {
                            let _ = rhm.insert(k.clone(), v.clone());
                        }
                        TreeOperation::Update => {
                            panic!("Update not supported")
                            // let _ = rhm.update(k, v);
                        }
                        TreeOperation::Delete => {
                            panic!("Delete not supported")
                            // let _ = rhm.delete(k);
                        }
                        TreeOperation::Get => {
                            let _ = rhm.get(k);
                        }
                    };
                }
            });
        }
    })
}

pub fn gen_paged_hash_map_in_mem(
) -> Arc<PagedHashMap<DummyEvictionPolicy, InMemPool<DummyEvictionPolicy>>> {
    let c_key = ContainerKey::new(0, 0);
    let map = PagedHashMap::new(get_in_mem_pool(), c_key, false, true);
    Arc::new(map)
}

pub fn gen_paged_hash_map_on_disk(
    bp_size: usize,
) -> Arc<PagedHashMap<LRUEvictionPolicy, BufferPoolForTest<LRUEvictionPolicy>>> {
    let c_key = ContainerKey::new(0, 0);
    let map = PagedHashMap::new(get_test_bp(bp_size), c_key, false, true);
    Arc::new(map)
}

pub fn gen_paged_hash_map_on_disk_without_ponter_swizzling(
    bp_size: usize,
) -> Arc<PagedHashMap<LRUEvictionPolicy, BufferPoolForTest<LRUEvictionPolicy>>> {
    let c_key = ContainerKey::new(0, 0);
    let map = PagedHashMap::new(get_test_bp(bp_size), c_key, false, false);
    Arc::new(map)
}

pub fn gen_rust_hash_map() -> Arc<RustHashMap> {
    let map = RustHashMap::new();
    Arc::new(map)
}

pub fn insert_into_paged_hash_map<E: EvictionPolicy, M: MemPool<E>>(
    phm: &Arc<PagedHashMap<E, M>>,
    kvs: &[RandomKVs],
) {
    // let func = |old: &[u8], new: &[u8]| new.to_vec();
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            // let _ = phm.upsert_with_merge(k, v, func);
            let _ = phm.insert(k, v);
        }
    }
}

pub fn insert_into_rust_hash_map(rhm: &Arc<RustHashMap>, kvs: &[RandomKVs]) {
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            rhm.insert(k.clone(), v.clone());
        }
    }
}

pub fn get_from_paged_hash_map<E: EvictionPolicy, M: MemPool<E>>(
    phm: &Arc<PagedHashMap<E, M>>,
    kvs: &[RandomKVs],
) {
    thread::scope(|s| {
        for partition in kvs.iter() {
            let phm = phm.clone();
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    let val = phm.get(k);
                    assert_eq!(val.unwrap(), *v);
                }
            });
        }
    });
}

pub fn get_from_paged_hash_map2<E: EvictionPolicy, M: MemPool<E>>(
    phm: &Arc<PagedHashMap<E, M>>,
    kvs: &[RandomKVs],
) {
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            let val = phm.get(k);
            assert_eq!(val.unwrap(), *v);
        }
    }
}

pub fn get_from_rust_hash_map(rhm: &Arc<RustHashMap>, kvs: &[RandomKVs]) {
    thread::scope(|s| {
        for partition in kvs.iter() {
            let rhm = rhm.clone();
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    let val = rhm.get(k);
                    assert_eq!(val.unwrap(), *v);
                }
            });
        }
    });
}

pub fn get_from_rust_hash_map2(rhm: &Arc<RustHashMap>, kvs: &[RandomKVs]) {
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            let val = rhm.get(k);
            assert_eq!(val.unwrap(), *v);
        }
    }
}
