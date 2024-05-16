use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::{
    buffer_pool::{
        get_in_mem_pool, get_test_bp,
        prelude::{
            ContainerKey, DummyEvictionPolicy, EvictionPolicy, InMemPool, LRUEvictionPolicy,
            MemPool,
        },
        BufferPoolForTest,
    },
    foster_btree::{FosterBtree, FosterBtreePage},
    log, log_trace,
    random::{RandomKVs, RandomOp},
};

#[derive(Serialize, Deserialize, Debug, Parser)]
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

    pub fn to_string(&self) -> String {
        serde_json::to_string_pretty(&self).unwrap()
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
) -> Arc<FosterBtree<LRUEvictionPolicy, BufferPoolForTest<LRUEvictionPolicy>>> {
    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let btree = FosterBtree::new(c_key, get_test_bp(bp_size));
    Arc::new(btree)
}

pub fn insert_into_foster_tree<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &Vec<RandomKVs>,
) {
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            btree.insert(k, v).unwrap();
        }
    }
}

pub fn insert_into_foster_tree_parallel<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &Vec<RandomKVs>,
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

pub fn insert_into_btree_map(mut btree: BTreeMap<Vec<u8>, Vec<u8>>, kvs: &Vec<RandomKVs>) {
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
                            let _ = btree.insert(&k, v);
                        }
                        TreeOperation::Update => {
                            let _ = btree.update(&k, v);
                        }
                        TreeOperation::Delete => {
                            let _ = btree.delete(&k);
                        }
                        TreeOperation::Get => {
                            let _ = btree.get(&k);
                        }
                    };
                }
            });
        }
    })
}
