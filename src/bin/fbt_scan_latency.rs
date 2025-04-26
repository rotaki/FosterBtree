use clap::Parser;
use core::panic;
use fbtree::{
    access_method::prelude::*,
    bp::{get_test_bp, BufferPool},
    prelude::PAGE_SIZE,
    random::gen_random_byte_vec,
};
use std::{sync::Arc, time::Duration};

use fbtree::{
    bp::{ContainerKey, MemPool},
    prelude::FosterBtree,
};

#[derive(Debug, Parser, Clone)]
pub struct Params {
    /// Number of hash buckets if applicable
    /// Default 1024
    #[clap(short = 'p', long, default_value = "1024")]
    pub buckets: usize,
    /// Buffer pool size. if 0 panic
    #[clap(short, long, default_value = "100000")]
    pub bp_size: usize,
    /// Number of records. Default 10 M
    #[clap(short, long, default_value = "10000000")]
    pub num_keys: usize,
    /// Key size
    #[clap(short, long, default_value = "8")]
    pub key_size: usize,
    /// Record size
    #[clap(short, long, default_value = "1000")]
    pub record_size: usize,
}

fn get_key_bytes(key: usize, key_size: usize) -> Vec<u8> {
    if key_size < std::mem::size_of::<usize>() {
        panic!("Key size is less than the size of usize");
    }
    let mut key_vec = vec![0u8; key_size];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[key_size - bytes.len()..].copy_from_slice(&bytes);
    key_vec
}

pub struct KeyValueGenerator {
    key_size: usize,
    value_size: usize,
    start_key: usize, // Inclusive
    end_key: usize,   // Exclusive
}

impl KeyValueGenerator {
    pub fn new(partition: usize, num_keys: usize, key_size: usize, value_size: usize) -> Vec<Self> {
        // Divide the keys equally among the partitions and
        // assign the remaining keys to the last partition
        let num_keys_per_partition = num_keys / partition;
        let mut generators = Vec::new();
        let mut count = 0;
        for i in 0..partition {
            let start_key = count;
            let end_key = if i == partition - 1 {
                num_keys
            } else {
                count + num_keys_per_partition
            };
            count = end_key;

            generators.push(Self {
                key_size,
                value_size,
                start_key,
                end_key,
            });
        }
        generators
    }
}

impl Iterator for KeyValueGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_key >= self.end_key {
            return None;
        }
        let key = get_key_bytes(self.start_key, self.key_size);
        let value = gen_random_byte_vec(self.value_size, self.value_size);
        self.start_key += 1;

        Some((key, value))
    }
}

pub fn load_table(params: &Params, table: &Arc<impl UniqueKeyIndex + Send + Sync + 'static>) {
    let num_insertion_threads = 6;

    let mut gen = KeyValueGenerator::new(
        num_insertion_threads,
        params.num_keys,
        params.key_size,
        params.record_size,
    );

    // Multi-thread insert. Use 6 threads to insert the keys.
    std::thread::scope(|s| {
        for _ in 0..6 {
            let table = table.clone();
            let key_gen = gen.pop().unwrap();
            s.spawn(move || {
                for (key, value) in key_gen {
                    table.insert(&key, &value).unwrap();
                }
            });
        }
    });
}

pub fn execute_workload(
    params: &Params,
    table: Arc<impl OrderedUniqueKeyIndex + Send + Sync + 'static>,
) -> Duration {
    let start = std::time::Instant::now();
    let mut count = 0;
    let iter = table.scan();
    for _ in iter {
        count += 1;
    }
    let elapsed = start.elapsed();
    assert_eq!(count, params.num_keys);
    elapsed
}

fn get_index(bp: Arc<BufferPool>, _params: &Params) -> Arc<FosterBtree<BufferPool>> {
    println!("Using FosterBtree");
    Arc::new(FosterBtree::new(ContainerKey::new(0, 0), bp))
}

fn main() {
    let params = Params::parse();
    println!("Page size: {}", PAGE_SIZE);
    println!("{:?}", params);

    let bp = get_test_bp(params.bp_size);
    let table = get_index(bp.clone(), &params);

    println!("Loading table...");
    load_table(&params, &table);

    println!("Buffer pool stats after load: {}", unsafe { bp.stats() });

    println!("--- Page stats ---\n{}", table.page_stats(false));

    bp.flush_all().unwrap();

    println!("Resetting stats...");
    unsafe { bp.reset_stats() };

    println!("Executing warmup...");
    let dur = execute_workload(&params, table.clone());
    println!("Warmup Latency: {:?}", dur);
    unsafe { bp.reset_stats() };

    println!("Executing workload...");
    let num_scans = 3;
    let mut total_time = Duration::new(0, 0);
    for i in 0..num_scans {
        let dur = execute_workload(&params, table.clone());
        total_time += dur;
        println!("Scan {} took {:?}", i, dur);
        println!("Buffer pool stats after exec: {}", unsafe { bp.stats() });
        bp.flush_all().unwrap();
        unsafe { bp.reset_stats() };
    }
    println!("Avg Latency: {:?}", total_time / num_scans);
}
