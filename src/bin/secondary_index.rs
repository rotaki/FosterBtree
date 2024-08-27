// Define two indices.
// One index is a primary index and the other is a secondary index.
// The primary index stores (key_1, value) pairs.
// The secondary index stores (key_2, key_1) pairs.

// We experiment with two access methods.
// The first access method is secondary_index_logical.
// The second access method is secondary_index_lipah.
// The logical access method uses the secondary index to find the key
// of the primary index and then uses the primary index to find the value.
// The lipah access method uses the secondary index to find both
// (logical_id, physical_address) pairs and then first uses
// the physical address to directly fetch the value from the page.
// If the value is not found due to relocation, the logical_id is used
// to find the value.
// We compare the speed of the two access methods without any relocation.

use fbtree::{
    access_method::fbt::FosterBtreeRangeScannerWithPageId,
    bp::{ContainerKey, MemPool},
    prelude::FosterBtree,
};

use clap::Parser;
use core::panic;
use fbtree::{
    access_method::prelude::*,
    access_method::{OrderedUniqueKeyIndex, UniqueKeyIndex},
    bp::{get_test_bp, BufferPool},
    prelude::PAGE_SIZE,
    random::gen_random_byte_vec,
};
use rand::prelude::Distribution;
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Debug, Parser, Clone)]
pub struct YCSBParams {
    /// Buffer pool size. if 0 panic
    #[clap(short, long, default_value = "100000")]
    pub bp_size: usize,
    /// Number of records. Default 10 M
    #[clap(short, long, default_value = "100000")]
    pub num_keys: usize,
    /// Key size
    #[clap(short, long, default_value = "50")]
    pub key_size: usize,
    /// Record size
    #[clap(short, long, default_value = "100")]
    pub record_size: usize,
    /// Skew factor
    #[clap(short, long, default_value = "0.0")]
    pub skew_factor: f64,
    /// Warmup time in seconds
    #[clap(short, long, default_value = "10")]
    pub warmup_time: usize,
    /// Execution time in seconds
    #[clap(short, long, default_value = "10")]
    pub exec_time: usize,
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

fn from_key_bytes(key: &[u8]) -> usize {
    // The last 8 bytes of the key is the key

    usize::from_be_bytes(
        key[key.len() - std::mem::size_of::<usize>()..]
            .try_into()
            .unwrap(),
    )
}

fn get_key(num_keys: usize, skew_factor: f64) -> usize {
    let mut rng = rand::thread_rng();
    if skew_factor <= 0f64 {
        rng.gen_range(0..num_keys)
    } else {
        let zipf = zipf::ZipfDistribution::new(num_keys, skew_factor).unwrap();
        let sample = zipf.sample(&mut rng);
        sample - 1
    }
}

fn get_new_value(value_size: usize) -> Vec<u8> {
    gen_random_byte_vec(value_size, value_size)
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

pub fn load_table(params: &YCSBParams, table: &Arc<FosterBtree<BufferPool>>) {
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

fn bench_secondary_index_logical(
    params: &YCSBParams,
    primary: &Arc<FosterBtree<BufferPool>>,
) -> u128 {
    // Iterate through the table to create the secondary index.
    let iter = primary.scan();
    let secondary = Arc::new(FosterBtree::new(
        ContainerKey::new(0, 1),
        primary.mem_pool.clone(),
    ));
    for (k, v) in iter.map(|(k, _v)| (k.clone(), k)) {
        secondary.insert(&k, &v).unwrap();
    }

    let sec_logical = FbtSecondaryLogical::new(primary.clone(), secondary);
    println!(
        "Secondary logical: \n{}",
        sec_logical.secondary.page_stats(false)
    );

    // Keep calling get on the secondary index to measure the speed.
    let warmup = params.warmup_time;
    let exec = params.exec_time;

    let mut avg = 0;
    // Warm up 3 times
    // Do this 5 times each.
    for i in 0..warmup + exec {
        let start = std::time::Instant::now();
        for _ in 0..params.num_keys {
            let key = get_key(params.num_keys, params.skew_factor);
            let key_bytes = get_key_bytes(key, params.key_size);
            let _ = sec_logical.get(&key_bytes).unwrap();
        }
        let elapsed = start.elapsed();
        let name = if i < warmup { "Warmup" } else { "Execution" };
        println!("Iteration({}) {}: time: {:?}", name, i, elapsed);
        if i >= warmup {
            avg += elapsed.as_millis();
        }
    }
    avg /= exec as u128;
    println!("Average time: {} ms", avg);
    println!("BP stats: \n{:?}", primary.mem_pool.stats());
    avg
}

fn bench_secondary_index_lipah(
    params: &YCSBParams,
    primary: &Arc<FosterBtree<BufferPool>>,
) -> u128 {
    // Iterate through the table to create the secondary index.
    let iter = FosterBtreeRangeScannerWithPageId::new(primary, &[], &[]);
    let secondary = Arc::new(FosterBtree::new(
        ContainerKey::new(0, 1),
        primary.mem_pool.clone(),
    ));
    for (k, v) in iter.map(|(k, _v, phys_addr)| (k.clone(), LipahKey::new(k, phys_addr).to_bytes()))
    {
        secondary.insert(&k, &v).unwrap();
    }

    let sec_lipah = FbtSecondaryLipah::new(primary.clone(), secondary, primary.mem_pool.clone());
    println!(
        "Secondary lipah: \n{}",
        sec_lipah.secondary.page_stats(false)
    );

    // Keep calling get on the secondary index to measure the speed.
    let warmup = params.warmup_time;
    let exec = params.exec_time;

    let mut avg = 0;
    // Warm up 3 times
    // Do this 5 times each.
    for i in 0..warmup + exec {
        let start = std::time::Instant::now();
        for _ in 0..params.num_keys {
            let key = get_key(params.num_keys, params.skew_factor);
            let key_bytes = get_key_bytes(key, params.key_size);
            let _ = sec_lipah.get(&key_bytes).unwrap();
        }
        let elapsed = start.elapsed();
        let name = if i < warmup { "Warmup" } else { "Execution" };
        println!("Iteration({}) {}: time: {:?}", name, i, elapsed);
        if i >= warmup {
            avg += elapsed.as_millis();
        }
    }
    avg /= exec as u128;
    println!("Average time: {} ms", avg);
    println!("BP stats: \n{:?}", primary.mem_pool.stats());
    avg
}

fn main() {
    let params = YCSBParams::parse();
    let bp = get_test_bp(params.bp_size);
    let table = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
    load_table(&params, &table);
    // Print the page stats
    println!("BP stats: \n{:?}", bp.stats());
    println!("Tree stats: \n{}", table.page_stats(false));

    println!("Measure the time for the logical access method");
    let logical_time = bench_secondary_index_logical(&params, &table);
    println!("++++++++++++++++++++++++++++++++++++++++++++");
    println!("Measure the time for the lipah access method");
    let lipah_time = bench_secondary_index_lipah(&params, &table);
    println!("++++++++++++++++++++++++++++++++++++++++++++");

    println!("Summary");
    println!("Logical access method: {} ms", logical_time);
    println!("Lipah access method: {} ms", lipah_time);
}
