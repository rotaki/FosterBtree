use clap::Parser;
use core::panic;
use criterion::black_box;
use fbtree::random::gen_random_byte_vec;
use std::time::Duration;

#[derive(Debug, Parser, Clone)]
pub struct Params {
    /// Buffer pool size in GB. If 0, use heuristic to determine the size from the number of keys.
    #[clap(short, long, default_value = "0")]
    pub bp_size: usize,
    /// Number of records. Default 10 M
    #[clap(short, long, default_value = "1000000")]
    pub num_keys: usize,
    /// Key size
    #[clap(short, long, default_value = "10")]
    pub key_size: usize,
    /// Record size
    #[clap(short, long, default_value = "100")]
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

pub fn load_table(params: &Params, table: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    (0..params.num_keys).for_each(|i| {
        let key = get_key_bytes(i, params.key_size);
        let value = gen_random_byte_vec(params.record_size, params.record_size);
        table.push((key, value));
    })
}

pub fn execute_workload(params: &Params, table: &Vec<(Vec<u8>, Vec<u8>)>) -> Duration {
    let start = std::time::Instant::now();
    let mut count = 0;
    for (key, value) in table.iter() {
        let key_cloned = key.clone();
        let value_cloned = value.clone();
        let _key = black_box(key_cloned);
        let _value = black_box(value_cloned);
        // Simulate a scan operation
        count += 1; // In a real scenario, you would do something with the key and value
    }
    let elapsed = start.elapsed();
    assert_eq!(count, params.num_keys);
    elapsed
}

fn main() {
    let params = Params::parse();
    println!("{:?}", params);

    // Otherwise, set frames equal to the specified size
    let mut table = Vec::with_capacity(params.num_keys);
    println!("Loading table...");
    load_table(&params, &mut table);

    println!("Executing workload...");
    let num_scans = 10;
    let mut total_time = Duration::new(0, 0);
    for i in 0..num_scans {
        let dur = execute_workload(&params, &table);
        total_time += dur;
        println!("Scan {} took {:?}", i, dur);
    }
    println!("Avg Latency: {:?}", total_time / num_scans);
}
