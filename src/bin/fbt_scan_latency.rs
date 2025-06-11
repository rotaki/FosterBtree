use clap::Parser;
use core::panic;
use fbtree::{
    access_method::prelude::*,
    bp::{get_test_bp_clock, ContainerKey, MemPool},
    prelude::{FosterBtree, PAGE_SIZE},
    random::gen_random_byte_vec,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{sync::Arc, time::Duration};

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

pub fn load_table(params: &Params, table: &Arc<impl UniqueKeyIndex + Send + Sync + 'static>) {
    (0..params.num_keys).into_par_iter().for_each(|i| {
        let key = get_key_bytes(i, params.key_size);
        let value = gen_random_byte_vec(params.record_size, params.record_size);
        table.insert(&key, &value).unwrap();
    })
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

fn get_index<M: MemPool>(bp: Arc<M>, _params: &Params) -> Arc<FosterBtree<M>> {
    Arc::new(FosterBtree::new(ContainerKey::new(0, 0), bp))
}

fn main() {
    let params = Params::parse();
    println!("{:?}", params);
    println!("Page size: {}", PAGE_SIZE);

    // Otherwise, set frames equal to the specified size
    let num_frames = if params.bp_size == 0 {
        let net_size = params.num_keys * (params.key_size + params.record_size + 10);
        (net_size as f64 * 2.3 / PAGE_SIZE as f64) as usize
    } else {
        params.bp_size * 1024 * 1024 * 1024 / PAGE_SIZE
    };
    println!(
        "BP size: {} GB ({} frames)",
        (num_frames * PAGE_SIZE) as f64 / (1024 * 1024 * 1024) as f64,
        num_frames
    );

    let bp = get_test_bp_clock::<64>(num_frames);
    let table = get_index(bp.clone(), &params);

    println!("Loading table...");
    load_table(&params, &table);

    println!("Buffer pool stats after load: {}", unsafe { bp.stats() });

    println!("--- Page stats ---\n{}", table.page_stats(false));

    println!("--- Warmup ---");
    for i in 0..5 {
        let dur = execute_workload(&params, table.clone());
        println!("Warmup scan {} took {:?}", i, dur);
    }
    println!("Executing workload...");
    let num_scans = 10;
    let mut total_time = Duration::new(0, 0);
    for i in 0..num_scans {
        let dur = execute_workload(&params, table.clone());
        total_time += dur;
        println!("Scan {} took {:?}", i, dur);
    }
    println!("Avg Latency: {:?}", total_time / num_scans);
}
