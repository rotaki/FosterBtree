use clap::Parser;
use fbtree::{
    access_method::{NonUniqueKeyIndex, OrderedUniqueKeyIndex, UniqueKeyIndex},
    bp::{get_in_mem_pool, get_test_bp, BufferPool, InMemPool},
    prelude::{HashFosterBtree, AVAILABLE_PAGE_SIZE, PAGE_SIZE},
    random::gen_random_byte_vec,
};
use rand::prelude::Distribution;
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use fbtree::{
    bp::{ContainerKey, MemPool},
    prelude::FosterBtree,
    random::gen_random_int,
};

#[derive(Debug, Parser, Clone)]
pub struct YCSBParams {
    /// Workload type
    #[clap(short, long, default_value = "A")]
    pub workload_type: char,
    // Number of threads
    #[clap(short, long, default_value = "1")]
    pub num_threads: usize,
    /// Buffer pool size. if 0 panic
    #[clap(short, long, default_value = "100000")]
    pub bp_size: usize,
    /// Number of records. Default 10 M
    #[clap(short, long, default_value = "10000000")]
    pub num_keys: usize,
    /// Key size
    #[clap(short, long, default_value = "10")]
    pub key_size: usize,
    /// Record size
    #[clap(short, long, default_value = "1000")]
    pub record_size: usize,
    /// Skew factor
    #[clap(short, long, default_value = "0.0")]
    pub skew_factor: f64,
    /// Warmup time in seconds
    #[clap(short, long, default_value = "3")]
    pub warmup_time: usize,
    /// Execution time in seconds
    #[clap(short, long, default_value = "10")]
    pub exec_time: usize,
}

// Returns the proportion of each workload type
// (read, update, scan, insert, read-modify-write)
fn workload_proportion(workload_type: char) -> (usize, usize, usize, usize, usize) {
    match workload_type {
        'A' => (50, 50, 0, 0, 0), // Update heavy
        'B' => (95, 5, 0, 0, 0),  // Read mostly
        'C' => (100, 0, 0, 0, 0), // Read only
        'D' => {
            panic!("Workload type D not supported yet");
            // (5, 0, 0, 95, 0),  // Read latest workload
        }
        'E' => {
            panic!("Workload type E not supported yet");
            // (0, 0, 95, 5, 0) // Scan workload
        }
        'F' => {
            panic!("Workload type F not supported yet");
            // (50, 0, 0, 0, 50) // Read-modify-write workload
        }
        _ => panic!("Invalid workload type. Choose from A, B, C, D, E, F"),
    }
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

pub fn load_table(params: &YCSBParams, table: &Arc<impl UniqueKeyIndex + Send + Sync + 'static>) {
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
                    let _ = table.insert(&key, &value).unwrap();
                }
            });
        }
    });
}

pub fn execute_workload(
    params: &YCSBParams,
    table: Arc<impl UniqueKeyIndex + Send + Sync + 'static>,
) -> (usize, usize, usize, usize, usize) {
    let warmup_flag = Arc::new(AtomicBool::new(true)); // Flag to stop the warmup
    let exec_flag = Arc::new(AtomicBool::new(true)); // Flag to stop the workload

    let (read, update, scan, insert, rmw) = workload_proportion(params.workload_type);

    // Execute the workload
    let mut threads = Vec::new();

    for _ in 0..params.num_threads {
        threads.push(std::thread::spawn({
            let table = table.clone();
            let warmup_flag = warmup_flag.clone();
            let exec_flag = exec_flag.clone();
            let params = params.clone();
            move || {
                let mut read_count = 0;
                let mut update_count = 0;
                let mut scan_count = 0;
                let mut insert_count = 0;
                let mut rmw_count = 0;

                while exec_flag.load(Ordering::Relaxed) {
                    let x = gen_random_int(1, 100);
                    if x <= read {
                        let key = get_key(params.num_keys, params.skew_factor);
                        // Read
                        let _ = table.get(&get_key_bytes(key, params.key_size)).unwrap();
                        if warmup_flag.load(Ordering::Relaxed) {
                            read_count += 1;
                        }
                    } else if x <= read + update {
                        // Update
                        let key = get_key(params.num_keys, params.skew_factor);
                        let _ = table
                            .update(
                                &get_key_bytes(key, params.key_size),
                                &get_new_value(params.record_size),
                            )
                            .unwrap();
                        if warmup_flag.load(Ordering::Relaxed) {
                            update_count += 1;
                        }
                    } else if x <= read + update + scan {
                        // Scan
                        unreachable!();
                        // if warmup_flag.load(Ordering::Relaxed) {
                        //     scan_count += 1;
                        // }
                    } else if x <= read + update + scan + insert {
                        // Insert
                        unreachable!();
                        // if warmup_flag.load(Ordering::Relaxed) {
                        //     insert_count += 1;
                        // }
                    } else if x <= read + update + scan + insert + rmw {
                        // Read-modify-write
                        unreachable!();
                        // if warmup_flag.load(Ordering::Relaxed) {
                        //     rmw_count += 1;
                        // }
                    } else {
                        panic!("Invalid operation");
                    }
                }
                (
                    read_count,
                    update_count,
                    scan_count,
                    insert_count,
                    rmw_count,
                )
            }
        }));
    }

    // Wait for the warmup time
    std::thread::sleep(std::time::Duration::from_secs(params.warmup_time as u64));
    warmup_flag.store(false, Ordering::Relaxed);

    // Wait for the execution time
    std::thread::sleep(std::time::Duration::from_secs(params.exec_time as u64));
    exec_flag.store(false, Ordering::Relaxed);

    // Collect the stats
    let mut read_count = 0;
    let mut update_count = 0;
    let mut scan_count = 0;
    let mut insert_count = 0;
    let mut rmw_count = 0;
    for t in threads {
        let (r, u, s, i, m) = t.join().unwrap();
        read_count += r;
        update_count += u;
        scan_count += s;
        insert_count += i;
        rmw_count += m;
    }

    (
        read_count,
        update_count,
        scan_count,
        insert_count,
        rmw_count,
    )
}

#[cfg(not(any(feature = "ycsb_fbt", feature = "ycsb_hash_fbt")))]
fn get_index(bp: Arc<BufferPool>) -> Arc<FosterBtree<BufferPool>> {
    Arc::new(FosterBtree::new(ContainerKey::new(0, 0), bp))
}

#[cfg(feature = "ycsb_fbt")]
fn get_index(bp: Arc<BufferPool>) -> Arc<FosterBtree<BufferPool>> {
    println!("Using FosterBtree");
    Arc::new(FosterBtree::new(ContainerKey::new(0, 0), bp))
}

#[cfg(feature = "ycsb_hash_fbt")]
fn get_index(bp: Arc<BufferPool>) -> Arc<HashFosterBtree<BufferPool>> {
    println!("Using HashFosterBtree");
    Arc::new(HashFosterBtree::new(ContainerKey::new(0, 0), bp, 1024))
}

fn print_stats(r: usize, u: usize, s: usize, i: usize, m: usize, total: usize, exec_time: usize) {
    println!(
        "{:16}: {:10}, ratio: {:.2}%",
        "Read count",
        r,
        r as f64 / total as f64 * 100f64
    );
    println!(
        "{:16}: {:10}, ratio: {:.2}%",
        "Update count",
        u,
        u as f64 / total as f64 * 100f64
    );
    println!(
        "{:16}: {:10}, ratio: {:.2}%",
        "Scan count",
        s,
        s as f64 / total as f64 * 100f64
    );
    println!(
        "{:16}: {:10}, ratio: {:.2}%",
        "Insert count",
        i,
        i as f64 / total as f64 * 100f64
    );
    println!(
        "{:16}: {:10}, ratio: {:.2}%",
        "RMW count",
        m,
        m as f64 / total as f64 * 100f64
    );
    println!("{:16}: {:10}", "Total count", total);
    println!(
        "{:16}: {:10.2} ops/sec",
        "Throughput",
        total as f64 / exec_time as f64
    );
}

fn main() {
    let params = YCSBParams::parse();
    println!("Page size: {}", PAGE_SIZE);
    println!("{:?}", params);

    let bp = get_test_bp(params.bp_size);
    let table = get_index(bp.clone());

    println!("Loading table...");
    load_table(&params, &table);

    println!("Buffer pool stats after load: {:?}", bp.stats());
    println!("Resetting stats...");
    bp.reset_stats();

    println!("Executing workload...");
    let (r, u, s, i, m) = execute_workload(&params, table);

    println!("Buffer pool stats after exec: {:?}", bp.stats());

    print_stats(r, u, s, i, m, r + u + s + i + m, params.exec_time);
}
