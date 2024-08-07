use clap::Parser;
use fbtree::{
    bp::{get_in_mem_pool, get_test_bp, BufferPool, InMemPool}, prelude::{AVAILABLE_PAGE_SIZE, PAGE_SIZE}, random::gen_random_byte_vec
};
use rand::prelude::Distribution;
use rand::Rng;

#[derive(Debug, Parser, Clone)]
pub struct YCSBParams {
    /// Workload type
    #[clap(short, long, default_value = "A")]
    pub workload_type: char,

    // Number of threads
    #[clap(short, long, default_value = "1")]
    pub num_threads: usize,

    /// Buffer pool size. if 0, in-memory pool is used
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
    num_keys: usize,
    key_size: usize,
    value_size: usize,
    current_key: usize,
}

impl KeyValueGenerator {
    pub fn new(partition: usize, num_keys: usize, key_size: usize, value_size: usize) -> Vec<Self> {
        // Each partition will have its own generator
        // Divide the keys equally among the partitions and 
        // the 
    }
}

impl Iterator for KeyValueGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_key == self.num_keys {
            return None;
        }

        let key = get_key_bytes(self.current_key, self.key_size);
        let value = gen_random_byte_vec(self.value_size, self.value_size);
        self.current_key += 1;

        Some((key, value))
    }
}

mod foster_btree_ycsb {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use fbtree::{
        bp::{ContainerKey, MemPool},
        prelude::FosterBtree,
        random::gen_random_int,
    };

    use crate::{get_key, get_key_bytes, get_new_value, workload_proportion, KeyValueGenerator};

    use super::YCSBParams;

    pub fn load_table(
        params: &YCSBParams,
        mem_pool: &Arc<impl MemPool>,
    ) -> Arc<FosterBtree<impl MemPool>> {
        let fbt = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), mem_pool.clone()));
        let mut gen = KeyValueGenerator::new(params.num_keys, params.key_size, params.record_size);
        for _ in 0..params.num_keys {
            let (key, value) = gen.next().unwrap();
            fbt.insert(&key, &value).unwrap();
        }
        fbt
    }

    pub fn execute_workload(params: &YCSBParams, table: &Arc<FosterBtree<impl MemPool + 'static>>) 
    -> (usize, usize, usize, usize, usize)
    {
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
                            let _ = table
                                .get(&get_key_bytes(
                                    key,
                                    params.key_size,
                                ))
                                .unwrap();
                            if warmup_flag.load(Ordering::Relaxed) {
                                read_count += 1;
                            }
                        } else if x <= read + update {
                            // Update
                            let key = get_key(params.num_keys, params.skew_factor);
                            let _ = table
                                .update(
                                    &get_key_bytes(
                                        key,
                                        params.key_size,
                                    ),
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
                    (read_count, update_count, scan_count, insert_count, rmw_count)
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

        (read_count, update_count, scan_count, insert_count, rmw_count)
    }
}

fn main() {
    let params = YCSBParams::parse();
    println!("Page size: {}", PAGE_SIZE);
    println!("{:?}", params);

    let (read_count, update_count, scan_count, insert_count, rmw_count) = if params.bp_size == 0 {
        let mem_pool = get_in_mem_pool();
        println!("Loading table in memory...");
        let table = foster_btree_ycsb::load_table(&params, &mem_pool);
        println!("Executing workload...");
        foster_btree_ycsb::execute_workload(&params, &table)
    } else {
        let bp = get_test_bp(params.bp_size);
        println!("Loading table in bp...");
        let table = foster_btree_ycsb::load_table(&params, &bp);
        println!("Buffer pool stats after load: {:?}", bp.stats());
        bp.reset_stats();
        println!("Executing workload...");
        let result = foster_btree_ycsb::execute_workload(&params, &table);
        println!("Buffer pool stats after exec: {:?}", bp.stats());
        result
    };

    // Print each count and the ratio. Format the ratio to 2 decimal places.
    // Print the total count / total time to get the throughput
    let total = read_count + update_count + scan_count + insert_count + rmw_count;

    println!("{:16}: {:10}, ratio: {:.2}%", "Read count", read_count, read_count as f64 / total as f64 * 100f64);
    println!("{:16}: {:10}, ratio: {:.2}%", "Update count", update_count, update_count as f64 / total as f64 * 100f64);
    println!("{:16}: {:10}, ratio: {:.2}%", "Scan count", scan_count, scan_count as f64 / total as f64 * 100f64);
    println!("{:16}: {:10}, ratio: {:.2}%", "Insert count", insert_count, insert_count as f64 / total as f64 * 100f64);
    println!("{:16}: {:10}, ratio: {:.2}%", "RMW count", rmw_count, rmw_count as f64 / total as f64 * 100f64);
    println!("{:16}: {:10}", "Total count", total);
    println!("{:16}: {:10.2} ops/sec", "Throughput", total as f64 / params.exec_time as f64);

}
