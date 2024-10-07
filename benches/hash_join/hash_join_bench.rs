use std::time::{Instant};
use std::sync::Arc;
use fbtree::{
    prelude::*,
    access_method::chain::mvcc_hash_join::HashJoinTable,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <data_file> <transactions_file>", args[0]);
        return Ok(());
    }
    let data_file = &args[1];
    let transactions_file = &args[2];

    // Read data and transactions
    let data = read_data_file(data_file)?;
    let txs = read_transactions_file(transactions_file)?;
    let tx_num = txs.len();

    // Initialize the hash join table
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0); // Adjust as needed
    let hash_join_table = HashJoinTable::new(c_key, mem_pool);

    // Start the benchmark
    let start_time = Instant::now();

    // Load data into the hash join table
    for (key, pkey, value) in data {
        hash_join_table.insert(key, pkey, 0, 0 ,value)?;
    }

    for (op, key, pkey, value, ts, tx_id) in txs {
        match op.as_str() {
            "insert" => {
                hash_join_table.insert(key, pkey, ts, tx_id, value)?;
            }
            "update" => {
                hash_join_table.update(key, pkey, ts, tx_id, value)?;
            }
            "delete" => {
                hash_join_table.delete(&key, &pkey, ts, tx_id)?;
            }
            _ => {
                eprintln!("Unknown operation: {}", op);
            }
        }
    }

    let duration = start_time.elapsed();
    println!("Executed {} operations in {:?}", tx_num, duration);

    // Optionally, perform hash joins or other queries
    // Measure the performance of these operations as well

    Ok(())
}


use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

fn read_data_file(file_path: &str) -> io::Result<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>> {
    let mut data = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 3 {
            let key = parts[0].as_bytes().to_vec();
            let pkey = parts[1].as_bytes().to_vec();
            let value = parts[2].as_bytes().to_vec();
            data.push((key, pkey, value));
        }
    }
    Ok(data)
}

fn read_transactions_file(file_path: &str) -> io::Result<Vec<(String, Vec<u8>, Vec<u8>, Vec<u8>, u64, u64)>> {
    let mut transactions = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 6 {
            let op = parts[0].to_string();
            let key = parts[1].as_bytes().to_vec();
            let pkey = parts[2].as_bytes().to_vec();
            let value = parts[3].as_bytes().to_vec();
            let ts = parts[4].parse::<u64>().unwrap_or(0);
            let tx_id = parts[5].parse::<u64>().unwrap_or(0);
            transactions.push((op, key, pkey, value, ts, tx_id));
        }
    }
    Ok(transactions)
}
