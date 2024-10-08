use std::time::Instant;
use std::sync::Arc;
use std::error::Error;
use fbtree::mvcc_index::hash_join::mvcc_hash_join::MvccIndex;
use fbtree::{
    prelude::*,
    mvcc_index::hash_join::mvcc_hash_join::HashJoinTable,
};

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <data_file> <ops_file>", args[0]);
        return Ok(());
    }
    let data_file = &args[1];
    let ops_file = &args[2];

    // Read data and operations
    let data = read_data_file(data_file)?;
    let ops = read_ops_file(ops_file)?;
    let op_num = ops.len();

    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = HashJoinTable::create(c_key, mem_pool)?;
    // let hash_join_table = HashJoinTable::create(c_key, mem_pool)?;

    // Start the benchmark
    let start_time = Instant::now();

    // Load data into the hash join table
    for (key, pkey, value) in data {
        hash_join_table.insert(key, pkey, 0, 0, value)?;
    }

    // Execute operations from ops.csv
    for op in ops {
        match op.op_type.as_str() {
            "insert" => {
                hash_join_table.insert(op.key, op.pkey, op.ts, op.tx_id, op.value)?;
            }
            "update" => {
                hash_join_table.update(op.key, op.pkey, op.ts, op.tx_id, op.value)?;
            }
            "delete" => {
                hash_join_table.delete(&op.key, &op.pkey, op.ts, op.tx_id)?;
            }
            "get" => {
                // Get the value associated with the key and pkey at the given timestamp
                let result = hash_join_table.get(&op.key, &op.pkey, op.ts)?;
                // match result {
                //     Some(value) => {
                //         // You can process the retrieved value if needed
                //         // For benchmarking, we might just want to count successful gets
                //     }
                //     None => {
                //         // Handle the case where the key-pkey pair is not found
                //     }
                // }
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }

    let duration = start_time.elapsed();
    println!("Executed {} operations in {:?}", op_num, duration);

    // Optionally, perform additional benchmarks like scan or delta_scan
    // For example:
    // let scan_start = Instant::now();
    // let scan_results = hash_join_table.scan(u64::MAX)?;
    // let scan_duration = scan_start.elapsed();
    // println!("Scan completed in {:?}", scan_duration);

    Ok(())
}

// Define a struct to represent an operation
struct Operation {
    tx_id: u64,
    ts: u64,
    op_type: String,
    key: Vec<u8>,
    pkey: Vec<u8>,
    value: Vec<u8>,
}

// Function to read data.csv
use std::fs::File;
use std::io::{self, BufRead};

fn read_data_file(file_path: &str) -> io::Result<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>> {
    let mut data = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }
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

// Function to read ops.csv
fn read_ops_file(file_path: &str) -> io::Result<Vec<Operation>> {
    let mut operations = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }
        // Use a CSV parser to handle commas in values
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(line.as_bytes());
        for result in rdr.records() {
            let record = result?;
            if record.len() >= 6 {
                let tx_id = record[0].parse::<u64>().unwrap_or(0);
                let ts = record[1].parse::<u64>().unwrap_or(0);
                let op_type = record[2].to_string();
                let key = record[3].as_bytes().to_vec();
                let pkey = record[4].as_bytes().to_vec();
                let value = record[5].as_bytes().to_vec();
                operations.push(Operation {
                    tx_id,
                    ts,
                    op_type,
                    key,
                    pkey,
                    value,
                });
            } else {
                eprintln!("Invalid line (expected 6 fields): {}", line);
            }
        }
    }
    Ok(operations)
}
