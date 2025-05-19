use std::sync::Arc;

use clap::Parser;
use criterion::black_box;
use fbtree::{
    bp::{BufferPool, MemPool},
    container::ContainerManager,
    prelude::{tpcc_gen_all_tables, PAGE_SIZE},
    txn_storage::NoWaitTxnStorage,
};

// #[global_allocator]
// static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

// use mimalloc::MiMalloc;
//
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

/// Configuration settings parsed from command-line arguments.
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct TPCCDBGenConfig {
    /// Number of warehouses.
    #[arg(short = 'w', long, default_value_t = 1)]
    pub num_warehouses: u16,
    /// Max buffer pool size in GB.
    #[arg(short = 'b', long, default_value_t = 32)]
    pub buffer_pool_size_max: u16,
}

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    let config = TPCCDBGenConfig::parse();
    println!("DB gen config: {:?}", config);

    // Set frames. We set the max buffer pool size from the command line.
    let num_frames_max = config.buffer_pool_size_max as usize * 1024 * 1024 * 1024 / PAGE_SIZE;
    let num_frames =
        (config.num_warehouses as usize * 1024 * 1024 * 1024 / PAGE_SIZE).min(num_frames_max);
    println!(
        "BP size: {} GB ({} frames)",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024),
        num_frames
    );

    let base_dir = format!("tpcc_db_w{}", config.num_warehouses);
    if std::path::Path::new(&base_dir).exists() {
        panic!(
            "Directory {} already exists. Please remove it before running the benchmark.",
            base_dir
        );
    }
    println!("Writing to directory: {}", base_dir);

    let cm = Arc::new(ContainerManager::new(base_dir, true, false).unwrap());
    let bp = Arc::new(BufferPool::new(num_frames, cm).unwrap());

    let txn_storage = NoWaitTxnStorage::new(&bp);
    let tbl_info = tpcc_gen_all_tables(&txn_storage, config.num_warehouses);
    black_box(&tbl_info);

    println!("BP stats after load: \n{}", unsafe { bp.stats() });
}
