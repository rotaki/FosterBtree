use std::sync::Arc;

use clap::Parser;
use fbtree::{
    bp::{BufferPool, ContainerKey, MemPool},
    container::ContainerManager,
    prelude::{FosterBtree, PAGE_SIZE},
};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

// #[global_allocator]
// static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

// use mimalloc::MiMalloc;
//
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

const KEY_SIZE: usize = 10;
const VAL_SIZE: usize = 100;

/// Configuration settings parsed from command-line arguments.
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct FBTDBGenConfig {
    /// Number of entries.
    #[arg(short = 'n', long, default_value_t = 400_000_000)]
    pub num_entries: usize,
    /// Max buffer pool size in GB.
    #[arg(short = 'b', long, default_value_t = 32)]
    pub buffer_pool_size_max: u16,
}

pub fn get_val(num: usize, size: usize) -> Vec<u8> {
    if size < 8 {
        panic!("Size must be at least 8 bytes");
    }
    let mut val = vec![0u8; size];
    val[size - 8..].copy_from_slice(&num.to_be_bytes());
    val
}

pub fn insert_into_db(fbt: Arc<FosterBtree<BufferPool>>, num_entries: usize) {
    // Parallel insertion using rayon
    (0..num_entries)
        .into_par_iter()
        .chunks(10000)
        .for_each(|chunk| {
            for i in chunk {
                // Insert key-value pairs into the FBT
                fbt.insert_kv(&get_val(i, KEY_SIZE), &get_val(i, VAL_SIZE), false)
                    .unwrap();
            }
        });
}

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    let config = FBTDBGenConfig::parse();
    println!("DB gen config: {:?}", config);

    // Set frames. We set the max buffer pool size from the command line.
    let num_frames_max = config.buffer_pool_size_max as usize * 1024 * 1024 * 1024 / PAGE_SIZE;
    let num_frames = (2 * 140 * config.num_entries / PAGE_SIZE).min(num_frames_max);
    println!(
        "BP size: {} GB ({} frames)",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024),
        num_frames
    );

    let base_dir = format!("fbt_db_n{}", config.num_entries);
    if std::path::Path::new(&base_dir).exists() {
        panic!(
            "Directory {} already exists. Please remove it before running the benchmark.",
            base_dir
        );
    }
    println!("Writing to directory: {}", base_dir);

    let cm = Arc::new(ContainerManager::new(base_dir, true, false).unwrap());
    let start = std::time::Instant::now();
    let bp = Arc::new(BufferPool::new(num_frames, cm).unwrap());
    let elapsed = start.elapsed();
    println!("Time taken to allocate buffer pool: {:?}", elapsed);

    let fbt = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), bp.clone()));

    let start = std::time::Instant::now();
    insert_into_db(fbt, config.num_entries);
    let elapsed = start.elapsed();
    println!(
        "Time taken to insert {} entries: {:?}",
        config.num_entries, elapsed
    );

    println!("BP stats after load: \n{}", unsafe { bp.stats() });
    bp.flush_all().unwrap();
}
