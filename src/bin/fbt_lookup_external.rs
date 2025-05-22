use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

use clap::Parser;
use criterion::black_box;
use fbtree::{
    affinity::{get_current_cpu, get_total_cpus, with_affinity},
    bp::{ContainerKey, MemPool},
    container::ContainerManager,
    event_tracer::trace_lookup,
    prelude::{urand_int, FosterBtree, PAGE_SIZE},
    print_cfg_flags,
};

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
pub struct FBTLookupConfig {
    /// Buffer pool size in GB.
    #[arg(short = 'b', long, default_value_t = 32)]
    pub bp_size: usize,
    /// Number of entries.
    #[arg(short = 'n', long, default_value_t = 100_000_000)]
    pub num_entries: usize,
    /// Number of threads.
    #[arg(short = 't', long, default_value_t = 1)]
    pub num_threads: usize,
    /// Warmup duration in seconds.
    #[arg(short = 'd', long, default_value_t = 0)]
    pub warmup_time: u64,
    /// Execution duration in seconds.
    #[arg(short = 'D', long, default_value_t = 10)]
    pub exec_time: u64,
}

pub fn get_val(num: usize, size: usize) -> Vec<u8> {
    if size < 8 {
        panic!("Size must be at least 8 bytes");
    }
    let mut val = vec![0u8; size];
    val[size - 8..].copy_from_slice(&num.to_be_bytes());
    val
}

pub fn get_bp(num_frames: usize, cm: Arc<ContainerManager>) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        use fbtree::bp::VMCachePool;
        Arc::new(VMCachePool::<false, 64>::new(num_frames, cm).unwrap())
    }
    #[cfg(feature = "bp_clock")]
    {
        use fbtree::bp::BufferPoolClock;
        Arc::new(BufferPoolClock::<64>::new(num_frames, cm).unwrap())
    }
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        use fbtree::bp::BufferPool;
        Arc::new(BufferPool::new(num_frames, cm).unwrap())
    }
}

pub fn run_bench_for_thread(
    is_warmup: bool,
    thread_id: usize,
    barrier: Arc<Barrier>,
    config: &FBTLookupConfig,
    fbt: Arc<FosterBtree<impl MemPool>>,
    flag: &AtomicBool,
) {
    let num_cores_minus_one = get_total_cpus() - 1; // Reserve one core for the main thread
    if thread_id >= num_cores_minus_one {
        panic!(
            "Thread ID {} exceeds number of cores minus one (for main thread): {}",
            thread_id, num_cores_minus_one
        );
    }

    with_affinity(thread_id, || {
        let current_cpu = get_current_cpu();
        println!("Thread {} pinned to CPU {}", thread_id, current_cpu);

        barrier.wait();

        while flag.load(Ordering::Acquire) {
            let key_num = urand_int(0, config.num_entries - 1);
            let key = get_val(key_num, KEY_SIZE);
            let (_, val) = fbt.get_kv(&key).expect(&format!(
                "Failed to get key {} in thread {}",
                key_num, thread_id
            ));
            debug_assert_eq!(val, get_val(key_num, VAL_SIZE));
            black_box(val);
            if !is_warmup {
                trace_lookup();
            }
        }
    })
    .unwrap();
}

pub fn run_bench(is_warmup: bool, config: &FBTLookupConfig, fbt: &Arc<FosterBtree<impl MemPool>>) {
    let flag = AtomicBool::new(true);
    let run_barrier = Arc::new(Barrier::new(config.num_threads + 1));
    std::thread::scope(|s| {
        let mut handlers = Vec::with_capacity(config.num_threads);
        for i in 0..config.num_threads {
            let thread_id = i;
            let run_barrier = run_barrier.clone();
            let config_ref = &config;
            let fbt_ref = fbt.clone();
            let flag_ref = &flag;
            let handler = s.spawn(move || {
                run_bench_for_thread(
                    is_warmup,
                    thread_id,
                    run_barrier,
                    config_ref,
                    fbt_ref,
                    flag_ref,
                );
            });
            handlers.push(handler);
        }

        run_barrier.wait();
        if is_warmup {
            std::thread::sleep(std::time::Duration::from_secs(config.warmup_time));
        } else {
            std::thread::sleep(std::time::Duration::from_secs(config.exec_time));
        }
        flag.store(false, Ordering::Release);

        for handler in handlers {
            handler.join().unwrap();
        }
    })
}

pub fn main() {
    println!("Page size: {}", PAGE_SIZE);
    print_cfg_flags::print_cfg_flags();
    let config = FBTLookupConfig::parse();
    println!("config: {:?}", config);

    // Set frames. We set the max buffer pool size from the command line.
    let num_frames = config.bp_size * 1024 * 1024 * 1024 / PAGE_SIZE;
    println!(
        "BP size: {} GB ({} frames)",
        num_frames * PAGE_SIZE / (1024 * 1024 * 1024),
        num_frames
    );

    let base_dir = format!("fbt_db_n{}", config.num_entries);
    if !std::path::Path::new(&base_dir).exists() {
        panic!(
            "Directory {} does not exist. Please run fbt_lookup_db_gen first.",
            base_dir
        )
    }
    println!("Reading from directory: {}", base_dir);
    let cm = Arc::new(ContainerManager::new(base_dir, true, false).unwrap());

    println!("Allocating buffer pool");
    let start = std::time::Instant::now();
    let bp = get_bp(num_frames, cm.clone());
    println!("Buffer pool allocated in {:?}", start.elapsed());

    let fbt = Arc::new(FosterBtree::load(ContainerKey::new(0, 0), bp.clone(), 0));

    with_affinity(get_total_cpus() - 1, || {
        let current_cpu = get_current_cpu();
        println!("Main thread pinned to CPU {}", current_cpu);

        // Warmup
        if config.warmup_time > 0 {
            println!("Running warmup for {} seconds", config.warmup_time);
            let _ = run_bench(true, &config, &fbt);
            println!("BP stats after warmup: \n{}", unsafe { bp.stats() });
        } else {
            println!("Warm up skipped");
        }

        // Run the benchmark
        if config.exec_time == 0 {
            panic!("Execution time is 0. Please specify a non-zero execution time.");
        } else {
            println!("Running benchmark for {} seconds", config.exec_time);
        }
        run_bench(false, &config, &fbt);
    })
    .unwrap();

    println!("BP stats: \n{}", unsafe { bp.stats() });
    bp.clear_dirty_flags().unwrap();
}
