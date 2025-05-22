use std::sync::atomic::{AtomicBool, Ordering};

use fbtree::affinity::{get_current_cpu, get_total_cpus, with_affinity};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn main() {
    with_affinity(get_total_cpus() - 1, || {
        let flag = AtomicBool::new(true); // while flag is true, keep running the benchmark

        let result = (0..10)
            .into_par_iter()
            .map(|_| {
                // Keep looping
                while flag.load(Ordering::Acquire) {
                    // Get the current CPU core
                    let thread = std::thread::current();
                    let cpu_core = get_current_cpu();
                    println!(
                        "Thread {:?} is running on CPU core {}",
                        thread.id(),
                        cpu_core
                    );
                }

                get_current_cpu()
            })
            .collect::<Vec<_>>();

        println!("Result: {:?}", result);
    })
    .unwrap();
}
