use libc::{cpu_set_t, sched_setaffinity, sysconf};
use std::io;
use std::mem;

pub fn get_num_cores() -> usize {
    // Get the number of logical cores
    // std::thread::available_parallelism().unwrap().get()
    unsafe {
        let n = sysconf(libc::_SC_NPROCESSORS_CONF);
        if n > 0 {
            return n as usize;
        }
        1 // extremely unlikely fallback
    }
}

pub fn set_affinity(cpu_id: usize) -> io::Result<()> {
    use libc::{CPU_SET, CPU_ZERO};

    unsafe {
        // Create a CPU set and clear it.
        let mut cpuset: cpu_set_t = mem::zeroed();
        CPU_ZERO(&mut cpuset);
        CPU_SET(cpu_id, &mut cpuset);

        // Set affinity of the current thread (pid 0 means current process/thread)
        let ret = sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &cpuset);
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn get_current_cpu() -> i32 {
    use libc::{sched_getcpu, CPU_SETSIZE};

    unsafe { sched_getcpu() % CPU_SETSIZE }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io, thread, time::Duration};

    /// Sanity-check that we detect at least one logical CPU.
    #[test]
    fn get_num_cores_returns_positive() {
        let n = get_num_cores();
        assert!(n >= 1, "reported core count must be ≥ 1 (got {n})");
    }

    #[test]
    fn set_affinity_and_get_number_of_cores() {
        // Set affinity to the first core.
        set_affinity(0).unwrap();

        // Check that we can still get the number of cores.
        let n = get_num_cores(); // This should not be affected by the affinity setting.
        println!("Number of cores: {n}");
        assert!(n >= 1, "affinity set to core 0, but got {n}");
    }

    /// Sets affinity to a *different* core (when available) and verifies that the
    /// running thread migrates there.  
    ///
    /// ⚠️  *This alters the process-wide affinity mask*; run with
    /// `cargo test -- --test-threads=1` if you have other parallel tests that
    /// rely on the default mask.
    #[cfg(target_os = "linux")]
    #[test]
    fn set_affinity_moves_thread() -> io::Result<()> {
        // Skip on single-core hosts.
        if get_num_cores() < 2 {
            eprintln!("single-core system – skipping affinity test");
            return Ok(());
        }

        // Run the experiment in its own OS thread so we don’t disturb the
        // test-harness main thread.
        thread::spawn(|| -> io::Result<()> {
            let start_cpu = get_current_cpu() as usize;
            let total     = get_num_cores();

            // Pick a different valid core id.
            let target = if start_cpu + 1 < total { start_cpu + 1 } else { start_cpu - 1 };

            // Request affinity change.
            set_affinity(target)?;

            // Give the scheduler a moment to migrate us.
            for _ in 0..1_000 {
                if get_current_cpu() as usize == target {
                    return Ok(());
                }
                thread::yield_now();
                thread::sleep(Duration::from_micros(50));
            }
            panic!(
                "after setting affinity to CPU {target}, sched_getcpu() never reported running there (still on {})",
                get_current_cpu()
            );
        })
        .join()
        .unwrap()
    }
}
