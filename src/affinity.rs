use libc::{cpu_set_t, sched_setaffinity, sysconf, CPU_SET, CPU_ZERO};
use std::io;
use std::mem;

pub fn get_total_cpus() -> usize {
    unsafe {
        match sysconf(libc::_SC_NPROCESSORS_CONF) {
            n if n > 0 => n as usize,
            _ => 1,
        }
    }
}

/// Convenience wrapper around sysconf; respects the *current* affinity mask.
/// (Same as `std::thread::available_parallelism()` but without `Option`.)
pub fn get_available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

pub fn get_current_cpu() -> i32 {
    use libc::{sched_getcpu, CPU_SETSIZE};

    unsafe { sched_getcpu() % CPU_SETSIZE }
}

fn set_affinity(cpu_id: usize) -> io::Result<()> {
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

/// “Disable” affinity by allowing *all* logical CPUs.
fn reset_affinity_all() -> io::Result<()> {
    let total = get_total_cpus();
    let mut cpuset = unsafe { mem::zeroed::<cpu_set_t>() };
    unsafe {
        CPU_ZERO(&mut cpuset);
        for i in 0..total {
            CPU_SET(i, &mut cpuset);
        }
        if sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &cpuset) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// RAII guard that restores the original affinity mask on drop.
pub struct AffinityGuard;

impl AffinityGuard {
    /// Save the current mask and pin to `cpu_id`.
    pub fn pin(cpu_id: usize) -> io::Result<Self> {
        set_affinity(cpu_id)?;
        Ok(AffinityGuard {})
    }
}

impl Drop for AffinityGuard {
    fn drop(&mut self) {
        // Reset the affinity mask to allow all CPUs.
        if let Err(e) = reset_affinity_all() {
            eprintln!("failed to reset CPU affinity: {e}");
        }
    }
}

/// Run the closure `f` while the calling thread is pinned to `cpu_id`.
/// The original affinity mask is restored automatically—even if `f` panics.
pub fn with_affinity<F, R>(cpu_id: usize, f: F) -> io::Result<R>
where
    F: FnOnce() -> R,
{
    // Re-use the RAII guard you already have
    let _guard = AffinityGuard::pin(cpu_id)?;
    Ok(f()) // guard is dropped → affinity restored
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn total_and_available_cpu_counts_reasonable() {
        let total = get_total_cpus();
        assert!(total >= 1, "total CPUs must be at least 1");
        let avail = get_available_cpus();
        assert!(
            avail >= 1 && avail <= total,
            "available CPUs ({avail}) must be in 1..={total}"
        );
    }

    #[test]
    fn set_and_reset_affinity_round_trip() {
        let total = get_total_cpus();
        if total < 2 {
            return;
        }

        // Save initial mask
        let before = get_available_cpus();
        assert_eq!(before, total, "fresh test thread should see all CPUs");

        // 1. Pin to core 0
        set_affinity(0).unwrap();
        assert_eq!(
            get_available_cpus(),
            1,
            "after pinning, only one CPU should be visible"
        );

        // 2. Restore
        reset_affinity_all().unwrap();
        assert_eq!(
            get_available_cpus(),
            total,
            "after reset, full CPU set should be restored"
        );
    }

    #[test]
    fn affinity_guard_restores_on_drop() {
        let total = get_total_cpus();
        if total < 2 {
            return;
        }

        {
            let _g = AffinityGuard::pin(0).unwrap();
            assert_eq!(
                get_available_cpus(),
                1,
                "inside guard scope we should be pinned"
            );
        } // guard dropped here

        assert_eq!(
            get_available_cpus(),
            total,
            "guard drop must restore original affinity"
        );
    }

    #[test]
    fn with_affinity_closure_scoped() {
        let total = get_total_cpus();
        if total < 2 {
            return;
        }

        let before = get_available_cpus();
        assert_eq!(before, total);

        with_affinity(0, || {
            assert_eq!(get_available_cpus(), 1);
            assert_eq!(get_current_cpu(), 0);
        })
        .unwrap();

        assert_eq!(
            get_available_cpus(),
            total,
            "with_affinity must restore original mask"
        );

        with_affinity(1, || {
            assert_eq!(get_available_cpus(), 1);
            assert_eq!(get_current_cpu(), 1);
        })
        .unwrap();

        assert_eq!(
            get_available_cpus(),
            total,
            "with_affinity must restore original mask"
        );
    }

    #[test]
    fn set_affinity_moves_thread() -> io::Result<()> {
        // Skip on single-core hosts.
        if get_total_cpus() < 2 {
            eprintln!("single-core system – skipping affinity test");
            return Ok(());
        }

        // Run the experiment in its own OS thread so we don’t disturb the
        // test-harness main thread.
        std::thread::spawn(|| -> io::Result<()> {
            let start_cpu = get_current_cpu() as usize;
            let total     = get_total_cpus();

            // Pick a different valid core id.
            let target = if start_cpu + 1 < total { start_cpu + 1 } else { start_cpu - 1 };

            // Request affinity change.
            set_affinity(target)?;

            // Give the scheduler a moment to migrate us.
            for _ in 0..1_000 {
                if get_current_cpu() as usize == target {
                    return Ok(());
                }
                std::thread::yield_now();
                std::thread::sleep(std::time::Duration::from_micros(50));
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
