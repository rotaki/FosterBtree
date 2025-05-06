//! resident_page_set.rs
//!
//! A lock-free, open-addressing hash table that tracks the
//! pages currently resident in the cache and supports a
//! second-chance “clock” replacement scan.

use std::{
    iter, mem, ptr,
    sync::atomic::{AtomicU64, Ordering},
};

use libc::{
    madvise, mmap, munmap, MADV_HUGEPAGE, MAP_ANONYMOUS, MAP_FAILED, MAP_PRIVATE, PROT_READ,
    PROT_WRITE,
};

const EMPTY: u64 = u64::MAX; // 0xFFFF‥‥FFFF
const TOMBSTONE: u64 = u64::MAX - 1; // 0xFFFF‥‥FFFE

/// One entry in the probe table — just an atomic page-id.
#[repr(C)]
struct Entry {
    pid: AtomicU64,
}

/// Return the next power-of-two ≥ x (x > 0).
#[inline(always)]
fn next_pow2(x: u64) -> u64 {
    x.next_power_of_two()
}

/// A very small Murmur ‐ style 64-bit hash identical to the C++ version.
#[inline(always)]
fn hash(mut k: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h = 0x8445d61a4e774912u64 ^ (8u64).wrapping_mul(M);

    k = k.wrapping_mul(M);
    k ^= k >> R;
    k = k.wrapping_mul(M);

    h ^= k;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Allocate `len` bytes backed by huge pages.  Panics on failure.
unsafe fn alloc_huge(len: usize) -> *mut u8 {
    let size = len;
    let flags = MAP_PRIVATE | MAP_ANONYMOUS;
    let prot = PROT_READ | PROT_WRITE;
    let raw = mmap(ptr::null_mut(), size, prot, flags, -1, 0);
    if std::ptr::eq(raw, MAP_FAILED) {
        panic!("mmap failed: {}", std::io::Error::last_os_error());
    }
    madvise(raw, size, MADV_HUGEPAGE);
    raw.cast()
}

/// A concurrent set of resident page-ids with second-chance scanning.
pub struct ResidentPageSet {
    ht: *mut Entry,       // raw, huge-page-backed table
    cnt: u64,             // table size (power of two)
    mask: u64,            // cnt-1  (for one-instruction modulo)
    clock_pos: AtomicU64, // global scan position
}

// SAFETY: all interior mutability goes through atomics.
unsafe impl Send for ResidentPageSet {}
unsafe impl Sync for ResidentPageSet {}

impl ResidentPageSet {
    /// Create a table that can hold at least `max_count` elements.
    pub fn new(max_count: u64) -> Self {
        let raw_cnt = (max_count * 3).div_ceil(2); // ≈ max_count × 1.5
        let cnt = next_pow2(raw_cnt);
        let bytes = cnt as usize * mem::size_of::<Entry>();

        // Allocate & fill with 0xFF so every slot starts at EMPTY.
        let ht = unsafe { alloc_huge(bytes) }.cast::<Entry>();
        unsafe { ptr::write_bytes(ht.cast::<u8>(), 0xFF, bytes) };

        Self {
            ht,
            cnt,
            mask: cnt - 1,
            clock_pos: AtomicU64::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.cnt as usize
    }

    /// Pointer helper: get a reference to entry `idx`.
    #[inline(always)]
    unsafe fn entry(&self, idx: u64) -> &AtomicU64 {
        &(*self.ht.add(idx as usize)).pid
    }

    pub fn contains(&self, pid: u64) -> bool {
        let start = hash(pid) & self.mask;
        let mut pos = start;
        loop {
            let curr = unsafe { self.entry(pos).load(Ordering::Acquire) };
            if curr == EMPTY {
                return false; // gave up: not present
            }
            if curr == pid {
                return true; // found it
            }
            pos = (pos + 1) & self.mask;
            if pos == start {
                return false; // gave up: not present
            }
        }
    }

    /// Insert `pid`; assumes it is not already present.
    pub fn insert(&self, pid: u64) {
        let start = hash(pid) & self.mask;
        let mut pos = start;
        loop {
            let curr = unsafe { self.entry(pos).load(Ordering::Acquire) };
            assert!(curr != pid, "duplicate insert detected: {}", pid);

            if (curr == EMPTY || curr == TOMBSTONE)
                && unsafe {
                    self.entry(pos)
                        .compare_exchange(curr, pid, Ordering::AcqRel, Ordering::Acquire)
                }
                .is_ok()
            {
                return;
            }
            pos = (pos + 1) & self.mask;
            if pos == start {
                panic!("table is full, cannot insert {}", pid);
            }
        }
    }

    /// Remove `pid`; returns `true` if it was found.
    pub fn remove(&self, pid: u64) -> bool {
        let start = hash(pid) & self.mask;
        let mut pos = start;
        loop {
            let curr = unsafe { self.entry(pos).load(Ordering::Acquire) };
            if curr == EMPTY {
                return false; // gave up: not present
            }
            if curr == pid
                && unsafe {
                    self.entry(pos).compare_exchange(
                        curr,
                        TOMBSTONE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                }
                .is_ok()
            {
                return true;
            }
            pos = (pos + 1) & self.mask;
            if pos == start {
                return false; // gave up: not present
            }
        }
    }

    /// Iterate the next `batch` entries of the global clock, calling `f(pid)`
    /// for every live PID encountered.
    pub fn clock_batch_iter(&self, batch: usize) -> impl Iterator<Item = u64> + '_ {
        // Atomically grab a window of the clock.
        let start = self
            .clock_pos
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pos| {
                Some((pos + batch as u64) & self.mask)
            })
            .expect("Should not fail because the func always returns Some");

        let mut idx = start;
        let mut remaining = batch;

        iter::from_fn(move || {
            while remaining > 0 {
                remaining -= 1;

                let pid = unsafe { self.entry(idx).load(Ordering::Acquire) };
                idx = (idx + 1) & self.mask;
                if pid != EMPTY && pid != TOMBSTONE {
                    return Some(pid);
                }
                // otherwise: skip sentinel, continue looping
            }
            None
        })
    }

    pub fn clear(&self) {
        let bytes = self.cnt as usize * mem::size_of::<Entry>();
        unsafe { ptr::write_bytes(self.ht.cast::<u8>(), 0xFF, bytes) };
        self.clock_pos.store(0, Ordering::Release);
    }
}

impl Drop for ResidentPageSet {
    fn drop(&mut self) {
        let bytes = self.cnt as usize * mem::size_of::<Entry>();
        unsafe {
            munmap(self.ht.cast(), bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        hint::black_box,
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    // -------- helpers -----------------------------------------------------

    /// Collect *all* live PIDs by sweeping the entire table once.
    /// (Slow but handy for asserts in tests.)
    fn collect_all(set: &ResidentPageSet) -> HashSet<u64> {
        set.clock_batch_iter(set.cnt as usize).collect()
    }

    // -------- basic single-threaded tests ---------------------------------

    #[test]
    fn basic_insert_remove() {
        let set = ResidentPageSet::new(32);

        // insert ten distinct ids
        for pid in 0..10 {
            set.insert(pid);
        }
        assert_eq!(collect_all(&set).len(), 10);

        // second remove ⇒ false
        for pid in 0..10 {
            assert!(set.remove(pid));
            assert!(!set.remove(pid));
        }
        assert!(collect_all(&set).is_empty());
    }

    #[test]
    #[should_panic] // the internal assert! on duplicate insert must fire
    fn duplicate_insert_panics() {
        let set = ResidentPageSet::new(4);
        set.insert(42);
        set.insert(42); // <-- boom
    }

    #[test]
    fn clock_iteration_yields_everything_once() {
        let set = ResidentPageSet::new(64);
        for pid in 100..132 {
            set.insert(pid);
        }
        // Grab a window larger than the number of live items
        let seen: Vec<u64> = set.clock_batch_iter(128).collect();
        assert_eq!(seen.len(), 32);
    }

    // -------- concurrent tests -------------------------------------------

    #[test]
    fn concurrent_inserts_and_then_removes() {
        const THREADS: usize = 8;
        const PER_THREAD: u64 = 1000;

        let set = Arc::new(ResidentPageSet::new((THREADS as u64) * PER_THREAD * 2));
        // phase-barrier so all writers start together
        let barrier = Arc::new(Barrier::new(THREADS));

        // -- writer threads
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let set = Arc::clone(&set);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let base = t as u64 * PER_THREAD;
                for pid in base..base + PER_THREAD {
                    set.insert(pid);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // verify every id is present
        assert_eq!(collect_all(&set).len(), THREADS * PER_THREAD as usize);

        // -- remover threads (same partitioning)
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let set = Arc::clone(&set);
            handles.push(thread::spawn(move || {
                let base = t as u64 * PER_THREAD;
                for pid in base..base + PER_THREAD {
                    assert!(set.remove(pid));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(collect_all(&set).is_empty());
    }

    #[test]
    fn concurrent_scans() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const IDS_PER_WRITER: u64 = 10_000;

        let set = Arc::new(ResidentPageSet::new(WRITERS as u64 * IDS_PER_WRITER * 2));

        // prepare data
        let mut handles = Vec::new();
        for t in 0..WRITERS {
            let set = Arc::clone(&set);
            handles.push(thread::spawn(move || {
                let base = t as u64 * IDS_PER_WRITER;
                for pid in base..base + IDS_PER_WRITER {
                    set.insert(pid);
                }
            }));
        }

        let stop = Arc::new(AtomicU64::new(0));

        // spawn readers that repeatedly iterate small batches
        for _ in 0..READERS {
            let set = Arc::clone(&set);
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                while stop.load(Ordering::Relaxed) == 0 {
                    for pid in set.clock_batch_iter(256) {
                        // do nothing with the pid
                        black_box(pid);
                    }
                }
            }));
        }

        // let them run for a short time
        thread::sleep(Duration::from_millis(250));
        stop.store(1, Ordering::Relaxed);

        // join
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_scans_inserts_removes() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const IDS_PER_WRITER: u64 = 10_000;

        let set = Arc::new(ResidentPageSet::new(WRITERS as u64 * IDS_PER_WRITER * 2));

        let stop = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        // spawn readers that repeatedly iterate small batches
        for _ in 0..READERS {
            let set = Arc::clone(&set);
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                while stop.load(Ordering::Relaxed) == 0 {
                    for pid in set.clock_batch_iter(256) {
                        // do nothing with the pid
                        black_box(pid);
                    }
                }
            }));
        }

        // insert data
        for t in 0..WRITERS {
            let set = Arc::clone(&set);
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                let base = t as u64 * IDS_PER_WRITER;
                while stop.load(Ordering::Relaxed) == 0 {
                    // Insert a batch of ids and then remove them
                    for pid in base..base + IDS_PER_WRITER {
                        set.insert(pid);
                    }
                    for pid in base..base + IDS_PER_WRITER {
                        assert!(set.remove(pid));
                    }
                }
            }));
        }

        // let them run for a short time
        thread::sleep(Duration::from_millis(1000));
        stop.store(1, Ordering::Relaxed);

        // join
        for h in handles {
            h.join().unwrap();
        }
    }
}
