mod buffer_pool;
mod buffer_pool_clock;
mod frame_guards;
mod in_mem_pool;
mod mem_pool_trait;
mod resident_set;
mod vmcache;

use std::sync::Arc;

pub use buffer_pool::BufferPool;
pub use buffer_pool_clock::BufferPoolClock;
pub use frame_guards::{FrameReadGuard, FrameWriteGuard};
pub use in_mem_pool::InMemPool;
pub use mem_pool_trait::{
    frame_hint_from_raw, frame_hint_to_raw, ContainerId, DatabaseId, FrameId, LocalContainerId,
    MemPool, MemPoolStatus, PageAddr, PageRef, INVALID_FRAME_ID,
};
pub use vmcache::VMCachePool;

use crate::{container::ContainerManager, random::gen_random_pathname};

use std::sync::atomic::{AtomicUsize, Ordering};

/// Statistics kept by the buffer pool.
/// These statistics are used for decision making.
pub struct BPStats {
    new_page_request: AtomicUsize,
    read_request: AtomicUsize,
    read_request_waiting_for_write: AtomicUsize,
    write_request: AtomicUsize,
}

impl std::fmt::Display for BPStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "New Page: {}\nRead Count: {}\nWrite Count: {}",
            self.new_page_request.load(Ordering::Relaxed),
            self.read_request.load(Ordering::Relaxed),
            self.write_request.load(Ordering::Relaxed)
        )
    }
}

impl BPStats {
    pub fn new() -> Self {
        BPStats {
            new_page_request: AtomicUsize::new(0),
            read_request: AtomicUsize::new(0),
            read_request_waiting_for_write: AtomicUsize::new(0),
            write_request: AtomicUsize::new(0),
        }
    }

    pub fn clear(&self) {
        self.new_page_request.store(0, Ordering::Relaxed);
        self.read_request.store(0, Ordering::Relaxed);
        self.read_request_waiting_for_write
            .store(0, Ordering::Relaxed);
        self.write_request.store(0, Ordering::Relaxed);
    }

    pub fn new_page(&self) -> usize {
        self.new_page_request.load(Ordering::Relaxed)
    }

    pub fn inc_new_page(&self) {
        #[cfg(feature = "stat")]
        self.new_page_request.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_new_pages(&self, _num_pages: usize) {
        #[cfg(feature = "stat")]
        self.new_page_request
            .fetch_add(_num_pages, Ordering::Relaxed);
    }

    pub fn read_count(&self) -> usize {
        self.read_request.load(Ordering::Relaxed)
    }

    pub fn inc_read_count(&self) {
        #[cfg(feature = "stat")]
        self.read_request.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_request_waiting_for_write_count(&self) -> usize {
        self.read_request_waiting_for_write.load(Ordering::Relaxed)
    }

    pub fn inc_read_request_waiting_for_write_count(&self) {
        #[cfg(feature = "stat")]
        self.read_request_waiting_for_write
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_count(&self) -> usize {
        self.write_request.load(Ordering::Relaxed)
    }

    pub fn inc_write_count(&self) {
        #[cfg(feature = "stat")]
        self.write_request.fetch_add(1, Ordering::Relaxed);
    }
}

/// Buffer pool with persistent storage.
pub fn get_bp(num_frames: usize, cm: Arc<ContainerManager>) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        Arc::new(VMCachePool::<false>::new(num_frames, cm).unwrap())
    }
    #[cfg(feature = "bp_clock")]
    {
        Arc::new(BufferPoolClock::new(num_frames, cm).unwrap())
    }
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        Arc::new(BufferPool::new(num_frames, cm).unwrap())
    }
}

/// Buffer pool without persistent storage, used for testing.
pub fn get_test_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        get_test_vmcache::<false>(num_frames)
    }
    #[cfg(feature = "bp_clock")]
    {
        get_test_bp_clock(num_frames)
    }
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        get_test_bp_lru(num_frames)
    }
}

pub fn get_test_vmcache<const IS_SMALL: bool>(num_frames: usize) -> Arc<VMCachePool<IS_SMALL>> {
    let base_dir = gen_random_pathname(Some("test_bp_vmcache_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(VMCachePool::<IS_SMALL>::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_lru(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_lru_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_lru_with_kpc(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_lru_kpc"));
    let cm = Arc::new(ContainerManager::new(base_dir, false, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_clock(num_frames: usize) -> Arc<BufferPoolClock> {
    let base_dir = gen_random_pathname(Some("test_bp_clock_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPoolClock::new(num_frames, cm).unwrap())
}

pub fn get_in_mem_pool() -> Arc<InMemPool> {
    Arc::new(InMemPool::new())
}
pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp_lru, BufferPool, ContainerId, DatabaseId, FrameId,
        FrameReadGuard, FrameWriteGuard, InMemPool, LocalContainerId, MemPool, MemPoolStatus,
        PageAddr, PageRef, INVALID_FRAME_ID,
    };
}
