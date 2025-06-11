mod buffer_pool;
mod buffer_pool_clock;
mod eviction_policy;
mod frame_guards;
mod in_mem_pool;
mod mem_pool_trait;
mod resident_set;
mod vmcache;

use std::sync::Arc;

pub use buffer_pool::BufferPool;
pub use buffer_pool_clock::BufferPoolClock;
pub use eviction_policy::EvictionPolicy;
pub use frame_guards::{FrameReadGuard, FrameWriteGuard};
pub use in_mem_pool::InMemPool;
pub use mem_pool_trait::{
    ContainerId, ContainerKey, DatabaseId, MemPool, MemPoolStatus, PageFrameKey,
};
pub use vmcache::VMCachePool;

use crate::{container::ContainerManager, random::gen_random_pathname};

/// Buffer pool with persistent storage.
pub fn get_bp(num_frames: usize, cm: Arc<ContainerManager>) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        Arc::new(VMCachePool::<false, 64>::new(num_frames, cm).unwrap())
    }
    #[cfg(feature = "bp_clock")]
    {
        Arc::new(BufferPoolClock::<64>::new(num_frames, cm).unwrap())
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
        get_test_vmcache::<false, 64>(num_frames)
    }
    #[cfg(feature = "bp_clock")]
    {
        get_test_bp_clock::<64>(num_frames)
    }
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        get_test_bp_lru(num_frames)
    }
}

pub fn get_test_vmcache<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize>(
    num_frames: usize,
) -> Arc<VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>> {
    let base_dir = gen_random_pathname(Some("test_bp_vmcache_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(VMCachePool::<IS_SMALL, EVICTION_BATCH_SIZE>::new(num_frames, cm).unwrap())
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

pub fn get_test_bp_clock<const EVICTION_BATCH_SIZE: usize>(
    num_frames: usize,
) -> Arc<BufferPoolClock<EVICTION_BATCH_SIZE>> {
    let base_dir = gen_random_pathname(Some("test_bp_clock_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPoolClock::<EVICTION_BATCH_SIZE>::new(num_frames, cm).unwrap())
}

pub fn get_in_mem_pool() -> Arc<InMemPool> {
    Arc::new(InMemPool::new())
}
pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp_lru, BufferPool, ContainerId, ContainerKey, DatabaseId,
        FrameReadGuard, FrameWriteGuard, InMemPool, MemPool, MemPoolStatus, PageFrameKey,
    };
}
