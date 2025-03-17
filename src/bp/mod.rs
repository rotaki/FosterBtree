mod buffer_frame;
mod buffer_pool;
mod eviction_policy;
mod in_mem_pool;
mod mem_pool_trait;

use std::sync::Arc;

pub use buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard};
pub use buffer_pool::BufferPool;
pub use in_mem_pool::InMemPool;
pub use mem_pool_trait::{
    ContainerId, ContainerKey, DatabaseId, MemPool, MemPoolStatus, PageFrameKey,
};

use crate::{container::ContainerManager, random::gen_random_pathname};

pub fn get_test_bp(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_with_kpc(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_with_kpc"));
    let cm = Arc::new(ContainerManager::new(base_dir, false, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cm).unwrap())
}
pub fn get_in_mem_pool() -> Arc<InMemPool> {
    Arc::new(InMemPool::new())
}
pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp, BufferFrame, BufferPool, ContainerId, ContainerKey,
        DatabaseId, FrameReadGuard, FrameWriteGuard, InMemPool, MemPool, MemPoolStatus,
        PageFrameKey,
    };
}
