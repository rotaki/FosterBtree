mod buffer_frame;
mod buffer_pool;
mod eviction_policy;
mod in_mem_pool;
mod mem_pool_trait;

use std::sync::Arc;

pub use buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard};
pub use buffer_pool::BufferPool;
pub use eviction_policy::{DummyEvictionPolicy, EvictionPolicy, LRUEvictionPolicy};
pub use in_mem_pool::InMemPool;
pub use mem_pool_trait::{
    ContainerId, ContainerKey, DatabaseId, MemPool, MemPoolStatus, PageFrameKey,
};

use crate::random::gen_random_pathname;

pub fn get_test_bp(num_frames: usize) -> Arc<BufferPool<LRUEvictionPolicy>> {
    let dir = gen_random_pathname(Some("test_bp"));
    Arc::new(BufferPool::new(dir, num_frames, true).unwrap())
}
pub fn get_in_mem_pool() -> Arc<InMemPool<DummyEvictionPolicy>> {
    Arc::new(InMemPool::new())
}
pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp, BufferFrame, BufferPool, ContainerId, ContainerKey,
        DatabaseId, DummyEvictionPolicy, EvictionPolicy, FrameReadGuard, FrameWriteGuard,
        InMemPool, LRUEvictionPolicy, MemPool, MemPoolStatus, PageFrameKey,
    };
}
