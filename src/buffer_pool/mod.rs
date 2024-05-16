mod buffer_frame;
mod eviction_policy;
mod in_mem_pool;
mod mem_pool_trait;
mod raw_buffer_pool;
mod war_buffer_pool;

use std::sync::Arc;

use eviction_policy::{DummyEvictionPolicy, EvictionPolicy, LRUEvictionPolicy};

pub use buffer_frame::{FrameReadGuard, FrameWriteGuard};
pub use in_mem_pool::InMemPool;
pub use mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, PageKey};
pub use raw_buffer_pool::RAWBufferPool;
use tempfile::TempDir;

pub struct BufferPoolForTest<E: EvictionPolicy> {
    pub _temp_dir: TempDir,
    pub bp: RAWBufferPool<E>,
}

impl<E: EvictionPolicy> BufferPoolForTest<E> {
    pub fn new(num_frames: usize) -> Self {
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir(temp_dir.path().join("0")).unwrap();
        let bp = RAWBufferPool::new(temp_dir.path(), num_frames).unwrap();
        Self {
            _temp_dir: temp_dir,
            bp,
        }
    }

    pub fn eviction_stats(&self) -> String {
        self.bp.eviction_stats()
    }

    // #[cfg(test)]
    // pub fn choose_victim(&self) -> Option<(usize, bool)> {
    //     self.bp.choose_victim()
    // }
}

impl<E: EvictionPolicy> MemPool<E> for BufferPoolForTest<E> {
    #[inline]
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard<E>, MemPoolStatus> {
        self.bp.create_new_page_for_write(c_key)
    }

    #[inline]
    fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard<E>, MemPoolStatus> {
        self.bp.get_page_for_read(key)
    }

    #[inline]
    fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard<E>, MemPoolStatus> {
        self.bp.get_page_for_write(key)
    }

    #[inline]
    fn reset(&self) {
        self.bp.reset();
    }

    #[cfg(test)]
    fn run_checks(&self) {
        self.bp.run_checks();
    }
}

pub fn get_test_bp<E: EvictionPolicy>(num_frames: usize) -> Arc<BufferPoolForTest<E>> {
    Arc::new(BufferPoolForTest::new(num_frames))
}

pub fn get_in_mem_pool() -> Arc<InMemPool<DummyEvictionPolicy>> {
    Arc::new(InMemPool::new())
}
pub mod prelude {
    pub use super::buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard};
    pub use super::eviction_policy::{DummyEvictionPolicy, EvictionPolicy, LRUEvictionPolicy};
    pub use super::mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, PageKey};
    pub use super::{get_in_mem_pool, get_test_bp, BufferPoolForTest};
    pub use super::{InMemPool, RAWBufferPool};
}
