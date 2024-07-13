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
use tempfile::TempDir;

pub struct BufferPoolForTest<E: EvictionPolicy> {
    pub _temp_dir: TempDir,
    pub bp: BufferPool<E>,
}

impl<E: EvictionPolicy> BufferPoolForTest<E> {
    pub fn new(num_frames: usize) -> Self {
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir(temp_dir.path().join("0")).unwrap();
        let bp = BufferPool::new(temp_dir.path(), num_frames).unwrap();
        Self {
            _temp_dir: temp_dir,
            bp,
        }
    }

    pub fn eviction_stats(&self) -> String {
        self.bp.eviction_stats()
    }

    pub fn file_stats(&self) -> String {
        self.bp.file_stats()
    }
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
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard<E>, MemPoolStatus> {
        self.bp.get_page_for_read(key)
    }

    #[inline]
    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard<E>, MemPoolStatus> {
        self.bp.get_page_for_write(key)
    }

    #[inline]
    fn stats(&self) -> String {
        self.bp.stats()
    }

    #[inline]
    fn reset_stats(&self) {
        self.bp.reset_stats();
    }

    #[inline]
    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.bp.flush_all()
    }

    #[inline]
    fn reset(&self) {
        self.bp.reset();
    }
}

pub fn get_test_bp<E: EvictionPolicy>(num_frames: usize) -> Arc<BufferPoolForTest<E>> {
    Arc::new(BufferPoolForTest::new(num_frames))
}

pub fn get_in_mem_pool() -> Arc<InMemPool<DummyEvictionPolicy>> {
    Arc::new(InMemPool::new())
}
pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp, BufferFrame, BufferPool, BufferPoolForTest, ContainerId,
        ContainerKey, DatabaseId, DummyEvictionPolicy, EvictionPolicy, FrameReadGuard,
        FrameWriteGuard, InMemPool, LRUEvictionPolicy, MemPool, MemPoolStatus, PageFrameKey,
    };
}
