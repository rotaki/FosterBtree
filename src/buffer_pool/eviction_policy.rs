use super::buffer_frame::BufferFrame;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// Static atomic counter for LRU timestamp
pub const INITIAL_COUNTER: u64 = 1;
static LRU_COUNTER: AtomicU64 = AtomicU64::new(INITIAL_COUNTER);

pub trait EvictionPolicy: Send + Sync {
    fn new() -> Self;
    /// Returns the eviction score of the buffer frame.
    /// The lower the score, the more likely the buffer frame is to be evicted.
    fn score(&self, frame: &BufferFrame<Self>) -> u64
    where
        Self: Sized;
    fn update(&mut self);
    fn reset(&mut self);
}

pub struct DummyEvictionPolicy; // Used for in-memory pool
impl EvictionPolicy for DummyEvictionPolicy {
    #[inline]
    fn new() -> Self {
        DummyEvictionPolicy
    }

    #[inline]
    fn score(&self, _frame: &BufferFrame<Self>) -> u64 {
        0
    }

    #[inline]
    fn update(&mut self) {}

    #[inline]
    fn reset(&mut self) {}
}

pub struct LRUEvictionPolicy {
    pub score: u64,
}

impl EvictionPolicy for LRUEvictionPolicy {
    fn new() -> Self {
        LRUEvictionPolicy {
            score: INITIAL_COUNTER,
        }
    }

    fn score(&self, _: &BufferFrame<Self>) -> u64
    where
        Self: Sized,
    {
        self.score
    }

    fn update(&mut self) {
        self.score = LRU_COUNTER.fetch_add(1, Ordering::AcqRel);
    }

    fn reset(&mut self) {
        self.score = INITIAL_COUNTER;
    }
}

#[cfg(test)]
mod tests {
    use super::{DummyEvictionPolicy, EvictionPolicy, LRUEvictionPolicy};
    use crate::buffer_pool::{
        prelude::{get_test_bp, BufferPoolForTest, MemPool},
        ContainerKey,
    };

    /*
    #[test]
    fn test_lru_eviction_policy() {
        let bp = get_test_bp::<LRUEvictionPolicy>(3);
        let c_key = ContainerKey::new(0, 0);
        let (p0_key, p1_key, p2_key) = {
            let p0 = bp.create_new_page_for_write(c_key).unwrap();
            let p1 = bp.create_new_page_for_write(c_key).unwrap();
            let p2 = bp.create_new_page_for_write(c_key).unwrap();
            (p0.key().unwrap(), p1.key().unwrap(), p2.key().unwrap())
        };

        let (victim, is_dirty) = bp.choose_victim().unwrap();
        assert_eq!(victim, 0);
        assert_eq!(is_dirty, true);
        let (victim, is_dirty) = bp.choose_victim().unwrap();
        assert_eq!(victim, 0);
        assert_eq!(is_dirty, true);

        {
            let _p0 = bp.get_page_for_read(p0_key).unwrap();
        }
        let (victim, is_dirty) = bp.choose_victim().unwrap();
        assert_eq!(victim, 1);
        assert_eq!(is_dirty, true);

        {
            let _p1 = bp.get_page_for_read(p1_key).unwrap();
        }
        let (victim, is_dirty) = bp.choose_victim().unwrap();
        assert_eq!(victim, 2);
        assert_eq!(is_dirty, true);

        {
            let _p2 = bp.get_page_for_read(p2_key).unwrap();
        }
        let (victim, is_dirty) = bp.choose_victim().unwrap();
        assert_eq!(victim, 0);
        assert_eq!(is_dirty, true);

        {
            let _p3 = bp.create_new_page_for_write(c_key).unwrap();
        }
        let (victim, is_dirty) = bp.choose_victim().unwrap();
        assert_eq!(victim, 1);
        assert_eq!(is_dirty, true);
    }
    */
}
