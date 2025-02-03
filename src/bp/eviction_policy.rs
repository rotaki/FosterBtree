use rand::RngCore;

use crate::random::small_thread_rng;

use super::buffer_frame::BufferFrame;
use std::sync::atomic::{AtomicU64, Ordering};

// Static atomic counter for LRU timestamp
pub const INITIAL_COUNTER: u64 = 1;
static LRU_COUNTER: AtomicU64 = AtomicU64::new(INITIAL_COUNTER);

// Structures implementing this trait are used to determine which buffer frame to evict.
// It must ensure that multiple threads can safely update the internal states concurrently.
pub trait EvictionPolicy: Send + Sync {
    fn new() -> Self;
    /// Returns the eviction score of the buffer frame.
    /// The lower the score, the more likely the buffer frame is to be evicted.
    fn score(&self, frame: &BufferFrame) -> u64
    where
        Self: Sized;
    fn update(&self);
    fn reset(&self);
}

pub struct DummyEvictionPolicy; // Used for in-memory pool
impl EvictionPolicy for DummyEvictionPolicy {
    #[inline]
    fn new() -> Self {
        DummyEvictionPolicy
    }

    #[inline]
    fn score(&self, _frame: &BufferFrame) -> u64 {
        0
    }

    #[inline]
    fn update(&self) {}

    #[inline]
    fn reset(&self) {}
}

pub struct LRUEvictionPolicy {
    pub score: AtomicU64,
}

impl EvictionPolicy for LRUEvictionPolicy {
    fn new() -> Self {
        LRUEvictionPolicy {
            score: AtomicU64::new(INITIAL_COUNTER),
        }
    }

    fn score(&self, _: &BufferFrame) -> u64
    where
        Self: Sized,
    {
        self.score.load(Ordering::Acquire)
    }

    fn update(&self) {
        let mut rng = small_thread_rng();
        // Only update the score with a probability of 1/10 because LRU_COUNTER is a shared resource
        if rng.next_u64() % 10 == 0 {
            self.score
                .fetch_max(LRU_COUNTER.fetch_add(1, Ordering::AcqRel), Ordering::AcqRel);
        }
    }

    fn reset(&self) {
        self.score.store(INITIAL_COUNTER, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {

    /*
    #[test]
    fn test_lru_eviction_policy() {
        let bp = get_test_bp::<LRUEvictionPolicy>(3);
        let c_key = ContainerKey::new(0, 0);
        let (p0_key, p1_key, p2_key) = {
            let p0 = bp.create_new_page_for_write(c_key).unwrap();
            let p1 = bp.create_new_page_for_write(c_key).unwrap();
            let p2 = bp.create_new_page_for_write(c_key).unwrap();
            (
                p0.page_frame_key().unwrap(),
                p1.page_frame_key().unwrap(),
                p2.page_frame_key().unwrap(),
            )
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
