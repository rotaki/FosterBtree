use std::sync::atomic::{AtomicI16, Ordering};

pub struct RwLatch {
    pub cnt: AtomicI16,
}

impl Default for RwLatch {
    fn default() -> Self {
        RwLatch {
            cnt: AtomicI16::new(0), // Up to 2^15 readers or 1 writer
        }
    }
}

impl std::fmt::Display for RwLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Debug for RwLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cnt = self.cnt.load(Ordering::Acquire);
        match cnt {
            0 => write!(f, "Unlocked"),
            cnt if cnt > 0 => write!(f, "Shared({})", cnt),
            _ => write!(f, "Exclusive"),
        }
    }
}

impl RwLatch {
    #[allow(dead_code)]
    pub fn is_locked(&self) -> bool {
        self.cnt.load(Ordering::Acquire) != 0
    }

    #[allow(dead_code)]
    pub fn is_shared(&self) -> bool {
        self.cnt.load(Ordering::Acquire) > 0
    }

    #[allow(dead_code)]
    pub fn is_exclusive(&self) -> bool {
        self.cnt.load(Ordering::Acquire) < 0
    }

    pub fn shared(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected >= 0
                && self.cnt.compare_exchange(
                    expected,
                    expected + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) == Ok(expected)
            {
                break;
            }
            std::hint::spin_loop();
        }
    }

    pub fn try_shared(&self) -> bool {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected < 0 {
                return false;
            }
            if self.cnt.compare_exchange(
                expected,
                expected + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) == Ok(expected)
            {
                return true;
            }
        }
    }

    pub fn exclusive(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected == 0
                && self
                    .cnt
                    .compare_exchange(expected, -1, Ordering::AcqRel, Ordering::Acquire)
                    == Ok(expected)
            {
                break;
            }
            std::hint::spin_loop();
        }
    }

    pub fn try_exclusive(&self) -> bool {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected != 0 {
                return false;
            }
            if self
                .cnt
                .compare_exchange(expected, -1, Ordering::AcqRel, Ordering::Acquire)
                == Ok(expected)
            {
                return true;
            }
        }
    }

    #[allow(dead_code)]
    pub fn upgrade(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected == 1
                && self
                    .cnt
                    .compare_exchange(expected, -1, Ordering::AcqRel, Ordering::Acquire)
                    == Ok(expected)
            {
                break;
            }
            std::hint::spin_loop();
        }
    }

    pub fn try_upgrade(&self) -> bool {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected != 1 {
                return false;
            }
            if self
                .cnt
                .compare_exchange(expected, -1, Ordering::AcqRel, Ordering::Acquire)
                == Ok(expected)
            {
                return true;
            }
        }
    }

    pub fn downgrade(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected == -1
                && self
                    .cnt
                    .compare_exchange(expected, 1, Ordering::AcqRel, Ordering::Acquire)
                    == Ok(expected)
            {
                break;
            }
            std::hint::spin_loop();
        }
    }

    pub fn release_shared(&self) {
        self.cnt.fetch_sub(1, Ordering::Release);
    }

    pub fn release_exclusive(&self) {
        self.cnt.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::UnsafeCell, thread};

    use super::*;

    // Need to wrap the UnsafeCell in a struct to implement Sync
    pub struct Counter(UnsafeCell<usize>);

    impl Counter {
        pub fn new() -> Self {
            Counter(UnsafeCell::new(0))
        }

        pub fn increment(&self) {
            unsafe {
                *self.0.get() += 1;
            }
        }

        pub fn read(&self) -> usize {
            unsafe { *self.0.get() }
        }
    }

    unsafe impl Sync for Counter {}

    pub struct RwLatchProtectedCounter {
        pub rwlatch: RwLatch,
        pub counter: Counter,
    }

    impl Default for RwLatchProtectedCounter {
        fn default() -> Self {
            RwLatchProtectedCounter {
                rwlatch: RwLatch::default(),
                counter: Counter::new(),
            }
        }
    }

    unsafe impl Sync for RwLatchProtectedCounter {}

    #[test]
    fn test_multiple_writers_consistency() {
        let counter = RwLatchProtectedCounter::default();
        thread::scope(|s| {
            for _ in 0..10000 {
                s.spawn(|| {
                    counter.rwlatch.exclusive();
                    counter.counter.increment();
                    counter.rwlatch.release_exclusive();
                });
            }
        });
        assert_eq!(counter.counter.read(), 10000);
    }

    #[test]
    fn test_multiple_readers_do_not_block() {
        let counter = RwLatchProtectedCounter::default();
        thread::scope(|s| {
            for _ in 0..1000 {
                s.spawn(|| {
                    counter.rwlatch.shared();
                    assert_eq!(counter.counter.read(), 0);
                });
            }
        });
        assert_eq!(counter.counter.read(), 0);
    }
}
