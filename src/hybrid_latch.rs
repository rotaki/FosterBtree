use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use crate::rwlatch::RwLatch;

#[allow(dead_code)]
pub struct HybridLatch {
    rwlatch: RwLatch,
    version: AtomicU64,
}

#[allow(dead_code)]
impl HybridLatch {
    pub fn new() -> Self {
        Self {
            rwlatch: RwLatch::default(),
            version: AtomicU64::new(0),
        }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn is_exclusive(&self) -> bool {
        self.rwlatch.is_exclusive()
    }

    pub fn release_exclusive(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.rwlatch.release_exclusive();
    }

    pub fn try_exclusive(&self) -> bool {
        self.rwlatch.try_exclusive()
    }
}

#[allow(dead_code)]
pub struct HybridLatchGuardedStructure<T: Clone> {
    latch: HybridLatch,
    data: T,
}

#[allow(dead_code)]
impl<T: Clone> HybridLatchGuardedStructure<T> {
    pub fn new(data: T) -> Self {
        Self {
            latch: HybridLatch::new(),
            data,
        }
    }

    pub fn optimistic_read(&self) -> OptimisticReadGuard<T> {
        let base = 2;
        let mut attempt = 0;

        while self.latch.is_exclusive() {
            // Exponential backoff
            std::thread::sleep(std::time::Duration::from_nanos(u64::pow(base, attempt)));
            attempt += 1;
        }

        let version = self.latch.version();
        OptimisticReadGuard {
            seen_version: version,
            guarded_struct: self,
        }
    }

    pub fn write(&self) -> WriteGuard<T> {
        let base = 2;
        let mut attempt = 0;

        while !self.latch.try_exclusive() {
            // Exponential backoff
            std::thread::sleep(std::time::Duration::from_nanos(u64::pow(base, attempt)));
            attempt += 1;
        }

        WriteGuard {
            downgraded: AtomicBool::new(false),
            guarded_struct: self as *const _ as *mut _,
        }
    }
}

#[allow(dead_code)]
pub struct OptimisticReadGuard<T: Clone> {
    seen_version: u64,
    guarded_struct: *const HybridLatchGuardedStructure<T>,
}

#[allow(dead_code)]
impl<T: Clone> OptimisticReadGuard<T> {
    pub fn validate(&self) -> bool {
        let guarded_struct = unsafe { &*self.guarded_struct };
        if guarded_struct.latch.is_exclusive() {
            false
        } else {
            guarded_struct.latch.version() == self.seen_version
        }
    }

    /// Upgrade fails in the following cases
    /// 1. If the version of the guarded struct has changed
    /// 2. If the upgrade to exclusive latch fails due to concurrent exclusive latch
    pub fn try_upgrade(self) -> Option<WriteGuard<T>> {
        let guarded_struct = unsafe { &*self.guarded_struct };
        if guarded_struct.latch.try_exclusive() {
            if guarded_struct.latch.version() == self.seen_version {
                let guarded_struct = unsafe { &mut *(self.guarded_struct as *mut _) };
                Some(WriteGuard {
                    downgraded: AtomicBool::new(false),
                    guarded_struct,
                })
            } else {
                // Validation failed
                guarded_struct.latch.release_exclusive();
                None
            }
        } else {
            // Concurrent exclusive latch
            None
        }
    }

    pub unsafe fn get(&self) -> &T {
        &(*self.guarded_struct).data
    }
}

#[allow(dead_code)]
pub struct WriteGuard<T: Clone> {
    downgraded: AtomicBool,
    guarded_struct: *mut HybridLatchGuardedStructure<T>,
}

#[allow(dead_code)]
impl<T: Clone> Deref for WriteGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: Exclusive latch is held
        unsafe { &(*self.guarded_struct).data }
    }
}

impl<T: Clone> DerefMut for WriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: Exclusive latch is held
        unsafe { &mut (*self.guarded_struct).data }
    }
}

impl<T: Clone> Drop for WriteGuard<T> {
    fn drop(&mut self) {
        // SAFETY: Exclusive latch is held
        if !self.downgraded.load(Ordering::Acquire) {
            unsafe {
                (*self.guarded_struct).latch.release_exclusive();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimistic_read_guard_concurrent_exclusive() {
        let guarded_struct = HybridLatchGuardedStructure::new(10);
        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 10);

        let mut write_guard = guarded_struct.write();
        *write_guard = 20;
        drop(write_guard);

        // Validation should fail
        assert!(!optimistic_read_guard.validate());

        // Check if the data is updated
        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 20);
        assert!(optimistic_read_guard.validate());
    }

    #[test]
    fn test_optimistic_read_guard_try_upgrade() {
        let guarded_struct = HybridLatchGuardedStructure::new(10);
        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 10);

        let mut write_guard = optimistic_read_guard.try_upgrade().unwrap();
        *write_guard = 30;
        drop(write_guard);

        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 30);
        assert!(optimistic_read_guard.validate());
    }

    #[test]
    fn test_optimistic_read_guard_try_upgrade_concurrent_exclusive() {
        let guarded_struct = HybridLatchGuardedStructure::new(10);

        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 10);

        let mut write_guard = guarded_struct.write();
        *write_guard = 30;

        assert!(optimistic_read_guard.try_upgrade().is_none());
    }

    #[test]
    fn test_many_optimistic_reads() {
        let guarded_struct = HybridLatchGuardedStructure::new(10);

        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 10);

        let mut write_guard = optimistic_read_guard.try_upgrade().unwrap();
        *write_guard = 20;
        drop(write_guard);

        let optimistic_read_guard1 = guarded_struct.optimistic_read();
        let optimistic_read_guard2 = guarded_struct.optimistic_read();
        let optimistic_read_guard3 = guarded_struct.optimistic_read();

        assert_eq!(*unsafe { optimistic_read_guard1.get() }, 20);
        assert_eq!(*unsafe { optimistic_read_guard2.get() }, 20);
        assert_eq!(*unsafe { optimistic_read_guard3.get() }, 20);

        assert!(optimistic_read_guard1.validate());
        assert!(optimistic_read_guard2.validate());
        assert!(optimistic_read_guard3.validate());
    }

    #[test]
    fn test_write_guard_drop() {
        let guarded_struct = HybridLatchGuardedStructure::new(10);
        let mut write_guard = guarded_struct.write();
        *write_guard = 20;
        drop(write_guard);

        let mut write_guard = guarded_struct.write();
        *write_guard = 30;
        drop(write_guard);

        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 30);
        assert!(optimistic_read_guard.validate());
    }

    #[test]
    fn test_optimistic_read_guard_validate_validation_failed() {
        let guarded_struct = HybridLatchGuardedStructure::new(10);
        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 10);

        let mut write_guard = guarded_struct.write();
        *write_guard = 20;
        drop(write_guard);

        assert!(!optimistic_read_guard.validate());

        let optimistic_read_guard = guarded_struct.optimistic_read();
        assert_eq!(*unsafe { optimistic_read_guard.get() }, 20);
        assert!(optimistic_read_guard.validate());
    }
}
