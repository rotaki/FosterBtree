//! Overflow table for predictive translation (Section 4.1 + 4.2).
//!
//! Chaining hash table with inlined first slot. **In-place updates** with a
//! versioned lock (PrediCache-style): writers mutate the chain under the lock;
//! readers read version → data → re-read version (no lock, no copy). Chain
//! nodes are allocated and retired via crossbeam_epoch for safe reclamation.

use super::mem_pool_trait::{ContainerKey, PageFrameKey, PageKey};
use crossbeam_epoch::{Atomic, Owned};
use std::cell::UnsafeCell;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

const LOCK_BIT: u64 = 1u64 << 63;
const MAX_READ_RETRIES: u32 = 32;
const MAX_WRITE_SPIN: u32 = 1_000_000;

/// Chain node: key, value, and atomic next pointer. Retired on remove via epoch.
struct ChainNode {
    key: PageKey,
    value: usize,
    next: Atomic<ChainNode>,
}

/// Per-bucket: versioned lock (MSB = locked) and in-place data.
struct Bucket {
    /// MSB = 1 when a writer holds the lock; low 63 bits = version (bumped on unlock).
    version: AtomicU64,
    /// Inlined first slot. Writers mutate under lock; readers read under version check.
    inlined: UnsafeCell<Option<(PageKey, usize)>>,
    /// Head of chain. Atomic so readers can follow without holding the lock.
    chain_head: Atomic<ChainNode>,
}

unsafe impl Send for Bucket {}
unsafe impl Sync for Bucket {}

impl Bucket {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            inlined: UnsafeCell::new(None),
            chain_head: Atomic::null(),
        }
    }

    #[inline]
    fn is_locked(v: u64) -> bool {
        v & LOCK_BIT != 0
    }

    /// Try to acquire the write lock (set MSB). Returns true on success.
    #[inline]
    fn try_lock(&self) -> bool {
        let mut v = self.version.load(Ordering::Acquire);
        for _ in 0..MAX_WRITE_SPIN {
            if Self::is_locked(v) {
                std::hint::spin_loop();
                v = self.version.load(Ordering::Acquire);
                continue;
            }
            match self.version.compare_exchange_weak(
                v,
                v | LOCK_BIT,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => v = actual,
            }
        }
        false
    }

    /// Release the write lock and bump version.
    #[inline]
    fn unlock(&self) {
        let v = self.version.load(Ordering::Acquire);
        debug_assert!(Self::is_locked(v));
        self.version
            .store((v & !LOCK_BIT).wrapping_add(1), Ordering::Release);
    }
}

/// Overflow table: fixed number of buckets, chaining with inlined first slot, in-place updates.
pub(crate) struct OverflowTable {
    num_buckets: usize,
    buckets: Vec<Bucket>,
}

impl OverflowTable {
    pub(crate) fn new(num_buckets: usize) -> Self {
        let buckets = (0..num_buckets).map(|_| Bucket::new()).collect();
        Self {
            num_buckets,
            buckets,
        }
    }

    #[inline]
    fn bucket_index(&self, key: &PageKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.num_buckets
    }

    /// Lock-free lookup using a precomputed bucket index.
    #[inline]
    pub(crate) fn lookup_with_bucket(&self, key: &PageKey, bucket_idx: usize) -> Option<usize> {
        let idx = bucket_idx % self.num_buckets;
        let bucket = &self.buckets[idx];
        let guard = crossbeam_epoch::pin();
        for _ in 0..MAX_READ_RETRIES {
            let v1 = bucket.version.load(Ordering::Acquire);
            if Bucket::is_locked(v1) {
                continue;
            }
            let result = unsafe {
                let inlined = (*bucket.inlined.get()).clone();
                if let Some((k, v)) = &inlined {
                    if k == key {
                        Some(*v)
                    } else {
                        Self::lookup_chain(&bucket.chain_head, key, &guard)
                    }
                } else {
                    Self::lookup_chain(&bucket.chain_head, key, &guard)
                }
            };
            let v2 = bucket.version.load(Ordering::Acquire);
            if v1 == v2 {
                return result;
            }
        }
        self.lookup_slow(key, idx, &guard)
    }

    #[inline]
    fn lookup_chain(
        head: &Atomic<ChainNode>,
        key: &PageKey,
        guard: &crossbeam_epoch::Guard,
    ) -> Option<usize> {
        let mut current = head.load(Ordering::Acquire, guard);
        while !current.is_null() {
            let node = unsafe { current.deref() };
            if node.key == *key {
                return Some(node.value);
            }
            current = node.next.load(Ordering::Acquire, guard);
        }
        None
    }

    #[cold]
    fn lookup_slow(
        &self,
        key: &PageKey,
        idx: usize,
        guard: &crossbeam_epoch::Guard,
    ) -> Option<usize> {
        let bucket = &self.buckets[idx];
        let inlined = unsafe { (*bucket.inlined.get()).clone() };
        if let Some((k, v)) = &inlined {
            if k == key {
                return Some(*v);
            }
        }
        Self::lookup_chain(&bucket.chain_head, key, guard)
    }

    /// Lock-free read path.
    #[inline]
    pub(crate) fn lookup(&self, key: &PageKey) -> Option<usize> {
        let idx = self.bucket_index(key);
        self.lookup_with_bucket(key, idx)
    }

    /// In-place insert: take versioned lock, mutate bucket, unlock. No clone.
    #[inline]
    pub(crate) fn insert(&self, key: PageKey, frame_id: usize) {
        let idx = self.bucket_index(&key);
        let bucket = &self.buckets[idx];
        let guard = crossbeam_epoch::pin();
        while !bucket.try_lock() {
            std::hint::spin_loop();
        }
        unsafe {
            let inlined = &mut *bucket.inlined.get();
            if let Some((k, ref mut slot_val)) = inlined {
                if *k == key {
                    *slot_val = frame_id;
                    bucket.unlock();
                    return;
                }
            }
            if let Some(ptr) = Self::find_in_chain_mut(&bucket.chain_head, &key, &guard) {
                *ptr = frame_id;
                bucket.unlock();
                return;
            }
            if inlined.is_none() {
                *inlined = Some((key, frame_id));
            } else {
                let current = bucket.chain_head.load(Ordering::Acquire, &guard);
                let new_node = Owned::new(ChainNode {
                    key,
                    value: frame_id,
                    next: Atomic::null(),
                });
                new_node.next.store(current, Ordering::Release);
                bucket
                    .chain_head
                    .store(new_node.into_shared(&guard), Ordering::Release);
            }
        }
        bucket.unlock();
    }

    /// Find *mut value in chain under lock. Caller holds lock; mutate via the returned pointer.
    #[inline]
    unsafe fn find_in_chain_mut(
        head: &Atomic<ChainNode>,
        key: &PageKey,
        guard: &crossbeam_epoch::Guard,
    ) -> Option<*mut usize> {
        let mut current = head.load(Ordering::Acquire, guard);
        while !current.is_null() {
            let node = current.deref() as *const ChainNode as *mut ChainNode;
            if (*node).key == *key {
                return Some(&mut (*node).value as *mut usize);
            }
            current = (*node).next.load(Ordering::Acquire, guard);
        }
        None
    }

    /// In-place remove: take versioned lock, unlink node, retire with epoch, unlock.
    #[inline]
    pub(crate) fn remove(&self, key: &PageKey) -> Option<usize> {
        let idx = self.bucket_index(key);
        let bucket = &self.buckets[idx];
        let guard = crossbeam_epoch::pin();
        while !bucket.try_lock() {
            std::hint::spin_loop();
        }
        let result = unsafe {
            let inlined = &mut *bucket.inlined.get();
            if let Some((k, val)) = inlined.as_ref() {
                if k == key {
                    let out = *val;
                    let head = bucket.chain_head.load(Ordering::Acquire, &guard);
                    if !head.is_null() {
                        let node = head.deref();
                        *inlined = Some((node.key, node.value));
                        bucket
                            .chain_head
                            .store(node.next.load(Ordering::Acquire, &guard), Ordering::Release);
                        guard.defer_destroy(head);
                    } else {
                        *inlined = None;
                    }
                    bucket.unlock();
                    return Some(out);
                }
            }
            Self::remove_from_chain(&bucket.chain_head, key, &guard)
        };
        bucket.unlock();
        result
    }

    /// Unlink the first node matching key from the chain; retire it. Returns its value. Caller holds lock.
    #[inline]
    unsafe fn remove_from_chain(
        head: &Atomic<ChainNode>,
        key: &PageKey,
        guard: &crossbeam_epoch::Guard,
    ) -> Option<usize> {
        let mut prev_ptr: Option<&Atomic<ChainNode>> = None;
        let mut current = head.load(Ordering::Acquire, guard);
        while !current.is_null() {
            let node = current.deref();
            if node.key == *key {
                let out = node.value;
                let next = node.next.load(Ordering::Acquire, guard);
                match prev_ptr {
                    Some(prev) => prev.store(next, Ordering::Release),
                    None => head.store(next, Ordering::Release),
                }
                guard.defer_destroy(current);
                return Some(out);
            }
            prev_ptr = Some(&node.next);
            current = node.next.load(Ordering::Acquire, guard);
        }
        None
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &PageKey) -> bool {
        self.lookup(key).is_some()
    }

    pub(crate) fn get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        let mut out = Vec::new();
        self.for_each_entry(|pk, frame_id| {
            if pk.c_key == c_key {
                out.push(PageFrameKey::new_with_frame_id(
                    pk.c_key,
                    pk.page_id,
                    frame_id as u32,
                ));
            }
        });
        out
    }

    /// Lock-free iteration: copy entries under version check, then call f.
    pub(crate) fn for_each_entry(&self, mut f: impl FnMut(PageKey, usize)) {
        let guard = crossbeam_epoch::pin();
        for bucket in &self.buckets {
            for _ in 0..MAX_READ_RETRIES {
                let v1 = bucket.version.load(Ordering::Acquire);
                if Bucket::is_locked(v1) {
                    continue;
                }
                let mut entries = Vec::new();
                unsafe {
                    if let Some((pk, fid)) = (*bucket.inlined.get()).clone() {
                        entries.push((pk, fid));
                    }
                    let mut current = bucket.chain_head.load(Ordering::Acquire, &guard);
                    while !current.is_null() {
                        let node = current.deref();
                        entries.push((node.key, node.value));
                        current = node.next.load(Ordering::Acquire, &guard);
                    }
                }
                let v2 = bucket.version.load(Ordering::Acquire);
                if v1 == v2 {
                    for (pk, fid) in entries {
                        f(pk, fid);
                    }
                    break;
                }
            }
        }
    }
}
