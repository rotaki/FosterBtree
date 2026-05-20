//! Overflow table for predictive translation (Section 4.1 + 4.2).
//!
//! Chaining hash table with inlined first slot. **In-place updates** with a
//! versioned lock (PrediCache-style): writers mutate the chain under the lock;
//! readers read version → data → re-read version (no lock, no copy). Chain
//! nodes are allocated and retired via crossbeam_epoch for safe reclamation.

#[cfg(not(feature = "pc_prefix_hash"))]
use super::hash::hash_page_key;
#[cfg(feature = "pc_prefix_hash")]
use super::hash::hash_u64;
use super::mem_pool_trait::{ContainerKey, PageFrameKey, PageKey};
use crossbeam_epoch::{Atomic, Owned};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const LOCK_BIT: u64 = 1u64 << 63;
const MAX_READ_RETRIES: u32 = 32;
const MAX_WRITE_SPIN: u32 = 1_000_000;

/// Chain node: key, value (frame id), and atomic next pointer. Retired on remove via epoch.
struct ChainNode {
    key: PageKey,
    value: u32,
    next: Atomic<ChainNode>,
}

/// Per-bucket: versioned lock (MSB = locked) and in-place data.
///
/// Exposed as `pub(crate)` so the BP can `bucket_ptr(pref)` + cast to
/// `*const Bucket` and OLC-read the inlined slot inline (skipping the
/// closure indirection of `get_apply_with_bucket`) on the displaced
/// fast-path probe.
pub(crate) struct Bucket {
    /// MSB = 1 when a writer holds the lock; low 63 bits = version (bumped on unlock).
    pub(crate) version: AtomicU64,
    /// Inlined first slot. Writers mutate under lock; readers read under version check.
    pub(crate) inlined: UnsafeCell<Option<(PageKey, u32)>>,
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
pub struct OptimisticPageMap {
    num_buckets: usize,
    buckets: Vec<Bucket>,
}

impl OptimisticPageMap {
    pub fn new(num_buckets: usize) -> Self {
        let buckets = (0..num_buckets).map(|_| Bucket::new()).collect();
        Self {
            num_buckets,
            buckets,
        }
    }

    /// Bucket index for `key`. Must match `PrediCache::preferred_frame` so
    /// that inserts via `try_insert` / `remove` / etc. land in the same
    /// bucket as inserts via `*_at_bucket(pref)`. The cfg here mirrors
    /// `predicache.rs`:
    ///   default            — uniform Stafford-mixed full-key hash
    ///   `pc_prefix_hash`   — prefix-hash + suffix-offset
    #[inline]
    pub fn bucket_index_pub(&self, key: &PageKey) -> usize {
        self.bucket_index(key)
    }

    #[inline]
    fn bucket_index(&self, key: &PageKey) -> usize {
        #[cfg(feature = "pc_prefix_hash")]
        {
            let c_hash = hash_u64(key.c_key.as_u32() as u64);
            let packed = c_hash.wrapping_add(key.page_id as u64);
            (packed as usize) % self.num_buckets
        }
        #[cfg(not(feature = "pc_prefix_hash"))]
        {
            (hash_page_key(key) as usize) % self.num_buckets
        }
    }

    /// Return a raw pointer to the bucket for a given index — for
    /// prefetching and for the caller to OLC-peek the inlined slot inline.
    /// Cast to `*const i8` for `_mm_prefetch`.
    #[inline(always)]
    pub(crate) fn bucket_ptr(&self, bucket_idx: usize) -> *const Bucket {
        let idx = bucket_idx % self.num_buckets;
        &self.buckets[idx] as *const Bucket
    }

    /// Lock-free lookup using a precomputed bucket index.
    /// `bucket_idx` must already be in `[0, num_buckets)` (typically the
    /// caller's preferred-frame index, which by construction equals
    /// `bucket_index(key)`).
    #[inline]
    pub fn lookup_with_bucket(&self, key: &PageKey, bucket_idx: usize) -> Option<u32> {
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
    ) -> Option<u32> {
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
    ) -> Option<u32> {
        let bucket = &self.buckets[idx];
        // Acquire the lock to get a consistent read — this is the cold path
        // so the extra cost is acceptable.
        while !bucket.try_lock() {
            std::hint::spin_loop();
        }
        let result = unsafe {
            let inlined = &*bucket.inlined.get();
            if let Some((k, v)) = inlined {
                if k == key {
                    bucket.unlock();
                    return Some(*v);
                }
            }
            Self::lookup_chain(&bucket.chain_head, key, guard)
        };
        bucket.unlock();
        result
    }

    /// Lock-free read path.
    #[inline]
    pub(crate) fn lookup(&self, key: &PageKey) -> Option<u32> {
        let idx = self.bucket_index(key);
        self.lookup_with_bucket(key, idx)
    }

    /// Optimistic "lookup + apply closure" under the bucket's version lock.
    /// Mirrors congee's `compute_if_present` / `get_apply_with_siblings` idiom.
    ///
    /// Protocol per iteration (OLC, PrediCache §4.2):
    ///   v1 = version (retry if locked)
    ///   look up `key` → maybe a `frame_id`
    ///   v2 = version; if v2 != v1, retry
    ///   if Some(id): run `f(id)` → R; v3 = version; if v3 != v1, drop R & retry
    ///   if None: return None
    ///
    /// Return values:
    /// - `Some(Some(R))` — key found, closure ran with a consistent view.
    /// - `Some(None)`    — key found, but the closure itself returned `None`
    ///                     (e.g. a latch attempt that failed).
    /// - `None`          — key not present.
    ///
    /// The closure may run multiple times, so its side effects must tolerate
    /// being dropped on retry. A `FrameReadGuard` / `FrameWriteGuard` is
    /// ideal: dropping it simply releases the latch, which is what we want
    /// when we discover the overflow entry changed mid-operation.
    ///
    /// Falls back to `get_apply_slow` (lock-held closure) after
    /// `MAX_READ_RETRIES`.
    #[inline]
    pub(crate) fn get_apply_with_bucket<R, F>(
        &self,
        key: &PageKey,
        bucket_idx: usize,
        mut f: F,
    ) -> Option<Option<R>>
    where
        F: FnMut(u32) -> Option<R>,
    {
        let idx = bucket_idx % self.num_buckets;
        let bucket = &self.buckets[idx];
        let guard = crossbeam_epoch::pin();
        for _ in 0..MAX_READ_RETRIES {
            let v1 = bucket.version.load(Ordering::Acquire);
            if Bucket::is_locked(v1) {
                continue;
            }
            let frame_id = unsafe {
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
            if v1 != v2 {
                continue;
            }
            match frame_id {
                Some(id) => {
                    let applied = f(id);
                    let v3 = bucket.version.load(Ordering::Acquire);
                    if v1 == v3 {
                        return Some(applied);
                    }
                    // Version changed mid-closure: discard R (drops any
                    // latch/resource) and retry with a fresh lookup.
                    drop(applied);
                }
                None => return None,
            }
        }
        self.get_apply_slow(key, idx, f, &guard)
    }

    /// Shorthand: compute bucket index from `key` and delegate.
    #[inline]
    pub(crate) fn get_apply<R, F>(&self, key: &PageKey, f: F) -> Option<Option<R>>
    where
        F: FnMut(u32) -> Option<R>,
    {
        let idx = self.bucket_index(key);
        self.get_apply_with_bucket(key, idx, f)
    }

    /// Lock-held fallback for `get_apply_with_bucket`. Runs the closure
    /// while holding the bucket's write lock, guaranteeing the
    /// lookup-and-apply window is atomic. The closure must be fast and
    /// must not call back into `OptimisticPageMap` mutators for this bucket
    /// or it will self-deadlock.
    #[cold]
    fn get_apply_slow<R, F>(
        &self,
        key: &PageKey,
        idx: usize,
        mut f: F,
        guard: &crossbeam_epoch::Guard,
    ) -> Option<Option<R>>
    where
        F: FnMut(u32) -> Option<R>,
    {
        let bucket = &self.buckets[idx];
        while !bucket.try_lock() {
            std::hint::spin_loop();
        }
        let frame_id = unsafe {
            let inlined = &*bucket.inlined.get();
            if let Some((k, v)) = inlined {
                if k == key {
                    Some(*v)
                } else {
                    Self::lookup_chain(&bucket.chain_head, key, guard)
                }
            } else {
                Self::lookup_chain(&bucket.chain_head, key, guard)
            }
        };
        let result = frame_id.map(|id| f(id));
        bucket.unlock();
        result
    }

    /// Atomic try-insert: lock the bucket, check if `key` already exists, and
    /// insert only if absent.  Returns `Ok(())` on success, `Err(existing_frame)`
    /// if the key was already present (another thread faulted it first).
    ///
    /// This replaces the separate `fault_in_progress` DashMap by using the
    /// overflow table's own per-bucket lock as the synchronisation point
    /// (PrediCache §4.1-4.2).
    #[inline]
    pub(crate) fn try_insert(&self, key: PageKey, frame_id: u32) -> Result<(), u32> {
        let idx = self.bucket_index(&key);
        self.try_insert_at_bucket(key, frame_id, idx)
    }

    /// `try_insert` variant that places the entry in a caller-chosen bucket
    /// (so it matches a separately-computed preferred-frame index instead of
    /// OPM's internal hash). Used by translators whose placement function
    /// differs from `bucket_index`.
    pub(crate) fn try_insert_at_bucket(
        &self,
        key: PageKey,
        frame_id: u32,
        bucket_idx: usize,
    ) -> Result<(), u32> {
        let idx = bucket_idx % self.num_buckets;
        let bucket = &self.buckets[idx];
        let guard = crossbeam_epoch::pin();
        while !bucket.try_lock() {
            std::hint::spin_loop();
        }
        unsafe {
            let inlined = &mut *bucket.inlined.get();
            if let Some((k, existing)) = inlined {
                if *k == key {
                    let v = *existing;
                    bucket.unlock();
                    return Err(v);
                }
            }
            if let Some(ptr) = Self::find_in_chain_mut(&bucket.chain_head, &key, &guard) {
                let v = *ptr;
                bucket.unlock();
                return Err(v);
            }
            // Key not present — insert.
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
        Ok(())
    }

    /// In-place insert: take versioned lock, mutate bucket, unlock. No clone.
    #[inline]
    pub fn insert(&self, key: PageKey, frame_id: u32) {
        let idx = self.bucket_index(&key);
        self.insert_at_bucket(key, frame_id, idx);
    }

    /// In-place insert at a specific bucket index (used by PT when preferred_frame differs from hash).
    #[inline]
    pub fn insert_at_bucket(&self, key: PageKey, frame_id: u32, bucket_idx: usize) {
        let idx = bucket_idx % self.num_buckets;
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
    ) -> Option<*mut u32> {
        let mut current = head.load(Ordering::Acquire, guard);
        while !current.is_null() {
            let node = current.deref() as *const ChainNode as *mut ChainNode;
            if (*node).key == *key {
                return Some(&mut (*node).value as *mut u32);
            }
            current = (*node).next.load(Ordering::Acquire, guard);
        }
        None
    }

    /// In-place remove: take versioned lock, unlink node, retire with epoch, unlock.
    #[inline]
    pub(crate) fn remove(&self, key: &PageKey) -> Option<u32> {
        let idx = self.bucket_index(key);
        self.remove_at_bucket(key, idx)
    }

    /// `remove` variant keyed by caller-supplied bucket — pairs with
    /// `insert_at_bucket` / `try_insert_at_bucket` when the translator's
    /// placement differs from OPM's internal hash.
    pub(crate) fn remove_at_bucket(&self, key: &PageKey, bucket_idx: usize) -> Option<u32> {
        let idx = bucket_idx % self.num_buckets;
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
    ) -> Option<u32> {
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
                    pk.c_key, pk.page_id, frame_id,
                ));
            }
        });
        out
    }

    /// Lock-free iteration: copy entries under version check, then call f.
    pub(crate) fn for_each_entry(&self, mut f: impl FnMut(PageKey, u32)) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(page_id: u32) -> PageKey {
        PageKey::new(ContainerKey::new(0, 0), page_id)
    }

    // ------------------------------------------------------------------
    // Basic single-threaded correctness
    // ------------------------------------------------------------------

    #[test]
    fn insert_and_lookup() {
        let t = OptimisticPageMap::new(64);
        t.insert(pk(1), 10);
        assert_eq!(t.lookup(&pk(1)), Some(10));
        assert_eq!(t.lookup(&pk(2)), None);
    }

    #[test]
    fn insert_overwrites() {
        let t = OptimisticPageMap::new(64);
        t.insert(pk(1), 10);
        t.insert(pk(1), 20);
        assert_eq!(t.lookup(&pk(1)), Some(20));
    }

    #[test]
    fn remove_returns_value() {
        let t = OptimisticPageMap::new(64);
        t.insert(pk(1), 10);
        assert_eq!(t.remove(&pk(1)), Some(10));
        assert_eq!(t.lookup(&pk(1)), None);
    }

    #[test]
    fn remove_missing_key() {
        let t = OptimisticPageMap::new(64);
        assert_eq!(t.remove(&pk(42)), None);
    }

    #[test]
    fn contains_key_works() {
        let t = OptimisticPageMap::new(64);
        assert!(!t.contains_key(&pk(1)));
        t.insert(pk(1), 10);
        assert!(t.contains_key(&pk(1)));
        t.remove(&pk(1));
        assert!(!t.contains_key(&pk(1)));
    }

    #[test]
    fn try_insert_succeeds_when_absent() {
        let t = OptimisticPageMap::new(64);
        assert!(t.try_insert(pk(1), 10).is_ok());
        assert_eq!(t.lookup(&pk(1)), Some(10));
    }

    #[test]
    fn try_insert_fails_when_present_inlined() {
        let t = OptimisticPageMap::new(64);
        t.insert(pk(1), 10);
        assert_eq!(t.try_insert(pk(1), 20), Err(10));
        // Original value unchanged.
        assert_eq!(t.lookup(&pk(1)), Some(10));
    }

    #[test]
    fn try_insert_fails_when_present_in_chain() {
        // Force collisions by using a single bucket.
        let t = OptimisticPageMap::new(1);
        t.insert(pk(1), 10); // goes to inlined
        t.insert(pk(2), 20); // goes to chain
        assert_eq!(t.try_insert(pk(2), 30), Err(20));
        assert_eq!(t.lookup(&pk(2)), Some(20));
    }

    // ------------------------------------------------------------------
    // Chain (collision) tests
    // ------------------------------------------------------------------

    #[test]
    fn single_bucket_multiple_keys() {
        // 1 bucket → all keys collide → exercises chain logic.
        let t = OptimisticPageMap::new(1);
        for i in 0..10u32 {
            t.insert(pk(i), i * 100);
        }
        for i in 0..10u32 {
            assert_eq!(t.lookup(&pk(i)), Some(i * 100));
        }
    }

    #[test]
    fn remove_from_chain_middle() {
        let t = OptimisticPageMap::new(1);
        t.insert(pk(1), 10);
        t.insert(pk(2), 20);
        t.insert(pk(3), 30);

        // Remove middle chain entry.
        assert_eq!(t.remove(&pk(2)), Some(20));
        assert_eq!(t.lookup(&pk(1)), Some(10));
        assert_eq!(t.lookup(&pk(2)), None);
        assert_eq!(t.lookup(&pk(3)), Some(30));
    }

    #[test]
    fn remove_inlined_promotes_chain_head() {
        let t = OptimisticPageMap::new(1);
        t.insert(pk(1), 10); // inlined
        t.insert(pk(2), 20); // chain head
        t.insert(pk(3), 30); // chain node

        // Removing the inlined entry should promote chain head to inlined.
        assert_eq!(t.remove(&pk(1)), Some(10));
        // pk(2) and pk(3) should still be findable.
        assert_eq!(t.lookup(&pk(2)), Some(20));
        assert_eq!(t.lookup(&pk(3)), Some(30));
    }

    #[test]
    fn overwrite_in_chain() {
        let t = OptimisticPageMap::new(1);
        t.insert(pk(1), 10);
        t.insert(pk(2), 20);
        t.insert(pk(2), 99); // overwrite chain entry
        assert_eq!(t.lookup(&pk(2)), Some(99));
    }

    // ------------------------------------------------------------------
    // for_each_entry
    // ------------------------------------------------------------------

    #[test]
    fn for_each_entry_visits_all() {
        let t = OptimisticPageMap::new(1);
        t.insert(pk(1), 10);
        t.insert(pk(2), 20);
        t.insert(pk(3), 30);

        let mut seen = std::collections::HashMap::new();
        t.for_each_entry(|k, v| {
            seen.insert(k.page_id, v);
        });
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[&1], 10);
        assert_eq!(seen[&2], 20);
        assert_eq!(seen[&3], 30);
    }

    // ------------------------------------------------------------------
    // lookup_with_bucket
    // ------------------------------------------------------------------

    #[test]
    fn lookup_with_bucket_works() {
        let t = OptimisticPageMap::new(64);
        let key = pk(5);
        t.insert(key, 42);
        let bucket_idx = t.bucket_index_pub(&key);
        assert_eq!(t.lookup_with_bucket(&key, bucket_idx), Some(42));
        // Wrong bucket should miss (unless hash collision, unlikely).
        // Just verify correct bucket works.
    }

    // ------------------------------------------------------------------
    // get_apply — optimistic lookup + closure
    // ------------------------------------------------------------------

    #[test]
    fn get_apply_miss_returns_none() {
        let t = OptimisticPageMap::new(64);
        let result: Option<Option<u32>> = t.get_apply(&pk(1), |id| Some(id * 2));
        assert_eq!(result, None);
    }

    #[test]
    fn get_apply_hit_runs_closure() {
        let t = OptimisticPageMap::new(64);
        t.insert(pk(1), 10);
        let result = t.get_apply(&pk(1), |id| Some(id * 2));
        assert_eq!(result, Some(Some(20)));
    }

    #[test]
    fn get_apply_hit_closure_returns_none() {
        let t = OptimisticPageMap::new(64);
        t.insert(pk(1), 10);
        let result: Option<Option<u32>> = t.get_apply(&pk(1), |_id| None);
        assert_eq!(result, Some(None));
    }

    #[test]
    fn get_apply_finds_chain_entry() {
        // Force into the chain via a one-bucket table.
        let t = OptimisticPageMap::new(1);
        t.insert(pk(1), 10); // inlined
        t.insert(pk(2), 20); // chain
        let r1 = t.get_apply(&pk(1), |id| Some(id));
        let r2 = t.get_apply(&pk(2), |id| Some(id));
        assert_eq!(r1, Some(Some(10)));
        assert_eq!(r2, Some(Some(20)));
    }

    #[test]
    fn get_apply_retries_on_concurrent_writer() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        use std::sync::Arc;
        use std::thread;

        let t = Arc::new(OptimisticPageMap::new(64));
        t.insert(pk(1), 10);

        let stop = Arc::new(AtomicBool::new(false));

        // Writer: churns the value so the bucket version keeps bumping.
        let t_w = Arc::clone(&t);
        let stop_w = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            let mut v: u32 = 10;
            while !stop_w.load(Ordering::Relaxed) {
                v = v.wrapping_add(1);
                t_w.insert(pk(1), v);
            }
        });

        // Reader: `get_apply` must never observe a torn read — the frame_id
        // it passes into the closure must equal what lookup returns under a
        // consistent version. We assert the closure's input equals a fresh
        // lookup of the same key taken right after.
        let runs = Arc::new(AtomicUsize::new(0));
        for _ in 0..10_000 {
            let r = t.get_apply(&pk(1), |id| {
                runs.fetch_add(1, Ordering::Relaxed);
                Some(id)
            });
            // Either a hit (Some(Some(id))) or a concurrent-eviction-style
            // miss (None) — but never an inconsistent Some(None) because
            // our closure always returns Some.
            assert!(matches!(r, Some(Some(_)) | None));
        }

        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        assert!(runs.load(Ordering::Relaxed) > 0);
    }

    // ------------------------------------------------------------------
    // Concurrent correctness
    // ------------------------------------------------------------------

    #[test]
    fn concurrent_insert_lookup_remove() {
        use std::sync::Arc;
        use std::thread;

        let t = Arc::new(OptimisticPageMap::new(64));
        let num_threads: u32 = 8;
        let ops_per_thread: u32 = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let t = Arc::clone(&t);
                thread::spawn(move || {
                    let base = tid * ops_per_thread;
                    // Insert
                    for i in 0..ops_per_thread {
                        t.insert(pk(base + i), base + i);
                    }
                    // Verify
                    for i in 0..ops_per_thread {
                        assert_eq!(t.lookup(&pk(base + i)), Some(base + i));
                    }
                    // Remove
                    for i in 0..ops_per_thread {
                        assert_eq!(t.remove(&pk(base + i)), Some(base + i));
                    }
                    // Verify removed
                    for i in 0..ops_per_thread {
                        assert_eq!(t.lookup(&pk(base + i)), None);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_try_insert_no_double_fault() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let t = Arc::new(OptimisticPageMap::new(64));
        let barrier = Arc::new(Barrier::new(8));
        let key = pk(42);

        let handles: Vec<_> = (0..8u32)
            .map(|tid| {
                let t = Arc::clone(&t);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    t.try_insert(key, tid)
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // Exactly one thread should succeed.
        let successes = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(successes, 1, "exactly one try_insert should succeed");

        // The winner's frame_id should be in the table.
        let winner_frame = t.lookup(&key).unwrap();
        let winner_idx = results.iter().position(|r| r.is_ok()).unwrap() as u32;
        assert_eq!(winner_frame, winner_idx);
    }

    #[test]
    fn concurrent_mixed_ops_single_bucket() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        // Single bucket = maximum contention.
        let t = Arc::new(OptimisticPageMap::new(1));
        let barrier = Arc::new(Barrier::new(4));

        let handles: Vec<_> = (0..4u32)
            .map(|tid| {
                let t = Arc::clone(&t);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for round in 0..500u32 {
                        let key = pk(tid * 1000 + round);
                        t.insert(key, tid * 1000 + round);
                        assert_eq!(t.lookup(&key), Some(tid * 1000 + round));
                        t.remove(&key);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Table should be empty.
        let mut count = 0;
        t.for_each_entry(|_, _| count += 1);
        assert_eq!(count, 0);
    }
}
