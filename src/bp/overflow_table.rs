//! Overflow table for predictive translation (Section 4.1 + 4.2).
//!
//! Chaining hash table with inlined first slot. Phase 2: optimistic latch with
//! **lock-free reads** — readers load a snapshot (Arc) via ArcSwap and validate
//! version; no mutex on the read path. Writers clone-modify-swap under a mutex.

use super::mem_pool_trait::{ContainerKey, PageFrameKey, PageKey};
use arc_swap::ArcSwap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Bucket payload: inlined first slot + chain. Cloneable for copy-on-write.
#[derive(Clone)]
struct BucketData {
    inlined: Option<(PageKey, usize)>,
    chain: Vec<(PageKey, usize)>,
}

impl BucketData {
    fn new() -> Self {
        Self {
            inlined: None,
            chain: Vec::new(),
        }
    }
}

/// Per-bucket: version (even = no writer), ArcSwap snapshot, mutex for writers only.
struct BucketHandle {
    version: AtomicU32,
    data: ArcSwap<BucketData>,
    write_mutex: Mutex<()>,
}

impl BucketHandle {
    fn new() -> Self {
        Self {
            version: AtomicU32::new(0),
            data: ArcSwap::from_pointee(BucketData::new()),
            write_mutex: Mutex::new(()),
        }
    }
}

/// Overflow table: fixed number of buckets, chaining with inlined first slot.
pub(crate) struct OverflowTable {
    num_buckets: usize,
    buckets: Vec<BucketHandle>,
}

const MAX_READ_RETRIES: u32 = 32;

impl OverflowTable {
    pub(crate) fn new(num_buckets: usize) -> Self {
        let buckets = (0..num_buckets).map(|_| BucketHandle::new()).collect();
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

    /// Lock-free lookup using a precomputed bucket index (avoids re-hashing when
    /// caller already has the placement index, e.g. same as preferred_frame when num_buckets == num_frames).
    #[inline]
    pub(crate) fn lookup_with_bucket(&self, key: &PageKey, bucket_idx: usize) -> Option<usize> {
        let idx = bucket_idx % self.num_buckets;
        let bucket = &self.buckets[idx];
        for _ in 0..MAX_READ_RETRIES {
            let v1 = bucket.version.load(Ordering::Acquire);
            if v1 % 2 != 0 {
                continue;
            }
            let result = {
                let snapshot = bucket.data.load();
                if let Some((k, v)) = &snapshot.inlined {
                    if k == key {
                        Some(*v)
                    } else {
                        snapshot.chain.iter().find(|(k2, _)| k2 == key).map(|(_, v)| *v)
                    }
                } else {
                    snapshot.chain.iter().find(|(k2, _)| k2 == key).map(|(_, v)| *v)
                }
            };
            let v2 = bucket.version.load(Ordering::Acquire);
            if v1 == v2 {
                return result;
            }
        }
        self.lookup_slow(key, idx)
    }

    /// Lock-free read path: load snapshot (no mutex), read, validate version.
    #[inline]
    pub(crate) fn lookup(&self, key: &PageKey) -> Option<usize> {
        let idx = self.bucket_index(key);
        let bucket = &self.buckets[idx];
        for _ in 0..MAX_READ_RETRIES {
            let v1 = bucket.version.load(Ordering::Acquire);
            if v1 % 2 != 0 {
                continue;
            }
            let result = {
                let snapshot = bucket.data.load();
                if let Some((k, v)) = &snapshot.inlined {
                    if k == key {
                        Some(*v)
                    } else {
                        snapshot.chain.iter().find(|(k2, _)| k2 == key).map(|(_, v)| *v)
                    }
                } else {
                    snapshot.chain.iter().find(|(k2, _)| k2 == key).map(|(_, v)| *v)
                }
            };
            let v2 = bucket.version.load(Ordering::Acquire);
            if v1 == v2 {
                return result;
            }
        }
        self.lookup_slow(key, idx)
    }

    #[cold]
    fn lookup_slow(&self, key: &PageKey, idx: usize) -> Option<usize> {
        let bucket = &self.buckets[idx];
        let snapshot = bucket.data.load();
        if let Some((k, v)) = &snapshot.inlined {
            if k == key {
                return Some(*v);
            }
        }
        snapshot.chain.iter().find(|(k, _)| k == key).map(|(_, v)| *v)
    }

    /// Write path: mutex, clone-modify-swap, bump version.
    #[inline]
    pub(crate) fn insert(&self, key: PageKey, frame_id: usize) {
        let idx = self.bucket_index(&key);
        let bucket = &self.buckets[idx];
        let _guard = bucket.write_mutex.lock().unwrap();
        let ver = bucket.version.load(Ordering::Acquire);
        bucket.version.store(ver.wrapping_add(1), Ordering::Release);
        let current = bucket.data.load();
        let mut new_data = (**current).clone();
        if let Some((k, ref mut slot_val)) = &mut new_data.inlined {
            if *k == key {
                *slot_val = frame_id;
                bucket.data.store(Arc::new(new_data));
                bucket.version.store(ver.wrapping_add(2), Ordering::Release);
                return;
            }
        }
        if let Some(entry) = new_data.chain.iter_mut().find(|(k, _)| *k == key) {
            entry.1 = frame_id;
            bucket.data.store(Arc::new(new_data));
            bucket.version.store(ver.wrapping_add(2), Ordering::Release);
            return;
        }
        if new_data.inlined.is_none() {
            new_data.inlined = Some((key, frame_id));
        } else {
            new_data.chain.push((key, frame_id));
        }
        bucket.data.store(Arc::new(new_data));
        bucket.version.store(ver.wrapping_add(2), Ordering::Release);
    }

    #[inline]
    pub(crate) fn remove(&self, key: &PageKey) -> Option<usize> {
        let idx = self.bucket_index(key);
        let bucket = &self.buckets[idx];
        let _guard = bucket.write_mutex.lock().unwrap();
        let ver = bucket.version.load(Ordering::Acquire);
        bucket.version.store(ver.wrapping_add(1), Ordering::Release);
        let current = bucket.data.load();
        let mut new_data = (**current).clone();
        let result = if let Some((k, val)) = &new_data.inlined {
            if k == key {
                let out = *val;
                if let Some((chain_key, chain_val)) = new_data.chain.pop() {
                    new_data.inlined = Some((chain_key, chain_val));
                } else {
                    new_data.inlined = None;
                }
                Some(out)
            } else {
                new_data.chain.iter().position(|(k2, _)| k2 == key).map(|pos| {
                    let (_, val) = new_data.chain.remove(pos);
                    val
                })
            }
        } else {
            new_data.chain.iter().position(|(k2, _)| k2 == key).map(|pos| {
                let (_, val) = new_data.chain.remove(pos);
                val
            })
        };
        if result.is_some() {
            bucket.data.store(Arc::new(new_data));
        }
        bucket.version.store(ver.wrapping_add(2), Ordering::Release);
        result
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

    /// Lock-free iteration: load snapshot, copy entries, validate version.
    pub(crate) fn for_each_entry(&self, mut f: impl FnMut(PageKey, usize)) {
        for bucket in &self.buckets {
            let mut done = false;
            for _ in 0..MAX_READ_RETRIES {
                let v1 = bucket.version.load(Ordering::Acquire);
                if v1 % 2 != 0 {
                    continue;
                }
                let mut entries = Vec::new();
                {
                    let snapshot = bucket.data.load();
                    if let Some((pk, fid)) = &snapshot.inlined {
                        entries.push((*pk, *fid));
                    }
                    for (pk, fid) in &snapshot.chain {
                        entries.push((*pk, *fid));
                    }
                }
                let v2 = bucket.version.load(Ordering::Acquire);
                if v1 == v2 {
                    for (pk, fid) in entries {
                        f(pk, fid);
                    }
                    done = true;
                    break;
                }
            }
            if !done {
                let snapshot = bucket.data.load();
                if let Some((pk, fid)) = &snapshot.inlined {
                    f(*pk, *fid);
                }
                for (pk, fid) in &snapshot.chain {
                    f(*pk, *fid);
                }
            }
        }
    }
}
