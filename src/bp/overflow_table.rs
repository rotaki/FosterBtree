//! Overflow table for predictive translation (Section 4.1).
//!
//! Chaining hash table with inlined first slot per bucket. Maps `PageKey` → frame index
//! for pages that are *not* in their preferred frame. Purely a hash map; bucket/chains
//! are for hash collisions only, unrelated to preferred-frame placement.

use super::mem_pool_trait::{ContainerKey, PageFrameKey, PageKey};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

/// One bucket: inlined first slot + chain for collisions.
struct Bucket {
    inlined: Option<(PageKey, usize)>,
    chain: Vec<(PageKey, usize)>,
}

impl Bucket {
    fn new() -> Self {
        Self {
            inlined: None,
            chain: Vec::new(),
        }
    }
}

/// Overflow table: fixed number of buckets, chaining with inlined first slot.
pub(crate) struct OverflowTable {
    num_buckets: usize,
    buckets: Vec<Mutex<Bucket>>,
}

impl OverflowTable {
    pub(crate) fn new(num_buckets: usize) -> Self {
        let buckets = (0..num_buckets)
            .map(|_| Mutex::new(Bucket::new()))
            .collect();
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

    #[inline]
    pub(crate) fn lookup(&self, key: &PageKey) -> Option<usize> {
        let idx = self.bucket_index(key);
        let guard = self.buckets[idx].lock().unwrap();
        if let Some((k, v)) = &guard.inlined {
            if k == key {
                return Some(*v);
            }
        }
        guard.chain.iter().find(|(k, _)| k == key).map(|(_, v)| *v)
    }

    #[inline]
    pub(crate) fn insert(&self, key: PageKey, frame_id: usize) {
        let idx = self.bucket_index(&key);
        let mut guard = self.buckets[idx].lock().unwrap();
        if let Some((k, ref mut v)) = &mut guard.inlined {
            if *k == key {
                *v = frame_id;
                return;
            }
        }
        if let Some(entry) = guard.chain.iter_mut().find(|(k, _)| *k == key) {
            entry.1 = frame_id;
            return;
        }
        if guard.inlined.is_none() {
            guard.inlined = Some((key, frame_id));
        } else {
            guard.chain.push((key, frame_id));
        }
    }

    #[inline]
    pub(crate) fn remove(&self, key: &PageKey) -> Option<usize> {
        let idx = self.bucket_index(key);
        let mut guard = self.buckets[idx].lock().unwrap();
        if let Some((k, v)) = &guard.inlined {
            if k == key {
                let out = *v;
                if let Some((chain_key, chain_val)) = guard.chain.pop() {
                    guard.inlined = Some((chain_key, chain_val));
                } else {
                    guard.inlined = None;
                }
                return Some(out);
            }
        }
        if let Some(pos) = guard.chain.iter().position(|(k, _)| k == key) {
            let (_, v) = guard.chain.remove(pos);
            return Some(v);
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

    /// Iterate over all (PageKey, frame_id) entries. For flush/invariants.
    pub(crate) fn for_each_entry(&self, mut f: impl FnMut(PageKey, usize)) {
        for bucket in &self.buckets {
            let guard = bucket.lock().unwrap();
            if let Some((pk, fid)) = &guard.inlined {
                f(*pk, *fid);
            }
            for (pk, fid) in &guard.chain {
                f(*pk, *fid);
            }
        }
    }
}
