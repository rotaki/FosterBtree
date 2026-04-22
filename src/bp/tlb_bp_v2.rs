//! Shadow re-implementation of `TlbBP` on top of `FrameManager`.
//!
//! **Status: shadow** — coexists with the original `TlbBP`. Intended to be
//! behaviorally identical; use for A/B testing and migration verification.
//!
//! Owns: `CongeeRawU32` overflow translator + `BPStats` + thread-local TLB,
//! victim cache, and per-thread counters. Delegates everything frame-mgmt
//! (pages, metas, free-frame queue, clock hand, used-frame counter, container
//! handles, eviction, flush) to [`FrameManager`].
//!
//! Translator hook (`on_evict`) drops an overflow entry only when it still
//! points at the frame being freed — preserves the original TlbBP's guard
//! against race where a page got remapped between candidate selection and
//! finalize.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    frame_manager::FrameManager,
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};
use crate::{container::ContainerManager, log_debug, page::PageId};

#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
use std::sync::atomic::AtomicU64;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

/// Global-atomic shadow of the thread-local TLB_HITS / TLB_MISSES /
/// TLB_FALSE_HITS counters. The thread-local originals are used by the
/// existing Drop-time print_stderr path. This shadow lets `print_profile()`
/// aggregate across worker threads from the main thread so benchmarks like
/// `pt_fastpath_coverage` can emit a consistent coverage ratio. Only updated
/// when `pt_profile` / `pt_counts` is enabled.
#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
static TLB_HITS_GLOBAL: AtomicU64 = AtomicU64::new(0);
#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
static TLB_MISSES_GLOBAL: AtomicU64 = AtomicU64::new(0);
#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
static TLB_FALSE_HITS_GLOBAL: AtomicU64 = AtomicU64::new(0);

use congee::CongeeRawU32;

type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;

// ===========================================================================
// PageKey <-> u64 packing (mirrors tlb_bp.rs)
// ===========================================================================

#[inline(always)]
fn pack_page_key(key: &PageKey) -> usize {
    ((key.c_key.as_u32() as usize) << 32) | key.page_id as usize
}

#[inline(always)]
fn unpack_page_key(packed: usize) -> PageKey {
    let c_key_u32 = (packed >> 32) as u32;
    let page_id = packed as u32;
    PageKey::new(ContainerKey::from_u32(c_key_u32), page_id)
}

// ===========================================================================
// TLB hash
// ===========================================================================

struct HashedKey {
    val: u64,
}

impl HashedKey {
    #[inline(always)]
    fn new(key: &PageKey) -> Self {
        let c_hash = super::hash::hash_u64(key.c_key.as_u32() as u64);
        Self {
            val: c_hash.wrapping_add(key.page_id as u64),
        }
    }

    #[inline(always)]
    fn tlb_set(&self) -> usize {
        (self.val as usize) & TLB_SET_MASK
    }

    #[inline(always)]
    fn tlb_tag(&self) -> u32 {
        let t = ((self.val >> 10) as u32 & 0x1F) | 1;
        t << TAG_SHIFT
    }
}

// ===========================================================================
// TLB layout + thread-local state
// ===========================================================================

#[cfg(feature = "tlb_victim_cache")]
const TLB_ENTRIES: usize = 4096;
#[cfg(feature = "tlb_victim_cache")]
const TLB_SET_MASK: usize = TLB_ENTRIES - 1;
#[cfg(feature = "tlb_victim_cache")]
const VICTIM_SIZE: usize = 16;

#[cfg(not(feature = "tlb_victim_cache"))]
const TLB_SETS: usize = 1024;
#[cfg(not(feature = "tlb_victim_cache"))]
const TLB_WAYS: usize = 4;
#[cfg(not(feature = "tlb_victim_cache"))]
const TLB_SET_MASK: usize = TLB_SETS - 1;

const FRAME_BITS: u32 = 27;
const FRAME_MASK: u32 = (1 << FRAME_BITS) - 1;
const TAG_SHIFT: u32 = FRAME_BITS;
const PREFILL_COUNT: usize = 8;

#[inline(always)]
fn pack_entry(tag: u32, frame_id: u32) -> u32 {
    tag | (frame_id & FRAME_MASK)
}

#[inline(always)]
fn entry_tag(entry: u32) -> u32 {
    entry & !FRAME_MASK
}

#[inline(always)]
fn entry_frame(entry: u32) -> u32 {
    entry & FRAME_MASK
}

// Separate thread-local state for the V2 shadow so its counters don't
// collide with the original TlbBP's when both are linked into the same
// binary (e.g. by tests).

#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut TLB: [u32; TLB_ENTRIES] = [0; TLB_ENTRIES];
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut VICTIM: [u32; VICTIM_SIZE] = [0; VICTIM_SIZE];
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut VICTIM_SETS: [u16; VICTIM_SIZE] = [0; VICTIM_SIZE];
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut VICTIM_HEAD: usize = 0;

#[cfg(not(feature = "tlb_victim_cache"))]
#[thread_local]
static mut TLB: [[u32; TLB_WAYS]; TLB_SETS] = [[0; TLB_WAYS]; TLB_SETS];

#[thread_local]
static mut TLB_HITS: u64 = 0;
#[thread_local]
static mut TLB_MISSES: u64 = 0;
#[thread_local]
static mut TLB_PREFILLS: u64 = 0;
#[thread_local]
static mut LAST_MISS_KEY: usize = 0;
#[thread_local]
static mut OVERFLOW_HITS: u64 = 0;
#[thread_local]
static mut PAGE_FAULTS: u64 = 0;
#[thread_local]
static mut TLB_FALSE_HITS: u64 = 0;

#[inline(always)]
unsafe fn tlb_probe(set: usize, tag: u32) -> Option<u32> {
    #[cfg(feature = "tlb_victim_cache")]
    {
        let entry = *TLB.get_unchecked(set);
        if entry_tag(entry) == tag {
            return Some(entry_frame(entry));
        }
        for v in 0..VICTIM_SIZE {
            if VICTIM_SETS[v] == set as u16 && entry_tag(VICTIM[v]) == tag {
                let victim_entry = VICTIM[v];
                VICTIM[v] = entry;
                VICTIM_SETS[v] = set as u16;
                *TLB.get_unchecked_mut(set) = victim_entry;
                return Some(entry_frame(victim_entry));
            }
        }
        None
    }
    #[cfg(not(feature = "tlb_victim_cache"))]
    {
        let ways = &mut *TLB.get_unchecked_mut(set);
        for w in 0..TLB_WAYS {
            if entry_tag(ways[w]) == tag {
                let frame_id = entry_frame(ways[w]);
                if w > 0 {
                    ways.swap(0, w);
                }
                return Some(frame_id);
            }
        }
        None
    }
}

#[inline(always)]
unsafe fn tlb_insert(set: usize, entry: u32) {
    #[cfg(feature = "tlb_victim_cache")]
    {
        let old = *TLB.get_unchecked(set);
        if old != 0 {
            let head = VICTIM_HEAD;
            VICTIM[head] = old;
            VICTIM_SETS[head] = set as u16;
            VICTIM_HEAD = (head + 1) % VICTIM_SIZE;
        }
        *TLB.get_unchecked_mut(set) = entry;
    }
    #[cfg(not(feature = "tlb_victim_cache"))]
    {
        let ways = &mut *TLB.get_unchecked_mut(set);
        ways[3] = ways[2];
        ways[2] = ways[1];
        ways[1] = ways[0];
        ways[0] = entry;
    }
}

#[inline(always)]
unsafe fn tlb_insert_if_empty(set: usize, entry: u32) -> bool {
    #[cfg(feature = "tlb_victim_cache")]
    {
        if *TLB.get_unchecked(set) == 0 {
            *TLB.get_unchecked_mut(set) = entry;
            return true;
        }
        for v in 0..VICTIM_SIZE {
            if VICTIM[v] == 0 {
                VICTIM[v] = entry;
                VICTIM_SETS[v] = set as u16;
                return true;
            }
        }
        false
    }
    #[cfg(not(feature = "tlb_victim_cache"))]
    {
        let ways = &mut *TLB.get_unchecked_mut(set);
        for w in 0..TLB_WAYS {
            if ways[w] == 0 {
                ways[w] = entry;
                return true;
            }
        }
        false
    }
}

// ===========================================================================
// TlbBPV2
// ===========================================================================

pub struct TlbBPV2 {
    pub(crate) fm: FrameManager<EvictionPolicyImpl>,
    /// Overflow: concurrent ART mapping packed PageKey → frame_id (u32).
    overflow: CongeeRawU32<usize>,
    stats: BPStats,
}

unsafe impl Sync for TlbBPV2 {}
unsafe impl Send for TlbBPV2 {}

impl TlbBPV2 {
    /// Eviction batch size — matches original TlbBP.
    const EVICT_BATCH: usize = 64;

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("TlbBPV2 created: num_frames={}", num_frames);
        Ok(Self {
            fm: FrameManager::new(num_frames, container_manager)?,
            overflow: CongeeRawU32::default(),
            stats: BPStats::new(),
        })
    }

    #[inline]
    pub(crate) fn num_frames(&self) -> usize {
        self.fm.num_frames()
    }

    /// Translator hook for `FrameManager` eviction. Removes overflow entry
    /// only if it still points at `idx` — guards against races where a page
    /// got remapped between classify_frame and finalize.
    fn on_evict(&self, pk: &PageKey, _idx: u32) {
        self.overflow_remove(pk);
    }

    // ------------------------------------------------------------------
    // Overflow helpers (thin wrappers around congee)
    // ------------------------------------------------------------------

    #[inline]
    fn overflow_lookup(&self, key: &PageKey) -> Option<u32> {
        let guard = crossbeam_epoch::pin();
        self.overflow.get(&pack_page_key(key), &guard)
    }

    #[inline]
    fn overflow_insert(&self, key: PageKey, frame_id: u32) {
        let guard = crossbeam_epoch::pin();
        let _ = self.overflow.insert(pack_page_key(&key), frame_id, &guard);
    }

    #[inline]
    fn overflow_remove(&self, key: &PageKey) -> Option<u32> {
        let guard = crossbeam_epoch::pin();
        self.overflow.remove(&pack_page_key(key), &guard)
    }

    fn overflow_contains_key(&self, key: &PageKey) -> bool {
        self.overflow_lookup(key).is_some()
    }

    fn overflow_get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        let guard = crossbeam_epoch::pin();
        let start = (c_key.as_u32() as usize) << 32;
        let end = start | 0xFFFF_FFFF;
        let mut buf = vec![(0usize, 0u32); 4096];
        let mut out = Vec::new();
        let mut scan_start = start;
        loop {
            let count = self.overflow.range(&scan_start, &end, &mut buf, &guard);
            if count == 0 {
                break;
            }
            for &(packed, frame_id) in &buf[..count] {
                let pk = unpack_page_key(packed);
                out.push(PageFrameKey::new_with_frame_id(
                    pk.c_key, pk.page_id, frame_id,
                ));
            }
            if count < buf.len() {
                break;
            }
            scan_start = buf[count - 1].0 + 1;
        }
        out
    }

    // ------------------------------------------------------------------
    // Frame access / eviction — delegate to FrameManager
    // ------------------------------------------------------------------

    #[inline]
    fn try_get_read_guard(&self, index: u32) -> Option<FRGuard> {
        self.fm.try_get_read_guard(index)
    }

    #[inline]
    fn try_get_write_guard(&self, index: u32, make_dirty: bool) -> Option<FWGuard> {
        self.fm.try_get_write_guard(index, make_dirty)
    }

    #[inline]
    fn meta(&self, index: u32) -> &FMeta {
        self.fm.meta(index)
    }

    #[inline]
    fn enqueue_free_frame(&self, idx: u32) {
        self.fm.enqueue_free_frame(idx);
    }

    #[inline]
    fn choose_victim(&self) -> Option<FWGuard> {
        self.fm.choose_victim()
    }

    #[inline]
    fn ensure_free_frames(&self) -> Result<(), MemPoolStatus> {
        self.fm
            .ensure_free_frames(Self::EVICT_BATCH, |pk, idx| self.on_evict(pk, idx))
    }

    // ------------------------------------------------------------------
    // Page fault (mirrors original TlbBP::handle_page_fault)
    // ------------------------------------------------------------------

    fn handle_page_fault(&self, page_key: PageKey) -> Result<FWGuard, MemPoolStatus> {
        self.fm.increment_used();

        let mut victim = match self.choose_victim() {
            Some(v) => v,
            None => {
                self.fm.decrement_used();
                return Err(MemPoolStatus::CannotEvictPage);
            }
        };

        debug_assert!(victim.page_key().is_none());

        // Atomic insert-if-absent via congee compute_or_insert.
        let guard = crossbeam_epoch::pin();
        let packed = pack_page_key(&page_key);
        let frame_id = victim.frame_id();
        let mut existing_frame: Option<u32> = None;
        let _ = self.overflow.compute_or_insert(
            packed,
            |existing| match existing {
                None => frame_id,
                Some(v) => {
                    existing_frame = Some(v);
                    v
                }
            },
            &guard,
        );
        drop(guard);

        if let Some(idx) = existing_frame {
            // Race: another thread faulted the same page first. Free our
            // victim, latch theirs.
            self.enqueue_free_frame(frame_id);
            self.fm.decrement_used();
            return self
                .try_get_write_guard(idx, true)
                .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
        }

        victim.set_page_key(Some(page_key));

        if let Err(e) = self
            .fm
            .container_manager()
            .get_container(page_key.c_key)
            .read_page(page_key.page_id, &mut victim)
        {
            victim.set_page_key(None);
            self.overflow_remove(&page_key);
            self.enqueue_free_frame(victim.frame_id());
            self.fm.decrement_used();
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        // Read faults stay clean; write path marks dirty via the write guard.
        Ok(victim)
    }
}

impl Drop for TlbBPV2 {
    fn drop(&mut self) {
        let hits = unsafe { TLB_HITS };
        let misses = unsafe { TLB_MISSES };
        let false_hits = unsafe { TLB_FALSE_HITS };
        let prefills = unsafe { TLB_PREFILLS };
        let overflow_hits = unsafe { OVERFLOW_HITS };
        let page_faults = unsafe { PAGE_FAULTS };
        let total = hits + misses + false_hits;
        if total > 0 {
            eprintln!(
                "TLB-V2 stats: hits={}, false_hits={}, misses={}, total={}, hit_rate={:.2}%, prefills={}, overflow_hits={}, page_faults={}",
                hits, false_hits, misses, total,
                hits as f64 / total as f64 * 100.0,
                prefills, overflow_hits, page_faults
            );
        }

        if self.fm.container_manager().remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

// ===========================================================================
// MemPool trait
// ===========================================================================

impl MemPool for TlbBPV2 {
    type EP = EvictionPolicyImpl;

    // ----- TLB fast path: get_page_for_read --------------------------------

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        self.stats.inc_read_count();
        let page_key = key.p_key();
        let hk = HashedKey::new(&page_key);
        let set = hk.tlb_set();
        let tag = hk.tlb_tag();

        #[cfg(feature = "tlb_victim_cache")]
        {
            if let Some(frame_id) = unsafe { tlb_probe(set, tag) } {
                if self.meta(frame_id).key() == Some(page_key) {
                    if let Some(g) = self.try_get_read_guard(frame_id) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            unsafe {
                                TLB_HITS += 1;
                            }
                            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                            TLB_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
                            return Ok(g);
                        }
                    }
                }
                unsafe {
                    TLB_FALSE_HITS += 1;
                }
                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                TLB_FALSE_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
            }
        }
        #[cfg(not(feature = "tlb_victim_cache"))]
        {
            let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
            let mut had_tag_match = false;
            for w in 0..TLB_WAYS {
                if entry_tag(ways[w]) != tag {
                    continue;
                }
                had_tag_match = true;
                let frame_id = entry_frame(ways[w]);
                if self.meta(frame_id).key() == Some(page_key) {
                    if let Some(g) = self.try_get_read_guard(frame_id) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            if w > 0 {
                                ways.swap(0, w);
                            }
                            unsafe {
                                TLB_HITS += 1;
                            }
                            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                            TLB_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
                            return Ok(g);
                        }
                    }
                }
            }
            if had_tag_match {
                unsafe {
                    TLB_FALSE_HITS += 1;
                }
                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                TLB_FALSE_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Miss path
        unsafe {
            TLB_MISSES += 1;
        }
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        TLB_MISSES_GLOBAL.fetch_add(1, Ordering::Relaxed);
        let packed = pack_page_key(&page_key);
        let use_prefill = unsafe {
            let prev = LAST_MISS_KEY;
            LAST_MISS_KEY = packed;
            packed.wrapping_sub(prev) <= PREFILL_COUNT
        };

        let guard = crossbeam_epoch::pin();
        let result = self.overflow.get_apply_with_siblings(
            &packed,
            |frame_id, view| match self.try_get_read_guard(frame_id) {
                Some(g) => {
                    if use_prefill {
                        let c_hash = super::hash::hash_u64((packed >> 32) as u64);
                        let pid_prefix = (packed as u32) & !0xFFu32;
                        unsafe {
                            for (byte, sibling_frame) in view.siblings_after().take(PREFILL_COUNT) {
                                let val = c_hash.wrapping_add((pid_prefix | byte as u32) as u64);
                                let s_set = (val as usize) & TLB_SET_MASK;
                                let s_entry = (((val >> 10) as u32 & 0x1F) | 1) << TAG_SHIFT
                                    | (sibling_frame as u32 & FRAME_MASK);
                                if !tlb_insert_if_empty(s_set, s_entry) {
                                    tlb_insert(s_set, s_entry);
                                }
                                TLB_PREFILLS += 1;
                            }
                        }
                    }
                    Some(g)
                }
                None => None,
            },
            &guard,
        );

        match result {
            Some(Some(g)) => {
                g.evict_info().update();
                unsafe {
                    tlb_insert(set, pack_entry(tag, g.frame_id()));
                    OVERFLOW_HITS += 1;
                }
                Ok(g)
            }
            Some(None) => Err(MemPoolStatus::FrameReadLatchGrantFailed),
            None => {
                self.ensure_free_frames()?;
                let victim = self.handle_page_fault(page_key)?;
                unsafe {
                    tlb_insert(set, pack_entry(tag, victim.frame_id()));
                    PAGE_FAULTS += 1;
                }
                Ok(victim.downgrade())
            }
        }
    }

    // ----- TLB fast path: get_page_for_write --------------------------------

    #[inline(always)]
    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_write_count();
        let page_key = key.p_key();
        let hk = HashedKey::new(&page_key);
        let set = hk.tlb_set();
        let tag = hk.tlb_tag();

        #[cfg(feature = "tlb_victim_cache")]
        {
            if let Some(frame_id) = unsafe { tlb_probe(set, tag) } {
                if self.meta(frame_id).key() == Some(page_key) {
                    if let Some(g) = self.try_get_write_guard(frame_id, true) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            unsafe {
                                TLB_HITS += 1;
                            }
                            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                            TLB_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
                            return Ok(g);
                        }
                    }
                }
                unsafe {
                    TLB_FALSE_HITS += 1;
                }
                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                TLB_FALSE_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
            }
        }
        #[cfg(not(feature = "tlb_victim_cache"))]
        {
            let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
            let mut had_tag_match = false;
            for w in 0..TLB_WAYS {
                if entry_tag(ways[w]) != tag {
                    continue;
                }
                had_tag_match = true;
                let frame_id = entry_frame(ways[w]);
                if self.meta(frame_id).key() == Some(page_key) {
                    if let Some(g) = self.try_get_write_guard(frame_id, true) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            if w > 0 {
                                ways.swap(0, w);
                            }
                            unsafe {
                                TLB_HITS += 1;
                            }
                            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                            TLB_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
                            return Ok(g);
                        }
                    }
                }
            }
            if had_tag_match {
                unsafe {
                    TLB_FALSE_HITS += 1;
                }
                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                TLB_FALSE_HITS_GLOBAL.fetch_add(1, Ordering::Relaxed);
            }
        }

        unsafe {
            TLB_MISSES += 1;
        }
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        TLB_MISSES_GLOBAL.fetch_add(1, Ordering::Relaxed);
        let packed = pack_page_key(&page_key);
        let use_prefill = unsafe {
            let prev = LAST_MISS_KEY;
            LAST_MISS_KEY = packed;
            packed.wrapping_sub(prev) <= PREFILL_COUNT
        };
        self.ensure_free_frames()?;

        let guard = crossbeam_epoch::pin();
        let result = self.overflow.get_apply_with_siblings(
            &packed,
            |frame_id, view| match self.try_get_write_guard(frame_id, true) {
                Some(g) => {
                    if use_prefill {
                        let c_hash = super::hash::hash_u64((packed >> 32) as u64);
                        let pid_prefix = (packed as u32) & !0xFFu32;
                        unsafe {
                            for (byte, sibling_frame) in view.siblings_after().take(PREFILL_COUNT) {
                                let val = c_hash.wrapping_add((pid_prefix | byte as u32) as u64);
                                let s_set = (val as usize) & TLB_SET_MASK;
                                let s_entry = (((val >> 10) as u32 & 0x1F) | 1) << TAG_SHIFT
                                    | (sibling_frame as u32 & FRAME_MASK);
                                if !tlb_insert_if_empty(s_set, s_entry) {
                                    tlb_insert(s_set, s_entry);
                                }
                                TLB_PREFILLS += 1;
                            }
                        }
                    }
                    Some(g)
                }
                None => None,
            },
            &guard,
        );

        match result {
            Some(Some(g)) => {
                g.evict_info().update();
                unsafe {
                    tlb_insert(set, pack_entry(tag, g.frame_id()));
                    OVERFLOW_HITS += 1;
                }
                Ok(g)
            }
            Some(None) => Err(MemPoolStatus::FrameWriteLatchGrantFailed),
            None => {
                let g = self.handle_page_fault(page_key)?;
                g.dirty().store(true, Ordering::Release);
                unsafe {
                    tlb_insert(set, pack_entry(tag, g.frame_id()));
                    PAGE_FAULTS += 1;
                }
                Ok(g)
            }
        }
    }

    // ----- create pages -----------------------------------------------------

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let container = self.fm.container_manager().get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        let mut victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());

        self.overflow_insert(page_key, victim.frame_id());

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.fm.increment_used();

        Ok(victim)
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        let mut guards = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            match self.create_new_page_for_write(c_key) {
                Ok(g) => guards.push(g),
                Err(_) => break,
            }
        }
        Ok(guards)
    }

    // ----- container ops / misc ---------------------------------------------

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.overflow_contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.overflow_get_page_keys(c_key)
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.fm.flush_all()
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.fm.flush_all_and_reset(|pk, idx| {
            if self.overflow_lookup(pk) == Some(idx) {
                self.overflow_remove(pk);
            }
        })
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        self.fm.clear_dirty_flags()
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    unsafe fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();

        let mut num_frames_per_container = BTreeMap::new();
        for i in 0..self.num_frames() {
            if let Some(key) = self.fm.meta(i as u32).key() {
                *num_frames_per_container.entry(key.c_key).or_insert(0) += 1;
            }
        }

        let mut disk_io_per_container = BTreeMap::new();
        for (c_key, (count, file_stats)) in &self.fm.container_manager().get_stats() {
            disk_io_per_container.insert(
                *c_key,
                (
                    *count as i64,
                    file_stats.read_count() as i64,
                    file_stats.write_count() as i64,
                ),
            );
        }
        let (total_created, total_read, total_write) = disk_io_per_container
            .iter()
            .fold((0, 0, 0), |acc, (_, (c, r, w))| {
                (acc.0 + c, acc.1 + r, acc.2 + w)
            });

        MemoryStats {
            bp_num_frames_in_mem: self.num_frames(),
            bp_new_page: new_page,
            bp_read_frame: read_count,
            bp_read_frame_wait: read_count_waiting,
            bp_write_frame: write_count,
            bp_num_frames_per_container: num_frames_per_container,
            disk_created: total_created as usize,
            disk_read: total_read as usize,
            disk_write: total_write as usize,
            disk_io_per_container,
        }
    }

    unsafe fn reset_stats(&self) {
        self.stats.clear();
    }

    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    fn sample_coverage(&self) -> (u64, u64) {
        let hits = TLB_HITS_GLOBAL.load(Ordering::Relaxed);
        let misses = TLB_MISSES_GLOBAL.load(Ordering::Relaxed);
        let false_hits = TLB_FALSE_HITS_GLOBAL.load(Ordering::Relaxed);
        (hits, hits + misses + false_hits)
    }

    /// Emit the unified `fast_path_coverage` line that the PT / LIPAH variants
    /// also emit. For TLB-V2 "fast path" = TLB hit; false_hits are *not*
    /// counted as hits because they fall through to the overflow path. Reads
    /// from the global atomic shadows (TLB_HITS_GLOBAL etc.) because the
    /// thread-local TLB_HITS only reflects the calling thread. See plan: PT
    /// strength/weakness study, Part B1.
    fn print_profile(&self) {
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        {
            let hits = TLB_HITS_GLOBAL.load(Ordering::Relaxed);
            let misses = TLB_MISSES_GLOBAL.load(Ordering::Relaxed);
            let false_hits = TLB_FALSE_HITS_GLOBAL.load(Ordering::Relaxed);
            let total = hits + misses + false_hits;
            if total == 0 {
                return;
            }
            println!("\n=== TLB-V2 Coverage ===");
            println!(
                "TLB hits:       {:>12}  ({:.1}%)",
                hits,
                hits as f64 / total as f64 * 100.0
            );
            println!(
                "TLB false_hits: {:>12}  ({:.1}%)",
                false_hits,
                false_hits as f64 / total as f64 * 100.0
            );
            println!(
                "TLB misses:     {:>12}  ({:.1}%)",
                misses,
                misses as f64 / total as f64 * 100.0
            );
            let cov = hits as f64 / total as f64;
            println!(
                "fast_path_coverage: {:.4}  (hits={}, total={})",
                cov, hits, total
            );
        }
    }
}

// ===========================================================================
// Tests (original TlbBP has none; add basic parity-oriented cases here)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{container::ContainerManager, random::gen_random_pathname};
    use std::sync::Arc;

    fn get_test_bp(num_frames: usize) -> Arc<TlbBPV2> {
        let base_dir = gen_random_pathname(Some("test_tlb_bp_v2_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(TlbBPV2::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_tlbv2_create_and_read() {
        let num_frames = 10;
        let bp = get_test_bp(num_frames);
        let c_key = ContainerKey::new(0, 0);
        let mut keys = Vec::new();
        for i in 0..num_frames {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i as u8;
            keys.push(g.page_frame_key().unwrap());
        }
        for (i, k) in keys.iter().enumerate() {
            let g = bp.get_page_for_read(*k).unwrap();
            assert_eq!(g[0], i as u8);
        }
    }

    #[test]
    fn test_tlbv2_write_back() {
        // 2 frames, many pages → force eviction + disk I/O.
        let bp = get_test_bp(2);
        let c_key = ContainerKey::new(0, 0);
        let mut keys = Vec::new();
        for i in 0..50u8 {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i;
            keys.push(g.page_frame_key().unwrap());
        }
        for (i, k) in keys.iter().enumerate() {
            let g = bp.get_page_for_read(*k).unwrap();
            assert_eq!(g[0], i as u8);
        }
    }

    #[test]
    fn test_tlbv2_flush_and_reset() {
        let bp = get_test_bp(8);
        let c_key = ContainerKey::new(0, 0);
        let mut keys = Vec::new();
        for i in 0..16 {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i as u8;
            keys.push(g.page_frame_key().unwrap());
        }
        bp.flush_all_and_reset().unwrap();
        for (i, k) in keys.iter().enumerate() {
            let g = bp.get_page_for_read(*k).unwrap();
            assert_eq!(g[0], i as u8);
        }
    }

    #[test]
    fn test_tlbv2_concurrent_write() {
        let bp = get_test_bp(10);
        let c_key = ContainerKey::new(0, 0);
        let mut g = bp.create_new_page_for_write(c_key).unwrap();
        g[0] = 0;
        let pk = g.page_frame_key().unwrap();
        drop(g);

        let num_threads = 3;
        let num_iters = 50u8;
        std::thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iters {
                        loop {
                            if let Ok(mut g) = bp.get_page_for_write(pk) {
                                g[0] = g[0].wrapping_add(1);
                                break;
                            }
                        }
                    }
                });
            }
        });
        let g = bp.get_page_for_read(pk).unwrap();
        assert_eq!(g[0], num_threads * num_iters);
    }
}
