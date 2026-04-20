//! Standalone TLB + overflow buffer pool.
//!
//! TLB: 4-way set-associative, 1024 sets, 5-bit tag pre-filter (16 KB).
//! Overflow: Congee (concurrent ART tree) — ordered by PageKey, enabling
//! future range-prefill of TLB entries for sequential scans.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{box_as_mut_ptr, FrameMeta, FrameReadGuard, FrameWriteGuard},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};
use crate::{
    container::ContainerManager,
    log_debug, log_warn,
    page::{Page, PageId},
};

use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use concurrent_queue::ConcurrentQueue;
use congee::CongeeRaw;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;

// ===========================================================================
// PageKey <-> u64 packing
// ===========================================================================

/// Pack PageKey into a usize for use as congee key.
/// Layout: [c_key: 32 bits | page_id: 32 bits]
/// This preserves ordering: pages within the same container are adjacent.
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
// TLB constants and helpers
// ===========================================================================

// --- TLB layout: direct-mapped + victim cache, or 4-way set-associative ---
#[cfg(feature = "tlb_victim_cache")]
const TLB_ENTRIES: usize = 4096; // direct-mapped primary
#[cfg(feature = "tlb_victim_cache")]
const TLB_SET_MASK: usize = TLB_ENTRIES - 1;
#[cfg(feature = "tlb_victim_cache")]
const VICTIM_SIZE: usize = 16; // fully-associative victim cache

#[cfg(not(feature = "tlb_victim_cache"))]
const TLB_SETS: usize = 1024;
#[cfg(not(feature = "tlb_victim_cache"))]
const TLB_WAYS: usize = 4;
#[cfg(not(feature = "tlb_victim_cache"))]
const TLB_SET_MASK: usize = TLB_SETS - 1;

const FRAME_BITS: u32 = 27;
const FRAME_MASK: u32 = (1 << FRAME_BITS) - 1;
const TAG_SHIFT: u32 = FRAME_BITS;

const PREFILL_COUNT: usize = 8; // target + 7 forward neighbors

#[inline(always)]
fn pack_entry(tag: u32, frame_id: u32) -> u32 {
    tag | (frame_id & FRAME_MASK)
}

#[inline(always)]
fn entry_tag(entry: u32) -> u32 {
    entry & !FRAME_MASK
}

#[inline(always)]
fn entry_frame(entry: u32) -> usize {
    (entry & FRAME_MASK) as usize
}

// --- Direct-mapped + victim cache ---
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut TLB: [u32; TLB_ENTRIES] = [0; TLB_ENTRIES];
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut VICTIM: [u32; VICTIM_SIZE] = [0; VICTIM_SIZE];
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut VICTIM_SETS: [u16; VICTIM_SIZE] = [0; VICTIM_SIZE]; // store set index for each victim entry
#[cfg(feature = "tlb_victim_cache")]
#[thread_local]
static mut VICTIM_HEAD: usize = 0; // FIFO insertion pointer

// --- 4-way set-associative ---
#[cfg(not(feature = "tlb_victim_cache"))]
#[thread_local]
static mut TLB: [[u32; TLB_WAYS]; TLB_SETS] = [[0; TLB_WAYS]; TLB_SETS];

#[thread_local]
static mut TLB_HITS: u64 = 0;
#[thread_local]
static mut TLB_MISSES: u64 = 0;
#[thread_local]
static mut TLB_PREFILLS: u64 = 0;
/// Last missed packed PageKey — used to detect sequential access for sibling prefill.
#[thread_local]
static mut LAST_MISS_KEY: usize = 0;
#[thread_local]
static mut OVERFLOW_HITS: u64 = 0;
#[thread_local]
static mut PAGE_FAULTS: u64 = 0;
#[thread_local]
static mut TLB_FALSE_HITS: u64 = 0;

// ===========================================================================
// TLB operations (abstracted over layout)
// ===========================================================================

/// Probe the TLB for a matching entry. Returns Some(frame_id) if found.
#[inline(always)]
unsafe fn tlb_probe(set: usize, tag: u32) -> Option<usize> {
    #[cfg(feature = "tlb_victim_cache")]
    {
        // 1. Check direct-mapped primary
        let entry = *TLB.get_unchecked(set);
        if entry_tag(entry) == tag {
            return Some(entry_frame(entry));
        }
        // 2. Check victim cache (fully associative)
        for v in 0..VICTIM_SIZE {
            if VICTIM_SETS[v] == set as u16 && entry_tag(VICTIM[v]) == tag {
                // Promote: swap victim entry into primary, evicted primary goes to victim slot
                let victim_entry = VICTIM[v];
                VICTIM[v] = entry; // old primary (possibly 0) goes to this victim slot
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

/// Insert an entry into the TLB (shift-down / evict as needed).
#[inline(always)]
unsafe fn tlb_insert(set: usize, entry: u32) {
    #[cfg(feature = "tlb_victim_cache")]
    {
        // Evict current primary to victim cache (FIFO), then write new entry
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

/// Insert an entry only if there's space (for prefill — less aggressive).
#[inline(always)]
unsafe fn tlb_insert_if_empty(set: usize, entry: u32) -> bool {
    #[cfg(feature = "tlb_victim_cache")]
    {
        if *TLB.get_unchecked(set) == 0 {
            *TLB.get_unchecked_mut(set) = entry;
            return true;
        }
        // Check if any victim slot is empty
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
// Buffer pool
// ===========================================================================

pub struct TlbBP {
    num_frames: usize,
    used_frames: AtomicUsize,
    clock_hand: AtomicUsize,
    container_manager: Arc<ContainerManager>,
    free_list: ConcurrentQueue<usize>,
    #[allow(clippy::vec_box)]
    pages: UnsafeCell<Vec<Box<Page>>>,
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>,
    /// Overflow: concurrent ART tree mapping packed PageKey (usize) -> frame_id (usize).
    overflow: CongeeRaw<usize, usize>,
    stats: BPStats,
}

unsafe impl Sync for TlbBP {}
unsafe impl Send for TlbBP {}

impl TlbBP {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("TlbBP created: num_frames={}", num_frames);

        let free_list = ConcurrentQueue::bounded(num_frames);
        for i in 0..num_frames {
            free_list.push(i).unwrap();
        }

        let pages: UnsafeCell<Vec<Box<Page>>> = UnsafeCell::new(
            (0..num_frames)
                .into_par_iter()
                .map(|_| Box::new(Page::new_empty()))
                .collect(),
        );

        let metas: UnsafeCell<Vec<Box<FMeta>>> = UnsafeCell::new(
            (0..num_frames)
                .into_par_iter()
                .map(|i| Box::new(FMeta::new(i as u32)))
                .collect(),
        );

        debug_assert!(
            num_frames <= u32::MAX as usize,
            "num_frames must fit in u32"
        );

        Ok(Self {
            num_frames,
            used_frames: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            container_manager,
            free_list,
            pages,
            metas,
            overflow: CongeeRaw::default(),
            stats: BPStats::new(),
        })
    }

    // ------------------------------------------------------------------
    // Overflow helpers (thin wrappers around congee)
    // ------------------------------------------------------------------

    #[inline]
    fn overflow_lookup(&self, key: &PageKey) -> Option<usize> {
        let guard = self.overflow.pin();
        self.overflow.get(&pack_page_key(key), &guard)
    }

    /// Lookup with sibling prefill in a single ART traversal.
    #[inline]
    fn overflow_lookup_with_prefill(&self, page_key: &PageKey) -> Option<usize> {
        let guard = self.overflow.pin();
        let packed = pack_page_key(page_key);
        let num_frames = self.num_frames;
        self.overflow.get_with_siblings(
            &packed,
            |frame_id, view| {
                for (_byte, neighbor_frame) in view.siblings_after().take(PREFILL_COUNT) {
                    if neighbor_frame >= num_frames {
                        continue;
                    }
                    let neighbor_packed = (packed & !0xFF) | (_byte as usize);
                    let neighbor_key = unpack_page_key(neighbor_packed);
                    let hk = HashedKey::new(&neighbor_key);
                    let set = hk.tlb_set();
                    let tag = hk.tlb_tag();
                    let entry = pack_entry(tag, neighbor_frame as u32);
                    unsafe {
                        if !tlb_insert_if_empty(set, entry) {
                            tlb_insert(set, entry);
                        }
                        TLB_PREFILLS += 1;
                    }
                }
                frame_id
            },
            &guard,
        )
    }

    #[inline]
    fn overflow_insert(&self, key: PageKey, frame_id: usize) {
        let guard = self.overflow.pin();
        let _ = self.overflow.insert(pack_page_key(&key), frame_id, &guard);
    }


    #[inline]
    fn overflow_remove(&self, key: &PageKey) -> Option<usize> {
        let guard = self.overflow.pin();
        self.overflow.remove(&pack_page_key(key), &guard)
    }

    fn overflow_contains_key(&self, key: &PageKey) -> bool {
        self.overflow_lookup(key).is_some()
    }

    fn overflow_get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        let guard = self.overflow.pin();
        let start = (c_key.as_u32() as usize) << 32;
        let end = start | 0xFFFF_FFFF;
        let mut buf = vec![(0usize, 0usize); 4096];
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
                    pk.c_key,
                    pk.page_id,
                    frame_id as u32,
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
    // Frame access
    // ------------------------------------------------------------------

    #[inline]
    fn try_get_read_guard(&self, index: usize) -> Option<FRGuard> {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FRGuard::try_new_with_key_slot(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
            std::ptr::null_mut(),
        )
    }

    #[inline]
    fn try_get_write_guard(&self, index: usize, make_dirty: bool) -> Option<FWGuard> {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FWGuard::try_new_with_key_slot(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
            std::ptr::null_mut(),
            make_dirty,
        )
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    #[inline]
    fn enqueue_free_frame(&self, idx: usize) {
        self.free_list.push(idx).ok();
    }

    fn choose_victim(&self) -> Option<FWGuard> {
        while let Ok(idx) = self.free_list.pop() {
            if let Some(guard) = self.try_get_write_guard(idx, false) {
                if guard.page_key().is_none() {
                    return Some(guard);
                }
            }
        }
        None
    }

    fn ensure_free_frames(&self) -> Result<(), MemPoolStatus> {
        let used = self.used_frames.load(Ordering::Acquire);
        let ratio = used as f64 / self.num_frames as f64;
        if ratio > 0.95 {
            log_warn!(
                "[TLB-EVICT] Used frames: {}/{} ({:.1}%). Evicting...",
                used,
                self.num_frames,
                ratio * 100.0,
            );
            self.evict_batch()
        } else {
            Ok(())
        }
    }

    fn fetch_add_clock_hand(&self, increment: usize) -> usize {
        self.clock_hand
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some((cur + increment) % self.num_frames)
            })
            .expect("clock hand update should not fail")
    }

    fn evict_batch(&self) -> Result<(), MemPoolStatus> {
        let batch = std::cmp::min(self.num_frames, 64);
        let max_iter = 2 * self.num_frames / batch;

        // Scratch space for multi-stage eviction.
        let mut clean_pages: Vec<(usize, *mut FMeta)> = Vec::new();
        let mut dirty_pages: Vec<(usize, FRGuard)> = Vec::new();
        let mut to_evict: Vec<(usize, FWGuard)> = Vec::new();

        // ─── 1. Collect candidates via clock scan ────────────────────
        let mut iters = 0;
        while clean_pages.len() + dirty_pages.len() < batch {
            if iters > max_iter {
                if clean_pages.is_empty() && dirty_pages.is_empty() {
                    return Err(MemPoolStatus::CannotEvictPage);
                }
                break;
            }
            let start = self.fetch_add_clock_hand(batch);
            for offset in 0..batch {
                let idx = (start + offset) % self.num_frames;
                let meta = &mut unsafe { &mut *self.metas.get() }[idx];

                if meta.key().is_none() || meta.latch.is_locked() {
                    continue;
                }

                // Clock: if marked, clear mark and skip. If unmarked, candidate.
                if meta.evict_info.score() > 0 {
                    meta.evict_info.reset();
                    continue;
                }

                let is_dirty = meta.is_dirty.load(Ordering::Acquire);
                if is_dirty {
                    if let Some(g) = FRGuard::try_new(
                        box_as_mut_ptr(meta),
                        box_as_mut_ptr(&mut unsafe { &mut *self.pages.get() }[idx]),
                    ) {
                        if g.page_key().is_some() {
                            dirty_pages.push((idx, g));
                        }
                    }
                } else {
                    clean_pages.push((idx, box_as_mut_ptr(meta)));
                }
            }
            iters += 1;
        }

        // ─── 2. Flush dirty pages under read latch ───────────────────
        for (_, g) in &dirty_pages {
            self.write_to_disk_if_dirty_r(g).unwrap();
        }

        // ─── 3. Latch clean pages for eviction ──────────────────────
        for (idx, meta) in clean_pages.drain(..) {
            if let Some(g) = FWGuard::try_new(
                meta,
                box_as_mut_ptr(&mut unsafe { &mut *self.pages.get() }[idx]),
                false,
            ) {
                if g.page_key().is_none() {
                    continue;
                }
                self.write_to_disk_if_dirty_w(&g).unwrap();
                to_evict.push((idx, g));
            }
        }

        // ─── 4. Upgrade dirty page latches (read → write) ───────────
        for (idx, g) in dirty_pages.drain(..) {
            if let Ok(gw) = g.try_upgrade(false) {
                to_evict.push((idx, gw));
            }
        }

        // ─── 5. Remove from overflow and finalize ───────────────────
        let mut freed = 0;
        for (idx, g) in to_evict.drain(..) {
            if let Some(pk) = g.page_key() {
                if self.overflow_lookup(&pk) == Some(idx) {
                    self.overflow_remove(&pk);
                }
            }
            g.set_page_key(None);
            g.evict_info().reset();
            self.enqueue_free_frame(idx);
            freed += 1;
        }

        if freed > 0 {
            self.used_frames.fetch_sub(freed, Ordering::AcqRel);
            Ok(())
        } else {
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    // ------------------------------------------------------------------
    // Page fault
    // ------------------------------------------------------------------

    fn handle_page_fault(
        &self,
        page_key: PageKey,
    ) -> Result<FWGuard, MemPoolStatus> {
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        let mut victim = match self.choose_victim() {
            Some(v) => v,
            None => {
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                return Err(MemPoolStatus::CannotEvictPage);
            }
        };

        debug_assert!(victim.page_key().is_none());

        // Atomic insert-if-absent via congee compute_or_insert.
        let guard = self.overflow.pin();
        let packed = pack_page_key(&page_key);
        let frame_id = victim.frame_id() as usize;
        let mut existing_frame: Option<usize> = None;
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
            // Another thread already faulted this page. Free our victim, latch theirs.
            self.enqueue_free_frame(frame_id);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return self
                .try_get_write_guard(idx, true)
                .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
        }

        victim.set_page_key(Some(page_key));

        if let Err(e) = self
            .container_manager
            .get_container(page_key.c_key)
            .read_page(page_key.page_id, &mut victim)
        {
            victim.set_page_key(None);
            self.overflow_remove(&page_key);
            self.enqueue_free_frame(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        // Don't mark dirty here — read faults should stay clean.
        // The write path marks dirty via the write guard.

        Ok(victim)
    }

    // ------------------------------------------------------------------
    // Disk I/O
    // ------------------------------------------------------------------

    fn write_to_disk_if_dirty_w(&self, guard: &FWGuard) -> Result<(), MemPoolStatus> {
        if let Some(key) = guard.page_key() {
            if guard
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.container_manager.get_container(key.c_key);
                container.write_page(key.page_id, guard)?;
            }
        }
        Ok(())
    }

    fn write_to_disk_if_dirty_r(&self, guard: &FRGuard) -> Result<(), MemPoolStatus> {
        if let Some(key) = guard.page_key() {
            if guard
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.container_manager.get_container(key.c_key);
                container.write_page(key.page_id, guard)?;
            }
        }
        Ok(())
    }
}

impl Drop for TlbBP {
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
                "TLB stats: hits={}, false_hits={}, misses={}, total={}, hit_rate={:.2}%, prefills={}, overflow_hits={}, page_faults={}",
                hits, false_hits, misses, total,
                hits as f64 / total as f64 * 100.0,
                prefills, overflow_hits, page_faults
            );
        }

        if self.container_manager.remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

// ===========================================================================
// MemPool trait
// ===========================================================================

impl MemPool for TlbBP {
    type EP = EvictionPolicyImpl;

    // ----- TLB fast path: get_page_for_read --------------------------------

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        self.stats.inc_read_count();
        let page_key = key.p_key();
        let hk = HashedKey::new(&page_key);
        let set = hk.tlb_set();
        let tag = hk.tlb_tag();
        let metas = unsafe { &*self.metas.get() };

        #[cfg(feature = "tlb_victim_cache")]
        {
            if let Some(frame_id) = unsafe { tlb_probe(set, tag) } {
                if metas[frame_id].key() == Some(page_key) {
                    if let Some(g) = self.try_get_read_guard(frame_id) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            unsafe { TLB_HITS += 1; }
                            return Ok(g);
                        }
                    }
                }
                unsafe { TLB_FALSE_HITS += 1; }
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
                if metas[frame_id].key() == Some(page_key) {
                    if let Some(g) = self.try_get_read_guard(frame_id) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            if w > 0 {
                                ways.swap(0, w);
                            }
                            unsafe { TLB_HITS += 1; }
                            return Ok(g);
                        }
                    }
                }
            }
            if had_tag_match {
                unsafe { TLB_FALSE_HITS += 1; }
            }
        }

        // Miss path — prefill siblings only if sequential pattern detected.
        unsafe { TLB_MISSES += 1; }
        let packed = pack_page_key(&page_key);
        let use_prefill = unsafe {
            let prev = LAST_MISS_KEY;
            LAST_MISS_KEY = packed;
            packed.wrapping_sub(prev) <= PREFILL_COUNT
        };
        self.ensure_free_frames()?;
        loop {
            let lookup = if use_prefill {
                self.overflow_lookup_with_prefill(&page_key)
            } else {
                self.overflow_lookup(&page_key)
            };
            if let Some(idx) = lookup {
                if let Some(g) = self.try_get_read_guard(idx) {
                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        unsafe {
                            tlb_insert(set, pack_entry(tag, idx as u32));
                            OVERFLOW_HITS += 1;
                        }
                        return Ok(g);
                    }
                    // Stale mapping — frame was reused. Retry.
                    continue;
                } else {
                    return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                }
            }
            // Not in overflow — page fault.
            let victim = self.handle_page_fault(page_key)?;
            unsafe {
                tlb_insert(set, pack_entry(tag, victim.frame_id()));
                PAGE_FAULTS += 1;
            }
            return Ok(victim.downgrade());
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
        let metas = unsafe { &*self.metas.get() };

        #[cfg(feature = "tlb_victim_cache")]
        {
            if let Some(frame_id) = unsafe { tlb_probe(set, tag) } {
                if metas[frame_id].key() == Some(page_key) {
                    if let Some(g) = self.try_get_write_guard(frame_id, true) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            unsafe { TLB_HITS += 1; }
                            return Ok(g);
                        }
                    }
                }
                unsafe { TLB_FALSE_HITS += 1; }
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
                if metas[frame_id].key() == Some(page_key) {
                    if let Some(g) = self.try_get_write_guard(frame_id, true) {
                        if g.page_key() == Some(page_key) {
                            g.evict_info().update();
                            if w > 0 {
                                ways.swap(0, w);
                            }
                            unsafe { TLB_HITS += 1; }
                            return Ok(g);
                        }
                    }
                }
            }
            if had_tag_match {
                unsafe { TLB_FALSE_HITS += 1; }
            }
        }

        unsafe { TLB_MISSES += 1; }
        let packed = pack_page_key(&page_key);
        let use_prefill = unsafe {
            let prev = LAST_MISS_KEY;
            LAST_MISS_KEY = packed;
            packed.wrapping_sub(prev) <= PREFILL_COUNT
        };
        self.ensure_free_frames()?;
        loop {
            let lookup = if use_prefill {
                self.overflow_lookup_with_prefill(&page_key)
            } else {
                self.overflow_lookup(&page_key)
            };
            if let Some(idx) = lookup {
                if let Some(g) = self.try_get_write_guard(idx, true) {
                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        unsafe {
                            tlb_insert(set, pack_entry(tag, idx as u32));
                            OVERFLOW_HITS += 1;
                        }
                        return Ok(g);
                    }
                    // Stale mapping — frame was reused. Retry.
                    continue;
                } else {
                    return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                }
            }
            // Not in overflow — page fault.
            let g = self.handle_page_fault(page_key)?;
            g.dirty().store(true, Ordering::Release);
            unsafe {
                tlb_insert(set, pack_entry(tag, g.frame_id()));
                PAGE_FAULTS += 1;
            }
            return Ok(g);
        }
    }

    // ----- create pages -----------------------------------------------------

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        let mut victim = self
            .choose_victim()
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());

        self.overflow_insert(page_key, victim.frame_id() as usize);

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.used_frames.fetch_add(1, Ordering::AcqRel);

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

    // ----- container ops ----------------------------------------------------

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    // ----- page presence ----------------------------------------------------

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.overflow_contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.overflow_get_page_keys(c_key)
    }

    // ----- misc -------------------------------------------------------------

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let frame = loop {
                if let Some(g) = self.try_get_read_guard(i) {
                    break g;
                }
                std::hint::spin_loop();
            };
            self.write_to_disk_if_dirty_r(&frame).unwrap();
        });
        self.container_manager.flush_all()?;
        Ok(())
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let mut frame = loop {
                if let Some(g) = self.try_get_write_guard(i, false) {
                    break g;
                }
                std::hint::spin_loop();
            };
            self.write_to_disk_if_dirty_w(&frame).unwrap();
            if let Some(pk) = frame.page_key() {
                if self.overflow_lookup(&pk) == Some(i) {
                    self.overflow_remove(&pk);
                }
            }
            frame.clear();
        });

        self.container_manager.flush_all()?;

        while self.free_list.pop().is_ok() {}
        for i in 0..self.num_frames {
            self.free_list.push(i).unwrap();
        }
        self.used_frames.store(0, Ordering::Release);

        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let meta = &mut unsafe { &mut *self.metas.get() }[i];
            meta.is_dirty.store(false, Ordering::Release);
        });
        self.container_manager.flush_all()?;
        Ok(())
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
        let metas = &*self.metas.get();
        for i in 0..self.num_frames {
            if let Some(key) = metas[i].key() {
                *num_frames_per_container.entry(key.c_key).or_insert(0) += 1;
            }
        }

        let mut disk_io_per_container = BTreeMap::new();
        for (c_key, (count, file_stats)) in &self.container_manager.get_stats() {
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
            bp_num_frames_in_mem: self.num_frames,
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
}
