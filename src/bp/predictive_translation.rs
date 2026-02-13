//! Predictive Translation Buffer Pool
//!
//! Buffer pool with **deterministic frame placement**: each page has a preferred
//! frame computed as `hash(page_key) % num_frames`.
//!
//! **Lookup:** check the preferred frame first (tag check on `FrameMeta::key()`).
//! If the preferred frame holds the right page, we're done (fast path).
//! Otherwise fall through to the overflow DashMap for pages not in their
//! preferred frame.
//!
//! **Page fault:** if the preferred frame is free, load the page there (no DashMap
//! entry needed). If occupied, load into any free frame and insert into the
//! overflow DashMap.
//!
//! **Eviction:** clock sweep over all frames. On evict, only remove from the
//! DashMap if the page was in overflow (i.e. not in its preferred frame).
//!
//! ## TODO — remaining paper ideas
//!
//! - [ ] **Promotion / demotion** (Sec 3.2, 5.1): after the second access to a
//!       page NOT in its preferred frame, probabilistically promote it
//!       (swap into preferred frame, demote current occupant).
//!       Probabilities: 1/50 (no demotion) or 1/512 (demotion needed).
//!
//! - [ ] **Optimistic latch** (Sec 4.2): replace per-bucket locking with a
//!       version-counter optimistic latch so read-path translations are lock-free.
//!       This is what lets the CPU overlap lookup and predicted-frame access.
//!
//! - [ ] **Inlined chaining hash table** (Sec 4.1): custom hash table with the
//!       first slot inlined in the bucket (no pointer chase). Frame header stored
//!       inside the hash entry — eliminates one indirection.
//!
//! - [ ] **Superscalar interleaving** (Sec 3.1, Listing 2): structure the read
//!       hot path so predicted-frame access and hash verification are independent
//!       loads, letting the CPU issue them in parallel.
//!
//! - [ ] **Benchmarks**: TPC-C + YCSB (uniform & skewed) vs BufferPoolClock /
//!       VMCachePool. Track throughput, IPC, L2 misses, promotion overhead.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};
use crate::{
    bp::frame_guards::box_as_mut_ptr,
    container::ContainerManager,
    log_debug, log_warn,
    page::{Page, PageId},
};

use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use concurrent_queue::ConcurrentQueue;
use dashmap::DashMap;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

// ---------------------------------------------------------------------------
// Type aliases (mirrors the convention in buffer_pool_clock.rs)
// ---------------------------------------------------------------------------
type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

// ---------------------------------------------------------------------------
// Overflow translation table
// ---------------------------------------------------------------------------
// Only holds pages that are NOT in their preferred frame (overflow / probes).
// Pages that sit in their preferred frame are found via the tag check and do
// not appear in this map.
struct OverflowTable {
    map: DashMap<PageKey, usize>, // PageKey -> frame index (overflow pages only)
}

impl OverflowTable {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    #[inline]
    fn lookup(&self, key: &PageKey) -> Option<usize> {
        self.map.get(key).map(|r| *r)
    }

    #[inline]
    fn insert(&self, key: PageKey, frame_id: usize) {
        self.map.insert(key, frame_id);
    }

    #[inline]
    fn remove(&self, key: &PageKey) -> Option<usize> {
        self.map.remove(key).map(|(_, v)| v)
    }

    #[inline]
    fn contains_key(&self, key: &PageKey) -> bool {
        self.map.contains_key(key)
    }

    fn get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.map
            .iter()
            .filter(|r| r.key().c_key == c_key)
            .map(|r| {
                let (pk, &frame_id) = r.pair();
                PageFrameKey::new_with_frame_id(pk.c_key, pk.page_id, frame_id as u32)
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// PredictiveTranslationBP
// ---------------------------------------------------------------------------

/// Buffer pool using predictive translation.
pub struct PredictiveTranslationBP {
    num_frames: usize,
    used_frames: AtomicUsize,
    clock_hand: AtomicUsize,
    container_manager: Arc<ContainerManager>,
    /// Free-frame hint queue (indices of frames known to be free).
    free_list: ConcurrentQueue<usize>,
    /// The actual page data for each frame.
    #[allow(clippy::vec_box)]
    pages: UnsafeCell<Vec<Box<Page>>>,
    /// Per-frame metadata (latch, dirty bit, eviction info, page key).
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>,
    /// Overflow table: maps PageKey -> frame index for pages NOT in their
    /// preferred frame.  Pages in their preferred frame are found via tag check.
    overflow: OverflowTable,
    /// Runtime statistics.
    stats: BPStats,
}

// SAFETY: synchronisation is done via per-frame latches and the translation table.
unsafe impl Sync for PredictiveTranslationBP {}
unsafe impl Send for PredictiveTranslationBP {}

impl Drop for PredictiveTranslationBP {
    fn drop(&mut self) {
        if self.container_manager.remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl PredictiveTranslationBP {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("PredictiveTranslationBP created: num_frames={}", num_frames);

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

        Ok(Self {
            num_frames,
            used_frames: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            container_manager,
            free_list,
            pages,
            metas,
            overflow: OverflowTable::new(),
            stats: BPStats::new(),
        })
    }

    // ------------------------------------------------------------------
    // Deterministic placement
    // ------------------------------------------------------------------

    /// Compute the preferred frame index for a page key.
    /// `hash(page_key) % num_frames`.
    #[inline]
    fn preferred_frame(&self, key: &PageKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.num_frames
    }

    /// Check whether frame `idx` currently holds `key` (tag check).
    /// This reads the atomic page key from FrameMeta without latching.
    #[inline]
    fn frame_holds_page(&self, idx: usize, key: &PageKey) -> bool {
        let meta = &unsafe { &*self.metas.get() }[idx];
        meta.key() == Some(*key)
    }

    /// Returns true if `page_key` is in its preferred frame (i.e. NOT in
    /// the overflow table).
    #[inline]
    fn is_in_preferred_frame(&self, page_key: &PageKey) -> bool {
        self.frame_holds_page(self.preferred_frame(page_key), page_key)
    }

    pub fn eviction_stats(&self) -> String {
        "PredictiveTranslation: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "PredictiveTranslation: file stats disabled".to_string()
    }

    // ------------------------------------------------------------------
    // Frame access helpers
    // ------------------------------------------------------------------

    fn try_get_read_guard(&self, index: usize) -> Option<FRGuard> {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FRGuard::try_new(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
        )
    }

    fn try_get_write_guard(&self, index: usize, make_dirty: bool) -> Option<FWGuard> {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FWGuard::try_new(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
            make_dirty,
        )
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /// Try to get a free frame from the free list.  If none are available,
    /// run clock eviction to free some up.
    fn choose_victim(&self) -> Option<FWGuard> {
        // Try the free list first.
        while let Ok(idx) = self.free_list.pop() {
            if let Some(guard) = self.try_get_write_guard(idx, false) {
                if guard.page_key().is_none() {
                    return Some(guard);
                }
                // Frame still has a page; don't push back (would re-queue non-free frame).
            } else {
                // Couldn't get latch (e.g. still held by evictor that just pushed); put back.
                self.free_list.push(idx).ok();
            }
        }
        None
    }

    fn ensure_free_frames(&self) -> Result<(), MemPoolStatus> {
        let used = self.used_frames.load(Ordering::Acquire);
        let ratio = used as f64 / self.num_frames as f64;
        if ratio > 0.95 {
            log_warn!(
                "[PT-EVICT] Used frames: {}/{} ({:.1}%). Evicting...",
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
        let mut evicted = 0usize;

        for _ in 0..max_iter {
            let start = self.fetch_add_clock_hand(batch);
            for offset in 0..batch {
                let idx = (start + offset) % self.num_frames;
                let meta = &mut unsafe { &mut *self.metas.get() }[idx];

                if meta.key().is_none() || meta.latch.is_locked() {
                    continue;
                }

                // Clock: if marked, clear mark and skip.  If unmarked, evict.
                if meta.evict_info.score() > 0 {
                    meta.evict_info.update(); // For clock this sets mark; we want to *reset*.
                    meta.evict_info.reset();
                    continue;
                }

                // Try to write-latch the frame for eviction.
                if let Some(guard) = self.try_get_write_guard(idx, false) {
                    if guard.page_key().is_none() {
                        continue;
                    }
                    // Flush if dirty.
                    self.write_to_disk_if_dirty_w(&guard).unwrap();
                    // Remove from overflow table only if NOT in preferred frame.
                    if let Some(pk) = guard.page_key() {
                        if self.preferred_frame(&pk) != idx {
                            self.overflow.remove(&pk);
                        }
                    }
                    // Clear the frame.
                    guard.set_page_key(None);
                    guard.evict_info().reset();
                    self.free_list.push(idx).ok();
                    evicted += 1;
                }
            }
            if evicted > 0 {
                self.used_frames.fetch_sub(evicted, Ordering::AcqRel);
                return Ok(());
            }
        }

        if evicted == 0 {
            Err(MemPoolStatus::CannotEvictPage)
        } else {
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // Page fault (shared by get_page_for_read and get_page_for_write)
    // ------------------------------------------------------------------

    /// Load a page from disk into a frame.  Returns a write guard on the
    /// newly-loaded frame.
    ///
    /// We always obtain a frame via `choose_victim()` (free-list pop) to keep
    /// the free-list invariant intact.  After getting the frame, we check
    /// whether it happens to be the preferred frame — if so the page is NOT
    /// added to the overflow table (it can be found via tag check).
    fn handle_page_fault_write(
        &self,
        page_key: PageKey,
        pref: usize,
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

        let in_preferred = victim.frame_id() as usize == pref;

        // Claim the mapping BEFORE reading from disk so that concurrent
        // lookups for this page see the frame and wait on the latch rather
        // than triggering a duplicate page-fault.
        //  - Set page_key early → tag check at the preferred frame matches.
        //  - Insert into overflow  → overflow lookup finds the frame.
        // We hold the write latch, so no concurrent reader can see
        // incomplete page data.
        victim.set_page_key(Some(page_key));
        if !in_preferred {
            self.overflow.insert(page_key, victim.frame_id() as usize);
        }

        // Read the page from disk.
        if let Err(e) = self
            .container_manager
            .get_container(page_key.c_key)
            .read_page(page_key.page_id, &mut victim)
        {
            // Undo claims.
            victim.set_page_key(None);
            if !in_preferred {
                self.overflow.remove(&page_key);
            }
            self.free_list.push(victim.frame_id() as usize).ok();
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        victim.dirty().store(true, Ordering::Release);

        Ok(victim)
    }

    // ------------------------------------------------------------------
    // Disk I/O helpers
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

// ===========================================================================
// MemPool trait implementation
// ===========================================================================

impl MemPool for PredictiveTranslationBP {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        // Container creation is lazy via ContainerManager.
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        // TODO: mark container as temp so writes are skipped on eviction.
        Ok(())
    }

    // ----- create new page ------------------------------------------------

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let mut victim = self
            .choose_victim()
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        // Allocate a new page id from the container.
        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        let in_preferred = victim.frame_id() as usize == self.preferred_frame(&page_key);

        // Only register in overflow table if NOT in preferred frame.
        if !in_preferred {
            self.overflow.insert(page_key, victim.frame_id() as usize);
        }

        // Initialise the frame.
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
                Err(_) => break, // Return what we have so far.
            }
        }
        Ok(guards)
    }

    // ----- page presence --------------------------------------------------

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        let pk = key.p_key();
        // Check preferred frame (tag check) OR overflow table.
        self.is_in_preferred_frame(&pk) || self.overflow.contains_key(&pk)
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        // We need to return ALL resident pages for this container:
        // (a) pages in their preferred frame (found by scanning frames), and
        // (b) pages in overflow.
        let mut result = Vec::new();

        // Scan all frames for pages belonging to c_key that are in their
        // preferred frame (these are NOT in the overflow table).
        for i in 0..self.num_frames {
            let meta = &unsafe { &*self.metas.get() }[i];
            if let Some(pk) = meta.key() {
                if pk.c_key == c_key && self.preferred_frame(&pk) == i {
                    result.push(PageFrameKey::new_with_frame_id(
                        pk.c_key,
                        pk.page_id,
                        i as u32,
                    ));
                }
            }
        }

        // Add overflow pages for this container.
        result.extend(self.overflow.get_page_keys(c_key));

        result
    }

    // ----- get page for write ---------------------------------------------

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        log_debug!("PT page write: {}", key);
        self.stats.inc_write_count();

        self.ensure_free_frames()?;

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        // 1. Fast path: check the preferred frame (tag check).
        if self.frame_holds_page(pref, &page_key) {
            if let Some(g) = self.try_get_write_guard(pref, true) {
                // Re-verify under latch: page may have been evicted between
                // the tag check and latch acquisition.
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    return Ok(g);
                }
                // Page was replaced between tag check and latch; drop guard
                // and fall through to overflow / fault.
            } else {
                // Latch failed.  Re-check: if the page is still here then
                // another thread genuinely holds the latch → tell the caller
                // to retry.  If the page disappeared (evicted between the
                // first tag check and latch attempt), fall through.
                if self.frame_holds_page(pref, &page_key) {
                    return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                }
            }
        }

        // 2. Slow path: check the overflow table.
        if let Some(idx) = self.overflow.lookup(&page_key) {
            if let Some(g) = self.try_get_write_guard(idx, true) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    return Ok(g);
                }
                // Stale overflow entry (page was evicted); fall through to fault.
            } else {
                // Overflow has the page but latch failed → someone holds it.
                return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
            }
        }

        // 3. Page fault — load from disk.
        self.handle_page_fault_write(page_key, pref)
    }

    // ----- get page for read ----------------------------------------------

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        log_debug!("PT page read: {}", key);
        self.stats.inc_read_count();

        self.ensure_free_frames()?;

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        // 1. Fast path: check the preferred frame (tag check).
        if self.frame_holds_page(pref, &page_key) {
            if let Some(g) = self.try_get_read_guard(pref) {
                // Re-verify under latch.
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    return Ok(g);
                }
                // Page was replaced; drop guard and fall through.
            } else {
                // Latch failed.  Re-check: if still here, someone else has
                // the write latch → retry.  If gone, fall through.
                if self.frame_holds_page(pref, &page_key) {
                    return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                }
            }
        }

        // 2. Slow path: check the overflow table.
        if let Some(idx) = self.overflow.lookup(&page_key) {
            if let Some(g) = self.try_get_read_guard(idx) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    return Ok(g);
                }
                // Stale overflow entry; fall through to fault.
            } else {
                // Overflow has the page but latch failed.
                return Err(MemPoolStatus::FrameReadLatchGrantFailed);
            }
        }

        // 3. Page fault — load from disk.
        let victim = self.handle_page_fault_write(page_key, pref)?;
        Ok(victim.downgrade())
    }

    // ----- prefetch -------------------------------------------------------

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        // No-op for now.
        Ok(())
    }

    // ----- flush / reset --------------------------------------------------

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
                // Only remove from overflow if NOT in preferred frame.
                if self.preferred_frame(&pk) != i {
                    self.overflow.remove(&pk);
                }
            }
            frame.clear();
        });

        self.container_manager.flush_all()?;

        // Repopulate the free list.
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
        // No-op hint for now.
        Ok(())
    }

    // ----- stats ----------------------------------------------------------

    unsafe fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();

        let mut num_frames_per_container = BTreeMap::new();
        for i in 0..self.num_frames {
            if let Some(key) = unsafe { &*self.metas.get() }[i].key() {
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
impl PredictiveTranslationBP {
    /// # Safety
    /// Must not be called while the BP is in use by other threads.
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_translation_table();
    }

    unsafe fn check_all_frames_unlatched(&self) {
        for i in 0..self.num_frames {
            assert!(
                self.try_get_write_guard(i, false).is_some(),
                "frame {} is still latched",
                i
            );
        }
    }

    unsafe fn check_translation_table(&self) {
        use std::collections::HashMap;
        // Build map of overflow entries: frame_id -> page_key.
        let mut overflow_frame_to_page: HashMap<usize, PageKey> = HashMap::new();
        self.overflow.map.iter().for_each(|r| {
            let (pk, &fid) = r.pair();
            overflow_frame_to_page.insert(fid, *pk);
        });
        for i in 0..self.num_frames {
            let meta = &(*self.metas.get())[i];
            if let Some(pk) = meta.key() {
                let pref = self.preferred_frame(&pk);
                if pref == i {
                    // Page is in its preferred frame — must NOT be in overflow.
                    assert!(
                        !overflow_frame_to_page.contains_key(&i),
                        "frame {} has key {:?} in preferred position but is also in overflow",
                        i, pk
                    );
                } else {
                    // Page is in overflow — must be in the overflow table.
                    assert!(
                        overflow_frame_to_page.get(&i) == Some(&pk),
                        "frame {} has key {:?} (preferred={}) but overflow table disagrees",
                        i, pk, pref
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{container::ContainerManager, random::gen_random_pathname};
    use std::sync::Arc;

    fn get_test_pt(num_frames: usize) -> Arc<PredictiveTranslationBP> {
        let base_dir = gen_random_pathname(Some("test_pt_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(PredictiveTranslationBP::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_pt_create_and_read() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        // Read them back.
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_pt_write_back() {
        let db_id = 0;
        let num_frames = 2;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        // Create more pages than frames → forces eviction & disk I/O.
        let mut keys = Vec::new();
        for i in 0..100u8 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        // All pages should be retrievable (from memory or disk).
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_pt_flush_and_reset() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..(num_frames * 2) {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        bp.flush_all_and_reset().unwrap();

        unsafe { bp.run_checks() };

        // After reset, pages should still be loadable from disk.
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_pt_concurrent_latch() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let frame = bp.create_new_page_for_write(c_key).unwrap();
        let key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iters = 80u8;
        std::thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iters {
                        loop {
                            if let Ok(mut g) = bp.get_page_for_write(key) {
                                g[0] += 1;
                                break;
                            }
                        }
                    }
                });
            }
        });

        let guard = bp.get_page_for_read(key).unwrap();
        assert_eq!(guard[0], num_threads * num_iters);
    }
}
