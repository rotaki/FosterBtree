//! Dashmap buffer pool: DashMap (sharded) translation + clock eviction.
//!
//! DashMap translation from (container_key, page_id) to frame index,
//! with the same clock-based eviction as the predictive translation BP. Does not
//! use the frame_id hint (no physical hints). Exists to benchmark PT against an
//! otherwise comparable BP that only differs in the lookup path (one hash lookup
//! vs preferred-frame + overflow).
//!
//! ## Verified subset of PT (same code paths except where noted)
//!
//! - **Eviction:** Identical. `ensure_free_frames` (ratio > 0.95), `fetch_add_clock_hand`,
//!   `evict_batch`: same `batch = min(num_frames, 64)`, `max_iter = 2*num_frames/batch`,
//!   same clock loop (meta.key/latch skip, evict_info.score/update/reset, try_get_write_guard,
//!   write_to_disk_if_dirty_w, set_page_key(None), evict_info().reset(), used_frames.fetch_sub).
//!   Only difference: we remove from `translation` when `lookup(pk)==Some(idx)`; PT removes
//!   from `overflow` when `preferred_frame(pk) != idx && overflow.lookup(pk)==Some(idx)`.
//! - **Frame layout / guards:** Same. `try_get_read_guard`, `try_get_write_guard`,
//!   `write_to_disk_if_dirty_w/r`, same `metas`/`pages` layout.
//! - **create_new_pages_for_write:** Identical.
//! - **flush_all_and_reset / clear_dirty_flags / stats:** Same structure; table cleanup
//!   is translation vs overflow as above.
//!
//! **Intentional differences (so we measure lookup/placement only):**
//!
//! - **Translation:** One DashMap (all pages) vs PT’s preferred-frame tag + overflow table.
//! - **choose_victim:** Queue pop (any frame) vs PT’s preferred-first then any from set.
//! - **Lookup:** `translation.lookup(key)` vs PT’s `frame_holds_page(pref)` + overflow lookup.
//! - **Fault:** Inlined in get_page_for_*; no `fault_in_progress` (PT serializes per-page fault).
//! - **create_new_page:** We always insert into translation; PT uses preferred frame when free
//!   and only inserts into overflow when not in preferred.
//! - No promotion, no overflow_access_count, no path stats in this BP.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
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
// Page-to-frame translation layer
// ---------------------------------------------------------------------------
// Plain DashMap keyed on (container_key, page_id) -> frame index.
// This is the piece that will eventually become hash-based deterministic placement.
struct TranslationTable {
    map: DashMap<PageKey, usize>, // PageKey -> frame index
}

impl TranslationTable {
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
// DashmapBP
// ---------------------------------------------------------------------------

/// Buffer pool: plain DashMap translation + clock eviction, no hints.
pub struct DashmapBP {
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
    /// Translation layer: maps PageKey -> frame index.
    translation: TranslationTable,
    /// Runtime statistics.
    stats: BPStats,
}

// SAFETY: synchronisation is done via per-frame latches and the translation table.
unsafe impl Sync for DashmapBP {}
unsafe impl Send for DashmapBP {}

impl Drop for DashmapBP {
    fn drop(&mut self) {
        if self.container_manager.remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl DashmapBP {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("DashmapBP (DashMap) created: num_frames={}", num_frames);

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
            translation: TranslationTable::new(),
            stats: BPStats::new(),
        })
    }

    pub fn eviction_stats(&self) -> String {
        "DashmapBP: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "DashmapBP: file stats disabled".to_string()
    }

    // ------------------------------------------------------------------
    // Frame access helpers
    // ------------------------------------------------------------------

    fn try_get_read_guard(&self, index: usize) -> Option<FRGuard> {
        let metas = unsafe { &*self.metas.get() };
        let pages = unsafe { &*self.pages.get() };
        FRGuard::try_new(
            Box::as_ptr(&metas[index]) as *mut FMeta,
            Box::as_ptr(&pages[index]) as *mut Page,
        )
    }

    fn try_get_write_guard(&self, index: usize, make_dirty: bool) -> Option<FWGuard> {
        let metas = unsafe { &*self.metas.get() };
        let pages = unsafe { &*self.pages.get() };
        FWGuard::try_new(
            Box::as_ptr(&metas[index]) as *mut FMeta,
            Box::as_ptr(&pages[index]) as *mut Page,
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
                "[BP-EVICT] Used frames: {}/{} ({:.1}%). Evicting...",
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
                let meta = &unsafe { &*self.metas.get() }[idx];

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
                    // Remove from translation only if this frame is the one in the table.
                    if let Some(pk) = guard.page_key() {
                        if self.translation.lookup(&pk) == Some(idx) {
                            self.translation.remove(&pk);
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

impl MemPool for DashmapBP {
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

        let mut victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        // Allocate a new page id from the container.
        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        // Register in translation table.
        self.translation
            .insert(page_key, victim.frame_id() as usize);

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
        // No physical hint — only use (c_key, page_id) via translation table.
        self.translation.contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.translation.get_page_keys(c_key)
    }

    // ----- get page for write ---------------------------------------------

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        log_debug!("BP page write: {}", key);
        self.stats.inc_write_count();

        self.ensure_free_frames()?;

        loop {
            if let Some(idx) = self.translation.lookup(&key.p_key()) {
                if let Some(g) = self.try_get_write_guard(idx, true) {
                    if g.page_key() == Some(key.p_key()) {
                        g.evict_info().update();
                        return Ok(g);
                    }
                    // Frame was evicted and reused; fall through to fault.
                } else if self.translation.lookup(&key.p_key()) == Some(idx) {
                    return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                }
                continue;
            }

            // Avoid duplicate fault: another thread may have inserted already.
            if self.translation.contains_key(&key.p_key()) {
                continue;
            }

            self.used_frames.fetch_add(1, Ordering::AcqRel);
            let mut victim = match self.choose_victim() {
                Some(v) => v,
                None => {
                    self.used_frames.fetch_sub(1, Ordering::AcqRel);
                    return Err(MemPoolStatus::CannotEvictPage);
                }
            };

            debug_assert!(victim.page_key().is_none());

            self.translation
                .insert(key.p_key(), victim.frame_id() as usize);

            // If we were overwritten, we're a duplicate — release and retry.
            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            // Re-check before disk I/O.
            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            if let Err(e) = self
                .container_manager
                .get_container(key.p_key().c_key)
                .read_page(key.p_key().page_id, &mut victim)
            {
                if self.translation.lookup(&key.p_key()) == Some(victim.frame_id() as usize) {
                    self.translation.remove(&key.p_key());
                }
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                return Err(MemPoolStatus::FileManagerError(e.to_string()));
            }

            victim.set_page_key(Some(key.p_key()));
            victim.evict_info().reset();
            victim.dirty().store(true, Ordering::Release);

            // After load, we may have been overwritten — don't return a duplicate.
            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                self.write_to_disk_if_dirty_w(&victim).ok();
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            return Ok(victim);
        }
    }

    // ----- get page for read ----------------------------------------------

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        log_debug!("BP page read: {}", key);
        self.stats.inc_read_count();

        self.ensure_free_frames()?;

        loop {
            if let Some(idx) = self.translation.lookup(&key.p_key()) {
                if let Some(g) = self.try_get_read_guard(idx) {
                    if g.page_key() == Some(key.p_key()) {
                        g.evict_info().update();
                        return Ok(g);
                    }
                    // Frame was evicted and reused; fall through to fault.
                } else if self.translation.lookup(&key.p_key()) == Some(idx) {
                    return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                }
                continue;
            }

            if self.translation.contains_key(&key.p_key()) {
                continue;
            }

            self.used_frames.fetch_add(1, Ordering::AcqRel);
            let mut victim = match self.choose_victim() {
                Some(v) => v,
                None => {
                    self.used_frames.fetch_sub(1, Ordering::AcqRel);
                    return Err(MemPoolStatus::CannotEvictPage);
                }
            };

            debug_assert!(victim.page_key().is_none());

            self.translation
                .insert(key.p_key(), victim.frame_id() as usize);

            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            // Re-check before disk I/O.
            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            if let Err(e) = self
                .container_manager
                .get_container(key.p_key().c_key)
                .read_page(key.p_key().page_id, &mut victim)
            {
                if self.translation.lookup(&key.p_key()) == Some(victim.frame_id() as usize) {
                    self.translation.remove(&key.p_key());
                }
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                return Err(MemPoolStatus::FileManagerError(e.to_string()));
            }

            victim.set_page_key(Some(key.p_key()));
            victim.evict_info().reset();

            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            return Ok(victim.downgrade());
        }
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
                if self.translation.lookup(&pk) == Some(i) {
                    self.translation.remove(&pk);
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
            let meta = &unsafe { &*self.metas.get() }[i];
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
impl DashmapBP {
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
        let mut frame_to_page: HashMap<usize, PageKey> = HashMap::new();
        self.translation.map.iter().for_each(|r| {
            let (pk, &fid) = r.pair();
            frame_to_page.insert(fid, *pk);
        });
        for i in 0..self.num_frames {
            let meta = &unsafe { &*self.metas.get() }[i];
            if let Some(pk) = meta.key() {
                assert!(
                    frame_to_page.get(&i) == Some(&pk),
                    "frame {} has key {:?} but translation table disagrees",
                    i,
                    pk
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{container::ContainerManager, random::gen_random_pathname};
    use std::sync::Arc;

    fn get_test_dashmap_bp(num_frames: usize) -> Arc<DashmapBP> {
        let base_dir = gen_random_pathname(Some("test_dashmap_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(DashmapBP::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_dashmap_create_and_read() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_dashmap_bp(num_frames);
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
    fn test_dashmap_write_back() {
        let db_id = 0;
        let num_frames = 2;
        let bp = get_test_dashmap_bp(num_frames);
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
    fn test_dashmap_flush_and_reset() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_dashmap_bp(num_frames);
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
    fn test_dashmap_concurrent_latch() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_dashmap_bp(num_frames);
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
