//! Predictive Translation Buffer Pool
//!
//! **Unified translation** (paper/C++ style): one hash table holds all page→frame
//! mappings. Lookup = `overflow.lookup_with_bucket(key, pref)` (bucket head =
//! preferred slot, chain = overflow). Fault always inserts into overflow; evict
//! removes when this frame is still the entry for that page.
//!
//! ## Paper alignment
//!
//! - [x] **Promotion / demotion** (Sec 3.2, 5.1): after the second access to a
//!       page NOT in its preferred frame, probabilistically promote it on write
//!       (move to preferred frame if free, or swap with occupant and demote).
//!       Probabilities: 1/50 (no demotion) or 1/512 (demotion needed).
//!
//! - [x] **One-hit-wonder** (Sec 3.2): overflow access count per page; promotion
//!       only considered when count >= 2 (so first access never promotes).
//!
//! - [x] **Lock-free overflow reads** (Sec 4.2): overflow table uses version +
//!       ArcSwap per bucket; read path has no mutex so lookup can overlap with
//!       preferred-frame load.
//!
//! - [x] **Inlined chaining overflow table** (Sec 4.1): custom table with first
//!       slot inlined per bucket. Frame metadata remains in `metas` (no header
//!       in hash entry yet).
//!
//! - [x] **Single lookup**: one overflow lookup per access (bucket = preferred slot).
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
use crate::random::small_thread_rng;
use rand::RngCore;

use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::{DashMap, DashSet};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

// ---------------------------------------------------------------------------
// Type aliases (mirrors the convention in buffer_pool_clock.rs)
// ---------------------------------------------------------------------------
type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

use super::overflow_table::OverflowTable;

// ---------------------------------------------------------------------------
// Per-page fault claim (ensures only one thread faults a given page at a time)
// ---------------------------------------------------------------------------

/// Guard that removes `key` from `fault_in_progress` on drop.
struct FaultClaim(Arc<DashMap<PageKey, ()>>, PageKey);

impl Drop for FaultClaim {
    fn drop(&mut self) {
        self.0.remove(&self.1);
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
    /// Indices of frames known to be free. Set allows taking the preferred frame when free.
    free_frames: DashSet<usize>,
    /// The actual page data for each frame.
    #[allow(clippy::vec_box)]
    pages: UnsafeCell<Vec<Box<Page>>>,
    /// Per-frame metadata (latch, dirty bit, eviction info, page key).
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>,
    /// Overflow table: maps PageKey -> frame index for pages NOT in their
    /// preferred frame.  Pages in their preferred frame are found via tag check.
    overflow: OverflowTable,
    /// One-hit-wonder (§3.2): access count per page while in overflow. Used to
    /// defer promotion until at least the second access.
    overflow_access_count: Arc<DashMap<PageKey, AtomicU32>>,
    /// Pages currently being faulted; claim before faulting, release on drop.
    fault_in_progress: Arc<DashMap<PageKey, ()>>,
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

        let free_frames = DashSet::new();
        for i in 0..num_frames {
            free_frames.insert(i);
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
            free_frames,
            pages,
            metas,
            overflow: OverflowTable::new(num_frames),
            overflow_access_count: Arc::new(DashMap::new()),
            fault_in_progress: Arc::new(DashMap::new()),
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

    /// Returns true if frame `idx` has no page (key is None). Lock-free read of atomic key.
    #[inline]
    fn frame_is_free(&self, idx: usize) -> bool {
        let metas = unsafe { &*self.metas.get() };
        metas[idx].key().is_none()
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

    /// Try to get a free frame. If `preferred` is Some(p), try to take that frame first
    /// (so pages are placed in their preferred frame when free — paper §3.1).
    fn choose_victim(&self, preferred: Option<usize>) -> Option<FWGuard> {
        // Prefer the preferred frame when it's free.
        if let Some(p) = preferred {
            if self.free_frames.remove(&p).is_some() {
                if let Some(guard) = self.try_get_write_guard(p, false) {
                    if guard.page_key().is_none() {
                        return Some(guard);
                    }
                }
                self.free_frames.insert(p);
            }
        }
        // Otherwise take any free frame. Snapshot indices to avoid unbounded or
        // inconsistent iteration over the concurrent set (could hang under contention).
        let indices: Vec<usize> = self.free_frames.iter().map(|x| *x).collect();
        for idx in indices {
            if self.free_frames.remove(&idx).is_some() {
                if let Some(guard) = self.try_get_write_guard(idx, false) {
                    if guard.page_key().is_none() {
                        return Some(guard);
                    }
                }
                self.free_frames.insert(idx);
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
                    // Unified: every resident page is in overflow; remove when we evict.
                    if let Some(pk) = guard.page_key() {
                        if self.overflow.lookup(&pk) == Some(idx) {
                            self.overflow.remove(&pk);
                            self.overflow_access_count.remove(&pk);
                        }
                    }
                    // Clear the frame.
                    guard.set_page_key(None);
                    guard.evict_info().reset();
                    self.free_frames.insert(idx);
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
    /// We obtain a frame via `choose_victim(Some(pref))`, so we use the preferred
    /// frame when it's free (paper §3.1). Otherwise we take any free frame and
    /// add to the overflow table.
    fn handle_page_fault_write(
        &self,
        page_key: PageKey,
        pref: usize,
    ) -> Result<FWGuard, MemPoolStatus> {
        // Only one thread may fault a given page at a time.
        if self.fault_in_progress.insert(page_key, ()).is_some() {
            return Err(MemPoolStatus::RetryPageFault);
        }
        let _fault_claim = FaultClaim(Arc::clone(&self.fault_in_progress), page_key);

        self.used_frames.fetch_add(1, Ordering::AcqRel);

        let mut victim = match self.choose_victim(Some(pref)) {
            Some(v) => v,
            None => {
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                return Err(MemPoolStatus::CannotEvictPage);
            }
        };

        debug_assert!(victim.page_key().is_none());

        // Avoid duplicate fault: another thread may have loaded this page (unified: only overflow).
        if self.overflow.contains_key(&page_key) {
            self.free_frames.insert(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::RetryPageFault);
        }

        victim.set_page_key(Some(page_key));
        self.overflow.insert(page_key, victim.frame_id() as usize);
        if self.overflow.lookup(&page_key) != Some(victim.frame_id() as usize) {
            victim.set_page_key(None);
            self.overflow.remove(&page_key);
            self.free_frames.insert(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::RetryPageFault);
        }

        // Re-check before disk I/O.
        if self.overflow.lookup(&page_key) != Some(victim.frame_id() as usize) {
            victim.set_page_key(None);
            self.overflow.remove(&page_key);
            self.free_frames.insert(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::RetryPageFault);
        }

        // Read the page from disk.
        if let Err(e) = self
            .container_manager
            .get_container(page_key.c_key)
            .read_page(page_key.page_id, &mut victim)
        {
            victim.set_page_key(None);
            if self.overflow.lookup(&page_key) == Some(victim.frame_id() as usize) {
                self.overflow.remove(&page_key);
                self.overflow_access_count.remove(&page_key);
            }
            self.free_frames.insert(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        victim.dirty().store(true, Ordering::Release);

        if self.overflow.lookup(&page_key) != Some(victim.frame_id() as usize) {
            self.write_to_disk_if_dirty_w(&victim).ok();
            victim.set_page_key(None);
            if self.overflow.lookup(&page_key) == Some(victim.frame_id() as usize) {
                self.overflow.remove(&page_key);
                self.overflow_access_count.remove(&page_key);
            }
            self.free_frames.insert(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::RetryPageFault);
        }

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

    // ------------------------------------------------------------------
    // Promotion / demotion (§3.2, 5.1) with one-hit-wonder
    // ------------------------------------------------------------------

    /// Paper probabilities: 1/50 when preferred frame is free, 1/512 when demotion needed.
    const PROMOTE_PROB_NO_DEMOTE: u32 = 50;
    const PROMOTE_PROB_DEMOTE: u32 = 512;

    /// Try to promote the page from its current (overflow) frame to its preferred frame.
    /// Caller holds a write guard on the overflow frame. Returns either the same guard
    /// (no promotion / failed) or a new write guard on the preferred frame (promotion done).
    fn try_promote_to_preferred(
        &self,
        mut current_guard: FWGuard,
        page_key: PageKey,
        pref: usize,
    ) -> Result<FWGuard, MemPoolStatus> {
        let current_idx = current_guard.frame_id() as usize;
        if current_idx == pref {
            return Ok(current_guard);
        }

        let mut pref_guard = match self.try_get_write_guard(pref, false) {
            Some(g) => g,
            None => return Ok(current_guard),
        };

        if pref_guard.page_key().is_none() {
            // Simple promotion: preferred frame is free. Move our page there.
            pref_guard.page_mut().copy(current_guard.page());
            pref_guard.set_page_key(Some(page_key));
            pref_guard
                .dirty()
                .store(current_guard.dirty().load(Ordering::Acquire), Ordering::Release);
            pref_guard.evict_info().update();

            current_guard.clear();
            self.overflow.remove(&page_key);
            self.overflow.insert(page_key, pref); // Unified: preferred slot still in table
            self.overflow_access_count.remove(&page_key);
            self.free_frames.insert(current_idx);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);

            drop(current_guard);
            return Ok(pref_guard);
        }

        // Demotion: preferred frame holds another page. Swap contents.
        let other_key = pref_guard.page_key().unwrap();
        let mut temp = Page::new_empty();
        temp.copy(current_guard.page());
        current_guard.page_mut().copy(pref_guard.page());
        pref_guard.page_mut().copy(&temp);

        pref_guard.set_page_key(Some(page_key));
        current_guard.set_page_key(Some(other_key));
        let other_dirty = pref_guard.dirty().load(Ordering::Acquire);
        pref_guard
            .dirty()
            .store(current_guard.dirty().load(Ordering::Acquire), Ordering::Release);
        current_guard.dirty().store(other_dirty, Ordering::Release);
        pref_guard.evict_info().update();
        current_guard.evict_info().update();

        self.overflow.remove(&page_key);
        self.overflow.insert(other_key, current_idx);
        self.overflow_access_count.remove(&page_key);

        drop(current_guard);
        Ok(pref_guard)
    }

    /// Roll for promotion: true with probability 1/denom. Uses next_u32() % denom == 0.
    #[inline]
    fn promote_roll(denom: u32) -> bool {
        let mut rng = small_thread_rng();
        rng.next_u32() % denom == 0
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

        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);
        let preferred = self.preferred_frame(&page_key);
        let mut victim = self
            .choose_victim(Some(preferred))
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        // Unified: every resident page is in the overflow table.
        self.overflow.insert(page_key, victim.frame_id() as usize);

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
        self.overflow.contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.overflow.get_page_keys(c_key)
    }

    // ----- get page for write ---------------------------------------------

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        log_debug!("PT page write: {}", key);
        self.stats.inc_write_count();

        self.ensure_free_frames()?;

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        loop {
            // Unified translation: single lookup (paper/C++ style).
            let frame_idx = self.overflow.lookup_with_bucket(&page_key, pref);

            if let Some(idx) = frame_idx {
                if let Some(g) = self.try_get_write_guard(idx, true) {
                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        // One-hit-wonder (§3.2): only consider promotion after second access
                        let prev = self
                            .overflow_access_count
                            .entry(page_key)
                            .or_insert_with(|| AtomicU32::new(0))
                            .fetch_add(1, Ordering::Relaxed);
                        if prev >= 1 {
                            let pref_free = self.frame_is_free(pref);
                            let denom = if pref_free {
                                Self::PROMOTE_PROB_NO_DEMOTE
                            } else {
                                Self::PROMOTE_PROB_DEMOTE
                            };
                            if Self::promote_roll(denom) {
                                let guard =
                                    self.try_promote_to_preferred(g, page_key, pref)?;
                                return Ok(guard);
                            }
                        }
                        return Ok(g);
                    }
                } else if self.overflow.lookup_with_bucket(&page_key, pref) == Some(idx) {
                    return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                }
                continue;
            }

            match self.handle_page_fault_write(page_key, pref) {
                Ok(g) => return Ok(g),
                Err(MemPoolStatus::RetryPageFault) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    // ----- get page for read ----------------------------------------------

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        log_debug!("PT page read: {}", key);
        self.stats.inc_read_count();

        self.ensure_free_frames()?;

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        loop {
            // Unified translation: single lookup (paper/C++ style).
            let frame_idx = self.overflow.lookup_with_bucket(&page_key, pref);

            if let Some(idx) = frame_idx {
                if let Some(g) = self.try_get_read_guard(idx) {
                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        // One-hit-wonder: count overflow accesses (promotion only on write path)
                        self.overflow_access_count
                            .entry(page_key)
                            .or_insert_with(|| AtomicU32::new(0))
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok(g);
                    }
                } else if self.overflow.lookup_with_bucket(&page_key, pref) == Some(idx) {
                    return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                }
                continue;
            }

            match self.handle_page_fault_write(page_key, pref) {
                Ok(victim) => return Ok(victim.downgrade()),
                Err(MemPoolStatus::RetryPageFault) => continue,
                Err(e) => return Err(e),
            }
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
                if self.overflow.lookup(&pk) == Some(i) {
                    self.overflow.remove(&pk);
                    self.overflow_access_count.remove(&pk);
                }
            }
            frame.clear();
        });

        self.container_manager.flush_all()?;

        // Repopulate the free set.
        self.free_frames.clear();
        for i in 0..self.num_frames {
            self.free_frames.insert(i);
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
        // Unified: every resident page is in overflow. Check overflow matches frames.
        let mut overflow_frame_to_page: HashMap<usize, PageKey> = HashMap::new();
        self.overflow.for_each_entry(|pk, fid| {
            overflow_frame_to_page.insert(fid, pk);
        });
        for i in 0..self.num_frames {
            let meta = &(*self.metas.get())[i];
            if let Some(pk) = meta.key() {
                assert!(
                    overflow_frame_to_page.get(&i) == Some(&pk),
                    "frame {} has key {:?} but overflow table disagrees",
                    i, pk
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
