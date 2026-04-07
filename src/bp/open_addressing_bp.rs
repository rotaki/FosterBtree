//! Open-addressing buffer pool: the frame array *is* the hash table.
//!
//! Instead of maintaining a separate page-to-frame translation table, we hash
//! the `PageKey` directly into the frame array index and use **linear probing**
//! to resolve collisions.  Each frame's metadata already stores the page key it
//! holds (`FrameMeta::key()`), so lookup is: hash → probe frames until we find
//! the matching key or an empty slot.
//!
//! ## Design
//!
//! - **Lookup:** `hash(page_key) % num_frames` gives the starting frame.  Walk
//!   forward (wrapping) checking `meta.key()` until we find the page or `None`.
//! - **Insert (page fault):** Linear probe for an empty (`key == None`) frame.
//! - **Eviction:** Clock-based.  When evicting a frame we must *not* leave a
//!   hole that breaks probe chains.  We use **backward-shift deletion**: after
//!   clearing a slot, shift subsequent entries back if they are displaced from
//!   their home slot.
//! - **Load factor:** The frame array is sized by the caller (num_frames).
//!   High load factors degrade probe length; we trigger batch eviction at 95%
//!   occupancy (same as other BPs).
//!
//! Clock eviction + linear probing, no auxiliary data structure.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    hash::hash_page_key,
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
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use rayon::iter::{IntoParallelIterator, ParallelIterator};

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------
type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

/// Fast modular reduction without division.
#[inline(always)]
fn fastmod(hash: u64, n: u64) -> usize {
    ((hash as u128 * n as u128) >> 64) as usize
}

// ---------------------------------------------------------------------------
// OpenAddressingBP
// ---------------------------------------------------------------------------

/// Buffer pool where the frame array doubles as an open-addressing hash table.
/// No separate page-to-frame mapping is maintained.
pub struct OpenAddressingBP {
    num_frames: usize,
    num_frames_u64: u64,
    used_frames: AtomicUsize,
    clock_hand: AtomicUsize,
    container_manager: Arc<ContainerManager>,
    /// The actual page data for each frame.
    #[allow(clippy::vec_box)]
    pages: UnsafeCell<Vec<Box<Page>>>,
    /// Per-frame metadata (latch, dirty bit, eviction info, page key).
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>,
    /// Runtime statistics.
    stats: BPStats,
}

// SAFETY: synchronisation is done via per-frame latches.
unsafe impl Sync for OpenAddressingBP {}
unsafe impl Send for OpenAddressingBP {}

impl Drop for OpenAddressingBP {
    fn drop(&mut self) {
        if self.container_manager.remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl OpenAddressingBP {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("OpenAddressingBP created: num_frames={}", num_frames);

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
            num_frames_u64: num_frames as u64,
            used_frames: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            container_manager,
            pages,
            metas,
            stats: BPStats::new(),
        })
    }

    pub fn eviction_stats(&self) -> String {
        "OpenAddressingBP: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "OpenAddressingBP: file stats disabled".to_string()
    }

    // ------------------------------------------------------------------
    // Hashing / probing helpers
    // ------------------------------------------------------------------

    /// Home frame index for a given page key.
    #[inline(always)]
    fn home_frame(&self, key: &PageKey) -> usize {
        fastmod(hash_page_key(key), self.num_frames_u64)
    }

    /// Advance an index by 1, wrapping around.
    #[inline(always)]
    fn next_index(&self, idx: usize) -> usize {
        let next = idx + 1;
        if next >= self.num_frames {
            0
        } else {
            next
        }
    }

    /// Distance from `home` to `current` in the probe sequence (wrapping).
    #[inline(always)]
    fn probe_distance(&self, home: usize, current: usize) -> usize {
        if current >= home {
            current - home
        } else {
            self.num_frames - home + current
        }
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

    /// Read the page key stored in frame `index` without acquiring a latch.
    /// This is safe because `FrameMeta::key()` uses atomic operations.
    #[inline]
    fn frame_key(&self, index: usize) -> Option<PageKey> {
        let metas = unsafe { &*self.metas.get() };
        metas[index].key()
    }

    // ------------------------------------------------------------------
    // Lookup: linear probe for a page
    // ------------------------------------------------------------------

    /// Search for `key` starting from its home frame.
    /// Returns `Some(frame_index)` if found, `None` if we hit a truly empty slot.
    /// Tombstoned slots are skipped (probe continues past them).
    fn probe_find(&self, key: &PageKey) -> Option<usize> {
        let start = self.home_frame(key);
        let mut idx = start;
        let metas = unsafe { &*self.metas.get() };
        for _ in 0..self.num_frames {
            if metas[idx].is_tombstone() {
                // Tombstone: probe chain continues past deleted slots.
                idx = self.next_index(idx);
                continue;
            }
            match self.frame_key(idx) {
                Some(k) if k == *key => return Some(idx),
                None => return None, // Truly empty → end of chain
                _ => idx = self.next_index(idx),
            }
        }
        None // Full table, key not found
    }

    /// Find the first empty or tombstoned slot starting from `start`.
    /// Returns `Some(frame_index)` or `None` if the table is completely full.
    fn probe_find_empty(&self, start: usize) -> Option<usize> {
        let mut idx = start;
        let metas = unsafe { &*self.metas.get() };
        for _ in 0..self.num_frames {
            if self.frame_key(idx).is_none() || metas[idx].is_tombstone() {
                return Some(idx);
            }
            idx = self.next_index(idx);
        }
        None
    }

    /// Find an empty frame slot for `key` via linear probing, acquire a write
    /// guard on it, and return the guard.  Retries if the slot gets taken by a
    /// concurrent thread between the unlocked probe and the latch acquisition.
    fn acquire_empty_frame(&self, key: &PageKey) -> Result<FWGuard, MemPoolStatus> {
        let home = self.home_frame(key);
        let mut start = home;
        for _ in 0..self.num_frames {
            if let Some(slot) = self.probe_find_empty(start) {
                if let Some(guard) = self.try_get_write_guard(slot, false) {
                    // Accept both truly empty and tombstoned slots.
                    if guard.page_key().is_none() {
                        // Clear tombstone state if present (now a clean empty slot).
                        let metas = unsafe { &*self.metas.get() };
                        if metas[slot].is_tombstone() {
                            metas[slot].set_key(None); // EMPTY, not TOMBSTONE
                        }
                        return Ok(guard);
                    }
                    // Slot was taken by a concurrent thread — continue from next.
                    drop(guard);
                }
                start = self.next_index(slot);
            } else {
                return Err(MemPoolStatus::CannotEvictPage);
            }
        }
        Err(MemPoolStatus::CannotEvictPage)
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

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

                if meta.key().is_none() || meta.is_tombstone() || meta.latch.is_locked() {
                    continue;
                }

                // Clock: if marked, clear mark and skip.
                if meta.evict_info.score() > 0 {
                    meta.evict_info.update();
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
                    // Mark as tombstone to preserve probe chains.
                    let meta = &unsafe { &*self.metas.get() }[idx];
                    meta.set_tombstone();
                    guard.evict_info().reset();
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

impl MemPool for OpenAddressingBP {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    // ----- create new page ------------------------------------------------

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        // Allocate a new page id from the container.
        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        // Find an empty slot and acquire its write guard.
        let mut guard = self.acquire_empty_frame(&page_key)?;

        debug_assert!(guard.page_key().is_none());

        // Initialise the frame.
        guard.set_id(page_id);
        guard.set_page_key(Some(page_key));
        guard.dirty().store(true, Ordering::Release);
        guard.evict_info().reset();
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        Ok(guard)
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

    // ----- page presence --------------------------------------------------

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.probe_find(&key.p_key()).is_some()
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        let mut result = Vec::new();
        for i in 0..self.num_frames {
            if let Some(pk) = self.frame_key(i) {
                if pk.c_key == c_key {
                    result.push(PageFrameKey::new_with_frame_id(
                        pk.c_key, pk.page_id, i as u32,
                    ));
                }
            }
        }
        result
    }

    // ----- get page for write ---------------------------------------------

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        log_debug!("BP page write: {}", key);
        self.stats.inc_write_count();

        self.ensure_free_frames()?;

        loop {
            // Probe for the page in the frame array.
            if let Some(idx) = self.probe_find(&key.p_key()) {
                if let Some(g) = self.try_get_write_guard(idx, true) {
                    // Validate: the frame still holds our page (no eviction race).
                    if g.page_key() == Some(key.p_key()) {
                        g.evict_info().update();
                        return Ok(g);
                    }
                    // Frame was evicted/reused under us — retry.
                    drop(g);
                    continue;
                } else {
                    // Frame exists but can't latch — someone else holds it.
                    // Check if it's still mapped to our page.
                    if self.frame_key(idx) == Some(key.p_key()) {
                        return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                    }
                    // Was evicted — retry the probe.
                    continue;
                }
            }

            // Page not in memory — page fault: read from disk into an empty frame.
            let mut guard = self.acquire_empty_frame(&key.p_key())?;

            // Another thread may have faulted this page in concurrently.
            if self.probe_find(&key.p_key()).is_some() {
                drop(guard);
                continue;
            }

            // Read page from disk.
            if let Err(e) = self
                .container_manager
                .get_container(key.p_key().c_key)
                .read_page(key.p_key().page_id, &mut guard)
            {
                return Err(MemPoolStatus::FileManagerError(e.to_string()));
            }

            guard.set_page_key(Some(key.p_key()));
            guard.evict_info().reset();
            guard.dirty().store(true, Ordering::Release);
            self.used_frames.fetch_add(1, Ordering::AcqRel);

            return Ok(guard);
        }
    }

    // ----- get page for read ----------------------------------------------

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        log_debug!("BP page read: {}", key);
        self.stats.inc_read_count();

        self.ensure_free_frames()?;

        loop {
            // Probe for the page in the frame array.
            if let Some(idx) = self.probe_find(&key.p_key()) {
                if let Some(g) = self.try_get_read_guard(idx) {
                    if g.page_key() == Some(key.p_key()) {
                        g.evict_info().update();
                        return Ok(g);
                    }
                    drop(g);
                    continue;
                } else {
                    if self.frame_key(idx) == Some(key.p_key()) {
                        return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                    }
                    continue;
                }
            }

            // Page fault: read from disk.
            let mut guard = self.acquire_empty_frame(&key.p_key())?;

            // Check if another thread faulted this page in.
            if self.probe_find(&key.p_key()).is_some() {
                drop(guard);
                continue;
            }

            if let Err(e) = self
                .container_manager
                .get_container(key.p_key().c_key)
                .read_page(key.p_key().page_id, &mut guard)
            {
                return Err(MemPoolStatus::FileManagerError(e.to_string()));
            }

            guard.set_page_key(Some(key.p_key()));
            guard.evict_info().reset();
            self.used_frames.fetch_add(1, Ordering::AcqRel);

            return Ok(guard.downgrade());
        }
    }

    // ----- prefetch -------------------------------------------------------

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
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
            frame.clear();
        });

        self.container_manager.flush_all()?;
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
impl OpenAddressingBP {
    /// # Safety
    /// Must not be called while the BP is in use by other threads.
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_probe_invariant();
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

    /// Verify that every occupied frame is reachable via linear probing from
    /// its home slot (i.e. no broken probe chains).
    unsafe fn check_probe_invariant(&self) {
        for i in 0..self.num_frames {
            if let Some(pk) = self.frame_key(i) {
                let found = self.probe_find(&pk);
                assert_eq!(
                    found,
                    Some(i),
                    "frame {} holds {:?} but probe_find returns {:?}",
                    i,
                    pk,
                    found
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

    fn get_test_open_addressing_bp(num_frames: usize) -> Arc<OpenAddressingBP> {
        let base_dir = gen_random_pathname(Some("test_oa_bp_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(OpenAddressingBP::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_oa_create_and_read() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_open_addressing_bp(num_frames);
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
    fn test_oa_write_back() {
        let db_id = 0;
        let num_frames = 2;
        let bp = get_test_open_addressing_bp(num_frames);
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
    fn test_oa_flush_and_reset() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_open_addressing_bp(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..(num_frames * 2) {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        bp.flush_all_and_reset().unwrap();

        // After reset, pages should still be loadable from disk.
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_oa_concurrent_latch() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_open_addressing_bp(num_frames);
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

    #[test]
    fn test_oa_probe_invariant_after_eviction() {
        // Create more pages than frames to trigger eviction, then verify
        // that all remaining pages are still reachable via linear probing.
        let db_id = 0;
        let num_frames = 20;
        let bp = get_test_open_addressing_bp(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        for i in 0..50u8 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i;
        }

        unsafe { bp.run_checks() };
    }
}
