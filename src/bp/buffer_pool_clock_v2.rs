//! Shadow re-implementation of `BufferPoolClock` (LIPAH) on top of
//! `FrameManager`.
//!
//! **Status: shadow** — coexists with the original `BufferPoolClock`. Intended
//! to be behaviorally identical; use for A/B testing and migration verification.
//! After PT and TLB-BP have also been migrated and parity is confirmed, the
//! original `BufferPoolClock` can be deleted and this renamed.
//!
//! Owns: `PageToFrame` (the translation table). Delegates everything else to
//! `FrameManager`.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    buffer_pool_clock::PageToFrame,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameReadGuard, FrameWriteGuard},
    frame_manager::FrameManager,
    macro_profile::{scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};
use crate::{container::ContainerManager, log_debug, page::PageId};

#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
use std::sync::atomic::AtomicU64;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

use dashmap::{mapref::entry, Entry};

type EvictionPolicyImpl = ClockEvictionPolicy;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

/// Counters to mirror PT's `fast_path_coverage` on LIPAH (`bp_clock_v2`).
/// Gated by the existing `pt_counts` feature so they add zero overhead by
/// default. See plan: PT strength/weakness study, Part B1.
#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
struct LipahCoverage {
    /// Reads + writes where the caller-supplied `frame_id` hint hit.
    pub hint_hits: AtomicU64,
    /// Reads + writes that fell through to the DashMap translation.
    pub hint_misses: AtomicU64,
    /// Pages evicted — used as a "residency churn" counter to match PT's
    /// `residency_evictions_from_preferred` concept (LIPAH has no preferred
    /// frame, so this is just total evictions).
    pub evictions: AtomicU64,
}

#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
impl LipahCoverage {
    fn new() -> Self {
        Self {
            hint_hits: AtomicU64::new(0),
            hint_misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

pub struct BufferPoolClockV2<const EVICTION_BATCH_SIZE: usize> {
    fm: FrameManager<EvictionPolicyImpl>,
    page_to_frame: PageToFrame,
    stats: BPStats,
    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    coverage: LipahCoverage,
}

impl<const EVICTION_BATCH_SIZE: usize> Drop for BufferPoolClockV2<EVICTION_BATCH_SIZE> {
    fn drop(&mut self) {
        if self.fm.container_manager().remove_dir_on_drop() {
            // ContainerManager cleans up its temp dir.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl<const EVICTION_BATCH_SIZE: usize> BufferPoolClockV2<EVICTION_BATCH_SIZE> {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        if num_frames < EVICTION_BATCH_SIZE {
            panic!("Number of frames must be greater than the eviction batch size");
        }
        Ok(Self {
            fm: FrameManager::new(num_frames, container_manager)?,
            page_to_frame: PageToFrame::new(),
            stats: BPStats::new(),
            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
            coverage: LipahCoverage::new(),
        })
    }

    /// Translator hook for eviction: drop mapping only if it still points at
    /// the frame being evicted (concurrent remap by another thread is possible
    /// in principle — this check keeps us safe).
    fn on_evict(&self, pk: &PageKey, idx: u32) {
        // Per-page lookup-then-remove. The original LIPAH batches by c_key
        // across a whole eviction batch (remove_batch_sorted); we trade that
        // minor optimization for the cleaner FrameManager hook API.
        let cmap = self.page_to_frame.get_cmap(&pk.c_key);
        let matches = cmap
            .get(&pk.page_id)
            .map(|e| *e == idx as usize)
            .unwrap_or(false);
        if matches {
            cmap.remove(&pk.page_id);
        }
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.coverage.evictions.fetch_add(1, Ordering::Relaxed);
    }
}

impl<const EVICTION_BATCH_SIZE: usize> MemPool for BufferPoolClockV2<EVICTION_BATCH_SIZE> {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        unimplemented!("Create container is not implemented");
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        unimplemented!("Drop container is not implemented");
    }

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::CreateNewPage);
        self.stats.inc_new_page();

        self.fm
            .ensure_free_frames(EVICTION_BATCH_SIZE, |pk, idx| self.on_evict(pk, idx))?;

        let mut victim = self
            .fm
            .choose_victim()
            .ok_or(MemPoolStatus::CannotEvictPage)?;
        assert!(victim.page_key().is_none());
        assert!(!victim.dirty().load(Ordering::Acquire));

        let container = self.fm.container_manager().get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);
        self.page_to_frame
            .insert(page_key, victim.frame_id() as usize);

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().update();
        self.fm.increment_used();

        Ok(victim)
    }

    fn create_new_pages_for_write(
        &self,
        _c_key: ContainerKey,
        _num_pages: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        unimplemented!("Create new pages for write is not implemented");
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        let frame_id = key.frame_id();
        if (frame_id as usize) < self.fm.num_frames()
            && self.fm.meta(frame_id).key() == Some(key.p_key())
        {
            return true;
        }
        self.page_to_frame.contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.page_to_frame.get_page_keys(c_key)
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageWrite);
        log_debug!("Page write: {}", key);
        self.stats.inc_write_count();

        // Fast path: use hint from caller.
        #[cfg(not(feature = "no_bp_hint"))]
        {
            let frame_id = key.frame_id();
            if (frame_id as usize) < self.fm.num_frames()
                && self.fm.meta(frame_id).key() == Some(key.p_key())
            {
                if let Some(g) = self.fm.try_get_write_guard(frame_id, false) {
                    if g.page_key() == Some(key.p_key()) {
                        g.evict_info().update();
                        g.dirty().store(true, Ordering::Release);
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.coverage.hint_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(g);
                    }
                }
            }
        }

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.coverage.hint_misses.fetch_add(1, Ordering::Relaxed);

        // Slow path.
        self.fm
            .ensure_free_frames(EVICTION_BATCH_SIZE, |pk, idx| self.on_evict(pk, idx))?;

        let cmap = self.page_to_frame.get_cmap(&key.p_key().c_key);
        let mut victim = match cmap.entry(key.p_key().page_id) {
            Entry::Occupied(entry) => {
                let guard = self.fm.try_get_write_guard(*entry.get() as u32, true);
                return guard
                    .inspect(|g| {
                        g.evict_info().update();
                    })
                    .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
            }
            Entry::Vacant(entry) => {
                self.fm.increment_used();
                let victim = self
                    .fm
                    .choose_victim()
                    .ok_or(MemPoolStatus::CannotEvictPage)?;
                entry.insert(victim.frame_id() as usize);
                victim
            }
        };
        assert!(victim.page_key().is_none());
        assert!(!victim.dirty().load(Ordering::Acquire));

        let container = self.fm.container_manager().get_container(key.p_key().c_key);
        container
            .read_page(key.p_key().page_id, &mut victim)
            .map(|()| {
                victim.set_page_key(Some(key.p_key()));
                victim.evict_info().update();
            })?;
        victim.dirty().store(true, Ordering::Release);
        Ok(victim)
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        log_debug!("Page read: {}", key);
        self.stats.inc_read_count();

        // Fast path: use hint from caller.
        #[cfg(not(feature = "no_bp_hint"))]
        {
            let frame_id = key.frame_id();
            if (frame_id as usize) < self.fm.num_frames()
                && self.fm.meta(frame_id).key() == Some(key.p_key())
            {
                if let Some(g) = self.fm.try_get_read_guard(frame_id) {
                    if g.page_key() == Some(key.p_key()) {
                        g.evict_info().update();
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.coverage.hint_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(g);
                    }
                }
            }
        }

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.coverage.hint_misses.fetch_add(1, Ordering::Relaxed);

        // Slow path.
        self.fm
            .ensure_free_frames(EVICTION_BATCH_SIZE, |pk, idx| self.on_evict(pk, idx))?;

        let cmap = self.page_to_frame.get_cmap(&key.p_key().c_key);
        let mut victim = match cmap.entry(key.p_key().page_id) {
            entry::Entry::Occupied(entry) => {
                let guard = self.fm.try_get_read_guard(*entry.get() as u32);
                return guard
                    .inspect(|g| {
                        g.evict_info().update();
                    })
                    .ok_or(MemPoolStatus::FrameReadLatchGrantFailed);
            }
            entry::Entry::Vacant(entry) => {
                self.fm.increment_used();
                let victim = self
                    .fm
                    .choose_victim()
                    .ok_or(MemPoolStatus::CannotEvictPage)?;
                entry.insert(victim.frame_id() as usize);
                victim
            }
        };
        assert!(victim.page_key().is_none());
        assert!(!victim.dirty().load(Ordering::Acquire));

        let container = self.fm.container_manager().get_container(key.p_key().c_key);
        container
            .read_page(key.p_key().page_id, &mut victim)
            .map(|()| {
                victim.set_page_key(Some(key.p_key()));
                victim.evict_info().update();
            })?;
        Ok(victim.downgrade())
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.fm.flush_all()
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        // Translator hook: clear all mappings as frames are swept.
        self.fm.flush_all_and_reset(|pk, _idx| {
            let cmap = self.page_to_frame.get_cmap(&pk.c_key);
            cmap.remove(&pk.page_id);
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
        let read_count_waiting_for_write = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();

        let mut num_frames_per_container = BTreeMap::new();
        for i in 0..self.fm.num_frames() {
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
        let (total_created, total_disk_read, total_disk_write) = disk_io_per_container
            .iter()
            .fold((0, 0, 0), |acc, (_, (created, read, write))| {
                (acc.0 + created, acc.1 + read, acc.2 + write)
            });

        MemoryStats {
            bp_num_frames_in_mem: self.fm.num_frames(),
            bp_new_page: new_page,
            bp_read_frame: read_count,
            bp_read_frame_wait: read_count_waiting_for_write,
            bp_write_frame: write_count,
            bp_num_frames_per_container: num_frames_per_container,
            disk_created: total_created as usize,
            disk_read: total_disk_read as usize,
            disk_write: total_disk_write as usize,
            disk_io_per_container,
        }
    }

    unsafe fn reset_stats(&self) {
        self.stats.clear();
    }

    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    fn sample_coverage(&self) -> (u64, u64) {
        let hits = self.coverage.hint_hits.load(Ordering::Relaxed);
        let misses = self.coverage.hint_misses.load(Ordering::Relaxed);
        (hits, hits + misses)
    }

    fn print_profile(&self) {
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        {
            let hits = self.coverage.hint_hits.load(Ordering::Relaxed);
            let misses = self.coverage.hint_misses.load(Ordering::Relaxed);
            let total = hits + misses;
            let cov = if total == 0 {
                0.0
            } else {
                hits as f64 / total as f64
            };
            println!("\n=== LIPAH-V2 Coverage ===");
            println!(
                "Hint hits:   {:>12}  ({:.1}%)",
                hits,
                hits as f64 / total.max(1) as f64 * 100.0
            );
            println!(
                "Hint misses: {:>12}  ({:.1}%)",
                misses,
                misses as f64 / total.max(1) as f64 * 100.0
            );
            println!(
                "Evictions:   {:>12}",
                self.coverage.evictions.load(Ordering::Relaxed)
            );
            println!(
                "fast_path_coverage: {:.4}  (hits={}, total={})",
                cov, hits, total
            );
        }
    }
}

#[cfg(test)]
impl<const EVICTION_BATCH_SIZE: usize> BufferPoolClockV2<EVICTION_BATCH_SIZE> {
    /// # Safety
    /// Caller must ensure no other thread is touching the buffer pool.
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_page_to_frame();
        self.check_frame_id_and_page_id_match();
    }

    unsafe fn check_all_frames_unlatched(&self) {
        for i in 0..self.fm.num_frames() {
            self.fm.try_get_write_guard(i as u32, false).unwrap();
        }
    }

    unsafe fn check_page_to_frame(&self) {
        use std::collections::HashMap;
        let mut frame_to_page = HashMap::new();
        for (c, k, v) in self.page_to_frame.iter() {
            frame_to_page.insert(v, PageKey::new(c, k));
        }
        for i in 0..self.fm.num_frames() {
            let frame = loop {
                if let Some(g) = self.fm.try_get_read_guard(i as u32) {
                    break g;
                }
                std::hint::spin_loop();
            };
            if let Some(pk) = frame_to_page.get(&i) {
                assert_eq!(frame.page_key().unwrap(), *pk);
            } else {
                assert_eq!(frame.page_key(), None);
            }
        }
    }

    unsafe fn check_frame_id_and_page_id_match(&self) {
        for i in 0..self.fm.num_frames() {
            let frame = loop {
                if let Some(g) = self.fm.try_get_read_guard(i as u32) {
                    break g;
                }
                std::hint::spin_loop();
            };
            if let Some(key) = frame.page_key() {
                assert_eq!(key.page_id, frame.get_id());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_test_bp_clock_v2;

    #[test]
    fn test_create_read_write() {
        let bp = get_test_bp_clock_v2::<1>(4);
        let c_key = ContainerKey::new(0, 0);

        let mut g = bp.create_new_page_for_write(c_key).unwrap();
        g[0] = 42;
        let pk = g.page_frame_key().unwrap();
        drop(g);

        let r = bp.get_page_for_read(pk).unwrap();
        assert_eq!(r[0], 42);
    }

    #[test]
    fn test_eviction_and_refault() {
        // 2 frames, 5 pages → force eviction + disk I/O
        let bp = get_test_bp_clock_v2::<1>(2);
        let c_key = ContainerKey::new(0, 0);
        let mut keys = Vec::new();
        for i in 0..5u8 {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i;
            keys.push(g.page_frame_key().unwrap());
        }
        for (i, k) in keys.iter().enumerate() {
            let r = bp.get_page_for_read(*k).unwrap();
            assert_eq!(r[0], i as u8);
        }
    }

    #[test]
    fn test_flush_and_reset() {
        let bp = get_test_bp_clock_v2::<1>(3);
        let c_key = ContainerKey::new(0, 0);
        let mut keys = Vec::new();
        for i in 0..6 {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i as u8;
            keys.push(g.page_frame_key().unwrap());
        }
        bp.flush_all_and_reset().unwrap();
        for (i, k) in keys.iter().enumerate() {
            let r = bp.get_page_for_read(*k).unwrap();
            assert_eq!(r[0], i as u8);
        }
    }

    #[test]
    fn test_concurrent_write() {
        use std::sync::Arc;
        let bp = get_test_bp_clock_v2::<1>(10);
        let c_key = ContainerKey::new(0, 0);
        let mut g = bp.create_new_page_for_write(c_key).unwrap();
        g[0] = 0;
        let pk = g.page_frame_key().unwrap();
        drop(g);

        let bp = Arc::new(bp);
        let threads: Vec<_> = (0..3)
            .map(|_| {
                let bp = bp.clone();
                std::thread::spawn(move || {
                    for _ in 0..50u8 {
                        loop {
                            if let Ok(mut g) = bp.get_page_for_write(pk) {
                                g[0] = g[0].wrapping_add(1);
                                break;
                            }
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let r = bp.get_page_for_read(pk).unwrap();
        assert_eq!(r[0], 150u8);
    }

    #[test]
    fn test_v2_and_frame_latch() {
        use std::thread;
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_bp_clock_v2::<2>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);
        let frame = bp.create_new_page_for_write(c_key).unwrap();
        let key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iterations = 80;
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        loop {
                            if let Ok(mut guard) = bp.get_page_for_write(key) {
                                guard[0] += 1;
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                });
            }
        });
        unsafe {
            bp.run_checks();
        }
        assert!(bp.is_in_mem(key));
        let guard = bp.get_page_for_read(key).unwrap();
        assert_eq!(guard[0], num_threads * num_iterations);
        drop(guard);
        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_v2_create_new_page_with_checks() {
        let db_id = 0;
        let num_frames = 2;
        let bp = get_test_bp_clock_v2::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let num_traversal = 100;
        let mut count = 0;
        let mut keys = Vec::new();

        for _ in 0..num_traversal {
            let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
            guard1[0] = count;
            count += 1;
            keys.push(guard1.page_frame_key().unwrap());

            let mut guard2 = bp.create_new_page_for_write(c_key).unwrap();
            guard2[0] = count;
            count += 1;
            keys.push(guard2.page_frame_key().unwrap());
        }

        unsafe {
            bp.run_checks();
        }

        for i in 0..num_traversal {
            let g1 = bp.get_page_for_read(keys[i * 2]).unwrap();
            assert_eq!(g1[0], i as u8 * 2);
            let g2 = bp.get_page_for_read(keys[i * 2 + 1]).unwrap();
            assert_eq!(g2[0], i as u8 * 2 + 1);
        }

        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_v2_all_frames_latched() {
        let db_id = 0;
        let num_frames = 1;
        let bp = get_test_bp_clock_v2::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut g1 = bp.create_new_page_for_write(c_key).unwrap();
        g1[0] = 1;

        // Single frame is latched → must fail.
        let res = bp.create_new_page_for_write(c_key);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(g1);
        let g2 = bp.create_new_page_for_write(c_key).unwrap();
        drop(g2);
    }

    #[test]
    fn test_v2_clear_frames() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_bp_clock_v2::<2>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 2 {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i as u8;
            keys.push(g.page_frame_key().unwrap());
        }
        unsafe {
            bp.run_checks();
        }

        bp.flush_all_and_reset().unwrap();
        unsafe {
            bp.run_checks();
        }

        for (i, key) in keys.iter().enumerate() {
            let g = bp.get_page_for_read(*key).unwrap();
            assert_eq!(g[0], i as u8);
        }
        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_v2_clear_frames_durable() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        let num_frames = 10;
        let mut keys = Vec::new();

        {
            let cm =
                Arc::new(crate::container::ContainerManager::new(&temp_dir, false, false).unwrap());
            let bp1 = BufferPoolClockV2::<2>::new(num_frames, cm).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..num_frames * 10 {
                let mut g = bp1.create_new_page_for_write(c_key).unwrap();
                g[0] = i as u8;
                keys.push(g.page_frame_key().unwrap());
            }
            unsafe {
                bp1.run_checks();
            }

            bp1.flush_all_and_reset().unwrap();
            unsafe {
                bp1.run_checks();
            }
        }

        {
            let cm =
                Arc::new(crate::container::ContainerManager::new(&temp_dir, false, false).unwrap());
            let bp2 = BufferPoolClockV2::<2>::new(num_frames, cm).unwrap();
            for (i, key) in keys.iter().enumerate() {
                let g = bp2.get_page_for_read(*key).unwrap();
                assert_eq!(g[0], i as u8);
            }
            unsafe {
                bp2.run_checks();
            }
        }
    }

    #[test]
    fn test_v2_stats() {
        // Mirrors the original test_bpc_stats: exercises the stats API without
        // asserting concrete counts (counters are gated behind feature = "stat").
        let db_id = 0;
        let num_frames = 1;
        let bp = get_test_bp_clock_v2::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let key_1 = {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = 1;
            g.page_frame_key().unwrap()
        };
        let _ = unsafe { bp.stats() };

        let key_2 = {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = 2;
            g.page_frame_key().unwrap()
        };
        let _ = unsafe { bp.stats() };

        {
            let g = bp.get_page_for_read(key_1).unwrap();
            assert_eq!(g[0], 1);
        }
        let _ = unsafe { bp.stats() };

        {
            let g = bp.get_page_for_read(key_2).unwrap();
            assert_eq!(g[0], 2);
        }
        let _ = unsafe { bp.stats() };
    }
}
