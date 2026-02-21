//! Hashmap buffer pool: single RwLock around the translation table (paper baseline).
//!
//! Same structure and eviction as DashmapBP, but translation is
//! `RwLock<HashMap<PageKey, usize>>` so every lookup/insert/remove acquires the lock.
//! Paper-style "traditional" baseline (one latch on the table). Use DashmapBP for
//! a concurrent (sharded) baseline.

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
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use concurrent_queue::ConcurrentQueue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

// ---------------------------------------------------------------------------
// Translation: single RwLock<HashMap<...>> (paper "traditional" baseline)
// ---------------------------------------------------------------------------
struct TranslationTable {
    map: RwLock<HashMap<PageKey, usize>>,
}

impl TranslationTable {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }

    #[inline]
    fn lookup(&self, key: &PageKey) -> Option<usize> {
        self.map.read().unwrap().get(key).copied()
    }

    #[inline]
    fn insert(&self, key: PageKey, frame_id: usize) {
        self.map.write().unwrap().insert(key, frame_id);
    }

    #[inline]
    fn remove(&self, key: &PageKey) -> Option<usize> {
        self.map.write().unwrap().remove(key)
    }

    #[inline]
    fn contains_key(&self, key: &PageKey) -> bool {
        self.map.read().unwrap().contains_key(key)
    }

    fn get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.map
            .read()
            .unwrap()
            .iter()
            .filter(|(pk, _)| pk.c_key == c_key)
            .map(|(pk, &frame_id)| {
                PageFrameKey::new_with_frame_id(pk.c_key, pk.page_id, frame_id as u32)
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// HashmapBP
// ---------------------------------------------------------------------------
pub struct HashmapBP {
    num_frames: usize,
    used_frames: AtomicUsize,
    clock_hand: AtomicUsize,
    container_manager: Arc<ContainerManager>,
    free_list: ConcurrentQueue<usize>,
    #[allow(clippy::vec_box)]
    pages: UnsafeCell<Vec<Box<Page>>>,
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>,
    translation: TranslationTable,
    stats: BPStats,
}

unsafe impl Sync for HashmapBP {}
unsafe impl Send for HashmapBP {}

impl Drop for HashmapBP {
    fn drop(&mut self) {
        if self.container_manager.remove_dir_on_drop() {
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl HashmapBP {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("HashmapBP (hashmap/single RwLock) created: num_frames={}", num_frames);

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
        "HashmapBP: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "HashmapBP: file stats disabled".to_string()
    }

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

    fn choose_victim(&self) -> Option<FWGuard> {
        while let Ok(idx) = self.free_list.pop() {
            if let Some(guard) = self.try_get_write_guard(idx, false) {
                if guard.page_key().is_none() {
                    return Some(guard);
                }
            } else {
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
                let meta = &mut unsafe { &mut *self.metas.get() }[idx];

                if meta.key().is_none() || meta.latch.is_locked() {
                    continue;
                }

                if meta.evict_info.score() > 0 {
                    meta.evict_info.update();
                    meta.evict_info.reset();
                    continue;
                }

                if let Some(guard) = self.try_get_write_guard(idx, false) {
                    if guard.page_key().is_none() {
                        continue;
                    }
                    self.write_to_disk_if_dirty_w(&guard).unwrap();
                    if let Some(pk) = guard.page_key() {
                        if self.translation.lookup(&pk) == Some(idx) {
                            self.translation.remove(&pk);
                        }
                    }
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

impl MemPool for HashmapBP {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let mut victim = self
            .choose_victim()
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        self.translation.insert(page_key, victim.frame_id() as usize);

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

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.translation.contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.translation.get_page_keys(c_key)
    }

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
                } else if self.translation.lookup(&key.p_key()) == Some(idx) {
                    return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
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

            self.translation.insert(key.p_key(), victim.frame_id() as usize);

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

            if let Err(e) = self.container_manager.get_container(key.p_key().c_key)
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

            self.translation.insert(key.p_key(), victim.frame_id() as usize);

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

            if self.translation.lookup(&key.p_key()) != Some(victim.frame_id() as usize) {
                victim.set_page_key(None);
                self.free_list.push(victim.frame_id() as usize).ok();
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            if let Err(e) = self.container_manager.get_container(key.p_key().c_key)
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
                if self.translation.lookup(&pk) == Some(i) {
                    self.translation.remove(&pk);
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
            .fold((0, 0, 0), |acc, (_, (c, r, w))| (acc.0 + c, acc.1 + r, acc.2 + w));

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
