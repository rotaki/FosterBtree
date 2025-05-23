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
    log_debug, log_error, log_warn,
    page::{Page, PageId},
};

use std::{
    cell::{RefCell, UnsafeCell},
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};

type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

use concurrent_queue::ConcurrentQueue;
use dashmap::{mapref::entry, DashMap};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub struct PageToFrame {
    map: DashMap<ContainerKey, Arc<DashMap<PageId, usize>>>, // (c_key, page_id) -> frame_index
}

impl PageToFrame {
    pub fn new() -> Self {
        PageToFrame {
            map: DashMap::new(),
        }
    }

    pub fn get_cmap(&self, c_key: &ContainerKey) -> Arc<DashMap<PageId, usize>> {
        match self.map.entry(*c_key) {
            dashmap::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::Entry::Vacant(entry) => {
                let cmap = Arc::new(DashMap::new());
                entry.insert(cmap.clone());
                cmap
            }
        }
    }

    pub fn get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        let cmap = self.get_cmap(&c_key);
        cmap.iter()
            .map(|r| {
                let (page_id, frame_index) = r.pair();
                PageFrameKey::new_with_frame_id(c_key, *page_id, *frame_index as u32)
            })
            .collect::<Vec<_>>()
    }

    pub fn contains_key(&self, p_key: &PageKey) -> bool {
        let cmap = self.get_cmap(&p_key.c_key);
        cmap.contains_key(&p_key.page_id)
    }

    pub fn insert(&self, p_key: PageKey, frame_id: usize) {
        let cmap = self.get_cmap(&p_key.c_key);
        cmap.insert(p_key.page_id, frame_id);
    }

    pub fn remove(&self, p_key: &PageKey) -> Option<usize> {
        let cmap = self.get_cmap(&p_key.c_key);
        cmap.remove(&p_key.page_id).map(|(_p_id, f_id)| f_id)
    }

    // The p_keys must be sorted by c_key
    pub fn remove_batch_sorted<T: IntoIterator<Item = PageKey>>(&self, p_keys: T) {
        // Remove it after grouping by c_key
        let mut last_c_key = None;
        let mut page_map: Option<Arc<DashMap<PageId, usize>>> = None;
        for p_key in p_keys {
            if last_c_key == Some(p_key.c_key) {
                // Last c_key is the same as current c_key. page_map is already set.
                page_map.as_ref().unwrap().remove(&p_key.page_id);
            } else {
                // Last c_key is different from current c_key. Set the page_map to the new c_key.
                page_map = Some(self.get_cmap(&p_key.c_key));
                last_c_key = Some(p_key.c_key);
                // Remove the old mapping
                page_map.as_ref().unwrap().remove(&p_key.page_id);
            }
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.map.clear();
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> impl IntoIterator<Item = (ContainerKey, PageId, usize)> + '_ {
        // self.map.iter().flat_map(|(c_key, page_map)| {
        self.map.iter().flat_map(|r| {
            let (c_key, page_map) = r.pair();
            page_map
                .iter()
                // .map(move |(page_id, frame_index)| (c_key, page_id, frame_index))
                .map(move |rr| {
                    let (page_id, frame_index) = rr.pair();
                    (*c_key, *page_id, *frame_index)
                })
                .collect::<Vec<_>>()
        })
    }

    #[allow(dead_code)]
    pub fn iter_container(
        &self,
        c_key: ContainerKey,
    ) -> impl IntoIterator<Item = (PageId, usize)> + '_ {
        self.map.get(&c_key).into_iter().flat_map(|page_map| {
            page_map
                .iter()
                .map(|r| {
                    let (page_id, frame_index) = r.pair();
                    (*page_id, *frame_index)
                })
                .collect::<Vec<_>>()
        })
    }
}

/// Your scratch buffers
struct EvictionScratchSpace {
    pub clean_pages: Vec<(usize, *mut FMeta)>,
    pub dirty_pages: Vec<(usize, FRGuard)>,
    pub to_evict: Vec<(usize, FWGuard)>,
}

impl EvictionScratchSpace {
    pub fn new(batch: usize) -> Self {
        Self {
            dirty_pages: Vec::with_capacity(batch),
            clean_pages: Vec::with_capacity(batch),
            to_evict: Vec::with_capacity(batch),
        }
    }

    /// wipe but keep allocations
    pub fn clear(&mut self) {
        self.dirty_pages.clear();
        self.clean_pages.clear();
        self.to_evict.clear();
    }
}

thread_local! {
    // One slot *per thread*, but initialised lazily
    static SCRATCH: OnceLock<RefCell<EvictionScratchSpace>> = const { OnceLock::new() };
}

fn with_eviction_scratch<F, R>(batch: usize, f: F) -> R
where
    F: FnOnce(&mut EvictionScratchSpace) -> R,
{
    SCRATCH.with(|slot| {
        // First access for this thread?
        let cell = slot.get_or_init(|| RefCell::new(EvictionScratchSpace::new(batch)));

        let mut borrow = cell.borrow_mut();

        // Already created, but maybe caller asked for a larger batch?
        borrow.clear(); // start fresh each time (optional)

        f(&mut borrow) // hand it to the caller
    })
}

/// Buffer pool that manages the buffer frames.
pub struct BufferPoolClock<const EVICTION_BATCH_SIZE: usize> {
    num_frames: usize,
    used_frames: AtomicUsize,
    clock_hand: AtomicUsize,
    container_manager: Arc<ContainerManager>,
    eviction_hints: ConcurrentQueue<usize>, // A hint for quickly finding a clean frame or a frame to evict. Whenever a clean frame is found, it is pushed to this queue so that it can be quickly found.
    #[allow(clippy::vec_box)]
    pages: UnsafeCell<Vec<Box<Page>>>, // Boxed to be able to use box::as_mut_ptr to have multiple mutable references to the same object
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>, // Boxed to be able to use box::as_mut_ptr to have multiple mutable references to the same object
    page_to_frame: PageToFrame, // (c_key, page_id) -> frame_index
    stats: BPStats,
}

impl<const EVICTION_BATCH_SIZE: usize> Drop for BufferPoolClock<EVICTION_BATCH_SIZE> {
    fn drop(&mut self) {
        if self.container_manager.remove_dir_on_drop() {
            // Do nothing. Directory will be removed when the container manager is dropped.
        } else {
            // Persist all the pages to disk
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl<const EVICTION_BATCH_SIZE: usize> BufferPoolClock<EVICTION_BATCH_SIZE> {
    /// Create a new buffer pool with the given number of frames.
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        if num_frames < EVICTION_BATCH_SIZE {
            panic!("Number of frames must be greater than the eviction batch size");
        }
        log_debug!("Buffer pool created: num_frames: {}", num_frames);

        let eviction_hints = ConcurrentQueue::bounded(num_frames);
        for i in 0..num_frames {
            eviction_hints.push(i).unwrap();
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

        Ok(BufferPoolClock {
            num_frames,
            used_frames: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            container_manager,
            page_to_frame: PageToFrame::new(),
            eviction_hints,
            pages,
            metas,
            stats: BPStats::new(),
        })
    }

    pub fn eviction_stats(&self) -> String {
        "Eviction stats not supported".to_string()
    }

    pub fn file_stats(&self) -> String {
        "File stat is disabled".to_string()
    }

    fn ensure_free_frames(&self) -> Result<(), MemPoolStatus> {
        let used_frames = self.used_frames.load(Ordering::Acquire);
        let used_percent = used_frames as f64 / self.num_frames as f64;
        if used_percent > 0.95 {
            log_warn!(
                "[EVICT] Used frames: {}/{}({}). Evicting pages...",
                used_frames,
                self.num_frames,
                used_percent
            );
            self.evict_batch()
        } else {
            log_warn!(
                "[EVICT] Used frames: {}/{}({}). No eviction needed.",
                used_frames,
                self.num_frames,
                used_percent
            );
            Ok(())
        }
    }

    fn fetch_add_clock_hand(&self, increment: usize) -> usize {
        self.clock_hand
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |clock_start| {
                let new_clock_start = (clock_start + increment) % self.num_frames;
                Some(new_clock_start)
            })
            .expect("Should not fail because the func always returns Some")
    }

    /// Evict up to `EVICTION_BATCH_SIZE` pages.
    pub fn evict_batch(&self) -> Result<(), MemPoolStatus> {
        with_eviction_scratch(EVICTION_BATCH_SIZE, |scratch| {
            // ─── 1. Collect candidate pages ────────────────────────────
            // This may return CannotEvictPage if it cannot find any candidates.
            self.collect_candidates(scratch, EVICTION_BATCH_SIZE)?;

            log_warn!(
                "    Trying to evict {} pages ({} dirty, {} clean)",
                scratch.dirty_pages.len() + scratch.clean_pages.len(),
                scratch.dirty_pages.len(),
                scratch.clean_pages.len()
            );

            // ─── 2. Write dirty pages to disk under read‑latch ─────────
            self.flush_dirty(&scratch.dirty_pages);

            // ─── 3. Latch clean pages for eviction ────────────────────
            self.latch_clean(&mut scratch.clean_pages, &mut scratch.to_evict);

            // ─── 4. Upgrade dirty‑page latches ────────────────────────
            self.upgrade_dirty(&mut scratch.dirty_pages, &mut scratch.to_evict);

            // ─── 5. Remove from page table and recycle frames ─────────
            self.remove_from_page_table(&mut scratch.to_evict);
            self.finalize_eviction(&mut scratch.to_evict);

            Ok(())
        })
    }

    // ───────────────────────── helpers ───────────────────────────────

    /// Scan the clock hand until enough candidates are found.
    fn collect_candidates(
        &self,
        scratch: &mut EvictionScratchSpace,
        batch: usize,
    ) -> Result<(), MemPoolStatus> {
        let max_iter = 2 * self.num_frames / batch;
        let (clean, dirty) = (&mut scratch.clean_pages, &mut scratch.dirty_pages);

        let mut iters = 0;
        while clean.len() + dirty.len() < batch {
            if iters > max_iter {
                if clean.is_empty() && dirty.is_empty() {
                    return Err(MemPoolStatus::CannotEvictPage);
                }
                break;
            }

            let clock_start = self.fetch_add_clock_hand(batch);
            for i in clock_start..clock_start + batch {
                self.classify_frame(i % self.num_frames, clean, dirty);
            }
            iters += 1;
        }
        Ok(())
    }

    #[inline]
    fn classify_frame(
        &self,
        index: usize,
        clean: &mut Vec<(usize, *mut FMeta)>,
        dirty: &mut Vec<(usize, FRGuard)>,
    ) {
        let meta = &mut unsafe { &mut *self.metas.get() }[index];

        // Skip empty or latched frames immediately.
        if meta.key().is_none() || meta.latch.is_locked() {
            return;
        }

        let marked = meta.evict_info.score() > 0;
        if !marked {
            meta.evict_info.update();
            return;
        }

        let is_dirty = meta.is_dirty.load(Ordering::Acquire);
        if is_dirty {
            // Try read‑latch on dirty page
            if let Some(g) = FRGuard::try_new(
                box_as_mut_ptr(meta),
                box_as_mut_ptr(&mut unsafe { &mut *self.pages.get() }[index]),
            ) {
                if g.page_key().is_some() {
                    dirty.push((index, g));
                }
            }
        } else {
            // Clean and marked
            clean.push((index, box_as_mut_ptr(meta)));
        }
    }

    /// Flush all dirty victims under read latches.
    fn flush_dirty(&self, dirty_pages: &[(usize, FRGuard)]) {
        for (_, g) in dirty_pages {
            self.write_victim_to_disk_if_dirty_r(g).unwrap();
        }
    }

    /// Acquire write latches on clean victims and move them to `to_evict`.
    fn latch_clean(
        &self,
        clean_pages: &mut Vec<(usize, *mut FMeta)>,
        to_evict: &mut Vec<(usize, FWGuard)>,
    ) {
        for (index, meta) in clean_pages.drain(..) {
            if let Some(g) = FWGuard::try_new(
                meta,
                box_as_mut_ptr(&mut unsafe { &mut *self.pages.get() }[index]),
                false,
            ) {
                if g.page_key().is_none() {
                    continue;
                }
                self.write_victim_to_disk_if_dirty_w(&g).unwrap();
                to_evict.push((index, g));
            }
        }
    }

    /// Upgrade read → write latches on dirty pages once they are flushed.
    fn upgrade_dirty(
        &self,
        dirty_pages: &mut Vec<(usize, FRGuard)>,
        to_evict: &mut Vec<(usize, FWGuard)>,
    ) {
        for (index, g) in dirty_pages.drain(..) {
            // We already checked that the page key is not None for dirty pages
            // in classify_frame, so we can skip the check here.
            // if g.page_key().is_none() {
            //     continue;
            // }
            if let Ok(gw) = g.try_upgrade(false) {
                to_evict.push((index, gw));
            }
        }
    }

    /// Remove pages from the page‑table in c_key order.
    fn remove_from_page_table(&self, to_evict: &mut [(usize, FWGuard)]) {
        to_evict.sort_unstable_by_key(|(_, g)| g.page_key().unwrap().c_key);
        self.page_to_frame
            .remove_batch_sorted(to_evict.iter().map(|(_, g)| g.page_key().unwrap()));
    }

    /// Reset frame metadata and recycle indices.
    fn finalize_eviction(&self, to_evict: &mut Vec<(usize, FWGuard)>) {
        let mut freed = 0;
        for (index, g) in to_evict.drain(..) {
            freed += 1;
            assert!(!g.dirty().load(Ordering::Acquire));
            g.set_page_key(None);
            g.evict_info().reset();
            self.eviction_hints.push(index).unwrap();
        }
        self.used_frames.fetch_sub(freed, Ordering::AcqRel);
    }

    #[allow(dead_code)]
    fn get_read_guard(&self, index: usize) -> FRGuard {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FRGuard::new(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
        )
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

    /// Choose a victim frame to be evicted.
    /// If all the frames are latched, then return None.
    fn choose_victim(&self) -> Option<FWGuard> {
        // First, try the eviction hints
        while let Ok(victim) = self.eviction_hints.pop() {
            let frame = self.try_get_write_guard(victim, false);
            if let Some(guard) = frame {
                assert!(guard.page_key().is_none());
                return Some(guard);
            } else {
                log_error!("Eviction hint failed: {}", victim);
                // The frame is latched for some reason. Try the next frame.
            }
        }
        None
    }

    // The exclusive latch is NOT NEEDED when calling this function
    // This function will write the victim page to disk if it is dirty, and set the dirty bit to false.
    fn write_victim_to_disk_if_dirty_w(&self, victim: &FWGuard) -> Result<(), MemPoolStatus> {
        if let Some(key) = victim.page_key() {
            if victim
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.container_manager.get_container(key.c_key);
                container.write_page(key.page_id, victim)?;
            }
        }

        Ok(())
    }

    // The exclusive latch is NOT NEEDED when calling this function
    // This function will write the victim page to disk if it is dirty, and set the dirty bit to false.
    fn write_victim_to_disk_if_dirty_r(&self, victim: &FRGuard) -> Result<(), MemPoolStatus> {
        if let Some(key) = victim.page_key() {
            // Compare and swap is_dirty because we don't want to write the page if it is already written by another thread.
            if victim
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.container_manager.get_container(key.c_key);
                container.write_page(key.page_id, victim)?;
            }
        }

        Ok(())
    }
}

impl<const EVICTION_BATCH_SIZE: usize> MemPool for BufferPoolClock<EVICTION_BATCH_SIZE> {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        unimplemented!("Create container is not implemented");
        // self.container_manager.create_container(c_key, is_temp);
        // Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        unimplemented!("Drop container is not implemented");
        // self.container_manager.get_container(c_key).set_temp(true);
        // self.shared();
        // let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        // for (_, frame_index) in page_to_frame.iter_container(c_key) {
        //     self.eviction_hints.push(*frame_index).unwrap();
        // }
        // self.release_shared();
        // Ok(())
    }

    /// Create a new page for write in memory.
    /// NOTE: This function does not write the page to disk.
    /// See more at `handle_page_fault(key, new_page=true)`
    /// The newly allocated page is not formatted except for the page id.
    /// The caller is responsible for initializing the page.
    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();

        self.ensure_free_frames()?;

        // 1. Choose victim
        let mut victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;
        // Victim must be clean and empty
        assert!(victim.page_key().is_none());
        assert!(!victim.dirty().load(Ordering::Acquire));

        // 3. Modify the page_to_frame mapping. Critical section.
        // Need to remove the old mapping and insert the new mapping.
        let page_key = {
            // Insert the new mapping
            let container = self.container_manager.get_container(c_key);
            let page_id = container.inc_page_count(1) as PageId;
            let index = victim.frame_id();
            let key = PageKey::new(c_key, page_id);
            self.page_to_frame.insert(key, index as usize);
            key
        };

        // 4. Initialize the page
        victim.set_id(page_key.page_id); // Initialize the page with the page id
        victim.set_page_key(Some(page_key)); // Set the frame key to the new page key
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset(); // Reset the eviction info
        self.used_frames.fetch_add(1, Ordering::AcqRel);

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
        {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            if (frame_id as usize) < self.num_frames
                && unsafe { &(*self.metas.get())[frame_id as usize] }.key() == Some(key.p_key())
            {
                return true;
            }
        }

        // Critical section.
        {
            self.page_to_frame.contains_key(&key.p_key())
        }
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.page_to_frame.get_page_keys(c_key)
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        log_debug!("Page write: {}", key);
        self.stats.inc_write_count();

        #[cfg(not(feature = "no_bp_hint"))]
        {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            if (frame_id as usize) < self.num_frames {
                // Check the page_key first to avoid acquiring the latch of a not-matching pageA
                if unsafe { &(*self.metas.get())[frame_id as usize] }.key() == Some(key.p_key()) {
                    match self.try_get_write_guard(frame_id as usize, false) {
                        Some(g) if g.page_key().map(|k| k == key.p_key()).unwrap_or(false) => {
                            g.evict_info().update();
                            g.dirty().store(true, Ordering::Release);
                            log_debug!("Page fast path write: {}", key);
                            return Ok(g);
                        }
                        _ => {}
                    }
                }
            }
            // Failed due to one of the following reasons:
            // 1. The page key does not match.
            // 2. The page key is not set (empty frame).
            // 3. The frame is latched.
            // 4. The frame id is out of bounds.
            log_debug!("Page fast path write failed{}", key);
        }

        // Ensure free frames
        self.ensure_free_frames()?;

        let cmap = self.page_to_frame.get_cmap(&key.p_key().c_key);
        let mut victim = match cmap.entry(key.p_key().page_id) {
            dashmap::Entry::Occupied(entry) => {
                let guard = self.try_get_write_guard(*entry.get(), true);
                return guard
                    .inspect(|g| {
                        g.evict_info().reset();
                    })
                    .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
            }
            dashmap::Entry::Vacant(entry) => {
                self.used_frames.fetch_add(1, Ordering::AcqRel);
                let victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;
                entry.insert(victim.frame_id() as usize);
                victim
            }
        };
        // Now we have a clean victim that can be used for writing.
        assert!(victim.page_key().is_none());
        assert!(!victim.dirty().load(Ordering::Acquire));

        let container = self.container_manager.get_container(key.p_key().c_key);
        container
            .read_page(key.p_key().page_id, &mut victim)
            .map(|()| {
                victim.set_page_key(Some(key.p_key()));
                victim.evict_info().reset();
            })?;
        victim.dirty().store(true, Ordering::Release); // Prepare the page for writing.

        Ok(victim)
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        log_debug!("Page read: {}", key);
        self.stats.inc_read_count();

        #[cfg(not(feature = "no_bp_hint"))]
        {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            if (frame_id as usize) < self.num_frames {
                // Check the page_key first to avoid acquiring the latch of a not-matching page
                if unsafe { &(*self.metas.get())[frame_id as usize] }.key() == Some(key.p_key()) {
                    let guard = self.try_get_read_guard(frame_id as usize);
                    match guard {
                        Some(g) if g.page_key().map(|k| k == key.p_key()).unwrap_or(false) => {
                            // Update the eviction info
                            g.evict_info().reset();
                            log_debug!("Page fast path read: {}", key);
                            return Ok(g);
                        }
                        _ => {}
                    }
                }
            }
            // Failed due to one of the following reasons:
            // 1. The page key does not match.
            // 2. The page key is not set (empty frame).
            // 3. The frame is latched.
            // 4. The frame id is out of bounds.
            log_debug!("Page fast path read failed: {}", key);
        };

        // Ensure free frames
        self.ensure_free_frames()?;

        // 1. Check the page-to-frame mapping and get a frame index.
        // 2. If the page is found, then try to acquire a read-latch, after which, the critical section ends.
        // 3. If the page is not found, then a victim must be chosen to evict.
        let cmap = self.page_to_frame.get_cmap(&key.p_key().c_key);
        let mut victim = match cmap.entry(key.p_key().page_id) {
            entry::Entry::Occupied(entry) => {
                let guard = self.try_get_read_guard(*entry.get());
                return guard
                    .inspect(|g| {
                        g.evict_info().reset();
                    })
                    .ok_or(MemPoolStatus::FrameReadLatchGrantFailed);
            }
            entry::Entry::Vacant(entry) => {
                self.used_frames.fetch_add(1, Ordering::AcqRel);
                let victim = self.choose_victim().ok_or(MemPoolStatus::CannotEvictPage)?;
                entry.insert(victim.frame_id() as usize); // Insert the new mapping
                victim
            }
        };

        // Now we have a clean victim that can be used for reading.
        assert!(victim.page_key().is_none());
        assert!(!victim.dirty().load(Ordering::Acquire));

        let container = self.container_manager.get_container(key.p_key().c_key);
        container
            .read_page(key.p_key().page_id, &mut victim)
            .map(|()| {
                victim.set_page_key(Some(key.p_key()));
                victim.evict_info().reset();
            })?;
        Ok(victim.downgrade())
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let frame = loop {
                if let Some(guard) = self.try_get_read_guard(i) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            self.write_victim_to_disk_if_dirty_r(&frame).unwrap();
        });

        // Call fsync on all the files
        self.container_manager.flush_all()?;
        Ok(())
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        // do nothing for now.
        Ok(())
    }

    // Just return the runtime stats
    unsafe fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting_for_write = self.stats.read_request_waiting_for_write_count();
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
        let (total_created, total_disk_read, total_disk_write) = disk_io_per_container
            .iter()
            .fold((0, 0, 0), |acc, (_, (created, read, write))| {
                (acc.0 + created, acc.1 + read, acc.2 + write)
            });
        MemoryStats {
            bp_num_frames_in_mem: self.num_frames,
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

    // Reset the runtime stats
    unsafe fn reset_stats(&self) {
        self.stats.clear();
    }

    /// Reset the buffer pool to its initial state.
    /// This will write all the dirty pages to disk and flush the files.
    /// After this operation, the buffer pool will have all the frames cleared.
    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let mut frame = loop {
                if let Some(guard) = self.try_get_write_guard(i, false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            self.write_victim_to_disk_if_dirty_w(&frame).unwrap();
            if let Some(key) = frame.page_key() {
                self.page_to_frame.remove(&key);
            }
            frame.clear();
        });

        self.container_manager.flush_all()?;

        while self.eviction_hints.pop().is_ok() {}
        for i in 0..self.num_frames {
            self.eviction_hints.push(i).unwrap();
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
}

#[cfg(test)]
impl<const EVICTION_BATCH_SIZE: usize> BufferPoolClock<EVICTION_BATCH_SIZE> {
    /// # Safety
    ///
    /// The caller must ensure that the buffer pool is not being used by any other thread.
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_page_to_frame();
        self.check_frame_id_and_page_id_match();
    }

    /// # Safety
    ///
    /// The caller must ensure that the buffer pool is not being used by any other thread.
    unsafe fn check_all_frames_unlatched(&self) {
        for i in 0..self.num_frames {
            self.try_get_write_guard(i, false).unwrap();
        }
    }

    /// Invariant: page_to_frame contains all the pages in the buffer pool
    /// # Safety
    ///
    /// The caller must ensure that the buffer pool is not being used by any other thread.
    unsafe fn check_page_to_frame(&self) {
        use std::collections::HashMap;
        let mut frame_to_page = HashMap::new();
        for (c, k, v) in self.page_to_frame.iter() {
            let p_key = PageKey::new(c, k);
            frame_to_page.insert(v, p_key);
        }
        for i in 0..self.num_frames {
            let frame = self.get_read_guard(i);
            if frame_to_page.contains_key(&i) {
                assert_eq!(frame.page_key().unwrap(), frame_to_page[&i]);
            } else {
                assert_eq!(frame.page_key(), None);
            }
        }
        // println!("page_to_frame: {:?}", page_to_frame);
    }

    /// # Safety
    ///
    /// The caller must ensure that the buffer pool is not being used by any other thread.
    unsafe fn check_frame_id_and_page_id_match(&self) {
        for i in 0..self.num_frames {
            let frame = self.get_read_guard(i);
            if let Some(key) = frame.page_key() {
                let page_id = frame.get_id();
                assert_eq!(key.page_id, page_id);
            }
        }
    }
}

unsafe impl<const EVICTION_BATCH_SIZE: usize> Sync for BufferPoolClock<EVICTION_BATCH_SIZE> {}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::log;
    use crate::{log_info, random::gen_random_pathname};

    use super::*;
    use std::thread::{self};
    use tempfile::TempDir;

    fn get_test_bp<const EVICTION_BATCH_SIZE: usize>(
        num_frames: usize,
    ) -> Arc<BufferPoolClock<EVICTION_BATCH_SIZE>> {
        let base_dir = gen_random_pathname(Some("test_bp_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(BufferPoolClock::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_bpc_and_frame_latch() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_bp::<2>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);
        let frame = bp.create_new_page_for_write(c_key).unwrap();
        let key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iterations = 80; // Note: u8 max value is 255
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        loop {
                            if let Ok(mut guard) = bp.get_page_for_write(key) {
                                guard[0] += 1;
                                break;
                            } else {
                                // spin
                                log_info!("Spin");
                                std::hint::spin_loop();
                            }
                        }
                    }
                });
            }
        });
        unsafe {
            bp.run_checks();
        }
        {
            assert!(bp.is_in_mem(key));
            let guard = bp.get_page_for_read(key).unwrap();
            assert_eq!(guard[0], num_threads * num_iterations);
        }
        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_bpc_write_back_simple() {
        let db_id = 0;
        let num_frames = 1;
        let bp = get_test_bp::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let key1 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 1;
            guard.page_frame_key().unwrap()
        };
        let key2 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 2;
            guard.page_frame_key().unwrap()
        };
        unsafe {
            bp.run_checks();
        }
        // check contents of evicted page
        {
            assert!(!bp.is_in_mem(key1));
            let guard = bp.get_page_for_read(key1).unwrap();
            assert_eq!(guard[0], 1);
        }
        // check contents of the second page
        {
            assert!(!bp.is_in_mem(key2));
            let guard = bp.get_page_for_read(key2).unwrap();
            assert_eq!(guard[0], 2);
        }
        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_bpc_write_back_many() {
        let db_id = 0;
        let mut keys = Vec::new();
        let num_frames = 1;
        let bp = get_test_bp::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        for i in 0..100 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i;
            keys.push(guard.page_frame_key().unwrap());
        }
        unsafe {
            bp.run_checks();
        }
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }
        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_bpc_create_new_page() {
        let db_id = 0;

        let num_frames = 2;
        let bp = get_test_bp::<1>(num_frames);
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

        // Traverse by 2 pages at a time
        for i in 0..num_traversal {
            let guard1 = bp.get_page_for_read(keys[i * 2]).unwrap();
            assert_eq!(guard1[0], i as u8 * 2);
            let guard2 = bp.get_page_for_read(keys[i * 2 + 1]).unwrap();
            assert_eq!(guard2[0], i as u8 * 2 + 1);
        }

        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_bpc_all_frames_latched() {
        let db_id = 0;

        let num_frames = 1;
        let bp = get_test_bp::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
        guard1[0] = 1;

        // Try to get a new page for write. This should fail because all the frames are latched.
        let res = bp.create_new_page_for_write(c_key);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(guard1);

        // Now, we should be able to get a new page for write.
        let guard2 = bp.create_new_page_for_write(c_key).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_bpc_clear_frames() {
        let db_id = 0;

        let num_frames = 10;
        let bp = get_test_bp::<2>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 2 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe {
            bp.run_checks();
        }

        // Clear the buffer pool
        bp.flush_all_and_reset().unwrap();

        unsafe {
            bp.run_checks();
        }

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe {
            bp.run_checks();
        }
    }

    #[test]
    fn test_bpc_clear_frames_durable() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        let num_frames = 10;
        let mut keys = Vec::new();

        {
            let cm = Arc::new(ContainerManager::new(&temp_dir, false, false).unwrap());
            let bp1 = BufferPoolClock::<2>::new(num_frames, cm).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..num_frames * 10 {
                let mut guard = bp1.create_new_page_for_write(c_key).unwrap();
                guard[0] = i as u8;
                keys.push(guard.page_frame_key().unwrap());
            }

            unsafe {
                bp1.run_checks();
            }

            // Clear the buffer pool
            bp1.flush_all_and_reset().unwrap();

            unsafe {
                bp1.run_checks();
            }
        }

        {
            let cm = Arc::new(ContainerManager::new(&temp_dir, false, false).unwrap());
            let bp2 = BufferPoolClock::<2>::new(num_frames, cm).unwrap();

            // Check the contents of the pages
            for (i, key) in keys.iter().enumerate() {
                let guard = bp2.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }

            unsafe {
                bp2.run_checks();
            }
        }
    }

    #[test]
    fn test_bpc_stats() {
        let db_id = 0;

        let num_frames = 1;
        let bp = get_test_bp::<1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let key_1 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 1;
            guard.page_frame_key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        let key_2 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 2;
            guard.page_frame_key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_1).unwrap();
            assert_eq!(guard[0], 1);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_2).unwrap();
            assert_eq!(guard[0], 2);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);
    }
}
