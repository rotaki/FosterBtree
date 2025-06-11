#[allow(unused)]
use crate::log;

use std::{
    cell::{RefCell, UnsafeCell},
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};

use crate::{
    bp::{eviction_policy::EvictionPolicy, frame_guards::box_as_mut_ptr},
    container::ContainerManager,
    log_warn,
    page::{Page, PageId, PAGE_SIZE},
};
use libc::{
    madvise, mmap, munmap, MADV_DONTNEED, MAP_ANONYMOUS, MAP_FAILED, MAP_NORESERVE, MAP_PRIVATE,
    PROT_READ, PROT_WRITE,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{io, ptr};

use super::{
    buffer_pool::BPStats,
    eviction_policy::ClockEvictionPolicy,
    frame_guards::FrameMeta,
    mem_pool_trait::{MemoryStats, PageKey},
    resident_set::ResidentPageSet,
    ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey,
};

type FMeta = FrameMeta<ClockEvictionPolicy>;
type FWGuard = FrameWriteGuard<ClockEvictionPolicy>;
type FRGuard = FrameReadGuard<ClockEvictionPolicy>;

pub const VMCACHE_LARGE_PAGE_ENTRIES: usize = 1 << 27; // 2^27 pages = 2 TiB with 16 KiB page size
const fn page_key_to_offset_large(page_key: &PageKey) -> usize {
    // Layout (LSB→MSB):
    // bits  0-22 : page_id   (23 bits, 2^23 * 2^14 = 2^37 B = 128 GiB)
    // bits 23-26 : container (4 bits,  up to 15)
    // total      : 27 bits   ⇒ 2^27 pages
    //
    // With a 16 KiB page, addressable memory = 2^27 × 2^14 B = 2^41 B = 2 TiB.

    assert!(page_key.c_key.db_id() == 0);
    assert!(page_key.c_key.c_id() < (1 << 4)); // fits in 4 bits
    assert!(page_key.page_id < (1 << 23)); // fits in 23 bits

    let container_part = (page_key.c_key.c_id() as usize) << 23;
    let page_part = page_key.page_id as usize; // already < 2^23

    container_part | page_part
}

pub const VMCACHE_SMALL_PAGE_ENTRIES: usize = 1 << 10; // 2^10 pages = 16 MiB with 16 KiB page size
const fn page_key_to_offset_small(page_key: &PageKey) -> usize {
    // Layout (LSB→MSB):
    // bits  0-8  : page_id   (9 bits, up to 511)
    // bits  9-9  : container (1 bit, up to      1)
    // total      : 10 bits   ⇒ 2^10 pages
    //
    // With a 16 KiB page, addressable memory = 2^10 × 2^14 B = 2^24 B = 16 MiB.

    assert!(page_key.c_key.db_id() == 0);
    assert!(page_key.c_key.c_id() < (1 << 1)); // fits in 1 bit
    assert!(page_key.page_id < (1 << 9)); // fits in 9 bits

    let container_part = (page_key.c_key.c_id() as usize) << 9;
    let page_part = page_key.page_id as usize; // already < 2^9

    container_part | page_part
}

struct PagePtrWrap {
    ptr: *mut Page,
}

impl PagePtrWrap {
    fn new(ptr: *mut Page) -> Self {
        PagePtrWrap { ptr }
    }
}

unsafe impl Sync for PagePtrWrap {}
unsafe impl Send for PagePtrWrap {}

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

/// A VMCachePool is a memory pool that uses virtual memory to manage pages.
/// IS_SMALL=true has 2^10 pages = 16 MiB with 16 KiB page size.
/// 1 bit (2^1) for container id and 9 bits (2^9) for page id for each container.
/// IS_SMALL=false has 2^25 pages = 512 GiB with 16 KiB page size.
/// 5 bits (2^5) for container id and 20 bits (2^20) for page id for each container.
pub struct VMCachePool<const IS_SMALL: bool = true, const EVICTION_BATCH_SIZE: usize = 64> {
    num_frames: usize,
    used_frames: AtomicUsize,
    resident_set: ResidentPageSet,
    container_manager: Arc<ContainerManager>,
    pages: PagePtrWrap,
    #[allow(clippy::vec_box)]
    metas: UnsafeCell<Vec<Box<FMeta>>>,
    stats: BPStats,
}

unsafe impl<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize> Sync
    for VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>
{
}

impl<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize> Drop
    for VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>
{
    fn drop(&mut self) {
        let size = Self::PAGE_ENTRIES * PAGE_SIZE;
        unsafe {
            if munmap(self.pages.ptr.cast(), size) != 0 {
                eprintln!("munmap failed: {}", io::Error::last_os_error());
            }
        }
    }
}

impl<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize>
    VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>
{
    pub const PAGE_ENTRIES: usize = if IS_SMALL {
        VMCACHE_SMALL_PAGE_ENTRIES
    } else {
        VMCACHE_LARGE_PAGE_ENTRIES
    };

    #[inline]
    const fn page_key_to_offset(&self, page_key: &PageKey) -> usize {
        if IS_SMALL {
            page_key_to_offset_small(page_key)
        } else {
            page_key_to_offset_large(page_key)
        }
    }

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        if num_frames < EVICTION_BATCH_SIZE {
            panic!("num_frames must be greater or equal to EVICTION_BATCH_SIZE");
        }
        let size = Self::PAGE_ENTRIES * PAGE_SIZE;
        let flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
        let prot = PROT_READ | PROT_WRITE;
        let raw = unsafe { mmap(ptr::null_mut(), size, prot, flags, -1, 0) };
        if ptr::eq(raw, MAP_FAILED) {
            return Err(MemPoolStatus::MemoryAllocationError("mmap failed"));
        }
        let metas = UnsafeCell::new(
            (0..Self::PAGE_ENTRIES)
                .into_par_iter()
                .map(|i| Box::new(FMeta::new(i as u32)))
                .collect::<Vec<_>>(),
        );

        Ok(VMCachePool {
            num_frames,
            used_frames: AtomicUsize::new(0),
            resident_set: ResidentPageSet::new(num_frames as u64),
            container_manager,
            pages: PagePtrWrap::new(raw as *mut Page),
            metas,
            stats: BPStats::new(),
        })
    }

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

    // A write guard is needed when calling this function because madvise will zero out the page.
    fn remove_page_entry(&self, guard: &FWGuard) -> Result<(), MemPoolStatus> {
        if let Some(page_key) = guard.page_key() {
            let page_offset = self.page_key_to_offset(&page_key);
            assert!(
                page_offset < Self::PAGE_ENTRIES,
                "Page offset out of bounds {}/{}",
                page_offset,
                Self::PAGE_ENTRIES
            );
            let ret = unsafe {
                madvise(
                    (self.pages.ptr as *mut u8).add(self.page_key_to_offset(&page_key) * PAGE_SIZE)
                        as *mut libc::c_void,
                    PAGE_SIZE,
                    MADV_DONTNEED,
                )
            };
            if ret != 0 {
                eprintln!("madvise failed: {}", std::io::Error::last_os_error());
                Err(MemPoolStatus::MemoryAllocationError("madvise failed"))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    fn ensure_free_pages(&self) -> Result<(), MemPoolStatus> {
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

    fn evict_batch(&self) -> Result<(), MemPoolStatus> {
        // Rule1. We need a read-latch of the frame when writing the page to disk.
        // Rule2. We need a write-latch of the frame when removing the page from the page-table because the OS will
        // zero out the page when we remove it from the page-table and we need to make sure that no one else
        // is using the page while we are removing it from the page-table.
        // Rule3. We don't want to hold the write-latch of the frame for too long (i.e. while writing to disk).
        // Rule4. We want to be able to evict the pages from the page-table or page-to-frame mapping in batches.

        // This suggests that we should first get a read-latch on the dirty pages and write them to disk
        // without holding any write-latch of other frames. We keep the read-latch on the dirty pages until
        // we finish writing all the dirty pages to disk.
        // Then, we get the write-latches on both dirty and clean pages and remove them from the page-table
        // all at once.
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
        let max_iter = 2 * self.resident_set.len() / batch;
        let (clean, dirty) = (&mut scratch.clean_pages, &mut scratch.dirty_pages);

        let mut iters = 0;
        while clean.len() + dirty.len() < batch {
            if iters > max_iter {
                if clean.is_empty() && dirty.is_empty() {
                    return Err(MemPoolStatus::CannotEvictPage);
                }
                break;
            }

            for i in self.resident_set.clock_batch_iter(batch) {
                self.classify_frame(i as usize, clean, dirty);
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
            if let Some(g) =
                FRGuard::try_new(box_as_mut_ptr(meta), unsafe { self.pages.ptr.add(index) })
            {
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
            if let Some(g) = FWGuard::try_new(meta, unsafe { self.pages.ptr.add(index) }, false) {
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
        for (index, _) in to_evict.iter() {
            let ret = unsafe {
                madvise(
                    (self.pages.ptr as *mut u8).add(index * PAGE_SIZE) as *mut libc::c_void,
                    PAGE_SIZE,
                    MADV_DONTNEED,
                )
            };
            assert_eq!(
                ret,
                0,
                "madvise failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }

    /// Reset frame metadata and recycle indices.
    fn finalize_eviction(&self, to_evict: &mut Vec<(usize, FWGuard)>) {
        let mut freed = 0;
        for (index, g) in to_evict.drain(..) {
            freed += 1;
            assert!(!g.dirty().load(Ordering::Acquire));
            g.set_page_key(None);
            g.evict_info().reset();
            assert!(self.resident_set.remove(index as u64));
        }
        self.used_frames.fetch_sub(freed, Ordering::AcqRel);
    }
}

impl<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize> MemPool
    for VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>
{
    type EP = ClockEvictionPolicy;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.stats.inc_new_page();

        self.ensure_free_pages()?;

        let container = self.container_manager.get_container(c_key);
        let (mut guard, page_key) = loop {
            if let Some(res) = container.inc_page_count_if(1, |current| {
                let page_id = current as PageId;
                let page_key = PageKey::new(c_key, page_id);
                let page_offset = self.page_key_to_offset(&page_key);
                FWGuard::try_new(
                    box_as_mut_ptr(&mut unsafe { &mut *self.metas.get() }[page_offset]),
                    unsafe { self.pages.ptr.add(page_offset) },
                    true,
                )
                .map(|g| (g, page_key))
            }) {
                break res.0;
            } else {
                std::hint::spin_loop();
            }
        };
        guard.set_id(page_key.page_id); // Initialize the page with the page id
        guard.set_page_key(Some(page_key)); // Set the frame key to the new page key
        guard.dirty().store(true, Ordering::Release);
        guard.evict_info().reset();
        self.resident_set
            .insert(self.page_key_to_offset(&page_key) as u64);
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        Ok(guard)
    }

    fn create_new_pages_for_write(
        &self,
        _c_key: ContainerKey,
        _num_pages: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        unimplemented!()
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        let meta = &mut unsafe { &mut *self.metas.get() }[self.page_key_to_offset(&key.p_key())];
        meta.key().is_some()
    }

    fn get_page_keys_in_mem(&self, _c_key: ContainerKey) -> Vec<PageFrameKey> {
        todo!()
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let page_offset = self.page_key_to_offset(&key.p_key());
        assert!(
            page_offset < Self::PAGE_ENTRIES,
            "Page offset out of bounds {}/{}",
            page_offset,
            Self::PAGE_ENTRIES
        );

        let meta = &mut unsafe { &mut *self.metas.get() }[page_offset];
        let page: *mut Page = unsafe { self.pages.ptr.add(page_offset) };
        let mut guard = FWGuard::try_new(box_as_mut_ptr(meta), page, true)
            .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)?;

        match guard.page_key() {
            Some(k) => {
                // Page is already in memory
                assert!(k == key.p_key(), "Page key mismatch");
                guard.evict_info().reset(); // Reset unmarks the page
                Ok(guard)
            }
            None => {
                // The page is not in memory. Load it from disk.
                self.ensure_free_pages()?;

                let container = self.container_manager.get_container(key.p_key().c_key);
                container
                    .read_page(key.p_key().page_id, &mut guard)
                    .map(|()| {
                        guard.set_page_key(Some(key.p_key()));
                        guard.evict_info().reset(); // Reset unmarks the page
                    })?;

                self.resident_set.insert(page_offset as u64);
                self.used_frames.fetch_add(1, Ordering::AcqRel);
                Ok(guard)
            }
        }
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let page_offset = self.page_key_to_offset(&key.p_key());
        assert!(
            page_offset < Self::PAGE_ENTRIES,
            "Page offset out of bounds {}/{}",
            page_offset,
            Self::PAGE_ENTRIES
        );

        let meta = &mut unsafe { &mut *self.metas.get() }[page_offset];
        let page: *mut Page = unsafe { self.pages.ptr.add(page_offset) };
        let guard = FRGuard::try_new(box_as_mut_ptr(meta), page)
            .ok_or(MemPoolStatus::FrameReadLatchGrantFailed)?;

        match guard.page_key() {
            Some(k) => {
                // Page is already in memory
                assert!(k == key.p_key(), "Page key mismatch");
                guard.evict_info().reset(); // Reset unmarks the page
                Ok(guard)
            }
            None => {
                // We need to evict a page if the buffer pool is almost full.
                self.ensure_free_pages()?;

                // The page is not in memory. Load it from disk.
                // 1. First, we need to get a write guard on the page to be able to read the page into the frame.
                // 2. Then, we covert the write guard to a read guard by downgrading it.
                let mut guard = guard
                    .try_upgrade(false)
                    .map_err(|_| MemPoolStatus::FrameWriteLatchGrantFailed)?; // Write latch is needed to read the page into the frame
                let container = self.container_manager.get_container(key.p_key().c_key);
                container
                    .read_page(key.p_key().page_id, &mut guard)
                    .map(|()| {
                        guard.set_page_key(Some(key.p_key()));
                        guard.evict_info().reset();
                    })?;
                self.resident_set.insert(page_offset as u64);
                self.used_frames.fetch_add(1, Ordering::AcqRel);

                Ok(guard.downgrade())
            }
        }
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        for i in self.resident_set.clock_batch_iter(self.resident_set.len()) {
            let guard = FRGuard::try_new(
                box_as_mut_ptr(&mut unsafe { &mut *self.metas.get() }[i as usize]),
                unsafe { self.pages.ptr.add(i as usize) },
            )
            .ok_or(MemPoolStatus::FrameReadLatchGrantFailed)?;
            self.write_victim_to_disk_if_dirty_r(&guard)?;
        }

        self.container_manager.flush_all()?;
        Ok(())
    }

    unsafe fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting_for_write = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();
        let mut num_frames_per_container = BTreeMap::new();
        // Traverse the resident set to count the number of frames per container.
        for i in self.resident_set.clock_batch_iter(self.resident_set.len()) {
            let meta = &mut unsafe { &mut *self.metas.get() }[i as usize];
            if let Some(key) = meta.key() {
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

    unsafe fn reset_stats(&self) {
        self.stats.clear();
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.flush_all()?;
        for i in self.resident_set.clock_batch_iter(self.resident_set.len()) {
            let meta = &mut unsafe { &mut *self.metas.get() }[i as usize];
            let guard = FWGuard::try_new(
                box_as_mut_ptr(meta),
                unsafe { self.pages.ptr.add(i as usize) },
                false,
            )
            .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)?;
            self.remove_page_entry(&guard)?;
            guard.set_page_key(None);
        }

        self.resident_set.clear();
        self.used_frames.store(0, Ordering::Release);

        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        for i in self.resident_set.clock_batch_iter(self.resident_set.len()) {
            let meta = &mut unsafe { &mut *self.metas.get() }[i as usize];
            let guard = FWGuard::try_new(
                box_as_mut_ptr(meta),
                unsafe { self.pages.ptr.add(i as usize) },
                false,
            )
            .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)?;
            guard.dirty().store(false, Ordering::Release);
        }

        Ok(())
    }
}

#[cfg(test)]
impl<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize>
    VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>
{
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_memory_resident_pages();
    }

    unsafe fn check_all_frames_unlatched(&self) {
        log_warn!("[CHECK] Checking all frames are unlatched...");
        for i in self.resident_set.clock_batch_iter(self.resident_set.len()) {
            let meta = &mut unsafe { &mut *self.metas.get() }[i as usize];
            assert!(!meta.latch.is_locked(), "Frame {} is latched", i);
        }
    }

    unsafe fn check_memory_resident_pages(&self) {
        log_warn!("[CHECK] Checking memory resident pages...");
        for i in self.resident_set.clock_batch_iter(self.resident_set.len()) {
            let meta = &mut unsafe { &mut *self.metas.get() }[i as usize];
            assert!(meta.key().is_some(), "Frame {} is not in memory", i);
            assert_eq!(
                i as usize,
                self.page_key_to_offset(&meta.key().unwrap()),
                "Frame {} does not match page key",
                i
            );
            assert!(
                unsafe { self.page_resident(&meta.key().unwrap()) },
                "Frame {} is not resident",
                i
            );
        }
        // Traverse metas to check if all pages are resident.
        let meta = &mut unsafe { &mut *self.metas.get() };
        for i in 0..meta.len() {
            // log_warn!("Checking frame {}...", i);
            let meta = &mut meta[i];
            if let Some(key) = meta.key() {
                assert_eq!(
                    { i },
                    self.page_key_to_offset(&key),
                    "Frame {} does not match page key",
                    i
                );
                assert!(self.resident_set.contains(i as u64));
            } else {
                assert!(!self.resident_set.contains(i as u64));
            }
        }
    }

    /// Return `true` if the page is present in OS page table.
    ///
    /// This is a direct Rust translation of the C helper that uses `mincore(2)`.
    unsafe fn page_resident(&self, page_key: &PageKey) -> bool {
        use libc::{c_int, c_uchar, c_void, mincore, size_t, sysconf, _SC_PAGESIZE};
        // 1. Get the page address.
        let i = self.page_key_to_offset(page_key);
        let addr = self.pages.ptr.add(i) as *mut c_void;

        // 2. Get system page size.
        let page_sz = sysconf(_SC_PAGESIZE) as usize;
        if page_sz == 0 {
            return false; // sysconf failed
        }

        // 2. Check if the page is aligned to the page boundary
        if (addr as usize) % page_sz != 0 {
            eprintln!(
                "Address {} is not aligned to page size {}",
                addr as usize, page_sz
            );
            return false; // Address is not aligned
        }

        // 3. Call mincore(); it fills one byte per page in `vec`.
        let mut vec: c_uchar = 0;
        let ret: c_int = mincore(addr, page_sz as size_t, &mut vec as *mut c_uchar);

        if ret == -1 {
            // mincore failed (e.g., the page is not mapped in this process)
            return false;
        }

        // 4. Bit 0 == 1 → page is resident.
        (vec & 1) == 1
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::random::gen_random_pathname;

    use super::*;
    use std::{hint::black_box, thread};

    fn get_test_vmcache<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize>(
        num_frames: usize,
    ) -> Arc<VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>> {
        let base_dir = gen_random_pathname(Some("test_bp_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(VMCachePool::<IS_SMALL, EVICTION_BATCH_SIZE>::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_vmc_small_allocation() {
        let mp = get_test_vmcache::<true, 2>(10);
        black_box(mp);
    }

    #[test]
    fn test_vmc_frame_latch() {
        let vmc = get_test_vmcache::<true, 2>(10);
        let c_key = ContainerKey::new(0, 0);

        let frame = vmc.create_new_page_for_write(c_key).unwrap();
        let page_key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iterations = 80;
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        loop {
                            if let Ok(mut guard) = vmc.get_page_for_write(page_key) {
                                guard[0] += 1;
                                break;
                            } else {
                                // spin
                                println!("spin: {:?}", thread::current().id());
                                std::hint::spin_loop();
                            }
                        }
                    }
                });
            }
        });

        unsafe { vmc.run_checks() };

        {
            assert!(vmc.is_in_mem(page_key));
            let guard = vmc.get_page_for_read(page_key).unwrap();
            assert_eq!(guard[0], num_threads * num_iterations);
        }

        unsafe { vmc.run_checks() };
    }

    #[test]
    fn test_vmc_write_back_simple() {
        let vmc = get_test_vmcache::<true, 1>(1);
        let c_key = ContainerKey::new(0, 0);

        let key1 = {
            let mut guard = vmc.create_new_page_for_write(c_key).unwrap();
            guard[0] = 1;
            guard.page_frame_key().unwrap()
        };
        let key2 = {
            let mut guard = vmc.create_new_page_for_write(c_key).unwrap();
            guard[0] = 2;
            guard.page_frame_key().unwrap()
        };
        unsafe {
            vmc.run_checks();
        }
        // check contents of evicted page
        {
            assert!(!vmc.is_in_mem(key1));
            let guard = vmc.get_page_for_read(key1).unwrap();
            assert_eq!(guard[0], 1);
        }
        // check contents of the second page
        {
            assert!(!vmc.is_in_mem(key2));
            let guard = vmc.get_page_for_read(key2).unwrap();
            assert_eq!(guard[0], 2);
        }
        unsafe {
            vmc.run_checks();
        }
    }

    #[test]
    fn test_vmc_write_back_many() {
        let mut keys = Vec::new();
        let vmc = get_test_vmcache::<true, 1>(1);
        let c_key = ContainerKey::new(0, 0);

        for i in 0..100 {
            let mut guard = vmc.create_new_page_for_write(c_key).unwrap();
            guard[0] = i;
            keys.push(guard.page_frame_key().unwrap());
        }
        unsafe {
            vmc.run_checks();
        }
        for (i, key) in keys.iter().enumerate() {
            let guard = vmc.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }
        unsafe {
            vmc.run_checks();
        }
    }

    #[test]
    fn test_vmc_create_new_page() {
        let db_id = 0;

        let num_frames = 2;
        let bp = get_test_vmcache::<true, 1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let num_traversal = 100;

        let mut count = 0;
        let mut keys = Vec::new();

        for _i in 0..num_traversal {
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
    fn test_vmc_all_frames_latched() {
        let db_id = 0;

        let num_frames = 1;
        let vmc = get_test_vmcache::<true, 1>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut guard1 = vmc.create_new_page_for_write(c_key).unwrap();
        guard1[0] = 1;

        // Try to get a new page for write. This should fail because all the frames are latched.
        let res = vmc.create_new_page_for_write(c_key);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(guard1);

        // Now, we should be able to get a new page for write.
        let guard2 = vmc.create_new_page_for_write(c_key).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_vmc_clear_frames() {
        let db_id = 0;

        let num_frames = 10;
        let vmc = get_test_vmcache::<true, 2>(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 2 {
            let mut guard = vmc.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe {
            vmc.run_checks();
        }

        // Clear the buffer pool
        vmc.flush_all_and_reset().unwrap();

        unsafe {
            vmc.run_checks();
        }

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = vmc.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe {
            vmc.run_checks();
        }
    }

    #[test]
    fn test_vmc_clear_frames_durable() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        let num_frames = 10;
        let mut keys = Vec::new();

        {
            let cm = Arc::new(ContainerManager::new(&temp_dir, false, false).unwrap());
            let vmc1 = Arc::new(VMCachePool::<true, 2>::new(num_frames, cm).unwrap());
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..num_frames * 10 {
                log_warn!("Creating page {}", i);
                let mut guard = vmc1.create_new_page_for_write(c_key).unwrap();
                guard[0] = i as u8;
                keys.push(guard.page_frame_key().unwrap());
            }

            log_warn!("Created {} pages", keys.len());
            unsafe {
                vmc1.run_checks();
            }

            log_warn!("Flushing all pages");

            // Clear the buffer pool
            vmc1.flush_all_and_reset().unwrap();

            log_warn!("Flushed all pages");

            unsafe {
                vmc1.run_checks();
            }
        }

        log_warn!("Creating a new VMCachePool");

        {
            let cm = Arc::new(ContainerManager::new(&temp_dir, false, false).unwrap());
            let vmc2 = Arc::new(VMCachePool::<true, 2>::new(num_frames, cm).unwrap());

            // Check the contents of the pages
            for (i, key) in keys.iter().enumerate() {
                let guard = vmc2.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }

            unsafe {
                vmc2.run_checks();
            }
        }
    }
}
