#[allow(unused)]
use crate::log;

use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    bp::{eviction_policy::EvictionPolicy, frame_guards::box_as_mut_ptr},
    container::ContainerManager,
    log_warn,
    page::{Page, PageId, PAGE_SIZE},
};
use libc::{
    c_int, c_uchar, c_void, madvise, mincore, mmap, munmap, size_t, sysconf, MADV_DONTNEED,
    MAP_ANONYMOUS, MAP_FAILED, MAP_NORESERVE, MAP_PRIVATE, PROT_READ, PROT_WRITE, _SC_PAGESIZE,
};
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

pub const VMCACHE_LARGE_PAGE_ENTRIES: usize = 1 << 25; // 2^25 pages = 512 GiB with 16 KiB page size
const fn page_key_to_offset_large(page_key: &PageKey) -> usize {
    // Layout (LSB→MSB):
    // bits  0-19 : page_id   (20 bits, up to 1 048 575)
    // bits 20-24 : container (5 bits,   up to      31)
    // total      : 25 bits   ⇒ 2^25 pages
    //
    // With a 16 KiB page, addressable memory = 2^25 × 2^14 B = 2^39 B = 512 GiB.

    assert!(page_key.c_key.db_id() == 0);
    assert!(page_key.c_key.c_id() < (1 << 5)); // fits in 5 bits
    assert!(page_key.page_id < (1 << 20)); // fits in 20 bits

    let container_part = (page_key.c_key.c_id() as usize) << 20;
    let page_part = page_key.page_id as usize; // already < 2^20

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
        let size = Self::PAGE_ENTRIES * PAGE_SIZE;
        let flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
        let prot = PROT_READ | PROT_WRITE;
        let raw = unsafe { mmap(ptr::null_mut(), size, prot, flags, -1, 0) };
        if ptr::eq(raw, MAP_FAILED) {
            return Err(MemPoolStatus::MemoryAllocationError("mmap failed"));
        }
        let metas = UnsafeCell::new(
            (0..Self::PAGE_ENTRIES)
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
        let mut eviction_trial_count = 0;
        loop {
            if eviction_trial_count > 5 {
                log_warn!(
                    "[EVICT{}] Eviction trial count exceeded. Cannot evict pages.",
                    eviction_trial_count
                );
                return Err(MemPoolStatus::CannotEvictPage);
            }
            let used_percent =
                self.used_frames.load(Ordering::Acquire) as f64 / self.num_frames as f64;
            if used_percent > 0.95 {
                log_warn!(
                    "[EVICT{}] Used frames: {}/{}({}). Evicting pages...",
                    eviction_trial_count,
                    self.used_frames.load(Ordering::Acquire),
                    self.num_frames,
                    used_percent
                );
                let _ = self.evict_batch();
            } else {
                return Ok(());
            }
            eviction_trial_count += 1;
        }
    }

    fn evict_batch(&self) -> Result<(), MemPoolStatus> {
        // Rule1. We need a read-latch of the frame when writing the page to disk.
        // Rule2. We need a write-latch of the frame when removing the page from the page-table.
        // Rule3. We don't want to hold the write-latch of the frame for too long (i.e. while writing to disk).
        // Rule4. We want to evict the pages from the page-table or page-to-frame mapping in batches.

        // This suggests that we should first get a read-latch on the dirty pages and write them to disk
        // without holding any write-latch of other frames. We keep the read-latch on the dirty pages until
        // we finish writing all the dirty pages to disk.
        // Then, we get the write-latches on both dirty and clean pages and remove them from the page-table
        // all at once.

        log_warn!(
            "    Used Frame Cnt: {}",
            self.used_frames.load(Ordering::Acquire)
        );

        let mut clean_victims = Vec::with_capacity(EVICTION_BATCH_SIZE);
        let mut dirty_victims = Vec::with_capacity(EVICTION_BATCH_SIZE);
        let mut to_evict = Vec::with_capacity(EVICTION_BATCH_SIZE);

        let mut num_iterations = 0;
        while clean_victims.len() + dirty_victims.len() < EVICTION_BATCH_SIZE {
            if num_iterations > 5 {
                if clean_victims.len() + dirty_victims.len() == 0 {
                    log_warn!(
                        "    Could not evict pages after {} iterations",
                        num_iterations
                    );
                    return Err(MemPoolStatus::CannotEvictPage);
                } else {
                    break;
                }
            }
            for i in self.resident_set.clock_batch_iter(EVICTION_BATCH_SIZE) {
                let meta = &mut unsafe { &mut *self.metas.get() }[i as usize];
                let is_marked = meta.evict_info.score() > 0;
                let is_latched = meta.latch.is_locked();
                if is_latched {
                    continue; // Skip the page if it is latched.
                }
                if is_marked {
                    // (marked and unlatched)
                    // Marked and not latched.
                    if meta.is_dirty.load(Ordering::Acquire) {
                        // Get a read latch on the page.
                        // If the page is dirty, we need to write it to disk.
                        let guard = FRGuard::try_new(box_as_mut_ptr(meta), unsafe {
                            self.pages.ptr.add(i as usize)
                        });
                        if let Some(guard) = guard {
                            dirty_victims.push((i, guard));
                        } else {
                            // Latched. Skip it.
                            continue;
                        }
                    } else {
                        // Clean page. Add to the eviction list.
                        clean_victims.push((i, meta));
                    }
                } else {
                    // Mark the page if it is unmarked and not latched.
                    meta.evict_info.update();
                }
            }
            num_iterations += 1;
        }

        log_warn!(
            "    Trying to evict {} pages ({} dirty, {} clean)",
            dirty_victims.len() + clean_victims.len(),
            dirty_victims.len(),
            clean_victims.len()
        );
        // Note: the sum of dirty and clean victims may be at most 2 times the batch size - 1
        // because the first clock_batch_iter() call may return at most batch size - 1 pages
        // and the second call may return at most batch size pages.

        // 1. Write the dirty pages to disk.
        for (_, guard) in dirty_victims.iter() {
            self.write_victim_to_disk_if_dirty_r(guard)?;
        }

        // 2. Try to get a write latch on the clean pages.
        for (i, meta) in clean_victims.into_iter() {
            // We need to get a write latch on the page to remove it from the page table.
            if let Some(guard) = FWGuard::try_new(
                box_as_mut_ptr(meta),
                unsafe { self.pages.ptr.add(i as usize) },
                false,
            ) {
                to_evict.push((i, guard));
            }
        }

        // 3. Try to upgrade the dirty pages to write latches.
        for (i, guard) in dirty_victims.into_iter() {
            // We need to get a write latch on the page to remove it from the page table.
            if let Ok(guard) = guard.try_upgrade(false) {
                to_evict.push((i, guard));
            }
        }

        // 4. Validate that the pages are still in the resident set.
        // Resident set is not transactional so we use the frame guards as locks
        // in OCC and check the resident set after we get the write latches.
        to_evict.retain(|(i, _guard)| self.resident_set.contains(*i));

        // 5. Remove the pages from the page table.
        for (_, guard) in to_evict.iter() {
            // Remove the page from the page table.
            self.remove_page_entry(guard)?;
        }

        // 6. Remove from the resident set and update the eviction policy.
        let mut count = 0;
        for (i, guard) in to_evict.into_iter() {
            count += 1;
            // Remove the page from the resident set.
            self.resident_set.remove(i);
            // Remove the frame info.
            guard.set_page_key(None);
            // Update the eviction policy.
            guard.evict_info().reset();
        }

        log_warn!("    Evicted {} pages", count,);

        self.used_frames.fetch_sub(count, Ordering::AcqRel);

        Ok(())
    }

    /// Return `true` if the page is present in physical memory, `false` otherwise.
    ///
    /// This is a direct Rust translation of the C helper that uses `mincore(2)`.
    unsafe fn page_resident(&self, page_key: &PageKey) -> bool {
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
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        let container = self.container_manager.get_container(c_key);
        let (mut guard, page_id) = loop {
            if let Some(res) = container.inc_page_count_if(1, |current| {
                let page_id = current as PageId;
                let page_key = PageKey::new(c_key, page_id);
                let page_offset = self.page_key_to_offset(&page_key);
                FWGuard::try_new(
                    box_as_mut_ptr(&mut unsafe { &mut *self.metas.get() }[page_offset]),
                    unsafe { self.pages.ptr.add(page_offset) },
                    true,
                )
            }) {
                break res;
            } else {
                std::hint::spin_loop();
            }
        };
        let page_key = PageKey::new(c_key, page_id as PageId);
        guard.set_id(page_key.page_id); // Initialize the page with the page id
        guard.set_page_key(Some(page_key)); // Set the frame key to the new page key
        guard.dirty().store(true, Ordering::Release);
        guard.evict_info().reset();
        guard.evict_info().update();
        self.resident_set
            .insert(self.page_key_to_offset(&guard.page_key().unwrap()) as u64);

        Ok(guard)
        // TODO: We need to evict a page if the buffer pool is almost full.
    }

    fn create_new_pages_for_write(
        &self,
        _c_key: ContainerKey,
        _num_pages: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        unimplemented!()
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        unsafe { self.page_resident(&key.p_key()) }
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
                debug_assert!(k == key.p_key(), "Page key mismatch");
                guard.evict_info().reset(); // Reset unmarks the page
                Ok(guard)
            }
            None => {
                // The page is not in memory. Load it from disk.
                self.ensure_free_pages()?;
                self.used_frames.fetch_add(1, Ordering::AcqRel);
                // TODO: We need to evict a page if the buffer pool is almost full.

                let container = self.container_manager.get_container(key.p_key().c_key);
                container
                    .read_page(key.p_key().page_id, &mut guard)
                    .map(|()| {
                        guard.set_page_key(Some(key.p_key()));
                        guard.evict_info().reset(); // Reset unmarks the page
                    })?;

                log_warn!(
                    "    Read page {} for write from container {}",
                    key.p_key().page_id,
                    key.p_key().c_key
                );
                self.resident_set.insert(page_offset as u64);
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
                debug_assert!(k == key.p_key(), "Page key mismatch");
                guard.evict_info().reset();
                Ok(guard)
            }
            None => {
                // We need to evict a page if the buffer pool is almost full.
                self.ensure_free_pages()?;
                self.used_frames.fetch_add(1, Ordering::AcqRel);

                // The page is not in memory. Load it from disk.
                // 1. First, we need to get a write guard on the page to be able to read the page into the frame.
                // 2. Then, we covert the write guard to a read guard by downgrading it.
                let container = self.container_manager.get_container(key.p_key().c_key);
                let mut guard = guard
                    .try_upgrade(false)
                    .map_err(|_| MemPoolStatus::FrameWriteLatchGrantFailed)?;
                container
                    .read_page(key.p_key().page_id, &mut guard)
                    .map(|()| {
                        guard.set_page_key(Some(key.p_key()));
                        guard.evict_info().reset();
                    })?;
                log_warn!(
                    "    Read page {} for read from container {}",
                    key.p_key().page_id,
                    key.p_key().c_key
                );
                self.resident_set.insert(page_offset as u64);

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

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        // do nothing for now.
        Ok(())
    }

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
