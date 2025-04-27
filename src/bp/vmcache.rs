use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

use crate::{
    bp::{eviction_policy::EvictionPolicy, frame_guards::box_as_mut_ptr},
    container::ContainerManager,
    page::{Page, PageId, PAGE_SIZE},
};
use libc::{
    madvise, mmap, munmap, pread, pwrite, MADV_DONTNEED, MAP_ANONYMOUS, MAP_FAILED, MAP_NORESERVE,
    MAP_PRIVATE, O_DIRECT, PROT_READ, PROT_WRITE,
};
use std::{
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    os::unix::fs::OpenOptionsExt,
    ptr,
};

use super::{
    buffer_pool::BPStats,
    frame_guards::FrameMeta,
    mem_pool_trait::{MemoryStats, PageKey},
    resident_set::ResidentPageSet,
    ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey,
};

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
pub struct VMCachePool<const IS_SMALL: bool> {
    num_frames: usize,
    resident_set: ResidentPageSet,
    container_manager: Arc<ContainerManager>,
    pages: PagePtrWrap,
    metas: UnsafeCell<Vec<Box<FrameMeta>>>,
    stats: BPStats,
}

unsafe impl<const IS_SMALL: bool> Sync for VMCachePool<IS_SMALL> {}

impl<const IS_SMALL: bool> Drop for VMCachePool<IS_SMALL> {
    fn drop(&mut self) {
        let size = Self::PAGE_ENTRIES * PAGE_SIZE;
        unsafe {
            if munmap(self.pages.ptr.cast(), size) != 0 {
                eprintln!("munmap failed: {}", io::Error::last_os_error());
            }
        }
    }
}

impl<const IS_SMALL: bool> VMCachePool<IS_SMALL> {
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
                .map(|i| Box::new(FrameMeta::new(i as u32)))
                .collect::<Vec<_>>(),
        );

        Ok(VMCachePool {
            num_frames,
            resident_set: ResidentPageSet::new(num_frames as u64 * 2),
            container_manager,
            pages: PagePtrWrap::new(raw as *mut Page),
            metas,
            stats: BPStats::new(),
        })
    }

    /// Choose a victim frame to be evicted.
    /// If all the frames are latched, then return None.
    fn choose_victim(&self) -> Option<FrameWriteGuard> {
        todo!()
    }

    // The exclusive latch is NOT NEEDED when calling this function
    // This function will write the victim page to disk if it is dirty, and set the dirty bit to false.
    fn write_victim_to_disk_if_dirty_w(
        &self,
        victim: &FrameWriteGuard,
    ) -> Result<(), MemPoolStatus> {
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
    fn write_victim_to_disk_if_dirty_r(
        &self,
        victim: &FrameReadGuard,
    ) -> Result<(), MemPoolStatus> {
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

    fn remove_page_entry(&self, page_key: &PageKey) -> Result<(), MemPoolStatus> {
        let page_offset = self.page_key_to_offset(page_key);
        assert!(
            page_offset < Self::PAGE_ENTRIES,
            "Page offset out of bounds {}/{}",
            page_offset,
            Self::PAGE_ENTRIES
        );
        let ret = unsafe {
            madvise(
                (self.pages.ptr as *mut u8).add(self.page_key_to_offset(page_key) * PAGE_SIZE)
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
    }
}

impl<const IS_SMALL: bool> MemPool for VMCachePool<IS_SMALL> {
    fn create_container(
        &self,
        c_key: super::ContainerKey,
        is_temp: bool,
    ) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, c_key: super::ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.stats.inc_new_page();

        let container = self.container_manager.get_container(c_key);
        let (mut guard, page_id) = loop {
            if let Some(res) = container.inc_page_count_if(1, |current| {
                let page_id = current as PageId;
                let page_key = PageKey::new(c_key, page_id);
                let page_offset = self.page_key_to_offset(&page_key);
                FrameWriteGuard::try_new(
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

        Ok(guard)
        // TODO: We need to evict a page if the buffer pool is almost full.
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus> {
        unimplemented!()
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        let page_offset = self.page_key_to_offset(&key.p_key());
        assert!(
            page_offset < Self::PAGE_ENTRIES,
            "Page offset out of bounds {}/{}",
            page_offset,
            Self::PAGE_ENTRIES
        );

        let meta = &unsafe { &mut *self.metas.get() }[page_offset];
        if meta.key().is_some() {
            debug_assert!(meta.key().unwrap() == key.p_key(), "Page key mismatch");
            true
        } else {
            false
        }
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        todo!()
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus> {
        let page_offset = self.page_key_to_offset(&key.p_key());
        assert!(
            page_offset < Self::PAGE_ENTRIES,
            "Page offset out of bounds {}/{}",
            page_offset,
            Self::PAGE_ENTRIES
        );

        let meta = &mut unsafe { &mut *self.metas.get() }[page_offset];
        let page: *mut Page = unsafe { self.pages.ptr.add(page_offset) };
        let mut guard = FrameWriteGuard::try_new(box_as_mut_ptr(meta), page, true)
            .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)?;

        match guard.page_key() {
            Some(k) => {
                // Page is already in memory
                debug_assert!(k == key.p_key(), "Page key mismatch");
                guard.evict_info().update();
                Ok(guard)
            }
            None => {
                // The page is not in memory. Load it from disk.
                // TODO: We need to evict a page if the buffer pool is almost full.

                let container = self.container_manager.get_container(key.p_key().c_key);
                container
                    .read_page(key.p_key().page_id, &mut guard)
                    .map(|()| {
                        guard.set_page_key(Some(key.p_key()));
                        guard.evict_info().reset();
                        guard.evict_info().update();
                    })?;
                Ok(guard)
            }
        }
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus> {
        let page_offset = self.page_key_to_offset(&key.p_key());
        assert!(
            page_offset < Self::PAGE_ENTRIES,
            "Page offset out of bounds {}/{}",
            page_offset,
            Self::PAGE_ENTRIES
        );

        let meta = &mut unsafe { &mut *self.metas.get() }[page_offset];
        let page: *mut Page = unsafe { self.pages.ptr.add(page_offset) };

        match meta.key() {
            Some(k) => {
                let guard = FrameReadGuard::try_new(box_as_mut_ptr(meta), page)
                    .ok_or(MemPoolStatus::FrameReadLatchGrantFailed)?;

                // Page is already in memory
                debug_assert!(k == key.p_key(), "Page key mismatch");
                guard.evict_info().update();
                Ok(guard)
            }
            None => {
                // We need to evict a page if the buffer pool is almost full.

                // The page is not in memory. Load it from disk.
                // 1. First, we need to get a write guard on the page to be able to read the page into the frame.
                // 2. Then, we covert the write guard to a read guard by downgrading it.
                let mut guard = FrameWriteGuard::try_new(box_as_mut_ptr(meta), page, false)
                    .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)?;

                let container = self.container_manager.get_container(key.p_key().c_key);
                container
                    .read_page(key.p_key().page_id, &mut guard)
                    .map(|()| {
                        guard.set_page_key(Some(key.p_key()));
                        guard.evict_info().reset();
                        guard.evict_info().update();
                    })?;

                Ok(guard.downgrade())
            }
        }
    }

    fn prefetch_page(&self, key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        // TODO: For all the dirty pages in the buffer pool, write them to disk.
        Ok(())
    }

    fn fast_evict(&self, frame_id: u32) -> Result<(), MemPoolStatus> {
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
        // TODO: For all the dirty pages in the buffer pool, write them to disk and clear all the frames
        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        // TODO: For all the dirty pages in the buffer pool, clear the dirty flags
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::random::gen_random_pathname;

    use super::*;
    use std::{hint::black_box, thread};

    fn get_test_vmcache<const IS_SMALL: bool>(num_frames: usize) -> Arc<VMCachePool<IS_SMALL>> {
        let base_dir = gen_random_pathname(Some("test_bp_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(VMCachePool::<IS_SMALL>::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_small_allocation() {
        let mp = get_test_vmcache::<true>(10);
        black_box(mp);
    }

    #[test]
    fn test_frame_latch() {
        let vmc = get_test_vmcache::<true>(10);
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

        // unsafe {
        //     vmc.check_all_frames_unlatched();
        // }
        // unsafe {
        //     vmc.check_page_to_frame();
        // }
        // unsafe {
        //     vmc.check_frame_id_and_page_id_match();
        // }
        let guard = vmc.get_page_for_read(page_key).unwrap();
        assert_eq!(guard[0], num_threads * num_iterations);
    }

    #[test]
    fn test_create_new_page() {
        let vmc = get_test_vmcache::<true>(10);
        let c_key = ContainerKey::new(0, 0);

        for i in 0..20 {
            let frame = vmc.create_new_page_for_write(c_key).unwrap();
            assert_eq!(frame.page_key().unwrap(), PageKey::new(c_key, i));
            drop(frame);
        }

        for i in 0..20 {
            let frame = vmc.get_page_for_read(PageFrameKey::new(c_key, i)).unwrap();
            assert_eq!(frame.page_key().unwrap(), PageKey::new(c_key, i));
        }

        // unsafe {
        //     vmc.check_all_frames_unlatched();
        // }
        // unsafe {
        //     vmc.check_page_to_frame();
        // }
        // unsafe {
        //     vmc.check_frame_id_and_page_id_match();
        // }
    }

    #[test]
    fn test_concurrent_create_new_page() {
        let vmc = get_test_vmcache::<true>(10);
        let c_key = ContainerKey::new(0, 0);

        let mut frame1 = vmc.create_new_page_for_write(c_key).unwrap();
        frame1[0] = 1;
        let mut frame2 = vmc.create_new_page_for_write(c_key).unwrap();
        frame2[0] = 2;
        assert_eq!(frame1.page_key().unwrap(), PageKey::new(c_key, 0));
        assert_eq!(frame2.page_key().unwrap(), PageKey::new(c_key, 1));
        drop(frame1);
        drop(frame2);

        let frame1 = vmc.get_page_for_read(PageFrameKey::new(c_key, 0)).unwrap();
        let frame2 = vmc.get_page_for_read(PageFrameKey::new(c_key, 1)).unwrap();
        assert_eq!(frame1[0], 1);
        assert_eq!(frame2[0], 2);
    }
}
