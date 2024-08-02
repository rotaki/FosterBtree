#![allow(unused_imports)]
use crate::{logger::log, page};

use core::panic;
use std::{
    cell::UnsafeCell,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::size_of,
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

#[cfg(feature = "stat")]
use stat::*;

use crate::bp::prelude::*;

use crate::page::PageId;

use super::overflowpage::{OverflowPage, OverflowPageError};
use super::shortkeypage::{
    ShortKeyPage, ShortKeyPageError, ShortKeyPageResult, SHORT_KEY_PAGE_HEADER_SIZE,
};
use crate::page::{Page, AVAILABLE_PAGE_SIZE};

// const DEFAULT_BUCKET_NUM: usize = 1024;
const DEFAULT_BUCKET_NUM: usize = 1024 * 8;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PointerSwizzlingMode {
    None,
    AtomicShared,
    UnsafeShared,
    ThreadLocal,
}

// #[derive(Clone, Copy, PartialEq, Eq, Debug)]
// pub enum

enum FrameBuckets {
    Atomic(Vec<AtomicU32>),
    Unsafe(UnsafeCell<[u32; 4096]>),
    ThreadLocal(ThreadLocalBuckets),
}

impl FrameBuckets {
    fn store(&self, index: usize, value: u32) {
        match self {
            FrameBuckets::Atomic(buckets) => {
                buckets[index].store(value, std::sync::atomic::Ordering::Release);
            }
            FrameBuckets::Unsafe(buckets) => unsafe {
                (*buckets.get())[index] = value;
            },
            FrameBuckets::ThreadLocal(buckets) => {
                buckets.store(index, value);
            }
        }
    }

    fn load(&self, index: usize) -> u32 {
        match self {
            FrameBuckets::Atomic(buckets) => {
                buckets[index].load(std::sync::atomic::Ordering::Acquire)
            }
            FrameBuckets::Unsafe(buckets) => unsafe { (*buckets.get())[index] },
            FrameBuckets::ThreadLocal(buckets) => buckets.load(index),
        }
    }
}

struct ThreadLocalBuckets {
    buckets: UnsafeCell<[u32; 4096]>,
}

unsafe impl Sync for FrameBuckets {}
unsafe impl Sync for ThreadLocalBuckets {}

impl ThreadLocalBuckets {
    fn new() -> Self {
        ThreadLocalBuckets {
            buckets: UnsafeCell::new([u32::MAX; 4096]),
        }
    }

    fn store(&self, index: usize, value: u32) {
        unsafe {
            (*self.buckets.get())[index] = value;
        }
    }

    fn load(&self, index: usize) -> u32 {
        unsafe { (*self.buckets.get())[index] }
    }
}

pub struct PagedHashMap<E: EvictionPolicy, T: MemPool<E>> {
    pub bp: Arc<T>,
    c_key: ContainerKey,

    pub bucket_num: usize, // number of hash header pages

    pointer_swizzling_mode: PointerSwizzlingMode, // mode for pointer swizzling
    frame_buckets: Option<FrameBuckets>,          // frame buckets for different modes
    pub bucket_head: Vec<PageId>,                 // bucket head pages

    // bucket_metas: Vec<BucketMeta>, // first_frame_id, last_page_id, last_frame_id, bloomfilter // no need to be page
    phantom: PhantomData<E>,
}

#[derive(Debug, PartialEq)]
pub enum PagedHashMapError {
    KeyExists,
    KeyNotFound,
    OutOfSpace,
    PageCreationFailed,
    WriteLatchFailed,
    OverflowPageError(OverflowPageError),
    Other(String),
}

impl From<OverflowPageError> for PagedHashMapError {
    fn from(err: OverflowPageError) -> Self {
        PagedHashMapError::OverflowPageError(err)
    }
}

struct _BucketMeta {
    first_frame_id: u32,
    last_page_id: u32,
    last_frame_id: u32,
    bloomfilter: Vec<u8>,
}

/// Opportunistically try to fix the child page frame id
fn fix_frame_id<'a, E: EvictionPolicy>(
    this: FrameReadGuard<'a, E>,
    new_frame_key: &PageFrameKey,
) -> FrameReadGuard<'a, E> {
    match this.try_upgrade(true) {
        Ok(mut write_guard) => {
            <Page as ShortKeyPage>::set_next_frame_id(&mut write_guard, new_frame_key.frame_id());
            write_guard.downgrade()
        }
        Err(read_guard) => read_guard,
    }
}

impl<E: EvictionPolicy, T: MemPool<E>> PagedHashMap<E, T> {
    pub fn new(
        bp: Arc<T>,
        c_key: ContainerKey,
        bucket_num: usize,
        from_container: bool,
        pointer_swizzling_mode: PointerSwizzlingMode,
    ) -> Self {
        if from_container {
            todo!("Implement from container");
        } else {
            // SET ROOT: Need to do something for the root page e.g. set n
            let root_page = bp.create_new_page_for_write(c_key).unwrap();
            #[cfg(feature = "stat")]
            inc_local_stat_total_page_count();
            // assert_eq!(root_page.get_id(), 0, "root page id should be 0");

            let frame_buckets = match pointer_swizzling_mode {
                PointerSwizzlingMode::AtomicShared => {
                    let buckets = (0..bucket_num + 1)
                        .map(|_| AtomicU32::new(u32::MAX))
                        .collect::<Vec<AtomicU32>>();
                    buckets[0].store(root_page.frame_id(), std::sync::atomic::Ordering::Release);
                    Some(FrameBuckets::Atomic(buckets))
                }
                PointerSwizzlingMode::UnsafeShared => {
                    let buckets = UnsafeCell::new([u32::MAX; 4096]);
                    unsafe {
                        (*buckets.get())[0] = root_page.frame_id();
                    }
                    Some(FrameBuckets::Unsafe(buckets))
                }
                PointerSwizzlingMode::ThreadLocal => {
                    let buckets = ThreadLocalBuckets::new();
                    buckets.store(0, root_page.frame_id());
                    Some(FrameBuckets::ThreadLocal(buckets))
                }
                _ => None,
            };

            let mut bucket_head: Vec<PageId> = vec![0; bucket_num + 1];
            bucket_head[0] = root_page.get_id();

            // SET HASH BUCKET PAGES
            for i in 1..=bucket_num {
                let mut new_page = bp.create_new_page_for_write(c_key).unwrap();
                #[cfg(feature = "stat")]
                inc_local_stat_total_page_count();
                if let Some(ref frame_buckets) = frame_buckets {
                    match frame_buckets {
                        FrameBuckets::Atomic(buckets) => {
                            buckets[i]
                                .store(new_page.frame_id(), std::sync::atomic::Ordering::Release);
                        }
                        FrameBuckets::Unsafe(buckets) => unsafe {
                            (*buckets.get())[i] = new_page.frame_id();
                        },
                        FrameBuckets::ThreadLocal(buckets) => {
                            buckets.store(i, new_page.frame_id());
                        }
                    }
                }
                <Page as ShortKeyPage>::init(&mut new_page);
                bucket_head[i] = new_page.get_id();
                // assert_eq!(
                //     new_page.get_id() as usize,
                //     i,
                //     "Initial new page id should be {}",
                //     i
                // );
            }

            PagedHashMap {
                bp: bp.clone(),
                c_key,
                bucket_num,
                pointer_swizzling_mode,
                frame_buckets,
                bucket_head,
                phantom: PhantomData,
            }
        }
    }

    fn hash<K: AsRef<[u8]> + Hash>(&self, key: &K) -> PageId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        ((hasher.finish() as usize) % self.bucket_num + 1) as PageId
    }

    /// Insert a new key-value pair into the index.
    /// If the key already exists, it will return an error.
    pub fn insert<K: AsRef<[u8]> + Hash>(&self, key: K, val: K) -> Result<(), PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();

        let key_ref = key.as_ref();
        let val_ref = val.as_ref();
        let required_space = key_ref.len() + val_ref.len() + 3 * size_of::<u32>();

        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            // Handle large value insertion using overflow pages
            return self.insert_large_value(key_ref, val_ref);
        }

        // Regular insert logic for small key-value pairs
        let hashed_key = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];
        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };
        let page_key = PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        if self.pointer_swizzling_mode != PointerSwizzlingMode::None
            && expect_frame_id != page_key.frame_id()
        {
            if let Some(ref frame_buckets) = self.frame_buckets {
                frame_buckets.store(hashed_key as usize, page_key.frame_id());
            }
        }
        let mut last_page = self.insert_traverse_to_endofchain_for_write(page_key, key_ref)?;

        match <Page as ShortKeyPage>::insert(&mut last_page, key_ref, val_ref) {
            Ok(_) => Ok(()),
            Err(ShortKeyPageError::KeyExists) => {
                panic!("Key exists should have been detected in traverse_to_endofchain_for_write")
            }
            Err(ShortKeyPageError::OutOfSpace) => {
                let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                #[cfg(feature = "stat")]
                {
                    inc_local_stat_total_page_count();
                    inc_local_stat_chain_len(hashed_key as usize - 1);
                }

                new_page.init();
                <Page as ShortKeyPage>::set_next_page_id(&mut last_page, new_page.get_id());
                <Page as ShortKeyPage>::set_next_frame_id(&mut last_page, new_page.frame_id());
                match <Page as ShortKeyPage>::insert(&mut new_page, key_ref, val_ref) {
                    Ok(_) => Ok(()),
                    Err(err) => panic!("Inserting into a new page should succeed: {:?}", err),
                }
            }
            Err(err) => Err(PagedHashMapError::Other(format!("Insert error: {:?}", err))),
        }
    }

    pub fn insert_large_value<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        val: &[u8],
    ) -> Result<(), PagedHashMapError> {
        let key_ref = key.as_ref();
        let val_len = val.len() as u32;

        // Hash the key to find the correct bucket
        let hashed_key = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];

        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };
        let page_key = PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        if self.pointer_swizzling_mode != PointerSwizzlingMode::None
            && expect_frame_id != page_key.frame_id()
        {
            if let Some(ref frame_buckets) = self.frame_buckets {
                frame_buckets.store(hashed_key as usize, page_key.frame_id());
            }
        }
        let mut last_page = self.insert_traverse_to_endofchain_for_write(page_key, key_ref)?;

        // Create the root overflow page and handle the large value
        let (root_page_id, root_frame_id) = self.create_overflow_pages(val)?;

        // Attempt to insert the metadata into the last page
        match <Page as ShortKeyPage>::insert_large_value(
            &mut last_page,
            key_ref,
            val_len,
            root_page_id,
            root_frame_id,
        ) {
            Ok(_) => Ok(()),
            Err(ShortKeyPageError::OutOfSpace) => {
                // If there's no space, create a new page and try inserting there
                let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                #[cfg(feature = "stat")]
                {
                    inc_local_stat_total_page_count();
                    inc_local_stat_chain_len(hashed_key as usize - 1);
                }

                new_page.init();
                <Page as ShortKeyPage>::set_next_page_id(&mut last_page, new_page.get_id());
                <Page as ShortKeyPage>::set_next_frame_id(&mut last_page, new_page.frame_id());
                match <Page as ShortKeyPage>::insert_large_value(
                    &mut new_page,
                    key_ref,
                    val_len,
                    root_page_id,
                    root_frame_id,
                ) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        panic!("Inserting into new overflow page should succeed: {:?}", err)
                    }
                }
            }
            Err(err) => Err(PagedHashMapError::Other(format!("Insert error: {:?}", err))),
        }
    }

    fn create_overflow_pages(&self, val: &[u8]) -> Result<(PageId, u32), PagedHashMapError> {
        let mut remaining_value = val;
        let mut current_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        inc_local_stat_total_page_count();
        let root_page_id = current_page.get_id();
        let root_frame_id = current_page.frame_id();

        while !remaining_value.is_empty() {
            current_page.ofp_init();

            let chunk_size = std::cmp::min(remaining_value.len(), current_page.get_max_val_len());
            let chunk = &remaining_value[..chunk_size];
            remaining_value = &remaining_value[chunk_size..];

            current_page.ofp_insert(chunk)?;

            if !remaining_value.is_empty() {
                let next_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                #[cfg(feature = "stat")]
                inc_local_stat_total_page_count();
                current_page.ofp_set_next_page_id(next_page.get_id());
                if self.pointer_swizzling_mode != PointerSwizzlingMode::None {
                    current_page.ofp_set_next_frame_id(next_page.frame_id());
                }
                current_page = next_page;
            }
        }

        Ok((root_page_id, root_frame_id))
    }

    /// Get the value of a key from the index.
    /// If the key does not exist, it will return an error.
    pub fn get<K: AsRef<[u8]> + Hash>(&self, key: K) -> Result<Vec<u8>, PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_get_count();

        let hashed_key = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];
        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };
        let mut page_key =
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        let mut current_page = self.read_page(page_key);
        if self.pointer_swizzling_mode != PointerSwizzlingMode::None
            && current_page.frame_id() != page_key.frame_id()
        {
            if let Some(ref frame_buckets) = self.frame_buckets {
                frame_buckets.store(hashed_key as usize, current_page.frame_id());
            }
        }

        let mut result;

        loop {
            result = current_page.get(key.as_ref());
            let next_page_id = current_page.get_next_page_id();
            if result.is_ok() || next_page_id == 0 {
                break;
            }
            let next_frame_id = if self.pointer_swizzling_mode != PointerSwizzlingMode::None {
                current_page.get_next_frame_id()
            } else {
                u32::MAX
            };
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.read_page(page_key);
            if self.pointer_swizzling_mode != PointerSwizzlingMode::None
                && next_frame_id != next_page.frame_id()
            {
                page_key.set_frame_id(next_page.frame_id());
                let _ = fix_frame_id(current_page, &page_key);
            }
            current_page = next_page;
        }

        match result {
            Ok(ShortKeyPageResult::Value(v)) => Ok(v),
            Ok(ShortKeyPageResult::Overflow {
                val_len,
                root_page_id,
                root_frame_id,
            }) => self.get_large_value(val_len, root_page_id, root_frame_id),
            Err(ShortKeyPageError::KeyNotFound) => Err(PagedHashMapError::KeyNotFound),
            Err(err) => Err(PagedHashMapError::Other(format!("Get error: {:?}", err))),
        }
    }

    fn get_large_value(
        &self,
        val_len: u32,
        root_page_id: PageId,
        root_frame_id: u32,
    ) -> Result<Vec<u8>, PagedHashMapError> {
        let mut value = Vec::with_capacity(val_len as usize);
        let mut current_page_id = root_page_id;
        let mut current_frame_id = if self.pointer_swizzling_mode != PointerSwizzlingMode::None {
            root_frame_id
        } else {
            u32::MAX
        };

        while current_page_id != 0 {
            let page_key =
                PageFrameKey::new_with_frame_id(self.c_key, current_page_id, current_frame_id);
            let current_page = self.read_page(page_key);

            // Read value chunk from the overflow page
            let chunk = current_page.ofp_get().unwrap();
            value.extend_from_slice(&chunk);

            // Move to the next page
            current_page_id = current_page.ofp_get_next_page_id();
            if self.pointer_swizzling_mode != PointerSwizzlingMode::None {
                current_frame_id = current_page.ofp_get_next_frame_id();
            }
        }

        Ok(value)
    }

    pub fn iter(self: &Arc<Self>) -> PagedHashMapIter<E, T> {
        PagedHashMapIter::new(self.clone())
    }

    pub fn scan(self: &Arc<Self>) -> PagedHashMapIter<E, T> {
        PagedHashMapIter::new(self.clone())
    }

    fn insert_traverse_to_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<FrameWriteGuard<E>, PagedHashMapError> {
        let base = Duration::from_nanos(1);
        let mut attempts = 0;
        loop {
            let page = self.try_insert_traverse_to_endofchain_for_write(page_key, key);
            match page {
                Ok(page) => {
                    return Ok(page);
                }
                Err(PagedHashMapError::WriteLatchFailed) => {
                    attempts += 1;
                    // std::thread::sleep(base * attempts);
                    // std::hint::spin_loop();
                    std::thread::yield_now();
                }
                Err(PagedHashMapError::KeyExists) => {
                    return Err(PagedHashMapError::KeyExists);
                }
                Err(err) => {
                    panic!("Traverse to end of chain error: {:?}", err);
                }
            }
        }
    }

    fn try_insert_traverse_to_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<FrameWriteGuard<E>, PagedHashMapError> {
        let mut current_page = self.read_page(page_key);
        loop {
            let (slot_id, _) = current_page.is_exist(key);
            if slot_id.is_some() {
                return Err(PagedHashMapError::KeyExists);
            }
            let next_page_id = current_page.get_next_page_id();
            if next_page_id == 0 {
                // If we check remain space before insert, we can avoid this case
                match current_page.try_upgrade(true) {
                    Ok(upgraded_page) => {
                        return Ok(upgraded_page);
                    }
                    Err(_) => {
                        return Err(PagedHashMapError::WriteLatchFailed);
                    }
                };
            }
            let next_frame_id = if self.pointer_swizzling_mode != PointerSwizzlingMode::None {
                current_page.get_next_frame_id()
            } else {
                u32::MAX
            };
            let mut next_page_key =
                PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.read_page(next_page_key);
            if self.pointer_swizzling_mode != PointerSwizzlingMode::None
                && next_frame_id != next_page.frame_id()
            {
                next_page_key.set_frame_id(next_page.frame_id());
                let _ = fix_frame_id(current_page, &next_page_key);
            }
            current_page = next_page;
        }
    }

    // read_page and update frame_buckets
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard<E> {
        loop {
            let page = self.bp.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => {
                    panic!("Read page error: {:?}", err);
                }
            }
        }
    }

    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard<E> {
        loop {
            let page = self.bp.get_page_for_write(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => {
                    panic!("Write page error: {:?}", err);
                }
            }
        }
    }

    /// Update the value of an existing key.
    /// If the key does not exist, it will return an error.
    pub fn update<K: AsRef<[u8]> + Hash>(&self, key: K, val: K) -> Result<(), PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_update_count();

        let hashed_key = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];
        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };
        let page_key = PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);

        let (mut updating_page, slot_id) =
            self.update_traverse_to_endofchain_for_write(page_key, key.as_ref())?;
        match updating_page.update_at_slot(slot_id, val.as_ref()) {
            Ok(_) => Ok(()),
            Err(ShortKeyPageError::KeyNotFound) => {
                panic!("Key not found should detected in traverse_to_endofchain_for_write")
            }
            Err(ShortKeyPageError::OutOfSpace) => {
                loop {
                    let next_page_id = updating_page.get_next_page_id();
                    if next_page_id == 0 {
                        // create new page
                        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                        #[cfg(feature = "stat")]
                        inc_local_stat_total_page_count();
                        new_page.init();
                        updating_page.set_next_page_id(new_page.get_id());
                        updating_page.set_next_frame_id(new_page.frame_id());
                        match new_page.insert(key.as_ref(), val.as_ref()) {
                            Ok(_) => return Ok(()),
                            Err(err) => panic!("Inserting to new page should succeed: {:?}", err),
                        }
                    }
                    let next_frame_id = updating_page.get_next_frame_id();
                    let next_page_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                    let mut next_page = self.write_page(next_page_key);
                    if next_frame_id != next_page.frame_id() {
                        updating_page.set_next_frame_id(next_page.frame_id());
                    }
                    match next_page.insert(key.as_ref(), val.as_ref()) {
                        Ok(_) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {
                            updating_page = next_page;
                        }
                        Err(err) => panic!("Unexpected Error: {:?}", err),
                    }
                }
            }
            Err(err) => Err(PagedHashMapError::Other(format!("Update error: {:?}", err))),
        }
    }

    fn update_traverse_to_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<(FrameWriteGuard<E>, u32), PagedHashMapError> {
        let base = Duration::from_millis(1);
        let mut attempts = 0;
        loop {
            let page = self.try_update_traverse_to_endofchain_for_write(page_key, key);
            match page {
                Ok(page) => {
                    return Ok(page);
                }
                Err(PagedHashMapError::WriteLatchFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(PagedHashMapError::KeyNotFound) => {
                    return Err(PagedHashMapError::KeyNotFound);
                }
                Err(err) => {
                    panic!("Traverse to end of chain error: {:?}", err);
                }
            }
        }
    }

    fn try_update_traverse_to_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<(FrameWriteGuard<E>, u32), PagedHashMapError> {
        let mut current_page = self.read_page(page_key);
        loop {
            let (slot_id, _) = current_page.is_exist(key);
            match slot_id {
                Some(slot_id) => {
                    match current_page.try_upgrade(true) {
                        Ok(upgraded_page) => {
                            return Ok((upgraded_page, slot_id));
                        }
                        Err(_) => {
                            return Err(PagedHashMapError::WriteLatchFailed);
                        }
                    };
                }
                None => {
                    let next_page_id = current_page.get_next_page_id();
                    if next_page_id == 0 {
                        return Err(PagedHashMapError::KeyNotFound);
                    }
                    let next_frame_id = current_page.get_next_frame_id();
                    let mut next_page_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                    let next_page = self.read_page(next_page_key);
                    if next_frame_id != next_page.frame_id() {
                        next_page_key.set_frame_id(next_page.frame_id());
                        let _ = fix_frame_id(current_page, &next_page_key);
                    }
                    current_page = next_page;
                }
            }
        }
    }

    /// Upsert a key-value pair into the index.
    /// If the key already exists, it will update the value.
    /// If the key does not exist, it will insert a new key-value pair.
    pub fn upsert<K: AsRef<[u8]> + Hash>(&self, key: K, value: K) -> Result<(), PagedHashMapError> {
        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            return Err(PagedHashMapError::OutOfSpace);
        }

        let hashed_key = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];

        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };

        let mut page_key =
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if self.pointer_swizzling_mode != PointerSwizzlingMode::None
            && current_page.frame_id() != page_key.frame_id()
        {
            if let Some(ref frame_buckets) = self.frame_buckets {
                frame_buckets.store(hashed_key as usize, current_page.frame_id());
            }
        }

        loop {
            match current_page.upsert(key.as_ref(), value.as_ref()) {
                Ok(()) => return Ok(()),
                Err(ShortKeyPageError::OutOfSpace) => {
                    let (next_page_id, next_frame_id) = (
                        current_page.get_next_page_id(),
                        current_page.get_next_frame_id(),
                    );
                    if next_page_id == 0 {
                        break;
                    }
                    page_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                    let next_page = self.bp.get_page_for_write(page_key).unwrap();
                    if next_frame_id != next_page.frame_id() {
                        current_page.set_next_frame_id(next_page.frame_id());
                    }
                    current_page = next_page;
                }
                Err(_) => return Err(PagedHashMapError::Other("Upsert error".to_string())),
            }
        }

        // If we reach here, we need to create a new page
        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        {
            inc_local_stat_chain_len(hashed_key as usize - 1);
            inc_local_stat_total_page_count();
        }
        new_page.init();
        current_page.set_next_page_id(new_page.get_id());
        current_page.set_next_frame_id(new_page.frame_id());

        new_page
            .upsert(key.as_ref(), value.as_ref())
            .map_err(|_| PagedHashMapError::OutOfSpace)?;
        Ok(())
    }

    /// Upsert with a custom merge function.
    /// If the key already exists, it will update the value with the merge function.
    /// If the key does not exist, it will insert a new key-value pair.
    pub fn upsert_with_merge_old<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
        merge: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), PagedHashMapError> {
        let required_space = key.as_ref().len() + value.as_ref().len() + 6;
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            return Err(PagedHashMapError::OutOfSpace);
        }

        let hashed_key: u32 = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];

        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };
        let mut page_key =
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if self.pointer_swizzling_mode != PointerSwizzlingMode::None
            && current_page.frame_id() != page_key.frame_id()
        {
            if let Some(ref frame_buckets) = self.frame_buckets {
                frame_buckets.store(hashed_key as usize, current_page.frame_id());
            }
        }

        loop {
            match current_page.get(key.as_ref()) {
                Ok(ShortKeyPageResult::Value(existing_value)) => {
                    let new_value = merge(&existing_value, value.as_ref());
                    match current_page.update(key.as_ref(), &new_value) {
                        Ok(()) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {}
                        Err(_) => return Err(PagedHashMapError::Other("Update error".to_string())),
                    }
                }
                Ok(ShortKeyPageResult::Overflow {
                    val_len,
                    root_page_id,
                    root_frame_id,
                }) => {
                    let existing_value =
                        self.get_large_value(val_len, root_page_id, root_frame_id)?;
                    let new_value = merge(&existing_value, value.as_ref());
                    match current_page.update(key.as_ref(), &new_value) {
                        Ok(()) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {}
                        Err(_) => return Err(PagedHashMapError::Other("Update error".to_string())),
                    }
                }
                Err(ShortKeyPageError::KeyNotFound) => {
                    match current_page.upsert(key.as_ref(), value.as_ref()) {
                        Ok(()) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {}
                        Err(_) => return Err(PagedHashMapError::Other("Upsert error".to_string())),
                    }
                }
                Err(_) => return Err(PagedHashMapError::Other("Get error".to_string())),
            }

            let (next_page_id, next_frame_id) = (
                current_page.get_next_page_id(),
                current_page.get_next_frame_id(),
            );
            if next_page_id == 0 {
                break;
            }
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_write(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page.set_next_frame_id(next_page.frame_id());
            }
            current_page = next_page;
        }

        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        {
            inc_local_stat_chain_len(hashed_key as usize - 1);
            inc_local_stat_total_page_count();
        }
        new_page.init();
        current_page.set_next_page_id(new_page.get_id());
        current_page.set_next_frame_id(new_page.frame_id());

        new_page
            .upsert(key.as_ref(), value.as_ref())
            .map_err(|_| PagedHashMapError::OutOfSpace)?;
        Ok(())
    }

    pub fn upsert_with_merge<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
        merge: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), PagedHashMapError> {
        let key_ref = key.as_ref();
        let val_ref = value.as_ref();
        let required_space = key_ref.len() + val_ref.len() + 3 * size_of::<u32>();

        // Handle large values using overflow pages
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            let existing_value = match self.get(key_ref) {
                Ok(existing_value) => existing_value,
                Err(PagedHashMapError::KeyNotFound) => vec![],
                Err(err) => return Err(err),
            };
            let new_value = merge(&existing_value, val_ref);
            return self.insert_large_value(key_ref, &new_value);
        }

        let hashed_key: u32 = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];

        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };

        let mut page_key =
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if self.pointer_swizzling_mode != PointerSwizzlingMode::None
            && current_page.frame_id() != page_key.frame_id()
        {
            if let Some(ref frame_buckets) = self.frame_buckets {
                frame_buckets.store(hashed_key as usize, current_page.frame_id());
            }
        }

        loop {
            match current_page.get(key_ref) {
                Ok(ShortKeyPageResult::Value(existing_value)) => {
                    let new_value = merge(&existing_value, val_ref);
                    match current_page.update(key_ref, &new_value) {
                        Ok(()) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {}
                        Err(_) => return Err(PagedHashMapError::Other("Update error".to_string())),
                    }
                }
                Ok(ShortKeyPageResult::Overflow {
                    val_len,
                    root_page_id,
                    root_frame_id,
                }) => {
                    let existing_value =
                        self.get_large_value(val_len, root_page_id, root_frame_id)?;
                    let new_value = merge(&existing_value, val_ref);
                    if new_value.len() as u32
                        > (AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE)
                            .try_into()
                            .unwrap()
                    {
                        return self.insert_large_value(key_ref, &new_value);
                    }
                    match current_page.update(key_ref, &new_value) {
                        Ok(()) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {}
                        Err(_) => return Err(PagedHashMapError::Other("Update error".to_string())),
                    }
                }
                Err(ShortKeyPageError::KeyNotFound) => {
                    match current_page.upsert(key_ref, val_ref) {
                        Ok(()) => return Ok(()),
                        Err(ShortKeyPageError::OutOfSpace) => {}
                        Err(_) => return Err(PagedHashMapError::Other("Upsert error".to_string())),
                    }
                }
                Err(_) => return Err(PagedHashMapError::Other("Get error".to_string())),
            }

            let (next_page_id, next_frame_id) = (
                current_page.get_next_page_id(),
                current_page.get_next_frame_id(),
            );
            if next_page_id == 0 {
                break;
            }
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_write(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page.set_next_frame_id(next_page.frame_id());
            }
            current_page = next_page;
        }

        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        {
            inc_local_stat_chain_len(hashed_key as usize - 1);
            inc_local_stat_total_page_count();
        }
        new_page.init();
        current_page.set_next_page_id(new_page.get_id());
        current_page.set_next_frame_id(new_page.frame_id());

        new_page
            .upsert(key_ref, val_ref)
            .map_err(|_| PagedHashMapError::OutOfSpace)?;
        Ok(())
    }

    pub fn remove<K: AsRef<[u8]> + Hash>(&self, key: K) -> Result<(), PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_remove_count();

        let hashed_key: u32 = self.hash(&key);
        let first_page_id = self.bucket_head[hashed_key as usize];
        let expect_frame_id = if let Some(ref frame_buckets) = self.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };

        let mut page_key =
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, expect_frame_id);
        let mut result = Err(PagedHashMapError::KeyNotFound);

        loop {
            let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
            match current_page.remove(key.as_ref()) {
                Ok(_) => return Ok(()),
                Err(ShortKeyPageError::KeyNotFound) => {
                    let next_page_id = current_page.get_next_page_id();
                    if next_page_id == 0 {
                        break;
                    }
                    page_key = PageFrameKey::new(self.c_key, next_page_id);
                }
                Err(err) => {
                    return Err(PagedHashMapError::Other(format!("Remove error: {:?}", err)))
                }
            }
        }

        result
    }

    pub fn get_chain_stats(&self) -> String {
        let mut chain_lengths = vec![0; self.bucket_num];
        let mut total_page_count = 0;
        let mut total_use_rate = 0.0;
        let mut min_use_rate = 1.0;
        let mut max_use_rate = 0.0;
        let mut page_with_high_use_rate_9 = 0; // use rate > 0.9
        let mut page_with_high_use_rate_8 = 0; // use rate > 0.8
        let mut page_with_low_use_rate_1 = 0; // use rate < 0.1
        let mut page_with_low_use_rate_2 = 0; // use rate < 0.2

        for bucket_id in 1..=self.bucket_num {
            let mut page_key = PageFrameKey::new(self.c_key, bucket_id as u32);
            let mut current_page = match self.bp.get_page_for_read(page_key) {
                Ok(page) => page,
                Err(_) => continue, // Skip empty buckets
            };

            let mut chain_length = 0;
            loop {
                chain_length += 1;

                let use_rate = current_page.get_use_rate();
                total_use_rate += current_page.get_use_rate();
                if min_use_rate > use_rate {
                    min_use_rate = use_rate;
                }
                if max_use_rate < use_rate {
                    max_use_rate = use_rate;
                }

                if use_rate > 0.9 {
                    page_with_high_use_rate_9 += 1;
                    page_with_high_use_rate_8 += 1;
                } else if use_rate > 0.8 {
                    page_with_high_use_rate_8 += 1;
                }

                if use_rate < 0.1 {
                    page_with_low_use_rate_1 += 1;
                    page_with_low_use_rate_2 += 1;
                } else if use_rate < 0.2 {
                    page_with_low_use_rate_2 += 1;
                }

                let next_page_id = current_page.get_next_page_id();
                if next_page_id == 0 {
                    break;
                }
                let next_frame_id = current_page.get_next_frame_id();
                page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                current_page = match self.bp.get_page_for_read(page_key) {
                    Ok(page) => page,
                    Err(_) => break,
                };
            }
            chain_lengths[bucket_id - 1] = chain_length;
            total_page_count += chain_length;
        }

        let max_chain_len = chain_lengths.iter().max().unwrap_or(&0);
        let min_chain_len = chain_lengths.iter().min().unwrap_or(&0);
        let avg_chain_len = total_page_count as f64 / self.bucket_num as f64;
        let avg_use_rate = total_use_rate / total_page_count as f64;

        let mut distribution = std::collections::HashMap::new();
        for &len in chain_lengths.iter() {
            *distribution.entry(len).or_insert(0) += 1;
        }

        let mut distribution_vec: Vec<_> = distribution.iter().collect();
        distribution_vec.sort();
        let mut distribution_str = String::new();
        for (len, count) in distribution_vec {
            distribution_str.push_str(&format!("Chain length {}: {} buckets\n", len, count));
        }

        format!(
            "Paged Hash Map Chain Statistics\n\
            total_page_count: {}\n\n\
            max_chain_len: {}\n\
            min_chain_len: {}\n\
            avg_chain_len: {:.2}\n\n\
            avg_use_rate: {:.2}\n\
            max_use_rate: {:.2}\n\
            min_use_rate: {:.2}\n\n\
            page_with_use_high_use_rate (>0.9): {}\n\
            rate_of_page_with_use_high_use_rate (>0.9): {:.2}\n\
            page_with_use_high_use_rate (>0.8): {}\n\
            rate_of_page_with_use_high_use_rate (>0.8): {:.2}\n\n\
            page_with_use_low_use_rate (<0.1): {}\n\
            rate_of_page_with_use_low_use_rate (<0.1): {:.2}\n\
            page_with_use_low_use_rate (<0.2): {}\n\
            rate_of_page_with_use_low_use_rate (<0.2): {:.2}\n\n\
            Chain length distribution:\n{}",
            total_page_count,
            max_chain_len,
            min_chain_len,
            avg_chain_len,
            avg_use_rate,
            max_use_rate,
            min_use_rate,
            page_with_high_use_rate_9,
            page_with_high_use_rate_9 as f64 / total_page_count as f64,
            page_with_high_use_rate_8,
            page_with_high_use_rate_8 as f64 / total_page_count as f64,
            page_with_low_use_rate_1,
            page_with_low_use_rate_1 as f64 / total_page_count as f64,
            page_with_low_use_rate_2,
            page_with_low_use_rate_2 as f64 / total_page_count as f64,
            distribution_str
        )
    }

    #[cfg(feature = "stat")]
    pub fn stats(&self) -> String {
        let stats = GLOBAL_STAT.lock().unwrap();
        LOCAL_STAT.with(|local_stat| {
            stats.merge(&local_stat.stat);
            local_stat.stat.clear();
        });
        stats.to_string()
    }

    #[cfg(feature = "stat")]
    pub fn reset_stats(&self) {
        {
            GLOBAL_STAT.lock().unwrap().clear();
        }
    }
}

unsafe impl<E: EvictionPolicy, T: MemPool<E>> Sync for PagedHashMap<E, T> {}
unsafe impl<E: EvictionPolicy, T: MemPool<E>> Send for PagedHashMap<E, T> {}

pub struct PagedHashMapIter<E: EvictionPolicy + 'static, T: MemPool<E>> {
    map: Arc<PagedHashMap<E, T>>,
    current_page: Option<FrameReadGuard<'static, E>>,
    current_index: usize,
    current_bucket: usize,
    initialized: bool,
    finished: bool,
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> PagedHashMapIter<E, T> {
    fn new(map: Arc<PagedHashMap<E, T>>) -> Self {
        PagedHashMapIter {
            map,
            current_page: None,
            current_index: 0,
            current_bucket: 1,
            initialized: false,
            finished: false,
        }
    }

    fn initialize(&mut self) {
        assert!(!self.initialized);
        let page_key = PageFrameKey::new(self.map.c_key, self.map.bucket_head[1]);
        let first_page = unsafe { std::mem::transmute(self.map.read_page(page_key)) };
        self.current_page = Some(first_page);
    }
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> Iterator for PagedHashMapIter<E, T> {
    type Item = (Vec<u8>, Vec<u8>); // Key and value types

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.initialized {
            self.initialize();
            self.initialized = true;
        }

        while let Some(page) = &self.current_page {
            if self.current_index < (page.num_slots() as usize) {
                let sks = page.decode_shortkey_slot(self.current_index as u32);
                let skv = page.decode_shortkey_value_by_id(self.current_index as u32);

                let mut key = sks.key_prefix.to_vec();
                key.extend_from_slice(&skv.remain_key);
                // slice length of sks.key_len
                key.truncate(sks.key_len as usize);

                let value = skv.vals.to_vec();

                self.current_index += 1;
                return Some((key, value)); // Adjust based on actual data structure
            }

            let next_page_id = page.get_next_page_id();

            // If end of current page, fetch next
            if next_page_id != 0 {
                let nex_page_key = PageFrameKey::new(self.map.c_key, next_page_id);
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard<'_, E>, FrameReadGuard<'static, E>>(
                        self.map.read_page(nex_page_key),
                    )
                };
                self.current_page = Some(next_page);
                self.current_index = 0;
                continue;
            }

            // No more pages in the current bucket, move to next bucket
            self.current_bucket += 1;
            if self.current_bucket <= self.map.bucket_num {
                let next_page_key = PageFrameKey::new(
                    self.map.c_key,
                    self.map.bucket_head[self.current_bucket] as u32,
                );
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard<'_, E>, FrameReadGuard<'static, E>>(
                        self.map.read_page(next_page_key),
                    )
                };
                self.current_page = Some(next_page);
                self.current_index = 0;
                continue;
            }

            // All buckets processed
            break;
        }

        self.finished = true;
        None
    }
}

pub struct PagedHashMapIterWithKey<E: EvictionPolicy + 'static, T: MemPool<E>> {
    map: Arc<PagedHashMap<E, T>>,
    current_page: Option<FrameReadGuard<'static, E>>,
    current_index: i64,
    current_bucket: usize,
    initialized: bool,
    finished: bool,
    key: Vec<u8>,
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> PagedHashMapIterWithKey<E, T> {
    fn new(map: Arc<PagedHashMap<E, T>>, key: &[u8]) -> Self {
        PagedHashMapIterWithKey {
            map,
            current_page: None,
            current_index: -1,
            current_bucket: 1,
            initialized: false,
            finished: false,
            key: key.to_vec(),
        }
    }

    fn initialize(&mut self) {
        assert!(!self.initialized);
        let hashed_key = self.map.hash(&self.key);
        let expected_frame_id = if let Some(ref frame_buckets) = self.map.frame_buckets {
            frame_buckets.load(hashed_key as usize)
        } else {
            u32::MAX
        };
        let page_key = PageFrameKey::new_with_frame_id(
            self.map.c_key,
            self.map.bucket_head[hashed_key as usize],
            expected_frame_id,
        );
        let first_page = unsafe { std::mem::transmute(self.map.read_page(page_key)) };
        self.current_page = Some(first_page);
    }
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> Iterator for PagedHashMapIterWithKey<E, T> {
    type Item = Vec<u8>; // value

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.initialized {
            self.initialize();
            self.initialized = true;
        }

        while let Some(page) = &self.current_page {
            if self.current_index == -1 {
                let (found, index) = page.search_slot(self.key.as_ref());

                if found {
                    self.current_index = index as i64;
                }
            }

            if self.current_index != -1 {
                let sks = page.decode_shortkey_slot(self.current_index as u32);
                let skv = page.decode_shortkey_value_by_id(self.current_index as u32);

                let mut key = sks.key_prefix.to_vec();
                key.extend_from_slice(&skv.remain_key);
                // slice length of sks.key_len
                key.truncate(sks.key_len as usize);

                if key == self.key {
                    let value = skv.vals.to_vec();

                    self.current_index += 1;
                    return Some(value);
                }
            }

            let next_page_id = page.get_next_page_id();
            let next_frame_id = if self.map.pointer_swizzling_mode != PointerSwizzlingMode::None {
                page.get_next_frame_id()
            } else {
                u32::MAX
            };

            // If end of current page, fetch next
            if next_page_id != 0 {
                let nex_page_key =
                    PageFrameKey::new_with_frame_id(self.map.c_key, next_page_id, next_frame_id);
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard<'_, E>, FrameReadGuard<'static, E>>(
                        self.map.read_page(nex_page_key),
                    )
                };
                self.current_page = Some(next_page);
                self.current_index = -1;
                continue;
            }

            break;
        }

        self.finished = true;
        None
    }
}

#[cfg(feature = "stat")]
mod stat {
    use super::*;
    use lazy_static::lazy_static;
    use std::{cell::UnsafeCell, sync::Mutex};

    pub struct PagedHashMapStat {
        pub insert_count: UnsafeCell<usize>,
        pub get_count: UnsafeCell<usize>,
        pub update_count: UnsafeCell<usize>,
        pub upsert_count: UnsafeCell<usize>,
        pub upsert_with_merge_count: UnsafeCell<usize>,
        pub remove_count: UnsafeCell<usize>,

        pub total_page_count: UnsafeCell<usize>,
        pub chain_len: UnsafeCell<[usize; DEFAULT_BUCKET_NUM]>,
    }

    impl PagedHashMapStat {
        pub fn new() -> Self {
            PagedHashMapStat {
                insert_count: UnsafeCell::new(0),
                get_count: UnsafeCell::new(0),
                update_count: UnsafeCell::new(0),
                upsert_count: UnsafeCell::new(0),
                upsert_with_merge_count: UnsafeCell::new(0),
                remove_count: UnsafeCell::new(0),
                total_page_count: UnsafeCell::new(0),
                chain_len: UnsafeCell::new([1; DEFAULT_BUCKET_NUM]),
            }
        }

        pub fn inc_insert_count(&self) {
            unsafe {
                *self.insert_count.get() += 1;
            }
        }

        pub fn inc_get_count(&self) {
            unsafe {
                *self.get_count.get() += 1;
            }
        }

        pub fn inc_update_count(&self) {
            unsafe {
                *self.update_count.get() += 1;
            }
        }

        pub fn inc_upsert_count(&self) {
            unsafe {
                *self.upsert_count.get() += 1;
            }
        }

        pub fn inc_upsert_with_merge_count(&self) {
            unsafe {
                *self.upsert_with_merge_count.get() += 1;
            }
        }

        pub fn inc_remove_count(&self) {
            unsafe {
                *self.remove_count.get() += 1;
            }
        }

        pub fn inc_total_page_count(&self) {
            unsafe {
                *self.total_page_count.get() += 1;
            }
        }

        pub fn inc_chain_len(&self, bucket_id: usize) {
            unsafe {
                let chain_len = &mut *self.chain_len.get();
                chain_len[bucket_id] += 1;
            }
        }

        pub fn _update_chain_len(&self, bucket_id: usize, len: usize) {
            unsafe {
                let chain_len = &mut *self.chain_len.get();
                if len > chain_len[bucket_id] {
                    chain_len[bucket_id] = len;
                }
            }
        }

        pub fn to_string(&self) -> String {
            let insert_count = unsafe { *self.insert_count.get() };
            let get_count = unsafe { *self.get_count.get() };
            let update_count = unsafe { *self.update_count.get() };
            let upsert_count = unsafe { *self.upsert_count.get() };
            let upsert_with_merge_count = unsafe { *self.upsert_with_merge_count.get() };
            let remove_count = unsafe { *self.remove_count.get() };
            let total_page_count = unsafe { *self.total_page_count.get() };

            let chain_len = unsafe { &*self.chain_len.get() };
            let max_chain_len = chain_len.iter().max().unwrap_or(&0);
            let min_chain_len = chain_len.iter().min().unwrap_or(&0);
            let avg_chain_len = chain_len.iter().sum::<usize>() as f64 / chain_len.len() as f64;

            // Calculate the distribution of chain lengths
            let mut distribution = std::collections::HashMap::new();
            for &len in chain_len.iter() {
                *distribution.entry(len).or_insert(0) += 1;
            }

            // Create a formatted string for the distribution, sorted by chain length
            let mut distribution_vec: Vec<_> = distribution.iter().collect();
            distribution_vec.sort();
            let mut distribution_str = String::new();
            for (len, count) in distribution_vec {
                distribution_str.push_str(&format!("Chain length {}: {} buckets\n", len, count));
            }

            format!(
                "Paged Hash Map Statistics\n\
                insert_count: {}\n\
                get_count: {}\n\
                update_count: {}\n\
                upsert_count: {}\n\
                upsert_with_merge_count: {}\n\
                remove_count: {}\n\n\
                total_page_count: {}\n\
                max_chain_len: {}\n\
                min_chain_len: {}\n\
                avg_chain_len: {:.2}\n\n\
                Chain length distribution:\n{}",
                insert_count,
                get_count,
                update_count,
                upsert_count,
                upsert_with_merge_count,
                remove_count,
                total_page_count,
                max_chain_len,
                min_chain_len,
                avg_chain_len,
                distribution_str
            )
        }

        pub fn merge(&self, other: &PagedHashMapStat) {
            unsafe {
                *self.insert_count.get() += *other.insert_count.get();
                *self.get_count.get() += *other.get_count.get();
                *self.update_count.get() += *other.update_count.get();
                *self.upsert_count.get() += *other.upsert_count.get();
                *self.upsert_with_merge_count.get() += *other.upsert_with_merge_count.get();
                *self.remove_count.get() += *other.remove_count.get();
                *self.total_page_count.get() += *other.total_page_count.get();

                let self_chain_len = &mut *self.chain_len.get();
                let other_chain_len = &*other.chain_len.get();
                for i in 0..DEFAULT_BUCKET_NUM {
                    self_chain_len[i] = std::cmp::max(self_chain_len[i], other_chain_len[i]);
                }
            }
        }

        pub fn clear(&self) {
            unsafe {
                *self.insert_count.get() = 0;
                *self.get_count.get() = 0;
                *self.update_count.get() = 0;
                *self.upsert_count.get() = 0;
                *self.upsert_with_merge_count.get() = 0;
                *self.remove_count.get() = 0;
                *self.total_page_count.get() = 0;

                let chain_len = &mut *self.chain_len.get();
                for i in 0..DEFAULT_BUCKET_NUM {
                    chain_len[i] = 1; // Reset to default length
                }
            }
        }
    }

    pub struct LocalStat {
        pub stat: PagedHashMapStat,
    }

    impl Drop for LocalStat {
        fn drop(&mut self) {
            GLOBAL_STAT.lock().unwrap().merge(&self.stat);
        }
    }

    lazy_static! {
        pub static ref GLOBAL_STAT: Mutex<PagedHashMapStat> = Mutex::new(PagedHashMapStat::new());
    }

    thread_local! {
        pub static LOCAL_STAT: LocalStat = LocalStat {
            stat: PagedHashMapStat::new()
        };
    }

    pub fn inc_local_stat_insert_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_insert_count();
        });
    }

    pub fn inc_local_stat_get_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_get_count();
        });
    }

    pub fn inc_local_stat_update_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_update_count();
        });
    }

    pub fn inc_local_stat_upsert_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_upsert_count();
        });
    }

    pub fn inc_local_stat_upsert_with_merge_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_upsert_with_merge_count();
        });
    }

    pub fn inc_local_stat_remove_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_remove_count();
        });
    }

    pub fn inc_local_stat_total_page_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_total_page_count();
        });
    }

    pub fn inc_local_stat_chain_len(bucket_id: usize) {
        LOCAL_STAT.with(|s| {
            s.stat.inc_chain_len(bucket_id);
        });
    }

    pub fn _update_local_stat_chain_len(bucket_id: usize, len: usize) {
        LOCAL_STAT.with(|s| {
            s.stat._update_chain_len(bucket_id, len);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashMap;
    use std::sync::Arc;

    // A simple hash function mimic for testing
    fn simple_hash_func(old_val: &[u8], new_val: &[u8]) -> Vec<u8> {
        [old_val, new_val].concat()
    }

    // Initialize the PagedHashMap for testing
    fn setup_paged_hash_map<E: EvictionPolicy, T: MemPool<E>>(bp: Arc<T>) -> PagedHashMap<E, T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        // let func = Box::new(simple_hash_func);
        // PagedHashMap::new(func, bp, c_key, false)
        PagedHashMap::new(
            bp,
            c_key,
            DEFAULT_BUCKET_NUM,
            false,
            PointerSwizzlingMode::AtomicShared,
        )
    }

    /// Helper function to generate random strings of a given length
    fn random_string(length: usize) -> Vec<u8> {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(|c| c as u8)
            .collect()
    }

    #[test]
    fn test_insert_and_get() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        assert!(
            map.insert(key, value).is_ok(),
            "Insert should succeed and return Ok on first insert"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value.to_vec(),
            "Retrieved value should match inserted value"
        );
    }

    #[test]
    fn test_update_existing_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();
        let func = simple_hash_func;

        assert!(map.upsert_with_merge(key, value1, func).is_ok());
        assert!(map.upsert_with_merge(key, value2, func).is_ok());

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            [value1, value2].concat(),
            "Retrieved value should be concatenated values"
        );
    }

    #[test]
    fn test_page_overflow_and_chain_handling() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key1 = "key1".as_bytes();
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Half page size to simulate near-full page
        let key2 = "key2".as_bytes();
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Another half to trigger page overflow

        let func = simple_hash_func;

        assert!(map.upsert_with_merge(key1, &value1, func).is_ok());
        assert!(map.upsert_with_merge(key2, &value2, func).is_ok()); // Should trigger handling of a new page in chain

        let retrieved_value1 = map.get(key1).unwrap();
        let retrieved_value2 = map.get(key2).unwrap();
        assert_eq!(retrieved_value1, value1, "First key should be retrievable");
        assert_eq!(
            retrieved_value2, value2,
            "Second key should be retrievable in a new page"
        );
    }

    #[test]
    fn test_edge_case_key_value_sizes() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = random_string(AVAILABLE_PAGE_SIZE / 4); // Large key
        let value = random_string(AVAILABLE_PAGE_SIZE / 2 - SHORT_KEY_PAGE_HEADER_SIZE - 10); // Large value

        let func = simple_hash_func;

        assert!(
            map.upsert_with_merge(&key, &value, func).is_ok(),
            "Should handle large sizes without panic"
        );
        assert_eq!(
            map.get(&key).unwrap(),
            value,
            "Should retrieve large value correctly"
        );
    }

    #[test]
    fn test_sequential_upsert_with_merge_multiple_pages() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut last_key = vec![];

        let func = simple_hash_func;

        for i in 0..(DEFAULT_BUCKET_NUM * 10) {
            // upsert_with_merge more than the default number of buckets
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            assert!(map.upsert_with_merge(&key, &value, func).is_ok());
            last_key = key.clone();

            // Check insertion correctness
            assert_eq!(
                map.get(&key).unwrap(),
                value,
                "Each upserted key should retrieve correctly"
            );
        }

        // Ensure the last upserted key is still retrievable, implying multi-page handling
        assert!(
            map.get(&last_key).is_ok(),
            "The last key should still be retrievable, indicating multi-page handling"
        );
    }

    #[test]
    fn test_random_operations_without_remove() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        let func = simple_hash_func;

        for _ in 0..200000 {
            let operation: u8 = rng.gen_range(0..3);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // Insert
                    map.upsert(&key, &value).unwrap();
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.upsert_with_merge(&key, &value, func).unwrap();
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    match expected {
                        Some(v) => {
                            assert_eq!(
                                map.get(&key).unwrap(),
                                v.clone(),
                                "Mismatched values on get"
                            );
                        }
                        None => {
                            assert!(
                                map.get(&key).is_err(),
                                "Get should return an error if key is not in data"
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_iterator_basic() {
        let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut expected_data = HashMap::new();

        let func = simple_hash_func;

        // upsert_with_merge a few key-value pairs
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.upsert_with_merge(&key, &value, func).unwrap();
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all upserted data was iterated over correctly
        assert_eq!(
            expected_data, iterated_data,
            "Iterator did not retrieve all key-value pairs correctly"
        );
    }

    #[test]
    fn test_iterator_across_pages() {
        let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut expected_data = HashMap::new();

        let func = simple_hash_func;

        // upsert_with_merge more key-value pairs than would fit on a single page
        for i in 0..1000 {
            // Adjust the count according to the capacity of a single page
            let key = format!("key_large_{}", i).into_bytes();
            let value = format!("large_value_{}", i).into_bytes();
            map.upsert_with_merge(&key, &value, func).unwrap();
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all upserted data was iterated over correctly
        assert_eq!(
            expected_data, iterated_data,
            "Iterator did not handle page overflow correctly"
        );
    }

    #[test]
    fn test_insert_existing_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        map.insert(key, value1).unwrap();
        assert!(
            map.insert(key, value2).is_err(),
            "Insert should return Err when key already exists"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value1.to_vec(),
            "Retrieved value should match the initially inserted value"
        );
    }

    #[test]
    fn test_update_existing_key2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        map.insert(key, value1).unwrap();
        assert!(map.update(key, value2).is_ok());

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value2.to_vec(),
            "Retrieved value should match the updated value"
        );
    }

    #[test]
    fn test_update_non_existent_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "value".as_bytes();

        assert!(
            map.update(key, value).is_err(),
            "Update should return Err when key does not exist"
        );
    }

    #[test]
    fn test_upsert() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        assert!(map.upsert(key, value1).is_ok());
        assert!(map.upsert(key, value2).is_ok());

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value2.to_vec(),
            "Retrieved value should match the last upserted value"
        );
    }

    #[test]
    fn test_remove_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        map.insert(key, value).unwrap();
        assert_eq!(
            map.get(key).unwrap(),
            value.to_vec(),
            "Key should be retrievable after insert"
        );

        assert!(map.remove(key).is_ok());

        assert!(
            map.get(key).is_err(),
            "Get should return an error after key is removed"
        );
    }

    #[test]
    fn test_remove_key2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        map.insert(key, value).unwrap();
        assert!(map.remove(key).is_ok());

        assert!(
            map.get(key).is_err(),
            "Key should no longer exist in the map"
        );
    }

    #[test]
    fn test_page_overflow_and_chain_handling2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key1 = "key1".as_bytes();
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Half page size to simulate near-full page
        let key2 = "key2".as_bytes();
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Another half to trigger page overflow

        map.insert(key1, &value1).unwrap();
        map.insert(key2, &value2).unwrap(); // Should trigger handling of a new page in chain

        let retrieved_value1 = map.get(key1).unwrap();
        let retrieved_value2 = map.get(key2).unwrap();
        assert_eq!(retrieved_value1, value1, "First key should be retrievable");
        assert_eq!(
            retrieved_value2, value2,
            "Second key should be retrievable in a new page"
        );
    }

    #[test]
    fn test_edge_case_key_value_sizes2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = random_string(AVAILABLE_PAGE_SIZE / 4); // Large key
        let value = random_string(AVAILABLE_PAGE_SIZE / 2 - SHORT_KEY_PAGE_HEADER_SIZE - 10); // Large value

        assert!(
            map.upsert(&key, &value).is_ok(),
            "Should handle large sizes without panic"
        );
        assert_eq!(
            map.get(&key).unwrap(),
            value,
            "Should retrieve large value correctly"
        );
    }

    #[test]
    fn test_random_operations() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        let func = simple_hash_func;

        for _ in 0..1000 {
            let operation: u8 = rng.gen_range(0..4);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // Insert
                    map.upsert(&key, &value).unwrap();
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.upsert_with_merge(&key, &value, func).unwrap();
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    match expected {
                        Some(v) => {
                            assert_eq!(
                                map.get(&key).unwrap(),
                                v.clone(),
                                "Mismatched values on get"
                            );
                        }
                        None => {
                            assert!(
                                map.get(&key).is_err(),
                                "Get should return an error if key is not in data"
                            );
                        }
                    }
                }
                3 if !inserted_keys.is_empty() => {
                    // Remove
                    let remove_index = rng.gen_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_ok(),
                        "Remove should return Ok for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_err(),
                        "Get should return Err after remove"
                    );
                    data.remove(&remove_key);
                }
                _ => {} // Skip remove if no keys are available
            }
        }
    }

    #[test]
    fn test_random_operations2() {
        let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        for _ in 0..1000 {
            let operation: u8 = rng.gen_range(0..4);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // Insert
                    map.upsert(&key, &value).unwrap();
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.update(&key, &value).unwrap();
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    match expected {
                        Some(v) => {
                            assert_eq!(
                                map.get(&key).unwrap(),
                                v.clone(),
                                "Mismatched values on get"
                            );
                        }
                        None => {
                            assert!(
                                map.get(&key).is_err(),
                                "Get should return an error if key is not in data"
                            );
                        }
                    }
                }
                3 if !inserted_keys.is_empty() => {
                    // Remove
                    let remove_index = rng.gen_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_ok(),
                        "Remove should return Ok for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_err(),
                        "Get should return Err after remove"
                    );
                    data.remove(&remove_key);
                }
                _ => {} // Skip remove if no keys are available
            }
        }
    }

    #[test]
    fn test_sequential_inserts_multiple_pages() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut last_key = vec![];

        for i in 0..(DEFAULT_BUCKET_NUM * 10) {
            // Insert more than the default number of buckets
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.upsert(&key, &value).unwrap();
            last_key = key.clone();

            // Check insertion correctness
            assert_eq!(
                map.get(&key).unwrap(),
                value,
                "Each inserted key should retrieve correctly"
            );
        }

        // Ensure the last inserted key is still retrievable, implying multi-page handling
        assert!(
            map.get(&last_key).is_ok(),
            "The last key should still be retrievable, indicating multi-page handling"
        );
    }

    #[test]
    fn test_large_value_insert_and_get() {
        let map = setup_paged_hash_map(get_in_mem_pool());

        // Test cases for large values
        let large_values = vec![
            random_string(AVAILABLE_PAGE_SIZE + 1), // Slightly more than a page size
            random_string(AVAILABLE_PAGE_SIZE * 2), // Twice the page size
            random_string(AVAILABLE_PAGE_SIZE * 3), // Three times the page size
        ];

        for (i, value) in large_values.iter().enumerate() {
            let test_key = format!("large_value_key_{}", i).into_bytes();
            assert!(
                map.insert(&test_key, value).is_ok(),
                "Insert should succeed for large value"
            );

            let retrieved_value = map.get(&test_key).unwrap();
            assert_eq!(
                retrieved_value, *value,
                "Retrieved value should match inserted large value"
            );
        }
    }

    #[test]
    fn test_large_and_non_large_value_insert_and_get() {
        let map = setup_paged_hash_map(get_in_mem_pool());

        // Non-large values (fit within a single page)
        let small_key1 = "small_key1".as_bytes();
        let small_value1 = "small_value1".as_bytes();

        let small_key2 = "small_key2".as_bytes();
        let small_value2 = "another_small_value".as_bytes();

        // Large values (span multiple pages)
        let large_key1 = "large_key1".as_bytes();
        let large_value1 = random_string(AVAILABLE_PAGE_SIZE + 100); // Slightly larger than one page

        let large_key2 = "large_key2".as_bytes();
        let large_value2 = random_string(AVAILABLE_PAGE_SIZE * 2 + 500); // Larger than two pages

        // Insert small values
        assert!(
            map.insert(small_key1, small_value1).is_ok(),
            "Insert should succeed for small value 1"
        );
        assert!(
            map.insert(small_key2, small_value2).is_ok(),
            "Insert should succeed for small value 2"
        );

        // Insert large values
        assert!(
            map.insert(large_key1, &large_value1).is_ok(),
            "Insert should succeed for large value 1"
        );
        assert!(
            map.insert(large_key2, &large_value2).is_ok(),
            "Insert should succeed for large value 2"
        );

        // Retrieve and verify small values
        assert_eq!(
            map.get(small_key1).unwrap(),
            small_value1,
            "Retrieved value should match inserted small value 1"
        );
        assert_eq!(
            map.get(small_key2).unwrap(),
            small_value2,
            "Retrieved value should match inserted small value 2"
        );

        // Retrieve and verify large values
        assert_eq!(
            map.get(large_key1).unwrap(),
            large_value1,
            "Retrieved value should match inserted large value 1"
        );
        assert_eq!(
            map.get(large_key2).unwrap(),
            large_value2,
            "Retrieved value should match inserted large value 2"
        );
    }

    #[test]
    fn stress_test_multiple_inserts() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = rand::thread_rng();
        let mut inserted_data = HashMap::new();

        // Define the number of inserts
        let num_inserts = 100;

        for i in 0..num_inserts {
            // Generate a random key
            let key = format!("stress_key_{}", i).into_bytes();

            // Randomly decide if the value is large or small
            let is_large_value = rng.gen_bool(0.3); // 30% chance to be a large value

            let value = if is_large_value {
                // Generate a large value (between 1 and 3 times the page size)
                let size_factor = rng.gen_range(1..=3);
                random_string(AVAILABLE_PAGE_SIZE * size_factor + rng.gen_range(0..100))
            } else {
                // Generate a small value (less than half the page size)
                random_string(rng.gen_range(1..AVAILABLE_PAGE_SIZE / 2))
            };

            // Insert into the map and track the data
            assert!(
                map.insert(&key, &value).is_ok(),
                "Insert should succeed for key: {:?}",
                key
            );
            inserted_data.insert(key.clone(), value.clone());
        }

        // Verify all inserted data is retrievable
        for (key, expected_value) in &inserted_data {
            let retrieved_value = map.get(key).unwrap();
            assert_eq!(
                retrieved_value, *expected_value,
                "Retrieved value should match inserted value for key: {:?}",
                key
            );
        }
    }

    // #[test]
    // fn test_concurrent_inserts_and_gets() {
    //     let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
    //     let barrier = Arc::new(std::sync::Barrier::new(10));
    //     let mut handles = vec![];

    //     for _ in 0..10 {
    //         let map_clone = map.clone();
    //         let barrier_clone = barrier.clone();
    //         handles.push(std::thread::spawn(move || {
    //             barrier_clone.wait();
    //             let key = random_string(10);
    //             let value = random_string(20);
    //             map_clone.insert(&key, &value).unwrap();
    //             assert_eq!(map_clone.get(&key).unwrap(), value);
    //         }));
    //     }

    //     for handle in handles {
    //         handle.join().expect("Thread panicked");
    //     }
    // }

    // #[test]
    // fn stress_test_concurrent_operations() {
    //     let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
    //     let num_threads = 10; // Number of concurrent threads
    //     let num_operations_per_thread = 1000; // Operations per thread
    //     let barrier = Arc::new(Barrier::new(num_threads));
    //     let mut handles = vec![];

    //     for _ in 0..num_threads {
    //         let map_clone = Arc::clone(&map);
    //         let barrier_clone = Arc::clone(&barrier);
    //         handles.push(thread::spawn(move || {
    //             barrier_clone.wait(); // Ensure all threads start at the same time
    //             let mut rng = thread_rng();

    //             for _ in 0..num_operations_per_thread {
    //                 let key = random_string(10);
    //                 let value = random_string(20);
    //                 let operation: u8 = rng.gen_range(0..4);

    //                 match operation {
    //                     0 => { // Insert
    //                         map_clone.insert(&key, &value);
    //                     },
    //                     1 if !map_clone.get(&key).is_none() => { // Update only if key exists
    //                         map_clone.insert(&key, &value);
    //                     },
    //                     2 => { // Get
    //                         let _ = map_clone.get(&key);
    //                     },
    //                     3 if !map_clone.get(&key).is_none() => { // Remove only if key exists
    //                         map_clone.remove(&key);
    //                     },
    //                     _ => {}
    //                 }
    //             }
    //         }));
    //     }

    //     for handle in handles {
    //         handle.join().expect("Thread panicked during execution");
    //     }

    //     // Optionally, perform any final checks or cleanup here
    // }
}
