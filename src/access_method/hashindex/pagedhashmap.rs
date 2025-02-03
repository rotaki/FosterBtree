#![allow(unused_imports)]
use crate::{access_method::AccessMethodError, logger::log};

use core::panic;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

#[cfg(feature = "stat")]
use stat::*;

use crate::bp::prelude::*;

use crate::{log_info, page::PageId};

use super::shortkeypage::{ShortKeyPage, ShortKeyPageError, SHORT_KEY_PAGE_HEADER_SIZE};
use crate::page::AVAILABLE_PAGE_SIZE;

// const DEFAULT_BUCKET_NUM: usize = 1024;
const DEFAULT_BUCKET_NUM: usize = 1024 * 4;

pub struct PagedHashMap<T: MemPool> {
    // func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>, // func(old_value, new_value) -> new_value
    pub bp: Arc<T>,
    c_key: ContainerKey,

    pub bucket_num: usize, // number of hash header pages
    frame_buckets: Vec<AtomicU32>, // vec of frame_id for each bucket
                           // bucket_metas: Vec<BucketMeta>, // first_frame_id, last_page_id, last_frame_id, bloomfilter // no need to be page
}

struct _BucketMeta {
    first_frame_id: u32,
    last_page_id: u32,
    last_frame_id: u32,
    bloomfilter: Vec<u8>,
}

/// Opportunistically try to fix the child page frame id
fn fix_frame_id<'a>(this: FrameReadGuard<'a>, new_frame_key: &PageFrameKey) -> FrameReadGuard<'a> {
    match this.try_upgrade(true) {
        Ok(mut write_guard) => {
            write_guard.set_next_frame_id(new_frame_key.frame_id());
            write_guard.downgrade()
        }
        Err(read_guard) => read_guard,
    }
}

impl<T: MemPool> PagedHashMap<T> {
    pub fn new(
        // func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>,
        bp: Arc<T>,
        c_key: ContainerKey,
        from_container: bool,
    ) -> Self {
        if from_container {
            todo!("Implement from container");
            // let bucket_num = bp.get_page_for_read(c_id, 0, |root_page| {
            //     <Page as HashMetaPage>::get_n(root_page)
            // });
            // PagedHashMap {
            //     func,
            //     bp,
            //     c_id,
            //     bucket_num,
            // }
        } else {
            let mut frame_buckets = (0..DEFAULT_BUCKET_NUM + 1)
                .map(|_| AtomicU32::new(u32::MAX))
                .collect::<Vec<AtomicU32>>();
            //vec![AtomicU32::new(u32::MAX); DEFAULT_BUCKET_NUM + 1];

            // SET ROOT: Need to do something for the root page e.g. set n
            let root_page = bp.create_new_page_for_write(c_key).unwrap();
            #[cfg(feature = "stat")]
            inc_local_stat_total_page_count();
            log_info!(
                "Root page id: {}, Need to set root page",
                root_page.get_id()
            );
            assert_eq!(root_page.get_id(), 0, "root page id should be 0");
            frame_buckets[0].store(root_page.frame_id(), std::sync::atomic::Ordering::Release);
            //root_page.frame_id();

            // SET HASH BUCKET PAGES
            for (i, frame_bucket) in frame_buckets
                .iter_mut()
                .enumerate()
                .skip(1)
                .take(DEFAULT_BUCKET_NUM)
            {
                let mut new_page = bp.create_new_page_for_write(c_key).unwrap();
                #[cfg(feature = "stat")]
                inc_local_stat_total_page_count();
                frame_bucket.store(new_page.frame_id(), std::sync::atomic::Ordering::Release);
                new_page.init();
                assert_eq!(
                    new_page.get_id() as usize,
                    i,
                    "Initial new page id should be {}",
                    i
                );
            }

            PagedHashMap {
                // func,
                bp: bp.clone(),
                c_key,
                bucket_num: DEFAULT_BUCKET_NUM,
                frame_buckets,
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
    pub fn insert<K: AsRef<[u8]> + Hash>(&self, key: K, val: K) -> Result<(), AccessMethodError> {
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();

        let required_space = key.as_ref().len() + val.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);

        let mut last_page = self.insert_traverse_to_endofchain_for_write(page_key, key.as_ref())?;
        match last_page.insert(key.as_ref(), val.as_ref()) {
            Ok(_) => Ok(()),
            Err(ShortKeyPageError::KeyExists) => {
                panic!("Key exists should detected in traverse_to_endofchain_for_write")
            }
            Err(ShortKeyPageError::OutOfSpace) => {
                let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                #[cfg(feature = "stat")]
                {
                    inc_local_stat_total_page_count();
                    inc_local_stat_chain_len(hashed_key as usize - 1);
                }

                new_page.init();
                last_page.set_next_page_id(new_page.get_id());
                last_page.set_next_frame_id(new_page.frame_id());
                match new_page.insert(key.as_ref(), val.as_ref()) {
                    Ok(_) => Ok(()),
                    Err(err) => panic!("Inserting to new page should succeed: {:?}", err),
                }
            }
            Err(err) => Err(AccessMethodError::Other(format!("Insert error: {:?}", err))),
        }
    }

    fn insert_traverse_to_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = Duration::from_millis(1);
        let mut attempts = 0;
        loop {
            let page = self.try_insert_traverse_to_endofchain_for_write(page_key, key);
            match page {
                Ok(page) => {
                    return Ok(page);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(AccessMethodError::KeyDuplicate) => {
                    return Err(AccessMethodError::KeyDuplicate);
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
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        if current_page.frame_id() != page_key.frame_id() {
            self.frame_buckets[page_key.p_key().page_id as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
        }
        loop {
            let (slot_id, _) = current_page.is_exist(key);
            if slot_id.is_some() {
                return Err(AccessMethodError::KeyDuplicate);
            }
            let next_page_id = current_page.get_next_page_id();
            if next_page_id == 0 {
                match current_page.try_upgrade(true) {
                    Ok(upgraded_page) => {
                        return Ok(upgraded_page);
                    }
                    Err(_) => {
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                };
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

    // read_page and update frame_buckets
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.bp.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    // if page_key.p_key().page_id <= DEFAULT_BUCKET_NUM as u32
                    //     && page.frame_id() != page_key.frame_id()
                    // {
                    //     self.frame_buckets[page_key.p_key().page_id as usize]
                    //         .store(page.frame_id(), std::sync::atomic::Ordering::Release);
                    // }
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

    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
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
    pub fn update<K: AsRef<[u8]> + Hash>(&self, key: K, val: K) -> Result<(), AccessMethodError> {
        #[cfg(feature = "stat")]
        inc_local_stat_update_count();

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);

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
            Err(err) => Err(AccessMethodError::Other(format!("Update error: {:?}", err))),
        }
    }

    fn update_traverse_to_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<(FrameWriteGuard, u16), AccessMethodError> {
        let base = Duration::from_millis(1);
        let mut attempts = 0;
        loop {
            let page = self.try_update_traverse_to_endofchain_for_write(page_key, key);
            match page {
                Ok(page) => {
                    return Ok(page);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    return Err(AccessMethodError::KeyNotFound);
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
    ) -> Result<(FrameWriteGuard, u16), AccessMethodError> {
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
                            return Err(AccessMethodError::PageWriteLatchFailed);
                        }
                    };
                }
                None => {
                    let next_page_id = current_page.get_next_page_id();
                    if next_page_id == 0 {
                        return Err(AccessMethodError::KeyNotFound);
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
    /// If the key already exists, it will update the value and return old value.
    /// If the key does not exist, it will insert a new key-value pair.
    pub fn upsert<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
    ) -> Result<Option<Vec<u8>>, AccessMethodError> {
        #[cfg(feature = "stat")]
        inc_local_stat_upsert_count();

        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
        }

        loop {
            match current_page.upsert(key.as_ref(), value.as_ref()) {
                (true, old_value) => {
                    return Ok(old_value);
                }
                (false, _old_value) => {
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

        match new_page.upsert(key.as_ref(), value.as_ref()) {
            (true, old_value) => Ok(old_value),
            (false, _) => Err(AccessMethodError::RecordTooLarge),
        }
    }

    /// Upsert with a custom merge function.
    /// If the key already exists, it will update the value with the merge function.
    /// If the key does not exist, it will insert a new key-value pair.
    pub fn upsert_with_merge<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
        merge: fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Option<Vec<u8>> {
        #[cfg(feature = "stat")]
        inc_local_stat_upsert_with_merge_count();

        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
            // self.frame_buckets[hashed_key as usize] = current_page.frame_id();
        }

        let mut current_value;

        loop {
            let inserted;
            (inserted, current_value) =
                current_page.upsert_with_merge(key.as_ref(), value.as_ref(), merge);
            if inserted {
                #[cfg(feature = "stat")]
                return current_value;
            }
            let (next_page_id, next_frame_id) = (
                current_page.get_next_page_id(),
                current_page.get_next_frame_id(),
            );

            if next_page_id == 0 {
                break;
            }
            if current_value.is_some() {
                break;
            }
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_write(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page.set_next_frame_id(next_page.frame_id());
            }
            current_page = next_page;
        }

        // 2 cases:
        // - no next page in chain: need to create new page. (next_page_id == 0)
        // - current_page is full and has next page in chain: need to move to next page in chain. (current_value.is_some())

        // Apply the function on existing value or insert new
        let new_value = if let Some(ref val) = current_value {
            (merge)(val, value.as_ref())
        } else {
            value.as_ref().to_vec()
        };

        let mut next_page_id = current_page.get_next_page_id();

        while next_page_id != 0 {
            let next_frame_id = current_page.get_next_frame_id();

            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_write(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page.set_next_frame_id(next_page.frame_id());
            }
            current_page = next_page;

            let (inserted, _) = current_page.upsert(key.as_ref(), &new_value);
            if inserted {
                return current_value;
            }
            next_page_id = current_page.get_next_page_id();
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

        new_page.upsert(key.as_ref(), &new_value);
        current_value
    }

    /// Get the value of a key from the index.
    /// If the key does not exist, it will return an error.
    pub fn get<K: AsRef<[u8]> + Hash>(&self, key: K) -> Result<Vec<u8>, AccessMethodError> {
        #[cfg(feature = "stat")]
        inc_local_stat_get_count();

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.read_page(page_key);
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
        }

        let mut result;

        loop {
            result = current_page.get(key.as_ref());
            let (next_page_id, next_frame_id) = (
                current_page.get_next_page_id(),
                current_page.get_next_frame_id(),
            );
            if result.is_some() || next_page_id == 0 {
                break;
            }
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.read_page(page_key);
            if next_frame_id != next_page.frame_id() {
                page_key.set_frame_id(next_page.frame_id());
                let _ = fix_frame_id(current_page, &page_key);
            }
            current_page = next_page;
        }
        match result {
            Some(v) => Ok(v),
            None => Err(AccessMethodError::KeyNotFound),
        }
    }

    pub fn remove<K: AsRef<[u8]> + Hash>(&self, key: K) -> Option<Vec<u8>> {
        #[cfg(feature = "stat")]
        {
            inc_local_stat_remove_count();
        }

        let mut page_key = PageFrameKey::new(self.c_key, self.hash(&key));
        let mut result;

        loop {
            let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
            result = current_page.remove(key.as_ref());
            if result.is_some() || current_page.get_next_page_id() == 0 {
                break;
            }
            page_key = PageFrameKey::new(self.c_key, current_page.get_next_page_id());
        }
        result
    }

    pub fn iter(self: &Arc<Self>) -> PagedHashMapIter<T> {
        PagedHashMapIter::new(self.clone())
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
}

unsafe impl<T: MemPool> Sync for PagedHashMap<T> {}
unsafe impl<T: MemPool> Send for PagedHashMap<T> {}

pub struct PagedHashMapIter<T: MemPool> {
    map: Arc<PagedHashMap<T>>,
    current_page: Option<FrameReadGuard<'static>>,
    current_index: usize,
    current_bucket: usize,
    initialized: bool,
    finished: bool,
}

impl<T: MemPool> PagedHashMapIter<T> {
    fn new(map: Arc<PagedHashMap<T>>) -> Self {
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
        let page_key = PageFrameKey::new(self.map.c_key, 1);
        let first_page = unsafe {
            std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(
                self.map.read_page(page_key),
            )
        };
        self.current_page = Some(first_page);
    }
}

impl<T: MemPool> Iterator for PagedHashMapIter<T> {
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
                let sks = page.decode_shortkey_slot(self.current_index as u16);
                let skv = page.decode_shortkey_value_by_id(self.current_index as u16);

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
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(
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
                let next_page_key = PageFrameKey::new(self.map.c_key, self.current_bucket as u32);
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(
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
    use crate::random::small_thread_rng;

    use super::*;
    use rand::{distr::Alphanumeric, Rng};
    use std::collections::HashMap;
    use std::sync::Arc;

    // A simple hash function mimic for testing
    fn simple_hash_func(old_val: &[u8], new_val: &[u8]) -> Vec<u8> {
        [old_val, new_val].concat()
    }

    // Initialize the PagedHashMap for testing
    fn setup_paged_hash_map<T: MemPool>(bp: Arc<T>) -> PagedHashMap<T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        // let func = Box::new(simple_hash_func);
        // PagedHashMap::new(func, bp, c_key, false)
        PagedHashMap::new(bp, c_key, false)
    }

    /// Helper function to generate random strings of a given length
    fn random_string(length: usize) -> Vec<u8> {
        small_thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
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

        map.upsert_with_merge(key, value1, func);
        let old_value = map.upsert_with_merge(key, value2, func);
        assert_eq!(
            old_value,
            Some(value1.to_vec()),
            "Insert should return old value on update"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            [value1, value2].concat(),
            "Retrieved value should be concatenated values"
        );
    }

    #[test]
    #[ignore]
    fn test_page_overflow_and_chain_handling() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key1 = "key1".as_bytes();
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Half page size to simulate near-full page
        let key2 = "key2".as_bytes();
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Another half to trigger page overflow

        let func = simple_hash_func;

        map.upsert_with_merge(key1, &value1, func);
        map.upsert_with_merge(key2, &value2, func); // Should trigger handling of a new page in chain

        let retrieved_value1 = map.get(key1).unwrap();
        let retrieved_value2 = map.get(key2).unwrap();
        assert_eq!(retrieved_value1, value1, "First key should be retrievable");
        assert_eq!(
            retrieved_value2, value2,
            "Second key should be retrievable in a new page"
        );
    }

    #[test]
    #[ignore]
    fn test_edge_case_key_value_sizes() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = random_string(AVAILABLE_PAGE_SIZE / 4); // Large key
        let value = random_string(AVAILABLE_PAGE_SIZE / 2 - SHORT_KEY_PAGE_HEADER_SIZE - 10); // Large value

        let func = simple_hash_func;

        assert!(
            map.upsert_with_merge(&key, &value, func).is_none(),
            "Should handle large sizes without panic"
        );
        assert_eq!(
            map.get(&key).unwrap(),
            value,
            "Should retrieve large value correctly"
        );
    }

    #[test]
    #[ignore]
    fn test_sequential_upsert_with_merge_multiple_pages() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut last_key = vec![];

        let func = simple_hash_func;

        for i in 0..(DEFAULT_BUCKET_NUM * 10) {
            // upsert_with_merge more than the default number of buckets
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.upsert_with_merge(&key, &value, func);
            last_key = key.clone();

            // Check insertion correctness
            assert_eq!(
                map.get(&key).unwrap(),
                value,
                "Each upsert_with_mergeed key should retrieve correctly"
            );
        }

        // Ensure the last upsert_with_mergeed key is still retrievable, implying multi-page handling
        assert!(
            map.get(&last_key).is_ok(),
            "The last key should still be retrievable, indicating multi-page handling"
        );
    }

    #[test]
    fn test_random_operations_without_remove() {
        // let mut map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = small_thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        let func = simple_hash_func;

        for _ in 0..20000 {
            let operation: u8 = rng.random_range(0..3);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // upsert_with_merge
                    map.upsert_with_merge(&key, &value, func);
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.upsert_with_merge(&key, &value, func);
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
                    // assert_eq!(map.get(&key), expected.cloned(), "Mismatched values on get");
                }
                3 if !inserted_keys.is_empty() => {
                    // Remove
                    let remove_index = rng.random_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_some(),
                        "Remove should return Some for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_err(),
                        "Get should return None after remove"
                    );
                    data.remove(&remove_key);
                }
                _ => {} // Skip remove if no keys are available
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
            map.upsert_with_merge(&key, &value, func);
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all upsert_with_mergeed data was iterated over correctly
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
            map.upsert_with_merge(&key, &value, func);
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all upsert_with_mergeed data was iterated over correctly
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
        map.update(key, value2).unwrap();
        // let old_value = map.update(key, value2).unwrap();
        // assert_eq!(old_value, value1.to_vec(), "Update should return old value");

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

        let old_value = map.upsert(key, value1).unwrap();
        assert_eq!(
            old_value, None,
            "Upsert should return None when key is newly inserted"
        );

        let old_value = map.upsert(key, value2).unwrap();
        assert_eq!(
            old_value,
            Some(value1.to_vec()),
            "Upsert should return old value when key is updated"
        );

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
        // let func = simple_hash_func;

        // map.upsert_with_merge(key, value, func);
        map.insert(key, value).unwrap();
        assert_eq!(
            map.get(key).unwrap(),
            value.to_vec(),
            "Key should be retrievable after insert"
        );

        let removed_value = map.remove(key);
        assert_eq!(
            removed_value,
            Some(value.to_vec()),
            "Remove should return the removed value"
        );

        let retrieved_value = map.get(key);
        print!("retrieved_value: {:?}", retrieved_value);
        // check error
        assert!(
            retrieved_value.is_err(),
            "Get should return an error after key is removed"
        );
    }

    #[test]
    fn test_remove_key2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        map.insert(key, value).unwrap();
        let removed_value = map.remove(key).unwrap();
        assert_eq!(
            removed_value,
            value.to_vec(),
            "Remove should return the removed value"
        );

        let retrieved_value = map.get(key);
        assert!(
            retrieved_value.is_err(),
            "Key should no longer exist in the map"
        );
    }

    #[test]
    #[ignore]
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
    #[ignore]
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
        // let mut map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = small_thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        let func = simple_hash_func;

        for _ in 0..1000 {
            let operation: u8 = rng.random_range(0..4);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // upsert_with_merge
                    map.insert(&key, &value).unwrap();
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.upsert_with_merge(&key, &value, func);
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    // if key is not in data, map.get is error
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
                    let remove_index = rng.random_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_some(),
                        "Remove should return Some for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_err(),
                        "Get should return None after remove"
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
        let mut rng = small_thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        for _ in 0..1000 {
            let operation: u8 = rng.random_range(0..4);
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
                    // assert_eq!(map.get(&key).unwrap(), expected.cloned().unwrap(), "Mismatched values on get");
                }
                3 if !inserted_keys.is_empty() => {
                    // Remove
                    let remove_index = rng.random_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_some(),
                        "Remove should return Some for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_err(),
                        "Get should return None after remove"
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
    //             let mut rng = small_thread_rng();

    //             for _ in 0..num_operations_per_thread {
    //                 let key = random_string(10);
    //                 let value = random_string(20);
    //                 let operation: u8 = rng.random_range(0..4);

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
