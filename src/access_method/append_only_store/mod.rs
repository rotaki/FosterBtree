mod append_only_page;

use append_only_page::AppendOnlyPage;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Duration,
};

use crate::{
    bp::{FrameReadGuard, FrameWriteGuard, MemPoolStatus},
    page::{Page, PageId},
    prelude::{ContainerKey, EvictionPolicy, MemPool, PageFrameKey},
};

pub mod prelude {
    pub use super::{AppendOnlyStore, AppendOnlyStoreError, AppendOnlyStoreScanner};
}

#[derive(Debug, PartialEq)]
pub enum AppendOnlyStoreError {
    PageFull,
    PageNotFound,
    RecordTooLarge,
}

struct RuntimeStats {
    num_recs: AtomicUsize,
    num_pages: AtomicUsize,
}

impl RuntimeStats {
    fn new() -> Self {
        RuntimeStats {
            num_recs: AtomicUsize::new(0),
            num_pages: AtomicUsize::new(0),
        }
    }

    fn inc_num_recs(&self) {
        self.num_recs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_num_recs(&self) -> usize {
        self.num_recs.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn inc_num_pages(&self) {
        self.num_pages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_num_pages(&self) -> usize {
        self.num_pages.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// In the append-only store, the pages forms a one-way linked list which we call a chain.
/// The first page is called the root page.
/// The append operation always appends data to the last page of the chain.
/// This is not optimized for multi-thread appends.
///
/// [Root Page] -> [Page 1] -> [Page 2] -> [Page 3] -> ... -> [Last Page]
///      |                                                        ^
///      |                                                        |
///      ----------------------------------------------------------
///
pub struct AppendOnlyStore<E: EvictionPolicy, T: MemPool<E>> {
    pub c_key: ContainerKey,
    pub root_key: PageFrameKey,        // Fixed.
    pub last_key: Mutex<PageFrameKey>, // Variable
    pub mem_pool: Arc<T>,
    stats: RuntimeStats, // Stats are not durable
    phantom: PhantomData<E>,
}

impl<E: EvictionPolicy, T: MemPool<E>> AppendOnlyStore<E, T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        // Root page contains the page id and frame id of the last page in the chain.
        let mut root_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        root_page.init();
        let root_key = {
            let page_id = root_page.get_id();
            let frame_id = root_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        let mut data_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        data_page.init();
        let data_key = {
            let page_id = data_page.get_id();
            let frame_id = data_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        // Set the next page of the root page to the data page.
        root_page.set_next_page(data_page.get_id(), data_page.frame_id());

        // Set the last page id and frame id to the root page.
        let data_key_bytes = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&data_page.get_id().to_be_bytes());
            bytes.extend_from_slice(&data_page.frame_id().to_be_bytes());
            bytes
        };
        assert!(root_page.append(&[], &data_key_bytes));

        AppendOnlyStore {
            c_key,
            root_key,
            last_key: Mutex::new(data_key),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
            phantom: PhantomData,
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, root_id: PageId) -> Self {
        // Assumes that root page's page_id is 0.
        let root_key = PageFrameKey::new(c_key, root_id);
        let last_key = {
            let root_page = mem_pool.get_page_for_read(root_key).unwrap();
            let (_, val) = root_page.get(0);
            let page_id = u32::from_be_bytes(val[0..4].try_into().unwrap());
            let frame_id = u32::from_be_bytes(val[4..8].try_into().unwrap());
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        AppendOnlyStore {
            c_key,
            root_key,
            last_key: Mutex::new(last_key),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
            phantom: PhantomData,
        }
    }

    pub fn bulk_insert_create<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let storage = Self::new(c_key, mem_pool);
        for (k, v) in iter {
            storage.append(k.as_ref(), v.as_ref()).unwrap();
        }
        storage
    }

    fn get_page_for_write(&self, page_key: &PageFrameKey) -> FrameWriteGuard<E> {
        let base = Duration::from_micros(10);
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(*page_key) {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(e) => panic!("Error: {}", e),
            }
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.stats.get_num_recs()
    }

    pub fn num_pages(&self) -> usize {
        self.stats.get_num_pages()
    }

    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<(), AppendOnlyStoreError> {
        let data_len = key.len() + value.len();
        if data_len > <Page as AppendOnlyPage>::max_record_size() {
            return Err(AppendOnlyStoreError::RecordTooLarge);
        }
        self.stats.inc_num_recs();

        let mut last_key = self.last_key.lock().unwrap();
        let mut last_page = self.get_page_for_write(&last_key);

        // Try to insert into the last page. If the page is full, create a new page and append to it.
        if last_page.append(key, value) {
            Ok(())
        } else {
            // New page is created.
            // The new page's page_id and frame_id are written to last page and the root page.
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            new_page.init();

            let page_id = new_page.get_id();
            let frame_id = new_page.frame_id();

            // Set the next page of the last page to the new page.
            last_page.set_next_page(page_id, frame_id);
            drop(last_page);

            // Set the last page id and frame id to the root page.
            let new_key_bytes = {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&frame_id.to_be_bytes());
                bytes
            };
            let mut root_page = self.get_page_for_write(&self.root_key);
            root_page.get_mut_val(0).copy_from_slice(&new_key_bytes);
            drop(root_page);

            self.stats.inc_num_pages();

            // Set the in-memory last key to the new page.
            let new_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
            *last_key = new_key;

            assert!(new_page.append(key, value));
            Ok(())
        }
    }

    pub fn scan(self: &Arc<Self>) -> AppendOnlyStoreScanner<E, T> {
        AppendOnlyStoreScanner {
            storage: self.clone(),
            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
        }
    }
}

pub struct AppendOnlyStoreScanner<E: EvictionPolicy + 'static, T: MemPool<E>> {
    storage: Arc<AppendOnlyStore<E, T>>,

    initialized: bool,
    finished: bool,
    current_page: Option<FrameReadGuard<'static, E>>,
    current_slot_id: u32,
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> AppendOnlyStoreScanner<E, T> {
    pub fn initialize(&mut self) {
        let root_key = self.storage.root_key;
        let root_page = self.storage.mem_pool.get_page_for_read(root_key).unwrap();
        // Read the first data page
        let (data_page_id, data_frame_id) = root_page.next_page().unwrap();
        let data_key =
            PageFrameKey::new_with_frame_id(self.storage.c_key, data_page_id, data_frame_id);
        let data_page = self.storage.mem_pool.get_page_for_read(data_key).unwrap();
        let data_page = unsafe {
            std::mem::transmute::<FrameReadGuard<E>, FrameReadGuard<'static, E>>(data_page)
        };
        self.current_page = Some(data_page);
        self.current_slot_id = 0;
    }
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> Iterator for AppendOnlyStoreScanner<E, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.initialized {
            self.initialize();
            self.initialized = true;
        }

        assert!(self.current_page.is_some());

        // Try to read from the current page.
        // If there are no more records in the current page, move to the next page
        // and try to read from it.
        if self.current_slot_id < self.current_page.as_ref().unwrap().slot_count() {
            let record = self
                .current_page
                .as_ref()
                .unwrap()
                .get(self.current_slot_id);
            self.current_slot_id += 1;
            Some((record.0.to_vec(), record.1.to_vec()))
        } else {
            let current_page = self.current_page.take().unwrap();
            let next_page = current_page.next_page();
            match next_page {
                Some((page_id, frame_id)) => {
                    let next_key =
                        PageFrameKey::new_with_frame_id(self.storage.c_key, page_id, frame_id);
                    let next_page = self.storage.mem_pool.get_page_for_read(next_key).unwrap();
                    let next_page = unsafe {
                        std::mem::transmute::<FrameReadGuard<E>, FrameReadGuard<'static, E>>(
                            next_page,
                        )
                    };
                    // Fast path eviction
                    self.storage
                        .mem_pool
                        .fast_evict(current_page.frame_id())
                        .unwrap();
                    drop(current_page);

                    self.current_page = Some(next_page);
                    self.current_slot_id = 0;
                    self.next()
                }
                None => {
                    // Fast path eviction
                    self.storage
                        .mem_pool
                        .fast_evict(current_page.frame_id())
                        .unwrap();
                    drop(current_page);

                    self.finished = true;
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bp::{get_test_bp, BufferPool, LRUEvictionPolicy};
    use crate::random::{gen_random_byte_vec, RandomKVs};

    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    fn get_c_key() -> ContainerKey {
        // Implementation of the container key creation
        ContainerKey::new(0, 0)
    }

    #[test]
    fn test_small_append() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let key = b"small key";
        let value = b"small value";
        assert_eq!(store.append(key, value), Ok(()));
    }

    #[test]
    fn test_large_append() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool));

        let key = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        let value = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        assert_eq!(
            store.append(&key, &value),
            Err(AppendOnlyStoreError::RecordTooLarge)
        );

        // Scan should return nothing
        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_page_overflow() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let key = gen_random_byte_vec(1000, 1000);
        let value = gen_random_byte_vec(1000, 1000);
        let num_appends = 100;

        for _ in 0..num_appends {
            assert_eq!(store.append(&key, &value), Ok(()));
        }
    }

    #[test]
    fn test_basic_scan() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool.clone()));

        let key = b"scanned key";
        let value = b"scanned value";
        for _ in 0..3 {
            store.append(key, value).unwrap();
        }

        assert_eq!(store.num_kvs(), 3);

        let mut scanner = store.scan();

        for _ in 0..3 {
            assert_eq!(scanner.next().unwrap(), (key.to_vec(), value.to_vec()));
        }
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_stress() {
        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomKVs::new(
            false,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        )
        .pop()
        .unwrap();

        let store = Arc::new(AppendOnlyStore::new(get_c_key(), get_test_bp(10)));

        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Appending record {} **********************",
                i
            );
            store.append(val.0, val.1).unwrap();
        }

        assert_eq!(store.num_kvs(), num_keys);

        let mut scanner = store.scan();
        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Scanning record {} **********************",
                i
            );
            assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
        }
    }

    #[test]
    fn test_concurrent_append() {
        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let num_threads = 3;
        let vals = RandomKVs::new(
            false,
            false,
            num_threads,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );

        let store = Arc::new(AppendOnlyStore::new(get_c_key(), get_test_bp(10)));

        let mut verify_vals = HashSet::new();
        for val_i in vals.iter() {
            for val in val_i.iter() {
                verify_vals.insert((val.0.to_vec(), val.1.to_vec()));
            }
        }

        thread::scope(|s| {
            for val_i in vals.iter() {
                let store_clone = store.clone();
                s.spawn(move || {
                    for val in val_i.iter() {
                        store_clone.append(val.0, val.1).unwrap();
                    }
                });
            }
        });

        assert_eq!(store.num_kvs(), num_keys);

        // Check if all values are appended.
        let scanner = store.scan();
        for val in scanner {
            assert!(verify_vals.remove(&val));
        }
        assert!(verify_vals.is_empty());
    }

    #[test]
    fn test_scan_finish_condition() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool.clone()));

        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_bulk_insert_create() {
        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomKVs::new(
            false,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        )
        .pop()
        .unwrap();

        let store = Arc::new(AppendOnlyStore::bulk_insert_create(
            get_c_key(),
            get_test_bp(10),
            vals.iter(),
        ));

        assert_eq!(store.num_kvs(), num_keys);

        let mut scanner = store.scan();
        for val in vals.iter() {
            assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
        }
    }

    #[test]
    fn test_durability() {
        let temp_dir = tempfile::tempdir().unwrap();

        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomKVs::new(
            false,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        )
        .pop()
        .unwrap();

        // Create a store and insert some values.
        // Drop the store and buffer pool
        {
            let bp = Arc::new(BufferPool::<LRUEvictionPolicy>::new(&temp_dir, 10, false).unwrap());

            let store = Arc::new(AppendOnlyStore::bulk_insert_create(
                get_c_key(),
                bp.clone(),
                vals.iter(),
            ));

            drop(store);
            drop(bp);
        }

        {
            let bp = Arc::new(BufferPool::<LRUEvictionPolicy>::new(&temp_dir, 10, false).unwrap());
            let store = Arc::new(AppendOnlyStore::load(get_c_key(), bp.clone(), 0));

            let mut scanner = store.scan();
            for val in vals.iter() {
                assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
            }
        }
    }
}
