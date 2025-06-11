mod fixed_size_page;

use fixed_size_page::FixedSizePage;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Duration,
};

use crate::{
    access_method::{fixed_size_store::fixed_size_page::FPS_PAGE_HEADER_SIZE, FilterType},
    bp::{FrameReadGuard, FrameWriteGuard, MemPoolStatus},
    page::{Page, PageId, PageVisitor, AVAILABLE_PAGE_SIZE},
    prelude::{ContainerKey, MemPool, NonUniqueKeyIndex, PageFrameKey},
    random::gen_truncated_randomized_exponential_backoff,
};

use super::AccessMethodError;

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
pub struct FixedSizeStore<T: MemPool> {
    pub c_key: ContainerKey,
    pub root_key: PageFrameKey,        // Fixed.
    pub last_key: Mutex<PageFrameKey>, // Variable
    pub mem_pool: Arc<T>,
    pub key_size: usize,
    pub value_size: usize,
    stats: RuntimeStats, // Stats are not durable
}

impl<T: MemPool> NonUniqueKeyIndex for FixedSizeStore<T> {
    type RangeIter = FixedSizeStoreScanner<T>;

    fn append(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.append(key, value)
    }

    fn scan(self: &Arc<Self>) -> Self::RangeIter {
        self.scan()
    }

    fn scan_key(self: &Arc<Self>, key: &[u8]) -> Self::RangeIter {
        let key = key.to_vec();
        let filter = Arc::new(move |k: &[u8], _v: &[u8]| k == key);
        self.scan_with_filter(filter)
    }
}

impl<T: MemPool> FixedSizeStore<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, key_size: usize, value_size: usize) -> Self {
        // Root page contains the page id and frame id of the last page in the chain.
        let mut root_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        root_page.init(key_size, value_size);
        assert_eq!(root_page.key_size() as usize, key_size);
        assert_eq!(root_page.val_size() as usize, value_size);
        let root_key = {
            let page_id = root_page.get_id();
            let frame_id = root_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        let mut data_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        data_page.init(key_size, value_size);
        assert_eq!(data_page.key_size() as usize, key_size);
        assert_eq!(data_page.val_size() as usize, value_size);
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
        root_page[FPS_PAGE_HEADER_SIZE..FPS_PAGE_HEADER_SIZE + 8].copy_from_slice(&data_key_bytes);

        FixedSizeStore {
            c_key,
            root_key,
            last_key: Mutex::new(data_key),
            mem_pool: mem_pool.clone(),
            key_size,
            value_size,
            stats: RuntimeStats::new(),
        }
    }

    pub fn load(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        root_id: PageId,
        key_size: usize,
        value_size: usize,
    ) -> Self {
        // Assumes that root page's page_id is 0.
        let root_key = PageFrameKey::new(c_key, root_id);
        let last_key = {
            let root_page = mem_pool.get_page_for_read(root_key).unwrap();
            let val = &root_page[FPS_PAGE_HEADER_SIZE..FPS_PAGE_HEADER_SIZE + 8];
            let page_id = u32::from_be_bytes(val[0..4].try_into().unwrap());
            let frame_id = u32::from_be_bytes(val[4..8].try_into().unwrap());
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        FixedSizeStore {
            c_key,
            root_key,
            last_key: Mutex::new(last_key),
            mem_pool: mem_pool.clone(),
            key_size,
            value_size,
            stats: RuntimeStats::new(),
        }
    }

    pub fn bulk_insert_create<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        key_size: usize,
        value_size: usize,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let storage = Self::new(c_key, mem_pool, key_size, value_size);
        for (k, v) in iter {
            storage.append(k.as_ref(), v.as_ref()).unwrap();
        }
        storage
    }

    fn write_page(&self, page_key: &PageFrameKey) -> FrameWriteGuard<T::EP> {
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(*page_key) {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    std::thread::sleep(Duration::from_nanos(
                        gen_truncated_randomized_exponential_backoff(attempts),
                    ));
                    attempts += 1;
                }
                Err(e) => panic!("Error: {}", e),
            }
        }
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard<T::EP> {
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_read(page_key) {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    std::thread::sleep(Duration::from_nanos(
                        gen_truncated_randomized_exponential_backoff(attempts),
                    ));
                    attempts += 1;
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

    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        if key.len() != self.key_size {
            panic!(
                "Key size mismatch: expected {}, got {}",
                self.key_size,
                key.len()
            );
        }
        if value.len() != self.value_size {
            panic!(
                "Value size mismatch: expected {}, got {}",
                self.value_size,
                value.len()
            );
        }
        self.stats.inc_num_recs();

        let mut last_key = self.last_key.lock().unwrap();
        let mut last_page = self.write_page(&last_key);

        // Try to insert into the last page. If the page is full, create a new page and append to it.
        if last_page.append(key, value) {
            Ok(())
        } else {
            // New page is created.
            // The new page's page_id and frame_id are written to last page and the root page.
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            new_page.init(self.key_size, self.value_size);

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
            let mut root_page = self.write_page(&self.root_key);
            root_page[FPS_PAGE_HEADER_SIZE..FPS_PAGE_HEADER_SIZE + 8]
                .copy_from_slice(&new_key_bytes);
            drop(root_page);

            self.stats.inc_num_pages();

            // Set the in-memory last key to the new page.
            let new_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
            *last_key = new_key;

            assert!(new_page.append(key, value));
            Ok(())
        }
    }

    pub fn scan(self: &Arc<Self>) -> FixedSizeStoreScanner<T> {
        FixedSizeStoreScanner {
            storage: self.clone(),
            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
            filter: None,
        }
    }

    pub fn scan_with_filter(self: &Arc<Self>, filter: FilterType) -> FixedSizeStoreScanner<T> {
        FixedSizeStoreScanner {
            storage: self.clone(),
            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
            filter: Some(filter),
        }
    }

    pub fn page_traverser(&self) -> FixedSizeStorePageTraversal<T> {
        FixedSizeStorePageTraversal::new(self)
    }

    pub fn page_stats(&self, verbose: bool) -> String {
        let mut stats = HeapStoreStats::new();
        let traverser = self.page_traverser();
        traverser.visit(&mut stats);
        stats.to_string(verbose)
    }
}

pub struct FixedSizeStoreScanner<T: MemPool> {
    storage: Arc<FixedSizeStore<T>>,

    initialized: bool,
    finished: bool,
    current_page: Option<FrameReadGuard<T::EP>>,
    current_slot_id: u32, // Current slot id in the current page

    filter: Option<FilterType>,
}

impl<T: MemPool> FixedSizeStoreScanner<T> {
    fn initialize(&mut self) {
        let root_key = self.storage.root_key;
        let root_page = self.storage.read_page(root_key);
        // Read the first data page
        let (data_page_id, data_frame_id) = root_page.next_page().unwrap();
        let data_key =
            PageFrameKey::new_with_frame_id(self.storage.c_key, data_page_id, data_frame_id);
        let data_page = self.storage.read_page(data_key);
        self.current_page = Some(data_page);
        self.current_slot_id = 0;
    }

    fn prefetch_next_page(&self) {
        if let Some(current_page) = &self.current_page {
            if let Some((page_id, frame_id)) = current_page.next_page() {
                let next_key =
                    PageFrameKey::new_with_frame_id(self.storage.c_key, page_id, frame_id);
                self.storage.mem_pool.prefetch_page(next_key).unwrap();
            }
        }
    }
}

impl<T: MemPool> Iterator for FixedSizeStoreScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.initialized {
            self.initialize();
            self.prefetch_next_page();
            self.initialized = true;
        }

        assert!(self.current_page.is_some());

        // Try to read from the current page.
        // If there are no more records in the current page, move to the next page
        // and try to read from it.
        if self.current_slot_id < self.current_page.as_ref().unwrap().slot_count() {
            // For fixed-size pages, we use slot-based access instead of offset-based
            let current_page = self.current_page.as_ref().unwrap();
            let key = &current_page[FPS_PAGE_HEADER_SIZE
                + self.current_slot_id as usize * (self.storage.key_size + self.storage.value_size)
                ..FPS_PAGE_HEADER_SIZE
                    + self.current_slot_id as usize
                        * (self.storage.key_size + self.storage.value_size)
                    + self.storage.key_size];
            let value = &current_page[FPS_PAGE_HEADER_SIZE
                + self.current_slot_id as usize * (self.storage.key_size + self.storage.value_size)
                + self.storage.key_size
                ..FPS_PAGE_HEADER_SIZE
                    + self.current_slot_id as usize
                        * (self.storage.key_size + self.storage.value_size)
                    + self.storage.key_size
                    + self.storage.value_size];
            self.current_slot_id += 1;
            if let Some(filter) = &self.filter {
                if !filter(&key, &value) {
                    return self.next(); // Skip this record
                }
            }
            Some((key.to_vec(), value.to_vec()))
        } else {
            let current_page = self.current_page.take().unwrap();
            let next_page = current_page.next_page();
            match next_page {
                Some((page_id, frame_id)) => {
                    let next_key =
                        PageFrameKey::new_with_frame_id(self.storage.c_key, page_id, frame_id);
                    let next_page = self.storage.read_page(next_key);
                    drop(current_page);

                    self.current_page = Some(next_page);
                    self.current_slot_id = 0;
                    self.prefetch_next_page();
                    self.next()
                }
                None => {
                    drop(current_page);

                    self.finished = true;
                    None
                }
            }
        }
    }
}

pub struct FixedSizeStorePageTraversal<T: MemPool> {
    c_key: ContainerKey,
    root_key: PageFrameKey,
    mem_pool: Arc<T>,
}

impl<T: MemPool> FixedSizeStorePageTraversal<T> {
    pub fn new(aps: &FixedSizeStore<T>) -> Self {
        Self {
            c_key: aps.c_key,
            mem_pool: aps.mem_pool.clone(),
            root_key: aps.root_key,
        }
    }

    pub fn visit<V>(&self, visitor: &mut V)
    where
        V: PageVisitor,
    {
        let mut stack = vec![(self.root_key, false)];
        while let Some((next_key, pre_visited)) = stack.last_mut() {
            let page = self.mem_pool.get_page_for_read(*next_key).unwrap();
            if *pre_visited {
                visitor.visit_post(&page);
                stack.pop();
                continue;
            } else {
                *pre_visited = true;
                visitor.visit_pre(&page);
                if let Some((next_page_id, next_frame_id)) = page.next_page() {
                    let next_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                    stack.push((next_key, false));
                }
            }
        }
    }
}

#[derive(Debug)]
struct PerPageStats {
    slot_count: usize,
    total_bytes_used: usize,
    total_free_space: usize,
}

impl std::fmt::Display for PerPageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        result.push_str(&format!("Slot count: {}\n", self.slot_count));
        result.push_str(&format!("Total bytes used: {}\n", self.total_bytes_used));
        result.push_str(&format!("Total free space: {}\n", self.total_free_space));
        write!(f, "{}", result)
    }
}

struct HeapStoreStats {
    count: usize,
    min_fillfactor: f64,
    max_fillfactor: f64,
    sum_fillfactor: f64,
    page_stats: BTreeMap<PageId, PerPageStats>,
}

impl HeapStoreStats {
    fn new() -> Self {
        Self {
            count: 0,
            min_fillfactor: 1.0,
            max_fillfactor: 0.0,
            sum_fillfactor: 0.0,
            page_stats: BTreeMap::new(),
        }
    }

    fn update(&mut self, page_id: PageId, stats: PerPageStats) {
        self.count += 1;
        let fillfactor = stats.total_bytes_used as f64 / AVAILABLE_PAGE_SIZE as f64;
        self.min_fillfactor = self.min_fillfactor.min(fillfactor);
        self.max_fillfactor = self.max_fillfactor.max(fillfactor);
        self.sum_fillfactor += fillfactor;
        self.page_stats.insert(page_id, stats);
    }

    fn to_string(&self, verbose: bool) -> String {
        let mut result = String::new();
        result.push_str(&format!("Page Created: {}\n", self.count));
        result.push_str(&format!("Min fillfactor: {:.4}\n", self.min_fillfactor));
        result.push_str(&format!("Max fillfactor: {:.4}\n", self.max_fillfactor));
        result.push_str(&format!(
            "Avg fillfactor: {:.4}\n",
            self.sum_fillfactor / self.count as f64
        ));
        // Print the page with high fillfactor
        if verbose {
            result.push_str("Pages with high fillfactor (> 0.99):\n");
            for (page_id, per_page_stats) in self.page_stats.iter() {
                let fillfactor =
                    per_page_stats.total_bytes_used as f64 / AVAILABLE_PAGE_SIZE as f64;
                if fillfactor > 0.99 {
                    result.push_str(&format!(
                        "Page {} has high fillfactor: {:.2}\n",
                        page_id, fillfactor
                    ));
                    result.push_str(&per_page_stats.to_string());
                }
            }

            // Print the page stats
            result.push_str("Individual page stats:\n");
            for (page_id, per_page_stats) in self.page_stats.iter() {
                result.push_str(&format!(
                    "----------------- Page {} -----------------\n",
                    page_id
                ));
                result.push_str(&per_page_stats.to_string());
            }
        }
        result
    }
}

impl PageVisitor for HeapStoreStats {
    fn visit_pre(&mut self, page: &Page) {
        let stats = PerPageStats {
            slot_count: page.slot_count() as usize,
            total_bytes_used: page.total_bytes_used() as usize,
            total_free_space: page.total_free_space() as usize,
        };
        self.update(page.get_id(), stats);
    }

    fn visit_post(&mut self, _page: &Page) {}
}

#[cfg(test)]
mod tests {
    use crate::bp::{get_test_bp_lru, BufferPool};
    use crate::container::ContainerManager;
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
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_c_key();
        let store = FixedSizeStore::new(container_key, mem_pool, 9, 11);

        let key = b"small key";
        let value = b"small value";
        assert_eq!(store.append(key, value), Ok(()));
    }

    #[test]
    fn test_page_overflow() {
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_c_key();
        let store = FixedSizeStore::new(container_key, mem_pool, 100, 100);

        let key = gen_random_byte_vec(100, 100);
        let value = gen_random_byte_vec(100, 100);

        let num_appends = 1000;

        for _ in 0..num_appends {
            assert_eq!(store.append(&key, &value), Ok(()));
        }
    }

    #[test]
    fn test_basic_scan() {
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_c_key();
        let store = Arc::new(FixedSizeStore::new(container_key, mem_pool.clone(), 11, 13));

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
        let val_size = 100;
        let vals = RandomKVs::new(false, false, 1, num_keys, key_size, val_size, val_size)
            .pop()
            .unwrap();

        let store = Arc::new(FixedSizeStore::new(
            get_c_key(),
            get_test_bp_lru(10),
            key_size,
            val_size,
        ));

        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Appending record {} **********************",
                i
            );
            store.append(val.0, val.1).unwrap();
        }
        println!("Page stats: \n{}", store.page_stats(false));

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
        let val_size = 100;
        let num_threads = 3;
        let vals = RandomKVs::new(
            false,
            false,
            num_threads,
            num_keys,
            key_size,
            val_size,
            val_size,
        );

        let store = Arc::new(FixedSizeStore::new(
            get_c_key(),
            get_test_bp_lru(10),
            key_size,
            val_size,
        ));

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
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_c_key();
        let store = Arc::new(FixedSizeStore::new(container_key, mem_pool.clone(), 10, 10));

        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_bulk_insert_create() {
        let num_keys = 10000;
        let key_size = 50;
        let val_size = 100;
        let vals = RandomKVs::new(false, false, 1, num_keys, key_size, val_size, val_size)
            .pop()
            .unwrap();

        let store = Arc::new(FixedSizeStore::bulk_insert_create(
            get_c_key(),
            get_test_bp_lru(10),
            key_size,
            val_size,
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
        let val_size = 100;
        let vals = RandomKVs::new(false, false, 1, num_keys, key_size, val_size, val_size)
            .pop()
            .unwrap();

        // Create a store and insert some values.
        // Drop the store and buffer pool
        {
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let bp = Arc::new(BufferPool::new(10, cm).unwrap());

            let store = Arc::new(FixedSizeStore::bulk_insert_create(
                get_c_key(),
                bp.clone(),
                key_size,
                val_size,
                vals.iter(),
            ));

            drop(store);
            drop(bp);
        }

        {
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let bp = Arc::new(BufferPool::new(10, cm).unwrap());
            let store = Arc::new(FixedSizeStore::load(
                get_c_key(),
                bp.clone(),
                0,
                key_size,
                val_size,
            ));

            let mut scanner = store.scan();
            for val in vals.iter() {
                assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
            }
        }
    }
}
