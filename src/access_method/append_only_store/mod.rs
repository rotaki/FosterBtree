mod append_only_page;

use append_only_page::AppendOnlyPage;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Duration,
};

use crate::{
    access_method::{
        append_only_store::append_only_page::{APS_PAGE_HEADER_SIZE, APS_RECORD_METADATA_SIZE},
        FilterType,
    },
    bp::{FrameReadGuard, FrameWriteGuard, MemPoolStatus},
    page::{Page, PageId, PageVisitor, AVAILABLE_PAGE_SIZE},
    prelude::{ContainerId, MemPool, NonUniqueKeyIndex, PageRef},
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
pub struct AppendOnlyStore<T: MemPool> {
    pub container_id: ContainerId,
    pub root_key: PageRef,        // Fixed.
    pub last_key: Mutex<PageRef>, // Variable
    pub mem_pool: Arc<T>,
    stats: RuntimeStats, // Stats are not durable
}

impl<T: MemPool> NonUniqueKeyIndex for AppendOnlyStore<T> {
    type RangeIter = AppendOnlyStoreScanner<T>;

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

impl<T: MemPool> AppendOnlyStore<T> {
    pub fn new(container_id: ContainerId, mem_pool: Arc<T>) -> Self {
        // Root page contains the page id and frame id of the last page in the chain.
        let mut root_page = mem_pool.create_new_page_for_write(container_id).unwrap();
        root_page.init();
        let root_key = {
            let page_id = root_page.page_id();
            let frame_id = root_page.frame_id();
            PageRef::new_with_frame_id(container_id, page_id, frame_id)
        };

        let mut data_page = mem_pool.create_new_page_for_write(container_id).unwrap();
        data_page.init();
        let data_key = {
            let page_id = data_page.page_id();
            let frame_id = data_page.frame_id();
            PageRef::new_with_frame_id(container_id, page_id, frame_id)
        };

        // Set the next page of the root page to the data page.
        root_page.set_next_page(data_page.page_id(), data_page.frame_id());

        // Set the last page id and frame id to the root page.
        let data_key_bytes = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&data_page.page_id().to_be_bytes());
            bytes.extend_from_slice(&data_page.frame_id().to_be_bytes());
            bytes
        };
        root_page[APS_PAGE_HEADER_SIZE..APS_PAGE_HEADER_SIZE + 8].copy_from_slice(&data_key_bytes);

        AppendOnlyStore {
            container_id,
            root_key,
            last_key: Mutex::new(data_key),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
        }
    }

    pub fn load(container_id: ContainerId, mem_pool: Arc<T>, root_id: PageId) -> Self {
        // Assumes that root page's page_id is 0.
        let root_key = PageRef::new(container_id, root_id);
        let last_key = {
            let root_page = mem_pool
                .get_page_for_read(
                    root_key.container_id(),
                    root_key.page_id(),
                    root_key.frame_hint(),
                )
                .unwrap();
            let val = &root_page[APS_PAGE_HEADER_SIZE..APS_PAGE_HEADER_SIZE + 8];
            let page_id = u32::from_be_bytes(val[0..4].try_into().unwrap());
            let frame_id = u32::from_be_bytes(val[4..8].try_into().unwrap());
            PageRef::new_with_frame_id(container_id, page_id, frame_id)
        };

        AppendOnlyStore {
            container_id,
            root_key,
            last_key: Mutex::new(last_key),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
        }
    }

    pub fn bulk_insert_create<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        container_id: ContainerId,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let storage = Self::new(container_id, mem_pool);
        for (k, v) in iter {
            storage.append(k.as_ref(), v.as_ref()).unwrap();
        }
        storage
    }

    fn write_page(&self, page_key: &PageRef) -> FrameWriteGuard {
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(
                page_key.container_id(),
                page_key.page_id(),
                page_key.frame_hint(),
            ) {
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

    fn read_page(&self, page_key: PageRef) -> FrameReadGuard {
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_read(
                page_key.container_id(),
                page_key.page_id(),
                page_key.frame_hint(),
            ) {
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
        let data_len = key.len() + value.len();
        if data_len > <Page as AppendOnlyPage>::max_record_size() {
            return Err(AccessMethodError::RecordTooLarge);
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
            let mut new_page = self
                .mem_pool
                .create_new_page_for_write(self.container_id)
                .unwrap();
            new_page.init();

            let page_id = new_page.page_id();
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
            root_page[APS_PAGE_HEADER_SIZE..APS_PAGE_HEADER_SIZE + 8]
                .copy_from_slice(&new_key_bytes);
            drop(root_page);

            self.stats.inc_num_pages();

            // Set the in-memory last key to the new page.
            let new_key = PageRef::new_with_frame_id(self.container_id, page_id, frame_id);
            *last_key = new_key;

            assert!(new_page.append(key, value));
            Ok(())
        }
    }

    pub fn scan(self: &Arc<Self>) -> AppendOnlyStoreScanner<T> {
        AppendOnlyStoreScanner {
            storage: self.clone(),
            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
            current_offset: 0,
            filter: None,
        }
    }

    pub fn scan_with_filter(self: &Arc<Self>, filter: FilterType) -> AppendOnlyStoreScanner<T> {
        AppendOnlyStoreScanner {
            storage: self.clone(),
            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
            current_offset: APS_PAGE_HEADER_SIZE,
            filter: Some(filter),
        }
    }

    pub fn page_traverser(&self) -> AppendOnlyStorePageTraversal<T> {
        AppendOnlyStorePageTraversal::new(self)
    }

    pub fn page_stats(&self, verbose: bool) -> String {
        let mut stats = HeapStoreStats::new();
        let traverser = self.page_traverser();
        traverser.visit(&mut stats);
        stats.to_string(verbose)
    }
}

pub struct AppendOnlyStoreScanner<T: MemPool> {
    storage: Arc<AppendOnlyStore<T>>,

    initialized: bool,
    finished: bool,
    current_page: Option<FrameReadGuard>,
    current_slot_id: u32, // Current slot id in the current page
    current_offset: usize,

    filter: Option<FilterType>,
}

impl<T: MemPool> AppendOnlyStoreScanner<T> {
    fn initialize(&mut self) {
        let root_key = self.storage.root_key;
        let root_page = self.storage.read_page(root_key);
        // Read the first data page
        let (data_page_id, data_frame_id) = root_page.next_page().unwrap();
        let data_key =
            PageRef::new_with_frame_id(self.storage.container_id, data_page_id, data_frame_id);
        let data_page = self.storage.read_page(data_key);
        self.current_page = Some(data_page);
        self.current_slot_id = 0;
        self.current_offset = APS_PAGE_HEADER_SIZE;
    }

    fn prefetch_next_page(&self) {
        if let Some(current_page) = &self.current_page {
            if let Some((page_id, frame_id)) = current_page.next_page() {
                let next_key =
                    PageRef::new_with_frame_id(self.storage.container_id, page_id, frame_id);
                self.storage
                    .mem_pool
                    .prefetch_page(
                        next_key.container_id(),
                        next_key.page_id(),
                        next_key.frame_hint(),
                    )
                    .unwrap();
            }
        }
    }
}

impl<T: MemPool> Iterator for AppendOnlyStoreScanner<T> {
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
            let record = self
                .current_page
                .as_ref()
                .unwrap()
                .get_at(self.current_offset as u32)
                .unwrap();
            self.current_slot_id += 1;
            self.current_offset += record.0.len() + record.1.len() + APS_RECORD_METADATA_SIZE; // 8 bytes for key and value size
            if let Some(filter) = &self.filter {
                if !filter(record.0, record.1) {
                    return self.next(); // Skip this record
                }
            }
            Some((record.0.to_vec(), record.1.to_vec()))
        } else {
            let current_page = self.current_page.take().unwrap();
            let next_page = current_page.next_page();
            match next_page {
                Some((page_id, frame_id)) => {
                    let next_key =
                        PageRef::new_with_frame_id(self.storage.container_id, page_id, frame_id);
                    let next_page = self.storage.read_page(next_key);
                    drop(current_page);

                    self.current_page = Some(next_page);
                    self.current_slot_id = 0;
                    self.current_offset = APS_PAGE_HEADER_SIZE; // Reset offset to the start of the new page
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

pub struct AppendOnlyStorePageTraversal<T: MemPool> {
    container_id: ContainerId,
    root_key: PageRef,
    mem_pool: Arc<T>,
}

impl<T: MemPool> AppendOnlyStorePageTraversal<T> {
    pub fn new(aps: &AppendOnlyStore<T>) -> Self {
        Self {
            container_id: aps.container_id,
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
            let page = self
                .mem_pool
                .get_page_for_read(
                    next_key.container_id(),
                    next_key.page_id(),
                    next_key.frame_hint(),
                )
                .unwrap();
            if *pre_visited {
                visitor.visit_post(&page);
                stack.pop();
                continue;
            } else {
                *pre_visited = true;
                visitor.visit_pre(&page);
                if let Some((next_page_id, next_frame_id)) = page.next_page() {
                    let next_key =
                        PageRef::new_with_frame_id(self.container_id, next_page_id, next_frame_id);
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
        self.update(page.page_id(), stats);
    }

    fn visit_post(&mut self, _page: &Page) {}
}

#[cfg(test)]
mod tests {
    use crate::bp::{get_test_bp_lru, BufferPoolLRU};
    use crate::container::ContainerManager;
    use crate::random::{gen_random_byte_vec, RandomKVs};

    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    fn get_container_id() -> ContainerId {
        ContainerId::new(0, 0)
    }

    #[test]
    fn test_small_append() {
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_container_id();
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let key = b"small key";
        let value = b"small value";
        assert_eq!(store.append(key, value), Ok(()));
    }

    #[test]
    fn test_large_append() {
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_container_id();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool));

        let key = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        let value = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        assert_eq!(
            store.append(&key, &value),
            Err(AccessMethodError::RecordTooLarge)
        );

        // Scan should return nothing
        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_page_overflow() {
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_container_id();
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
        let mem_pool = get_test_bp_lru(10);
        let container_key = get_container_id();
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

        let store = Arc::new(AppendOnlyStore::new(
            get_container_id(),
            get_test_bp_lru(10),
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

        let store = Arc::new(AppendOnlyStore::new(
            get_container_id(),
            get_test_bp_lru(10),
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
        let container_key = get_container_id();
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
            get_container_id(),
            get_test_bp_lru(10),
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
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let bp = Arc::new(BufferPoolLRU::new(10, cm).unwrap());

            let store = Arc::new(AppendOnlyStore::bulk_insert_create(
                get_container_id(),
                bp.clone(),
                vals.iter(),
            ));

            drop(store);
            drop(bp);
        }

        {
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let bp = Arc::new(BufferPoolLRU::new(10, cm).unwrap());
            let store = Arc::new(AppendOnlyStore::load(get_container_id(), bp.clone(), 0));

            let mut scanner = store.scan();
            for val in vals.iter() {
                assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
            }
        }
    }
}
