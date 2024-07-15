mod append_only_page;

use append_only_page::AppendOnlyPage;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc, Mutex},
    thread::current,
    time::Duration,
};

use crate::{
    bp::{FrameReadGuard, FrameWriteGuard, MemPoolStatus},
    page::{Page, PageId, PAGE_SIZE},
    prelude::{ContainerKey, EvictionPolicy, MemPool, PageFrameKey},
};

pub mod prelude {
    pub use super::{AccessMethodStatus, AppendOnlyStore, AppendOnlyStoreScanner};
}

#[derive(Debug, PartialEq)]
pub enum AccessMethodStatus {
    PageFull,
    PageNotFound,
    RecordTooLarge,
}

/// In the append-only store, the pages forms a one-way linked list which we call a chain.
/// The first page is called the root page.
/// The append operation always appends data to the last page of the chain.
/// This is not optimized for multi-thread appends.
pub struct AppendOnlyStore<E: EvictionPolicy, T: MemPool<E>> {
    pub c_key: ContainerKey,
    pub root_key: PageFrameKey,        // Fixed.
    pub last_key: Mutex<PageFrameKey>, // Variable
    pub mem_pool: Arc<T>,
    phantom: PhantomData<E>,
}

impl<E: EvictionPolicy, T: MemPool<E>> AppendOnlyStore<E, T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut root_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let root_key = {
            let page_id = root_page.get_id();
            let frame_id = root_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };
        root_page.init();

        AppendOnlyStore {
            c_key,
            root_key,
            last_key: Mutex::new(root_key.clone()),
            mem_pool: mem_pool.clone(),
            phantom: PhantomData,
        }
    }

    fn write_page(&self, page_key: &PageFrameKey) -> FrameWriteGuard<E> {
        let base = Duration::from_micros(10);
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(page_key.clone()) {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(e) => panic!("Error: {}", e),
            }
        }
    }

    pub fn append(&self, data: &[u8]) -> Result<(), AccessMethodStatus> {
        if data.len() > <Page as AppendOnlyPage>::max_record_size() {
            return Err(AccessMethodStatus::RecordTooLarge);
        }

        let mut last_key = self.last_key.lock().unwrap();
        let mut last_page = self.write_page(&*last_key);

        // Try to insert into the last page. If the page is full, create a new page and append to it.
        if last_page.append(data) {
            Ok(())
        } else {
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            new_page.init();

            let page_id = new_page.get_id();
            let frame_id = new_page.frame_id();
            let new_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
            last_page.set_next_page(page_id, frame_id);
            *last_key = new_key;

            assert!(new_page.append(data));
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
    current_slot_id: u16,
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> AppendOnlyStoreScanner<E, T> {
    pub fn initialize(&mut self) {
        let root_key = self.storage.root_key.clone();
        let root_page = self.storage.mem_pool.get_page_for_read(root_key).unwrap();
        let root_page = unsafe {
            std::mem::transmute::<FrameReadGuard<E>, FrameReadGuard<'static, E>>(root_page)
        };
        self.current_page = Some(root_page);
        self.current_slot_id = 0;
    }
}

impl<E: EvictionPolicy + 'static, T: MemPool<E>> Iterator for AppendOnlyStoreScanner<E, T> {
    type Item = Vec<u8>;

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
            return Some(record.to_vec());
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
                    self.current_page = Some(next_page);
                    self.current_slot_id = 0;
                    return self.next();
                }
                None => {
                    self.finished = true;
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bp::{BufferPoolForTest, LRUEvictionPolicy};
    use crate::random::{gen_random_byte_vec, RandomVals};

    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    // Helper functions for setting up the memory pool and container key
    fn initialize_memory_pool() -> Arc<BufferPoolForTest<LRUEvictionPolicy>> {
        // Implementation of the memory pool setup
        Arc::new(BufferPoolForTest::new(100))
    }

    fn create_container_key() -> ContainerKey {
        // Implementation of the container key creation
        ContainerKey::new(0, 0)
    }

    #[test]
    fn test_small_append() {
        let mem_pool = initialize_memory_pool();
        let container_key = create_container_key();
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let data = b"small data";
        assert_eq!(store.append(data), Ok(()));
    }

    #[test]
    fn test_large_append() {
        let mem_pool = initialize_memory_pool();
        let container_key = create_container_key();
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let data = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        assert_eq!(store.append(&data), Err(AccessMethodStatus::RecordTooLarge));
    }

    #[test]
    fn test_page_overflow() {
        let mem_pool = initialize_memory_pool();
        let container_key = create_container_key();
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let data = gen_random_byte_vec(1000, 1000);
        let num_appends = (PAGE_SIZE / data.len()) + 1; // Ensure overflow

        for _ in 0..num_appends {
            assert_eq!(store.append(&data), Ok(()));
        }
    }

    #[test]
    fn test_basic_scan() {
        let mem_pool = initialize_memory_pool();
        let container_key = create_container_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool.clone()));

        let data = b"scanned data";
        for _ in 0..3 {
            store.append(data).unwrap();
        }

        let mut scanner = store.scan();

        for _ in 0..3 {
            assert_eq!(scanner.next().unwrap(), data);
        }
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_stress() {
        let num_vals = 10000;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomVals::new(num_vals, 1, val_min_size, val_max_size)
            .pop()
            .unwrap();

        let store = Arc::new(AppendOnlyStore::new(
            create_container_key(),
            initialize_memory_pool(),
        ));

        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Appending record {} **********************",
                i
            );
            store.append(val).unwrap();
        }

        let mut scanner = store.scan();
        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Scanning record {} **********************",
                i
            );
            assert_eq!(&scanner.next().unwrap(), val);
        }
    }

    #[test]
    fn test_concurrent_append() {
        let num_vals = 10000;
        let val_min_size = 50;
        let val_max_size = 100;
        let num_threads = 3;
        let vals = RandomVals::new(num_vals, num_threads, val_min_size, val_max_size);

        let store = Arc::new(AppendOnlyStore::new(
            create_container_key(),
            initialize_memory_pool(),
        ));

        let mut verify_vals = HashSet::new();
        for val_i in vals.iter() {
            for val in val_i.iter() {
                verify_vals.insert(val.clone());
            }
        }

        thread::scope(|s| {
            for val_i in vals.iter() {
                let store_clone = store.clone();
                s.spawn(move || {
                    for val in val_i.iter() {
                        store_clone.append(val).unwrap();
                    }
                });
            }
        });

        // Check if all values are appended.
        let scanner = store.scan();
        for val in scanner {
            assert!(verify_vals.remove(&val));
        }
        assert!(verify_vals.is_empty());
    }

    #[test]
    fn test_scan_finish_condition() {
        let mem_pool = initialize_memory_pool();
        let container_key = create_container_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool.clone()));

        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }
}
