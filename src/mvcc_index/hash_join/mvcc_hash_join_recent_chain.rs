use core::panic;
use std::{
    collections::BTreeMap,
    sync::{atomic::{self, AtomicU32}, Arc},
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_trace, log_warn,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::{
    mvcc_hash_join_recent_page::MvccHashJoinRecentPage,
    Timestamp,
};

pub struct MvccHashJoinRecentChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,
    
    first_page_id: PageId,
    first_frame_id: AtomicU32,

    last_page_id: AtomicU32, // cache the last page id for faster insertion
    last_frame_id: AtomicU32,
}

impl<T: MemPool> MvccHashJoinRecentChain<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let first_page_id = page.get_id();
        let first_frame_id = page.frame_id();
        
        MvccHashJoinRecentPage::init(&mut *page);
        drop(page);

        Self {
            mem_pool,
            c_key,
            first_page_id,
            first_frame_id: AtomicU32::new(first_frame_id),
            last_page_id: AtomicU32::new(first_page_id),
            last_frame_id: AtomicU32::new(first_frame_id),
        }
    }

    pub fn new_from_page(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        pid: PageId,
    ) -> Self {
        let page_key = PageFrameKey::new(c_key, pid);
        let page = mem_pool.get_page_for_read(page_key).unwrap();
        let first_frame_id = page.frame_id();
        drop(page);

        Self {
            mem_pool,
            c_key,
            first_page_id: pid,
            first_frame_id: AtomicU32::new(first_frame_id),
            last_page_id: AtomicU32::new(pid),
            last_frame_id: AtomicU32::new(first_frame_id),
        }
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let space_need = <Page as MvccHashJoinRecentPage>::space_need(key, pkey, val);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }

        let mut current_pid = self.last_page_id.load(atomic::Ordering::Acquire);
        let mut current_fid = self.last_frame_id.load(atomic::Ordering::Acquire);

        let base = 2;
        let mut attempts = 0;
        
        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let page = self.read_page(page_key);
            match MvccHashJoinRecentPage::next_page(& *page) {
                Some((next_pid, next_fid)) => {
                    current_pid = next_pid;
                    current_fid = next_fid;
                    continue;
                },
                None => {
                    log_debug!("Reached end of chain, inserting into end of chain");
                }
            }
            if space_need < MvccHashJoinRecentPage::free_space_with_compaction(& *page) {
                if space_need > MvccHashJoinRecentPage::free_space_without_compaction(& *page) {
                    log_debug!("Compaction needed");
                    // Compaction needed, now just add new page.
                }
                else {
                    match page.try_upgrade(true) {
                        Ok(mut upgraded_page) => {
                            match MvccHashJoinRecentPage::insert(&mut *upgraded_page, key, pkey, ts, val) {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(_) => {
                                    panic!("Unexpected error");
                                }
                            }
                        }
                        Err(_) => {
                            log_debug!("Page upgrade failed, retrying");
                            attempts += 1;
                            std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                            continue;
                        }
                    }
                }
            }
            match page.try_upgrade(true) {
                Ok(mut upgraded_page) => {
                    let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                    let new_pid = new_page.get_id();
                    let new_fid = new_page.frame_id();
                    MvccHashJoinRecentPage::init(&mut *new_page);
                    match MvccHashJoinRecentPage::insert(&mut *new_page, key, pkey, ts, val) {
                        Ok(_) => {},
                        Err(_) => {
                            panic!("Unexpected error");
                        }
                    }
                    MvccHashJoinRecentPage::set_next_page(&mut *upgraded_page, new_pid, new_fid);
                    self.last_page_id.store(new_pid, atomic::Ordering::Release);
                    self.last_frame_id.store(new_fid, atomic::Ordering::Release);
                    drop(new_page);
                    return Ok(());
                }
                Err(_) => {
                    log_debug!("Page upgrade failed, retrying");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }  
            } 
        }
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let page = self.read_page(page_key);

            // Attempt to retrieve the value from the current page
            match MvccHashJoinRecentPage::get(&*page, key, pkey, ts) {
                Ok(val) => {
                    // Value found
                    return Ok(val);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    // Key not found in this page, check for next page
                    match MvccHashJoinRecentPage::next_page(&*page) {
                        Some((next_pid, next_fid)) => {
                            // Move to the next page
                            current_pid = next_pid;
                            current_fid = next_fid;
                            continue;
                        }
                        None => {
                            // End of the chain reached, key not found
                            return Err(AccessMethodError::KeyNotFound);
                        }
                    }
                }
                Err(e) => {
                    // Propagate other errors
                    return Err(e);
                }
            }
        }
    }
    
    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);
    
        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let mut page = self.write_page(page_key);
    
            // Attempt to update the value in the current page
            match MvccHashJoinRecentPage::update(&mut *page, key, pkey, ts, val) {
                Ok((old_ts, old_val)) => {
                    // Update successful
                    return Ok((old_ts, old_val));
                }
                Err(AccessMethodError::KeyNotFound) => {
                    // Key not found in this page, check for next page
                    match MvccHashJoinRecentPage::next_page(&*page) {
                        Some((next_pid, next_fid)) => {
                            // Move to the next page
                            current_pid = next_pid;
                            current_fid = next_fid;
                            continue;
                        }
                        None => {
                            // End of the chain reached, key not found
                            return Err(AccessMethodError::KeyNotFound);
                        }
                    }
                }
                Err(e) => {
                    // Propagate other errors
                    return Err(e);
                }
            }
        }
    }
    

    pub fn first_page_id(&self) -> PageId {
        self.first_page_id
    }

    pub fn first_frame_id(&self) -> u32 {
        self.first_frame_id.load(atomic::Ordering::Acquire)
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.mem_pool.get_page_for_write(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    log_warn!("Exclusive page latch grant failed: {:?}. Will retry", page_key);
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to write the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_chain_insert_and_get_entries() {
        // Initialize mem_pool and container key
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        // Create a new chain
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Entries to insert
        let entries: Vec<(&[u8], &[u8], Timestamp, &[u8])> = vec![
            (b"key1", b"pkey1", 10u64, b"value1"),
            (b"key2", b"pkey2", 20u64, b"value2"),
            (b"key_longer_than_prefix", b"pkey3", 30u64, b"value3"),
            (b"key4", b"pkey_longer_than_prefix", 40u64, b"value4"),
            (b"key_long", b"pkey_long", 50u64, b"value5"),
        ];

        // Insert entries
        for (key, pkey, ts, val) in &entries {
            chain.insert(key, pkey, *ts, val).unwrap();
        }

        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = chain.get(key, pkey, *ts).unwrap();
            assert_eq!(retrieved_val, *val);
        }

        // Attempt to retrieve a non-existent key
        let result = chain.get(b"nonexistent_key", b"nonexistent_pkey", 60u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_insert_empty_keys_and_values() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Entries with empty keys, pkeys, and values
        let entries: Vec<(&[u8], &[u8], Timestamp, &[u8])> = vec![
            (b"", b"pkey1", 10u64, b"value1"),
            (b"key2", b"", 20u64, b"value2"),
            (b"key3", b"pkey3", 30u64, b""),
            (b"", b"", 40u64, b""),
        ];

        // Insert entries
        for (key, pkey, ts, val) in &entries {
            chain.insert(key, pkey, *ts, val).unwrap();
        }

        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = chain.get(key, pkey, *ts).unwrap();
            assert_eq!(retrieved_val, *val);
        }
    }

    #[test]
    fn test_chain_insert_duplicate_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        chain.insert(b"key1", b"pkey1", 10u64, b"value1").unwrap();

        // Attempt to insert the same key-pkey with a different value and timestamp
        chain.insert(b"key1", b"pkey2", 20u64, b"value2").unwrap();

        // Retrieve the entry with the latest timestamp
        let retrieved_val = chain.get(b"key1", b"pkey2", 20u64).unwrap();
        assert_eq!(retrieved_val, b"value2");

        // Retrieve the entry with the earlier timestamp
        let retrieved_val = chain.get(b"key1", b"pkey1", 10u64).unwrap();
        assert_eq!(retrieved_val, b"value1");
    }

    #[test]
    fn test_chain_insert_large_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Generate a large value that is close to the page size limit
        let large_value = vec![b'a'; (AVAILABLE_PAGE_SIZE / 2) as usize];

        // Insert entries until a new page is allocated
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            chain.insert(&key, &pkey, i as u64 * 10, &large_value).unwrap();
        }

        // Verify that all entries can be retrieved
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let retrieved_val = chain.get(&key, &pkey, i as u64 * 10).unwrap();
            assert_eq!(retrieved_val, large_value);
        }
    }

    #[test]
    fn test_chain_insert_oversized_entry() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Generate a value larger than the page size
        let oversized_value = vec![b'a'; (AVAILABLE_PAGE_SIZE + 1) as usize];

        // Attempt to insert the oversized entry
        let result = chain.insert(b"key1", b"pkey1", 10u64, &oversized_value);
        assert!(matches!(result, Err(AccessMethodError::RecordTooLarge)));
    }

    #[test]
    fn test_chain_update_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        chain.insert(b"key1", b"pkey1", 10u64, b"value1").unwrap();

        // Update the entry
        let (old_ts, old_val) = chain.update(b"key1", b"pkey1", 20u64, b"value2").unwrap();
        assert_eq!(old_ts, 10u64);
        assert_eq!(old_val, b"value1");

        // Retrieve the updated entry
        let retrieved_val = chain.get(b"key1", b"pkey1", 20u64).unwrap();
        assert_eq!(retrieved_val, b"value2");

        // Attempt to update a non-existent entry
        let result = chain.update(b"key_nonexistent", b"pkey_nonexistent", 30u64, b"value3");
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_concurrent_inserts_and_reads() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = Arc::new(MvccHashJoinRecentChain::new(c_key, mem_pool.clone()));

        let chain_clone = chain.clone();
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in 0..100 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                chain_clone.insert(&key, &pkey, i as u64, &value).unwrap();
            }
        });

        // Read entries while inserts are happening
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = chain.get(&key, &pkey, i as u64);
        }

        handle.join().unwrap();

        // Verify all entries after insertions are complete
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let retrieved_val = chain.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val, expected_value);
        }
    }
}
