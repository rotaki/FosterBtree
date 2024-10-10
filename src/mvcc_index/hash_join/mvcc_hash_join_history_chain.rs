use std::{
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_warn,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::{
    mvcc_hash_join_history_page::MvccHashJoinHistoryPage,
    Timestamp,
};

pub struct MvccHashJoinHistoryChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    first_page_id: PageId,
    first_frame_id: AtomicU32,
}

impl<T: MemPool> MvccHashJoinHistoryChain<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let first_page_id = page.get_id();
        let first_frame_id = page.frame_id();

        // Initialize the page as a history page
        MvccHashJoinHistoryPage::init(&mut *page);
        drop(page);

        Self {
            mem_pool,
            c_key,
            first_page_id,
            first_frame_id: AtomicU32::new(first_frame_id),
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
        }
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let space_need = <Page as MvccHashJoinHistoryPage>::space_need(key, pkey, val);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }

        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);

        let base = 2;
        let mut attempts = 0;

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let page = self.read_page(page_key);

            if space_need < MvccHashJoinHistoryPage::free_space_without_compaction(&*page) {
                match page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        match MvccHashJoinHistoryPage::insert(
                            &mut *upgraded_page,
                            key,
                            pkey,
                            start_ts,
                            end_ts,
                            val,
                        ) {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(_) => {
                                panic!("Unexpected error during insert");
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
            } else {
                // Move to the next page if it exists
                match MvccHashJoinHistoryPage::next_page(&*page) {
                    Some((next_pid, next_fid)) => {
                        current_pid = next_pid;
                        current_fid = next_fid;
                        continue;
                    }
                    None => {
                        // Need to allocate a new page
                        match page.try_upgrade(true) {
                            Ok(mut upgraded_page) => {
                                let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                                let new_pid = new_page.get_id();
                                let new_fid = new_page.frame_id();
                                MvccHashJoinHistoryPage::init(&mut *new_page);
                                MvccHashJoinHistoryPage::insert(
                                    &mut *new_page,
                                    key,
                                    pkey,
                                    start_ts,
                                    end_ts,
                                    val,
                                )?;

                                MvccHashJoinHistoryPage::set_next_page(&mut *upgraded_page, new_pid, new_fid);
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

            match MvccHashJoinHistoryPage::get(&*page, key, pkey, ts) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    match MvccHashJoinHistoryPage::next_page(&*page) {
                        Some((next_pid, next_fid)) => {
                            current_pid = next_pid;
                            current_fid = next_fid;
                            continue;
                        }
                        None => {
                            return Err(AccessMethodError::KeyNotFound);
                        }
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let page = self.read_page(page_key);

            match page.try_upgrade(true) {
                Ok(mut upgraded_page) => {
                    match MvccHashJoinHistoryPage::update(
                        &mut *upgraded_page,
                        key,
                        pkey,
                        start_ts,
                        end_ts,
                        val,
                    ) {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(AccessMethodError::KeyNotFound) => {
                            // Try next page
                            match MvccHashJoinHistoryPage::next_page(&*upgraded_page) {
                                Some((next_pid, next_fid)) => {
                                    current_pid = next_pid;
                                    current_fid = next_fid;
                                    continue;
                                }
                                None => {
                                    return Err(AccessMethodError::KeyNotFound);
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                Err(_) => {
                    log_debug!("Page upgrade failed, retrying");
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
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
                    log_warn!(
                        "All frames are latched and cannot evict page to read the page: {:?}. Will retry",
                        page_key
                    );
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
    fn test_history_chain_insert_and_get() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // Entries to insert
        let entries: Vec<(&[u8], &[u8], Timestamp, Timestamp, &[u8])> = vec![
            (b"key1", b"pkey1", 10u64, 20u64, b"value1"),
            (b"key2", b"pkey2", 15u64, 25u64, b"value2"),
            (b"key1", b"pkey1", 20u64, 30u64, b"value3"),
        ];

        // Insert entries
        for (key, pkey, start_ts, end_ts, val) in &entries {
            chain.insert(key, pkey, *start_ts, *end_ts, val).unwrap();
        }

        // Retrieve entries at different timestamps
        let retrieved_val = chain.get(b"key1", b"pkey1", 12u64).unwrap();
        assert_eq!(retrieved_val, b"value1");

        let retrieved_val = chain.get(b"key1", b"pkey1", 22u64).unwrap();
        assert_eq!(retrieved_val, b"value3");

        // Attempt to retrieve a non-existent key
        let result = chain.get(b"key3", b"pkey3", 18u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    // #[test]
    // fn test_history_chain_update() {
    //     let mem_pool = get_in_mem_pool();
    //     let c_key = ContainerKey::new(0, 0);

    //     let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

    //     // Insert an entry
    //     chain.insert(b"key1", b"pkey1", 10u64, 20u64, b"value1").unwrap();

    //     // Update the entry's end_ts
    //     chain.update(b"key1", b"pkey1", 10u64, 25u64, b"value1").unwrap();

    //     // Retrieve the entry at a timestamp within the new range
    //     let retrieved_val = chain.get(b"key1", b"pkey1", 22u64).unwrap();
    //     assert_eq!(retrieved_val, b"value1");

    //     // Attempt to retrieve at a timestamp outside the new range
    //     let result = chain.get(b"key1", b"pkey1", 26u64);
    //     assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
    // }

    #[test]
    fn test_history_chain_basic_insert_and_get() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // Entries to insert
        let entries: Vec<(&[u8], &[u8], Timestamp, Timestamp, &[u8])> = vec![
            (b"key1", b"pkey1", 10u64, 20u64, b"value1"),
            (b"key2", b"pkey2", 15u64, 25u64, b"value2"),
            (b"key3", b"pkey3", 20u64, 30u64, b"value3"),
        ];

        // Insert entries
        for (key, pkey, start_ts, end_ts, val) in &entries {
            chain.insert(key, pkey, *start_ts, *end_ts, val).unwrap();
        }

        // Retrieve entries at different timestamps
        for (key, pkey, start_ts, end_ts, val) in &entries {
            let ts_within_range = (*start_ts + *end_ts) / 2;
            let retrieved_val = chain.get(key, pkey, ts_within_range).unwrap();
            assert_eq!(retrieved_val, *val);

            // Attempt to retrieve at a timestamp before the range
            let ts_before_range = start_ts - 1;
            let result = chain.get(key, pkey, ts_before_range);
            assert!(matches!(result, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));

            // Attempt to retrieve at a timestamp after the range
            let ts_after_range = end_ts;
            let result = chain.get(key, pkey, *ts_after_range);
            assert!(matches!(result, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
        }
    }

    #[test]
    fn test_history_chain_edge_cases_with_timestamps() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // Insert an entry with minimal timestamp range
        chain.insert(b"key_edge", b"pkey_edge", 0u64, 1u64, b"value_edge").unwrap();

        // Retrieve at start_ts
        let retrieved_val = chain.get(b"key_edge", b"pkey_edge", 0u64).unwrap();
        assert_eq!(retrieved_val, b"value_edge");

        // Attempt to retrieve at end_ts (should fail)
        let result = chain.get(b"key_edge", b"pkey_edge", 1u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
    }

    #[test]
    fn test_history_chain_multiple_versions_same_key() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // Insert multiple versions of the same key-pkey
        let versions = vec![
            (10u64, 20u64, b"value_v1"),
            (20u64, 30u64, b"value_v2"),
            (30u64, 40u64, b"value_v3"),
        ];

        for (start_ts, end_ts, val) in &versions {
            chain.insert(b"key_multi", b"pkey_multi", *start_ts, *end_ts, *val).unwrap();
        }

        // Retrieve each version at different timestamps
        for (i, (start_ts, end_ts, val)) in versions.iter().enumerate() {
            let ts_within_range = (*start_ts + *end_ts) / 2;
            let retrieved_val = chain.get(b"key_multi", b"pkey_multi", ts_within_range).unwrap();
            assert_eq!(retrieved_val, *val);

            // Attempt to retrieve at a timestamp outside the range
            let ts_out_of_range = if i == 0 {
                start_ts - 1
            } else {
                41 
            };
            let result = chain.get(b"key_multi", b"pkey_multi", ts_out_of_range);
            assert!(matches!(result, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
        }
    }

    #[test]
    fn test_history_chain_insert_causing_page_splits() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // Create a large value to fill the page quickly
        let large_val = vec![b'x'; (AVAILABLE_PAGE_SIZE / 4) as usize];

        // Insert entries until a new page is allocated
        let mut inserted_entries = vec![];
        for i in 0..10 {
            let key = format!("key_page_split_{}", i).into_bytes();
            let pkey = format!("pkey_page_split_{}", i).into_bytes();
            let start_ts = i * 10;
            let end_ts = start_ts + 10;
            chain.insert(&key, &pkey, start_ts, end_ts, &large_val).unwrap();
            inserted_entries.push((key, pkey, start_ts, end_ts, large_val.clone()));
        }

        // Verify that all entries can be retrieved
        for (key, pkey, start_ts, end_ts, val) in &inserted_entries {
            let ts_within_range = (start_ts + end_ts) / 2;
            let retrieved_val = chain.get(key, pkey, ts_within_range).unwrap();
            assert_eq!(retrieved_val, *val);
        }
    }

    #[test]
    fn test_history_chain_retrieve_non_existent_keys() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // Insert some entries
        chain.insert(b"key_exist", b"pkey_exist", 10u64, 20u64, b"value_exist").unwrap();

        // Attempt to retrieve a key that was never inserted
        let result = chain.get(b"key_nonexistent", b"pkey_nonexistent", 15u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

        // Attempt to retrieve with an incorrect pkey
        let result = chain.get(b"key_exist", b"pkey_wrong", 15u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

        // Attempt to retrieve with a timestamp outside the range
        let result = chain.get(b"key_exist", b"pkey_exist", 25u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
    }

    #[test]
    fn test_history_chain_insert_overlapping_timestamps() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
    
        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());
    
        // Insert entries with overlapping timestamp ranges for different keys
        chain.insert(b"key_overlap1", b"pkey1", 10u64, 30u64, b"value1").unwrap();
        chain.insert(b"key_overlap2", b"pkey2", 20u64, 40u64, b"value2").unwrap();
    
        // Retrieve entries at timestamps where ranges overlap
        let retrieved_val1 = chain.get(b"key_overlap1", b"pkey1", 25u64).unwrap();
        assert_eq!(retrieved_val1, b"value1");
    
        let retrieved_val2 = chain.get(b"key_overlap2", b"pkey2", 25u64).unwrap();
        assert_eq!(retrieved_val2, b"value2");
    }
    
    // #[test]
    // fn test_history_chain_insert_same_key_overlapping_ranges() {
    //     let mem_pool = get_in_mem_pool();
    //     let c_key = ContainerKey::new(0, 0);
    
    //     let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());
    
    //     // Insert entries with overlapping timestamp ranges for the same key-pkey
    //     chain.insert(b"key_same", b"pkey_same", 10u64, 30u64, b"value1").unwrap();
    //     chain.insert(b"key_same", b"pkey_same", 20u64, 40u64, b"value2").unwrap();
    
    //     // Retrieve at timestamps covered by both ranges
    //     let retrieved_val = chain.get(b"key_same", b"pkey_same", 25u64).unwrap();
    //     // Depending on the implementation, the chain might return the first or the last inserted value
    //     // Let's assume it returns the value with the latest start_ts less than or equal to ts
    //     assert_eq!(retrieved_val, b"value2");
    
    //     // Retrieve at timestamps covered by only one range
    //     let retrieved_val = chain.get(b"key_same", b"pkey_same", 15u64).unwrap();
    //     assert_eq!(retrieved_val, b"value1");
    // }
    
    // #[test]
    // fn test_history_chain_insert_with_max_timestamps() {
    //     let mem_pool = get_in_mem_pool();
    //     let c_key = ContainerKey::new(0, 0);

    //     let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

    //     let max_timestamp = u64::MAX;

    //     // Insert an entry with end_ts as u64::MAX
    //     chain.insert(b"key_max_ts", b"pkey_max_ts", 50u64, max_timestamp, b"value_max_ts").unwrap();

    //     // Retrieve at a timestamp less than max_timestamp
    //     let retrieved_val = chain.get(b"key_max_ts", b"pkey_max_ts", 100u64).unwrap();
    //     assert_eq!(retrieved_val, b"value_max_ts");

    //     // Attempt to retrieve at max_timestamp (should fail as end_ts is exclusive)
    //     let result = chain.get(b"key_max_ts", b"pkey_max_ts", max_timestamp);
    //     assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
    // }

    #[test]
    fn test_history_chain_insert_many_entries_spanning_multiple_pages() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

        // We'll calculate the number of entries needed to fill more than one page.
        // We'll use small keys and values to make the calculation straightforward.
        let key_base = b"key_multi_page";
        let pkey_base = b"pkey_multi_page";
        let val_base = b"value_multi_page";

        // Determine the space needed per entry.
        let key = key_base;
        let pkey = pkey_base;
        let val = val_base;
        let start_ts = 10u64;
        let end_ts = 20u64;

        let space_per_entry = <Page as MvccHashJoinHistoryPage>::space_need(key, pkey, val) as usize;

        // Calculate the number of entries to exceed one page
        let available_space = AVAILABLE_PAGE_SIZE;
        let entries_per_page = available_space / space_per_entry;
        let num_entries = entries_per_page * 3; // Enough to fill 3 pages

        // Insert entries
        for i in 0..num_entries {
            let key = format!("{}{}", std::str::from_utf8(key_base).unwrap(), i).into_bytes();
            let pkey = format!("{}{}", std::str::from_utf8(pkey_base).unwrap(), i).into_bytes();
            let val = format!("{}{}", std::str::from_utf8(val_base).unwrap(), i).into_bytes();
            let start_ts = 10u64 + i as u64;
            let end_ts = start_ts + 10;
            chain.insert(&key, &pkey, start_ts, end_ts, &val).unwrap();
        }

        // Retrieve entries
        for i in 0..num_entries {
            let key = format!("{}{}", std::str::from_utf8(key_base).unwrap(), i).into_bytes();
            let pkey = format!("{}{}", std::str::from_utf8(pkey_base).unwrap(), i).into_bytes();
            let val = format!("{}{}", std::str::from_utf8(val_base).unwrap(), i).into_bytes();
            let ts = 15u64 + i as u64; // Within the timestamp range of each entry

            let retrieved_val = chain.get(&key, &pkey, ts).unwrap();
            assert_eq!(retrieved_val, val);
        }

        // Optionally, verify that attempting to retrieve an entry at an invalid timestamp fails
        let invalid_ts = 5u64; // Before any entry's start_ts
        let result = chain.get(&key_base.to_vec(), &pkey_base.to_vec(), invalid_ts);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }


}
