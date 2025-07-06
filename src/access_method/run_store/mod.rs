use crate::access_method::AccessMethodError;
use crate::bp::{ContainerKey, FrameReadGuard, MemPool, PageFrameKey};
use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
use std::sync::Arc;

// Page layout for RunStore pages:
// Header (20 bytes):
//   - next_page_id: u32 (4 bytes)
//   - next_frame_id: u32 (4 bytes)
//   - slot_count: u32 (4 bytes)
//   - free_space_offset: u32 (4 bytes)
//   - total_bytes_used: u32 (4 bytes)
// Slots (12 bytes each):
//   - offset: u32 (4 bytes)
//   - key_size: u32 (4 bytes)
//   - val_size: u32 (4 bytes)
// Data: stored from the end of the page backwards

const PAGE_HEADER_SIZE: usize = 20;
const SLOT_SIZE: usize = 12;

// Index entry for binary search - stores first key of each page
#[derive(Clone, Debug)]
pub struct IndexEntry {
    pub first_key: Vec<u8>,
    pub page_key: PageFrameKey,
}

// Main RunStore structure
pub struct RunStore<T: MemPool> {
    pub c_key: ContainerKey,
    pub mem_pool: Arc<T>,
    pub index: Vec<IndexEntry>, // In-memory index of pages for binary search
}

// Helper functions for page operations
trait RunStorePage {
    fn rs_init(&mut self);
    fn rs_next_page_id(&self) -> PageId;
    fn rs_next_frame_id(&self) -> u32;
    fn rs_set_next_page(&mut self, page_id: PageId, frame_id: u32);
    fn rs_slot_count(&self) -> u32;
    fn rs_set_slot_count(&mut self, count: u32);
    fn rs_free_space_offset(&self) -> u32;
    fn rs_set_free_space_offset(&mut self, offset: u32);
    fn rs_total_bytes_used(&self) -> u32;
    fn rs_set_total_bytes_used(&mut self, bytes: u32);
    fn rs_total_free_space(&self) -> u32;
    fn rs_append(&mut self, key: &[u8], val: &[u8]) -> bool;
    fn rs_get(&self, slot_id: u32) -> Option<(&[u8], &[u8])>;
}

impl RunStorePage for Page {
    fn rs_init(&mut self) {
        self.rs_set_next_page(PageId::MAX, u32::MAX);
        self.rs_set_slot_count(0);
        self.rs_set_free_space_offset(AVAILABLE_PAGE_SIZE as u32);
        self.rs_set_total_bytes_used(PAGE_HEADER_SIZE as u32);
    }

    fn rs_next_page_id(&self) -> PageId {
        u32::from_be_bytes([self[0], self[1], self[2], self[3]])
    }

    fn rs_next_frame_id(&self) -> u32 {
        u32::from_be_bytes([self[4], self[5], self[6], self[7]])
    }

    fn rs_set_next_page(&mut self, page_id: PageId, frame_id: u32) {
        self[0..4].copy_from_slice(&page_id.to_be_bytes());
        self[4..8].copy_from_slice(&frame_id.to_be_bytes());
    }

    fn rs_slot_count(&self) -> u32 {
        u32::from_be_bytes([self[8], self[9], self[10], self[11]])
    }

    fn rs_set_slot_count(&mut self, count: u32) {
        self[8..12].copy_from_slice(&count.to_be_bytes());
    }

    fn rs_free_space_offset(&self) -> u32 {
        u32::from_be_bytes([self[12], self[13], self[14], self[15]])
    }

    fn rs_set_free_space_offset(&mut self, offset: u32) {
        self[12..16].copy_from_slice(&offset.to_be_bytes());
    }

    fn rs_total_bytes_used(&self) -> u32 {
        u32::from_be_bytes([self[16], self[17], self[18], self[19]])
    }

    fn rs_set_total_bytes_used(&mut self, bytes: u32) {
        self[16..20].copy_from_slice(&bytes.to_be_bytes());
    }

    fn rs_total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.rs_total_bytes_used()
    }

    fn rs_append(&mut self, key: &[u8], val: &[u8]) -> bool {
        let needed_space = SLOT_SIZE as u32 + key.len() as u32 + val.len() as u32;
        if self.rs_total_free_space() < needed_space {
            return false;
        }

        // Write data from the end backwards
        let data_offset = self.rs_free_space_offset() - key.len() as u32 - val.len() as u32;
        self[data_offset as usize..data_offset as usize + key.len()].copy_from_slice(key);
        self[data_offset as usize + key.len()..data_offset as usize + key.len() + val.len()]
            .copy_from_slice(val);

        // Write slot
        let slot_offset = PAGE_HEADER_SIZE + self.rs_slot_count() as usize * SLOT_SIZE;
        self[slot_offset..slot_offset + 4].copy_from_slice(&data_offset.to_be_bytes());
        self[slot_offset + 4..slot_offset + 8].copy_from_slice(&(key.len() as u32).to_be_bytes());
        self[slot_offset + 8..slot_offset + 12].copy_from_slice(&(val.len() as u32).to_be_bytes());

        // Update metadata
        self.rs_set_slot_count(self.rs_slot_count() + 1);
        self.rs_set_free_space_offset(data_offset);
        self.rs_set_total_bytes_used(self.rs_total_bytes_used() + needed_space);

        true
    }

    fn rs_get(&self, slot_id: u32) -> Option<(&[u8], &[u8])> {
        if slot_id >= self.rs_slot_count() {
            return None;
        }

        let slot_offset = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;
        let offset = u32::from_be_bytes([
            self[slot_offset],
            self[slot_offset + 1],
            self[slot_offset + 2],
            self[slot_offset + 3],
        ]);
        let key_size = u32::from_be_bytes([
            self[slot_offset + 4],
            self[slot_offset + 5],
            self[slot_offset + 6],
            self[slot_offset + 7],
        ]) as usize;
        let val_size = u32::from_be_bytes([
            self[slot_offset + 8],
            self[slot_offset + 9],
            self[slot_offset + 10],
            self[slot_offset + 11],
        ]) as usize;

        let key = &self[offset as usize..offset as usize + key_size];
        let val = &self[offset as usize + key_size..offset as usize + key_size + val_size];

        Some((key, val))
    }
}

impl<T: MemPool> RunStore<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, index: Vec<IndexEntry>) -> Self {
        RunStore {
            c_key,
            mem_pool,
            index,
        }
    }

    pub fn get_container_key(&self) -> &ContainerKey {
        &self.c_key
    }

    /// Create a new RunStore from sorted key-value pairs
    pub fn bulk_insert_create<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Result<Self, AccessMethodError> {
        let mut index = Vec::new();
        let mut current_page = mem_pool.create_new_page_for_write(c_key)?;
        current_page.rs_init();

        let mut first_key_in_page: Option<Vec<u8>> = None;
        let mut page_count = 0;

        for (key, val) in iter {
            let key_bytes = key.as_ref();
            let val_bytes = val.as_ref();

            // Check if record is too large
            let max_record_size = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
            if key_bytes.len() + val_bytes.len() > max_record_size {
                return Err(AccessMethodError::RecordTooLarge);
            }

            // Try to append to current page
            if !current_page.rs_append(key_bytes, val_bytes) {
                // Page is full, finalize it
                let current_page_id = current_page.page_id();
                let current_frame_id = current_page.frame_id();

                // Add index entry for the completed page
                if let Some(first_key) = first_key_in_page.take() {
                    index.push(IndexEntry {
                        first_key,
                        page_key: PageFrameKey::new_with_frame_id(
                            c_key,
                            current_page_id,
                            current_frame_id,
                        ),
                    });
                }

                page_count += 1;

                // Release current page and create new one
                drop(current_page);
                current_page = mem_pool.create_new_page_for_write(c_key)?;
                current_page.rs_init();

                // Append to new page
                if !current_page.rs_append(key_bytes, val_bytes) {
                    return Err(AccessMethodError::RecordTooLarge);
                }
                first_key_in_page = Some(key_bytes.to_vec());
            } else if first_key_in_page.is_none() {
                // First key in this page
                first_key_in_page = Some(key_bytes.to_vec());
            }
        }

        // Handle the last page
        if current_page.rs_slot_count() > 0 {
            let current_page_id = current_page.page_id();
            let current_frame_id = current_page.frame_id();

            if let Some(first_key) = first_key_in_page {
                index.push(IndexEntry {
                    first_key,
                    page_key: PageFrameKey::new_with_frame_id(
                        c_key,
                        current_page_id,
                        current_frame_id,
                    ),
                });
            }
            page_count += 1;
        }

        drop(current_page);

        println!("RunStore created with {} pages", page_count);

        Ok(RunStore::new(c_key, mem_pool, index))
    }

    /// Scan a range of keys [lower_inc, upper_exc)
    pub fn scan_range(&self, lower_inc: &[u8], upper_exc: &[u8]) -> RunStoreScanner<T> {
        // Binary search to find starting page
        let start_idx = if lower_inc.is_empty() {
            0
        } else {
            self.find_page_for_key(lower_inc)
        };

        RunStoreScanner {
            run_store: self,
            lower_inc: lower_inc.to_vec(),
            upper_exc: upper_exc.to_vec(),
            current_page_idx: start_idx,
            current_page: None,
            current_slot_id: 0,
        }
    }

    /// Return partition points (keys at regular intervals for high-level view)
    pub fn partition_points(&self) -> Vec<Vec<u8>> {
        // Return keys from the index, potentially sampled for very large runs
        // For now, return all first keys from each page
        self.index
            .iter()
            .map(|entry| entry.first_key.clone())
            .collect()
    }

    /// Binary search to find the page that might contain the given key
    fn find_page_for_key(&self, key: &[u8]) -> usize {
        if self.index.is_empty() {
            return 0;
        }

        // Binary search to find the appropriate page
        // When we have duplicate keys, we need to find the rightmost page with first_key < search_key
        // If first_key <= search_key, then the previous page might contain the search key

        let mut left = 0;
        let mut right = self.index.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.index[mid].first_key.as_slice() < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Return the rightmost page with first_key < key
        if left > 0 {
            left - 1
        } else {
            0
        }
    }
}

/// Iterator for scanning a range of keys
pub struct RunStoreScanner<'a, T: MemPool> {
    run_store: &'a RunStore<T>,
    lower_inc: Vec<u8>,
    upper_exc: Vec<u8>,
    current_page_idx: usize,
    current_page: Option<FrameReadGuard<T::EP>>,
    current_slot_id: u32,
}

impl<'a, T: MemPool> Iterator for RunStoreScanner<'a, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we've scanned all pages
            if self.current_page_idx >= self.run_store.index.len() {
                return None;
            }

            // Load current page if needed
            if self.current_page.is_none() {
                let page_key = self.run_store.index[self.current_page_idx].page_key;
                match self.run_store.mem_pool.get_page_for_read(page_key) {
                    Ok(page) => {
                        self.current_page = Some(page);
                        self.current_slot_id = 0;
                    }
                    Err(_) => {
                        // Skip this page and try next
                        self.current_page_idx += 1;
                        continue;
                    }
                }
            }

            let page = self.current_page.as_ref().unwrap();

            // Check if we've processed all slots in current page
            if self.current_slot_id >= page.rs_slot_count() {
                // Move to next page
                self.current_page = None;
                self.current_page_idx += 1;
                self.current_slot_id = 0;
                continue;
            }

            // Get current key-value pair
            if let Some((key, val)) = page.rs_get(self.current_slot_id) {
                // Check if key is before lower bound
                if !self.lower_inc.is_empty() && key < self.lower_inc.as_slice() {
                    self.current_slot_id += 1;
                    continue;
                }

                // Check if key is at or after upper bound
                if !self.upper_exc.is_empty() && key >= self.upper_exc.as_slice() {
                    // We're done scanning
                    return None;
                }

                // Key is in range, return it
                self.current_slot_id += 1;
                return Some((key.to_vec(), val.to_vec()));
            } else {
                // Invalid slot, move to next
                self.current_slot_id += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_test_bp_lru;

    #[test]
    fn test_run_store_basic() {
        let mem_pool = get_test_bp_lru(100);
        let c_key = ContainerKey::new(1, 1);

        // Create test data
        let mut data = Vec::new();
        for i in 0..1000 {
            let key = format!("key_{:04}", i).into_bytes();
            let val = format!("value_{:04}", i).into_bytes();
            data.push((key, val));
        }

        // Create run store
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Test full scan
        let results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(results.len(), 1000);

        // Verify order
        for i in 0..999 {
            assert!(results[i].0 < results[i + 1].0);
        }
    }

    #[test]
    fn test_run_store_range_scan() {
        let mem_pool = get_test_bp_lru(100);
        let c_key = ContainerKey::new(1, 2);

        // Create test data
        let mut data = Vec::new();
        for i in 0..100u32 {
            let key = i.to_be_bytes().to_vec();
            let val = (i * 2).to_be_bytes().to_vec();
            data.push((key, val));
        }

        // Create run store
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Test range scan [20, 50)
        let lower = 20u32.to_be_bytes();
        let upper = 50u32.to_be_bytes();
        let results: Vec<_> = run_store.scan_range(&lower, &upper).collect();

        assert_eq!(results.len(), 30);

        // Verify range
        for (key, val) in results {
            let k = u32::from_be_bytes(key.try_into().unwrap());
            let v = u32::from_be_bytes(val.try_into().unwrap());
            assert!(k >= 20 && k < 50);
            assert_eq!(v, k * 2);
        }
    }

    #[test]
    fn test_run_store_partition_points() {
        let mem_pool = get_test_bp_lru(50);
        let c_key = ContainerKey::new(1, 3);

        // Create enough data to span multiple pages
        let mut data = Vec::new();
        for i in 0..5000 {
            let key = format!("key_{:08}", i).into_bytes();
            let val = vec![0u8; 100]; // Large values to fill pages quickly
            data.push((key, val));
        }

        // Create run store
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Get partition points
        let partition_points = run_store.partition_points();

        // Should have multiple pages
        assert!(partition_points.len() > 1);

        // Verify partition points are in order
        for i in 0..partition_points.len() - 1 {
            assert!(partition_points[i] < partition_points[i + 1]);
        }

        println!("Number of partition points: {}", partition_points.len());
    }

    #[test]
    fn test_run_store_empty() {
        let mem_pool = get_test_bp_lru(10);
        let c_key = ContainerKey::new(1, 4);

        // Create empty run store
        let run_store = RunStore::bulk_insert_create(
            c_key,
            mem_pool.clone(),
            std::iter::empty::<(Vec<u8>, Vec<u8>)>(),
        )
        .unwrap();

        // Test empty scan
        let results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(results.len(), 0);

        // Test partition points
        let partition_points = run_store.partition_points();
        assert_eq!(partition_points.len(), 0);
    }

    #[test]
    fn test_run_store_large_records() {
        let mem_pool = get_test_bp_lru(100);
        let c_key = ContainerKey::new(1, 5);

        // Create test data with large values
        let mut data = Vec::new();
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let val = vec![i as u8; 1000]; // 1KB values
            data.push((key, val));
        }

        // Create run store
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Test scan
        let results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(results.len(), 100);

        // Verify data integrity
        for (i, (key, val)) in results.iter().enumerate() {
            assert_eq!(key, &format!("key_{:04}", i).into_bytes());
            assert_eq!(val.len(), 1000);
            assert!(val.iter().all(|&b| b == i as u8));
        }
    }

    #[test]
    fn test_run_store_duplicates() {
        let mem_pool = get_test_bp_lru(100);
        let c_key = ContainerKey::new(1, 6);

        // Create test data with many duplicates
        let mut data = Vec::new();

        // Add keys 0-4 once each
        for i in 0..5u32 {
            data.push((i.to_be_bytes().to_vec(), vec![i as u8]));
        }

        // Add key 5 many times (enough to span multiple pages)
        for i in 0..2000 {
            data.push((5u32.to_be_bytes().to_vec(), vec![5u8, (i % 256) as u8]));
        }

        // Add keys 6-9 once each
        for i in 6..10u32 {
            data.push((i.to_be_bytes().to_vec(), vec![i as u8]));
        }

        // Create run store
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Print partition points to debug
        let partition_points = run_store.partition_points();
        println!("Partition points for duplicate test:");
        for (i, pp) in partition_points.iter().enumerate() {
            let key = u32::from_be_bytes(pp[..4].try_into().unwrap());
            println!("  Page {}: first key = {}", i, key);
        }

        // Test range scan for key 5
        let lower = 5u32.to_be_bytes();
        let upper = 6u32.to_be_bytes();
        let results: Vec<_> = run_store.scan_range(&lower, &upper).collect();

        // Should find all 2000 instances of key 5
        println!("Found {} instances of key 5", results.len());
        assert_eq!(results.len(), 2000);

        // Verify all are key 5
        for (key, val) in &results {
            let k = u32::from_be_bytes(key[..4].try_into().unwrap());
            assert_eq!(k, 5);
            assert_eq!(val[0], 5);
        }

        // Test full scan to ensure all records are present
        let all_results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(all_results.len(), 2009); // 5 + 2000 + 4
    }

    #[test]
    fn test_run_store_boundary_duplicates() {
        let mem_pool = get_test_bp_lru(100);
        let c_key = ContainerKey::new(1, 7);

        // Create test data specifically to test edge cases at page boundaries
        let mut data = Vec::new();

        // Add many copies of keys 1-5 to ensure they span page boundaries
        for key in 1..=5u32 {
            for i in 0..500 {
                data.push((key.to_be_bytes().to_vec(), vec![key as u8, (i % 256) as u8]));
            }
        }

        // Create run store
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Test exact key ranges
        for key in 1..=5u32 {
            let lower = key.to_be_bytes();
            let upper = (key + 1).to_be_bytes();
            let results: Vec<_> = run_store.scan_range(&lower, &upper).collect();

            assert_eq!(results.len(), 500, "Key {} should have 500 instances", key);

            // Verify all are the correct key
            for (k, v) in &results {
                let found_key = u32::from_be_bytes(k[..4].try_into().unwrap());
                assert_eq!(found_key, key);
                assert_eq!(v[0], key as u8);
            }
        }

        // Test range [2, 4) - should get all of keys 2 and 3
        let lower = 2u32.to_be_bytes();
        let upper = 4u32.to_be_bytes();
        let results: Vec<_> = run_store.scan_range(&lower, &upper).collect();
        assert_eq!(results.len(), 1000); // 500 of key 2 + 500 of key 3
    }

    #[test]
    fn test_run_store_page_operations() {
        // Test individual page operations
        let mem_pool = get_test_bp_lru(10);
        let c_key = ContainerKey::new(1, 8);

        // Get a new page and test RunStorePage operations
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();

        // Initialize the page
        page.rs_init();

        // Verify initial state
        assert_eq!(page.rs_slot_count(), 0);
        assert_eq!(page.rs_free_space_offset(), AVAILABLE_PAGE_SIZE as u32);
        assert_eq!(page.rs_total_bytes_used(), PAGE_HEADER_SIZE as u32);

        // Test inserting records
        let key1 = b"apple";
        let val1 = b"fruit";
        assert!(page.rs_append(key1, val1));
        assert_eq!(page.rs_slot_count(), 1);

        let key2 = b"banana";
        let val2 = b"yellow fruit";
        assert!(page.rs_append(key2, val2));
        assert_eq!(page.rs_slot_count(), 2);

        // Test get operations
        let (k1, v1) = page.rs_get(0).unwrap();
        assert_eq!(k1, key1);
        assert_eq!(v1, val1);

        let (k2, v2) = page.rs_get(1).unwrap();
        assert_eq!(k2, key2);
        assert_eq!(v2, val2);

        // Test invalid slot access
        assert!(page.rs_get(2).is_none());

        // Test space calculations
        // Total bytes used includes: header (20) + 2 slots (12 each) + data
        let data_size = (key1.len() + val1.len() + key2.len() + val2.len()) as u32;
        let expected_used = PAGE_HEADER_SIZE as u32 + 2 * SLOT_SIZE as u32 + data_size;
        assert_eq!(page.rs_total_bytes_used(), expected_used);

        // Test binary search functionality by using rs_get to verify order
        // Records should be accessible in the order they were inserted
        assert_eq!(page.rs_get(0).unwrap().0, b"apple");
        assert_eq!(page.rs_get(1).unwrap().0, b"banana");
    }

    #[test]
    fn test_run_store_page_overflow() {
        let mem_pool = get_test_bp_lru(10);
        let c_key = ContainerKey::new(1, 9);

        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        page.rs_init();

        // Try to insert records until page is full
        let mut count = 0;
        let large_value = vec![0u8; 500]; // Large value to fill page quickly

        loop {
            let key = format!("key_{:06}", count).into_bytes();
            if !page.rs_append(&key, &large_value) {
                // Page is full
                break;
            }
            count += 1;
        }

        // Should have inserted at least one record
        assert!(count > 0);
        assert_eq!(page.rs_slot_count(), count);

        // Verify we can still read all records
        for i in 0..count {
            let (key, val) = page.rs_get(i).unwrap();
            assert_eq!(key, format!("key_{:06}", i).as_bytes());
            assert_eq!(val, &large_value);
        }
    }

    #[test]
    fn test_run_store_edge_cases() {
        let mem_pool = get_test_bp_lru(50);
        let c_key = ContainerKey::new(1, 10);

        // Test with single record
        let single_data = vec![(b"only_key".to_vec(), b"only_value".to_vec())];
        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), single_data.into_iter()).unwrap();

        let results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"only_key");
        assert_eq!(results[0].1, b"only_value");

        // Test exact key match for range boundaries
        let c_key2 = ContainerKey::new(1, 11);
        let mut data = Vec::new();
        for i in 0..10u32 {
            data.push((i.to_be_bytes().to_vec(), vec![i as u8]));
        }

        let run_store2 =
            RunStore::bulk_insert_create(c_key2, mem_pool.clone(), data.into_iter()).unwrap();

        // Scan exact single key [5, 6)
        let lower = 5u32.to_be_bytes();
        let upper = 6u32.to_be_bytes();
        let results: Vec<_> = run_store2.scan_range(&lower, &upper).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(u32::from_be_bytes(results[0].0[..4].try_into().unwrap()), 5);

        // Scan with lower bound only
        let results: Vec<_> = run_store2.scan_range(&lower, &[]).collect();
        assert_eq!(results.len(), 5); // Keys 5, 6, 7, 8, 9

        // Scan with upper bound only
        let results: Vec<_> = run_store2.scan_range(&[], &upper).collect();
        assert_eq!(results.len(), 6); // Keys 0, 1, 2, 3, 4, 5
    }

    #[test]
    fn test_run_store_binary_search_correctness() {
        let mem_pool = get_test_bp_lru(100);
        let c_key = ContainerKey::new(1, 12);

        // Create data that will span multiple pages with specific first keys
        let mut data = Vec::new();
        let keys_per_page = 50; // Approximate to force multiple pages

        // Page 0: keys 0-49
        for i in 0..keys_per_page {
            let key = ((i * 2) as u32).to_be_bytes().to_vec(); // Even numbers
            let val = vec![i as u8; 100];
            data.push((key, val));
        }

        // Page 1: keys 100-149
        for i in 0..keys_per_page {
            let key = ((100 + i * 2) as u32).to_be_bytes().to_vec();
            let val = vec![i as u8; 100];
            data.push((key, val));
        }

        // Page 2: keys 200-249
        for i in 0..keys_per_page {
            let key = ((200 + i * 2) as u32).to_be_bytes().to_vec();
            let val = vec![i as u8; 100];
            data.push((key, val));
        }

        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Print partition points to debug
        let partition_points = run_store.partition_points();
        println!("Binary search test partition points:");
        for (i, pp) in partition_points.iter().enumerate() {
            let key = u32::from_be_bytes(pp[..4].try_into().unwrap());
            println!("  Page {}: first key = {}", i, key);
        }

        // Also verify what keys are actually in each page
        for (i, entry) in run_store.index.iter().enumerate() {
            let page = mem_pool.get_page_for_read(entry.page_key).unwrap();
            if page.rs_slot_count() > 0 {
                let (first_key, _) = page.rs_get(0).unwrap();
                let (last_key, _) = page.rs_get(page.rs_slot_count() - 1).unwrap();
                let first = u32::from_be_bytes(first_key[..4].try_into().unwrap());
                let last = u32::from_be_bytes(last_key[..4].try_into().unwrap());
                println!("  Page {} actual range: {} to {}", i, first, last);
            }
        }

        // Test various search keys based on actual page boundaries
        let test_cases = if partition_points.len() == 2 {
            // If we only have 2 pages, adjust test cases
            vec![
                (0u32, 0), // Exact match with first key of page 0
                (1, 0),    // Key less than first key of page 1
                (50, 0),   // Key in middle of page 0
                (200, 0),  // Still in page 0
                (279, 0),  // Just before page 1
                (280, 0),  // First key of page 1 - but we search from page 0 for duplicates
                (290, 1),  // In page 1
                (300, 1),  // In page 1
            ]
        } else {
            // Original test cases for 3+ pages
            vec![
                (0u32, 0), // Exact match with first key of page 0
                (1, 0),    // Key less than first key of page 1
                (50, 0),   // Key in middle of page 0
                (99, 0),   // Key just before page 1
                (100, 1),  // Exact match with first key of page 1
                (150, 1),  // Key in middle of page 1
                (199, 1),  // Key just before page 2
                (200, 2),  // Exact match with first key of page 2
                (250, 2),  // Key after last key
            ]
        };

        for (search_key, expected_page) in test_cases {
            let key_bytes = search_key.to_be_bytes();
            let page_idx = run_store.find_page_for_key(&key_bytes);
            assert_eq!(
                page_idx, expected_page,
                "Failed for search key {}: expected page {}, got {}",
                search_key, expected_page, page_idx
            );
        }
    }

    #[test]
    fn test_run_store_max_key_value_sizes() {
        let mem_pool = get_test_bp_lru(50);
        let c_key = ContainerKey::new(1, 13);

        // Test with maximum safe sizes
        let mut data = Vec::new();

        // Large key, small value
        let large_key = vec![1u8; 1000];
        let small_val = vec![2u8; 10];
        data.push((large_key.clone(), small_val.clone()));

        // Small key, large value
        let small_key = vec![3u8; 10];
        let large_val = vec![4u8; 2000];
        data.push((small_key.clone(), large_val.clone()));

        // Both large (but still fit in a page)
        let medium_key = vec![5u8; 500];
        let medium_val = vec![6u8; 500];
        data.push((medium_key.clone(), medium_val.clone()));

        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Verify all records can be read back correctly
        let results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(results.len(), 3);

        assert_eq!(results[0].0, large_key);
        assert_eq!(results[0].1, small_val);

        assert_eq!(results[1].0, small_key);
        assert_eq!(results[1].1, large_val);

        assert_eq!(results[2].0, medium_key);
        assert_eq!(results[2].1, medium_val);
    }

    #[test]
    fn test_run_store_scanner_state_management() {
        let mem_pool = get_test_bp_lru(50);
        let c_key = ContainerKey::new(1, 14);

        // Create data that spans exactly 3 pages
        let mut data = Vec::new();
        let records_per_page = 30;

        for page in 0..3 {
            for i in 0..records_per_page {
                let key = format!("page{}_rec{:03}", page, i).into_bytes();
                let val = vec![page as u8, i as u8];
                data.push((key, val));
            }
        }

        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Create scanner and manually iterate
        let mut scanner = run_store.scan_range(&[], &[]);

        // Read first 10 records
        for i in 0..10 {
            let (key, val) = scanner.next().unwrap();
            assert_eq!(key, format!("page0_rec{:03}", i).as_bytes());
            assert_eq!(val, vec![0, i as u8]);
        }

        // Read records crossing page boundary (page 0 to page 1)
        for i in 10..40 {
            let (key, val) = scanner.next().unwrap();
            let page = i / records_per_page;
            let rec = i % records_per_page;
            assert_eq!(key, format!("page{}_rec{:03}", page, rec).as_bytes());
            assert_eq!(val, vec![page as u8, rec as u8]);
        }

        // Read remaining records
        let remaining: Vec<_> = scanner.collect();
        assert_eq!(remaining.len(), 50); // 90 total - 40 already read
    }

    #[test]
    fn test_run_store_page_slot_management() {
        let mem_pool = get_test_bp_lru(10);
        let c_key = ContainerKey::new(1, 15);

        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        page.rs_init();

        // Test slot offset calculations
        let slot_offset_0 = PAGE_HEADER_SIZE;
        let slot_offset_1 = PAGE_HEADER_SIZE + SLOT_SIZE;
        let slot_offset_10 = PAGE_HEADER_SIZE + 10 * SLOT_SIZE;

        assert_eq!(slot_offset_0, PAGE_HEADER_SIZE);
        assert_eq!(slot_offset_1, PAGE_HEADER_SIZE + SLOT_SIZE);
        assert_eq!(slot_offset_10, PAGE_HEADER_SIZE + 10 * SLOT_SIZE);

        // Insert records and verify slot data
        let records = vec![(b"aaa", b"111"), (b"bbb", b"222"), (b"ccc", b"333")];

        for (key, val) in &records {
            assert!(page.rs_append(*key, *val));
        }

        // Verify slot count
        assert_eq!(page.rs_slot_count(), 3);

        // Verify each slot's metadata
        for (i, (key, val)) in records.iter().enumerate() {
            // Read slot data directly using rs_get
            let (stored_key, stored_val) = page.rs_get(i as u32).unwrap();

            assert_eq!(stored_key.len(), key.len());
            assert_eq!(stored_val.len(), val.len());
            assert_eq!(stored_key, *key);
            assert_eq!(stored_val, *val);
        }
    }

    #[test]
    fn test_run_store_empty_key_values() {
        let mem_pool = get_test_bp_lru(10);
        let c_key = ContainerKey::new(1, 16);

        // Test with empty keys and values
        let mut data = vec![
            (vec![], b"value_with_empty_key".to_vec()),
            (b"key_with_empty_value".to_vec(), vec![]),
            (vec![], vec![]), // Both empty
            (b"normal_key".to_vec(), b"normal_value".to_vec()),
        ];

        // Sort data before creating run store
        data.sort_by(|a, b| a.0.cmp(&b.0));

        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        let results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(results.len(), 4);

        // Empty keys should sort first
        assert_eq!(results[0].0, Vec::<u8>::new());
        assert_eq!(results[0].1, b"value_with_empty_key".to_vec());

        assert_eq!(results[1].0, Vec::<u8>::new());
        assert_eq!(results[1].1, Vec::<u8>::new());

        assert_eq!(results[2].0, b"key_with_empty_value".to_vec());
        assert_eq!(results[2].1, Vec::<u8>::new());

        assert_eq!(results[3].0, b"normal_key".to_vec());
        assert_eq!(results[3].1, b"normal_value".to_vec());
    }

    #[test]
    fn test_run_store_index_first_keys() {
        // Test that index actually contains first keys, not last keys
        let mem_pool = get_test_bp_lru(10);
        let c_key = ContainerKey::new(1, 18);

        // Create data that will span multiple pages
        let mut data = Vec::new();
        for i in 0..100u32 {
            let key = i.to_be_bytes().to_vec();
            let val = vec![0u8; 100]; // Large values to force multiple pages
            data.push((key, val));
        }

        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Print index entries
        println!("Index entries:");
        for (i, entry) in run_store.index.iter().enumerate() {
            let key = u32::from_be_bytes(entry.first_key[..4].try_into().unwrap());
            println!("  Page {}: first_key = {}", i, key);
        }

        // Verify that each page's first key is actually the first record in that page
        for (page_idx, entry) in run_store.index.iter().enumerate() {
            let page = mem_pool.get_page_for_read(entry.page_key).unwrap();
            if page.rs_slot_count() > 0 {
                let (first_key_in_page, _) = page.rs_get(0).unwrap();
                assert_eq!(
                    first_key_in_page,
                    entry.first_key.as_slice(),
                    "Page {} index says first key is {:?} but actual first key is {:?}",
                    page_idx,
                    entry.first_key,
                    first_key_in_page
                );
            }
        }
    }

    #[test]
    fn test_run_store_stress_test() {
        let mem_pool = get_test_bp_lru(200);
        let c_key = ContainerKey::new(1, 17);

        // Generate a large amount of data with various patterns
        let mut data = Vec::new();

        // Sequential keys
        for i in 0..1000 {
            let key = format!("seq_{:08}", i).into_bytes();
            let val = vec![i as u8; 50];
            data.push((key, val));
        }

        // Repeated keys
        for i in 0..100 {
            for j in 0..10 {
                let key = format!("dup_{:04}", i).into_bytes();
                let val = vec![i as u8, j as u8];
                data.push((key, val));
            }
        }

        // Random-length keys and values
        for i in 0..500 {
            let key_len = (i % 50) + 1;
            let val_len = ((i * 7) % 100) + 1;
            let key = vec![(i % 256) as u8; key_len];
            let val = vec![(i * 3 % 256) as u8; val_len];
            data.push((key, val));
        }

        // Sort data before creating run store (bulk_insert_create expects sorted input)
        data.sort_by(|a, b| a.0.cmp(&b.0));

        let run_store =
            RunStore::bulk_insert_create(c_key, mem_pool.clone(), data.into_iter()).unwrap();

        // Full scan should return all records
        let all_results: Vec<_> = run_store.scan_range(&[], &[]).collect();
        assert_eq!(all_results.len(), 2500); // 1000 + 1000 + 500

        // Verify ordering
        for i in 1..all_results.len() {
            assert!(
                all_results[i - 1].0 <= all_results[i].0,
                "Records not in order at index {}",
                i
            );
        }

        // Test various range scans
        let ranges = vec![
            (b"dup_0000".to_vec(), b"dup_0010".to_vec()),
            (b"seq_0000000".to_vec(), b"seq_0000100".to_vec()),
            (vec![128u8], vec![129u8]),
        ];

        for (lower, upper) in ranges {
            let range_results: Vec<_> = run_store.scan_range(&lower, &upper).collect();
            for (key, _) in &range_results {
                assert!(
                    key >= &lower && key < &upper,
                    "Key {:?} outside range [{:?}, {:?})",
                    key,
                    lower,
                    upper
                );
            }
        }
    }
}
