// Append and get only page
// This page can potentially support insert, delete, and update operations in the future
// but currently only supports append and get operations.

// Page layout:
// 4 byte: next page id
// 4 byte: next frame id
// 4 byte: key size
// 4 byte: value size
// 4 byte: slot count
pub const FPS_PAGE_HEADER_SIZE: usize = 4 * 5;

use crate::prelude::{Page, PageId, AVAILABLE_PAGE_SIZE};

/// A fixed-size slotted page where the key and value sizes are known at
/// runtime.
pub trait FixedSizePage {
    /* ---------- page-level metadata ---------- */
    fn init(&mut self, key_size: usize, val_size: usize);

    fn next_page(&self) -> Option<(PageId, u32)>;
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);

    fn set_kv_sizes(&mut self, key_size: usize, val_size: usize);

    #[cfg(test)]
    fn max_records(&self) -> u32 {
        // Calculate the maximum number of records that can fit in the page
        let record_size = self.record_size();
        if record_size == 0 {
            return 0; // Avoid division by zero
        }
        (AVAILABLE_PAGE_SIZE - FPS_PAGE_HEADER_SIZE) as u32 / record_size as u32
    }

    fn total_bytes_used(&self) -> u32 {
        // This will be overridden with proper size calculation per page
        FPS_PAGE_HEADER_SIZE as u32 + self.slot_count() * self.record_size() as u32
    }

    fn slot_count(&self) -> u32;
    fn set_slot_count(&mut self, n: u32);

    fn key_size(&self) -> u32;
    fn val_size(&self) -> u32;

    fn record_size(&self) -> usize {
        (self.key_size() + self.val_size()) as usize
    }

    fn offset_for_slot(&self, slot_idx: u32) -> usize {
        FPS_PAGE_HEADER_SIZE + (slot_idx as usize * self.record_size())
    }

    fn total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.total_bytes_used()
    }

    /* ---------- record access ---------- */

    /// Appends one fixed-width record if it fits.
    ///
    /// Returns `false` and leaves the page unchanged when there is
    /// insufficient space.
    fn append(&mut self, key: &[u8], value: &[u8]) -> bool;

    /// Gets the record at the specified slot index.
    fn get_slot(&self, slot_idx: u32) -> Option<(&[u8], &[u8])>;

    /// Iterator over every key–value pair in slot order.
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        (0..self.slot_count()).filter_map(move |slot_idx| self.get_slot(slot_idx))
    }
}

// Implement FixedSizePage directly on Page to avoid complex wrapper issues
impl FixedSizePage for Page {
    fn init(&mut self, key_size: usize, val_size: usize) {
        let next_page_id = PageId::MAX;
        let next_frame_id = u32::MAX;
        let slot_count = 0;

        self.set_next_page(next_page_id, next_frame_id);
        self.set_kv_sizes(key_size, val_size);
        self.set_slot_count(slot_count);
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        let next_page_id = u32::from_be_bytes([self[0], self[1], self[2], self[3]]);
        let next_frame_id = u32::from_be_bytes([self[4], self[5], self[6], self[7]]);
        if next_page_id == PageId::MAX {
            None
        } else {
            Some((next_page_id, next_frame_id))
        }
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        self[0..4].copy_from_slice(&next_page_id.to_be_bytes());
        self[4..8].copy_from_slice(&frame_id.to_be_bytes());
    }

    fn set_kv_sizes(&mut self, key_size: usize, val_size: usize) {
        let offset = 8; // After next_page_id and next_frame_id
        self[offset..offset + std::mem::size_of::<u32>()]
            .copy_from_slice(&(key_size as u32).to_be_bytes());
        self[offset + 4..offset + 4 + std::mem::size_of::<u32>()]
            .copy_from_slice(&(val_size as u32).to_be_bytes());
    }

    fn key_size(&self) -> u32 {
        let offset = 8; // After next_page_id and next_frame_id
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn val_size(&self) -> u32 {
        let offset = 12;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn total_bytes_used(&self) -> u32 {
        // This will be overridden with proper size calculation per page
        FPS_PAGE_HEADER_SIZE as u32 + self.slot_count() * self.record_size() as u32
    }

    fn slot_count(&self) -> u32 {
        let offset = 16;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_slot_count(&mut self, slot_count: u32) {
        let offset = 16;
        self[offset..offset + std::mem::size_of::<u32>()]
            .copy_from_slice(&slot_count.to_be_bytes());
    }

    fn total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.total_bytes_used()
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> bool {
        debug_assert_eq!(key.len(), self.key_size() as usize);
        debug_assert_eq!(value.len(), self.val_size() as usize);
        let record_size = key.len() + value.len();
        if record_size > self.total_free_space() as usize {
            return false; // Not enough space to append
        }
        let slot_count = self.slot_count();
        let offset = self.offset_for_slot(slot_count);
        // Write key and value into the page
        self[offset..offset + key.len()].copy_from_slice(key);
        self[offset + key.len()..offset + key.len() + value.len()].copy_from_slice(value);
        // Update total bytes used and slot count
        self.set_slot_count(slot_count + 1);
        true
    }

    fn get_slot(&self, slot_idx: u32) -> Option<(&[u8], &[u8])> {
        if slot_idx >= self.slot_count() {
            return None; // Invalid slot index
        }
        let offset = self.offset_for_slot(slot_idx);
        let key = &self[offset..offset + self.key_size() as usize];
        let value = &self[offset + self.key_size() as usize..offset + self.record_size()];
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_initialization() {
        let mut page = Page::new_empty();
        page.init(4, 8);

        assert_eq!(page.total_bytes_used(), FPS_PAGE_HEADER_SIZE as u32);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(
            page.total_free_space(),
            (AVAILABLE_PAGE_SIZE - FPS_PAGE_HEADER_SIZE) as u32
        );
        assert_eq!(page.next_page(), None);
    }

    #[test]
    fn test_set_next_page() {
        let mut page = Page::new_empty();
        page.init(4, 8);
        page.set_next_page(123, 456);

        assert_eq!(page.next_page(), Some((123, 456)));
    }

    #[test]
    fn test_set_kv_sizes() {
        let mut page = Page::new_empty();
        page.init(6, 10);

        assert_eq!(page.key_size(), 6);
        assert_eq!(page.val_size(), 10);
        assert_eq!(page.record_size(), 16);
    }

    #[test]
    fn test_wrapper_methods() {
        let mut page = Page::new_empty();
        page.init(4, 8);

        assert_eq!(page.key_size(), 4);
        assert_eq!(page.val_size(), 8);
        assert_eq!(page.record_size(), 12);
    }

    #[test]
    fn test_record_append() {
        let mut page = Page::new_empty();
        page.init(4, 8);

        let key = [1, 2, 3, 4];
        let value = [6, 7, 8, 9, 10, 11, 12, 13];
        let success = page.append(&key, &value);

        assert!(success);
        assert_eq!(page.slot_count(), 1);
        assert_eq!(page.total_bytes_used(), (FPS_PAGE_HEADER_SIZE + 12) as u32);

        let (retrieved_key, retrieved_value) = page.get_slot(0).unwrap();
        assert_eq!(retrieved_key, key.to_vec());
        assert_eq!(retrieved_value, value.to_vec());
    }

    #[test]
    fn test_record_append_multiple() {
        let mut page = Page::new_empty();
        page.init(4, 8);

        let key1 = [1, 2, 3, 4];
        let value1 = [5, 6, 7, 8, 9, 10, 11, 12];
        let success1 = page.append(&key1, &value1);
        assert!(success1);

        let key2 = [13, 14, 15, 16];
        let value2 = [17, 18, 19, 20, 21, 22, 23, 24];
        let success2 = page.append(&key2, &value2);
        assert!(success2);

        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.iter().count(), 2);

        let (retrieved_key1, retrieved_value1) = page.get_slot(0).unwrap();
        assert_eq!(retrieved_key1, key1.to_vec());
        assert_eq!(retrieved_value1, value1.to_vec());

        let (retrieved_key2, retrieved_value2) = page.get_slot(1).unwrap();
        assert_eq!(retrieved_key2, key2.to_vec());
        assert_eq!(retrieved_value2, value2.to_vec());
    }

    #[test]
    fn test_record_append_failure_due_to_size() {
        let mut page = Page::new_empty();
        page.init(4, 8);

        let max_records = page.max_records();

        // Fill up the page
        for i in 0..max_records {
            let key = [(i % 256) as u8; 4];
            let value = [(i % 256) as u8; 8];
            let success = page.append(&key, &value);
            assert!(success, "Failed to append record {}", i);
        }

        // Try to add one more - should fail
        let key = [255, 255, 255, 255];
        let value = [255, 255, 255, 255, 255, 255, 255, 255];
        let success = page.append(&key, &value);

        assert!(!success);
        assert_eq!(page.slot_count(), max_records as u32);
    }

    #[test]
    fn test_iterator() {
        let mut page = Page::new_empty();
        page.init(4, 8);

        let records = vec![
            ([1, 2, 3, 4], [5, 6, 7, 8, 9, 10, 11, 12]),
            ([13, 14, 15, 16], [17, 18, 19, 20, 21, 22, 23, 24]),
            ([25, 26, 27, 28], [29, 30, 31, 32, 33, 34, 35, 36]),
        ];

        for (key, value) in &records {
            assert!(page.append(key, value));
        }

        let collected: Vec<_> = page.iter().collect();
        assert_eq!(collected.len(), records.len());

        for (i, (key, value)) in collected.iter().enumerate() {
            assert_eq!(key, &records[i].0.to_vec());
            assert_eq!(value, &records[i].1.to_vec());
        }
    }
}
