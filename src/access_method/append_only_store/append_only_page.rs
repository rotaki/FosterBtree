// Append and get only page
// This page can potentially support insert, delete, and update operations in the future
// but currently only supports append and get operations.

// Page layout:
// 4 byte: next page id
// 4 byte: next frame id
// 4 byte: total bytes used (PAGE_HEADER_SIZE + records)
// 4 byte: record count
pub const APS_PAGE_HEADER_SIZE: usize = 4 * 4;
pub const APS_RECORD_METADATA_SIZE: usize = 8; // 4 bytes for key size, 4 bytes for value size

use crate::prelude::{Page, PageId, AVAILABLE_PAGE_SIZE};

pub trait AppendOnlyPage {
    fn init(&mut self);
    fn max_record_size() -> usize {
        AVAILABLE_PAGE_SIZE - APS_PAGE_HEADER_SIZE
    }

    // Header operations
    fn next_page(&self) -> Option<(PageId, u32)>; // (next_page_id, next_frame_id)
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);
    fn total_bytes_used(&self) -> u32;
    fn total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.total_bytes_used()
    }
    fn set_total_bytes_used(&mut self, total_bytes_used: u32);
    fn slot_count(&self) -> u32;
    fn set_slot_count(&mut self, slot_count: u32);
    fn increment_slot_count(&mut self) {
        let slot_count = self.slot_count();
        self.set_slot_count(slot_count + 1);
    }

    /// Try to append a key value pair to the page.
    /// If the key value is too large to fit in the page, return false.
    /// When false is returned, the page is not modified.
    /// Otherwise, the key value is appended to the page and the page is modified.
    fn append(&mut self, key: &[u8], value: &[u8]) -> bool;

    fn get_at(&self, offset: u32) -> Option<(&[u8], &[u8])>;

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_;
}

impl AppendOnlyPage for Page {
    fn init(&mut self) {
        let next_page_id = PageId::MAX;
        let next_frame_id = u32::MAX;
        let total_bytes_used = APS_PAGE_HEADER_SIZE as u32;
        let slot_count = 0;

        self.set_next_page(next_page_id, next_frame_id);
        self.set_total_bytes_used(total_bytes_used);
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

    fn total_bytes_used(&self) -> u32 {
        let offset = 8;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
        let offset = 8;
        self[offset..offset + std::mem::size_of::<u32>()]
            .copy_from_slice(&total_bytes_used.to_be_bytes());
    }

    fn slot_count(&self) -> u32 {
        let offset = 12;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_slot_count(&mut self, slot_count: u32) {
        let offset = 12;
        self[offset..offset + std::mem::size_of::<u32>()]
            .copy_from_slice(&slot_count.to_be_bytes());
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> bool {
        let total_len = key.len() + value.len() + APS_RECORD_METADATA_SIZE; // 8 bytes (4 for key size, 4 for value size)
                                                                            // Check if the page has enough space for slot and the record
        if self.total_free_space() < total_len as u32 {
            false
        } else {
            // Append the slot and the record
            let mut offset = self.total_bytes_used();
            self[offset as usize..offset as usize + 4]
                .copy_from_slice(&(key.len() as u32).to_be_bytes());
            offset += 4;
            self[offset as usize..offset as usize + 4]
                .copy_from_slice(&(value.len() as u32).to_be_bytes());
            offset += 4;
            self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
            offset += key.len() as u32;
            self[offset as usize..offset as usize + value.len()].copy_from_slice(value);
            offset += value.len() as u32;

            self.increment_slot_count();
            self.set_total_bytes_used(offset);
            true
        }
    }

    fn get_at(&self, offset: u32) -> Option<(&[u8], &[u8])> {
        assert!(
            offset >= APS_PAGE_HEADER_SIZE as u32,
            "Offset must be at least the size of the page header"
        );
        let mut offset = offset as usize;
        if offset + APS_RECORD_METADATA_SIZE > self.total_bytes_used() as usize {
            return None; // Offset is beyond the used space of the page
        }
        let key_size = u32::from_be_bytes(self[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let value_size = u32::from_be_bytes(self[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + key_size + value_size > self.total_bytes_used() as usize {
            return None; // Not enough space for key or value
        }
        let key = &self[offset..offset + key_size];
        offset += key_size;
        let value = &self[offset..offset + value_size];
        Some((key, value))
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        let mut offset = APS_PAGE_HEADER_SIZE as u32;
        let slot_count = self.slot_count();
        let mut current_slot = 0;

        std::iter::from_fn(move || {
            if current_slot >= slot_count {
                return None;
            }

            let key_size = u32::from_be_bytes(
                self[offset as usize..offset as usize + 4]
                    .try_into()
                    .unwrap(),
            );
            offset += 4;

            let value_size = u32::from_be_bytes(
                self[offset as usize..offset as usize + 4]
                    .try_into()
                    .unwrap(),
            );
            offset += 4;

            let key = &self[offset as usize..offset as usize + key_size as usize];
            offset += key_size;

            let value = &self[offset as usize..offset as usize + value_size as usize];
            offset += value_size;

            current_slot += 1;
            Some((key, value))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_initialization() {
        let mut page = Page::new_empty();
        page.init();

        assert_eq!(page.total_bytes_used(), APS_PAGE_HEADER_SIZE as u32);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(
            page.total_free_space(),
            (AVAILABLE_PAGE_SIZE - APS_PAGE_HEADER_SIZE) as u32
        );
        assert_eq!(page.next_page(), None);
    }

    #[test]
    fn test_set_next_page() {
        let mut page = Page::new_empty();
        page.set_next_page(123, 456);

        assert_eq!(page.next_page(), Some((123, 456)));
    }

    #[test]
    fn test_record_append() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3, 4, 5];
        let value = vec![6, 7, 8, 9, 10];
        let success = page.append(&key, &value);

        assert!(success);
        assert_eq!(
            page.iter().next().unwrap(),
            (key.as_slice(), value.as_slice())
        );
        assert_eq!(page.slot_count(), 1);
        assert_eq!(
            page.total_bytes_used(),
            (APS_PAGE_HEADER_SIZE + 8 + key.len() + value.len()) as u32
        );
    }

    #[test]
    fn test_record_append_multiple() {
        let mut page = Page::new_empty();
        page.init();

        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];
        let success1 = page.append(&key1, &value1);
        assert!(success1);

        let key2 = vec![7, 8, 9];
        let value2 = vec![10, 11, 12];
        let success2 = page.append(&key2, &value2);
        assert!(success2);

        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.iter().count(), 2);
        let mut iter = page.iter();
        assert_eq!(iter.next().unwrap(), (key1.as_slice(), value1.as_slice()));
        assert_eq!(iter.next().unwrap(), (key2.as_slice(), value2.as_slice()));
    }

    #[test]
    fn test_record_append_failure_due_to_size() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![0; AVAILABLE_PAGE_SIZE + 1]; // Exceeding available page size
        let value = vec![0; 1];
        let success = page.append(&key, &value);

        assert!(!success);
        assert_eq!(page.slot_count(), 0); // No slots should have been added
    }
}
