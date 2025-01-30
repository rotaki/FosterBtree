// Append only page

// Page layout:
// 4 byte: next page id
// 4 byte: next frame id
// 4 byte: total bytes used (PAGE_HEADER_SIZE + slots + records)
// 4 byte: slot count
// 4 byte: free space offset
const PAGE_HEADER_SIZE: usize = 4 * 5;

use crate::prelude::{Page, PageId, AVAILABLE_PAGE_SIZE};

mod slot {
    pub const SLOT_SIZE: usize = std::mem::size_of::<u32>() * 3;

    pub struct Slot {
        offset: u32,
        key_size: u32,
        val_size: u32,
    }

    impl Slot {
        pub fn from_bytes(bytes: &[u8; SLOT_SIZE]) -> Self {
            let mut current_pos = 0;
            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += 4;
            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += 4;
            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );

            Slot {
                offset,
                key_size,
                val_size,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0; SLOT_SIZE];
            let mut current_pos = 0;
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.offset.to_be_bytes());
            current_pos += 4;
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.key_size.to_be_bytes());
            current_pos += 4;
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.val_size.to_be_bytes());
            bytes
        }

        pub fn new(offset: u32, key_size: u32, val_size: u32) -> Self {
            Slot {
                offset,
                key_size,
                val_size,
            }
        }

        pub fn offset(&self) -> u32 {
            self.offset
        }

        pub fn key_size(&self) -> u32 {
            self.key_size
        }

        pub fn val_size(&self) -> u32 {
            self.val_size
        }
    }
}
use slot::*;

pub trait AppendOnlyPage {
    fn init(&mut self);
    fn max_record_size() -> usize {
        AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
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

    fn rec_start_offset(&self) -> u32;
    fn set_rec_start_offset(&mut self, rec_start_offset: u32);

    // Helpers
    fn slot_offset(&self, slot_id: u32) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }
    fn slot(&self, slot_id: u32) -> Option<Slot>;

    // Append a slot at the end of the slots.
    // Increment the slot count.
    // The rec_start_offset is also updated.
    // Only call this function when there is enough space for the slot and record.
    fn append_slot(&mut self, slot: &Slot);

    /// Try to append a key value pair to the page.
    /// If the key value is too large to fit in the page, return false.
    /// When false is returned, the page is not modified.
    /// Otherwise, the key value is appended to the page and the page is modified.
    fn append(&mut self, key: &[u8], value: &[u8]) -> bool;

    /// Get the record at the slot_id.
    /// If the slot_id is invalid, panic.
    fn get(&self, slot_id: u32) -> (&[u8], &[u8]);

    /// Get the mutable val at the slot_id.
    /// If the slot_id is invalid, panic.
    /// This function is used for updating the val in place.
    /// Updates of the record should not change the size of the val.
    fn get_mut_val(&mut self, slot_id: u32) -> &mut [u8];
}

impl AppendOnlyPage for Page {
    fn init(&mut self) {
        let next_page_id = PageId::MAX;
        let next_frame_id = u32::MAX;
        let total_bytes_used = PAGE_HEADER_SIZE as u32;
        let slot_count = 0;
        let rec_start_offset = AVAILABLE_PAGE_SIZE as u32;

        self.set_next_page(next_page_id, next_frame_id);
        self.set_total_bytes_used(total_bytes_used);
        self.set_slot_count(slot_count);
        self.set_rec_start_offset(rec_start_offset);
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

    fn rec_start_offset(&self) -> u32 {
        let offset = 16;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        let offset = 16;
        self[offset..offset + std::mem::size_of::<u32>()]
            .copy_from_slice(&rec_start_offset.to_be_bytes());
    }

    fn slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id < self.slot_count() {
            let offset = self.slot_offset(slot_id);
            let slot_bytes: [u8; SLOT_SIZE] = self[offset..offset + SLOT_SIZE].try_into().unwrap();
            Some(Slot::from_bytes(&slot_bytes))
        } else {
            None
        }
    }

    fn append_slot(&mut self, slot: &Slot) {
        let slot_id = self.slot_count();
        self.increment_slot_count();

        // Update the slot
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update the header
        let offset = self.rec_start_offset().min(slot.offset());
        self.set_rec_start_offset(offset);
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> bool {
        let total_len = key.len() + value.len();
        // Check if the page has enough space for slot and the record
        if self.total_free_space() < SLOT_SIZE as u32 + total_len as u32 {
            false
        } else {
            // Append the slot and the record
            let rec_start_offset = self.rec_start_offset() - total_len as u32;
            self[rec_start_offset as usize..rec_start_offset as usize + key.len()]
                .copy_from_slice(key);
            self[rec_start_offset as usize + key.len()..rec_start_offset as usize + total_len]
                .copy_from_slice(value);
            let slot = Slot::new(rec_start_offset, key.len() as u32, value.len() as u32);
            self.append_slot(&slot);

            // Update the total bytes used
            self.set_total_bytes_used(
                self.total_bytes_used() + SLOT_SIZE as u32 + total_len as u32,
            );
            true
        }
    }

    fn get(&self, slot_id: u32) -> (&[u8], &[u8]) {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key = &self[offset..offset + slot.key_size() as usize];
        let value = &self[offset + slot.key_size() as usize
            ..offset + slot.key_size() as usize + slot.val_size() as usize];
        (key, value)
    }

    fn get_mut_val(&mut self, slot_id: u32) -> &mut [u8] {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        &mut self[offset + slot.key_size() as usize
            ..offset + slot.key_size() as usize + slot.val_size() as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_initialization() {
        let mut page = Page::new_empty();
        page.init();

        assert_eq!(page.total_bytes_used(), PAGE_HEADER_SIZE as u32);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(
            page.total_free_space(),
            (AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE) as u32
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
    fn test_slot_handling() {
        let mut page = Page::new_empty();
        page.init();

        let slot = Slot::new(100, 50, 200);
        page.append_slot(&slot);

        assert_eq!(page.slot_count(), 1);
        assert_eq!(page.slot(0).unwrap().offset(), 100);
        assert_eq!(page.slot(0).unwrap().key_size(), 50);
        assert_eq!(page.slot(0).unwrap().val_size(), 200);
    }

    #[test]
    fn test_record_append() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3, 4, 5];
        let value = vec![6, 7, 8, 9, 10];
        let success = page.append(&key, &value);

        assert!(success);
        assert_eq!(page.get(0), (key.as_slice(), value.as_slice()));
        assert_eq!(page.slot_count(), 1);
        assert_eq!(
            page.total_bytes_used(),
            (PAGE_HEADER_SIZE + SLOT_SIZE + key.len() + value.len()) as u32
        );
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

    #[test]
    fn test_get_invalid_slot() {
        let page = Page::new_empty();
        let result = std::panic::catch_unwind(|| {
            page.get(0); // Should panic because slot_id 0 is invalid without any appends
        });

        assert!(result.is_err());
    }
}
