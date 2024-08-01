/*
Currently not maintained. Don't use heap page.
 */

#[allow(unused_imports)]
use crate::log;
use crate::log_trace;
use crate::page::Page;

mod page_header {
    pub const PAGE_HEADER_SIZE: usize = 4;
    pub struct PageHeader {
        active_slot_count: u16, // Monotonically increasing counter. Slot deletion does not decrement this counter.
        rec_start_offset: u16,
    }

    impl PageHeader {
        pub fn from_bytes(data: [u8; PAGE_HEADER_SIZE]) -> Self {
            Self {
                active_slot_count: u16::from_be_bytes(data[..2].try_into().unwrap()),
                rec_start_offset: u16::from_be_bytes(data[2..4].try_into().unwrap()),
            }
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut buf = [0; PAGE_HEADER_SIZE];
            buf[..2].copy_from_slice(&self.active_slot_count.to_be_bytes());
            buf[2..4].copy_from_slice(&self.rec_start_offset.to_be_bytes());
            buf
        }

        pub fn new(rec_start_offset: u16) -> Self {
            Self {
                active_slot_count: 0,
                rec_start_offset,
            }
        }

        pub fn active_slot_count(&self) -> u16 {
            self.active_slot_count
        }

        pub fn increment_active_slots(&mut self) {
            self.active_slot_count += 1;
        }

        pub fn rec_start_offset(&self) -> u16 {
            self.rec_start_offset
        }

        pub fn set_rec_start_offset(&mut self, offset: u16) {
            self.rec_start_offset = offset;
        }
    }
}

mod slot {
    pub const SLOT_SIZE: usize = 4;

    pub struct Slot {
        offset: u16, // First bit is used to indicate if the slot is valid
        size: u16,
    }

    impl Slot {
        pub fn from_bytes(data: [u8; SLOT_SIZE]) -> Self {
            Self {
                offset: u16::from_be_bytes(data[..2].try_into().unwrap()),
                size: u16::from_be_bytes(data[2..4].try_into().unwrap()),
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut buf = [0; SLOT_SIZE];
            buf[..2].copy_from_slice(&self.offset.to_be_bytes());
            buf[2..4].copy_from_slice(&self.size.to_be_bytes());
            buf
        }

        pub fn new(offset: u16, size: u16) -> Self {
            Self { offset, size }
        }

        pub fn is_valid(&self) -> bool {
            self.offset & 0b1000_0000_0000_0000 == 0
        }

        pub fn set_valid(&mut self, valid: bool) {
            if valid {
                self.offset &= 0b0111_1111_1111_1111;
            } else {
                self.offset |= 0b1000_0000_0000_0000;
            }
        }

        pub fn offset(&self) -> u16 {
            self.offset & 0b0111_1111_1111_1111
        }

        pub fn set_offset(&mut self, offset: u16) {
            self.offset = (self.offset & 0b1000_0000_0000_0000) | (offset & 0b0111_1111_1111_1111);
        }

        #[allow(dead_code)]
        pub fn size(&self) -> u16 {
            self.size
        }

        pub fn set_size(&mut self, size: u16) {
            self.size = size;
        }
    }
}

use page_header::{PageHeader, PAGE_HEADER_SIZE};
use slot::{Slot, SLOT_SIZE};

pub struct HeapPage<'a> {
    page: &'a mut Page,
}

impl<'a> HeapPage<'a> {
    fn header(&self) -> PageHeader {
        let header_bytes: [u8; PAGE_HEADER_SIZE] =
            self.page[0..PAGE_HEADER_SIZE].try_into().unwrap();
        PageHeader::from_bytes(header_bytes)
    }

    fn update_header(&mut self, page_header: PageHeader) {
        self.page[0..PAGE_HEADER_SIZE].copy_from_slice(&page_header.to_bytes());
    }

    fn slot_offset(slot_id: u16) -> usize {
        PAGE_HEADER_SIZE + SLOT_SIZE * slot_id as usize
    }

    fn slot(&self, slot_id: u16) -> Option<Slot> {
        if slot_id < self.header().active_slot_count() {
            let offset = HeapPage::slot_offset(slot_id);
            let slot_bytes: [u8; SLOT_SIZE] =
                self.page[offset..offset + SLOT_SIZE].try_into().unwrap();
            Some(Slot::from_bytes(slot_bytes))
        } else {
            None
        }
    }

    fn insert_slot(&mut self, size: usize) -> (u16, Slot) {
        let mut page_header = self.header();
        let slot_id = page_header.active_slot_count();
        page_header.increment_active_slots();
        let slot_offset = page_header.rec_start_offset() - size as u16;
        page_header.set_rec_start_offset(slot_offset);
        self.update_header(page_header);

        let slot = Slot::new(slot_offset, size as u16);
        let res = self.update_slot(slot_id, &slot);
        assert!(res);
        (slot_id, slot)
    }

    fn update_slot(&mut self, slot_id: u16, slot: &Slot) -> bool {
        if slot_id < self.header().active_slot_count() {
            let offset = HeapPage::slot_offset(slot_id);
            self.page[offset..offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
            true
        } else {
            false
        }
    }

    // Returns the first invalid slot
    fn invalid_slot(&self) -> Option<u16> {
        for slot_id in 0..self.header().active_slot_count() {
            if let Some(slot) = self.slot(slot_id) {
                if !slot.is_valid() {
                    return Some(slot_id);
                }
            }
        }
        None
    }

    fn free_space(&self) -> usize {
        let next_slot_offset = HeapPage::slot_offset(self.header().active_slot_count());
        let rec_start_offset = self.header().rec_start_offset();
        println!(
            "next_slot_offset: {}, rec_start_offset: {}",
            next_slot_offset, rec_start_offset
        );
        rec_start_offset as usize - next_slot_offset
    }

    // [ [rec4 ][rec3 ][rec2 ][rec1 ] ]
    //   ^             ^     ^
    //   |             |     |
    //   rec_start_offset
    //                 |     |
    //                 shift_start_offset
    //                       |
    //                  <----> shift_size
    //    <------------> recs to be shifted
    //
    // Delete slot2. Shift [rec4 ][rec3 ] to the right by rec2.size
    //
    // [        [rec4 ][rec3 ][rec1 ] ]
    //
    // The left offset of slot4 is `rec_start_offset`.
    // The left offset of slot2 is `shift_start_offset`.
    // The size of slot2 is `shift_size`.
    fn shift_recs(&mut self, shift_start_offset: u16, shift_size: u16) {
        // Chunks of recs to be shifted is in the range of [start..end)
        // The new range is [new_start..new_end)
        log_trace!(
            "Shifting recs. Start offset: {}, size: {}",
            shift_start_offset,
            shift_size
        );
        let start = self.header().rec_start_offset() as usize;
        let end = shift_start_offset as usize;
        // No need to shift if start >= end OR shift_size == 0
        if start >= end || shift_size == 0 {
            return;
        }
        // Use copy_within to shift the recs
        let new_start = start + shift_size as usize;
        self.page.copy_within(start..end, new_start);

        // For each valid slot shifted, update the slot
        for slot_id in 0..self.header().active_slot_count() {
            if let Some(mut slot) = self.slot(slot_id) {
                let current_offset = slot.offset();
                if current_offset < shift_start_offset && slot.is_valid() {
                    // Update slot
                    let new_offset = current_offset + shift_size;
                    slot.set_offset(new_offset);
                    self.update_slot(slot_id, &slot);
                }
            } else {
                panic!("Slot should be available");
            }
        }

        // Update the rec_start_offset of the page
        let mut page_header = self.header();
        page_header.set_rec_start_offset(new_start as u16);
        self.update_header(page_header);
    }
}

impl<'a> HeapPage<'a> {
    pub fn init(page: &mut Page) {
        log_trace!("[Init] Heap page");
        let header = PageHeader::new(page.len() as u16);
        page[0..PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn new(page: &'a mut Page) -> Self {
        HeapPage { page }
    }

    pub fn add_value(&mut self, bytes: &[u8]) -> Option<u16> {
        let slot_size = bytes.len();
        if let Some(slot_id) = self.invalid_slot() {
            log_trace!("[Add] Reusing slot {}", slot_id);
            if slot_size > self.free_space() {
                log_trace!("[Add] Not enough space for slot {}", slot_id);
                return None;
            }

            // Copy the bytes into the address before the rec_start_offset
            let slot_offset = self.header().rec_start_offset() as usize - slot_size;
            self.page[slot_offset..slot_offset + slot_size].copy_from_slice(bytes);

            // Update slot
            let mut slot = self.slot(slot_id).unwrap();
            slot.set_offset(slot_offset as u16);
            slot.set_size(slot_size as u16);
            slot.set_valid(true);
            self.update_slot(slot_id, &slot);

            // Update header
            let mut page_header = self.header();
            page_header.set_rec_start_offset(slot_offset as u16);
            self.update_header(page_header);

            Some(slot_id)
        } else {
            if SLOT_SIZE + slot_size > self.free_space() {
                log_trace!("[Add] Not enough space for new slot");
                return None;
            }
            let (slot_id, slot) = self.insert_slot(slot_size);
            log_trace!(
                "[Add] New slot {}, offset: {}, size: {}",
                slot_id,
                slot.offset(),
                slot.size()
            );
            let offset = slot.offset() as usize;
            self.page[offset..offset + slot_size].copy_from_slice(bytes);
            Some(slot_id)
        }
    }

    #[allow(dead_code)]
    pub fn get_value(&self, slot_id: u16) -> Option<&[u8]> {
        if let Some(slot) = self.slot(slot_id) {
            if slot.is_valid() {
                log_trace!(
                    "[Get] Slot {}, offset: {}, size: {}",
                    slot_id,
                    slot.offset(),
                    slot.size()
                );
                let offset = slot.offset() as usize;
                let size = slot.size() as usize;
                Some(&self.page[offset..offset + size])
            } else {
                None
            }
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn delete_value(&mut self, slot_id: u16) -> Option<()> {
        if let Some(mut slot) = self.slot(slot_id) {
            if slot.is_valid() {
                log_trace!(
                    "[Delete] Slot {}, offset: {}, size: {}",
                    slot_id,
                    slot.offset(),
                    slot.size()
                );
                slot.set_valid(false);
                self.update_slot(slot_id, &slot);
                let shift_start_offset = slot.offset();
                let shift_size = slot.size();
                self.shift_recs(shift_start_offset, shift_size);
                Some(())
            } else {
                None
            }
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn get_all_valid_records(&self) -> Vec<(u16, &[u8])> {
        let mut records = vec![];
        for slot_id in 0..self.header().active_slot_count() {
            if let Some(rec) = self.get_value(slot_id) {
                records.push((slot_id, rec));
            }
        }
        records
    }
}

#[cfg(any(test, debug_assertions))]
impl HeapPage<'_> {
    #[allow(dead_code)]
    pub fn check_slot_offset_and_rec_start_offset(&self) {
        let slot_offset = HeapPage::slot_offset(self.header().active_slot_count());
        let rec_start_offset = self.header().rec_start_offset();
        assert!(slot_offset <= rec_start_offset as usize);
    }
}

#[cfg(test)]
mod tests {
    use crate::page::AVAILABLE_PAGE_SIZE;

    use super::*;

    #[test]
    #[ignore]
    fn test_heap_page_simple() {
        let mut page = Page::new_empty();
        HeapPage::init(&mut page);
        let mut heap_page = HeapPage::new(&mut page);

        let value1 = [1, 2, 3, 4];
        let value2 = [5, 6, 7, 8, 9, 10];
        let value3 = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
        let value4 = [
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        ];

        let slot_id1 = heap_page.add_value(&value1).unwrap();
        let slot_id2 = heap_page.add_value(&value2).unwrap();
        let slot_id3 = heap_page.add_value(&value3).unwrap();
        let slot_id4 = heap_page.add_value(&value4).unwrap();

        assert_eq!(heap_page.get_value(slot_id1).unwrap(), value1);
        assert_eq!(heap_page.get_value(slot_id2).unwrap(), value2);
        assert_eq!(heap_page.get_value(slot_id3).unwrap(), value3);
        assert_eq!(heap_page.get_value(slot_id4).unwrap(), value4);

        heap_page.delete_value(slot_id2).unwrap();
        assert_eq!(heap_page.get_value(slot_id2), None);

        let value5 = [41, 42, 43, 44, 45, 46, 47, 48, 49, 50];
        let slot_id5 = heap_page.add_value(&value5).unwrap();
        assert_eq!(heap_page.get_value(slot_id5).unwrap(), value5);
    }

    #[test]
    #[ignore]
    fn test_heap_page_reuse_slot() {
        let mut page = Page::new_empty();
        HeapPage::init(&mut page);
        let mut heap_page = HeapPage::new(&mut page);

        let value1 = [1, 2, 3, 4];
        let value2 = [5, 6, 7, 8, 9, 10];
        let value3 = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
        let value4 = [
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        ];

        let slot_id1 = heap_page.add_value(&value1).unwrap();
        let slot_id2 = heap_page.add_value(&value2).unwrap();
        let slot_id3 = heap_page.add_value(&value3).unwrap();
        let slot_id4 = heap_page.add_value(&value4).unwrap();

        assert_eq!(heap_page.get_value(slot_id1).unwrap(), value1);
        assert_eq!(heap_page.get_value(slot_id2).unwrap(), value2);
        assert_eq!(heap_page.get_value(slot_id3).unwrap(), value3);
        assert_eq!(heap_page.get_value(slot_id4).unwrap(), value4);

        heap_page.delete_value(slot_id2).unwrap();
        assert_eq!(heap_page.get_value(slot_id2), None);

        let value5 = [41, 42, 43, 44, 45, 46, 47, 48, 49, 50];
        let slot_id5 = heap_page.add_value(&value5).unwrap();
        assert_eq!(slot_id5, slot_id2);
        assert_eq!(heap_page.get_value(slot_id5).unwrap(), value5);

        heap_page.delete_value(slot_id3).unwrap();
        assert_eq!(heap_page.get_value(slot_id3), None);

        let value6 = [51, 52, 53, 54, 55, 56, 57, 58, 59, 60];
        let slot_id6 = heap_page.add_value(&value6).unwrap();
        assert_eq!(slot_id6, slot_id3);
        assert_eq!(heap_page.get_value(slot_id6).unwrap(), value6);
    }

    #[test]
    #[ignore]
    fn test_heap_page_no_space() {
        let mut page = Page::new_empty();
        HeapPage::init(&mut page);
        let mut heap_page = HeapPage::new(&mut page);

        let value1 = [1; AVAILABLE_PAGE_SIZE / 4];
        let value2 = [2; AVAILABLE_PAGE_SIZE / 4];
        let value3 = [3; AVAILABLE_PAGE_SIZE / 4];

        let slot_id1 = heap_page.add_value(&value1).unwrap();
        let slot_id2 = heap_page.add_value(&value2).unwrap();
        let slot_id3 = heap_page.add_value(&value3).unwrap();

        assert_eq!(heap_page.get_value(slot_id1).unwrap(), value1);
        assert_eq!(heap_page.get_value(slot_id2).unwrap(), value2);
        assert_eq!(heap_page.get_value(slot_id3).unwrap(), value3);

        let value4 = [4; AVAILABLE_PAGE_SIZE / 4];
        assert_eq!(heap_page.add_value(&value4), None);

        heap_page.delete_value(slot_id2).unwrap();
        assert_eq!(heap_page.get_value(slot_id2), None);

        let slot_id4 = heap_page.add_value(&value4).unwrap();
        assert_eq!(slot_id4, slot_id2);
        assert_eq!(heap_page.get_value(slot_id4).unwrap(), value4);
    }
}
