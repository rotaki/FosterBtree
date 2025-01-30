use std::cmp::Ordering;

use crate::{
    access_method::AccessMethodError,
    prelude::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

mod header {
    use crate::page::{PageId, AVAILABLE_PAGE_SIZE};
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

    pub struct Header {
        next_page_id: PageId,
        next_frame_id: u32,
        total_bytes_used: u32, // (PAGE_HEADER_SIZE + slots + records)
        slot_count: u32,
        rec_start_offset: u32,
    }

    impl Header {
        pub fn from_bytes(bytes: &[u8; PAGE_HEADER_SIZE]) -> Self {
            let mut current_pos = 0;
            let next_page_id = crate::page::PageId::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<crate::page::PageId>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<crate::page::PageId>();
            let next_frame_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let total_bytes_used = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let slot_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );

            Header {
                next_page_id,
                next_frame_id,
                total_bytes_used,
                slot_count,
                rec_start_offset,
            }
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut bytes = [0; PAGE_HEADER_SIZE];
            let mut current_pos = 0;
            bytes[current_pos..current_pos + std::mem::size_of::<crate::page::PageId>()]
                .copy_from_slice(&self.next_page_id.to_be_bytes());
            current_pos += std::mem::size_of::<crate::page::PageId>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.next_frame_id.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.slot_count.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.rec_start_offset.to_be_bytes());
            bytes
        }

        pub fn new() -> Self {
            Header {
                next_page_id: PageId::MAX,
                next_frame_id: u32::MAX,
                total_bytes_used: PAGE_HEADER_SIZE as u32,
                slot_count: 0,
                rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
            }
        }

        pub fn next_page(&self) -> Option<(PageId, u32)> {
            if self.next_page_id == PageId::MAX {
                None
            } else {
                Some((self.next_page_id, self.next_frame_id))
            }
        }

        pub fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
            self.next_page_id = next_page_id;
            self.next_frame_id = frame_id;
        }

        pub fn next_page_id(&self) -> PageId {
            self.next_page_id
        }

        pub fn set_next_page_id(&mut self, next_page_id: PageId) {
            self.next_page_id = next_page_id;
        }

        pub fn next_frame_id(&self) -> u32 {
            self.next_frame_id
        }

        pub fn set_next_frame_id(&mut self, next_frame_id: u32) {
            self.next_frame_id = next_frame_id;
        }

        pub fn total_bytes_used(&self) -> u32 {
            self.total_bytes_used
        }

        pub fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
            self.total_bytes_used = total_bytes_used;
        }

        pub fn slot_count(&self) -> u32 {
            self.slot_count
        }

        pub fn set_slot_count(&mut self, slot_count: u32) {
            self.slot_count = slot_count;
        }

        pub fn increment_slot_count(&mut self) {
            self.slot_count += 1;
        }

        pub fn decrement_slot_count(&mut self) {
            self.slot_count -= 1;
        }

        pub fn rec_start_offset(&self) -> u32 {
            self.rec_start_offset
        }

        pub fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
            self.rec_start_offset = rec_start_offset;
        }
    }
}
use header::*;

mod slot {
    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq)]
    pub struct Slot {
        key_size: u32,
        key_prefix: [u8; SLOT_KEY_PREFIX_SIZE],
        val_size: u32,
        offset: u32,
        // sts, ets, pid
    }

    impl Slot {
        pub fn from_bytes(bytes: &[u8; SLOT_SIZE]) -> Self {
            let mut current_pos = 0;

            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            key_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();

            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );

            Slot {
                key_size,
                key_prefix,
                val_size,
                offset,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0u8; SLOT_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.key_size.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();

            bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]
                .copy_from_slice(&self.key_prefix);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.val_size.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.offset.to_be_bytes());

            bytes
        }

        pub fn new(key: &[u8], val: &[u8], offset: usize) -> Self {
            let key_size = key.len() as u32;
            let val_size = val.len() as u32;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            let copy_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            key_prefix[..copy_len].copy_from_slice(&key[..copy_len]);

            Slot {
                key_size,
                key_prefix,
                val_size,
                offset: offset as u32,
            }
        }

        pub fn key_size(&self) -> u32 {
            self.key_size
        }

        pub fn key_prefix(&self) -> &[u8] {
            &self.key_prefix
        }

        pub fn val_size(&self) -> u32 {
            self.val_size
        }

        pub fn set_val_size(&mut self, val_size: usize) {
            self.val_size = val_size as u32;
        }

        pub fn offset(&self) -> u32 {
            self.offset
        }

        pub fn set_offset(&mut self, offset: u32) {
            self.offset = offset;
        }
    }
}
use slot::*;

mod record {
    use super::slot::SLOT_KEY_PREFIX_SIZE;
    pub struct Record {
        remain_key: Vec<u8>,
        val: Vec<u8>,
    }

    impl Record {
        pub fn from_bytes(bytes: &[u8], key_size: u32, val_size: u32) -> Self {
            let key_size = key_size as usize;
            let val_size = val_size as usize;

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
            let remain_key = bytes[..remain_key_size].to_vec();
            let val = bytes[remain_key_size..remain_key_size + val_size].to_vec();

            Record { remain_key, val }
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(self.remain_key.len() + self.val.len());

            bytes.extend_from_slice(&self.remain_key);
            bytes.extend_from_slice(&self.val);

            bytes
        }

        pub fn new(key: &[u8], val: &[u8]) -> Self {
            let remain_key = if key.len() > SLOT_KEY_PREFIX_SIZE {
                key[SLOT_KEY_PREFIX_SIZE..].to_vec()
            } else {
                Vec::new()
            };

            Record {
                remain_key,
                val: val.to_vec(),
            }
        }

        pub fn remain_key(&self) -> &[u8] {
            &self.remain_key
        }

        pub fn val(&self) -> &[u8] {
            &self.val
        }

        pub fn update(&mut self, new_val: &[u8]) {
            self.val = new_val.to_vec();
        }

        pub fn update_with_merge(&mut self, new_val: &[u8], merge_fn: fn(&[u8], &[u8]) -> Vec<u8>) {
            self.val = merge_fn(&self.val, new_val);
        }
    }
}
use record::*;

pub trait ReadOptimizedPage {
    fn init(&mut self);
    fn max_record_size() -> usize {
        AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
    }
    fn rec_size(&self, key: &[u8], val: &[u8]) -> usize {
        key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE) + val.len()
    }
    fn size_require(&self, key: &[u8], val: &[u8]) -> usize {
        SLOT_SIZE + self.rec_size(key, val)
    }

    // Header operations
    fn header(&self) -> Header;
    fn set_header(&mut self, header: &Header);

    fn next_page(&self) -> Option<(PageId, u32)>; // (next_page_id, next_frame_id)
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);

    fn total_bytes_used(&self) -> u32;
    fn total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.total_bytes_used()
    }
    fn set_total_bytes_used(&mut self, total_bytes_used: u32);
    fn increase_total_bytes_used(&mut self, bytes: u32) {
        self.set_total_bytes_used(self.total_bytes_used() + bytes);
    }
    fn decrease_total_bytes_used(&mut self, bytes: u32) {
        self.set_total_bytes_used(self.total_bytes_used() - bytes);
    }

    fn slot_count(&self) -> u32;
    fn set_slot_count(&mut self, slot_count: u32);
    fn increment_slot_count(&mut self);
    fn decrement_slot_count(&mut self);

    fn rec_start_offset(&self) -> u32;
    fn set_rec_start_offset(&mut self, rec_start_offset: u32);

    // Helpers
    fn slot_offset(&self, slot_id: u32) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }
    fn slot(&self, slot_id: u32) -> Option<Slot>;
    fn set_slot(&mut self, slot_id: u32, slot: &Slot);

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
    fn get_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, &[u8]);

    /// Get the mutable val at the slot_id.
    /// If the slot_id is invalid, panic.
    /// This function is used for updating the val in place.
    /// Updates of the record should not change the size of the val.
    // fn get_mut_val_with_slot_id(&mut self, slot_id: u32) -> &mut [u8];
    /// Helpers (Jun)
    fn get_key_with_slot_id(&self, slot_id: u32) -> Vec<u8>;
    fn get_value_with_slot_id(&self, slot_id: u32) -> &[u8];
    fn get_value_with_slot(&self, slot: &Slot) -> &[u8];

    fn record(&self, slot_id: u32) -> Record;
    fn search_slot(&self, key: &[u8]) -> (bool, u32) {
        self.binary_search(key)
    }
    fn binary_search(&self, key: &[u8]) -> (bool, u32); // (found, slot_id)
    fn total_free_space_before_compaction(&self) -> u32 {
        self.rec_start_offset() - self.slot_offset(self.slot_count()) as u32
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    fn write_record(&mut self, offset: usize, key: &[u8], val: &[u8]);

    fn insert_slot_at_id(&mut self, slot_id: u32, slot: &Slot);
    fn delete_slot_at_id(&mut self, slot_id: u32);

    /// Insert a new key-value pair into the index.
    /// If the key already exists, it will return an error.
    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError>;
    fn insert_at_slot_id(
        &mut self,
        key: &[u8],
        val: &[u8],
        slot_id: u32,
    ) -> Result<(), AccessMethodError>;

    fn get(&self, key: &[u8]) -> Result<&[u8], AccessMethodError>;

    /// Update the value of an existing key.
    /// If the key does not exist, it will return an error.
    fn update(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError>;
    fn update_at_slot_id(
        &mut self,
        key: &[u8],
        val: &[u8],
        slot_id: u32,
    ) -> Result<(), AccessMethodError>;

    /// Upsert a key-value pair into the index.
    /// If the key already exists, it will update the value.
    /// If the key does not exist, it will insert a new key-value pair.
    fn upsert(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError>;

    /// Upsert with a custom merge function.
    /// If the key already exists, it will update the value with the merge function.
    /// If the key does not exist, it will insert a new key-value pair.
    fn upsert_with_merge<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> Result<(), AccessMethodError>
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>;

    fn compact(&mut self) -> Result<(), AccessMethodError>;
    // compact with moving record of slot_id to the front of the rec_start_offset
    fn compact_update(&mut self, slot_id: u32) -> Result<(), AccessMethodError>;

    fn delete(&mut self, key: &[u8]) -> Result<(), AccessMethodError>;
    fn delete_at_slot_id(&mut self, slot_id: u32) -> Result<(), AccessMethodError>;
}

impl ReadOptimizedPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn header(&self) -> Header {
        let header_bytes: [u8; PAGE_HEADER_SIZE] = self[0..PAGE_HEADER_SIZE].try_into().unwrap();
        Header::from_bytes(&header_bytes)
    }

    fn set_header(&mut self, header: &Header) {
        let header_bytes = header.to_bytes();
        self[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        self.header().next_page()
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        let mut header = self.header();
        header.set_next_page(next_page_id, frame_id);
        self.set_header(&header);
    }

    fn total_bytes_used(&self) -> u32 {
        self.header().total_bytes_used()
    }

    fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
        let mut header = self.header();
        header.set_total_bytes_used(total_bytes_used);
        self.set_header(&header);
    }

    fn slot_count(&self) -> u32 {
        self.header().slot_count()
    }

    fn set_slot_count(&mut self, slot_count: u32) {
        let mut header = self.header();
        header.set_slot_count(slot_count);
        self.set_header(&header);
    }

    fn increment_slot_count(&mut self) {
        let mut header = self.header();
        header.increment_slot_count();
        self.set_header(&header);
    }

    fn decrement_slot_count(&mut self) {
        let mut header = self.header();
        header.decrement_slot_count();
        self.set_header(&header);
    }

    fn rec_start_offset(&self) -> u32 {
        self.header().rec_start_offset()
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
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

    fn set_slot(&mut self, slot_id: u32, slot: &Slot) {
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
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
            let rec_start_offset = self.rec_start_offset() as usize - total_len;
            self[rec_start_offset..rec_start_offset + key.len()].copy_from_slice(key);
            self[rec_start_offset + key.len()..rec_start_offset + total_len].copy_from_slice(value);
            let slot = Slot::new(key, value, rec_start_offset);
            self.append_slot(&slot);

            // Update the total bytes used
            self.set_total_bytes_used(
                self.total_bytes_used() + SLOT_SIZE as u32 + total_len as u32,
            );
            true
        }
    }

    fn get_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, &[u8]) {
        let slot = self.slot(slot_id).expect("Invalid slot_id");

        let offset = slot.offset() as usize;
        let key_prefix = slot.key_prefix();
        let key_size = slot.key_size() as usize;
        let val_size = slot.val_size() as usize;

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_key = &self[offset..offset + remain_key_size];

        let val_offset = offset + remain_key_size;
        let val = &self[val_offset..val_offset + val_size];

        let mut whole_key = Vec::with_capacity(key_size);
        whole_key.extend_from_slice(&key_prefix[..key_size.min(SLOT_KEY_PREFIX_SIZE)]);
        if key_size > SLOT_KEY_PREFIX_SIZE {
            whole_key.extend_from_slice(remain_key);
        }

        (whole_key, val)
    }

    fn get_key_with_slot_id(&self, slot_id: u32) -> Vec<u8> {
        let slot = self.slot(slot_id).expect("Invalid slot_id");

        let key_prefix = slot.key_prefix();
        let key_size = slot.key_size() as usize;

        if key_size <= SLOT_KEY_PREFIX_SIZE {
            return key_prefix[..key_size].to_vec();
        }

        let offset = slot.offset() as usize;

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_key = &self[offset..offset + remain_key_size];

        let mut whole_key = Vec::with_capacity(key_size);
        whole_key.extend_from_slice(&key_prefix[..SLOT_KEY_PREFIX_SIZE]);
        whole_key.extend_from_slice(remain_key);

        whole_key
    }

    fn get_value_with_slot_id(&self, slot_id: u32) -> &[u8] {
        let slot = self.slot(slot_id).expect("Invalid slot_id");
        self.get_value_with_slot(&slot)
    }

    fn get_value_with_slot(&self, slot: &Slot) -> &[u8] {
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let val_size = slot.val_size() as usize;

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let val_offset = offset + remain_key_size;
        &self[val_offset..val_offset + val_size]
    }

    // fn get_mut_val_with_slot_id(&mut self, slot_id: u32) -> &mut [u8] {
    //     let slot = self.slot(slot_id).unwrap();
    //     let offset = slot.offset() as usize;
    //     &mut self[offset + slot.key_size() as usize
    //         ..offset + slot.key_size() as usize + slot.val_size() as usize]
    // }

    fn record(&self, slot_id: u32) -> Record {
        let slot = self.slot(slot_id).expect("Invalid slot_id");

        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let val_size = slot.val_size() as usize;

        let record_bytes =
            &self[offset..offset + (key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE) + val_size)];
        Record::from_bytes(record_bytes, key_size as u32, val_size as u32)
    }

    fn binary_search(&self, key: &[u8]) -> (bool, u32) {
        let mut high = self.slot_count();
        if high == 0 {
            return (false, 0);
        }

        high -= 1;
        let high_key = self.get_key_with_slot_id(high);

        if key > high_key.as_slice() {
            return (false, high + 1);
        } else if key == high_key.as_slice() {
            return (true, high);
        } else if high == 0 {
            return (false, 0);
        }

        let mut low = 0;
        let low_key = self.get_key_with_slot_id(low);

        match key.cmp(low_key.as_slice()) {
            Ordering::Less => return (false, 0),
            Ordering::Equal => return (true, low),
            _ => {}
        }

        while low < high {
            let mid = low + (high - low) / 2;
            let mid_key = self.get_key_with_slot_id(mid);
            match mid_key.as_slice().cmp(key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Greater => {
                    high = mid;
                }
                Ordering::Equal => {
                    return (true, mid);
                }
            }
        }

        (false, low)
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn write_record(&mut self, offset: usize, key: &[u8], val: &[u8]) {
        let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
        if remain_key_size > 0 {
            self.write_bytes(offset, &key[SLOT_KEY_PREFIX_SIZE..]);
        }
        self.write_bytes(offset + remain_key_size, val);
    }

    fn insert_slot_at_id(&mut self, slot_id: u32, slot: &Slot) {
        if slot_id < self.slot_count() {
            let start_offset = self.slot_offset(slot_id);
            let end_offset = self.slot_offset(self.slot_count());
            self.copy_within(start_offset..end_offset, start_offset + SLOT_SIZE);
        }

        self.set_slot(slot_id, slot);
        self.increase_total_bytes_used(SLOT_SIZE as u32);
        self.increment_slot_count();
    }

    fn delete_slot_at_id(&mut self, slot_id: u32) {
        if slot_id < self.slot_count() {
            let start_offset = self.slot_offset(slot_id + 1);
            let end_offset = self.slot_offset(self.slot_count());
            self.copy_within(start_offset..end_offset, start_offset - SLOT_SIZE);
        }
        self.decrement_slot_count();
        self.decrease_total_bytes_used(SLOT_SIZE as u32);
        self.write_bytes(
            self.slot_offset(self.slot_count()),
            [0u8; SLOT_SIZE].as_ref(),
        );
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError> {
        if self.rec_size(key, val) > <Page as ReadOptimizedPage>::max_record_size() {
            return Err(AccessMethodError::RecordTooLarge);
        }

        let (found, slot_id) = self.binary_search(key);
        if found {
            return Err(AccessMethodError::KeyDuplicate);
        }
        self.insert_at_slot_id(key, val, slot_id)
    }

    fn insert_at_slot_id(
        &mut self,
        key: &[u8],
        val: &[u8],
        slot_id: u32,
    ) -> Result<(), AccessMethodError> {
        let bytes_needed = self.size_require(key, val) as u32;

        if bytes_needed > self.total_free_space_before_compaction() {
            if bytes_needed > self.total_free_space() {
                return Err(AccessMethodError::OutOfSpace);
            }
            // need to compact the page
            self.compact()?;
        }

        let rec_size = self.rec_size(key, val);
        let rec_start_offset = self.rec_start_offset() as usize - rec_size;

        self.write_record(rec_start_offset, key, val);

        let new_slot = Slot::new(key, val, rec_start_offset);
        self.insert_slot_at_id(slot_id, &new_slot);

        self.set_rec_start_offset(rec_start_offset as u32);
        self.increase_total_bytes_used(rec_size as u32);

        Ok(())
    }

    fn update(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError> {
        if self.rec_size(key, val) > <Page as ReadOptimizedPage>::max_record_size() {
            return Err(AccessMethodError::RecordTooLarge);
        }

        let (found, slot_id) = self.binary_search(key);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        self.update_at_slot_id(key, val, slot_id)
    }

    fn update_at_slot_id(
        &mut self,
        key: &[u8],
        val: &[u8],
        slot_id: u32,
    ) -> Result<(), AccessMethodError> {
        let key_size = key.len();
        let new_rec_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE) + val.len();

        let mut slot = self.slot(slot_id).expect("Invalid slot_id");
        let old_val = self.get_value_with_slot(&slot);
        let offset = slot.offset() as usize;
        let old_val_size = slot.val_size() as usize;
        let old_rec_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE) + old_val_size;

        if self.total_free_space_before_compaction() < new_rec_size as u32 {
            if (self.total_free_space() as i32) < (new_rec_size as i32) - (old_rec_size as i32) {
                return Err(AccessMethodError::OutOfSpaceForUpdate(old_val.to_vec()));
                // return Err(AccessMethodError::OutOfSpace);
            }
            if new_rec_size > old_rec_size
                && self.total_free_space() != self.total_free_space_before_compaction()
            {
                self.compact_update(slot_id)?;
            }
        }

        let new_rec_offset;
        // Case 1: New value size is smaller or equal (or) Case 2: Offset matches `rec_start_offset`
        if new_rec_size <= old_rec_size || offset == self.rec_start_offset() as usize {
            new_rec_offset = offset + old_rec_size - new_rec_size;
            self.write_record(new_rec_offset, key, val);
            if new_rec_size < old_rec_size {
                self.write_bytes(offset, &vec![0; old_rec_size - new_rec_size]);
            }
            if offset == self.rec_start_offset() as usize {
                self.set_rec_start_offset(new_rec_offset as u32);
            }
        }
        // Case 3: New value is larger and offset doesn't match `rec_start_offset`
        else {
            new_rec_offset = self.rec_start_offset() as usize - new_rec_size;
            self.write_record(new_rec_offset, key, val);
            self.set_rec_start_offset(new_rec_offset as u32);
        }

        slot.set_val_size(val.len());
        slot.set_offset(new_rec_offset as u32);
        self.set_slot(slot_id, &slot);

        self.increase_total_bytes_used(new_rec_size as u32);
        self.decrease_total_bytes_used(old_rec_size as u32);

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<&[u8], AccessMethodError> {
        let (found, slot_id) = self.binary_search(key);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }

        Ok(self.get_value_with_slot_id(slot_id))
    }

    fn upsert(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError> {
        let (found, slot_id) = self.binary_search(key);
        if found {
            self.update_at_slot_id(key, val, slot_id)
        } else {
            self.insert_at_slot_id(key, val, slot_id)
        }
    }

    fn upsert_with_merge<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> Result<(), AccessMethodError>
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>,
    {
        let (found, slot_id) = self.binary_search(key);
        if found {
            let existing_value = self.get_value_with_slot_id(slot_id);
            let merged_value = update_fn(existing_value, value);
            self.update_at_slot_id(key, &merged_value, slot_id)
        } else {
            self.insert_at_slot_id(key, value, slot_id)
        }
    }

    fn compact(&mut self) -> Result<(), AccessMethodError> {
        let mut current_offset = AVAILABLE_PAGE_SIZE;
        let mut records_buffer = Vec::new();

        // Step 1: Collect all records based on their offsets, update slot offsets, and prepare the buffer
        for slot_id in 0..self.slot_count() {
            let mut slot = self.slot(slot_id).expect("Invalid slot_id");

            // Get the size and offset of the record
            let key_size = slot.key_size() as usize;
            let val_size = slot.val_size() as usize;
            let record_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE) + val_size;
            let record_offset = slot.offset() as usize;

            // Extract the record bytes from the current offset
            let record_bytes = &self[record_offset..record_offset + record_size];

            // Prepend the extracted record bytes to the buffer
            records_buffer.splice(0..0, record_bytes.iter().cloned());

            // Update the offset for the current slot to the new compacted position
            current_offset -= record_size;
            slot.set_offset(current_offset as u32);
            self.set_slot(slot_id, &slot);
        }

        // Step 2: Write the records from the buffer back to the page
        self.write_bytes(current_offset, &records_buffer);

        // Step 3: Zero-pad the space between the end of the slots and the start of the records
        let slot_area_end = self.slot_offset(self.slot_count());
        if slot_area_end > current_offset {
            return Err(AccessMethodError::OutOfSpace);
        }
        self.write_bytes(slot_area_end, &vec![0; current_offset - slot_area_end]);

        // Step 4: Update the header's record start offset
        self.set_rec_start_offset(current_offset as u32);

        Ok(())
    }

    fn compact_update(&mut self, target_slot_id: u32) -> Result<(), AccessMethodError> {
        let mut current_offset = AVAILABLE_PAGE_SIZE;
        let mut records_buffer = Vec::new();
        let mut target_slot = None;

        // Step 1: Collect and move all records except the target_slot_id
        for slot_id in 0..self.slot_count() {
            if slot_id == target_slot_id {
                target_slot = self.slot(slot_id); // Clone the target slot to move later
                continue; // Skip this slot for now
            }

            let mut slot = self.slot(slot_id).expect("Invalid slot_id");

            // Get the size and offset of the record
            let key_size = slot.key_size() as usize;
            let val_size = slot.val_size() as usize;
            let record_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE) + val_size;
            let record_offset = slot.offset() as usize;

            // Extract the record bytes from the current offset
            let record_bytes = &self[record_offset..record_offset + record_size];

            // Prepend the extracted record bytes to the buffer
            records_buffer.splice(0..0, record_bytes.iter().cloned());

            // Update the offset for the current slot to the new compacted position
            current_offset -= record_size;
            slot.set_offset(current_offset as u32);
            self.set_slot(slot_id, &slot);
        }

        // Step 2: Move the target_slot_id's record to its new compacted position
        if let Some(mut slot) = target_slot {
            let key_size = slot.key_size() as usize;
            let val_size = slot.val_size() as usize;
            let record_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE) + val_size;
            let record_offset = slot.offset() as usize;

            // Extract the target record bytes from the current offset
            let record_bytes = &self[record_offset..record_offset + record_size];

            // Prepend the extracted record bytes to the buffer
            records_buffer.splice(0..0, record_bytes.iter().cloned());

            // Update the offset for the target slot to the new compacted position
            current_offset -= record_size;
            slot.set_offset(current_offset as u32);
            self.set_slot(target_slot_id, &slot);
        } else {
            return Err(AccessMethodError::KeyNotFound);
        }

        // Step 3: Write the records from the buffer back to the page
        self.write_bytes(current_offset, &records_buffer);

        // Step 4: Zero-pad the space between the end of the slots and the start of the records
        let slot_area_end = self.slot_offset(self.slot_count());
        if slot_area_end > current_offset {
            return Err(AccessMethodError::OutOfSpace);
        }
        self.write_bytes(slot_area_end, &vec![0; current_offset - slot_area_end]);

        // Step 5: Update the header's record start offset
        self.set_rec_start_offset(current_offset as u32);

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), AccessMethodError> {
        let (found, slot_id) = self.binary_search(key);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        self.delete_at_slot_id(slot_id)
    }

    fn delete_at_slot_id(&mut self, slot_id: u32) -> Result<(), AccessMethodError> {
        let slot = self.slot(slot_id).expect("Invalid slot_id");
        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(
                slot.offset()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot.val_size(),
            );
        }
        self.delete_slot_at_id(slot_id);
        self.decrease_total_bytes_used(
            slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32) + slot.val_size(),
        );
        Ok(())
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

        // size 50 key with u8
        let key = vec![1u8; 50];
        let val = vec![2u8; 200];
        let slot = Slot::new(&key, &val, 100);
        page.append_slot(&slot);

        assert_eq!(page.slot_count(), 1);
        assert_eq!(page.slot(0).unwrap().offset(), 100);
        assert_eq!(page.slot(0).unwrap().key_size(), 50);
        assert_eq!(page.slot(0).unwrap().val_size(), 200);
    }

    // #[test]
    // fn test_record_append() {
    //     let mut page = Page::new_empty();
    //     page.init();

    //     let key = vec![1, 2, 3, 4, 5];
    //     let value = vec![6, 7, 8, 9, 10];
    //     let success = page.append(&key, &value);

    //     assert!(success);
    //     assert_eq!(page.get_with_slot_id(0), (key.to_vec(), value.as_slice()));
    //     assert_eq!(page.slot_count(), 1);
    //     assert_eq!(
    //         page.total_bytes_used(),
    //         (PAGE_HEADER_SIZE + SLOT_SIZE + key.len() + value.len()) as u32
    //     );
    // }

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
            page.get_with_slot_id(0); // Should panic because slot_id 0 is invalid without any appends
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_insert_and_get_single_key_value() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let value = vec![6, 7, 8, 9, 10];

        // Insert the key-value pair
        page.insert(&key, &value)
            .expect("Failed to insert key-value pair");

        // Retrieve the value using the key
        let retrieved_value = page.get(&key).expect("Failed to get value");

        // Assert the value matches
        assert_eq!(retrieved_value, value.as_slice());
    }

    #[test]
    fn test_insert_and_get_multiple_key_value_pairs() {
        let mut page = Page::new_empty();
        page.init();

        let key1 = vec![1, 2, 3];
        let value1 = vec![10, 20, 30];
        let key2 = vec![4; 10];
        let value2 = vec![40, 50, 60];
        let key3 = vec![7, 8, 9];
        let value3 = vec![70, 80, 90];

        // Insert multiple key-value pairs
        page.insert(&key1, &value1)
            .expect("Failed to insert key-value pair 1");
        page.insert(&key2, &value2)
            .expect("Failed to insert key-value pair 2");
        page.insert(&key3, &value3)
            .expect("Failed to insert key-value pair 3");

        // Retrieve and assert each value
        assert_eq!(
            page.get(&key1).expect("Failed to get value 1"),
            value1.as_slice()
        );
        assert_eq!(
            page.get(&key2).expect("Failed to get value 2"),
            value2.as_slice()
        );
        assert_eq!(
            page.get(&key3).expect("Failed to get value 3"),
            value3.as_slice()
        );
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3];
        let value1 = vec![10, 20, 30];
        let value2 = vec![40, 50, 60];

        // Insert the key-value pair
        page.insert(&key, &value1)
            .expect("Failed to insert key-value pair 1");

        // Attempt to insert the same key with a different value
        let result = page.insert(&key, &value2);

        // Assert that the insert fails with a KeyExists error
        assert_eq!(result, Err(AccessMethodError::KeyDuplicate));
    }

    #[test]
    fn test_insert_out_of_space() {
        let mut page = Page::new_empty();
        page.init();

        // Create a key-value pair that exceeds the available space
        let key = vec![0; AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE + 1]; // Key too large
        let value = vec![1];

        // Attempt to insert the large key-value pair
        let result = page.insert(&key, &value);

        // Assert that the insert fails with an OutOfSpace error
        assert_eq!(result, Err(AccessMethodError::RecordTooLarge));
    }

    #[test]
    fn test_get_non_existent_key() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3];

        // Attempt to get a value for a non-existent key
        let result = page.get(&key);

        // Assert that the get operation returns a KeyNotFound error
        assert_eq!(result, Err(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_order_preservation() {
        let mut page = Page::new_empty();
        page.init();

        // Insert keys in a random order
        let keys: Vec<&[u8]> = vec![b"delta", b"alpha", b"echo", b"bravo", b"charlie"];
        for key in keys.iter() {
            page.insert(key, b"value")
                .expect("Failed to insert key-value pair");
        }

        // Retrieve keys by slot IDs and check if they are sorted
        let mut retrieved_keys = vec![];
        for i in 0..page.slot_count() {
            let key = page.get_key_with_slot_id(i);
            // Convert the key to a String
            let key_string = String::from_utf8_lossy(&key).to_string();
            retrieved_keys.push(key_string);
        }

        // Assert that the keys are retrieved in the expected order
        assert_eq!(
            retrieved_keys,
            vec!["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }

    #[test]
    fn test_update_with_smaller_value() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let initial_value = vec![10, 20, 30, 40];
        page.insert(&key, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a smaller value
        let new_value = vec![50, 60];
        page.update(&key, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page.get(&key).expect("Failed to get updated value");

        assert_eq!(updated_value, &[50, 60]);
    }

    #[test]
    fn test_update_with_larger_value() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let initial_value = vec![10, 20];
        page.insert(&key, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a larger value
        let new_value = vec![30, 40, 50, 60];
        page.update(&key, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page.get(&key).expect("Failed to get updated value");

        // Assert that the value has been updated correctly
        assert_eq!(updated_value, &[30, 40, 50, 60]);
    }

    #[test]
    fn test_update_with_equal_size_value() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let initial_value = vec![10, 20, 30];
        page.insert(&key, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a value of equal size
        let new_value = vec![40, 50, 60];
        page.update(&key, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page.get(&key).expect("Failed to get updated value");

        // Assert that the value has been updated correctly
        assert_eq!(updated_value, &[40, 50, 60]);
    }

    #[test]
    fn test_update_non_existent_key() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let value = vec![10, 20, 30];
        page.insert(&key, &value)
            .expect("Failed to insert key-value pair");

        // Attempt to update a non-existent key
        let non_existent_key = vec![4, 5, 6];
        let new_value = vec![40, 50, 60];
        let result = page.update(&non_existent_key, &new_value);

        // Assert that the update operation fails with KeyNotFound
        assert_eq!(result, Err(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_update_with_large_value_out_of_space() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let initial_value = vec![10, 20];
        page.insert(&key, &initial_value)
            .expect("Failed to insert key-value pair");

        // Attempt to update with a value that is too large
        let large_value = vec![0; AVAILABLE_PAGE_SIZE]; // Value larger than the remaining space
        let result = page.update(&key, &large_value);

        // Assert that the update operation fails with OutOfSpace
        assert_eq!(result, Err(AccessMethodError::RecordTooLarge));
    }

    #[test]
    fn test_insert_multiple_and_update_multiple() {
        let mut page = Page::new_empty();
        page.init();

        // Insert multiple key-value pairs
        let keys = [
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
            vec![10, 11, 12],
        ];
        let values = [
            vec![10, 20, 30],
            vec![40, 50, 60],
            vec![70, 80, 90],
            vec![100, 110, 120],
        ];

        for (key, value) in keys.iter().zip(values.iter()) {
            page.insert(key, value)
                .expect("Failed to insert key-value pair");
        }

        // Update the values
        let new_values = [
            vec![13, 14, 15],
            vec![16, 17, 18],
            vec![19, 20, 21],
            vec![22, 23, 24],
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            page.update(key, new_value).expect("Failed to update value");
        }

        // Retrieve the updated values and verify correctness
        for (key, expected_value) in keys.iter().zip(new_values.iter()) {
            let retrieved_value = page.get(key).expect("Failed to get updated value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_multiple_update_mixed_sizes_and_get() {
        let mut page = Page::new_empty();
        page.init();

        // Insert multiple key-value pairs
        let keys = [vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];
        let values = [vec![10, 20, 30], vec![40, 50, 60], vec![70, 80, 90]];

        for (key, value) in keys.iter().zip(values.iter()) {
            page.insert(key, value)
                .expect("Failed to insert key-value pair");
        }

        // Update the values with mixed sizes
        let new_values = [
            vec![13, 14],     // Smaller
            vec![16, 17, 18], // Same size
            vec![19, 20, 21, 22, 23],
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            page.update(key, new_value).expect("Failed to update value");
        }

        // Retrieve the updated values and verify correctness
        let expected_values = [
            vec![13, 14],     // Padded with zeros
            vec![16, 17, 18], // Same size
            vec![19, 20, 21, 22, 23],
        ];

        for (key, expected_value) in keys.iter().zip(expected_values.iter()) {
            let retrieved_value = page.get(key).expect("Failed to get updated value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_update_and_get_mixed() {
        let mut page = Page::new_empty();
        page.init();

        // Insert initial key-value pairs
        let initial_keys = [vec![1, 1, 1], vec![2, 2, 2]];
        let initial_values = [vec![11, 12, 13], vec![21, 22, 23]];

        for (key, value) in initial_keys.iter().zip(initial_values.iter()) {
            page.insert(key, value)
                .expect("Failed to insert key-value pair");
        }

        // Update existing and insert new key-value pairs
        let keys_to_update = [vec![1, 1, 1], vec![2, 2, 2]];
        let new_values = [vec![14, 15, 16], vec![24, 25]];

        for (key, new_value) in keys_to_update.iter().zip(new_values.iter()) {
            page.update(key, new_value).expect("Failed to update value");
        }

        // Insert a new key-value pair
        let new_key = vec![3, 3, 3];
        let new_value = vec![31, 32, 33];
        page.insert(&new_key, &new_value)
            .expect("Failed to insert new key-value pair");

        // Verify updates and new insertions
        let retrieved_values = [
            vec![14, 15, 16], // Updated
            vec![24, 25],     // Updated with padding
            vec![31, 32, 33],
        ];

        let all_keys = [vec![1, 1, 1], vec![2, 2, 2], vec![3, 3, 3]];

        for (key, expected_value) in all_keys.iter().zip(retrieved_values.iter()) {
            let retrieved_value = page.get(key).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_and_get_large_keys() {
        let mut page = Page::new_empty();
        page.init();

        // Key sizes larger than SLOT_KEY_PREFIX_SIZE (8 bytes)
        let keys = [
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],              // 10 bytes
            vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21], // 11 bytes
            vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32],
        ];
        let values = [
            vec![101, 102, 103],
            vec![201, 202, 203],
            vec![251, 252, 253],
        ];

        // Insert key-value pairs with large keys
        for (key, value) in keys.iter().zip(values.iter()) {
            page.insert(key, value)
                .expect("Failed to insert key-value pair");
        }

        // Retrieve the values using the large keys and verify correctness
        for (key, expected_value) in keys.iter().zip(values.iter()) {
            let retrieved_value = page.get(key).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_update_large_keys() {
        let mut page = Page::new_empty();
        page.init();

        // Key sizes larger than SLOT_KEY_PREFIX_SIZE (8 bytes)
        let key = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 10 bytes
        let initial_value = vec![101, 102, 103];

        // Insert a key-value pair
        page.insert(&key, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a larger value
        let new_value = vec![104, 105, 106, 107, 108];
        page.update(&key, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value and verify correctness
        let retrieved_value = page.get(&key).expect("Failed to get updated value");
        assert_eq!(retrieved_value, new_value.as_slice());

        // Update the value with a smaller value
        let smaller_value = vec![109, 110];
        page.update(&key, &smaller_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let retrieved_value = page.get(&key).expect("Failed to get updated value");
        assert_eq!(retrieved_value, smaller_value.as_slice());
    }

    #[test]
    fn test_insert_update_and_get_with_large_keys() {
        let mut page = Page::new_empty();
        page.init();

        // Insert multiple key-value pairs with keys larger than 8 bytes
        let keys = [
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],              // 9 bytes
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19], // 10 bytes
            vec![20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        ];
        let initial_values = [
            vec![101, 102],
            vec![201, 202, 203],
            vec![251, 252, 253, 254],
        ];

        for (key, value) in keys.iter().zip(initial_values.iter()) {
            page.insert(key, value)
                .expect("Failed to insert key-value pair");
        }

        // Update some of the values with new data
        let new_values = [
            vec![103, 104, 105], // Update with a larger value
            vec![206],           // Update with a smaller value
            vec![208, 209],
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            page.update(key, new_value).expect("Failed to update value");
        }

        // Retrieve and verify all values
        let expected_values = [
            vec![103, 104, 105], // Updated with a larger value
            vec![206],           // Updated with a smaller value, no padding
            vec![208, 209],
        ];

        for (key, expected_value) in keys.iter().zip(expected_values.iter()) {
            let retrieved_value = page.get(key).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_upsert_insert() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3];
        let value = vec![10, 20, 30];

        // First upsert should insert the key-value pair
        page.upsert(&key, &value).expect("Failed to upsert");

        // Retrieve and verify the value
        let retrieved_value = page.get(&key).expect("Failed to get value");
        assert_eq!(retrieved_value, value.as_slice());
    }

    #[test]
    fn test_upsert_update() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1, 2, 3];
        let value = vec![10, 20, 30];
        let new_value = vec![40, 50, 60];

        // Insert the key-value pair
        page.upsert(&key, &value).expect("Failed to upsert");

        // Upsert with the same key but different value should update the value
        page.upsert(&key, &new_value).expect("Failed to upsert");

        // Retrieve and verify the updated value
        let retrieved_value = page.get(&key).expect("Failed to get value");
        assert_eq!(retrieved_value, new_value.as_slice());
    }

    #[test]
    fn test_upsert_with_merge() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1; 20];
        let value = vec![10, 20, 30];
        let new_value = vec![5, 5, 5];

        // Insert the key-value pair
        page.upsert(&key, &value).expect("Failed to upsert");

        // Merge function adds corresponding elements
        let merge_fn = |old_val: &[u8], new_val: &[u8]| {
            old_val
                .iter()
                .zip(new_val.iter())
                .map(|(o, n)| o + n)
                .collect()
        };

        // Upsert with merge
        page.upsert_with_merge(&key, &new_value, merge_fn)
            .expect("Failed to upsert with merge");

        // The merged value should be [15, 25, 35]
        let expected_value = vec![15, 25, 35];

        // Retrieve and verify the merged value
        let retrieved_value = page.get(&key).expect("Failed to get value");
        assert_eq!(retrieved_value, expected_value.as_slice());
    }

    #[test]
    fn test_upsert_with_merge_insert() {
        let mut page = Page::new_empty();
        page.init();

        let key = vec![1; 20];
        let value = vec![10, 20, 30];

        // Upsert with merge should insert if the key does not exist
        let merge_fn = |_: &[u8], new_val: &[u8]| new_val.to_vec(); // Just returns the new value
        page.upsert_with_merge(&key, &value, merge_fn)
            .expect("Failed to upsert with merge");

        // Retrieve and verify the inserted value
        let retrieved_value = page.get(&key).expect("Failed to get value");
        assert_eq!(retrieved_value, value.as_slice());
    }

    #[test]
    fn test_upsert_with_merge_concat_and_key_size_increase() {
        let mut page = Page::new_empty();
        page.init();

        // Initial key and value
        let key = vec![1, 2, 3, 4, 5, 6, 7, 8, 9]; // 9 bytes, larger than SLOT_KEY_PREFIX_SIZE
        let initial_value = vec![10, 20, 30];

        // Insert the key-value pair
        page.upsert(&key, &initial_value)
            .expect("Failed to upsert initial key-value pair");

        // New value to merge with the old one
        let new_value = vec![40, 50, 60];

        // Merge function concatenates the old and new values
        let merge_fn = |old_val: &[u8], new_val: &[u8]| {
            let mut combined = Vec::with_capacity(old_val.len() + new_val.len());
            combined.extend_from_slice(old_val);
            combined.extend_from_slice(new_val);
            combined
        };

        // Upsert with merge
        page.upsert_with_merge(&key, &new_value, merge_fn)
            .expect("Failed to upsert with merge");

        // The expected merged value should be the concatenation of the initial and new values
        let expected_value = vec![10, 20, 30, 40, 50, 60];

        // Retrieve the updated value
        let retrieved_value = page.get(&key).expect("Failed to get merged value");

        // Verify that the value is the concatenation of the initial and new values
        assert_eq!(retrieved_value, expected_value.as_slice());
    }

    #[test]
    fn test_insert_until_full_and_compact_update() {
        let mut page = Page::new_empty();
        page.init();

        // Step 1: Insert key-value pairs until the page is full
        let mut keys = vec![];
        let mut values = vec![];
        let mut i = 0;
        while page.total_free_space() > 0 {
            let key = format!("key_long_{}", i).into_bytes(); // Key length > 8 bytes
            let value = vec![i as u8; 50]; // Arbitrary value size of 50 bytes
            let result = page.insert(&key, &value);
            if result.is_err() {
                break;
            }
            keys.push(key.clone());
            values.push(value.clone());
            i += 1;
        }
        let inserted_count = keys.len();
        assert!(
            inserted_count > 0,
            "No keys were inserted before the page filled up."
        );

        // Step 2: Update all keys with very small values
        let small_value = vec![1]; // Small value (e.g., 1 byte)
        for key in &keys {
            page.update(key, &small_value)
                .expect("Failed to update key with small value");
        }

        // Step 3: Attempt to insert or update more key-value pairs
        let additional_key = b"key_additional".to_vec();
        let additional_value = vec![255; 100]; // Larger value to force compaction
        let insert_result = page.insert(&additional_key, &additional_value);

        if insert_result.is_err() {
            // If insertion fails, let's perform compaction and retry the insertion
            page.compact().expect("Failed to compact the page");
            page.insert(&additional_key, &additional_value)
                .expect("Failed to insert after compaction");
        }

        // Step 4: Verify that all keys are still present and correct
        for (key, _original_value) in keys.iter().zip(values.iter()) {
            assert_eq!(
                page.get(key).unwrap(),
                small_value.as_slice(),
                "Key's value wasn't updated correctly"
            );
        }

        // Verify that the additional key was inserted correctly
        assert_eq!(
            page.get(&additional_key).unwrap(),
            additional_value.as_slice(),
            "Additional key's value wasn't inserted correctly"
        );
    }
}
