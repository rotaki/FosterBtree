use crate::{
    access_method::AccessMethodError, log_debug, prelude::{Page, PageId, AVAILABLE_PAGE_SIZE}
};
use super::Timestamp;

mod header {
    use crate::page::{PageId, AVAILABLE_PAGE_SIZE};
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

    #[derive(Debug)]
    pub struct Header {
        next_page_id: PageId,
        next_frame_id: u32,
        total_bytes_used: u32, // only valid slots and records.
        slot_count: u32,
        rec_start_offset: u32,
    }

    impl Header {
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < PAGE_HEADER_SIZE {
                return Err("Insufficient bytes to form Header".into());
            }

            let mut current_pos = 0;

            let next_page_id = PageId::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<PageId>()].try_into().map_err(|_| "Failed to parse next_page_id")?,
            );
            current_pos += std::mem::size_of::<PageId>();

            let next_frame_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse next_frame_id")?,
            );
            current_pos += 4;

            let total_bytes_used = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse total_bytes_used")?,
            );
            current_pos += 4;

            let slot_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse slot_count")?,
            );
            current_pos += 4;

            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse rec_start_offset")?,
            );

            Ok(Header {
                next_page_id,
                next_frame_id,
                total_bytes_used,
                slot_count,
                rec_start_offset,
            })
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut bytes = [0; PAGE_HEADER_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + std::mem::size_of::<PageId>()].copy_from_slice(&self.next_page_id.to_be_bytes());
            current_pos += std::mem::size_of::<PageId>();

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.next_frame_id.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.slot_count.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.rec_start_offset.to_be_bytes());

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
    use super::Timestamp;
    use std::convert::TryInto;

    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq)]
    pub struct Slot {
        // hash key for join
        key_size: u32,
        key_prefix: [u8; SLOT_KEY_PREFIX_SIZE],
        // primary key for row
        pkey_size: u32,
        pkey_prefix: [u8; SLOT_PKEY_PREFIX_SIZE],
        // timestamp
        ts: Timestamp,
        // value
        val_size: u32,
        offset: u32,
    }

    impl Slot {
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < SLOT_SIZE {
                return Err("Insufficient bytes to form Slot".into());
            }

            let mut current_pos = 0;

            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse key_size")?,
            );
            current_pos += 4;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            key_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            let pkey_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse pkey_size")?,
            );
            current_pos += 4;

            let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
            pkey_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE]);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            let ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + 8].try_into().map_err(|_| "Failed to parse timestamp")?,
            );
            current_pos += 8;

            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse val_size")?,
            );
            current_pos += 4;

            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse offset")?,
            );

            Ok(Slot {
                key_size,
                key_prefix,
                pkey_size,
                pkey_prefix,
                ts,
                val_size,
                offset,
            })
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0u8; SLOT_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.key_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE].copy_from_slice(&self.key_prefix);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.pkey_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE].copy_from_slice(&self.pkey_prefix);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + 8].copy_from_slice(&self.ts.to_be_bytes());
            current_pos += 8;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.val_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.offset.to_be_bytes());

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8], offset: usize) -> Self {
            let key_size = key.len() as u32;
            let pkey_size = pkey.len() as u32;
            let val_size = val.len() as u32;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            key_prefix[..key_prefix_len].copy_from_slice(&key[..key_prefix_len]);

            let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
            let pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());
            pkey_prefix[..pkey_prefix_len].copy_from_slice(&pkey[..pkey_prefix_len]);

            Slot {
                key_size,
                key_prefix,
                pkey_size,
                pkey_prefix,
                ts,
                val_size,
                offset: offset as u32,
            }
        }

        pub fn key_size(&self) -> u32 {
            self.key_size
        }

        pub fn key_prefix(&self) -> &[u8] {
            if self.key_size as usize > SLOT_KEY_PREFIX_SIZE {
                &self.key_prefix
            } else {
                &self.key_prefix[..self.key_size as usize]
            }
        }

        pub fn pkey_size(&self) -> u32 {
            self.pkey_size
        }

        pub fn pkey_prefix(&self) -> &[u8] {
            if self.pkey_size as usize > SLOT_PKEY_PREFIX_SIZE {
                &self.pkey_prefix
            } else {
                &self.pkey_prefix[..self.pkey_size as usize]
            }
        }

        pub fn ts(&self) -> Timestamp {
            self.ts
        }

        pub fn set_ts(&mut self, ts: Timestamp) {
            self.ts = ts;
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
    use super::slot::{SLOT_KEY_PREFIX_SIZE, SLOT_PKEY_PREFIX_SIZE};

    pub struct Record {
        remain_key: Vec<u8>,
        remain_pkey: Vec<u8>,
        val: Vec<u8>,
    }

    impl Record {
        pub fn from_bytes(bytes: &[u8], key_size: u32, pkey_size: u32, val_size: u32) -> Self {
            let key_size = key_size as usize;
            let pkey_size = pkey_size as usize;
            let val_size = val_size as usize;

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE);

            let total_size = remain_key_size + remain_pkey_size + val_size;
            assert!(bytes.len() >= total_size, "Insufficient bytes for Record");

            let mut current_pos = 0;

            let remain_key = bytes[current_pos..current_pos + remain_key_size].to_vec();
            current_pos += remain_key_size;

            let remain_pkey = bytes[current_pos..current_pos + remain_pkey_size].to_vec();
            current_pos += remain_pkey_size;

            let val = bytes[current_pos..current_pos + val_size].to_vec();

            Record {
                remain_key,
                remain_pkey,
                val,
            }
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(self.remain_key.len() + self.remain_pkey.len() + self.val.len());

            bytes.extend_from_slice(&self.remain_key);
            bytes.extend_from_slice(&self.remain_pkey);
            bytes.extend_from_slice(&self.val);

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], val: &[u8]) -> Self {
            let remain_key = if key.len() > SLOT_KEY_PREFIX_SIZE {
                key[SLOT_KEY_PREFIX_SIZE..].to_vec()
            } else {
                Vec::new()
            };

            let remain_pkey = if pkey.len() > SLOT_PKEY_PREFIX_SIZE {
                pkey[SLOT_PKEY_PREFIX_SIZE..].to_vec()
            } else {
                Vec::new()
            };

            Record {
                remain_key,
                remain_pkey,
                val: val.to_vec(),
            }
        }

        pub fn remain_key(&self) -> &[u8] {
            &self.remain_key
        }

        pub fn remain_pkey(&self) -> &[u8] {
            &self.remain_pkey
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

pub trait MvccHashJoinRecentPage {
    fn init(&mut self);
    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError>;
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError>;
    fn update(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError>;
    
    fn next_page(&self) -> Option<(PageId, u32)>;
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);
    
    // helper function
    fn header(&self) -> Header;
    fn set_header(&mut self, header: &Header);

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32;
    fn free_space_without_compaction(&self) -> u32 {
        let header = self.header();
        header.rec_start_offset() - header.slot_count() * SLOT_SIZE as u32
    }
    fn free_space_with_compaction(&self) -> u32 {
        let header = self.header();
        header.total_bytes_used()
    }
}

impl MvccHashJoinRecentPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let space_need = <Page as MvccHashJoinRecentPage>::space_need(key, pkey, val);
        if space_need > self.free_space_without_compaction() {
            log_debug!("should not happen, detect before calling insert");
            return Err(AccessMethodError::OutOfSpace);
        }

        let mut header = self.header();
        let record_size = space_need - SLOT_SIZE as u32;
        let rec_offset = header.rec_start_offset() - record_size;
        let slot_offset = PAGE_HEADER_SIZE as u32 + header.slot_count() * SLOT_SIZE as u32;
        if rec_offset < slot_offset + SLOT_SIZE as u32 {
            log_debug!("should not happen, detect before calling insert");
            return Err(AccessMethodError::OutOfSpace);
        }

        let slot = Slot::new(key, pkey, ts, val, rec_offset as usize);
        let slot_bytes = slot.to_bytes();
        
        let record = Record::new(key, pkey, val);
        let record_bytes = record.to_bytes();
        
        self[slot_offset as usize..slot_offset as usize + SLOT_SIZE].copy_from_slice(&slot_bytes);
        self[rec_offset as usize..rec_offset as usize + record_size as usize].copy_from_slice(&record_bytes);
        
        header.increment_slot_count();
        header.set_total_bytes_used(header.total_bytes_used() + space_need);
        header.set_rec_start_offset(rec_offset);
        self.set_header(&header);

        Ok(())
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let header = self.header();
        let slot_count = header.slot_count();
        let mut slot_offset = PAGE_HEADER_SIZE;

        for _ in 0..slot_count {
            let slot_bytes = &self[slot_offset..slot_offset + SLOT_SIZE];
            let slot = Slot::from_bytes(slot_bytes).unwrap();
            if slot.key_size() == key.len() as u32 
                && slot.pkey_size() == pkey.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())] 
            {
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size() 
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32) 
                    + slot.pkey_size().saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let record = Record::from_bytes(
                    record_bytes, 
                    slot.key_size(), 
                    slot.pkey_size(), 
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());
                
                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                if full_key == key && full_pkey == pkey {
                    if slot.ts() <= ts {
                        return Ok(record.val().to_vec());
                    } else {
                        return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                }
            }
            slot_offset += SLOT_SIZE;
        }
        Err(AccessMethodError::KeyNotFound)
    }

    fn update(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let header = self.header();
        let slot_count = header.slot_count();
        let mut slot_offset = PAGE_HEADER_SIZE;

        for _ in 0..slot_count {
            let slot_bytes = &self[slot_offset..slot_offset + SLOT_SIZE];
            let mut slot = Slot::from_bytes(slot_bytes).unwrap();

            // Check if key and pkey match
            if slot.key_size() == key.len() as u32
                && slot.pkey_size() == pkey.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())]
            {
                // Retrieve the record
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot.pkey_size().saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let mut record = Record::from_bytes(
                    record_bytes,
                    slot.key_size(),
                    slot.pkey_size(),
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());

                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                if full_key == key && full_pkey == pkey {
                    // Check timestamp
                    if slot.ts() <= ts {
                        // Save old timestamp and value
                        let old_ts = slot.ts();
                        let old_val = record.val().to_vec();

                        let new_val_size = val.len() as u32;

                        if new_val_size <= slot.val_size() {
                            // Overwrite in place
                            record.update(val);
                            let new_rec_size = record.to_bytes().len() as u32;

                            // Write back the record
                            let record_bytes_new = record.to_bytes();
                            self[rec_offset..rec_offset + new_rec_size as usize]
                                .copy_from_slice(&record_bytes_new);

                            // Update slot timestamp
                            slot.set_ts(ts);
                            slot.set_val_size(new_val_size as usize);

                            let slot_bytes_new = slot.to_bytes();
                            self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot_bytes_new);

                            return Ok((old_ts, old_val));
                        } else {
                            // Need to allocate new space for the record
                            // Calculate space need for new record
                            let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
                            let remain_pkey_size = pkey.len().saturating_sub(SLOT_PKEY_PREFIX_SIZE);
                            let new_rec_size =
                                remain_key_size as u32 + remain_pkey_size as u32 + new_val_size;

                            // Check if there is enough space
                            let free_space = self.free_space_without_compaction();
                            if new_rec_size > free_space {
                                return Err(AccessMethodError::OutOfSpace);
                            }

                            // Update header
                            let mut header = self.header();
                            let new_rec_offset = header.rec_start_offset() - new_rec_size;
                            header.set_rec_start_offset(new_rec_offset);
                            header.set_total_bytes_used(
                                header.total_bytes_used() + new_rec_size - rec_size,
                            );
                            self.set_header(&header);

                            // Create new record
                            let new_record = Record::new(key, pkey, val);
                            let new_record_bytes = new_record.to_bytes();
                            self[new_rec_offset as usize
                                ..(new_rec_offset + new_rec_size) as usize]
                                .copy_from_slice(&new_record_bytes);

                            // Update slot
                            slot.set_ts(ts);
                            slot.set_val_size(new_val_size as usize);
                            slot.set_offset(new_rec_offset);
                            let slot_bytes_new = slot.to_bytes();
                            self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot_bytes_new);

                            return Ok((old_ts, old_val));
                        }
                    } else {
                        return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                }
            }

            slot_offset += SLOT_SIZE;
        }

        Err(AccessMethodError::KeyNotFound)
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        let header = self.header();
        header.next_page()
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        let mut header = self.header();
        header.set_next_page(next_page_id, frame_id);
        self.set_header(&header);
    }

    // helper function
    fn header(&self) -> Header {
        let header_bytes = &self[0..PAGE_HEADER_SIZE];
        Header::from_bytes(&header_bytes).unwrap()
    }

    fn set_header(&mut self, header: &Header) {
        let header_bytes = header.to_bytes();
        self[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey.len().saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        SLOT_SIZE as u32 + remain_key_size as u32 + remain_pkey_size as u32 + val.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{Page, AVAILABLE_PAGE_SIZE};
    
    #[test]
    fn test_insert_and_get_key_pkey_len_less_than_prefix() {
        // Key and PKey lengths less than prefix sizes
        let key = b"key1"; // Length 4
        let pkey = b"pk1"; // Length 3
        let ts: Timestamp = 1;
        let val = b"value1";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_and_get_key_len_less_than_prefix_pkey_len_greater_than_prefix() {
        // Key length less than prefix size, PKey length greater than prefix size
        let key = b"key2"; // Length 4
        let pkey = b"primarykey_longer"; // Length > 8
        let ts: Timestamp = 2;
        let val = b"value2";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_and_get_key_len_greater_than_prefix_pkey_len_less_than_prefix() {
        // Key length greater than prefix size, PKey length less than prefix size
        let key = b"key_longer_than_prefix"; // Length > 8
        let pkey = b"pk2"; // Length 3
        let ts: Timestamp = 3;
        let val = b"value3";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_and_get_key_pkey_len_greater_than_prefix() {
        // Key and PKey lengths greater than prefix sizes
        let key = b"key_longer_than_prefix_size"; // Length > 8
        let pkey = b"primarykey_longer_than_prefix"; // Length > 8
        let ts: Timestamp = 4;
        let val = b"value4";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_multiple_entries_with_various_key_pkey_lengths() {
        // Define the entries with keys and pkeys as slices (&[u8])
        let entries: Vec<(&[u8], &[u8], u64, &[u8])> = vec![
            (b"k1", b"p1", 10u64, b"v1"), // Both key and pkey < prefix size
            (b"key_longlong", b"p2", 20u64, b"v2"), // Key > prefix size, pkey < prefix size
            (b"k3", b"primarykey_long", 30u64, b"v3"), // Key < prefix size, pkey > prefix size
            (b"key_very_long", b"primarykey_very_long", 40u64, b"v4"), // Both key and pkey > prefix size
        ];
    
        let mut page = Page::new_empty();
        page.init();
    
        // Insert entries
        for (key, pkey, ts, val) in &entries {
            page.insert(key, pkey, *ts, val).unwrap();
        }
    
        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = page.get(key, pkey, *ts).unwrap();
            assert_eq!(retrieved_val, *val);
        }
    }

    #[test]
    fn test_insert_and_get_with_timestamp_check() {
        // Test that entries with timestamps greater than the query timestamp are not returned
        let key = b"key_test";
        let pkey = b"pkey_test";
        let ts_insert: Timestamp = 100;
        let ts_query: Timestamp = 50; // Less than ts_insert
        let val = b"value_test";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val).unwrap();

        // Attempt to retrieve with earlier timestamp
        let result = page.get(key, pkey, ts_query);
        assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));

        // Retrieve with correct timestamp
        let retrieved_val = page.get(key, pkey, ts_insert).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_duplicate_keys() {
        // Insert entries with the same key and pkey but different timestamps
        let key = b"key_dup";
        let pkey1: &[u8; 8] = b"pkey_dup";
        let pkey2: &[u8; 9] = b"pkey_dup2";
        let val1 = b"value1";
        let val2 = b"value2";
        let ts1: Timestamp = 1;
        let ts2: Timestamp = 2;

        let mut page = Page::new_empty();
        page.init();

        // Insert first entry
        page.insert(key, pkey1, ts1, val1).unwrap();

        // Insert second entry with a newer timestamp
        page.insert(key, pkey2, ts2, val2).unwrap();

        // Retrieve with ts1
        let retrieved_val = page.get(key, pkey1, ts1).unwrap();
        assert_eq!(retrieved_val, val1);

        // Retrieve with ts2
        let retrieved_val = page.get(key, pkey2, ts2).unwrap();
        assert_eq!(retrieved_val, val2);
    }

    #[test]
    fn test_insert_when_page_full() {
        // Fill the page to capacity and attempt to insert another entry
        let mut page = Page::new_empty();
        page.init();

        // Use fixed-length keys and pkeys
        let key = b"key_full_full";         // Length 12 bytes
        let pkey = b"pkey_full_full";       // Length 13 bytes
        let val = b"value_full";            // Value size
        let ts: Timestamp = 1;

        let space_per_entry = Page::space_need(key, pkey, val) as usize;
        let available_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        let page_capacity = available_space / space_per_entry;

        // Insert entries until the page is full
        for _ in 0..page_capacity {
            page.insert(key, pkey, ts, val).unwrap();
        }

        // Attempt to insert one more entry
        let result = page.insert(key, pkey, ts, val);
        assert!(matches!(result, Err(AccessMethodError::OutOfSpace)));
    }

    #[test]
    fn test_update_same_size_value() {
        let key = b"key1";
        let pkey = b"pkey1";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"value1"; // Length 6
        let val_update = b"value2"; // Length 6 (same as val_insert)

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val_insert).unwrap();

        // Update the entry
        let (old_ts, old_val) = page.update(key, pkey, ts_update, val_update).unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val_insert);

        // Retrieve the updated entry
        let retrieved_val = page.get(key, pkey, ts_update).unwrap();
        assert_eq!(retrieved_val, val_update);
    }

    #[test]
    fn test_update_smaller_value() {
        let key = b"key2";
        let pkey = b"pkey2";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"value_longer"; // Length 12
        let val_update = b"short";        // Length 5 (smaller than val_insert)

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val_insert).unwrap();

        // Update the entry
        let (old_ts, old_val) = page.update(key, pkey, ts_update, val_update).unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val_insert);

        // Retrieve the updated entry
        let retrieved_val = page.get(key, pkey, ts_update).unwrap();
        assert_eq!(retrieved_val, val_update);
    }

    #[test]
    fn test_update_larger_value_enough_space() {
        let key = b"key3";
        let pkey = b"pkey3";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"short";           // Length 5
        let val_update = b"value_is_longer"; // Length 14 (larger than val_insert)

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val_insert).unwrap();

        // Update the entry
        let (old_ts, old_val) = page.update(key, pkey, ts_update, val_update).unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val_insert);

        // Retrieve the updated entry
        let retrieved_val = page.get(key, pkey, ts_update).unwrap();
        assert_eq!(retrieved_val, val_update);
    }

    #[test]
    fn test_update_larger_value_insufficient_space() {
        let key = b"key4";
        let pkey = b"pkey4";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"val";           // Length 3
        let val_update = vec![b'a'; (AVAILABLE_PAGE_SIZE / 2) as usize]; // Large value

        let mut page = Page::new_empty();
        page.init();

        // Fill the page to limit the available space
        page.insert(b"key_dummy", b"pkey_dummy", 50, &vec![b'b'; (AVAILABLE_PAGE_SIZE / 2) as usize]).unwrap();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val_insert).unwrap();

        // Attempt to update the entry with a larger value
        let result = page.update(key, pkey, ts_update, &val_update);
        assert!(matches!(result, Err(AccessMethodError::OutOfSpace)));
    }

    #[test]
    fn test_update_non_existent_key() {
        let key = b"key_nonexistent";
        let pkey = b"pkey_nonexistent";
        let ts_update: Timestamp = 100;
        let val_update = b"value";

        let mut page = Page::new_empty();
        page.init();

        // Attempt to update a non-existent key
        let result = page.update(key, pkey, ts_update, val_update);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_update_invalid_timestamp() {
        let key = b"key5";
        let pkey = b"pkey5";
        let ts_insert: Timestamp = 200;
        let ts_update: Timestamp = 100; // Less than ts_insert
        let val_insert = b"value1";
        let val_update = b"value2";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val_insert).unwrap();

        // Attempt to update with an earlier timestamp
        let result = page.update(key, pkey, ts_update, val_update);
        assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
    }

}