use crate::{
    access_method::AccessMethodError,
    prelude::{Page, PageId, AVAILABLE_PAGE_SIZE},
};
type Timestamp = u64;

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
                bytes[current_pos..current_pos + 8].try_into().map_err(|_| "Failed to parse next_page_id")?,
            );
            current_pos += 8;

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

            bytes[current_pos..current_pos + 8].copy_from_slice(&self.next_page_id.to_be_bytes());
            current_pos += 8;

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

pub trait MvccHashJoinHistoryPage {
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
    ) -> Result<(), AccessMethodError>;
    
    fn next_page(&self) -> Option<(PageId, u32)>;
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);
    
    // helper function
    fn header(&self) -> Header;
    fn set_header(&mut self, header: &Header);
}

impl MvccHashJoinHistoryPage for Page {
    fn init(&mut self) {
    }

    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        // let header = self.header();
        // let slot_count = header.slot_count();
        // let total_bytes_used = header.total_bytes_used();
        // let rec_start_offset = header.rec_start_offset();

        // let slot = Slot::new(key, pkey, ts, val, rec_start_offset as usize);
        // let slot_bytes = slot.to_bytes();

        // let record = Record::new(key, pkey, val);
        // let record_bytes = record.to_bytes();

        // let new_total_bytes_used = total_bytes_used + SLOT_SIZE as u32 + record_bytes.len() as u32;
        // if new_total_bytes_used > AVAILABLE_PAGE_SIZE as u32 {
        //     return Err(AccessMethodError::OutOfSpace);
        // }

        // self[total_bytes_used as usize..total_bytes_used as usize + SLOT_SIZE].copy_from_slice(&slot_bytes);
        // self[rec_start_offset as usize..rec_start_offset as usize + record_bytes.len()].copy_from_slice(&record_bytes);

        // let mut new_header = header;
        // new_header.increment_slot_count();
        // new_header.set_total_bytes_used(new_total_bytes_used);
        // new_header.set_rec_start_offset(rec_start_offset + record_bytes.len() as u32);

        // self.set_header(&new_header);

        Ok(())
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        // let header = self.header();
        // let slot_count = header.slot_count();
        // let total_bytes_used = header.total_bytes_used();
        // let rec_start_offset = header.rec_start_offset();

        // let mut current_pos = PAGE_HEADER_SIZE;
        // for _ in 0..slot_count {
        //     let slot_bytes: [u8; SLOT_SIZE] = self[current_pos..current_pos + SLOT_SIZE].try_into().unwrap();
        //     let slot = Slot::from_bytes(&slot_bytes);

        //     if slot.key_prefix() == key && slot.pkey_prefix() == pkey {
        //         let record_bytes: Vec<u8> = self[slot.offset() as usize..rec_start_offset as usize].to_vec();
        //         let record = Record::from_bytes(&record_bytes, slot.key_size(), slot.pkey_size(), slot.val_size());

        //         return Ok(record.val().to_vec());
        //     }

        //     current_pos += SLOT_SIZE;
        // }

        Err(AccessMethodError::KeyNotFound)
    }

    fn update(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        // let header = self.header();
        // let slot_count = header.slot_count();
        // let total_bytes_used = header.total_bytes_used();
        // let rec_start_offset = header.rec_start_offset();

        // let mut current_pos = PAGE_HEADER_SIZE;
        // for i in 0..slot_count {
        //     let slot_bytes: [u8; SLOT_SIZE] = self[current_pos..current_pos + SLOT_SIZE].try_into().unwrap();
        //     let slot = Slot::from_bytes(&slot_bytes);

        //     if slot.key_prefix() == key && slot.pkey_prefix() == pkey {
        //         let record_bytes: Vec<u8> = self[slot.offset() as usize..rec_start_offset as usize].to_vec();
        //         let mut record = Record::from_bytes(&record_bytes, slot.key_size(), slot.pkey_size(), slot.val_size());

        //         record.update(val);
        //         let new_record_bytes = record.to_bytes();

        //         self[slot.offset() as usize..rec_start_offset as usize].copy_from_slice(&new_record_bytes);

        //         return Ok(());
        //     }

        //     current_pos += SLOT_SIZE;
        // }

        Err(AccessMethodError::KeyNotFound)
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        // self.header().next_page()
        todo!()
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        // let mut header = self.header();
        // header.set_next_page(next_page_id, frame_id);
        // self.set_header(&header);
    }

    // helper function
    fn header(&self) -> Header {
        let header_bytes: [u8; PAGE_HEADER_SIZE] = self[0..PAGE_HEADER_SIZE].try_into().unwrap();
        Header::from_bytes(&header_bytes).unwrap()
    }

    fn set_header(&mut self, header: &Header) {
        // let header_bytes = header.to_bytes();
        // self[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
    }
}