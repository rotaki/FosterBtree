use std::{cmp::Ordering, mem::size_of};

use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
const PAGE_HEADER_SIZE: usize = 0;

pub const SHORT_KEY_PAGE_HEADER_SIZE: usize = size_of::<ShortKeyHeader>();
pub const SHORT_KEY_SLOT_SIZE: usize = size_of::<ShortKeySlot>();

pub trait ShortKeyPage {
    fn new() -> Self;
    fn init(&mut self);

    /// Insert a new key-value pair into the index.
    /// If the key already exists, it will return an error.
    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), ShortKeyPageError>;
    fn insert_large_value(
        &mut self,
        key: &[u8],
        val_len: u32,
        root_pid: PageId,
        root_fid: u32,
    ) -> Result<(), ShortKeyPageError>;

    /// Update the value of an existing key.
    /// If the key does not exist, it will return an error.
    fn update(&mut self, key: &[u8], val: &[u8]) -> Result<(), ShortKeyPageError>;
    fn update_with_return(&mut self, key: &[u8], val: &[u8]) -> Result<Vec<u8>, ShortKeyPageError>;
    fn update_at_slot(&mut self, slot_id: u32, val: &[u8]) -> Result<(), ShortKeyPageError>;

    /// Upsert a key-value pair into the index.
    /// If the key already exists, it will update the value.
    /// If the key does not exist, it will insert a new key-value pair.
    fn upsert(&mut self, key: &[u8], val: &[u8]) -> Result<(), ShortKeyPageError>;

    /// Upsert with a custom merge function.
    /// If the key already exists, it will update the value with the merge function.
    /// If the key does not exist, it will insert a new key-value pair.
    fn upsert_with_merge<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> Result<(), ShortKeyPageError>
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>;

    fn get(&self, key: &[u8]) -> Result<ShortKeyPageResult, ShortKeyPageError>;

    fn remove(&mut self, key: &[u8]) -> Result<(), ShortKeyPageError>;

    fn encode_shortkey_header(&mut self, header: &ShortKeyHeader);
    fn decode_shortkey_header(&self) -> ShortKeyHeader;

    fn get_next_page_id(&self) -> PageId;
    fn set_next_page_id(&mut self, next_page_id: PageId);

    fn get_next_frame_id(&self) -> u32;
    fn set_next_frame_id(&mut self, next_frame_id: u32);

    fn encode_shortkey_slot(&mut self, index: u32, slot: &ShortKeySlot);
    fn decode_shortkey_slot(&self, index: u32) -> ShortKeySlot;
    fn remove_shortkey_slot(&mut self, index: u32);

    fn encode_shortkey_value(&mut self, offset: usize, entry: &ShortKeyValue);
    fn encode_shortkey_value_sample(&mut self, offset: usize, entry: &ShortKeyValueSample);

    fn decode_shortkey_value(&self, offset: usize, remain_key_len: u32) -> ShortKeyValue;
    fn decode_shortkey_value_by_id(&self, slot_id: u32) -> ShortKeyValue;

    fn search_slot(&self, key: &[u8]) -> (bool, u32) {
        self.binary_search(|slot_id: u32| self.compare_key(key, slot_id))
        // self.interpolation_search(key)
    }

    fn compare_key(&self, key: &[u8], slot_id: u32) -> Ordering;
    fn binary_search<F>(&self, f: F) -> (bool, u32)
    // return (found, index)
    where
        F: Fn(u32) -> Ordering;

    fn key_to_u64(key: &[u8]) -> u64; // Convert key to a numeric value for interpolation search
    fn interpolation_search(&self, key: &[u8]) -> (bool, u32);

    fn is_exist(&self, key: &[u8]) -> (Option<u32>, usize); // return (Option(slot_id), vals_len)

    fn slot_end_offset(&self) -> usize;
    fn num_slots(&self) -> u32;
    fn total_free_spcae(&self) -> usize;
    fn get_free_space(&self) -> usize;

    fn get_use_rate(&self) -> f64;
}

// define error type
#[derive(Debug)]
pub enum ShortKeyPageError {
    KeyExists,
    KeyNotFound,
    OutOfSpace,
}

pub enum ShortKeyPageResult {
    Value(Vec<u8>),
    Overflow {
        val_len: u32,
        root_page_id: PageId,
        root_frame_id: u32,
    },
}

#[derive(Debug, Clone)]
pub struct ShortKeyHeader {
    next_page_id: PageId,
    next_frame_id: u32,
    slot_num: u32,
    val_start_offset: u32,
}

#[derive(Debug, Clone)]
pub struct ShortKeySlot {
    pub key_len: u32,
    pub key_prefix: [u8; 8],
    pub val_offset: u32,
}

#[derive(Debug, Clone)]
pub struct ShortKeyValue {
    pub remain_key: Vec<u8>, // dynamic size (remain part of actual key), if key_len > 8
    pub vals_len: u32, // if its u32::MAX, then it means we store the overflowed value in the other pages
    pub vals: Vec<u8>, // vector of vals (variable size), if vals_len == u32::MAX then store (val_len, PageId, FrameId) for overflowed value
}

pub struct ShortKeyValueSample<'a> {
    pub remain_key: &'a [u8],
    pub vals_len: u32,
    pub vals: &'a [u8],
}

impl ShortKeyPage for Page {
    fn new() -> Self {
        let mut page = Page::new_empty();
        let header = ShortKeyHeader {
            next_page_id: 0,
            next_frame_id: u32::MAX,
            slot_num: 0,
            val_start_offset: AVAILABLE_PAGE_SIZE as u32,
        };
        Self::encode_shortkey_header(&mut page, &header);
        page
    }

    fn init(&mut self) {
        let header = ShortKeyHeader {
            next_page_id: 0,
            next_frame_id: u32::MAX,
            slot_num: 0,
            val_start_offset: AVAILABLE_PAGE_SIZE as u32,
        };
        Self::encode_shortkey_header(self, &header);
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ShortKeyPageError> {
        let (found, index) = self.search_slot(key);
        if found {
            return Err(ShortKeyPageError::KeyExists);
        }

        let mut header = self.decode_shortkey_header();
        let remain_key_len = key.len().saturating_sub(8);
        let required_space =
            SHORT_KEY_SLOT_SIZE + (remain_key_len + size_of::<u32>() + value.len());

        if required_space > self.get_free_space() {
            return Err(ShortKeyPageError::OutOfSpace);
        }

        if index < header.slot_num {
            let start_pos = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + index as usize * SHORT_KEY_SLOT_SIZE;
            let end_pos = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + header.slot_num as usize * SHORT_KEY_SLOT_SIZE;
            self.copy_within(start_pos..end_pos, start_pos + SHORT_KEY_SLOT_SIZE);
        }

        let new_val_offset =
            header.val_start_offset as usize - (value.len() + size_of::<u32>() + remain_key_len);

        let new_slot = ShortKeySlot {
            key_len: key.len() as u32,
            key_prefix: {
                let mut prefix = [0u8; 8];
                let copy_len = std::cmp::min(8, key.len());
                prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                prefix
            },
            val_offset: new_val_offset as u32,
        };
        self.encode_shortkey_slot(index, &new_slot);

        let new_value_entry = ShortKeyValueSample {
            remain_key: &key[key.len().min(8)..],
            vals_len: value.len() as u32,
            vals: value,
        };
        self.encode_shortkey_value_sample(new_val_offset, &new_value_entry);

        header.slot_num += 1;
        header.val_start_offset = new_val_offset as u32;
        self.encode_shortkey_header(&header);

        Ok(())
    }

    fn insert_large_value(
        &mut self,
        key: &[u8],
        val_len: u32,
        root_pid: PageId,
        root_fid: u32,
    ) -> Result<(), ShortKeyPageError> {
        let (found, index) = self.search_slot(key);
        if found {
            return Err(ShortKeyPageError::KeyExists);
        }

        let mut header = self.decode_shortkey_header();
        let remain_key_len = key.len().saturating_sub(8);
        let required_space =
            SHORT_KEY_SLOT_SIZE + remain_key_len + size_of::<u32>() * 3 + size_of::<PageId>(); // 3 for root_pid, root_fid, real val_len, u32::MAX val_len

        if required_space > self.get_free_space() {
            return Err(ShortKeyPageError::OutOfSpace);
        }

        if index < header.slot_num {
            let start_pos = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + index as usize * SHORT_KEY_SLOT_SIZE;
            let end_pos = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + header.slot_num as usize * SHORT_KEY_SLOT_SIZE;
            self.copy_within(start_pos..end_pos, start_pos + SHORT_KEY_SLOT_SIZE);
        }

        let new_val_offset = header.val_start_offset as usize
            - (remain_key_len + size_of::<u32>() * 3 + size_of::<PageId>());

        let new_slot = ShortKeySlot {
            key_len: key.len() as u32,
            key_prefix: {
                let mut prefix = [0u8; 8];
                let copy_len = std::cmp::min(8, key.len());
                prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                prefix
            },
            val_offset: new_val_offset as u32,
        };
        self.encode_shortkey_slot(index, &new_slot);

        // Encode the overflow metadata (root_pid, root_fid, val_len)
        let mut overflow_metadata = Vec::with_capacity(size_of::<u32>() * 3);
        overflow_metadata.extend_from_slice(&val_len.to_le_bytes());
        overflow_metadata.extend_from_slice(&root_pid.to_le_bytes());
        overflow_metadata.extend_from_slice(&root_fid.to_le_bytes());

        let new_value_entry = ShortKeyValue {
            remain_key: key[8..].to_vec(),
            vals_len: u32::MAX, // Indicates overflow
            vals: overflow_metadata,
        };
        self.encode_shortkey_value(new_val_offset, &new_value_entry);

        header.slot_num += 1;
        header.val_start_offset = new_val_offset as u32;
        self.encode_shortkey_header(&header);

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<ShortKeyPageResult, ShortKeyPageError> {
        let (found, index) = self.search_slot(key);

        if found {
            let slot = self.decode_shortkey_slot(index);
            let value_entry = self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

            if value_entry.vals_len == u32::MAX {
                // Indicates that the value is stored in overflow pages
                let val_len = u32::from_le_bytes(value_entry.vals[0..4].try_into().unwrap());
                let root_page_id =
                    PageId::from_le_bytes(value_entry.vals[4..8].try_into().unwrap());
                let root_frame_id = u32::from_le_bytes(value_entry.vals[8..12].try_into().unwrap());

                Ok(ShortKeyPageResult::Overflow {
                    val_len,
                    root_page_id,
                    root_frame_id,
                })
            } else {
                Ok(ShortKeyPageResult::Value(value_entry.vals))
            }
        } else {
            Err(ShortKeyPageError::KeyNotFound)
        }
    }

    fn update(&mut self, key: &[u8], val: &[u8]) -> Result<(), ShortKeyPageError> {
        let (found, index) = self.search_slot(key);

        if !found {
            return Err(ShortKeyPageError::KeyNotFound);
        }

        self.update_at_slot(index as u32, val)
    }

    fn upsert(&mut self, key: &[u8], val: &[u8]) -> Result<(), ShortKeyPageError> {
        let (found, index) = self.search_slot(key);

        if found {
            self.update_at_slot(index as u32, val)?;
        } else {
            self.insert(key, val)?;
        }

        Ok(())
    }

    fn upsert_with_merge<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> Result<(), ShortKeyPageError>
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>,
    {
        let (found, index) = self.search_slot(key);

        if found {
            let mut slot = self.decode_shortkey_slot(index);
            let old_value_entry =
                self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

            let new_value = update_fn(&old_value_entry.vals, value);

            if new_value.len() <= old_value_entry.vals.len() {
                let mut updated_value_entry = old_value_entry;
                updated_value_entry.vals = new_value.clone();
                updated_value_entry.vals_len = new_value.len() as u32;
                self.encode_shortkey_value(slot.val_offset as usize, &updated_value_entry);
            } else {
                let remain_key_len = key.len().saturating_sub(8);
                let required_space = remain_key_len + new_value.len() + size_of::<u32>();
                let mut header = self.decode_shortkey_header();

                if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                    self.remove_shortkey_slot(index);
                    return Err(ShortKeyPageError::OutOfSpace);
                }

                let new_val_offset = header.val_start_offset as usize
                    - (new_value.len() + size_of::<u32>() + remain_key_len);
                let new_value_entry = ShortKeyValue {
                    remain_key: old_value_entry.remain_key,
                    vals_len: new_value.len() as u32,
                    vals: new_value,
                };

                slot.val_offset = new_val_offset as u32;
                self.encode_shortkey_slot(index, &slot);
                self.encode_shortkey_value(new_val_offset, &new_value_entry);
                header.val_start_offset = new_val_offset as u32;
                self.encode_shortkey_header(&header);
            }
        } else {
            self.insert(key, value)?;
        }

        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ShortKeyPageError> {
        let (found, index) = self.search_slot(key);

        if found {
            self.remove_shortkey_slot(index);
            Ok(())
        } else {
            Err(ShortKeyPageError::KeyNotFound)
        }
    }

    fn slot_end_offset(&self) -> usize {
        PAGE_HEADER_SIZE
            + SHORT_KEY_PAGE_HEADER_SIZE
            + SHORT_KEY_SLOT_SIZE * self.decode_shortkey_header().slot_num as usize
    }

    fn compare_key(&self, key: &[u8], slot_id: u32) -> Ordering {
        let slot = self.decode_shortkey_slot(slot_id);
        let mut key_prefix = [0u8; 8];
        let copy_len = std::cmp::min(8, key.len());
        key_prefix[..copy_len].copy_from_slice(&key[..copy_len]);

        let prefix_compare = slot.key_prefix.as_slice().cmp(&key_prefix);

        match prefix_compare {
            Ordering::Equal => {
                if key.len() <= 8 {
                    return Ordering::Equal;
                }
                let value_entry =
                    self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);
                value_entry.remain_key.as_slice().cmp(&key[8..])
            }
            _ => prefix_compare,
        }
    }

    // Find the left-most key where f(key) = gte.
    // Assumes that f(key, search_key) is lt for all keys to the left of the returned index.
    // [lt, lt, lt, lt, lt, gte, gte, gte]
    //                       ^
    //                       |
    //                       return this index
    // If all keys are lt, then return the len (i.e. slot_count)
    fn binary_search<F>(&self, f: F) -> (bool, u32)
    where
        F: Fn(u32) -> Ordering,
    {
        let header = self.decode_shortkey_header();
        if header.slot_num == 0 {
            return (false, 0);
        }

        let mut low = 0;
        let mut high = header.slot_num - 1;

        match f(low) {
            Ordering::Equal => return (true, low),
            Ordering::Greater => return (false, 0),
            Ordering::Less => {}
        }

        match f(high) {
            Ordering::Equal => return (true, high),
            Ordering::Less => return (false, high + 1),
            Ordering::Greater => {}
        }

        // Invairant: f(high) = Gte
        while low < high {
            let mid = low + (high - low) / 2;
            match f(mid) {
                Ordering::Equal => return (true, mid),
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
            }
        }

        (false, low)
    }

    // Interpolation search with input key
    fn interpolation_search(&self, key: &[u8]) -> (bool, u32) {
        let header = self.decode_shortkey_header();
        if header.slot_num == 0 {
            return (false, 0);
        }

        let mut low = 0;
        let mut high = header.slot_num - 1;
        let key_value = Self::key_to_u64(key);

        while low <= high {
            let low_value = Self::key_to_u64(&self.decode_shortkey_slot(low).key_prefix);
            match key_value.cmp(&low_value) {
                Ordering::Equal => return (true, low),
                Ordering::Less => return (false, low),
                Ordering::Greater => {}
            }

            let high_value = Self::key_to_u64(&self.decode_shortkey_slot(high).key_prefix);
            match key_value.cmp(&high_value) {
                Ordering::Equal => return (true, high),
                Ordering::Greater => return (false, high + 1),
                Ordering::Less => {}
            }

            // Estimate the position using interpolation
            let pos = low as u64
                + ((key_value - low_value) / (high_value - low_value) * (high as u64 - low as u64));

            if pos > high as u64 {
                panic!("Should filtered by the above condition");
            }

            let pos = pos as u32;
            let pos_value = Self::key_to_u64(&self.decode_shortkey_slot(pos).key_prefix);

            match pos_value.cmp(&key_value) {
                Ordering::Equal => return (true, pos),
                Ordering::Less => low = pos + 1,
                Ordering::Greater => high = pos,
            }
        }

        (false, low)
    }

    // when update, if the new value is bigger so OutofSpace, then remove the slot
    fn update_with_return(&mut self, key: &[u8], val: &[u8]) -> Result<Vec<u8>, ShortKeyPageError> {
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        // let (found, index) = self.interpolation_search(key);
        let (found, index) = self.search_slot(key);

        if !found {
            return Err(ShortKeyPageError::KeyNotFound);
        }

        let mut slot = self.decode_shortkey_slot(index);
        let mut old_value_entry =
            self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

        if val.len() <= old_value_entry.vals.len() {
            let old_vals = std::mem::replace(&mut old_value_entry.vals, val.to_vec());
            old_value_entry.vals_len = val.len() as u32;
            self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
            Ok(old_vals)
        } else {
            let remain_key_len = key.len().saturating_sub(8);
            let required_space = remain_key_len + val.len() + size_of::<u32>();
            if required_space
                > self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
            {
                self.remove_shortkey_slot(index);
                return Err(ShortKeyPageError::OutOfSpace);
            }

            let new_val_offset = self.decode_shortkey_header().val_start_offset as usize
                - (val.len() + size_of::<u32>() + remain_key_len);
            let new_value_entry = ShortKeyValue {
                remain_key: old_value_entry.remain_key,
                vals_len: val.len() as u32,
                vals: val.to_vec(),
            };

            slot.val_offset = new_val_offset as u32;
            self.encode_shortkey_slot(index, &slot);

            self.encode_shortkey_value(new_val_offset, &new_value_entry);
            let mut header = self.decode_shortkey_header();
            header.val_start_offset = new_val_offset as u32;
            self.encode_shortkey_header(&header);

            Ok(old_value_entry.vals)
        }
    }

    fn update_at_slot(&mut self, slot_id: u32, val: &[u8]) -> Result<(), ShortKeyPageError> {
        let slot = self.decode_shortkey_slot(slot_id);
        let mut old_value_entry =
            self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

        if val.len() <= old_value_entry.vals.len() {
            let _old_vals = std::mem::replace(&mut old_value_entry.vals, val.to_vec());
            old_value_entry.vals_len = val.len() as u32;
            self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
            Ok(())
        } else {
            let remain_key_len = slot.key_len.saturating_sub(8) as usize;
            let required_space = remain_key_len + val.len() + size_of::<u32>();
            if required_space
                > self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
            {
                self.remove_shortkey_slot(slot_id);
                return Err(ShortKeyPageError::OutOfSpace);
            }

            let new_val_offset = self.decode_shortkey_header().val_start_offset as usize
                - (val.len() + size_of::<u32>() + remain_key_len);
            let new_value_entry = ShortKeyValue {
                remain_key: old_value_entry.remain_key,
                vals_len: val.len() as u32,
                vals: val.to_vec(),
            };

            let mut slot = self.decode_shortkey_slot(slot_id);
            slot.val_offset = new_val_offset as u32;
            self.encode_shortkey_slot(slot_id, &slot);

            self.encode_shortkey_value(new_val_offset, &new_value_entry);
            let mut header = self.decode_shortkey_header();
            header.val_start_offset = new_val_offset as u32;
            self.encode_shortkey_header(&header);

            Ok(())
        }
    }

    fn encode_shortkey_header(&mut self, header: &ShortKeyHeader) {
        let offset = PAGE_HEADER_SIZE;
        self[offset..offset + 4].copy_from_slice(&header.next_page_id.to_le_bytes());
        self[offset + 4..offset + 8].copy_from_slice(&header.next_frame_id.to_le_bytes());
        self[offset + 8..offset + 12].copy_from_slice(&header.slot_num.to_le_bytes());
        self[offset + 12..offset + 16].copy_from_slice(&header.val_start_offset.to_le_bytes());
    }

    fn decode_shortkey_header(&self) -> ShortKeyHeader {
        let offset = PAGE_HEADER_SIZE;
        let next_page_id = PageId::from_le_bytes(
            self[offset..offset + 4]
                .try_into()
                .expect("Invalid slice length"),
        );
        let next_frame_id = u32::from_le_bytes(
            self[offset + 4..offset + 8]
                .try_into()
                .expect("Invalid slice length"),
        );
        let slot_num = u32::from_le_bytes(
            self[offset + 8..offset + 12]
                .try_into()
                .expect("Invalid slice length"),
        );
        let val_start_offset = u32::from_le_bytes(
            self[offset + 12..offset + 16]
                .try_into()
                .expect("Invalid slice length"),
        );

        ShortKeyHeader {
            next_page_id,
            next_frame_id,
            slot_num,
            val_start_offset,
        }
    }

    fn get_next_page_id(&self) -> PageId {
        self.decode_shortkey_header().next_page_id
    }

    fn set_next_page_id(&mut self, next_page_id: PageId) {
        let mut header = self.decode_shortkey_header();
        header.next_page_id = next_page_id;
        self.encode_shortkey_header(&header);
    }

    fn get_next_frame_id(&self) -> u32 {
        self.decode_shortkey_header().next_frame_id
    }

    fn set_next_frame_id(&mut self, next_frame_id: u32) {
        let mut header = self.decode_shortkey_header();
        header.next_frame_id = next_frame_id;
        self.encode_shortkey_header(&header);
    }

    fn encode_shortkey_slot(&mut self, index: u32, slot: &ShortKeySlot) {
        let offset =
            PAGE_HEADER_SIZE + SHORT_KEY_PAGE_HEADER_SIZE + index as usize * SHORT_KEY_SLOT_SIZE;

        self[offset..offset + 4].copy_from_slice(&slot.key_len.to_le_bytes());
        self[offset + 4..offset + 12].copy_from_slice(&slot.key_prefix);
        self[offset + 12..offset + 16].copy_from_slice(&slot.val_offset.to_le_bytes());
    }

    fn decode_shortkey_slot(&self, index: u32) -> ShortKeySlot {
        let offset =
            PAGE_HEADER_SIZE + SHORT_KEY_PAGE_HEADER_SIZE + index as usize * SHORT_KEY_SLOT_SIZE;

        let key_len = u32::from_le_bytes(
            self[offset..offset + 4]
                .try_into()
                .expect("Invalid slice length"),
        );
        let mut key_prefix = [0u8; 8];
        key_prefix.copy_from_slice(&self[offset + 4..offset + 12]);
        let val_offset = u32::from_le_bytes(
            self[offset + 12..offset + 16]
                .try_into()
                .expect("Invalid slice length"),
        );

        ShortKeySlot {
            key_len,
            key_prefix,
            val_offset,
        }
    }

    fn remove_shortkey_slot(&mut self, index: u32) {
        let mut header = self.decode_shortkey_header();
        if index < header.slot_num - 1 {
            let start = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + (index + 1) as usize * SHORT_KEY_SLOT_SIZE;
            let end = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + header.slot_num as usize * SHORT_KEY_SLOT_SIZE;
            let dest = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + index as usize * SHORT_KEY_SLOT_SIZE;

            self.copy_within(start..end, dest);
        }

        header.slot_num -= 1;
        self.encode_shortkey_header(&header);
    }

    fn encode_shortkey_value(&mut self, offset: usize, entry: &ShortKeyValue) {
        self[offset..offset + entry.remain_key.len()].copy_from_slice(&entry.remain_key);
        self[offset + entry.remain_key.len()..offset + entry.remain_key.len() + 4]
            .copy_from_slice(&entry.vals_len.to_le_bytes());
        self[offset + entry.remain_key.len() + 4
            ..offset + entry.remain_key.len() + 4 + entry.vals.len()]
            .copy_from_slice(&entry.vals);
    }

    fn encode_shortkey_value_sample(&mut self, offset: usize, entry: &ShortKeyValueSample) {
        self[offset..offset + entry.remain_key.len()].copy_from_slice(entry.remain_key);
        self[offset + entry.remain_key.len()..offset + entry.remain_key.len() + 4]
            .copy_from_slice(&entry.vals_len.to_le_bytes());
        self[offset + entry.remain_key.len() + 4
            ..offset + entry.remain_key.len() + 4 + entry.vals.len()]
            .copy_from_slice(entry.vals);
    }

    fn decode_shortkey_value(&self, offset: usize, key_len: u32) -> ShortKeyValue {
        let remain_key_len = if key_len > 8 { key_len as usize - 8 } else { 0 };

        let remain_key = if remain_key_len > 0 {
            self[offset..offset + remain_key_len].to_vec()
        } else {
            Vec::new()
        };

        let vals_len_start = offset + remain_key_len;

        let vals_len = u32::from_le_bytes(
            self[vals_len_start..vals_len_start + size_of::<u32>()]
                .try_into()
                .expect("Invalid slice length"),
        );

        if vals_len == u32::MAX {
            // Handle overflow case
            let vals_start = vals_len_start + size_of::<u32>();
            let val_len = u32::from_le_bytes(
                self[vals_start..vals_start + size_of::<u32>()]
                    .try_into()
                    .expect("Invalid slice length"),
            );
            let root_page_id = PageId::from_le_bytes(
                self[vals_start + size_of::<u32>()..vals_start + 2 * size_of::<u32>()]
                    .try_into()
                    .expect("Invalid slice length"),
            );
            let root_frame_id = u32::from_le_bytes(
                self[vals_start + 2 * size_of::<u32>()..vals_start + 3 * size_of::<u32>()]
                    .try_into()
                    .expect("Invalid slice length"),
            );

            ShortKeyValue {
                remain_key,
                vals_len,
                vals: [
                    val_len.to_le_bytes().to_vec(),
                    root_page_id.to_le_bytes().to_vec(),
                    root_frame_id.to_le_bytes().to_vec(),
                ]
                .concat(),
            }
        } else {
            // Handle regular value case
            let vals_start = vals_len_start + size_of::<u32>();
            let vals = self[vals_start..vals_start + vals_len as usize].to_vec();

            ShortKeyValue {
                remain_key,
                vals_len,
                vals,
            }
        }
    }

    fn decode_shortkey_value_by_id(&self, slot_id: u32) -> ShortKeyValue {
        let slot = self.decode_shortkey_slot(slot_id);
        self.decode_shortkey_value(slot.val_offset as usize, slot.key_len)
    }

    fn key_to_u64(key: &[u8]) -> u64 {
        let mut key_array = [0u8; 8];
        let copy_len = std::cmp::min(8, key.len());
        key_array[..copy_len].copy_from_slice(&key[..copy_len]);
        u64::from_be_bytes(key_array)
    }

    fn is_exist(&self, key: &[u8]) -> (Option<u32>, usize) {
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        let (found, index) = self.search_slot(key);
        if found {
            (
                Some(index),
                self.decode_shortkey_value_by_id(index).vals_len as usize,
            )
        } else {
            (None, 0)
        }
    }

    fn num_slots(&self) -> u32 {
        self.decode_shortkey_header().slot_num
    }

    fn total_free_spcae(&self) -> usize {
        self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
    }

    fn get_free_space(&self) -> usize {
        self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
    }

    fn get_use_rate(&self) -> f64 {
        1.0 - (self.get_free_space() as f64 / AVAILABLE_PAGE_SIZE as f64)
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn update_fn(old_value: &[u8], new_value: &[u8]) -> Vec<u8> {
        old_value.iter().chain(new_value.iter()).copied().collect()
    }

    fn random_string(length: usize) -> Vec<u8> {
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(length)
            .map(|c| c as u8)
            .collect()
    }

    #[test]
    fn test_insert_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let value = b"test_value";

        assert!(
            page.insert(key, value).is_ok(),
            "Insertion should be successful"
        );
        assert_eq!(
            page.get(key).unwrap(),
            value.to_vec(),
            "Retrieved value should match the inserted value"
        );
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let value1 = b"first_value";
        let value2 = b"second_value";

        assert!(
            page.insert(key, value1).is_ok(),
            "First insertion should be successful"
        );
        let result = page.insert(key, value2);
        assert!(
            matches!(result, Err(ShortKeyPageError::KeyExists)),
            "Insertion of a duplicate key should return a KeyExists error"
        );
        assert_eq!(page.get(key).unwrap(), value1.to_vec(), "The value associated with the key should remain unchanged after a failed insertion attempt");
    }

    #[test]
    fn test_update_existing_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let initial_value = b"initial_value";
        let updated_value = b"updated_value";

        page.insert(key, initial_value).unwrap();
        assert_eq!(
            page.update_with_return(key, updated_value).unwrap(),
            initial_value.to_vec(),
            "Old value should be returned on update"
        );
        assert_eq!(
            page.get(key).unwrap(),
            updated_value.to_vec(),
            "Retrieved value should match the updated value"
        );
    }

    #[test]
    fn test_upsert_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_123";
        let value = b"value_123";

        assert!(page.upsert(key, value).is_ok());
        assert_eq!(page.get(key).unwrap(), value.to_vec());
    }

    #[test]
    fn test_upsert_duplicate_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_456";
        let value1 = b"value_456";
        let value2 = b"new_value_456";

        assert!(page.upsert(key, value1).is_ok());
        assert!(page.upsert(key, value2).is_ok());
        assert_eq!(page.get(key).unwrap(), value2.to_vec());
    }

    #[test]
    fn test_upsert_out_of_space() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_789";
        let value = vec![0u8; AVAILABLE_PAGE_SIZE]; // Unrealistically large value to simulate out-of-space

        assert!(matches!(
            page.upsert(key, &value),
            Err(ShortKeyPageError::OutOfSpace)
        ));
        assert!(matches!(page.get(key), Err(ShortKeyPageError::KeyNotFound)));
    }

    #[test]
    fn test_upsert_multiple_keys_ordering() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys = vec![
            (b"alpha", b"value_alpha"),
            (b"gamma", b"value_gamma"),
            (b"betaa", b"value_betaa"),
        ];

        for (key, value) in keys.iter() {
            page.upsert(*key, *value).unwrap();
        }

        assert_eq!(page.get(b"alpha").unwrap(), b"value_alpha".to_vec());
        assert_eq!(page.get(b"betaa").unwrap(), b"value_betaa".to_vec());
        assert_eq!(page.get(b"gamma").unwrap(), b"value_gamma".to_vec());
    }

    #[test]
    fn test_boundary_key_upserts() {
        let mut page = <Page as ShortKeyPage>::new();
        let min_key = b"aaaaaaa";
        let max_key = b"zzzzzzz";
        let value = b"value";

        assert!(page.upsert(min_key, value).is_ok());
        assert!(page.upsert(max_key, value).is_ok());

        assert_eq!(page.get(min_key).unwrap(), value.to_vec());
        assert_eq!(page.get(max_key).unwrap(), value.to_vec());
    }

    #[test]
    fn test_consecutive_upserts_deletes() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3"];

        for key in &keys {
            assert!(page.upsert(key, b"value").is_ok());
        }

        for key in &keys {
            assert!(page.remove(key).is_ok());
        }

        for key in &keys {
            assert!(matches!(page.get(key), Err(ShortKeyPageError::KeyNotFound)));
        }

        for key in &keys {
            assert!(page.upsert(key, b"new_value").is_ok());
        }

        for key in &keys {
            assert_eq!(page.get(key).unwrap(), b"new_value".to_vec());
        }
    }

    #[test]
    fn test_updates_with_varying_value_sizes() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let small_value = b"small";
        let large_value = b"this_is_a_much_larger_value_than_before";

        assert!(page.upsert(key, small_value).is_ok());
        assert!(page.upsert(key, large_value).is_ok());
        assert_eq!(page.get(key).unwrap(), large_value.to_vec());

        assert!(page.upsert(key, small_value).is_ok());
        assert_eq!(page.get(key).unwrap(), small_value.to_vec());
    }

    #[test]
    fn test_order_preservation() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"delta", b"alpha", b"echo", b"bravo", b"charlie"];

        for key in keys.iter() {
            page.upsert(*key, b"value").unwrap();
        }

        let mut retrieved_keys = vec![];
        for i in 0..keys.len() {
            let key = page.decode_shortkey_slot(i as u32);
            let key_string = String::from_utf8_lossy(&key.key_prefix)
                .trim_end_matches('\0')
                .to_string();
            retrieved_keys.push(key_string);
        }

        assert_eq!(
            retrieved_keys,
            vec!["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }

    #[test]
    fn test_upsert_with_merge_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"new_key";
        let value = b"value";

        assert!(page.upsert_with_merge(key, value, update_fn).is_ok());
        assert_eq!(page.get(key).unwrap(), value.to_vec());
    }

    #[test]
    fn test_upsert_with_merge_update_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"update_key";
        let value1 = b"value1";
        let value2 = b"value2";

        page.upsert(key, value1).unwrap();
        assert!(page.upsert_with_merge(key, value2, update_fn).is_ok());
        assert_eq!(page.get(key).unwrap(), update_fn(value1, value2));
    }

    #[test]
    fn test_upsert_with_merge_update_key_diff_size() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"update_key_diff_size";
        let value1 = b"value1";
        let value2 = b"value2_longer";

        page.upsert(key, value1).unwrap();
        assert!(page.upsert_with_merge(key, value2, update_fn).is_ok());
        assert_eq!(page.get(key).unwrap(), update_fn(value1, value2));
    }

    #[test]
    fn test_upsert_with_merge_out_of_space() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let large_value = vec![
            0u8;
            AVAILABLE_PAGE_SIZE
                - SHORT_KEY_PAGE_HEADER_SIZE
                - SHORT_KEY_SLOT_SIZE
                - size_of::<u32>()
        ];

        assert!(page.upsert(key, &large_value).is_ok());

        let key2 = b"key2";
        let value2 = b"value2";
        assert!(matches!(
            page.upsert_with_merge(key2, value2, update_fn),
            Err(ShortKeyPageError::OutOfSpace)
        ));
        assert!(matches!(
            page.get(key2),
            Err(ShortKeyPageError::KeyNotFound)
        ));
    }

    #[test]
    fn test_upsert_with_merge_existing_value_update() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"existing_key";
        let value1 = b"value1";
        let value2 = b"value2";

        page.upsert(key, value1).unwrap();
        assert!(page
            .upsert_with_merge(key, value2, |_, new| new.to_vec())
            .is_ok());
        assert_eq!(page.get(key).unwrap(), value2.to_vec());
    }

    #[test]
    fn test_upsert_with_merge_key_too_large() {
        let mut page = <Page as ShortKeyPage>::new();
        let large_key = vec![0u8; AVAILABLE_PAGE_SIZE];
        let value = b"value";

        assert!(matches!(
            page.upsert_with_merge(&large_key, value, update_fn),
            Err(ShortKeyPageError::OutOfSpace)
        ));
        assert!(matches!(
            page.get(&large_key),
            Err(ShortKeyPageError::KeyNotFound)
        ));
    }

    #[test]
    fn test_upsert_with_merge_value_too_large() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let large_value = vec![0u8; AVAILABLE_PAGE_SIZE];

        assert!(matches!(
            page.upsert_with_merge(key, &large_value, update_fn),
            Err(ShortKeyPageError::OutOfSpace)
        ));
        assert!(matches!(page.get(key), Err(ShortKeyPageError::KeyNotFound)));
    }

    #[test]
    fn test_remove_existing_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"remove_key";
        let value = b"value";

        page.upsert(key, value).unwrap();
        assert!(
            page.remove(key).is_ok(),
            "Key should be removed successfully"
        );
        assert!(
            matches!(page.get(key), Err(ShortKeyPageError::KeyNotFound)),
            "Key should no longer exist after removal"
        );
    }

    #[test]
    fn test_remove_non_existent_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"non_existent_key";

        assert!(
            matches!(page.remove(key), Err(ShortKeyPageError::KeyNotFound)),
            "Non-existent key should not be removable"
        );
    }

    #[test]
    fn stress_test_random_keys_and_values() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert!(page.upsert(&key, &value).is_ok());
        }

        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key).unwrap(), value);
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_order() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert!(page.upsert(&key, &value).is_ok());
        }

        keys_and_values.sort_by(|a, b| a.0.cmp(&b.0));

        for (key, expected_value) in keys_and_values.iter() {
            assert_eq!(page.get(key).unwrap(), expected_value.clone());
        }

        let mut last_key = vec![];
        for i in 0..page.decode_shortkey_header().slot_num {
            let slot = page.decode_shortkey_slot(i);
            let current_key = [
                &slot.key_prefix[..],
                &page
                    .decode_shortkey_value(slot.val_offset as usize, slot.key_len)
                    .remain_key[..],
            ]
            .concat();
            if !last_key.is_empty() {
                assert!(last_key <= current_key, "Keys are not sorted correctly.");
            }
            last_key = current_key;
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_merge() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert!(page.upsert_with_merge(&key, &value, update_fn).is_ok());
        }

        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key).unwrap(), value);
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_merge_and_order() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert!(page.upsert_with_merge(&key, &value, update_fn).is_ok());
        }

        keys_and_values.sort_by(|a, b| a.0.cmp(&b.0));

        for (key, expected_value) in keys_and_values.iter() {
            assert_eq!(page.get(key).unwrap(), expected_value.clone());
        }

        let mut last_key = vec![];
        for i in 0..page.decode_shortkey_header().slot_num {
            let slot = page.decode_shortkey_slot(i);
            let current_key = [
                &slot.key_prefix[..],
                &page
                    .decode_shortkey_value(slot.val_offset as usize, slot.key_len)
                    .remain_key[..],
            ]
            .concat();
            if !last_key.is_empty() {
                assert!(last_key <= current_key, "Keys are not sorted correctly.");
            }
            last_key = current_key;
        }
    }

    #[test]
    fn test_order_preservation_with_merge() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"delta", b"alpha", b"echo", b"bravo", b"charlie"];

        for key in keys.iter() {
            page.upsert_with_merge(*key, b"value", update_fn).unwrap();
        }

        let mut retrieved_keys = vec![];
        for i in 0..keys.len() {
            let key = page.decode_shortkey_slot(i as u32);
            // Convert the key_prefix to a String and trim null characters
            let key_string = String::from_utf8_lossy(&key.key_prefix)
                .trim_end_matches('\0')
                .to_string();
            retrieved_keys.push(key_string);
        }

        // Assert that the keys are retrieved in the expected order
        assert_eq!(
            retrieved_keys,
            vec!["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }

    #[test]
    fn test_multiple_operations() {
        let mut page = <Page as ShortKeyPage>::new();

        // Initial inserts
        let key1 = b"key1";
        let value1 = b"value1";
        let key2 = b"key2";
        let value2 = b"value2";
        let key3 = b"key3";
        let value3 = b"value3";

        assert!(page.insert(key1, value1).is_ok(), "Failed to insert key1");
        assert!(page.insert(key2, value2).is_ok(), "Failed to insert key2");
        assert!(page.insert(key3, value3).is_ok(), "Failed to insert key3");

        // Update key2
        let updated_value2 = b"updated_value2";
        assert_eq!(
            page.update_with_return(key2, updated_value2).unwrap(),
            value2.to_vec(),
            "Update for key2 failed"
        );

        // Ensure key2 has been updated
        assert_eq!(
            page.get(key2).unwrap(),
            updated_value2.to_vec(),
            "Key2 should have the updated value"
        );

        // Delete key1
        assert!(page.remove(key1).is_ok(), "Failed to remove key1");

        // Ensure key1 is no longer present
        assert!(
            matches!(page.get(key1), Err(ShortKeyPageError::KeyNotFound)),
            "Key1 should be removed and return None"
        );

        // Check that key3 is still intact
        assert_eq!(
            page.get(key3).unwrap(),
            value3.to_vec(),
            "Key3 should still have the original value"
        );
    }

    #[test]
    fn stress_test_multiple_operations() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];
        let mut inserted_keys = vec![];

        for _ in 0..40 {
            let operation: u8 = rng.gen_range(0..4);
            let key = random_string(100);
            let value = random_string(20);

            match operation {
                0 => {
                    // Insert
                    if page.insert(&key, &value).is_ok() {
                        keys_and_values.push((key.clone(), value.clone()));
                        inserted_keys.push(key);
                    }
                }
                1 => {
                    // Update
                    if !inserted_keys.is_empty() {
                        let key_to_update = &inserted_keys[rng.gen_range(0..inserted_keys.len())];
                        let new_value = random_string(20);
                        if page.update_with_return(&key_to_update, &new_value).is_ok() {
                            keys_and_values.retain(|(k, _)| k != key_to_update);
                            keys_and_values.push((key_to_update.clone(), new_value.clone()));
                        }
                    }
                }
                2 => {
                    // Get
                    if !inserted_keys.is_empty() {
                        let key_to_get = &inserted_keys[rng.gen_range(0..inserted_keys.len())];
                        let expected_value = keys_and_values
                            .iter()
                            .find(|(k, _)| k == key_to_get)
                            .map(|(_, v)| v.clone());
                        assert_eq!(page.get(key_to_get).unwrap(), expected_value.unwrap());
                    }
                }
                3 => {
                    // Remove
                    if !inserted_keys.is_empty() {
                        let key_to_remove =
                            inserted_keys.remove(rng.gen_range(0..inserted_keys.len()));
                        let expected_value = keys_and_values
                            .iter()
                            .find(|(k, _)| *k == key_to_remove)
                            .map(|(_, v)| v.clone());
                        assert!(page.remove(&key_to_remove).is_ok());
                        keys_and_values.retain(|(k, _)| *k != key_to_remove);
                    }
                }
                _ => {}
            }
        }

        // Verify all remaining keys and values
        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key).unwrap(), value);
        }
    }
}
*/
