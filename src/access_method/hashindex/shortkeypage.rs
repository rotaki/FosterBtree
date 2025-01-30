use std::{cmp::Ordering, mem::size_of};

use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
const PAGE_HEADER_SIZE: usize = 0;

pub const SHORT_KEY_PAGE_HEADER_SIZE: usize = size_of::<ShortKeyHeader>(); // 4 + 4 + 2 + 2 = 12
pub const SHORT_KEY_SLOT_SIZE: usize = size_of::<ShortKeySlot>(); // 2 + 8 + 2 = 12

pub trait ShortKeyPage {
    fn new() -> Self;
    fn init(&mut self);

    /// Insert a new key-value pair into the index.
    /// If the key already exists, it will return an error.
    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), ShortKeyPageError>;

    /// Update the value of an existing key.
    /// If the key does not exist, it will return an error.
    fn update(&mut self, key: &[u8], val: &[u8]) -> Result<Vec<u8>, ShortKeyPageError>;
    fn update_at_slot(&mut self, slot_id: u16, val: &[u8]) -> Result<(), ShortKeyPageError>;

    /// Upsert a key-value pair into the index.
    /// If the key already exists, it will update the value.
    /// If the key does not exist, it will insert a new key-value pair.
    fn upsert(&mut self, key: &[u8], val: &[u8]) -> (bool, Option<Vec<u8>>); // return (success, old_value)

    /// Upsert with a custom merge function.
    /// If the key already exists, it will update the value with the merge function.
    /// If the key does not exist, it will insert a new key-value pair.
    fn upsert_with_merge<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> (bool, Option<Vec<u8>>)
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>;

    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>>;

    fn write_values(&mut self, start_offset: usize, values: &[u8]);

    fn adjust_offsets_and_collect_values(&mut self) -> Vec<u8>;
    fn compact(&mut self) -> Result<(), ShortKeyPageError>;

    fn encode_shortkey_header(&mut self, header: &ShortKeyHeader);
    fn decode_shortkey_header(&self) -> ShortKeyHeader;

    fn get_next_page_id(&self) -> PageId;
    fn set_next_page_id(&mut self, next_page_id: PageId);
    fn get_next_page(&self) -> PageId;
    fn set_next_page(&mut self, next_page_id: PageId, next_frame_id: u32);

    fn get_next_frame_id(&self) -> u32;
    fn set_next_frame_id(&mut self, next_frame_id: u32);
    fn get_next_frame(&self) -> u32;
    fn set_next_frame(&mut self, next_frame_id: u32);

    fn encode_shortkey_slot(&mut self, index: u16, slot: &ShortKeySlot);
    fn decode_shortkey_slot(&self, index: u16) -> ShortKeySlot;
    fn remove_shortkey_slot(&mut self, index: u16);

    fn encode_shortkey_value(&mut self, offset: usize, entry: &ShortKeyValue);
    fn encode_shortkey_value_sample(&mut self, offset: usize, entry: &ShortKeyValueSample);

    fn decode_shortkey_value(&self, offset: usize, remain_key_len: u16) -> ShortKeyValue;
    fn decode_shortkey_value_by_id(&self, slot_id: u16) -> ShortKeyValue;

    fn search_slot(&self, key: &[u8]) -> (bool, u16) {
        self.binary_search(|slot_id| self.compare_key(key, slot_id))
        // self.interpolation_search(key)
    }

    fn compare_key(&self, key: &[u8], slot_id: u16) -> Ordering;
    fn binary_search<F>(&self, f: F) -> (bool, u16)
    // return (found, index)
    where
        F: Fn(u16) -> Ordering;

    fn key_to_u64(key: &[u8]) -> u64; // Convert key to a numeric value for interpolation search
    fn interpolation_search(&self, key: &[u8]) -> (bool, u16);

    fn is_exist(&self, key: &[u8]) -> (Option<u16>, usize); // return (Option(slot_id), vals_len)

    fn slot_end_offset(&self) -> usize;
    fn num_slots(&self) -> u16;
    fn total_free_spcae(&self) -> usize;
    fn get_free_space(&self) -> usize;
}

// define error type
#[derive(Debug)]
pub enum ShortKeyPageError {
    KeyExists,
    KeyNotFound,
    OutOfSpace,
}

#[derive(Debug, Clone)]
pub struct ShortKeyHeader {
    next_page_id: PageId,  // u32
    next_frame_id: u32,    // u32
    slot_num: u16,         // u16
    val_start_offset: u16, // u16
}

#[derive(Debug, Clone)]
pub struct ShortKeySlot {
    pub key_len: u16,        // u16 (key length)
    pub key_prefix: [u8; 8], // 8 bytes (fixed size for the key prefix)
    pub val_offset: u16,     // u16
}

#[derive(Debug, Clone)]
pub struct ShortKeyValue {
    pub remain_key: Vec<u8>, // dynamic size (remain part of actual key), if key_len > 8
    pub vals_len: u16,       // u16
    pub vals: Vec<u8>, // vector of vals (variable size), decode in the top of the page (HashTable)
}

impl ShortKeyValue {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.remain_key.len() + 2 + self.vals.len());

        encoded.extend_from_slice(&self.remain_key);
        encoded.extend_from_slice(&self.vals_len.to_le_bytes());
        encoded.extend_from_slice(&self.vals);

        encoded
    }
}

pub struct ShortKeyValueSample<'a> {
    pub remain_key: &'a [u8],
    pub vals_len: u16,
    pub vals: &'a [u8],
}

impl ShortKeyPage for Page {
    fn new() -> Self {
        let mut page = Page::new_empty();
        let header = ShortKeyHeader {
            next_page_id: 0,
            next_frame_id: u32::MAX,
            slot_num: 0,
            val_start_offset: AVAILABLE_PAGE_SIZE as u16,
        };
        Self::encode_shortkey_header(&mut page, &header);
        page
    }

    fn init(&mut self) {
        let header = ShortKeyHeader {
            next_page_id: 0,
            next_frame_id: u32::MAX,
            slot_num: 0,
            val_start_offset: AVAILABLE_PAGE_SIZE as u16,
        };
        Self::encode_shortkey_header(self, &header);
    }

    fn slot_end_offset(&self) -> usize {
        PAGE_HEADER_SIZE
            + SHORT_KEY_PAGE_HEADER_SIZE
            + SHORT_KEY_SLOT_SIZE * self.decode_shortkey_header().slot_num as usize
    }

    fn compare_key(&self, key: &[u8], slot_id: u16) -> Ordering {
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
    fn binary_search<F>(&self, f: F) -> (bool, u16)
    where
        F: Fn(u16) -> Ordering,
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
    fn interpolation_search(&self, key: &[u8]) -> (bool, u16) {
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

            let pos = pos as u16;
            let pos_value = Self::key_to_u64(&self.decode_shortkey_slot(pos).key_prefix);

            match pos_value.cmp(&key_value) {
                Ordering::Equal => return (true, pos),
                Ordering::Less => low = pos + 1,
                Ordering::Greater => high = pos,
            }
        }

        (false, low)
    }
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ShortKeyPageError> {
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        // let (found, index) = self.interpolation_search(key);
        let (found, index) = self.search_slot(key);

        if found {
            return Err(ShortKeyPageError::KeyExists);
        }

        let mut header = self.decode_shortkey_header();
        let remain_key_len = key.len().saturating_sub(8);
        let required_space = SHORT_KEY_SLOT_SIZE + (remain_key_len + 2 + value.len());
        if required_space > header.val_start_offset as usize - self.slot_end_offset() {
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

        let new_val_offset = header.val_start_offset as usize - (value.len() + 2 + remain_key_len);

        let new_slot = ShortKeySlot {
            key_len: key.len() as u16,
            key_prefix: {
                let mut prefix = [0u8; 8];
                let copy_len = std::cmp::min(8, key.len());
                prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                prefix
            },
            val_offset: new_val_offset as u16,
        };
        self.encode_shortkey_slot(index, &new_slot);

        /*
        let remain_key = if key.len() > 8 {
            key[8..].to_vec()
        } else {
            Vec::new()
        };

        let new_value_entry = ShortKeyValue {
            remain_key,
            vals_len: value.len() as u16,
            vals: value.to_vec(),
        };
        */
        let new_value_entry = ShortKeyValueSample {
            remain_key: &key[key.len().min(8)..],
            vals_len: value.len() as u16,
            vals: value,
        };
        self.encode_shortkey_value_sample(new_val_offset, &new_value_entry);

        header.slot_num += 1;
        header.val_start_offset = new_val_offset as u16;
        self.encode_shortkey_header(&header);

        Ok(())
    }

    // when update, if the new value is bigger so OutofSpace, then remove the slot
    fn update(&mut self, key: &[u8], val: &[u8]) -> Result<Vec<u8>, ShortKeyPageError> {
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
            old_value_entry.vals_len = val.len() as u16;
            self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
            Ok(old_vals)
        } else {
            let remain_key_len = key.len().saturating_sub(8);
            let required_space = remain_key_len + val.len() + 2;
            if required_space
                > self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
            {
                self.remove_shortkey_slot(index);
                return Err(ShortKeyPageError::OutOfSpace);
            }

            let new_val_offset = self.decode_shortkey_header().val_start_offset as usize
                - (val.len() + 2 + remain_key_len);
            let new_value_entry = ShortKeyValue {
                remain_key: old_value_entry.remain_key,
                vals_len: val.len() as u16,
                vals: val.to_vec(),
            };

            slot.val_offset = new_val_offset as u16;
            self.encode_shortkey_slot(index, &slot);

            self.encode_shortkey_value(new_val_offset, &new_value_entry);
            let mut header = self.decode_shortkey_header();
            header.val_start_offset = new_val_offset as u16;
            self.encode_shortkey_header(&header);

            Ok(old_value_entry.vals)
        }
    }

    fn update_at_slot(&mut self, slot_id: u16, val: &[u8]) -> Result<(), ShortKeyPageError> {
        let slot = self.decode_shortkey_slot(slot_id);
        let mut old_value_entry =
            self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

        if val.len() <= old_value_entry.vals.len() {
            let _old_vals = std::mem::replace(&mut old_value_entry.vals, val.to_vec());
            old_value_entry.vals_len = val.len() as u16;
            self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
            Ok(())
        } else {
            let remain_key_len = slot.key_len.saturating_sub(8) as usize;
            let required_space = remain_key_len + val.len() + 2;
            if required_space
                > self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
            {
                self.remove_shortkey_slot(slot_id);
                return Err(ShortKeyPageError::OutOfSpace);
            }

            let new_val_offset = self.decode_shortkey_header().val_start_offset as usize
                - (val.len() + 2 + remain_key_len);
            let new_value_entry = ShortKeyValue {
                remain_key: old_value_entry.remain_key,
                vals_len: val.len() as u16,
                vals: val.to_vec(),
            };

            let mut slot = self.decode_shortkey_slot(slot_id);
            slot.val_offset = new_val_offset as u16;
            self.encode_shortkey_slot(slot_id, &slot);

            self.encode_shortkey_value(new_val_offset, &new_value_entry);
            let mut header = self.decode_shortkey_header();
            header.val_start_offset = new_val_offset as u16;
            self.encode_shortkey_header(&header);

            Ok(())
        }
    }

    fn upsert(&mut self, key: &[u8], value: &[u8]) -> (bool, Option<Vec<u8>>) {
        let mut header = self.decode_shortkey_header();
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        // let (found, index) = self.interpolation_search(key);
        let (found, index) = self.search_slot(key);

        if found {
            let mut slot = self.decode_shortkey_slot(index);
            let mut old_value_entry =
                self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

            if value.len() <= old_value_entry.vals.len() {
                let old_vals = std::mem::replace(&mut old_value_entry.vals, value.to_vec());
                old_value_entry.vals_len = value.len() as u16;
                self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
                (true, Some(old_vals))
            } else {
                let remain_key_len = key.len().saturating_sub(8);
                let required_space = remain_key_len + value.len() + 2;
                if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                    self.remove_shortkey_slot(index);
                    return (false, Some(old_value_entry.vals.to_vec()));
                }

                let new_val_offset =
                    header.val_start_offset as usize - (value.len() + 2 + remain_key_len);
                let new_value_entry = ShortKeyValue {
                    remain_key: old_value_entry.remain_key,
                    vals_len: value.len() as u16,
                    vals: value.to_vec(),
                };

                slot.val_offset = new_val_offset as u16;
                self.encode_shortkey_slot(index, &slot);

                self.encode_shortkey_value(new_val_offset, &new_value_entry);
                header.val_start_offset = new_val_offset as u16;
                self.encode_shortkey_header(&header);

                (true, Some(old_value_entry.vals.to_vec()))
            }
        } else {
            let remain_key_len = key.len().saturating_sub(8);
            let required_space = SHORT_KEY_SLOT_SIZE + remain_key_len + value.len() + 2;
            if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                return (false, None);
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
                header.val_start_offset as usize - (value.len() + 2 + remain_key_len);
            let new_slot = ShortKeySlot {
                key_len: key.len() as u16,
                key_prefix: {
                    let mut prefix = [0u8; 8];
                    let copy_len = std::cmp::min(8, key.len());
                    prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                    prefix
                },
                val_offset: new_val_offset as u16,
            };
            self.encode_shortkey_slot(index, &new_slot);

            let remain_key = if key.len() > 8 {
                key[8..].to_vec()
            } else {
                Vec::new()
            };
            let new_value_entry = ShortKeyValue {
                remain_key,
                vals_len: value.len() as u16,
                vals: value.to_vec(),
            };
            self.encode_shortkey_value(new_val_offset, &new_value_entry);

            header.slot_num += 1;
            header.val_start_offset = new_val_offset as u16;
            self.encode_shortkey_header(&header);

            (true, None)
        }
    }

    fn upsert_with_merge<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> (bool, Option<Vec<u8>>)
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>,
    {
        let mut header = self.decode_shortkey_header();
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        // let (found, index) = self.interpolation_search(key);
        let (found, index) = self.search_slot(key);

        if found {
            let mut slot = self.decode_shortkey_slot(index);
            let mut old_value_entry =
                self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

            let new_value = update_fn(&old_value_entry.vals, value);

            if new_value.len() == old_value_entry.vals.len() {
                let old_vals = std::mem::replace(&mut old_value_entry.vals, new_value);
                self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
                (true, Some(old_vals))
            } else {
                let remain_key_len = key.len().saturating_sub(8);
                let required_space = remain_key_len + new_value.len() + 2;
                if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                    self.remove_shortkey_slot(index);
                    return (false, Some(old_value_entry.vals.to_vec()));
                }

                let new_val_offset =
                    header.val_start_offset as usize - (new_value.len() + 2 + remain_key_len);
                let new_value_entry = ShortKeyValue {
                    remain_key: old_value_entry.remain_key,
                    vals_len: new_value.len() as u16,
                    vals: new_value,
                };

                slot.val_offset = new_val_offset as u16;
                self.encode_shortkey_slot(index, &slot);

                self.encode_shortkey_value(new_val_offset, &new_value_entry);
                header.val_start_offset = new_val_offset as u16;
                self.encode_shortkey_header(&header);

                (true, Some(old_value_entry.vals.to_vec()))
            }
        } else {
            let remain_key_len = key.len().saturating_sub(8);
            let required_space = SHORT_KEY_SLOT_SIZE + remain_key_len + value.len() + 2;
            if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                return (false, None);
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
                header.val_start_offset as usize - (value.len() + 2 + remain_key_len);
            let new_slot = ShortKeySlot {
                key_len: key.len() as u16,
                key_prefix: {
                    let mut prefix = [0u8; 8];
                    let copy_len = std::cmp::min(8, key.len());
                    prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                    prefix
                },
                val_offset: new_val_offset as u16,
            };
            self.encode_shortkey_slot(index, &new_slot);

            let remain_key = if key.len() > 8 {
                key[8..].to_vec()
            } else {
                Vec::new()
            };
            let new_value_entry = ShortKeyValue {
                remain_key,
                vals_len: value.len() as u16,
                vals: value.to_vec(),
            };
            self.encode_shortkey_value(new_val_offset, &new_value_entry);

            header.slot_num += 1;
            header.val_start_offset = new_val_offset as u16;
            self.encode_shortkey_header(&header);

            (true, None)
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        // let (found, index) = self.interpolation_search(key);
        let (found, index) = self.search_slot(key);

        if found {
            let slot = self.decode_shortkey_slot(index);
            let value_entry = self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);
            return Some(value_entry.vals.to_vec());
        }
        None
    }

    fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        // let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        // let (found, index) = self.interpolation_search(key);
        let (found, index) = self.search_slot(key);

        if found {
            let slot = self.decode_shortkey_slot(index);
            let value_entry = self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);
            let old_values = value_entry.vals.clone();

            self.remove_shortkey_slot(index);
            Some(old_values)
        } else {
            None
        }
    }

    fn encode_shortkey_header(&mut self, header: &ShortKeyHeader) {
        let offset = PAGE_HEADER_SIZE;
        self[offset..offset + 4].copy_from_slice(&header.next_page_id.to_le_bytes());
        self[offset + 4..offset + 8].copy_from_slice(&header.next_frame_id.to_le_bytes());
        self[offset + 8..offset + 10].copy_from_slice(&header.slot_num.to_le_bytes());
        self[offset + 10..offset + 12].copy_from_slice(&header.val_start_offset.to_le_bytes());
    }

    fn decode_shortkey_header(&self) -> ShortKeyHeader {
        let offset = PAGE_HEADER_SIZE;
        let next_page_id = u32::from_le_bytes(
            self[offset..offset + 4]
                .try_into()
                .expect("Invalid slice length"),
        );
        let next_frame_id = u32::from_le_bytes(
            self[offset + 4..offset + 8]
                .try_into()
                .expect("Invalid slice length"),
        );
        let slot_num = u16::from_le_bytes(
            self[offset + 8..offset + 10]
                .try_into()
                .expect("Invalid slice length"),
        );
        let val_start_offset = u16::from_le_bytes(
            self[offset + 10..offset + 12]
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

    fn get_next_page(&self) -> PageId {
        self.decode_shortkey_header().next_page_id
    }

    fn set_next_page(&mut self, next_page_id: PageId, next_frame_id: u32) {
        let mut header = self.decode_shortkey_header();
        header.next_page_id = next_page_id;
        header.next_frame_id = next_frame_id;
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

    fn get_next_frame(&self) -> u32 {
        self.decode_shortkey_header().next_frame_id
    }

    fn set_next_frame(&mut self, next_frame_id: u32) {
        let mut header = self.decode_shortkey_header();
        header.next_frame_id = next_frame_id;
        self.encode_shortkey_header(&header);
    }

    fn encode_shortkey_slot(&mut self, index: u16, slot: &ShortKeySlot) {
        let offset =
            PAGE_HEADER_SIZE + SHORT_KEY_PAGE_HEADER_SIZE + index as usize * SHORT_KEY_SLOT_SIZE;

        self[offset..offset + 2].copy_from_slice(&slot.key_len.to_le_bytes());
        self[offset + 2..offset + 10].copy_from_slice(&slot.key_prefix);
        self[offset + 10..offset + 12].copy_from_slice(&slot.val_offset.to_le_bytes());
    }

    fn decode_shortkey_slot(&self, index: u16) -> ShortKeySlot {
        let offset =
            PAGE_HEADER_SIZE + SHORT_KEY_PAGE_HEADER_SIZE + index as usize * SHORT_KEY_SLOT_SIZE;

        let key_len = u16::from_le_bytes([self[offset], self[offset + 1]]);
        let mut key_prefix = [0u8; 8];
        key_prefix.copy_from_slice(&self[offset + 2..offset + 10]);
        let val_offset = u16::from_le_bytes([self[offset + 10], self[offset + 11]]);

        ShortKeySlot {
            key_len,
            key_prefix,
            val_offset,
        }
    }

    fn remove_shortkey_slot(&mut self, index: u16) {
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
        self[offset + entry.remain_key.len()..offset + entry.remain_key.len() + 2]
            .copy_from_slice(&entry.vals_len.to_le_bytes());
        self[offset + entry.remain_key.len() + 2
            ..offset + entry.remain_key.len() + 2 + entry.vals.len()]
            .copy_from_slice(&entry.vals);
    }

    fn encode_shortkey_value_sample(&mut self, offset: usize, entry: &ShortKeyValueSample) {
        self[offset..offset + entry.remain_key.len()].copy_from_slice(entry.remain_key);
        self[offset + entry.remain_key.len()..offset + entry.remain_key.len() + 2]
            .copy_from_slice(&entry.vals_len.to_le_bytes());
        self[offset + entry.remain_key.len() + 2
            ..offset + entry.remain_key.len() + 2 + entry.vals.len()]
            .copy_from_slice(entry.vals);
    }

    fn decode_shortkey_value(&self, offset: usize, key_len: u16) -> ShortKeyValue {
        let remain_key_len = if key_len > 8 { key_len as usize - 8 } else { 0 };

        let remain_key = if remain_key_len > 0 {
            self[offset..offset + remain_key_len].to_vec()
        } else {
            Vec::new()
        };

        let vals_len_start = offset + remain_key_len;

        let vals_len = u16::from_le_bytes(
            self[vals_len_start..vals_len_start + 2]
                .try_into()
                .expect("Invalid slice length"),
        ) as usize;

        let vals_start = vals_len_start + 2;
        let vals = self[vals_start..vals_start + vals_len].to_vec();

        ShortKeyValue {
            remain_key,
            vals_len: vals_len as u16,
            vals,
        }
    }

    fn decode_shortkey_value_by_id(&self, slot_id: u16) -> ShortKeyValue {
        let slot = self.decode_shortkey_slot(slot_id);
        self.decode_shortkey_value(slot.val_offset as usize, slot.key_len)
    }

    fn key_to_u64(key: &[u8]) -> u64 {
        let mut key_array = [0u8; 8];
        let copy_len = std::cmp::min(8, key.len());
        key_array[..copy_len].copy_from_slice(&key[..copy_len]);
        u64::from_be_bytes(key_array)
    }

    fn is_exist(&self, key: &[u8]) -> (Option<u16>, usize) {
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

    fn num_slots(&self) -> u16 {
        self.decode_shortkey_header().slot_num
    }

    fn total_free_spcae(&self) -> usize {
        self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
    }

    fn get_free_space(&self) -> usize {
        self.decode_shortkey_header().val_start_offset as usize - self.slot_end_offset()
    }

    fn write_values(&mut self, start_offset: usize, values: &[u8]) {
        let end_offset = start_offset + values.len();
        self[start_offset..end_offset].copy_from_slice(values);
    }

    fn adjust_offsets_and_collect_values(&mut self) -> Vec<u8> {
        let mut new_values_block = Vec::new();
        let mut current_offset = AVAILABLE_PAGE_SIZE; // Start from the end of the page

        let mut header = self.decode_shortkey_header();

        for i in 0..header.slot_num {
            let mut slot = self.decode_shortkey_slot(i);
            let remain_key_len = if slot.key_len > 8 {
                slot.key_len - 8
            } else {
                0
            };
            let value_entry = self.decode_shortkey_value(slot.val_offset as usize, remain_key_len);

            // Calculate new offset
            let value_size = value_entry.vals.len() + 2 + value_entry.remain_key.len();
            current_offset -= value_size;

            // Update the slot with the new offset
            slot.val_offset = current_offset as u16;
            self.encode_shortkey_slot(i, &slot);

            // Use the encode function to get the byte representation of the value entry
            let value_bytes = value_entry.encode();

            // Insert the value_bytes at the beginning of the new_values_block
            new_values_block.splice(0..0, value_bytes);
        }

        assert_eq!(current_offset, AVAILABLE_PAGE_SIZE - new_values_block.len());
        header.val_start_offset = current_offset as u16;
        self.encode_shortkey_header(&header);

        new_values_block
    }

    fn compact(&mut self) -> Result<(), ShortKeyPageError> {
        let new_values_block = self.adjust_offsets_and_collect_values();
        self.write_values(
            AVAILABLE_PAGE_SIZE - new_values_block.len(),
            &new_values_block,
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::random::small_thread_rng;

    use super::*;
    use rand::Rng;

    fn update_fn(old_value: &[u8], new_value: &[u8]) -> Vec<u8> {
        old_value.iter().chain(new_value.iter()).copied().collect()
    }

    fn random_string(length: usize) -> Vec<u8> {
        small_thread_rng()
            .sample_iter(&rand::distr::Alphanumeric)
            .take(length)
            .collect()
    }

    #[test]
    fn test_upsert_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_123";
        let value = b"value_123";

        // Test upsertion of a new key
        assert_eq!(page.upsert(key, value), (true, None));

        // Verify the upserted key and value
        let retrieved_value = page.get(key);
        assert_eq!(retrieved_value, Some(value.to_vec()));
    }

    #[test]
    fn test_upsert_duplicate_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_456";
        let value1 = b"value_456";
        let value2 = b"new_value_456";

        // Insert the key initially
        // assert!(page.upsert(key, value1).is_none());
        assert_eq!(page.upsert(key, value1), (true, None));

        // Insert the same key with a new value
        let old_value = page.upsert(key, value2);
        assert_eq!(old_value, (true, Some(value1.to_vec())));

        // Verify the updated value
        let retrieved_value = page.get(key);
        assert_eq!(retrieved_value, Some(value2.to_vec()));
    }

    #[test]
    fn test_upsert_out_of_space() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_789";
        let value = vec![0u8; AVAILABLE_PAGE_SIZE]; // Unrealistically large value to simulate out-of-space

        // Attempt to upsert a value that's too large for the page
        assert_eq!(page.upsert(key, &value), (false, None));

        // Ensure the key was not upserted
        let retrieved_value = page.get(key);
        assert!(retrieved_value.is_none());
    }

    #[test]
    fn test_upsert_multiple_keys_ordering() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys = [
            (b"alpha", b"value_alpha"),
            (b"gamma", b"value_gamma"),
            (b"betaa", b"value_betaa"),
        ];

        // Insert keys in a specific order
        for (key, value) in keys.iter() {
            page.upsert(*key, *value);
        }

        // Check if keys are retrieved in sorted order
        assert_eq!(page.get(b"alpha"), Some(b"value_alpha".to_vec()));
        assert_eq!(page.get(b"betaa"), Some(b"value_betaa".to_vec()));
        assert_eq!(page.get(b"gamma"), Some(b"value_gamma".to_vec()));
    }

    #[test]
    fn test_boundary_key_upserts() {
        let mut page = <Page as ShortKeyPage>::new();
        let min_key = b"aaaaaaa";
        let max_key = b"zzzzzzz";
        let value = b"value";

        assert_eq!(page.upsert(min_key, value), (true, None));
        assert_eq!(page.upsert(max_key, value), (true, None));

        assert_eq!(page.get(min_key), Some(value.to_vec()));
        assert_eq!(page.get(max_key), Some(value.to_vec()));
    }

    #[test]
    fn test_consecutive_upserts_deletes() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3"];

        // Insert keys
        for key in &keys {
            assert_eq!(page.upsert(key, b"value"), (true, None));
        }

        // Delete keys
        for key in &keys {
            assert!(page.remove(key).is_some());
        }

        // Check deletions
        for key in &keys {
            assert!(page.get(key).is_none());
        }

        // Re-upsert same keys
        for key in &keys {
            assert_eq!(page.upsert(key, b"new_value"), (true, None));
        }

        // Verify re-upsertions
        for key in &keys {
            assert_eq!(page.get(key), Some(b"new_value".to_vec()));
        }
    }

    #[test]
    fn test_updates_with_varying_value_sizes() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let small_value = b"small";
        let large_value = b"this_is_a_much_larger_value_than_before";

        assert_eq!(page.upsert(key, small_value), (true, None));
        assert_eq!(
            page.upsert(key, large_value),
            (true, Some(small_value.to_vec()))
        );
        assert_eq!(page.get(key), Some(large_value.to_vec()));

        // Update back to a smaller value
        assert_eq!(
            page.upsert(key, small_value),
            (true, Some(large_value.to_vec()))
        );
        assert_eq!(page.get(key), Some(small_value.to_vec()));
    }

    #[test]
    fn stress_test_random_keys_and_values() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = small_thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.random_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.random_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(page.upsert(&key, &value), (true, None));
        }

        // Verify all keys and values
        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key), Some(value));
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_order() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = small_thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.random_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.random_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(page.upsert(&key, &value), (true, None));
        }

        // Sort keys_and_values by keys to check the order after upsertion
        keys_and_values.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify all keys and values in sorted order
        for (key, expected_value) in keys_and_values.iter() {
            assert_eq!(page.get(key), Some(expected_value.clone()));
        }

        // Verify the order of keys directly from the page
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
    fn test_order_preservation() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"delta", b"alpha", b"echo", b"bravo", b"charlie"];

        for key in keys.iter() {
            page.upsert(key, b"value");
        }

        let mut retrieved_keys = vec![];
        for i in 0..keys.len() {
            let key = page.decode_shortkey_slot(i as u16);
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
    fn test_upsert_with_merge_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"new_key";
        let value = b"value";

        let (success, old_value) = page.upsert_with_merge(key, value, update_fn);
        assert!(success);
        assert_eq!(old_value, None);
        assert_eq!(page.get(key), Some(value.to_vec()));
    }

    #[test]
    fn test_upsert_with_merge_update_key1() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"update_key";
        let value1 = b"value1";
        let value2 = b"value2";

        page.upsert(key, value1);
        let (success, old_value) = page.upsert_with_merge(key, value2, update_fn);
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(update_fn(value1, value2)));
    }

    #[test]
    fn test_upsert_with_merge_update_key2() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"update_key_diff_size";
        let value1 = b"value1";
        let value2 = b"value2_longer";

        page.upsert(key, value1);
        let (success, old_value) = page.upsert_with_merge(key, value2, update_fn);
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(update_fn(value1, value2)));
    }

    #[test]
    #[ignore]
    fn test_upsert_with_merge_out_of_space() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        // key.len() <= 8, so only - 2 for the value length
        let large_value =
            vec![0u8; AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE - SHORT_KEY_SLOT_SIZE - 2];

        // Fill the page almost completely
        let (success, _) = page.upsert(key, &large_value);
        assert!(success);

        // Try to upsert another value which should fail due to lack of space
        let key2 = b"key2";
        let value2 = b"value2";
        let (success, _) = page.upsert_with_merge(key2, value2, update_fn);
        assert!(!success);
        assert_eq!(page.get(key2), None);
    }

    #[test]
    fn test_upsert_with_merge_update_existing_value() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"existing_key";
        let value1 = b"value1";
        let value2 = b"value2";

        page.upsert(key, value1);
        let (success, old_value) = page.upsert_with_merge(key, value2, |_, new| new.to_vec());
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(value2.to_vec()));
    }

    #[test]
    fn test_upsert_with_merge_key_too_large() {
        let mut page = <Page as ShortKeyPage>::new();
        let large_key = vec![0u8; AVAILABLE_PAGE_SIZE];
        let value = b"value";

        let (success, _) = page.upsert_with_merge(&large_key, value, update_fn);
        assert!(!success);
        assert_eq!(page.get(&large_key), None);
    }

    #[test]
    fn test_upsert_with_merge_value_too_large() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let large_value = vec![0u8; AVAILABLE_PAGE_SIZE];

        let (success, _) = page.upsert_with_merge(key, &large_value, update_fn);
        assert!(!success);
        assert_eq!(page.get(key), None);
    }

    #[test]
    fn test_upsert_with_merge_value_update_replaces_old_value() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let value1 = b"old_value";
        let value2 = b"new_value";

        page.upsert(key, value1);
        let (success, old_value) = page.upsert_with_merge(key, value2, |_, new| new.to_vec());
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(value2.to_vec()));
    }

    #[test]
    fn stress_test_random_keys_and_values_with_merge() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = small_thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.random_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.random_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(
                page.upsert_with_merge(&key, &value, update_fn),
                (true, None)
            );
        }

        // Verify all keys and values
        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key), Some(value));
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_merge_and_order() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = small_thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.random_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.random_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(
                page.upsert_with_merge(&key, &value, update_fn),
                (true, None)
            );
        }

        // Sort keys_and_values by keys to check the order after upsertion
        keys_and_values.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify all keys and values in sorted order
        for (key, expected_value) in keys_and_values.iter() {
            assert_eq!(page.get(key), Some(expected_value.clone()));
        }

        // Verify the order of keys directly from the page
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
            page.upsert_with_merge(key, b"value", update_fn);
        }

        let mut retrieved_keys = vec![];
        for i in 0..keys.len() {
            let key = page.decode_shortkey_slot(i as u16);
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
    fn test_insert_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let value = b"test_value";

        // Test insertion of a new key
        assert!(
            page.insert(key, value).is_ok(),
            "Insertion should be successful"
        );

        // Verify the inserted key and value
        let retrieved_value = page.get(key);
        assert_eq!(
            retrieved_value,
            Some(value.to_vec()),
            "Retrieved value should match the inserted value"
        );
    }

    #[test]
    fn test_update_existing_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let initial_value = b"initial_value";
        let updated_value = b"updated_value";

        // Insert the initial value
        page.insert(key, initial_value).unwrap();

        // Update the existing key
        let old_value = page.update(key, updated_value);
        assert_eq!(
            old_value.unwrap(),
            initial_value.to_vec(),
            "Old value should be returned on update"
        );

        // Verify the updated value
        let retrieved_value = page.get(key);
        assert_eq!(
            retrieved_value,
            Some(updated_value.to_vec()),
            "Retrieved value should match the updated value"
        );
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let value1 = b"first_value";
        let value2 = b"second_value";

        // Insert the first value with the key
        assert!(
            page.insert(key, value1).is_ok(),
            "First insertion should be successful"
        );

        // Attempt to insert another value with the same key
        let result = page.insert(key, value2);

        // Check that the second insertion fails due to duplicate key
        assert!(
            matches!(result, Err(ShortKeyPageError::KeyExists)),
            "Insertion of a duplicate key should return a KeyExists error"
        );

        // Verify that the value in the page is still the first one
        let retrieved_value = page.get(key);
        assert_eq!(
            retrieved_value,
            Some(value1.to_vec()),
            "The value associated with the key should remain unchanged after a failed insertion attempt"
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
            page.update(key2, updated_value2).unwrap(),
            value2.to_vec(),
            "Update for key2 failed"
        );

        // Ensure key2 has been updated
        assert_eq!(
            page.get(key2),
            Some(updated_value2.to_vec()),
            "Key2 should have the updated value"
        );

        // Delete key1
        assert_eq!(
            page.remove(key1),
            Some(value1.to_vec()),
            "Failed to remove key1"
        );

        // Ensure key1 is no longer present
        assert!(
            page.get(key1).is_none(),
            "Key1 should be removed and return None"
        );

        // Check that key3 is still intact
        assert_eq!(
            page.get(key3),
            Some(value3.to_vec()),
            "Key3 should still have the original value"
        );
    }

    #[test]
    fn stress_test_multiple_operations() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = small_thread_rng();
        let mut keys_and_values = vec![];
        let mut inserted_keys = vec![];

        for _ in 0..20 {
            let operation: u8 = rng.random_range(0..4);
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
                        let key_to_update =
                            &inserted_keys[rng.random_range(0..inserted_keys.len())];
                        let new_value = random_string(20);
                        if page.update(key_to_update, &new_value).is_ok() {
                            keys_and_values.retain(|(k, _)| k != key_to_update);
                            keys_and_values.push((key_to_update.clone(), new_value.clone()));
                        }
                    }
                }
                2 => {
                    // Get
                    if !inserted_keys.is_empty() {
                        let key_to_get = &inserted_keys[rng.random_range(0..inserted_keys.len())];
                        let expected_value = keys_and_values
                            .iter()
                            .find(|(k, _)| k == key_to_get)
                            .map(|(_, v)| v.clone());
                        assert_eq!(page.get(key_to_get), expected_value);
                    }
                }
                3 => {
                    // Remove
                    if !inserted_keys.is_empty() {
                        let key_to_remove =
                            inserted_keys.remove(rng.random_range(0..inserted_keys.len()));
                        let expected_value = keys_and_values
                            .iter()
                            .find(|(k, _)| *k == key_to_remove)
                            .map(|(_, v)| v.clone());
                        assert_eq!(page.remove(&key_to_remove), expected_value);
                        keys_and_values.retain(|(k, _)| *k != key_to_remove);
                    }
                }
                _ => {}
            }
        }

        // Verify all remaining keys and values
        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key), Some(value));
        }
    }

    #[test]
    #[ignore]
    fn test_compact_after_deletions() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys_and_values = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
            (b"key4", b"value4"),
        ];

        // Insert keys
        for &(key, value) in &keys_and_values {
            assert_eq!(page.upsert(key, value), (true, None));
        }

        // Remove some keys
        assert!(page.remove(b"key2").is_some());
        assert!(page.remove(b"key4").is_some());

        // Compact the page
        assert!(page.compact().is_ok());

        // Verify the remaining keys
        assert_eq!(page.get(b"key1"), Some(b"value1".to_vec()));
        assert!(page.get(b"key2").is_none());
        assert_eq!(page.get(b"key3"), Some(b"value3".to_vec()));
        assert!(page.get(b"key4").is_none());
    }

    #[test]
    #[ignore]
    fn test_compact_on_nearly_full_page() {
        let mut page = <Page as ShortKeyPage>::new();
        let large_value =
            vec![
                0u8;
                (AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE) / 2 - SHORT_KEY_SLOT_SIZE - 2
            ];
        let key1 = b"key1";
        let key2 = b"key2";

        // Insert two large values
        assert_eq!(page.upsert(key1, &large_value), (true, None));
        assert_eq!(page.upsert(key2, &large_value), (true, None));

        // Remove one key and compact
        assert!(page.remove(key1).is_some());
        assert!(page.compact().is_ok());

        // Verify the remaining key
        assert!(page.get(key1).is_none());
        assert_eq!(page.get(key2), Some(large_value.clone()));

        // Insert a new key
        let key3 = b"key3";
        assert_eq!(page.upsert(key3, &large_value), (true, None));
        assert_eq!(page.get(key3), Some(large_value.clone()));
    }

    #[test]
    fn test_compact_empty_page() {
        let mut page = <Page as ShortKeyPage>::new();

        // Compact an empty page
        assert!(page.compact().is_ok());

        // Verify the page is still empty
        assert!(page.get(b"any_key").is_none());
    }

    #[test]
    fn test_compact_after_multiple_operations() {
        let mut page = <Page as ShortKeyPage>::new();
        let key1 = b"key1";
        let key2 = b"key2";
        let key3 = b"key3";
        let value1 = b"value1";
        let value2 = b"value2";
        let value3 = b"value3";

        // Insert keys
        assert_eq!(page.upsert(key1, value1), (true, None));
        assert_eq!(page.upsert(key2, value2), (true, None));

        // Update a key
        assert_eq!(page.upsert(key1, value3), (true, Some(value1.to_vec())));

        // Delete a key
        assert!(page.remove(key2).is_some());

        // Compact the page
        assert!(page.compact().is_ok());

        // Verify remaining keys
        assert_eq!(page.get(key1), Some(value3.to_vec()));
        assert!(page.get(key2).is_none());

        // Insert another key
        assert_eq!(page.upsert(key3, value3), (true, None));
        assert_eq!(page.get(key3), Some(value3.to_vec()));
    }

    #[test]
    #[ignore]
    fn test_compact_with_full_page() {
        let mut page = <Page as ShortKeyPage>::new();
        let key1 = b"key1";
        let key2 = b"key2";
        let key3 = b"key3";
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 4];
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 4];
        let value3 = vec![0u8; AVAILABLE_PAGE_SIZE / 4];

        // Insert three large values
        assert_eq!(page.upsert(key1, &value1), (true, None));
        assert_eq!(page.upsert(key2, &value2), (true, None));
        assert_eq!(page.upsert(key3, &value3), (true, None));

        // Remove one key and compact
        assert!(page.remove(key2).is_some());
        assert!(page.compact().is_ok());

        // Verify remaining keys
        assert_eq!(page.get(key1), Some(value1.clone()));
        assert!(page.get(key2).is_none());
        assert_eq!(page.get(key3), Some(value3.clone()));

        // Try to insert a new key after compaction
        let key4 = b"key4";
        let value4 = vec![0u8; AVAILABLE_PAGE_SIZE / 4];
        assert_eq!(page.upsert(key4, &value4), (true, None));
        assert_eq!(page.get(key4), Some(value4.clone()));
    }
}
