use crate::page::Page;

// Page layout:
// 1 byte: flags (is_root, is_leaf, leftmost, rightmost, has_foster_children)
// 2 byte: active slot count (generally >=2  because of low and high fences)
// 2 byte: free space
// Slotted page layout:
// * slot [offset: u16, key_size: u16, value_size: u16]. The first bit of the offset is used to indicate if the slot is a ghost slot.
//  The slots are sorted based on the key.
// * recs [key: [u8], value: [u8]] // value should be a page id if the page is a non-leaf page, otherwise it should be a value.
// The first slot is the low fence and the last slot is the high fence.

// Assumptions
// * Keys are unique

// [slot0] -- low_fence. If the page is the leftmost page, then the low_fence is offset 0, size 0. Should not be referenced.
// [slotN] -- high_fence. If the page is the rightmost page, then the high_fence is offset 0, size 0. Should not be referenced.

// Words
// * A slot refers to the metadata of a record in the page. It specifies the offset of the record, the size of the key, and the size of the value.
// * A record refers to the key-value pair in the page.

mod slot {
    pub const SLOT_SIZE: usize = 6;

    pub struct Slot {
        offset: u16, // The first bit of the offset is used to indicate if the slot is a ghost slot.
        key_size: u16, // The size of the key
        value_size: u16, // The size of the value
    }

    impl Slot {
        pub fn from_bytes(bytes: [u8; SLOT_SIZE]) -> Self {
            let offset = u16::from_be_bytes([bytes[0], bytes[1]]);
            let key_size = u16::from_be_bytes([bytes[2], bytes[3]]);
            let value_size = u16::from_be_bytes([bytes[4], bytes[5]]);
            Slot {
                offset,
                key_size,
                value_size,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let offset_bytes = self.offset.to_be_bytes();
            let key_size_bytes = self.key_size.to_be_bytes();
            let value_size_bytes = self.value_size.to_be_bytes();
            [
                offset_bytes[0],
                offset_bytes[1],
                key_size_bytes[0],
                key_size_bytes[1],
                value_size_bytes[0],
                value_size_bytes[1],
            ]
        }

        pub fn new(offset: u16, key_size: u16, value_size: u16) -> Self {
            // Always clear the first bit of the offset.
            // i.e. Always assume that the slot is not a ghost slot on new.
            let offset = offset & 0b0111_1111_1111_1111;
            Slot {
                offset,
                key_size,
                value_size,
            }
        }

        pub fn is_ghost(&self) -> bool {
            self.offset & 0b1000_0000_0000_0000 != 0
        }

        pub fn set_ghost(&mut self, is_ghost: bool) {
            if is_ghost {
                self.offset |= 0b1000_0000_0000_0000;
            } else {
                self.offset &= 0b0111_1111_1111_1111;
            }
        }

        pub fn offset(&self) -> u16 {
            self.offset & 0b0111_1111_1111_1111
        }

        pub fn set_offset(&mut self, offset: u16) {
            self.offset = (self.offset & 0b1000_0000_0000_0000) | (offset & 0b0111_1111_1111_1111);
        }

        pub fn key_size(&self) -> u16 {
            self.key_size
        }

        pub fn set_key_size(&mut self, key_size: u16) {
            self.key_size = key_size;
        }

        pub fn value_size(&self) -> u16 {
            self.value_size
        }

        pub fn set_value_size(&mut self, value_size: u16) {
            self.value_size = value_size;
        }
    }
}

use log::trace;
const PAGE_HEADER_SIZE: usize = 5;
use slot::{Slot, SLOT_SIZE};

pub enum BTreeKey<'a> {
    MinusInfty,
    Normal(&'a [u8]),
    PlusInfty,
}

// implement unwrap
impl<'a> BTreeKey<'a> {
    pub fn new(key: &'a [u8]) -> Self {
        BTreeKey::Normal(key)
    }

    #[cfg(any(test, debug_assertions))]
    fn str(key: &'a str) -> Self {
        BTreeKey::Normal(key.as_bytes())
    }

    pub fn unwrap(&self) -> &'a [u8] {
        match self {
            BTreeKey::MinusInfty => panic!("Cannot unwrap MinusInfty"),
            BTreeKey::Normal(key) => key,
            BTreeKey::PlusInfty => panic!("Cannot unwrap PlusInfty"),
        }
    }
}

impl<'a> PartialEq for BTreeKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BTreeKey::MinusInfty, BTreeKey::MinusInfty) => true,
            (BTreeKey::MinusInfty, BTreeKey::Normal(_)) => false,
            (BTreeKey::MinusInfty, BTreeKey::PlusInfty) => false,
            (BTreeKey::Normal(key1), BTreeKey::Normal(key2)) => key1 == key2,
            (BTreeKey::Normal(_), BTreeKey::MinusInfty) => false,
            (BTreeKey::Normal(_), BTreeKey::PlusInfty) => false,
            (BTreeKey::PlusInfty, BTreeKey::MinusInfty) => false,
            (BTreeKey::PlusInfty, BTreeKey::Normal(_)) => false,
            (BTreeKey::PlusInfty, BTreeKey::PlusInfty) => true,
        }
    }
}

impl Eq for BTreeKey<'_> {}

impl<'a> PartialOrd for BTreeKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (BTreeKey::MinusInfty, BTreeKey::MinusInfty) => Some(std::cmp::Ordering::Equal),
            (BTreeKey::MinusInfty, BTreeKey::Normal(_)) => Some(std::cmp::Ordering::Less),
            (BTreeKey::MinusInfty, BTreeKey::PlusInfty) => Some(std::cmp::Ordering::Less),
            (BTreeKey::Normal(_), BTreeKey::MinusInfty) => Some(std::cmp::Ordering::Greater),
            (BTreeKey::Normal(key1), BTreeKey::Normal(key2)) => Some(key1.cmp(key2)),
            (BTreeKey::Normal(_), BTreeKey::PlusInfty) => Some(std::cmp::Ordering::Less),
            (BTreeKey::PlusInfty, BTreeKey::MinusInfty) => Some(std::cmp::Ordering::Greater),
            (BTreeKey::PlusInfty, BTreeKey::Normal(_)) => Some(std::cmp::Ordering::Greater),
            (BTreeKey::PlusInfty, BTreeKey::PlusInfty) => Some(std::cmp::Ordering::Equal),
        }
    }
}

impl Ord for BTreeKey<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::fmt::Debug for BTreeKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BTreeKey::MinusInfty => write!(f, "-inf"),
            BTreeKey::Normal(key) => {
                let str = std::str::from_utf8(key).unwrap();
                write!(f, "{}", str)
            }
            BTreeKey::PlusInfty => write!(f, "+inf"),
        }
    }
}

pub trait FosterBtreePage {
    // Helper functions
    fn get_id(&self) -> u32;
    fn slot_offset(&self, slot_id: u16) -> usize;
    fn slot(&self, slot_id: u16) -> Option<Slot>;
    fn append_slot(&mut self, slot: &Slot);
    fn update_slot(&mut self, slot_id: u16, slot: &Slot);
    fn free_space(&self) -> usize;
    fn shift_records(&mut self, shift_start_offset: u16, shift_size: u16);
    fn linear_search<F>(&self, f: F) -> u16
    where
        F: Fn(BTreeKey) -> bool;
    fn binary_search<F>(&self, f: F) -> u16
    where
        F: Fn(BTreeKey) -> bool;
    fn get_low_fence(&self) -> BTreeKey;
    fn get_high_fence(&self) -> BTreeKey;
    fn range(&self) -> (BTreeKey, BTreeKey);
    fn low_fence_slot_id(&self) -> u16;
    fn high_fence_slot_id(&self) -> u16;
    fn foster_child_slot_id(&self) -> u16;
    fn is_fence(&self, slot_id: u16) -> bool;
    fn compact_space(&mut self);

    // Header operations
    fn is_root(&self) -> bool;
    fn set_root(&mut self, is_root: bool);
    fn is_leaf(&self) -> bool;
    fn set_leaf(&mut self, is_leaf: bool);
    fn is_left_most(&self) -> bool;
    fn set_left_most(&mut self, is_left_most: bool);
    fn is_right_most(&self) -> bool;
    fn set_right_most(&mut self, is_right_most: bool);
    fn has_foster_child(&self) -> bool;
    fn set_foster_child(&mut self, has_foster_child: bool);
    fn active_slot_count(&self) -> u16;
    fn set_active_slot_count(&mut self, active_slot_count: u16);
    fn increment_active_slots(&mut self);
    fn decrement_active_slots(&mut self);
    fn rec_start_offset(&self) -> u16;
    fn set_rec_start_offset(&mut self, rec_start_offset: u16);

    // Page operations
    fn init(&mut self);
    fn init_as_root(&mut self);
    fn is_ghost_slot(&self, slot_id: u16) -> bool;
    fn empty(&self) -> bool;
    fn get_raw_key(&self, slot_id: u16) -> &[u8];
    fn get_btree_key(&self, slot_id: u16) -> BTreeKey;
    fn get_foster_key(&self) -> &[u8];
    fn get_foster_page_id(&self) -> u32;
    fn get_val(&self, slot_id: u16) -> &[u8];
    fn inside_range(&self, key: &BTreeKey) -> bool;
    fn lower_bound_slot_id(&self, key: &BTreeKey) -> u16;
    fn find_slot_id(&self, key: &BTreeKey) -> Option<u16>;
    fn insert_at(&mut self, slot_id: u16, key: &[u8], value: &[u8]);
    fn remove_at(&mut self, slot_id: u16);
    fn insert_low_fence(&mut self, key: &[u8]);
    fn insert_high_fence(&mut self, key: &[u8]);
    fn insert(&mut self, key: &[u8], value: &[u8], make_ghost: bool) -> bool;
    fn mark_ghost(&mut self, key: &[u8]);
    fn remove(&mut self, key: &[u8]);
    fn insert_sorted(&mut self, recs: Vec<(&[u8], &[u8])>) -> bool;
    fn remove_range(&mut self, start: u16, end: u16);

    #[cfg(any(test, debug_assertions))]
    fn run_consistency_checks(&self, include_no_garbage_checks: bool);
    #[cfg(any(test, debug_assertions))]
    fn check_keys_are_sorted(&self);
    #[cfg(any(test, debug_assertions))]
    fn check_fence_slots_exists(&self);
    #[cfg(any(test, debug_assertions))]
    fn check_rec_start_offset_match_slots(&self);
    #[cfg(any(test, debug_assertions))]
    fn check_ideal_space_usage(&self);
    #[cfg(any(test, debug_assertions))]
    fn print_all(&self);
}

impl FosterBtreePage for Page {
    fn is_root(&self) -> bool {
        self[0] & 0b1000_0000 != 0
    }

    fn set_root(&mut self, is_root: bool) {
        if is_root {
            self[0] |= 0b1000_0000;
        } else {
            self[0] &= 0b0111_1111;
        }
    }

    fn is_leaf(&self) -> bool {
        self[0] & 0b0100_0000 != 0
    }

    fn set_leaf(&mut self, is_leaf: bool) {
        if is_leaf {
            self[0] |= 0b0100_0000;
        } else {
            self[0] &= 0b1011_1111;
        }
    }

    fn is_left_most(&self) -> bool {
        self[0] & 0b0010_0000 != 0
    }

    fn set_left_most(&mut self, is_left_most: bool) {
        if is_left_most {
            self[0] |= 0b0010_0000;
        } else {
            self[0] &= 0b1101_1111;
        }
    }

    fn is_right_most(&self) -> bool {
        self[0] & 0b0001_0000 != 0
    }

    fn set_right_most(&mut self, is_right_most: bool) {
        if is_right_most {
            self[0] |= 0b0001_0000;
        } else {
            self[0] &= 0b1110_1111;
        }
    }

    fn has_foster_child(&self) -> bool {
        self[0] & 0b0000_1000 != 0
    }

    fn set_foster_child(&mut self, has_foster_child: bool) {
        if has_foster_child {
            self[0] |= 0b0000_1000;
        } else {
            self[0] &= 0b1111_0111;
        }
    }

    /// The number of active slots in the page.
    /// The low fence and high fence are always present.
    /// Therefore, the active slot count should be at least 2 after the initialization.
    fn active_slot_count(&self) -> u16 {
        u16::from_be_bytes([self[1], self[2]])
    }

    fn set_active_slot_count(&mut self, active_slot_count: u16) {
        let bytes = active_slot_count.to_be_bytes();
        self[1] = bytes[0];
        self[2] = bytes[1];
    }

    fn increment_active_slots(&mut self) {
        let active_slot_count = self.active_slot_count();
        self.set_active_slot_count(active_slot_count + 1);
    }

    fn decrement_active_slots(&mut self) {
        let active_slot_count = self.active_slot_count();
        self.set_active_slot_count(active_slot_count - 1);
    }

    fn rec_start_offset(&self) -> u16 {
        u16::from_be_bytes([self[3], self[4]])
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u16) {
        let bytes = rec_start_offset.to_be_bytes();
        self[3] = bytes[0];
        self[4] = bytes[1];
    }

    fn get_id(&self) -> u32 {
        self.get_id()
    }

    fn slot_offset(&self, slot_id: u16) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }

    fn slot(&self, slot_id: u16) -> Option<Slot> {
        if slot_id < self.active_slot_count() {
            let offset = self.slot_offset(slot_id);
            let slot_bytes: [u8; SLOT_SIZE] = self[offset..offset + SLOT_SIZE].try_into().unwrap();
            Some(Slot::from_bytes(slot_bytes))
        } else {
            None
        }
    }

    /// Append a slot at the end of the active slots.
    /// Increment the active slot count.
    /// The header is also updated to set the rec_start_offset to the minimum of the current rec_start_offset and the slot's offset.
    fn append_slot(&mut self, slot: &Slot) {
        // Increment the active slot count and update the header
        let slot_id = self.active_slot_count();
        self.increment_active_slots();
        // Update the slot (rec_start_offset is updated in the update_slot function)
        self.update_slot(slot_id, &slot);
    }

    /// Update the slot at slot_id.
    /// Panic if the slot_id is out of range.
    /// The header is also updated to set the rec_start_offset to the minimum of the current rec_start_offset and the slot's offset.
    fn update_slot(&mut self, slot_id: u16, slot: &Slot) {
        if slot_id >= self.active_slot_count() {
            panic!("Slot does not exist");
        }
        // Update the slot
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update the header
        let offset = self.rec_start_offset().min(slot.offset());
        self.set_rec_start_offset(offset);
    }

    fn free_space(&self) -> usize {
        let next_slot_offset = self.slot_offset(self.active_slot_count());
        let rec_start_offset = self.rec_start_offset();
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
    // The left offset of rec4 is `rec_start_offset`.
    // The left offset of rec2 is `shift_start_offset`.
    // The size of rec2 is `shift_size`.
    fn shift_records(&mut self, shift_start_offset: u16, shift_size: u16) {
        // Chunks of records to be shifted is in the range of [start..end)
        // The new range is [new_start..new_end)
        trace!(
            "Shifting records. Start offset: {}, size: {}",
            shift_start_offset,
            shift_size
        );
        let start = self.rec_start_offset() as usize;
        let end = shift_start_offset as usize;
        // No need to shift if start >= end OR shift_size == 0
        if start >= end || shift_size == 0 {
            return;
        }
        let data = self[start..end].to_vec();

        let new_start = start + shift_size as usize;
        let new_end = end + shift_size as usize;
        self[new_start..new_end].copy_from_slice(&data);

        // For each slot shifted, update the slot.
        // Shifting includes the ghost slots.
        for slot_id in 0..self.active_slot_count() {
            if let Some(mut slot) = self.slot(slot_id) {
                let current_offset = slot.offset();
                if current_offset < shift_start_offset as u16 {
                    // Update slot.
                    let new_offset = current_offset + shift_size;
                    slot.set_offset(new_offset);
                    self.update_slot(slot_id, &slot);
                }
            } else {
                panic!("Slot should be available");
            }
        }

        // Update the rec_start_offset of the page
        self.set_rec_start_offset(new_start as u16);
    }

    // Find the left-most key where f(key) = true.
    // Assumes that f(key, search_key) is false for all keys to the left of the returned index.
    // [false, false, false, true, true, true]
    //                        ^
    //                        |
    //                        return this index
    // If all keys are false, then return the len (i.e. active_slot_count)
    fn linear_search<F>(&self, f: F) -> u16
    where
        F: Fn(BTreeKey) -> bool,
    {
        for i in 0..self.active_slot_count() {
            let slot_key = self.get_btree_key(i);
            if f(slot_key) {
                return i;
            }
        }
        self.active_slot_count()
    }

    // Find the left-most key where f(key) = true.
    // Assumes that f(key, search_key) is false for all keys to the left of the returned index.
    // [false, false, false, true, true, true]
    //                        ^
    //                        |
    //                        return this index
    // If all keys are false, then return the len (i.e. active_slot_count)
    fn binary_search<F>(&self, f: F) -> u16
    where
        F: Fn(BTreeKey) -> bool,
    {
        let low_fence = self.get_btree_key(self.low_fence_slot_id());
        let mut ng = if !f(low_fence) {
            self.low_fence_slot_id()
        } else {
            return self.low_fence_slot_id();
        };
        let high_fence = self.get_btree_key(self.high_fence_slot_id());
        let mut ok = if f(high_fence) {
            self.high_fence_slot_id()
        } else {
            return self.high_fence_slot_id() + 1; // equals to active_slot_count
        };

        // Invariant: f(ng) = false, f(ok) = true
        while ok - ng > 1 {
            let mid = ng + (ok - ng) / 2;
            let slot_key = self.get_btree_key(mid);
            if f(slot_key) {
                ok = mid;
            } else {
                ng = mid;
            }
        }
        ok
    }

    fn get_low_fence(&self) -> BTreeKey {
        if self.is_left_most() {
            BTreeKey::MinusInfty
        } else {
            let slot = self.slot(self.low_fence_slot_id()).unwrap();
            let offset = slot.offset() as usize;
            let key_size = slot.key_size() as usize;
            BTreeKey::Normal(&self[offset..offset + key_size])
        }
    }

    fn get_high_fence(&self) -> BTreeKey {
        if self.is_right_most() {
            BTreeKey::PlusInfty
        } else {
            let slot = self.slot(self.high_fence_slot_id()).unwrap();
            let offset = slot.offset() as usize;
            let key_size = slot.key_size() as usize;
            BTreeKey::Normal(&self[offset..offset + key_size])
        }
    }

    fn range(&self) -> (BTreeKey, BTreeKey) {
        let low_fence = self.get_low_fence();
        let high_fence = self.get_high_fence();
        (low_fence, high_fence)
    }

    fn is_fence(&self, slot_id: u16) -> bool {
        slot_id == self.low_fence_slot_id() || slot_id == self.high_fence_slot_id()
    }

    fn low_fence_slot_id(&self) -> u16 {
        0
    }

    fn high_fence_slot_id(&self) -> u16 {
        self.active_slot_count() - 1
    }

    fn foster_child_slot_id(&self) -> u16 {
        self.active_slot_count() - 2
    }

    fn compact_space(&mut self) {
        let mut rec_mem_usage = 0;
        for i in 0..self.active_slot_count() {
            if let Some(slot) = self.slot(i) {
                rec_mem_usage += slot.key_size() + slot.value_size();
            }
        }
        let ideal_start_offset = self.len() as u16 - rec_mem_usage;
        let rec_start_offset = self.rec_start_offset();

        if rec_start_offset > ideal_start_offset {
            panic!("corrupted page");
        } else if rec_start_offset == ideal_start_offset {
            // No need to compact
        } else {
            let mut recs = vec![0; rec_mem_usage as usize];
            let mut current_size = 0;

            // Copy the records into a temporary buffer and update the slots
            for i in 0..self.active_slot_count() {
                if let Some(mut slot) = self.slot(i) {
                    let offset = slot.offset() as usize;
                    let key_size = slot.key_size() as usize;
                    let value_size = slot.value_size() as usize;
                    let size = key_size + value_size;
                    current_size += size;

                    // Page       [.....    [                Records                   ]]
                    // Records              [[.............][key2][value2][key1][value1]]
                    //                                       <-----------> size
                    //                                       <-------------------------> current_size
                    //                                      ^
                    //                       <-------------> local_offset
                    //             <-----------------------> global_offset
                    //             <-------> ideal_start_offset

                    let local_offset = rec_mem_usage as usize - current_size;
                    recs[local_offset..local_offset + size]
                        .copy_from_slice(&self[offset..offset + size]);

                    // Update the slot
                    let global_offset = (self.len() - current_size) as u16;
                    slot.set_offset(global_offset);
                    self.update_slot(i, &slot);
                }
            }
            // Copy the records back to the page
            self[ideal_start_offset as usize..].copy_from_slice(&recs);

            // Update the header
            self.set_rec_start_offset(ideal_start_offset);
        }
    }

    fn init(&mut self) {
        // Default is non-root, non-leaf, non-leftmost, non-rightmost, non-foster_children
        self.set_root(false);
        self.set_leaf(false);
        self.set_left_most(false);
        self.set_right_most(false);
        self.set_foster_child(false);
        self.set_active_slot_count(0);
        self.set_rec_start_offset(self.len() as u16);
    }

    fn init_as_root(&mut self) {
        self.set_root(true);
        self.set_leaf(true);
        self.set_left_most(true);
        self.set_right_most(true);
        self.set_foster_child(false);
        self.set_rec_start_offset(self.len() as u16);

        // Insert low fence
        self.insert_low_fence(&[]);
        // Insert high fence
        self.insert_high_fence(&[]);
    }

    fn is_ghost_slot(&self, slot_id: u16) -> bool {
        if let Some(slot) = self.slot(slot_id) {
            slot.is_ghost()
        } else {
            false
        }
    }

    fn empty(&self) -> bool {
        self.active_slot_count() <= 2 // low fence and high fence
    }

    fn get_raw_key(&self, slot_id: u16) -> &[u8] {
        assert!(slot_id < self.active_slot_count());
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        &self[offset..offset + key_size]
    }

    fn get_btree_key(&self, slot_id: u16) -> BTreeKey {
        if slot_id == self.low_fence_slot_id() {
            self.get_low_fence()
        } else if slot_id == self.high_fence_slot_id() {
            self.get_high_fence()
        } else {
            BTreeKey::Normal(self.get_raw_key(slot_id))
        }
    }

    fn get_foster_key(&self) -> &[u8] {
        assert!(self.has_foster_child());
        let foster_slot_id = self.foster_child_slot_id();
        self.get_raw_key(foster_slot_id)
    }

    fn get_foster_page_id(&self) -> u32 {
        assert!(self.has_foster_child());
        let foster_slot_id = self.foster_child_slot_id();
        let slot = self.slot(foster_slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let value_size = slot.value_size() as usize;
        let value = &self[offset + key_size..offset + key_size + value_size];
        let foster_page_id = u32::from_be_bytes([value[0], value[1], value[2], value[3]]);
        foster_page_id
    }

    fn get_val(&self, slot_id: u16) -> &[u8] {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let value_size = slot.value_size() as usize;
        let value = &self[offset + key_size..offset + key_size + value_size];
        value
    }

    fn inside_range(&self, key: &BTreeKey) -> bool {
        let (low_fence, high_fence) = self.range();
        low_fence <= *key && *key < high_fence
    }

    /// Returns the right-most slot_id where the key is less than or equal to the given key.
    /// The returned slot_id satisfies lower_fence_slot_id <= slot_id < high_fence_slot_id.
    ///
    /// key: search key
    ///   It must be in the range of the page. i.e. low_fence <= key < high_fence
    ///
    /// Note: To insert a new key, use this function to find the right-most slot_id where the key
    /// is less than or equal to the given key. Then, shift the slots to the right starting from
    /// slot_id + 1 to the end of the active slots. Finally, insert the new key at slot_id + 1.
    fn lower_bound_slot_id(&self, key: &BTreeKey) -> u16 {
        if !self.inside_range(key) {
            panic!("key is out of the range of the page");
        }
        // Binary search returns the left-most slot_id where the key is greater than the given key.
        let slot_id = self.binary_search(|slot_key| *key < slot_key);
        assert!(self.low_fence_slot_id() + 1 <= slot_id && slot_id <= self.high_fence_slot_id());
        // The right-most slot_id where the key is less than or equal to the given key.
        slot_id - 1
    }

    /// Returns the slot_id where the key is equal to the given key if it exists.
    /// Otherwise, return None. lower_bound_slot_id will not be returned.
    ///
    /// key: search key
    ///  It must be in the range of the page. i.e. low_fence <= key < high_fence
    fn find_slot_id(&self, key: &BTreeKey) -> Option<u16> {
        if !self.inside_range(key) {
            panic!("key is out of the range of the page");
        }
        // Binary search returns the left-most slot_id where the key is greater than the given key.
        let slot_id = self.binary_search(|slot_key| *key < slot_key);
        assert!(self.low_fence_slot_id() + 1 <= slot_id && slot_id <= self.high_fence_slot_id());
        // The right-most slot_id where the key is less than or equal to the given key.
        let slot_id = slot_id - 1;
        if slot_id != self.low_fence_slot_id() && self.get_btree_key(slot_id) == *key {
            Some(slot_id)
        } else {
            None
        }
    }

    fn insert_low_fence(&mut self, key: &[u8]) {
        // Low fence is always at slot_id 0. This never changes.
        if self.active_slot_count() != 0 {
            panic!("Cannot insert low fence when active_slot_count != 0");
        }
        let current_offset = self.rec_start_offset();
        let rec_size = key.len();
        // Insert the key
        let offset = current_offset - rec_size as u16;
        self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
        // Insert the slot
        let mut slot = Slot::new(offset, key.len() as u16, 0);
        slot.set_ghost(true);
        self.append_slot(&slot);
    }

    fn insert_high_fence(&mut self, key: &[u8]) {
        // High fence is initially inserted at slot_id 1 (slot_id will change if other slots are inserted).
        if self.active_slot_count() != 1 {
            // Assumes that the low fence is already inserted but no other slots are inserted.
            panic!("Cannot insert high fence when active_slot_count != 1");
        }
        let current_offset = self.rec_start_offset();
        let rec_size = key.len();
        // Insert the key
        let offset = current_offset - rec_size as u16;
        self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
        // Insert the slot
        let mut slot = Slot::new(offset, key.len() as u16, 0);
        slot.set_ghost(true);
        self.append_slot(&slot);
    }

    /// Insert a key-value-pair at slot_id.
    /// This function first create the slot at slot_id by shifting
    /// the slot_ids [slot_id..] to the right by 1.
    /// Then, it inserts the key-value-pair at the beginning of the record space.
    ///
    /// For example, if the page has 3 slots and the active_slot_count is 3.
    /// [ [slot1][slot2][slot3] ]
    /// We want to insert a new key-value pair at slot2.
    /// Need to shift slot2 and slot3 to the right.
    /// [ [slot1][slot2'][slot2][slot3] ]
    fn insert_at(&mut self, slot_id: u16, key: &[u8], value: &[u8]) {
        let start = self.slot_offset(slot_id);
        let end = self.slot_offset(self.active_slot_count());
        if start > end {
            panic!("Slot does not exist at the given slot_id");
        } else if start == end {
            // No need to shift if start == end. Just add a new slot at the end.

            // Insert the key-value pair
            let current_offset = self.rec_start_offset();
            let rec_size = key.len() + value.len();
            let offset = current_offset - rec_size as u16;
            self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
            self[offset as usize + key.len()..offset as usize + rec_size].copy_from_slice(value);

            // Insert the slot
            let slot = Slot::new(offset, key.len() as u16, value.len() as u16);
            self.append_slot(&slot);
            assert!(self.active_slot_count() == slot_id + 1);
        } else {
            // Insert the key-value pair
            let current_offset = self.rec_start_offset();
            let rec_size = key.len() + value.len();
            let offset = current_offset - rec_size as u16;
            self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
            self[offset as usize + key.len()..offset as usize + rec_size].copy_from_slice(value);

            // Shift the slots to the right by 1
            let data = self[start..end].to_vec();
            let new_start = start + SLOT_SIZE as usize;
            let new_end = end + SLOT_SIZE as usize;
            self[new_start..new_end].copy_from_slice(&data);

            // Update the slot
            let slot = Slot::new(offset, key.len() as u16, value.len() as u16);
            self.update_slot(slot_id, &slot);

            // Update the header
            self.set_rec_start_offset(offset);
            self.increment_active_slots();
        }
    }

    /// Remove a slot at slot_id.
    /// This function **DOES NOT** remove the key-value (record) from the page.
    ///
    /// It only removes the slot at slot_id by shifting
    /// the slot_ids [slot_id+1..] to the left by 1.
    ///
    /// To reclaim the space, run `compact_space`.
    ///
    /// For example, if the page has 3 slots and the active_slot_count is 3.
    /// [ [slot1][slot2][slot3] ]
    ///          ^       
    /// We want to delete slot2.
    /// Need to shift slot3 to the left by 1 SLOT_SIZE.
    /// [ [slot1][slot3] ]
    //          ^
    fn remove_at(&mut self, slot_id: u16) {
        let start = self.slot_offset(slot_id + 1);
        let end = self.slot_offset(self.active_slot_count());
        if start == 0 {
            panic!("Cannot shift slot to the left if start == 0");
        } else if start > end {
            panic!("Slot does not exist at the given slot_id");
        } else if start == end {
            // No need to shift slots if start == end. Just decrement the active_slot_count of the page.
            self.decrement_active_slots();
        } else {
            // Shift the slots to the left by 1
            let data = self[start..end].to_vec();
            let new_start = start - SLOT_SIZE as usize;
            let new_end = end - SLOT_SIZE as usize;
            self[new_start..new_end].copy_from_slice(&data);
            // Update the active_slot_count of the page
            self.decrement_active_slots();
        }
    }

    /// Insert a key-value pair into the page.
    /// Need to check the existence of the key before inserting.
    /// Otherwise two keys with the same value will be inserted.
    ///
    /// Insert fails in the following cases:
    /// 1. Returns false if the page does not have enough space to insert the key-value pair.
    /// 2. Panics if the key is out of the range of the page.
    /// 3. Panics if the key already exists in the page.
    fn insert(&mut self, key: &[u8], value: &[u8], _make_ghost: bool) -> bool {
        let rec_size = key.len() + value.len();
        if SLOT_SIZE + rec_size > self.free_space() {
            false
        } else {
            let slot_id = self.lower_bound_slot_id(&BTreeKey::Normal(key));
            // Check duplicate key. Duplication is only allowed for LOWER FENCE.
            if slot_id != self.low_fence_slot_id()
                && self.get_btree_key(slot_id) == BTreeKey::Normal(key)
            {
                panic!("Duplicate key");
            }
            // Insert at slot_id + 1
            self.insert_at(slot_id + 1, key, value);
            true
        }
    }

    /// Mark the slot with the given key as a ghost slot.
    fn mark_ghost(&mut self, key: &[u8]) {
        let slot_id = self.find_slot_id(&BTreeKey::Normal(key));
        if let Some(slot_id) = slot_id {
            let mut slot = self.slot(slot_id).unwrap();
            slot.set_ghost(true);
            self.update_slot(slot_id, &slot);
        } else {
            panic!("key does not exist");
        }
    }

    /// Remove the slot.
    /// To reclaim the space, run `compact_space`
    fn remove(&mut self, key: &[u8]) {
        let slot_id = self.find_slot_id(&BTreeKey::Normal(key));
        if let Some(slot_id) = slot_id {
            if slot_id == self.low_fence_slot_id() {
                panic!("Cannot remove the low fence")
            } else {
                self.remove_at(slot_id);
            }
        } else {
            panic!("key does not exist");
        }
    }

    /// Insert a sorted list of key-value pairs to a empty page.
    /// The input recs must be sorted.
    /// The page must be empty except for the low and high fences.
    ///
    /// This function fails in the following cases:
    /// 1. Returns false if the page does not have enough space to insert the key-value pairs.
    /// 2. Panics if the page is not empty except for the low and high fences.
    /// 3. Panics if the range of the key-value pairs is out of the range of the page.
    fn insert_sorted(&mut self, recs: Vec<(&[u8], &[u8])>) -> bool {
        if recs.is_empty() {
            return true;
        }

        #[cfg(any(test, debug_assertions))]
        {
            // lower fence < recs < high fence
            let low_fence = self.get_low_fence();
            let high_fence = self.get_high_fence();
            assert!(low_fence <= BTreeKey::Normal(recs[0].0));
            assert!(BTreeKey::Normal(recs[recs.len() - 1].0) < high_fence);
            // Check sortedness of the input recs
            for i in 1..recs.len() {
                assert!(recs[i - 1].0 < recs[i].0);
            }
        }
        if self.active_slot_count() != 2 {
            panic!("Page must be empty except for the low and high fences");
        }
        // Calculate the inserting size and check if the page has enough space.
        let inserting_size =
            recs.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + recs.len() * SLOT_SIZE;
        if inserting_size > self.free_space() {
            return false;
        }

        // Place the key-value pairs in the record space and create the slots.
        // For page header,
        // 1. increment the active_slot_count
        // 2. update the rec_start_offset
        let mut slots = Vec::new();
        let mut offset = self.rec_start_offset();
        for (key, value) in recs {
            let rec_size = key.len() + value.len();
            offset -= rec_size as u16;
            let slot = Slot::new(offset, key.len() as u16, value.len() as u16);
            // Copy the key-value pair to the record space
            self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
            self[offset as usize + key.len()..offset as usize + rec_size].copy_from_slice(value);
            slots.push(slot);
        }
        let high_fence_slot = self.slot(1).unwrap(); // high fence is at slot_id 1 on an empty page
        slots.push(high_fence_slot);

        // slots contains the key-value pairs, and the high fence.
        for (i, slot) in slots.iter().enumerate() {
            let slot_id = i + 1; // 1 for the low fence
            let slot_offset = self.slot_offset(slot_id as u16);
            self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
        }

        // Update the header
        self.set_active_slot_count(slots.len() as u16 + 1); // 1 for the low fence. The high fence is already included.
        self.set_rec_start_offset(offset);

        true
    }

    /// Removes slots [from..to) from the page.
    /// Panics if from >= to or the low fence or high fence is included in the range.
    /// The slots are shifted to the left by (to - from) slots.
    /// The active_slot_count is decremented by (to - from).
    /// This function does not reclaim the space. Run `compact_space` to reclaim the space.
    fn remove_range(&mut self, from: u16, to: u16) {
        if from >= to {
            panic!("start must be less than end");
        }
        if from <= self.low_fence_slot_id() || to > self.high_fence_slot_id() {
            panic!("Cannot remove the low fence or high fence");
        }
        // Shift the slots to the left by 1

        // Before:
        // [ [slot1][slot2][slot3][slot4][slot5][slot6] ]
        //          <--------------------> slots to be removed
        //          ^                     ^            ^
        //          from                  to
        //                                <------------> slots to be shifted
        //                                start        end
        // After:
        // [ [slot1][slot5][slot6] ]
        //

        let start = self.slot_offset(to);
        let end = self.slot_offset(self.active_slot_count());
        let data = self[start..end].to_vec();
        let new_start = start - (to - from) as usize * SLOT_SIZE;
        let new_end = end - (to - from) as usize * SLOT_SIZE;
        self[new_start..new_end].copy_from_slice(&data);
        // Update the active_slot_count of the page
        for _ in from..to {
            self.decrement_active_slots();
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn run_consistency_checks(&self, include_no_garbage_checks: bool) {
        self.check_keys_are_sorted();
        self.check_fence_slots_exists();
        if include_no_garbage_checks {
            self.check_rec_start_offset_match_slots();
            self.check_ideal_space_usage();
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn check_keys_are_sorted(&self) {
        // debug print all the keys
        // for i in 0..self.header().active_slot_count() {
        //     let key = self.get_slot_key(i).unwrap();
        //     println!("{:?}", key);
        // }
        for i in 1..self.active_slot_count() {
            let key1 = self.get_btree_key(i - 1);
            let key2 = self.get_btree_key(i);
            if i == 1 {
                // Low fence key could be equal to the first key
                assert!(key1 <= key2);
            } else {
                assert!(key1 < key2);
            }
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn check_fence_slots_exists(&self) {
        assert!(self.active_slot_count() >= 2);
    }

    #[cfg(any(test, debug_assertions))]
    fn check_rec_start_offset_match_slots(&self) {
        let mut rec_start_offset = u16::MAX;
        for i in 0..self.active_slot_count() {
            let slot = self.slot(i).unwrap();
            let offset = slot.offset();
            if offset < rec_start_offset {
                rec_start_offset = offset;
            }
        }
        assert_eq!(rec_start_offset, self.rec_start_offset());
    }

    #[cfg(any(test, debug_assertions))]
    fn check_ideal_space_usage(&self) {
        let mut rec_mem_usage = 0;
        for i in 0..self.active_slot_count() {
            if let Some(slot) = self.slot(i) {
                rec_mem_usage += slot.key_size() + slot.value_size();
            }
        }
        let ideal_start_offset = self.len() as u16 - rec_mem_usage;
        assert_eq!(ideal_start_offset, self.rec_start_offset());
    }

    #[cfg(any(test, debug_assertions))]
    fn print_all(&self) {
        print!("[");
        let mut sep = "";
        for i in 0..self.active_slot_count() {
            let key = self.get_btree_key(i);
            let value = self.get_val(i);
            print!(
                "{}({:?}, {:?})",
                sep,
                key,
                std::str::from_utf8(value).unwrap()
            );
            sep = ", ";
        }
        println!("]");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Page;

    /*
    #[test]
    fn test_init() {
        let mut page = Page::new_empty();
        let low_fence = "a".as_bytes();
        let high_fence = "d".as_bytes();
        FosterBtreePage::init(&mut page);
        let mut fbt_page = FosterBtreePage::new(&mut page);
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);

        fbt_page.check_keys_are_sorted();
        fbt_page.check_rec_start();

        assert_eq!(fbt_page.active_slot_count(), 2);
        assert_eq!(fbt_page.get_slot_key(0).unwrap(), low_fence);
        assert_eq!(fbt_page.get_slot_key(1).unwrap(), high_fence);
    }
    */

    #[test]
    fn test_insert() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.check_fence_slots_exists();

        let make_ghost = false;

        assert!(fbt_page.insert("b".as_bytes(), "bb".as_bytes(), make_ghost));
        assert!(fbt_page.active_slot_count() == 3);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        assert!(fbt_page.insert("c".as_bytes(), "cc".as_bytes(), make_ghost));
        assert!(fbt_page.active_slot_count() == 4);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
        assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
    }

    #[test]
    #[should_panic]
    fn test_insert_out_of_range() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.check_fence_slots_exists();

        let make_ghost = false;
        fbt_page.insert("a".as_bytes(), "aa".as_bytes(), make_ghost);
    }

    #[test]
    #[should_panic]
    fn test_insert_duplicate() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.check_fence_slots_exists();

        let make_ghost = false;
        fbt_page.insert("c".as_bytes(), "c".as_bytes(), make_ghost);
        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), make_ghost);
    }

    #[test]
    fn test_lower_bound_and_find() {
        {
            // Non-left-most and non-right-most page
            let mut fbt_page = Page::new_empty();
            let low_fence = "b".as_bytes();
            let high_fence = "d".as_bytes();
            fbt_page.init();
            fbt_page.insert_low_fence(low_fence);
            fbt_page.insert_high_fence(high_fence);
            fbt_page.check_fence_slots_exists();
            assert!(!fbt_page.is_left_most());
            assert!(!fbt_page.is_right_most());

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 0);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

            assert!(fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false));
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 3);

            assert!(fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false));
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
            assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
            assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
        }
        {
            // Root page
            let mut fbt_page = Page::new_empty();
            fbt_page.init_as_root();
            fbt_page.check_fence_slots_exists();

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 0);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("a")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("d")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("e")), None);

            fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 3);
            fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 2);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("a")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
            assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
            assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("d")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("e")), None);
        }

        {
            // Left most page
            let mut fbt_page = Page::new_empty();
            let low_fence = "".as_bytes();
            let high_fence = "d".as_bytes();
            fbt_page.init();
            fbt_page.insert_low_fence(low_fence);
            fbt_page.insert_high_fence(high_fence);
            fbt_page.set_left_most(true);
            fbt_page.check_fence_slots_exists();

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 0);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("a")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

            fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 3);
            fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("a")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
            assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
            assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
        }

        {
            // Right most page
            let mut fbt_page = Page::new_empty();
            let low_fence = "b".as_bytes();
            let high_fence = "".as_bytes();
            fbt_page.init();
            fbt_page.insert_low_fence(low_fence);
            fbt_page.insert_high_fence(high_fence);
            fbt_page.set_right_most(true);
            fbt_page.check_fence_slots_exists();

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 0);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 0);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("d")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("e")), None);

            fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 3);
            fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.active_slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 2);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
            assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
            assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("d")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("e")), None);
        }
    }

    #[test]
    fn test_remove_and_compact_space() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
        assert_eq!(fbt_page.active_slot_count(), 3);
        fbt_page.run_consistency_checks(true);

        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
        assert_eq!(fbt_page.active_slot_count(), 4);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("c".as_bytes());
        fbt_page.run_consistency_checks(false);
        assert_eq!(fbt_page.active_slot_count(), 3);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.active_slot_count(), 3);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        fbt_page.remove("b".as_bytes());
        fbt_page.run_consistency_checks(false);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0); // Low fence
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 0); // Low fence
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
    }

    #[test]
    #[should_panic]
    fn test_remove_low_fence() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("b".as_bytes());
    }

    #[test]
    #[should_panic]
    fn test_remove_high_fence() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("d".as_bytes());
    }

    #[test]
    #[should_panic]
    fn test_remove_non_existent() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("c".as_bytes());
    }

    #[test]
    #[should_panic]
    fn test_remove_out_of_range() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("a".as_bytes());
    }

    #[test]
    fn test_insert_sorted() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        let recs = vec![
            ("b".as_bytes(), "bb".as_bytes()),
            ("c".as_bytes(), "cc".as_bytes()),
        ];
        assert!(fbt_page.insert_sorted(recs));
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.active_slot_count(), 4);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
        assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
    }

    #[test]
    fn test_remove_range() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        let recs = vec![
            ("b".as_bytes(), "c".as_bytes()),
            ("bb".as_bytes(), "cc".as_bytes()),
            ("bbb".as_bytes(), "ccc".as_bytes()),
            ("bbbb".as_bytes(), "cccc".as_bytes()),
            ("bbbbb".as_bytes(), "ccccc".as_bytes()),
            ("bbbbbb".as_bytes(), "cccccc".as_bytes()),
        ];
        assert!(fbt_page.insert_sorted(recs));
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.active_slot_count(), 8);
        // fbt_page.print_all();

        let bbb_id = fbt_page.find_slot_id(&BTreeKey::str("bbb")).unwrap();
        let high_fence_id = fbt_page.high_fence_slot_id();
        fbt_page.remove_range(bbb_id, high_fence_id);
        fbt_page.run_consistency_checks(false);
        assert_eq!(fbt_page.active_slot_count(), 4);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("bb")), 2);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "c".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("bb")), Some(2));
        assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        // fbt_page.print_all();
    }
}