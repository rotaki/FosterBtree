use crate::page::Page;

// Page layout:
// 1 byte: flags (is_valid, is_root, leftmost, rightmost, has_foster_children) (u8)
// 1 byte: level (0 for leaf) (u8)
// 4 byte: slot count (generally >=2  because of low and high fences) (u32)
// 4 byte: total bytes used (PAGE_HEADER_SIZE + slots + records) (u32)
// 4 byte: rec start offset (u32)
pub const PAGE_HEADER_SIZE: usize = 1 + 1 + 4 + 4 + 4;
// Slotted page layout:
// * slot [ghost_bit: u8, offset: u32, key_size: u32, value_size: u32].
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
    pub const SLOT_SIZE: usize = std::mem::size_of::<u8>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u32>();

    pub struct Slot {
        ghost: u8,       // 0 if the slot is active, 1 if the slot is a ghost
        offset: u32,     // The offset of the key-value pair in the page
        key_size: u32,   // The size of the key
        value_size: u32, // The size of the value
    }

    impl Slot {
        pub fn from_bytes(bytes: [u8; SLOT_SIZE]) -> Self {
            let mut current_pos = 0;
            let ghost = bytes[current_pos];
            current_pos += std::mem::size_of::<u8>();

            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let value_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            Slot {
                ghost,
                offset,
                key_size,
                value_size,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let offset_bytes = self.offset.to_be_bytes();
            let key_size_bytes = self.key_size.to_be_bytes();
            let value_size_bytes = self.value_size.to_be_bytes();
            let mut bytes = [0; SLOT_SIZE];
            let mut current_pos = 0;
            bytes[current_pos] = self.ghost;
            current_pos += std::mem::size_of::<u8>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&offset_bytes);
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&key_size_bytes);
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&value_size_bytes);
            bytes
        }

        pub fn new(is_ghost: bool, offset: u32, key_size: u32, value_size: u32) -> Self {
            Slot {
                ghost: is_ghost as u8,
                offset,
                key_size,
                value_size,
            }
        }

        pub fn is_ghost(&self) -> bool {
            self.ghost == 1
        }

        pub fn set_ghost(&mut self, ghost: u8) {
            self.ghost = ghost;
        }

        pub fn offset(&self) -> u32 {
            self.offset
        }

        pub fn set_offset(&mut self, offset: u32) {
            self.offset = offset;
        }

        pub fn key_size(&self) -> u32 {
            self.key_size
        }

        pub fn set_key_size(&mut self, key_size: u32) {
            self.key_size = key_size;
        }

        pub fn value_size(&self) -> u32 {
            self.value_size
        }

        pub fn set_value_size(&mut self, value_size: u32) {
            self.value_size = value_size;
        }

        pub fn total_size(&self) -> u32 {
            self.key_size + self.value_size + SLOT_SIZE as u32
        }
    }
}

use slot::{Slot, SLOT_SIZE};

pub enum BTreeKey<'a> {
    MinusInfty,
    Normal(&'a [u8]),
    PlusInfty,
}

impl<'a> BTreeKey<'a> {
    pub fn new(key: &'a [u8]) -> Self {
        BTreeKey::Normal(key)
    }

    #[cfg(test)]
    fn str(key: &'a str) -> Self {
        BTreeKey::Normal(key.as_bytes())
    }
}

impl PartialEq for BTreeKey<'_> {
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

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for BTreeKey<'_> {
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
                // write the bytes
                write!(f, "{:?}", key)
            }
            BTreeKey::PlusInfty => write!(f, "+inf"),
        }
    }
}

impl AsRef<[u8]> for BTreeKey<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            BTreeKey::MinusInfty => &[],
            BTreeKey::Normal(key) => key,
            BTreeKey::PlusInfty => &[],
        }
    }
}

pub trait FosterBtreePage {
    // Helper functions
    fn page_size(&self) -> usize;
    fn get_id(&self) -> u32;
    fn slot_offset(&self, slot_id: u32) -> usize;
    fn slot(&self, slot_id: u32) -> Option<Slot>;
    fn append_slot(&mut self, slot: &Slot);
    fn update_slot(&mut self, slot_id: u32, slot: &Slot);
    fn linear_search<F>(&self, f: F) -> u32
    where
        F: Fn(BTreeKey) -> bool;
    fn binary_search<F>(&self, f: F) -> u32
    where
        F: Fn(BTreeKey) -> bool;
    fn get_low_fence(&self) -> BTreeKey;
    fn get_high_fence(&self) -> BTreeKey;
    fn range(&self) -> (BTreeKey, BTreeKey);
    fn low_fence_slot_id(&self) -> u32;
    fn high_fence_slot_id(&self) -> u32;
    fn foster_child_slot_id(&self) -> u32;
    fn is_fence(&self, slot_id: u32) -> bool;
    fn compact_space(&mut self);

    // Header operations
    fn total_free_space(&self) -> u32;
    fn total_bytes_used(&self) -> u32;
    fn set_total_bytes_used(&mut self, total_bytes_used: u32);
    fn bytes_used(&self, range: std::ops::Range<u32>) -> u32;
    fn bytes_needed(&self, key: &[u8], value: &[u8]) -> u32;
    fn is_valid(&self) -> bool;
    fn set_valid(&mut self, is_valid: bool);
    fn is_root(&self) -> bool;
    fn set_root(&mut self, is_root: bool);
    fn is_leaf(&self) -> bool;
    fn is_left_most(&self) -> bool;
    fn set_left_most(&mut self, is_left_most: bool);
    fn is_right_most(&self) -> bool;
    fn set_right_most(&mut self, is_right_most: bool);
    fn has_foster_child(&self) -> bool;
    fn set_has_foster_child(&mut self, has_foster_child: bool);
    fn level(&self) -> u8;
    fn set_level(&mut self, level: u8);
    fn increment_level(&mut self);
    fn decrement_level(&mut self);
    fn slot_count(&self) -> u32;
    fn active_slot_count(&self) -> u32;
    fn set_slot_count(&mut self, slot_count: u32);
    fn increment_slot_count(&mut self);
    fn decrement_slot_count(&mut self);
    fn rec_start_offset(&self) -> u32;
    fn set_rec_start_offset(&mut self, rec_start_offset: u32);
    fn contiguous_free_space(&self) -> u32;

    // Page operations
    fn init(&mut self);
    fn init_as_root(&mut self);
    fn empty(&self) -> bool;
    fn get_raw_key(&self, slot_id: u32) -> &[u8];
    fn get_btree_key(&self, slot_id: u32) -> BTreeKey;
    fn get_foster_key(&self) -> &[u8];
    fn get_foster_val(&self) -> &[u8];
    fn get_val(&self, slot_id: u32) -> &[u8];
    fn inside_range(&self, key: &BTreeKey) -> bool;
    fn lower_bound_slot_id(&self, key: &BTreeKey) -> u32;
    fn upper_bound_slot_id(&self, key: &BTreeKey) -> u32;
    fn find_slot_id(&self, key: &BTreeKey) -> Option<u32>;
    fn ghostify_at(&mut self, slot_id: u32);
    fn unghostify_at(&mut self, slot_id: u32);
    fn is_ghost(&self, slot_id: u32) -> bool;
    fn insert_at(&mut self, slot_id: u32, key: &[u8], value: &[u8], is_ghost: bool) -> bool;
    fn update_at(&mut self, slot_id: u32, key: Option<&[u8]>, value: &[u8]) -> bool;
    fn remove_at(&mut self, slot_id: u32);
    fn set_low_fence(&mut self, key: &[u8]);
    fn set_high_fence(&mut self, key: &[u8]);
    fn insert(&mut self, key: &[u8], value: &[u8], is_ghost: bool) -> bool;
    fn remove(&mut self, key: &[u8]);
    fn append_sorted<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, recs: &[(bool, K, V)]) -> bool;
    fn remove_range(&mut self, start: u32, end: u32);

    fn run_consistency_checks(&self, include_no_garbage_checks: bool);
    fn check_keys_are_sorted(&self);
    fn check_total_bytes_used(&self);
    fn check_fence_slots_exists(&self);
    fn check_rec_start_offset_match_slots(&self);
    fn check_ideal_space_usage(&self);
    fn print_all<K: Fn(&[u8]) -> String>(&self, key_fn: K);
}

impl FosterBtreePage for Page {
    fn is_valid(&self) -> bool {
        self[0] & 0b1000_0000 != 0
    }

    fn set_valid(&mut self, is_valid: bool) {
        if is_valid {
            self[0] |= 0b1000_0000;
        } else {
            self[0] &= 0b0111_1111;
        }
    }

    fn is_root(&self) -> bool {
        self[0] & 0b0100_0000 != 0
    }

    fn set_root(&mut self, is_root: bool) {
        if is_root {
            self[0] |= 0b0100_0000;
        } else {
            self[0] &= 0b1011_1111;
        }
    }

    fn is_leaf(&self) -> bool {
        self.level() == 0
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

    fn set_has_foster_child(&mut self, has_foster_child: bool) {
        if has_foster_child {
            self[0] |= 0b0000_1000;
        } else {
            self[0] &= 0b1111_0111;
        }
    }

    fn level(&self) -> u8 {
        self[1]
    }

    fn set_level(&mut self, level: u8) {
        self[1] = level;
    }

    fn increment_level(&mut self) {
        self[1] += 1; // Towards the root
    }

    fn decrement_level(&mut self) {
        self[1] -= 1; // Towards the leaf
    }

    /// The number of slots in the page.
    /// The low fence and high fence are always present.
    /// Therefore, the slot count should be at least 2 after the initialization.
    fn slot_count(&self) -> u32 {
        let offset = 2;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    /// The number of active slots in the page.
    /// The low fence and high fence are always present.
    /// Active slot count is slot_count excluding the low fence and high fence.
    fn active_slot_count(&self) -> u32 {
        self.slot_count() - 2
    }

    fn set_slot_count(&mut self, slot_count: u32) {
        let bytes = slot_count.to_be_bytes();
        let offset = 2;
        self[offset..offset + std::mem::size_of::<u32>()].copy_from_slice(&bytes);
    }

    fn increment_slot_count(&mut self) {
        let slot_count = self.slot_count();
        self.set_slot_count(slot_count + 1);
    }

    fn decrement_slot_count(&mut self) {
        let slot_count = self.slot_count();
        self.set_slot_count(slot_count - 1);
    }

    fn page_size(&self) -> usize {
        self.len()
    }

    fn total_free_space(&self) -> u32 {
        self.len() as u32 - self.total_bytes_used()
    }

    fn total_bytes_used(&self) -> u32 {
        let offset = 2 + 4;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
        let bytes = total_bytes_used.to_be_bytes();
        let offset = 2 + 4;
        self[offset..offset + std::mem::size_of::<u32>()].copy_from_slice(&bytes);
    }

    fn bytes_used(&self, range: std::ops::Range<u32>) -> u32 {
        // Check if the range is valid
        if range.end > self.slot_count() {
            panic!("Invalid range");
        }
        let mut sum_used = 0;
        for i in range {
            let slot = self.slot(i).unwrap();
            sum_used += slot.key_size() as usize + slot.value_size() as usize;
            sum_used += SLOT_SIZE;
        }
        sum_used as u32
    }

    fn bytes_needed(&self, key: &[u8], value: &[u8]) -> u32 {
        (key.len() + value.len() + SLOT_SIZE) as u32
    }

    fn rec_start_offset(&self) -> u32 {
        let offset = 2 + 4 + 4;
        u32::from_be_bytes(
            self[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        let bytes = rec_start_offset.to_be_bytes();
        let offset = 2 + 4 + 4;
        self[offset..offset + std::mem::size_of::<u32>()].copy_from_slice(&bytes);
    }

    fn contiguous_free_space(&self) -> u32 {
        let next_slot_offset = self.slot_offset(self.slot_count());
        let rec_start_offset = self.rec_start_offset();
        rec_start_offset - next_slot_offset as u32
    }

    fn get_id(&self) -> u32 {
        self.get_id()
    }

    fn slot_offset(&self, slot_id: u32) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }

    fn slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id < self.slot_count() {
            let offset = self.slot_offset(slot_id);
            let slot_bytes: [u8; SLOT_SIZE] = self[offset..offset + SLOT_SIZE].try_into().unwrap();
            Some(Slot::from_bytes(slot_bytes))
        } else {
            None
        }
    }

    fn ghostify_at(&mut self, slot_id: u32) {
        let mut slot = self.slot(slot_id).unwrap();
        let slot_offset = self.slot_offset(slot_id);
        slot.set_ghost(1);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
    }

    fn unghostify_at(&mut self, slot_id: u32) {
        let mut slot = self.slot(slot_id).unwrap();
        let slot_offset = self.slot_offset(slot_id);
        slot.set_ghost(0);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
    }

    fn is_ghost(&self, slot_id: u32) -> bool {
        let slot = self.slot(slot_id).unwrap();
        slot.is_ghost()
    }

    /// Append a slot at the end of the slots.
    /// Increment the slot count.
    /// The header is also updated to set the rec_start_offset to the minimum of the current rec_start_offset and the slot's offset.
    fn append_slot(&mut self, slot: &Slot) {
        // Increment the slot count and update the header
        let slot_id = self.slot_count();

        self.increment_slot_count();

        // Update the slot
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update the header
        let offset = self.rec_start_offset().min(slot.offset());
        self.set_rec_start_offset(offset);
    }

    /// Update the slot at slot_id.
    /// Panic if the slot_id is out of range.
    /// The header is also updated to set the rec_start_offset to the minimum of the current rec_start_offset and the slot's offset.
    fn update_slot(&mut self, slot_id: u32, slot: &Slot) {
        if slot_id >= self.slot_count() {
            panic!("Slot does not exist");
        }
        // Update the slot
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update the header
        let offset = self.rec_start_offset().min(slot.offset());
        self.set_rec_start_offset(offset);
    }

    // Find the left-most key where f(key) = true.
    // Assumes that f(key, search_key) is false for all keys to the left of the returned index.
    // [false, false, false, true, true, true]
    //                        ^
    //                        |
    //                        return this index
    // If all keys are false, then return the len (i.e. slot_count)
    fn linear_search<F>(&self, f: F) -> u32
    where
        F: Fn(BTreeKey) -> bool,
    {
        for i in 0..self.slot_count() {
            let slot_key = self.get_btree_key(i);
            if f(slot_key) {
                return i;
            }
        }
        self.slot_count()
    }

    // Find the left-most key where f(key) = true.
    // Assumes that f(key, search_key) is false for all keys to the left of the returned index.
    // [false, false, false, true, true, true]
    //                        ^
    //                        |
    //                        return this index
    // If all keys are false, then return the len (i.e. slot_count)
    fn binary_search<F>(&self, f: F) -> u32
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
            return self.high_fence_slot_id() + 1; // equals to slot_count
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

    fn is_fence(&self, slot_id: u32) -> bool {
        slot_id == self.low_fence_slot_id() || slot_id == self.high_fence_slot_id()
    }

    fn low_fence_slot_id(&self) -> u32 {
        0
    }

    fn high_fence_slot_id(&self) -> u32 {
        self.slot_count() - 1
    }

    fn foster_child_slot_id(&self) -> u32 {
        self.slot_count() - 2
    }

    fn compact_space(&mut self) {
        let rec_mem_usage = self.total_bytes_used()
            - PAGE_HEADER_SIZE as u32
            - SLOT_SIZE as u32 * self.slot_count();
        let ideal_start_offset = self.len() as u32 - rec_mem_usage;
        let rec_start_offset = self.rec_start_offset();

        match rec_start_offset.cmp(&ideal_start_offset) {
            std::cmp::Ordering::Greater => {
                panic!("corrupted page");
            }
            std::cmp::Ordering::Equal => {
                // No need to compact
            }
            std::cmp::Ordering::Less => {
                let mut recs = vec![0; rec_mem_usage as usize];
                let mut current_size = 0;

                // Copy the records into a temporary buffer and update the slots
                for i in 0..self.slot_count() {
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
                        let global_offset = (self.len() - current_size) as u32;
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
    }

    fn init(&mut self) {
        // Default is valid, non-root, leaf, non-leftmost, non-rightmost, non-foster_children
        self.set_valid(true);
        self.set_root(false);
        self.set_left_most(false);
        self.set_right_most(false);
        self.set_has_foster_child(false);
        self.set_level(0);
        self.set_slot_count(0);
        self.set_rec_start_offset(self.len() as u32);
        self.set_total_bytes_used(PAGE_HEADER_SIZE as u32);

        // Insert low and high fence
        self.insert_at(0, &[], &[], false);
        self.insert_at(1, &[], &[], false);
    }

    fn init_as_root(&mut self) {
        self.set_valid(true);
        self.set_root(true);
        self.set_left_most(true);
        self.set_right_most(true);
        self.set_has_foster_child(false);
        self.set_level(0);
        self.set_slot_count(0);
        self.set_rec_start_offset(self.len() as u32);
        self.set_total_bytes_used(PAGE_HEADER_SIZE as u32);

        // Insert low and high fence
        self.insert_at(0, &[], &[], false);
        self.insert_at(1, &[], &[], false);
    }

    fn empty(&self) -> bool {
        self.slot_count() == 2 // low fence and high fence
    }

    fn get_raw_key(&self, slot_id: u32) -> &[u8] {
        assert!(slot_id < self.slot_count());
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        &self[offset..offset + key_size]
    }

    fn get_btree_key(&self, slot_id: u32) -> BTreeKey {
        if slot_id == self.low_fence_slot_id() {
            self.get_low_fence()
        } else if slot_id == self.high_fence_slot_id() {
            self.get_high_fence()
        } else {
            let key = self.get_raw_key(slot_id);
            if key.is_empty() {
                BTreeKey::MinusInfty
            } else {
                BTreeKey::Normal(key)
            }
        }
    }

    fn get_foster_key(&self) -> &[u8] {
        assert!(self.has_foster_child());
        let foster_slot_id = self.foster_child_slot_id();
        self.get_raw_key(foster_slot_id)
    }

    fn get_foster_val(&self) -> &[u8] {
        assert!(self.has_foster_child());
        let foster_slot_id = self.foster_child_slot_id();
        self.get_val(foster_slot_id)
    }

    fn get_val(&self, slot_id: u32) -> &[u8] {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let value_size = slot.value_size() as usize;
        &self[offset + key_size..offset + key_size + value_size]
    }

    fn inside_range(&self, key: &BTreeKey) -> bool {
        let (low_fence, high_fence) = self.range();
        low_fence <= *key && *key < high_fence
    }

    /// Returns the left-most slot_id where the key is greater or equal to the given key.
    /// The returned slot_id satisfies low_fence_slot_id <= slot_id <= high_fence_slot_id.
    ///
    /// key: search key
    /// It must be in the range of the page. i.e. low_fence <= key < high_fence
    ///
    /// Example:
    ///   0  1  2  3  4  5
    /// [ 2, 3, 5, 5, 8, 13 ]
    ///
    /// lower_bound_slot_id(0) = NOT IN RANGE
    /// lower_bound_slot_id(2) = 0
    /// lower_bound_slot_id(5) = 2
    /// lower_bound_slot_id(6) = 4
    /// lower_bound_slot_id(13) = NOT IN RANGE
    fn lower_bound_slot_id(&self, key: &BTreeKey) -> u32 {
        debug_assert!(
            self.inside_range(key),
            "key is out of the range of the page"
        );
        // Binary search returns the left-most slot_id where the key is greater or equal to the given key.
        let slot_id = self.binary_search(|slot_key| *key <= slot_key);
        debug_assert!(self.low_fence_slot_id() <= slot_id && slot_id <= self.high_fence_slot_id());
        slot_id
    }

    /// Returns the left-most slot_id where the key is greater than the given key.
    /// The returned slot_id satisfies low_fence_slot_id < slot_id <= high_fence_slot_id.
    ///
    /// key: search key
    ///  It must be in the range of the page. i.e. low_fence <= key < high_fence
    ///
    /// Example:
    ///   0  1  2  3  4  5
    /// [ 2, 3, 5, 5, 8, 13 ]
    ///
    /// upper_bound_slot_id(0) = NOT IN RANGE
    /// upper_bound_slot_id(2) = 1
    /// upper_bound_slot_id(5) = 4
    /// upper_bound_slot_id(6) = 4
    /// upper_bound_slot_id(13) = NOT IN RANGE
    fn upper_bound_slot_id(&self, key: &BTreeKey) -> u32 {
        debug_assert!(
            self.inside_range(key),
            "key is out of the range of the page"
        );
        // Binary search returns the left-most slot_id where the key is greater than the given key.
        let slot_id = self.binary_search(|slot_key| *key < slot_key);
        debug_assert!(self.low_fence_slot_id() < slot_id && slot_id <= self.high_fence_slot_id());
        slot_id
    }

    /// Returns the right-most slot_id where the key is equal to the given key if it exists.
    /// Otherwise, return None. low_fence_slot_id will not be returned.
    ///
    /// key: search key
    ///  It must be in the range of the page. i.e. low_fence <= key < high_fence
    fn find_slot_id(&self, key: &BTreeKey) -> Option<u32> {
        let slot_id = self.upper_bound_slot_id(key);
        // The right-most slot_id where the key is less than or equal to the given key.
        let slot_id = slot_id - 1;
        if slot_id != self.low_fence_slot_id() && self.get_btree_key(slot_id) == *key {
            Some(slot_id)
        } else {
            None
        }
    }

    fn set_low_fence(&mut self, key: &[u8]) {
        if key.is_empty() {
            self.set_left_most(true)
        }
        let res = self.update_at(self.low_fence_slot_id(), Some(key), &[]);
        assert!(res);
    }

    /// Set the high fence of the page.
    /// This function will
    fn set_high_fence(&mut self, key: &[u8]) {
        if key.is_empty() {
            self.set_right_most(true)
        }
        let res = self.update_at(self.high_fence_slot_id(), Some(key), &[]);
        assert!(res);
    }

    /// Insert a key-value-pair at slot_id.
    /// This function first create the slot at slot_id by shifting
    /// the slot_ids [slot_id..] to the right by 1.
    /// Then, it inserts the key-value-pair at the beginning of the record space.
    ///
    /// For example, if the page has 3 slots and the slot_count is 3.
    /// [ [slot1][slot2][slot3] ]
    /// We want to insert a new key-value pair at slot2.
    /// Need to shift slot2 and slot3 to the right.
    /// [ [slot1][slot2'][slot2][slot3] ]
    ///
    /// This function **DOES NOT** check the existence of the key before inserting.
    /// This function **DOES NOT** check the sorted order of the keys.
    fn insert_at(&mut self, slot_id: u32, key: &[u8], value: &[u8], is_ghost: bool) -> bool {
        let rec_size = key.len() + value.len();
        if SLOT_SIZE + rec_size > self.contiguous_free_space() as usize {
            if SLOT_SIZE + rec_size > self.total_free_space() as usize {
                return false;
            } else {
                self.compact_space();
                return self.insert_at(slot_id, key, value, is_ghost);
            }
        }
        let start = self.slot_offset(slot_id);
        let end = self.slot_offset(self.slot_count());
        match start.cmp(&end) {
            std::cmp::Ordering::Greater => {
                panic!("Slot does not exist at the given slot_id");
            }
            std::cmp::Ordering::Equal => {
                // No need to shift if start == end. Just add a new slot at the end.

                // Insert the key-value pair
                let current_offset = self.rec_start_offset();
                let rec_size = key.len() + value.len();
                let offset = current_offset - rec_size as u32;
                self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
                self[offset as usize + key.len()..offset as usize + rec_size]
                    .copy_from_slice(value);

                // Insert the slot
                let slot = Slot::new(is_ghost, offset, key.len() as u32, value.len() as u32);
                self.append_slot(&slot);
                self.set_total_bytes_used(self.total_bytes_used() + slot.total_size());
                debug_assert!(self.slot_count() == slot_id + 1);
            }
            std::cmp::Ordering::Less => {
                // Insert the key-value pair
                let current_offset = self.rec_start_offset();
                let rec_size = key.len() + value.len();
                let offset = current_offset - rec_size as u32;
                self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
                self[offset as usize + key.len()..offset as usize + rec_size]
                    .copy_from_slice(value);

                // Shift the slots to the right by 1
                // Use copy within to avoid heap allocation
                self.copy_within(start..end, start + SLOT_SIZE);

                // Update the slot
                let slot = Slot::new(is_ghost, offset, key.len() as u32, value.len() as u32);
                self.update_slot(slot_id, &slot);

                // Update the header
                self.set_rec_start_offset(offset);
                self.increment_slot_count();
                self.set_total_bytes_used(self.total_bytes_used() + slot.total_size());
            }
        }
        true
    }

    /// Update the key-value pair at slot_id.
    ///
    /// There are three cases to consider:
    /// 1. The record can be updated in-place. This is when the new_rec_size is smaller or equal to the old_rec_size.
    /// 2. The new record can be inserted into the contiguous free space.
    /// 3. The new record cannot be inserted into the contiguous free space. In this case, compact the space and try again.
    ///
    /// If the new record cannot be inserted into the page, return false without removing the old record.
    /// This function **DOES NOT** check the sorted order of the keys.
    fn update_at(&mut self, slot_id: u32, key: Option<&[u8]>, value: &[u8]) -> bool {
        assert!(slot_id < self.slot_count());
        let mut slot = self.slot(slot_id).unwrap();
        let old_rec_size = slot.key_size() as usize + slot.value_size() as usize;
        let new_rec_size = key.map(|k| k.len()).unwrap_or(slot.key_size() as usize) + value.len();
        if new_rec_size <= old_rec_size {
            let offset = slot.offset() as usize;
            if let Some(key) = key {
                // Key changes
                self[offset..offset + key.len()].copy_from_slice(key);
                self[offset + key.len()..offset + new_rec_size].copy_from_slice(value);
                slot.set_key_size(key.len() as u32);
            } else {
                // Key is the same
                self[offset + slot.key_size() as usize..offset + new_rec_size]
                    .copy_from_slice(value);
            }
            slot.set_value_size(value.len() as u32);
            self.update_slot(slot_id, &slot);
            self.set_total_bytes_used(
                self.total_bytes_used() - old_rec_size as u32 + new_rec_size as u32,
            );
            return true;
        }

        if new_rec_size > self.contiguous_free_space() as usize {
            if new_rec_size > self.total_free_space() as usize {
                return false;
            } else {
                // Run compact_space and try again
                self.compact_space();
                return self.update_at(slot_id, key, value);
            }
        }

        // Insert the key-value pair at the beginning of the record space.
        let current_offset = self.rec_start_offset();
        let is_ghost = slot.is_ghost();
        let offset = current_offset - new_rec_size as u32;
        let new_slot = if let Some(key) = key {
            // Key changes
            self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
            self[offset as usize + key.len()..offset as usize + new_rec_size]
                .copy_from_slice(value);
            // Update the slot
            Slot::new(is_ghost, offset, key.len() as u32, value.len() as u32)
        } else {
            // Key is the same
            self.copy_within(
                slot.offset() as usize..slot.offset() as usize + slot.key_size() as usize,
                offset as usize,
            );
            self[offset as usize + slot.key_size() as usize..offset as usize + new_rec_size]
                .copy_from_slice(value);
            // Update the slot
            Slot::new(is_ghost, offset, slot.key_size(), value.len() as u32)
        };
        self.update_slot(slot_id, &new_slot);
        self.set_total_bytes_used(
            self.total_bytes_used() - old_rec_size as u32 + new_rec_size as u32,
        );
        true
    }

    /// Remove a slot at slot_id.
    /// This function **DOES NOT** remove the key-value (record) from the page.
    ///
    /// It only removes the slot at slot_id by shifting
    /// the slot_ids [slot_id+1..] to the left by 1.
    ///
    /// To reclaim the space, run `compact_space`.
    ///
    /// For example, if the page has 3 slots and the slot_count is 3.
    /// [ [slot1][slot2][slot3] ]
    ///          ^       
    /// We want to delete slot2.
    /// Need to shift slot3 to the left by 1 SLOT_SIZE.
    /// [ [slot1][slot3] ]
    //          ^
    fn remove_at(&mut self, slot_id: u32) {
        let start = self.slot_offset(slot_id + 1);
        let end = self.slot_offset(self.slot_count());
        if start == 0 {
            panic!("Cannot shift slot to the left if start == 0");
        } else if start > end {
            panic!("Slot does not exist at the given slot_id");
        } else if start == end {
            self.set_total_bytes_used(
                self.total_bytes_used() - self.slot(slot_id).unwrap().total_size(),
            );
            // No need to shift slots if start == end. Just decrement the slot_count of the page.
            self.decrement_slot_count();
        } else {
            self.set_total_bytes_used(
                self.total_bytes_used() - self.slot(slot_id).unwrap().total_size(),
            );

            // Shift the slots to the left by 1
            // Use copy_within to avoid heap allocation
            self.copy_within(start..end, start - SLOT_SIZE);

            // Update the slot_count of the page
            self.decrement_slot_count();
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
    fn insert(&mut self, key: &[u8], value: &[u8], is_ghost: bool) -> bool {
        let slot_id = self.upper_bound_slot_id(&BTreeKey::Normal(key));
        // Check duplicate key. Duplication is only allowed for LOWER FENCE.
        if (slot_id - 1) != self.low_fence_slot_id()
            && self.get_btree_key(slot_id - 1) == BTreeKey::Normal(key)
        {
            panic!("Duplicate key");
        }
        // Insert at slot_id
        self.insert_at(slot_id, key, value, is_ghost)
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

    /// Append a sorted list of key-value pairs to the page.
    /// The input recs must be sorted in the ascending order of the keys.
    /// The input recs must be in [last_slot_key, high_fence)
    ///
    /// This function fails in the following cases:
    /// 1. Returns false if the page does not have enough space to insert the key-value pairs.
    /// 2. Panics if the page is not empty except for the low and high fences.
    /// 3. Panics if the range of the key-value pairs is out of the range of the page.
    fn append_sorted<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, recs: &[(bool, K, V)]) -> bool {
        if recs.is_empty() {
            return true;
        }

        #[cfg(any(test, debug_assertions))]
        {
            // last_slot_key < recs < high fence
            let last_key = self.get_btree_key(self.high_fence_slot_id() - 1);
            let high_fence = self.get_high_fence();
            if self.low_fence_slot_id() == self.high_fence_slot_id() - 1 {
                debug_assert!(last_key <= BTreeKey::Normal(recs[0].1.as_ref()));
            } else {
                debug_assert!(last_key < BTreeKey::Normal(recs[0].1.as_ref()));
            }
            assert!(BTreeKey::Normal(recs[recs.len() - 1].1.as_ref()) < high_fence);
            // Check sortedness of the input recs
            for i in 1..recs.len() {
                debug_assert!(recs[i - 1].1.as_ref() < recs[i].1.as_ref());
            }
        }

        // Calculate the inserting size and check if the page has enough space.
        let inserting_size = recs
            .iter()
            .map(|(_, k, v)| k.as_ref().len() + v.as_ref().len())
            .sum::<usize>()
            + recs.len() * SLOT_SIZE;
        if inserting_size > self.contiguous_free_space() as usize {
            if inserting_size > self.total_free_space() as usize {
                return false;
            } else {
                self.compact_space();
                return self.append_sorted(recs);
            }
        }

        // Place the key-value pairs in the record space and create the slots.
        let mut offset = self.rec_start_offset();
        let high_fence_slot = self.slot(self.high_fence_slot_id()).unwrap();
        self.decrement_slot_count(); // Remove the high fence slot temporarily

        for (is_ghost, key, value) in recs
            .iter()
            .map(|(is_ghost, k, v)| (is_ghost, k.as_ref(), v.as_ref()))
        {
            let rec_size = key.len() + value.len();
            offset -= rec_size as u32;
            let slot = Slot::new(*is_ghost, offset, key.len() as u32, value.len() as u32);
            // Copy the key-value pair to the record space
            self[offset as usize..offset as usize + key.len()].copy_from_slice(key);
            self[offset as usize + key.len()..offset as usize + rec_size].copy_from_slice(value);
            self.append_slot(&slot);
        }

        self.append_slot(&high_fence_slot); // Restore the high fence slot

        // Update the header
        self.set_rec_start_offset(offset);
        self.set_total_bytes_used(self.total_bytes_used() + inserting_size as u32);

        true
    }

    /// Removes slots [from..to) from the page.
    /// Panics if from >= to or the low fence or high fence is included in the range.
    /// The slots are shifted to the left by (to - from) slots.
    /// The slot_count is decremented by (to - from).
    /// This function does not reclaim the space. Run `compact_space` to reclaim the space.
    fn remove_range(&mut self, from: u32, to: u32) {
        if from == to {
            return;
        }
        if from > to {
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

        for i in from..to {
            self.set_total_bytes_used(self.total_bytes_used() - self.slot(i).unwrap().total_size());
        }

        let start = self.slot_offset(to);
        let end = self.slot_offset(self.slot_count());
        let new_start = start - (to - from) as usize * SLOT_SIZE;
        self.copy_within(start..end, new_start);

        // Update the slot_count of the page
        for _ in from..to {
            self.decrement_slot_count();
        }
    }

    fn run_consistency_checks(&self, include_no_garbage_checks: bool) {
        self.check_keys_are_sorted();
        self.check_fence_slots_exists();
        self.check_total_bytes_used();
        if include_no_garbage_checks {
            self.check_rec_start_offset_match_slots();
            self.check_ideal_space_usage();
        }
    }

    fn check_keys_are_sorted(&self) {
        // debug print all the keys
        // for i in 0..self.header().slot_count() {
        //     let key = self.get_slot_key(i).unwrap();
        //     println!("{:?}", key);
        // }
        for i in 1..self.slot_count() {
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

    fn check_total_bytes_used(&self) {
        let mut sum_used = PAGE_HEADER_SIZE;
        for i in 0..self.slot_count() {
            let slot = self.slot(i).unwrap();
            sum_used += slot.total_size() as usize;
        }
        assert_eq!(sum_used, self.total_bytes_used() as usize);
    }

    fn check_fence_slots_exists(&self) {
        assert!(self.slot_count() >= 2);
    }

    fn check_rec_start_offset_match_slots(&self) {
        let mut rec_start_offset = u32::MAX;
        for i in 0..self.slot_count() {
            let slot = self.slot(i).unwrap();
            let offset = slot.offset();
            if offset < rec_start_offset {
                rec_start_offset = offset;
            }
        }
        assert_eq!(rec_start_offset, self.rec_start_offset());
    }

    fn check_ideal_space_usage(&self) {
        let mut rec_mem_usage = 0;
        for i in 0..self.slot_count() {
            if let Some(slot) = self.slot(i) {
                rec_mem_usage += slot.key_size() + slot.value_size();
            }
        }
        let ideal_start_offset = self.len() as u32 - rec_mem_usage;
        assert_eq!(ideal_start_offset, self.rec_start_offset());
    }

    fn print_all<K: Fn(&[u8]) -> String>(&self, key_fn: K) {
        print!("[");
        let mut sep = "";
        for i in 0..self.slot_count() {
            let key = self.get_btree_key(i);
            let value = self.get_val(i);
            print!("{}({:?}, {:?})", sep, key_fn(key.as_ref()), value,);
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

        assert_eq!(fbt_page.slot_count(), 2);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.check_fence_slots_exists();

        assert!(fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false));
        assert!(fbt_page.slot_count() == 3);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        assert!(fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false));
        assert!(fbt_page.slot_count() == 4);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.check_fence_slots_exists();

        fbt_page.insert("a".as_bytes(), "aa".as_bytes(), false);
    }

    #[test]
    #[should_panic]
    fn test_insert_duplicate() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.check_fence_slots_exists();

        fbt_page.insert("c".as_bytes(), "c".as_bytes(), false);
        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
    }

    #[test]
    fn test_lower_bound_and_find() {
        {
            // Non-left-most and non-right-most page
            let mut fbt_page = Page::new_empty();
            let low_fence = "b".as_bytes();
            let high_fence = "d".as_bytes();
            fbt_page.init();
            fbt_page.set_low_fence(low_fence);
            fbt_page.set_high_fence(high_fence);
            fbt_page.check_fence_slots_exists();
            assert!(!fbt_page.is_left_most());
            assert!(!fbt_page.is_right_most());

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

            assert!(fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false));
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 3);

            assert!(fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false));
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);

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

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("d")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("e")), 1);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("a")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("d")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("e")), None);

            fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 3);
            fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 3);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("d")), 3);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 3);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("e")), 3);
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
            let high_fence = "d".as_bytes();
            fbt_page.init();
            fbt_page.set_low_fence(&[]);
            fbt_page.set_high_fence(high_fence);
            fbt_page.check_fence_slots_exists();

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("a")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

            fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 3);
            fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("a")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);
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
            fbt_page.init();
            fbt_page.set_low_fence(low_fence);
            fbt_page.set_high_fence(&[]);
            fbt_page.check_fence_slots_exists();

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("d")), 1);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 1);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("e")), 1);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("d")), None);
            assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("e")), None);

            fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 3);
            fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
            fbt_page.run_consistency_checks(true);
            assert!(fbt_page.slot_count() == 4);

            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("d")), 3);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("d")), 3);
            assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("e")), 3);
            assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("e")), 3);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
        assert_eq!(fbt_page.slot_count(), 3);
        fbt_page.run_consistency_checks(true);

        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
        assert_eq!(fbt_page.slot_count(), 4);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("c".as_bytes());
        fbt_page.run_consistency_checks(false);
        assert_eq!(fbt_page.slot_count(), 3);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.slot_count(), 3);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), None);

        fbt_page.remove("b".as_bytes());
        fbt_page.run_consistency_checks(false);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0); // Low fence
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 1); // High fence
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 1); // High fence
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 1); // High fence
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.remove("a".as_bytes());
    }

    #[test]
    fn test_insert_sorted() {
        let mut fbt_page = Page::new_empty();
        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.init();
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        let recs = vec![
            (false, "b".as_bytes(), "bb".as_bytes()),
            (false, "c".as_bytes(), "cc".as_bytes()),
        ];
        assert!(fbt_page.append_sorted(&recs));
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.slot_count(), 4);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);
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
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        let recs = vec![
            (false, "b".as_bytes(), "c".as_bytes()),
            (false, "bb".as_bytes(), "cc".as_bytes()),
            (false, "bbb".as_bytes(), "ccc".as_bytes()),
            (false, "bbbb".as_bytes(), "cccc".as_bytes()),
            (false, "bbbbb".as_bytes(), "ccccc".as_bytes()),
            (false, "bbbbbb".as_bytes(), "cccccc".as_bytes()),
        ];
        assert!(fbt_page.append_sorted(&recs));
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.slot_count(), 8);
        // fbt_page.print_all();

        let bbb_id = fbt_page.find_slot_id(&BTreeKey::str("bbb")).unwrap();
        let high_fence_id = fbt_page.high_fence_slot_id();
        fbt_page.remove_range(bbb_id, high_fence_id);
        fbt_page.run_consistency_checks(false);
        assert_eq!(fbt_page.slot_count(), 4);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("bb")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("bb")), 3);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "c".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("bb")), Some(2));
        assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        // fbt_page.print_all();
    }

    #[test]
    fn test_update_at() {
        let mut fbt_page = Page::new_empty();
        fbt_page.init();
        fbt_page.run_consistency_checks(true);

        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.insert("b".as_bytes(), "bb".as_bytes(), false);
        fbt_page.run_consistency_checks(true);
        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
        fbt_page.run_consistency_checks(true);

        assert!(fbt_page.update_at(1, Some("b_new".as_bytes()), "bbb".as_bytes()));
        fbt_page.run_consistency_checks(false);
        // fbt_page.print_all(|key| String::from_utf8_lossy(key).to_string());
        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.get_raw_key(1), "b_new".as_bytes());
        assert_eq!(fbt_page.get_val(1), "bbb".as_bytes());

        // update only the value
        assert!(fbt_page.update_at(1, None, "bbbb".as_bytes()));
        fbt_page.run_consistency_checks(false);
        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.get_raw_key(1), "b_new".as_bytes());
        assert_eq!(fbt_page.get_val(1), "bbbb".as_bytes());
    }

    #[test]
    fn test_ghost_record() {
        // Turn some records into ghost record by setting the
        // ghost bit in the slot.
        let mut fbt_page = Page::new_empty();
        fbt_page.init();
        fbt_page.run_consistency_checks(true);

        let low_fence = "b".as_bytes();
        let high_fence = "d".as_bytes();
        fbt_page.set_low_fence(low_fence);
        fbt_page.set_high_fence(high_fence);
        fbt_page.run_consistency_checks(true);

        fbt_page.insert("b".as_bytes(), "bb".as_bytes(), true);
        fbt_page.run_consistency_checks(true);
        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), true);
        fbt_page.run_consistency_checks(true);
        fbt_page.compact_space();
        fbt_page.run_consistency_checks(true);
        assert_eq!(fbt_page.slot_count(), 4); // 2 records + 2 fence slots
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("b")), 0);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("b")), 2);
        assert_eq!(fbt_page.lower_bound_slot_id(&BTreeKey::str("c")), 2);
        assert_eq!(fbt_page.upper_bound_slot_id(&BTreeKey::str("c")), 3);
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("b")), Some(1));
        assert_eq!(fbt_page.get_val(1), "bb".as_bytes());
        assert_eq!(fbt_page.find_slot_id(&BTreeKey::str("c")), Some(2));
        assert_eq!(fbt_page.get_val(2), "cc".as_bytes());
        assert!(fbt_page.is_ghost(1));
        assert!(fbt_page.is_ghost(2));
        fbt_page.unghostify_at(1);
        fbt_page.unghostify_at(2);
        fbt_page.run_consistency_checks(true);
        assert!(!fbt_page.is_ghost(1));
        assert!(!fbt_page.is_ghost(2));
    }
}
