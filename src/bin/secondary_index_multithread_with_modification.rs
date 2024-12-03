// Define two indices.
// One index is a primary index and the other is a secondary index.
// The primary index stores (key_1, value) pairs.
// The secondary index stores (key_2, key_1) pairs.

// We experiment with two access methods.
// The first access method is secondary_index_logical.
// The second access method is secondary_index_lipah.
// The logical access method uses the secondary index to find the key
// of the primary index and then uses the primary index to find the value.
// The lipah access method uses the secondary index to find both
// (logical_id, physical_address) pairs and then first uses
// the physical address to directly fetch the value from the page.
// If the value is not found due to relocation, the logical_id is used
// to find the value.
// We compare the speed of the two access methods without any relocation.

use criterion::black_box;
use fbtree::{
    access_method::fbt::{BTreeKey, FosterBtreeCursor},
    bp::{self, ContainerId, ContainerKey, MemPool, PageFrameKey},
    prelude::{FosterBtree, FosterBtreePage, PageId},
    utils::Permutation,
};

use clap::Parser;
use fbtree::{
    access_method::{AccessMethodError, UniqueKeyIndex},
    bp::{get_test_bp, BufferPool},
    random::gen_random_byte_vec,
};
use rand::prelude::Distribution;
use rand::Rng;
use std::{
    fs::File,
    io::Write,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub trait SecondaryIndex<T: MemPool>: Sync + Send {
    fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> Self;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError>;
    fn stats(&self) -> String;
}

pub struct SecondaryNoHint<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryIndex<T> for SecondaryNoHint<T> {
    fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryNoHint<T> {
        // Iterate through the table to create the secondary index.
        let mut iter = FosterBtreeCursor::new(primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::new(
            ContainerKey::new(0, c_id),
            primary.mem_pool.clone(),
        ));
        while let Some((p_key, _)) = iter.get_kv() {
            // Concatenate the primary key with the physical address
            let val = p_key.to_vec();
            secondary.insert(&p_key, &val).unwrap();
            iter.go_to_next_kv();
        }
        SecondaryNoHint {
            primary: primary.clone(),
            secondary,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        // let val = self.secondary.get(key)?;
        // self.primary.get(&val)
        let sec_leaf_page = self
            .secondary
            .traverse_to_leaf_for_read_with_hint(key, None);
        let sec_slot_id = sec_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
        if sec_slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else if sec_leaf_page.get_raw_key(sec_slot_id) == key {
            // Logical ID is the key in the primary FBT
            // Physical address is the page_id of the page that contains the value in the primary FBT
            let p_key = sec_leaf_page.get_val(sec_slot_id);

            let pri_page = self
                .primary
                .traverse_to_leaf_for_read_with_hint(p_key, None);
            let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
            if pri_slot_id == 0 {
                // Lower fence. Non-existent key
                Err(AccessMethodError::KeyNotFound)
            } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                Ok(pri_page.get_val(pri_slot_id).to_vec())
            } else {
                // Non-existent key
                Err(AccessMethodError::KeyNotFound)
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn stats(&self) -> String {
        format!(
            "[No hint] secondary index: \n{}",
            self.secondary.page_stats(false)
        )
    }
}

pub struct SecondaryLeafPageHint<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryIndex<T> for SecondaryLeafPageHint<T> {
    fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryLeafPageHint<T> {
        // Iterate through the table to create the secondary index.
        let mut iter = FosterBtreeCursor::new(primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::new(
            ContainerKey::new(0, c_id),
            primary.mem_pool.clone(),
        ));
        while let Some((p_key, _)) = iter.get_kv() {
            let (page_id, _, _) = iter.get_physical_address();
            // Concatenate the primary key with the physical address
            let mut val = p_key.to_vec();
            val.extend_from_slice(&page_id.to_be_bytes());
            secondary.insert(&p_key, &val).unwrap();
            iter.go_to_next_kv();
        }
        SecondaryLeafPageHint {
            primary: primary.clone(),
            secondary,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let sec_leaf_page = self
            .secondary
            .traverse_to_leaf_for_read_with_hint(key, None);
        let sec_slot_id = sec_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
        if sec_slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else if sec_leaf_page.get_raw_key(sec_slot_id) == key {
            // Logical ID is the key in the primary FBT
            // Physical address is the page_id of the page that contains the value in the primary FBT
            let val = sec_leaf_page.get_val(sec_slot_id);

            // Split the value into logical_id and physical_address
            let (p_key, expected_phys_addr) = val.split_at(val.len() - 4);
            let expected_page_id = PageId::from_be_bytes(expected_phys_addr.try_into().unwrap());
            let expected_page_frame_key = PageFrameKey::new(self.primary.c_key, expected_page_id);

            // Traverse the primary FBT with the hint
            let pri_page = self
                .primary
                .traverse_to_leaf_for_read_with_hint(p_key, Some(expected_page_frame_key));
            let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
            let result = if pri_slot_id == 0 {
                // Lower fence. Non-existent key
                return Err(AccessMethodError::KeyNotFound);
            } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                pri_page.get_val(pri_slot_id).to_vec()
            } else {
                // Non-existent key
                return Err(AccessMethodError::KeyNotFound);
            };

            let actual_page_id = pri_page.get_id();
            if actual_page_id == expected_page_id {
                // No relocation
                Ok(result)
            } else {
                let mut val = val.to_vec();
                match sec_leaf_page.try_upgrade(false) {
                    Ok(mut sec_write_page) => {
                        // Modify the last 4 bytes of the value to the new page_id
                        let val_len = val.len();
                        val[val_len - 4..].copy_from_slice(&actual_page_id.to_be_bytes());
                        let res = sec_write_page.update_at(sec_slot_id, None, &val);
                        assert!(res);
                        Ok(result)
                    }
                    Err(_) => {
                        // Someone else is writing to the page. Do it in the next iteration.
                        Ok(result)
                    }
                }
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn stats(&self) -> String {
        format!(
            "[Page] hint secondary index: \n{}",
            self.secondary.page_stats(false)
        )
    }
}

pub struct SecondaryLeafPageFrameHint<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryIndex<T> for SecondaryLeafPageFrameHint<T> {
    fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryLeafPageFrameHint<T> {
        // Iterate through the table to create the secondary index.
        let mut iter = FosterBtreeCursor::new(primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::new(
            ContainerKey::new(0, c_id),
            primary.mem_pool.clone(),
        ));
        while let Some((p_key, _)) = iter.get_kv() {
            let (page_id, frame_id, _) = iter.get_physical_address();
            // Concatenate the primary key with the physical address
            let mut val = p_key.to_vec();
            val.extend_from_slice(&page_id.to_be_bytes());
            val.extend_from_slice(&frame_id.to_be_bytes());
            secondary.insert(&p_key, &val).unwrap();
            iter.go_to_next_kv();
        }
        SecondaryLeafPageFrameHint {
            primary: primary.clone(),
            secondary,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let sec_leaf_page = self
            .secondary
            .traverse_to_leaf_for_read_with_hint(key, None);
        let sec_slot_id = sec_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
        if sec_slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else if sec_leaf_page.get_raw_key(sec_slot_id) == key {
            // Logical ID is the key in the primary FBT
            // Physical address is the page_id of the page that contains the value in the primary FBT
            let val = sec_leaf_page.get_val(sec_slot_id);

            // Split the value into logical_id and physical_address
            let (p_key, expected_phys_addr) = val.split_at(val.len() - 8);
            let (expected_page_id, expected_frame_id) = expected_phys_addr.split_at(4);
            let expected_page_id = PageId::from_be_bytes(expected_page_id.try_into().unwrap());
            let expected_frame_id = u32::from_be_bytes(expected_frame_id.try_into().unwrap());
            let expected_page_frame_key = PageFrameKey::new_with_frame_id(
                self.primary.c_key,
                expected_page_id,
                expected_frame_id,
            );

            // Traverse the primary FBT with the hint
            let pri_page = self
                .primary
                .traverse_to_leaf_for_read_with_hint(p_key, Some(expected_page_frame_key));
            let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
            let result = if pri_slot_id == 0 {
                // Lower fence. Non-existent key
                return Err(AccessMethodError::KeyNotFound);
            } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                pri_page.get_val(pri_slot_id).to_vec()
            } else {
                // Non-existent key
                return Err(AccessMethodError::KeyNotFound);
            };

            let (actual_page_id, actual_frame_id) = (pri_page.get_id(), pri_page.frame_id());
            if actual_page_id == expected_page_id && actual_frame_id == expected_frame_id {
                // No relocation
                Ok(result)
            } else {
                let mut val = val.to_vec();
                match sec_leaf_page.try_upgrade(false) {
                    Ok(mut sec_write_page) => {
                        // Modify the last 4 bytes of the value to the new page_id
                        let val_len = val.len();
                        val[val_len - 8..val_len - 4]
                            .copy_from_slice(&actual_page_id.to_be_bytes());
                        val[val_len - 4..].copy_from_slice(&actual_frame_id.to_be_bytes());
                        let res = sec_write_page.update_at(sec_slot_id, None, &val);
                        assert!(res);
                        Ok(result)
                    }
                    Err(_) => {
                        // Someone else is writing to the page. Do it in the next iteration.
                        Ok(result)
                    }
                }
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn stats(&self) -> String {
        format!(
            "[Page, Frame] hint secondary index: \n{}",
            self.secondary.page_stats(false)
        )
    }
}

pub struct SecondaryPageSlotHint<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryIndex<T> for SecondaryPageSlotHint<T> {
    fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryPageSlotHint<T> {
        // Iterate through the table to create the secondary index.
        let mut iter = FosterBtreeCursor::new(primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::new(
            ContainerKey::new(0, c_id),
            primary.mem_pool.clone(),
        ));
        while let Some((p_key, _)) = iter.get_kv() {
            let (page_id, _frame_id, slot_id) = iter.get_physical_address();
            // Concatenate the primary key with the physical address
            let mut val = p_key.to_vec();
            val.extend_from_slice(&page_id.to_be_bytes());
            val.extend_from_slice(&slot_id.to_be_bytes());
            secondary.insert(&p_key, &val).unwrap();
            iter.go_to_next_kv();
        }
        SecondaryPageSlotHint {
            primary: primary.clone(),
            secondary,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let sec_leaf_page = self
            .secondary
            .traverse_to_leaf_for_read_with_hint(key, None);
        let sec_slot_id = sec_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
        if sec_slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else if sec_leaf_page.get_raw_key(sec_slot_id) == key {
            // Logical ID is the key in the primary FBT
            // Physical address is the page_id of the page that contains the value in the primary FBT
            let val = sec_leaf_page.get_val(sec_slot_id);

            // Split the value into logical_id and physical_address
            let (p_key, expected_phys_addr) = val.split_at(val.len() - 8);
            let (expected_page_id, expected_slot_id) = expected_phys_addr.split_at(4);
            let expected_page_id = PageId::from_be_bytes(expected_page_id.try_into().unwrap());
            let expected_slot_id = u32::from_be_bytes(expected_slot_id.try_into().unwrap());
            let expected_page_key = PageFrameKey::new(self.primary.c_key, expected_page_id);

            // Traverse the primary FBT with the hint
            let pri_page = self
                .primary
                .traverse_to_leaf_for_read_with_hint(p_key, Some(expected_page_key));

            // Check if expected slot_id is the same as the slot id
            let (result, actual_slot_id) = if pri_page.get_id() == expected_page_id
                && pri_page.low_fence_slot_id() < expected_slot_id
                && expected_slot_id < pri_page.high_fence_slot_id()
                && pri_page.get_raw_key(expected_slot_id) == p_key
            {
                (
                    pri_page.get_val(expected_slot_id).to_vec(),
                    expected_slot_id,
                )
            } else {
                let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
                if pri_slot_id == 0 {
                    // Lower fence. Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                    (pri_page.get_val(pri_slot_id).to_vec(), pri_slot_id)
                } else {
                    // Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                }
            };

            let actual_page_id = pri_page.get_id();
            if actual_page_id == expected_page_id && actual_slot_id == expected_slot_id {
                // No relocation
                Ok(result)
            } else {
                let mut val = val.to_vec();
                match sec_leaf_page.try_upgrade(false) {
                    Ok(mut write_leaf_page) => {
                        // Modify the last 8 bytes of the value to the new page_id and slot_id
                        let val_len = val.len();
                        val[val_len - 8..val_len - 4]
                            .copy_from_slice(&actual_page_id.to_be_bytes());
                        val[val_len - 4..].copy_from_slice(&actual_slot_id.to_be_bytes());
                        let res = write_leaf_page.update_at(sec_slot_id, None, &val);
                        assert!(res);
                        Ok(result)
                    }
                    Err(_) => {
                        // Someone else is writing to the page. Do it in the next iteration.
                        Ok(result)
                    }
                }
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn stats(&self) -> String {
        format!(
            "[Page, Slot] hint secondary index: \n{}",
            self.secondary.page_stats(false)
        )
    }
}

pub struct SecondaryPageFrameSlotHint<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryIndex<T> for SecondaryPageFrameSlotHint<T> {
    fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryPageFrameSlotHint<T> {
        // Iterate through the table to create the secondary index.
        let mut iter = FosterBtreeCursor::new(primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::new(
            ContainerKey::new(0, c_id),
            primary.mem_pool.clone(),
        ));
        while let Some((p_key, _)) = iter.get_kv() {
            let (page_id, frame_id, slot_id) = iter.get_physical_address();
            // Concatenate the primary key with the physical address
            let mut val = p_key.to_vec();
            val.extend_from_slice(&page_id.to_be_bytes());
            val.extend_from_slice(&frame_id.to_be_bytes());
            val.extend_from_slice(&slot_id.to_be_bytes());
            secondary.insert(&p_key, &val).unwrap();
            iter.go_to_next_kv();
        }
        SecondaryPageFrameSlotHint {
            primary: primary.clone(),
            secondary,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let sec_leaf_page = self
            .secondary
            .traverse_to_leaf_for_read_with_hint(key, None);
        let sec_slot_id = sec_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
        if sec_slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else if sec_leaf_page.get_raw_key(sec_slot_id) == key {
            // Logical ID is the key in the primary FBT
            // Physical address is the page_id of the page that contains the value in the primary FBT
            let val = sec_leaf_page.get_val(sec_slot_id);

            // Split the value into logical_id and physical_address
            let (p_key, expected_phys_addr) = val.split_at(val.len() - 12);
            let (expected_page_id, expected_frame_slot_id) = expected_phys_addr.split_at(4);
            let expected_page_id = PageId::from_be_bytes(expected_page_id.try_into().unwrap());
            let (expected_frame_id, expected_slot_id) = expected_frame_slot_id.split_at(4);
            let expected_frame_id = u32::from_be_bytes(expected_frame_id.try_into().unwrap());
            let expected_slot_id = u32::from_be_bytes(expected_slot_id.try_into().unwrap());

            let expected_page_frame_key = PageFrameKey::new_with_frame_id(
                self.primary.c_key,
                expected_page_id,
                expected_frame_id,
            );

            // Traverse the primary FBT with the hint
            let pri_page = self
                .primary
                .traverse_to_leaf_for_read_with_hint(p_key, Some(expected_page_frame_key));

            // Check if expected slot_id is the same as the slot id
            let (result, actual_slot_id) = if pri_page.get_id() == expected_page_id
                && pri_page.low_fence_slot_id() < expected_slot_id
                && expected_slot_id < pri_page.high_fence_slot_id()
                && pri_page.get_raw_key(expected_slot_id) == p_key
            {
                (
                    pri_page.get_val(expected_slot_id).to_vec(),
                    expected_slot_id,
                )
            } else {
                let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
                if pri_slot_id == 0 {
                    // Lower fence. Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                    (pri_page.get_val(pri_slot_id).to_vec(), pri_slot_id)
                } else {
                    // Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                }
            };

            let (actual_page_id, actual_frame_id) = (pri_page.get_id(), pri_page.frame_id());
            if actual_page_id == expected_page_id
                && actual_frame_id == expected_frame_id
                && actual_slot_id == expected_slot_id
            {
                // No relocation
                Ok(result)
            } else {
                let mut val = val.to_vec();
                match sec_leaf_page.try_upgrade(false) {
                    Ok(mut write_leaf_page) => {
                        // Modify the last 12 bytes of the value to the new page_id, frame_id and slot_id
                        let val_len = val.len();
                        val[val_len - 12..val_len - 8]
                            .copy_from_slice(&actual_page_id.to_be_bytes());
                        val[val_len - 8..val_len - 4]
                            .copy_from_slice(&actual_frame_id.to_be_bytes());
                        val[val_len - 4..].copy_from_slice(&actual_slot_id.to_be_bytes());
                        let res = write_leaf_page.update_at(sec_slot_id, None, &val);
                        assert!(res);
                        Ok(result)
                    }
                    Err(_) => {
                        // Someone else is writing to the page. Do it in the next iteration.
                        Ok(result)
                    }
                }
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn stats(&self) -> String {
        format!(
            "[Page, Frame, Slot] hint secondary index: \n{}",
            self.secondary.page_stats(false)
        )
    }
}
#[derive(Debug, Parser, Clone)]
pub struct SecBenchParams {
    /// Number of threads
    #[clap(short = 't', long, default_value = "1")]
    pub num_threads: usize,
    /// Buffer pool size. if 0 panic
    #[clap(short = 'b', long, default_value = "100000")]
    pub bp_size: usize,
    /// Number of records.
    #[clap(short = 'n', long, default_value = "100000")]
    pub num_keys: usize,
    /// Key size
    #[clap(short = 'k', long, default_value = "50")]
    pub key_size: usize,
    /// Record size
    #[clap(short = 'r', long, default_value = "100")]
    pub record_size: usize,
    /// Skew factor
    #[clap(short = 's', long, default_value = "0.0")]
    pub skew_factor: f64,
    /// Warmup time in seconds
    #[clap(short = 'd', long, default_value = "10")]
    pub warmup_time: usize,
    /// Execution time in seconds
    #[clap(short = 'D', long, default_value = "10")]
    pub exec_time: usize,
}

fn get_key_bytes(key: usize, key_size: usize) -> Vec<u8> {
    if key_size < std::mem::size_of::<usize>() {
        panic!("Key size is less than the size of usize");
    }
    let mut key_vec = vec![0u8; key_size];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[key_size - bytes.len()..].copy_from_slice(&bytes);
    key_vec
}

fn from_key_bytes(key: &[u8]) -> usize {
    // The last 8 bytes of the key is the key

    usize::from_be_bytes(
        key[key.len() - std::mem::size_of::<usize>()..]
            .try_into()
            .unwrap(),
    )
}

fn get_key(num_keys: usize, skew_factor: f64) -> usize {
    let mut rng = rand::thread_rng();
    if skew_factor <= 0f64 {
        rng.gen_range(0..num_keys)
    } else {
        let zipf = zipf::ZipfDistribution::new(num_keys, skew_factor).unwrap();
        let sample = zipf.sample(&mut rng);
        sample - 1
    }
}

fn get_new_value(value_size: usize) -> Vec<u8> {
    gen_random_byte_vec(value_size, value_size)
}

pub struct KeyValueGenerator {
    key_size: usize,
    value_size: usize,
    start_key: usize, // Inclusive
    end_key: usize,   // Exclusive
}

impl KeyValueGenerator {
    pub fn new(partition: usize, num_keys: usize, key_size: usize, value_size: usize) -> Vec<Self> {
        // Divide the keys equally among the partitions and
        // assign the remaining keys to the last partition
        let num_keys_per_partition = num_keys / partition;
        let mut generators = Vec::new();
        let mut count = 0;
        for i in 0..partition {
            let start_key = count;
            let end_key = if i == partition - 1 {
                num_keys
            } else {
                count + num_keys_per_partition
            };
            count = end_key;

            generators.push(Self {
                key_size,
                value_size,
                start_key,
                end_key,
            });
        }
        generators
    }
}

impl Iterator for KeyValueGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_key >= self.end_key {
            return None;
        }
        let key = get_key_bytes(self.start_key, self.key_size);
        let value = gen_random_byte_vec(self.value_size, self.value_size);
        self.start_key += 1;

        Some((key, value))
    }
}

pub fn load_table(params: &SecBenchParams, table: &Arc<FosterBtree<BufferPool>>) {
    let num_insertion_threads = 6;

    let mut gen = KeyValueGenerator::new(
        num_insertion_threads,
        params.num_keys,
        params.key_size,
        params.record_size,
    );

    // Multi-thread insert. Use 6 threads to insert the keys.
    std::thread::scope(|s| {
        for _ in 0..6 {
            let table = table.clone();
            let key_gen = gen.pop().unwrap();
            s.spawn(move || {
                for (key, value) in key_gen {
                    table.insert(&key, &value).unwrap();
                }
            });
        }
    });
}

fn bench_secondary<M: MemPool, T: SecondaryIndex<M>>(
    params: &SecBenchParams,
    secondary: &T,
    bp: &Arc<M>,
) -> u128 {
    println!("{}", secondary.stats());

    // Keep calling get on the secondary index to measure the speed.
    let warmup = params.warmup_time;
    let exec = params.exec_time;

    let mut avg = 0;
    for i in 0..warmup + exec {
        let bp_stats_pre = bp.stats();
        let perm = Permutation::new(0, params.num_keys - 1);
        let start = std::time::Instant::now();
        for key in perm {
            let key_bytes = get_key_bytes(key, params.key_size);
            let result = secondary.get(&key_bytes).unwrap();
            black_box(result);
        }
        // for _ in 0..params.num_keys {
        //     let key = get_key(params.num_keys, params.skew_factor);
        //     let key_bytes = get_key_bytes(key, params.key_size);
        //     let result = secondary.get(&key_bytes).unwrap();
        //     black_box(result);
        // }
        let elapsed = start.elapsed();
        let bp_stats_post = bp.stats();
        let diff = bp_stats_post.diff(&bp_stats_pre);
        let name = if i < warmup { "Warmup" } else { "Execution" };
        println!(
            "Iteration({}) {}: time: {:?}, bp_stats: {}",
            name, i, elapsed, diff
        );
        if i >= warmup {
            avg += elapsed.as_millis();
        }
    }
    avg /= exec as u128;
    println!("Average time: {} ms", avg);
    avg
}

fn bench_multi_thread<M: MemPool, T: SecondaryIndex<M>>(
    params: &SecBenchParams,
    primary: &Arc<FosterBtree<M>>,
    bp: &Arc<M>,
) -> Vec<usize> {
    // Create the #threads secondary indexes
    let secondary = T::new(&primary, 100 as ContainerId);

    // Print the bp stats
    let stats_after_secondary = bp.stats();
    println!(
        "BP stats after secondary index creation: \n{}",
        &stats_after_secondary
    );

    let flag = AtomicBool::new(true);
    // Run warmups first
    let result = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(params.num_threads);
        for _thread_id in 0..params.num_threads {
            let flag = &flag;
            let params = params.clone();
            let secondary = &secondary;
            let handle = s.spawn(move || per_thread_bench(flag, &params, secondary));
            handles.push(handle);
        }

        std::thread::sleep(std::time::Duration::from_secs(params.warmup_time as u64));
        flag.store(false, Ordering::Relaxed);

        let mut results = Vec::with_capacity(params.num_threads);
        for handle in handles {
            results.push(handle.join().unwrap());
        }
        results
    });

    println!("Warmup results: {:?}", result);
    let stats_after_warmup = bp.stats();
    let diff = stats_after_warmup.diff(&stats_after_secondary);
    println!("BP stats diff after warmup: \n{}", diff);

    flag.store(true, Ordering::Relaxed);

    let results = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(params.num_threads);
        for _thread_id in 0..params.num_threads {
            let flag = &flag;
            let params = params.clone();
            let secondary = &secondary;
            let handle = s.spawn(move || per_thread_bench(flag, &params, secondary));
            handles.push(handle);
        }
        std::thread::sleep(std::time::Duration::from_secs(params.exec_time as u64));
        flag.store(false, Ordering::Relaxed);

        let mut results = Vec::with_capacity(params.num_threads);
        for handle in handles {
            results.push(handle.join().unwrap());
        }
        results
    });

    println!("Execution results: {:?}", results);
    let stats_after_exec = bp.stats();
    let diff = stats_after_exec.diff(&stats_after_warmup);
    println!("BP stats diff after execution: \n{}", diff);

    results
}

fn per_thread_bench<T: SecondaryIndex<M>, M: MemPool>(
    flag: &AtomicBool, // While true, run the benchmark
    params: &SecBenchParams,
    secondary: &T,
) -> usize {
    let mut count = 0;
    while flag.load(Ordering::Relaxed) {
        let key = get_key(params.num_keys, params.skew_factor);
        let key_bytes = get_key_bytes(key, params.key_size);
        let result = secondary.get(&key_bytes).unwrap();
        black_box(result);
        count += 1;
    }
    count
}

fn flush_internal_cache_and_everything() {
    // Sync the file system to flush pending writes
    if let Err(e) = sync_filesystem() {
        eprintln!("Error syncing filesystem: {}", e);
    }
}

fn sync_filesystem() -> Result<(), std::io::Error> {
    let status = Command::new("sync").status()?;
    if !status.success() {
        eprintln!("sync command failed");
    }
    Ok(())
}

fn main() {
    let params = SecBenchParams::parse();
    println!("Params: {:?}", params);

    // if sec_bench_normal is specified, or nothing is specified
    #[cfg(feature = "sec_bench_no_hint")]
    {
        flush_internal_cache_and_everything();
        println!("=========================================================================================");
        let bp = get_test_bp(params.bp_size);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
        load_table(&params, &primary);
        // Print the page stats
        println!("BP stats: \n{}", bp.stats());
        println!("Tree stats: \n{}", primary.page_stats(false));
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("No hint");
        // Multi-threaded benchmark
        let result =
            bench_multi_thread::<BufferPool, SecondaryNoHint<BufferPool>>(&params, &primary, &bp);
        println!("Summary");
        // Sum the results from all the threads and divide it by the execution time
        let sum: usize = result.iter().sum();
        let avg = sum as f64 / params.exec_time as f64;
        println!("System Throughput: {} ops/sec", avg);
        println!(
            "Per thread Throughput: {:?} ops/sec",
            avg / params.num_threads as f64
        );
        println!("=========================================================================================");
    }

    #[cfg(feature = "sec_bench_page_hint")]
    {
        flush_internal_cache_and_everything();
        println!("=========================================================================================");
        let bp = get_test_bp(params.bp_size);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
        load_table(&params, &primary);
        // Print the page stats
        println!("BP stats: \n{}", bp.stats());
        println!("Tree stats: \n{}", primary.page_stats(false));
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("[Page] hint");
        // Multi-threaded benchmark
        let result = bench_multi_thread::<BufferPool, SecondaryLeafPageHint<BufferPool>>(
            &params, &primary, &bp,
        );
        println!("Summary");
        // Sum the results from all the threads and divide it by the execution time
        let sum: usize = result.iter().sum();
        let avg = sum as f64 / params.exec_time as f64;
        println!("System Throughput: {} ops/sec", avg);
        println!(
            "Per thread Throughput: {:?} ops/sec",
            avg / params.num_threads as f64
        );
        println!("=========================================================================================");
    }

    #[cfg(feature = "sec_bench_page_frame_hint")]
    {
        flush_internal_cache_and_everything();
        println!("=========================================================================================");
        let bp = get_test_bp(params.bp_size);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
        load_table(&params, &primary);
        // Print the page stats
        println!("BP stats: \n{}", bp.stats());
        println!("Tree stats: \n{}", primary.page_stats(false));
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("[Page, Frame] hint");
        // Multi-threaded benchmark
        let result = bench_multi_thread::<BufferPool, SecondaryLeafPageFrameHint<BufferPool>>(
            &params, &primary, &bp,
        );
        println!("Summary");
        // Sum the results from all the threads and divide it by the execution time
        let sum: usize = result.iter().sum();
        let avg = sum as f64 / params.exec_time as f64;
        println!("System Throughput: {} ops/sec", avg);
        println!(
            "Per thread Throughput: {:?} ops/sec",
            avg / params.num_threads as f64
        );
        println!("=========================================================================================");
    }

    #[cfg(feature = "sec_bench_page_slot_hint")]
    {
        println!("=========================================================================================");
        let bp = get_test_bp(params.bp_size);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
        load_table(&params, &primary);
        // Print the page stats
        println!("BP stats: \n{}", bp.stats());
        println!("Tree stats: \n{}", primary.page_stats(false));
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("[Page, Slot] hint");
        // Multi-threaded benchmark
        let result = bench_multi_thread::<BufferPool, SecondaryPageSlotHint<BufferPool>>(
            &params, &primary, &bp,
        );
        println!("Summary");
        // Sum the results from all the threads and divide it by the execution time
        let sum: usize = result.iter().sum();
        let avg = sum as f64 / params.exec_time as f64;
        println!("System Throughput: {} ops/sec", avg);
        println!(
            "Per thread Throughput: {:?} ops/sec",
            avg / params.num_threads as f64
        );
        println!("=========================================================================================");
    }

    #[cfg(feature = "sec_bench_page_frame_slot_hint")]
    {
        flush_internal_cache_and_everything();
        println!("=========================================================================================");
        let bp = get_test_bp(params.bp_size);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
        load_table(&params, &primary);
        // Print the page stats
        println!("BP stats: \n{}", bp.stats());
        println!("Tree stats: \n{}", primary.page_stats(false));
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("[Page, Frame, Slot] hint");
        // Multi-threaded benchmark
        let result = bench_multi_thread::<BufferPool, SecondaryPageFrameSlotHint<BufferPool>>(
            &params, &primary, &bp,
        );
        println!("Summary");
        // Sum the results from all the threads and divide it by the execution time
        let sum: usize = result.iter().sum();
        let avg = sum as f64 / params.exec_time as f64;
        println!("System Throughput: {} ops/sec", avg);
        println!(
            "Per thread Throughput: {:?} ops/sec",
            avg / params.num_threads as f64
        );
        println!("=========================================================================================");
    }
}
