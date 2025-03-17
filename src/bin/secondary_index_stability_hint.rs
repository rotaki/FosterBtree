use fbtree::{
    access_method::fbt::{BTreeKey, FosterBtreeCursor},
    bp::{ContainerId, ContainerKey, MemPool, PageFrameKey},
    prelude::{FosterBtree, FosterBtreePage, PageId},
    utils::Permutation,
};

use clap::Parser;
use fbtree::{
    access_method::UniqueKeyIndex,
    bp::{get_test_bp, BufferPool},
    random::gen_random_byte_vec,
};
use std::{process::Command, sync::Arc};

pub struct SecondaryIndex<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryIndex<T> {
    pub fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryIndex<T> {
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
        SecondaryIndex {
            primary: primary.clone(),
            secondary,
        }
    }

    /*
    pub fn get(&self, key: &[u8]) -> Result<*const u8, AccessMethodError> {
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
                    pri_page.get_val(expected_slot_id).as_ptr(),
                    expected_slot_id,
                )
            } else {
                let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
                if pri_slot_id == 0 {
                    // Lower fence. Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                    (pri_page.get_val(pri_slot_id).as_ptr(), pri_slot_id)
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
                // let fake_page_id = actual_page_id.saturating_sub(1);
                // let fake_frame_id = actual_frame_id.saturating_sub(1);
                // let fake_slot_id = actual_slot_id.saturating_sub(1);
                let val_len = val.len();
                let val: *const u8 = val.as_ptr();
                match sec_leaf_page.try_upgrade(false) {
                    Ok(write_leaf_page) => {
                        unsafe {
                            // Modify the last 4 bytes of the value to the new page_id
                            let start = val.add(val_len - 12) as *mut u8;
                            std::ptr::copy_nonoverlapping(
                                &expected_page_id.to_be_bytes()[0],
                                start,
                                4,
                            );
                            // Modify the next 4 bytes of the value to the new frame_id
                            let start = val.add(val_len - 8) as *mut u8;
                            std::ptr::copy_nonoverlapping(
                                &expected_frame_id.to_be_bytes()[0],
                                start,
                                4,
                            );
                            // Modify the next 4 bytes of the value to the new slot_id
                            let start = val.add(val_len - 4) as *mut u8;
                            std::ptr::copy_nonoverlapping(
                                &expected_slot_id.to_be_bytes()[0],
                                start,
                                4,
                            );
                        }
                        drop(write_leaf_page);
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
    */

    /*
    pub fn scan(&self) -> Result<(usize, usize, usize), AccessMethodError> {
        // Returns (result_hash, total_count, repair_count)
        let mut result_hash = 0;
        let mut total_count = 0;
        let mut repair_count = 0;
        let mut cursor = FosterBtreeCursor::new(&self.secondary, &[], &[]);
        while let Some((_s_key, s_value)) = cursor.get_kv() {
            // Parse the s_value
            let (p_key, expected_phys_addr) = s_value.split_at(s_value.len() - 12);
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
                    pri_page.get_val(expected_slot_id).as_ptr(),
                    expected_slot_id,
                )
            } else {
                let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
                if pri_slot_id == 0 {
                    // Lower fence. Non-existent key
                    return Err(AccessMethodError::KeyNotFound);
                } else if pri_page.get_raw_key(pri_slot_id) == p_key {
                    (pri_page.get_val(pri_slot_id).as_ptr(), pri_slot_id)
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
            } else {
                if self.repair_type.always_repair {
                    repair_count += 1;
                    // Repair always
                    let actual_page_id = actual_page_id.to_be_bytes();
                    let actual_frame_id = actual_frame_id.to_be_bytes();
                    let actual_slot_id = actual_slot_id.to_be_bytes();
                    let new_s_val =
                        [p_key, &actual_page_id, &actual_frame_id, &actual_slot_id].concat();
                    cursor.opportunistic_update(&new_s_val, false);
                } else if self.repair_type.ignore_slot
                    && (actual_page_id != expected_page_id || actual_frame_id != expected_frame_id)
                {
                    repair_count += 1;
                    // Repair only if the page id or frame id is different
                    let actual_page_id = actual_page_id.to_be_bytes();
                    let actual_frame_id = actual_frame_id.to_be_bytes();
                    let actual_slot_id = actual_slot_id.to_be_bytes();
                    let new_s_val =
                        [p_key, &actual_page_id, &actual_frame_id, &actual_slot_id].concat();
                    cursor.opportunistic_update(&new_s_val, false);
                } else if self.repair_type.ignore_all {
                    // Do nothing.
                }
            }
            result_hash ^= result as usize;
            total_count += 1;

            cursor.go_to_next_kv();
        }
        Ok((result_hash, total_count, repair_count))
    }
    */

    pub fn stats(&self) -> String {
        format!(
            "[Page, Frame, Slot] hint secondary index: \n{}",
            self.secondary.page_stats(false)
        )
    }

    pub fn check_hint_correctness(&self) -> String {
        // Iterate through the secondary index. Compute the total number of correct hints / total number of hints.
        let mut iter = FosterBtreeCursor::new(&self.secondary, &[], &[]);
        let mut total_hints = 0;
        let mut correct_page_hints = 0;
        let mut correct_frame_hints = 0;
        let mut correct_slot_hints = 0;
        while let Some((_s_key, v)) = iter.get_kv() {
            let (p_key, expected_phys_addr) = v.split_at(v.len() - 12);
            let (expected_page_id, expected_frame_slot_id) = expected_phys_addr.split_at(4);
            let expected_page_id = PageId::from_be_bytes(expected_page_id.try_into().unwrap());
            let (expected_frame_id, expected_slot_id) = expected_frame_slot_id.split_at(4);
            let expected_frame_id = u32::from_be_bytes(expected_frame_id.try_into().unwrap());
            let expected_slot_id = u32::from_be_bytes(expected_slot_id.try_into().unwrap());

            // Now, check if the expected frame id contains the expected page id
            let actual_frame_id = self
                .primary
                .mem_pool
                .get_page_for_read(PageFrameKey::new_with_frame_id(
                    self.primary.c_key,
                    expected_page_id,
                    expected_frame_id,
                ))
                .unwrap()
                .frame_id();
            if actual_frame_id == expected_frame_id {
                correct_frame_hints += 1;
            }

            // Now, check if the page contains the expected key.
            let pri_page = self.primary.traverse_to_leaf_for_read_with_hint(
                p_key,
                Some(PageFrameKey::new_with_frame_id(
                    self.primary.c_key,
                    expected_page_id,
                    expected_frame_id,
                )),
            );
            if pri_page.get_id() == expected_page_id {
                correct_page_hints += 1;
            }

            // Now, check if the slot id is correct
            let pri_slot_id = pri_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
            if pri_slot_id == expected_slot_id {
                correct_slot_hints += 1;
            }

            total_hints += 1;
            iter.go_to_next_kv();
        }
        format!(
            "Total hints: {}, Correct page hints: {} ({:.2}%), Correct frame hints: {} ({:.2}%), Correct slot hints: {} ({:.2}%)",
            total_hints,
            correct_page_hints,
            (correct_page_hints as f64 / total_hints as f64) * 100.0,
            correct_frame_hints,
            (correct_frame_hints as f64 / total_hints as f64) * 100.0,
            correct_slot_hints,
            (correct_slot_hints as f64 / total_hints as f64) * 100.0,
        )
    }
}
#[derive(Debug, Parser, Clone)]
pub struct SecBenchParams {
    /// Buffer pool size. if 0 panic
    #[clap(short, long, default_value = "100000")]
    pub bp_size: usize,
    /// Number of records.
    #[clap(short, long, default_value = "100000")]
    pub num_keys: usize,
    /// Key size
    #[clap(short, long, default_value = "50")]
    pub key_size: usize,
    /// Record size
    #[clap(short, long, default_value = "100")]
    pub record_size: usize,
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

fn get_new_value(value_size: usize) -> Vec<u8> {
    gen_random_byte_vec(value_size, value_size)
}

// Insert num_keys keys into the table
pub fn load_table(
    params: &SecBenchParams,
    table: &Arc<FosterBtree<BufferPool>>,
    iter: impl Iterator<Item = usize>,
) {
    for key in iter {
        let key = get_key_bytes(key, params.key_size);
        let value = get_new_value(params.record_size);
        table.insert(&key, &value).unwrap();
    }
}

pub fn insert<T: MemPool>(
    primary: &Arc<FosterBtree<T>>,
    secondary: &Arc<FosterBtree<T>>,
    key: &[u8],
    value: &[u8],
) {
    // Insert into the primary index and then into the secondary index.
    primary.insert(key, value).unwrap();
    let iter = FosterBtreeCursor::new(primary, key, &[]);
    if let Some((p_key, _)) = iter.get_kv() {
        assert_eq!(p_key, key);
        let (page_id, frame_id, slot_id) = iter.get_physical_address();
        let mut s_val = p_key.to_vec();
        s_val.extend_from_slice(&page_id.to_be_bytes());
        s_val.extend_from_slice(&frame_id.to_be_bytes());
        s_val.extend_from_slice(&slot_id.to_be_bytes());
        secondary.insert(key, &s_val).unwrap();
    } else {
        panic!("Key not found in the primary index");
    }
}

pub fn insert_into_tables<M: MemPool>(
    params: &SecBenchParams,
    primary: &Arc<FosterBtree<M>>,
    secondary: &Arc<FosterBtree<M>>,
    iter: impl Iterator<Item = usize>,
) {
    for key in iter {
        let key = get_key_bytes(key, params.key_size);
        let value = get_new_value(params.record_size);
        insert(primary, secondary, &key, &value);
    }
}

pub fn delete_from_tables<M: MemPool>(
    params: &SecBenchParams,
    primary: &Arc<FosterBtree<M>>,
    secondary: &Arc<FosterBtree<M>>,
    iter: impl Iterator<Item = usize>,
) {
    for key in iter {
        let key = get_key_bytes(key, params.key_size);
        primary.delete(&key).unwrap();
        secondary.delete(&key).unwrap();
    }
}

pub fn insert_experiment(params: SecBenchParams) {
    let bp = get_test_bp(params.bp_size);
    let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
    let total_num_keys = (params.num_keys as f64 * 2.0) as usize;
    let perm = Permutation::new(0, total_num_keys - 1);
    // Check uniqueness of the permutation
    println!("Loading table with {} keys", params.num_keys);
    load_table(
        &params,
        &primary,
        perm.perm[0..params.num_keys].iter().copied(),
    );
    println!("BP stats: \n{}", bp.stats());
    println!("Tree stats: \n{}", primary.page_stats(false));
    println!("++++++++++++++++++++++++++++++++++++++++++++");
    println!("[Page, Frame, Slot] hint");
    let secondary = SecondaryIndex::new(&primary, 50);
    println!(
        "Secondary index stats: \n{}",
        secondary.secondary.page_stats(false)
    );
    println!("Hint correctness: {}", secondary.check_hint_correctness());

    println!("bp stats: \n{}", bp.stats());

    let num_insertions = 10;

    for i in 0..num_insertions {
        println!(
            "============================== Insertion {} ===============================",
            i + 1
        );
        // Insert 10% of the keys each time
        let start_idx = params.num_keys + i * params.num_keys / 10;
        let end_idx = if i == num_insertions - 1 {
            total_num_keys
        } else {
            params.num_keys + (i + 1) * params.num_keys / 10
        };
        println!("Inserting keys from {} to {}", start_idx, end_idx);
        insert_into_tables(
            &params,
            &primary,
            &secondary.secondary,
            perm.perm[start_idx..end_idx].iter().copied(),
        );
        println!("Hint correctness: {}", secondary.check_hint_correctness());
        println!("bp stats: \n{}", bp.stats());
    }

    println!("BP stats: \n{}", bp.stats());
    bp.clear_dirty_flags().unwrap();
}

pub fn delete_experiment(params: SecBenchParams) {
    let bp = get_test_bp(params.bp_size);
    let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
    let total_num_keys = params.num_keys;
    let perm = Permutation::new(0, total_num_keys - 1);
    // Check uniqueness of the permutation
    println!("Loading table with {} keys", params.num_keys);
    load_table(
        &params,
        &primary,
        perm.perm[0..params.num_keys].iter().copied(),
    );
    println!("BP stats: \n{}", bp.stats());
    println!("Tree stats: \n{}", primary.page_stats(false));
    println!("++++++++++++++++++++++++++++++++++++++++++++");
    println!("[Page, Frame, Slot] hint");
    let secondary = SecondaryIndex::new(&primary, 50);
    println!(
        "Secondary index stats: \n{}",
        secondary.secondary.page_stats(false)
    );
    println!("Hint correctness: {}", secondary.check_hint_correctness());
    let num_deletions = 9;

    for i in 0..num_deletions {
        println!(
            "============================== Deletion {} ===============================",
            i + 1
        );
        // Delete 10% of the keys each time
        let start_idx = params.num_keys * i / 10;
        let end_idx = params.num_keys * (i + 1) / 10;
        println!("Deleting keys from {} to {}", start_idx, end_idx);
        delete_from_tables(
            &params,
            &primary,
            &secondary.secondary,
            perm.perm[start_idx..end_idx].iter().copied(),
        );
        println!("Hint correctness: {}", secondary.check_hint_correctness());
    }

    println!("BP stats: \n{}", bp.stats());
    bp.clear_dirty_flags().unwrap();
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

    println!("+++++++++++++++++++++++++ INSERTION EXPERIMENT +++++++++++++++++++++++++++++");
    {
        flush_internal_cache_and_everything();
        insert_experiment(params.clone());
    }

    println!("+++++++++++++++++++++++++ DELETION EXPERIMENT +++++++++++++++++++++++++++++");

    {
        flush_internal_cache_and_everything();
        delete_experiment(params.clone());
    }
}
