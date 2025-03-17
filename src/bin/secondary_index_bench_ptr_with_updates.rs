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
    bp::{ContainerId, ContainerKey, MemPool, PageFrameKey},
    prelude::{FosterBtree, FosterBtreePage, PageId},
    random::gen_random_int,
    utils::Permutation,
};

use clap::Parser;
use fbtree::{
    access_method::{AccessMethodError, UniqueKeyIndex},
    bp::{get_test_bp, BufferPool},
    random::gen_random_byte_vec,
};
use std::{process::Command, sync::Arc};

pub struct SecondaryIncorrectHintIndex<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

pub struct HintCorrectness {
    pub page: usize,
    pub frame: usize,
    pub slot: usize,
}

impl HintCorrectness {
    pub fn new(page: usize, frame: usize, slot: usize) -> HintCorrectness {
        HintCorrectness { page, frame, slot }
    }

    fn validate(&self) {
        if self.page > 100 || self.frame > 100 || self.slot > 100 {
            panic!("Hint correctness should be between 0 and 100");
        }
    }
}

impl<T: MemPool> SecondaryIncorrectHintIndex<T> {
    pub fn new(
        primary: &Arc<FosterBtree<T>>,
        c_id: ContainerId,
        hint_correctness: HintCorrectness,
    ) -> SecondaryIncorrectHintIndex<T> {
        hint_correctness.validate();
        // Iterate through the table to create the secondary index.
        let mut iter = FosterBtreeCursor::new(primary, &[], &[]);
        let secondary = Arc::new(FosterBtree::new(
            ContainerKey::new(0, c_id),
            primary.mem_pool.clone(),
        ));
        while let Some((p_key, _)) = iter.get_kv() {
            let (page_id, frame_id, slot_id) = iter.get_physical_address();
            // Embed wrong page id in the secondary index with a probability of hint_correctness
            // The frame id is "correct" because it contains the expected page id.
            let (page_id, frame_id) = if gen_random_int(1, 100) <= hint_correctness.page as u32 {
                (page_id, frame_id)
            } else {
                let fake_page_id = page_id.saturating_sub(1);
                let frame_id = primary
                    .mem_pool
                    .get_page_for_read(PageFrameKey::new(primary.c_key, fake_page_id))
                    .unwrap()
                    .frame_id();
                (fake_page_id, frame_id)
            };

            // Embed wrong frame id in the secondary index with a probability of hint_correctness
            let frame_id = if gen_random_int(1, 100) <= hint_correctness.frame as u32 {
                frame_id
            } else {
                frame_id.saturating_sub(1)
            };

            // Embed wrong slot id in the secondary index with a probability of hint_correctness
            let slot_id = if gen_random_int(1, 100) <= hint_correctness.slot as u32 {
                slot_id
            } else {
                slot_id.saturating_sub(1)
            };
            // Concatenate the primary key with the physical address
            let mut val = p_key.to_vec();
            val.extend_from_slice(&page_id.to_be_bytes());
            val.extend_from_slice(&frame_id.to_be_bytes());
            val.extend_from_slice(&slot_id.to_be_bytes());
            secondary.insert(&p_key, &val).unwrap();
            iter.go_to_next_kv();
        }
        SecondaryIncorrectHintIndex {
            primary: primary.clone(),
            secondary,
        }
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

impl<T: MemPool> SecondaryIncorrectHintIndex<T> {
    fn get(&self, key: &[u8]) -> Result<*const u8, AccessMethodError> {
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

    fn get_without_hint(&self, key: &[u8]) -> Result<*const u8, AccessMethodError> {
        let sec_leaf_page = self
            .secondary
            .traverse_to_leaf_for_read_with_hint(key, None);
        let sec_slot_id = sec_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(key)) - 1;
        if sec_slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else if sec_leaf_page.get_raw_key(sec_slot_id) == key {
            let val = sec_leaf_page.get_val(sec_slot_id);
            let (p_key, _expected_phys_addr) = val.split_at(val.len() - 12);
            let pri_leaf_page = self
                .primary
                .traverse_to_leaf_for_read_with_hint(p_key, None);
            let pri_slot_id = pri_leaf_page.upper_bound_slot_id(&BTreeKey::Normal(p_key)) - 1;
            if pri_slot_id == 0 {
                // Lower fence. Non-existent key
                Err(AccessMethodError::KeyNotFound)
            } else if pri_leaf_page.get_raw_key(pri_slot_id) == p_key {
                Ok(pri_leaf_page.get_val(pri_slot_id).as_ptr())
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
            "[Page, Frame, Slot] hint secondary index: \n{}",
            self.secondary.page_stats(false)
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
    /// Skew factor
    #[clap(short, long, default_value = "0.0")]
    pub skew_factor: f64,
    /// Warmup time in seconds
    #[clap(short, long, default_value = "20")]
    pub warmup_time: usize,
    /// Execution time in seconds
    #[clap(short, long, default_value = "20")]
    pub exec_time: usize,
    /// Correct hint percentage
    #[clap(long, default_value = "100")]
    pub page_hint_correctness: usize,
    #[clap(long, default_value = "100")]
    pub frame_hint_correctness: usize,
    #[clap(long, default_value = "100")]
    pub slot_hint_correctness: usize,
    /// With or without hint. 0: without hint, 1: with hint
    #[clap(short = 'h', long, default_value = "1")]
    pub with_hint: usize,
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

fn bench_secondary<M: MemPool>(
    params: &SecBenchParams,
    secondary: &SecondaryIncorrectHintIndex<M>,
    _bp: &Arc<M>,
) -> u128 {
    println!("{}", secondary.stats());

    // Keep calling get on the secondary index to measure the speed.
    let warmup = params.warmup_time;
    let exec = params.exec_time;

    let mut avg = 0;
    for i in 0..warmup + exec {
        // let bp_stats_pre = bp.stats();
        let perm = Permutation::new(0, params.num_keys - 1);
        let start = std::time::Instant::now();
        for key in perm {
            let key_bytes = get_key_bytes(key, params.key_size);
            let result = if params.with_hint == 0 {
                secondary.get_without_hint(&key_bytes).unwrap()
            } else {
                secondary.get(&key_bytes).unwrap()
            };
            black_box(result);
        }
        // for _ in 0..params.num_keys {
        //     let key = get_key(params.num_keys, params.skew_factor);
        //     let key_bytes = get_key_bytes(key, params.key_size);
        //     let result = secondary.get(&key_bytes).unwrap();
        //     black_box(result);
        // }
        let elapsed = start.elapsed();
        // let bp_stats_post = bp.stats();
        // let diff = bp_stats_post.diff(&bp_stats_pre);
        let name = if i < warmup { "Warmup" } else { "Execution" };
        println!("Iteration({}) {}: time: {:?}", name, i, elapsed,);
        if i >= warmup {
            avg += elapsed.as_millis();
        }
    }
    avg /= exec as u128;
    println!("Average time: {} ms", avg);
    avg
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
    let hint_correctness = HintCorrectness::new(
        params.page_hint_correctness,
        params.frame_hint_correctness,
        params.slot_hint_correctness,
    );
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
        let with_slot_hint = SecondaryIncorrectHintIndex::new(&primary, 50, hint_correctness);
        println!(
            "Pre hint correctness: {}",
            with_slot_hint.check_hint_correctness()
        );
        println!("BP stats: \n{}", bp.stats());
        let with_slot_hint_time = bench_secondary(&params, &with_slot_hint, &bp);
        println!(
            "Post hint correctness: {}",
            with_slot_hint.check_hint_correctness()
        );
        println!("BP stats: \n{}", bp.stats());
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("Summary");
        println!("With all hints enabled: {} ms", with_slot_hint_time);
        println!("=========================================================================================");
        bp.clear_dirty_flags().unwrap();
    }
}
