// Create a secondary index pointing to a primary index.
// Create two workloads:
// 1. Read the secondary index with a key.
// 2. Modify a hint in the secondary index.
// Measure the latency of the read operation and plot it as a histogram.
// We control the ratio between these two workloads.
//
// To generate the key to read/modify, we use a Zipf distribution between
// the range of keys in the secondary index.

use criterion::black_box;
use fbtree::{
    access_method::fbt::{BTreeKey, FosterBtreeCursor},
    bp::{ContainerId, ContainerKey, MemPool, PageFrameKey},
    prelude::{FosterBtree, FosterBtreePage, PageId},
    random::{gen_random_int, FastZipf},
};

use clap::Parser;
use fbtree::{
    access_method::{AccessMethodError, UniqueKeyIndex},
    bp::{get_test_bp, BufferPool},
    random::gen_random_byte_vec,
};
use hdrhistogram::Histogram;
use rand::{rngs::SmallRng, SeedableRng};
use std::sync::{atomic::AtomicBool, Arc};

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
    /// Modification ratio
    #[clap(short, long, default_value = "0")]
    pub modification_ratio: usize,
    /// Repair type. 0: always repair, 1: ignore slot, 2: ignore all
    #[clap(long, default_value = "0")]
    pub repair_type: usize,
    /// With hint. 0: no hint, 1: with hint
    #[clap(short = 'h', long, default_value = "1")]
    pub with_hint: usize,
}

#[derive(Debug, Clone)]
pub struct ReadWriteRatio {
    pub read_ratio: usize,
    pub write_ratio: usize,
}

impl ReadWriteRatio {
    pub fn new(read_ratio: usize, write_ratio: usize) -> ReadWriteRatio {
        if read_ratio + write_ratio != 100 {
            panic!("The sum of read_ratio and write_ratio must be 100.");
        }
        ReadWriteRatio {
            read_ratio,
            write_ratio,
        }
    }

    pub fn which_to_do(&self) -> usize {
        let r = gen_random_int(1, 100);
        if r <= self.read_ratio {
            0
        } else {
            1
        }
    }
}

#[derive(Debug, Clone)]
pub struct RepairType {
    pub always_repair: bool,
    pub ignore_slot: bool,
    pub ignore_all: bool,
}

impl RepairType {
    pub fn new(always_repair: bool, ignore_slot: bool, ignore_all: bool) -> RepairType {
        // Only one of the three options can be true. At least one of the three options must be true.
        #[allow(clippy::nonminimal_bool)]
        if (always_repair && ignore_slot)
            || (always_repair && ignore_all)
            || (ignore_slot && ignore_all)
            || (!always_repair && !ignore_slot && !ignore_all)
        {
            panic!("Only one of the three options can be true. At least one of the three options must be true.");
        }

        RepairType {
            always_repair,
            ignore_slot,
            ignore_all,
        }
    }

    pub fn from_u8(repair_type: u8) -> RepairType {
        match repair_type {
            0 => RepairType::new(true, false, false),
            1 => RepairType::new(false, true, false),
            2 => RepairType::new(false, false, true),
            _ => panic!("Invalid value"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HintModificationRatio {
    pub page_id: usize,
    pub frame_id: usize,
    pub slot_id: usize,
}

impl HintModificationRatio {
    pub fn new(page_id: usize, frame_id: usize, slot_id: usize) -> HintModificationRatio {
        if page_id + frame_id + slot_id != 100 {
            panic!("The sum of page_id, frame_id, and slot_id must be 100.");
        }
        HintModificationRatio {
            page_id,
            frame_id,
            slot_id,
        }
    }

    pub fn which_to_modify(&self) -> usize {
        let r = gen_random_int(0, 100);
        if r < self.page_id {
            0
        } else if r < self.page_id + self.frame_id {
            1
        } else {
            2
        }
    }
}

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

    pub fn modify_hint(
        &self,
        key: &[u8],
        hmr: &HintModificationRatio,
    ) -> Result<(), AccessMethodError> {
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
            let (_, expected_phys_addr) = val.split_at(val.len() - 12);
            let (expected_page_id, expected_frame_slot_id) = expected_phys_addr.split_at(4);
            let expected_page_id = PageId::from_be_bytes(expected_page_id.try_into().unwrap());
            let (expected_frame_id, expected_slot_id) = expected_frame_slot_id.split_at(4);
            let expected_frame_id = u32::from_be_bytes(expected_frame_id.try_into().unwrap());
            let expected_slot_id = u32::from_be_bytes(expected_slot_id.try_into().unwrap());

            let (page_id, frame_id, slot_id) = match hmr.which_to_modify() {
                0 => (
                    expected_page_id.saturating_sub(1),
                    expected_frame_id,
                    expected_slot_id,
                ),
                1 => (
                    expected_page_id,
                    expected_frame_id.saturating_sub(1),
                    expected_slot_id,
                ),
                2 => (
                    expected_page_id,
                    expected_frame_id,
                    expected_slot_id.saturating_sub(1),
                ),
                _ => panic!("Invalid value"),
            };

            let val_len = val.len();
            let val: *const u8 = val.as_ptr();
            match sec_leaf_page.try_upgrade(false) {
                Ok(write_leaf_page) => {
                    unsafe {
                        // Modify the last 4 bytes of the value to the new page_id
                        let start = val.add(val_len - 12) as *mut u8;
                        std::ptr::copy_nonoverlapping(&page_id.to_be_bytes()[0], start, 4);
                        // Modify the next 4 bytes of the value to the new frame_id
                        let start = val.add(val_len - 8) as *mut u8;
                        std::ptr::copy_nonoverlapping(&frame_id.to_be_bytes()[0], start, 4);
                        // Modify the next 4 bytes of the value to the new slot_id
                        let start = val.add(val_len - 4) as *mut u8;
                        std::ptr::copy_nonoverlapping(&slot_id.to_be_bytes()[0], start, 4);
                    }
                    drop(write_leaf_page);
                    Ok(())
                }
                Err(_) => {
                    // Someone else is writing to the page. Do it in the next iteration.
                    Ok(())
                }
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    pub fn get(
        &self,
        key: &[u8],
        repair_type: &RepairType,
    ) -> Result<*const u8, AccessMethodError> {
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
                // If always_repair is true, always repair the hint.
                // If ignore_slot is true, repair the hint only if the page id or frame id is different.
                if repair_type.always_repair
                    || (repair_type.ignore_slot
                        && (actual_page_id != expected_page_id
                            || actual_frame_id != expected_frame_id))
                {
                    let val_len = val.len();
                    let val: *const u8 = val.as_ptr();
                    match sec_leaf_page.try_upgrade(false) {
                        Ok(write_leaf_page) => {
                            unsafe {
                                // Modify the last 4 bytes of the value to the new page_id
                                let start = val.add(val_len - 12) as *mut u8;
                                std::ptr::copy_nonoverlapping(
                                    &actual_page_id.to_be_bytes()[0],
                                    start,
                                    4,
                                );
                                // Modify the next 4 bytes of the value to the new frame_id
                                let start = val.add(val_len - 8) as *mut u8;
                                std::ptr::copy_nonoverlapping(
                                    &actual_frame_id.to_be_bytes()[0],
                                    start,
                                    4,
                                );
                                // Modify the next 4 bytes of the value to the new slot_id
                                let start = val.add(val_len - 4) as *mut u8;
                                std::ptr::copy_nonoverlapping(
                                    &actual_slot_id.to_be_bytes()[0],
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
                } else {
                    Ok(result)
                }
            }
        } else {
            // Non-existent key
            Err(AccessMethodError::KeyNotFound)
        }
    }

    pub fn get_without_hint(&self, key: &[u8]) -> Result<*const u8, AccessMethodError> {
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
    /*
    pub fn scan(&self) -> Result<(usize, usize, usize), AccessMethodError> { // Returns (result_hash, total_count, repair_count)
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
                } else if self.repair_type.ignore_slot && (actual_page_id != expected_page_id || actual_frame_id != expected_frame_id) {
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

fn get_key_bytes(key: usize, key_size: usize) -> Vec<u8> {
    if key_size < std::mem::size_of::<usize>() {
        panic!("Key size is less than the size of usize");
    }
    let mut key_vec = vec![0u8; key_size];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[key_size - bytes.len()..].copy_from_slice(&bytes);
    key_vec
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

pub fn do_work(
    done: Arc<AtomicBool>,
    secondary: &SecondaryIndex<BufferPool>,
    params: &SecBenchParams,
    repair_type: &RepairType,
    read_write_ratio: &ReadWriteRatio,
) -> Histogram<u64> {
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).unwrap();
    let hmr = HintModificationRatio::new(50, 10, 40);
    let mut read_count = 0;
    let mut modify_hint_count = 0;
    let mut zipf = FastZipf::new(SmallRng::from_os_rng(), params.skew_factor, params.num_keys);
    while !done.load(std::sync::atomic::Ordering::Relaxed) {
        let key = get_key_bytes(zipf.sample(), params.key_size);
        match read_write_ratio.which_to_do() {
            0 => {
                // Read
                let start = std::time::Instant::now();
                let res = if params.with_hint == 1 {
                    secondary.get(&key, repair_type).unwrap()
                } else {
                    secondary.get_without_hint(&key).unwrap()
                };
                let duration = start.elapsed().as_nanos() as u64;
                histogram.record(duration).unwrap();
                black_box(res);
                read_count += 1;
            }
            1 => {
                // Modify hint
                secondary.modify_hint(&key, &hmr).unwrap();
                black_box(());
                modify_hint_count += 1;
            }
            _ => panic!("Invalid value"),
        }
    }
    let total_ops = read_count + modify_hint_count;
    let read_ratio = (read_count as f64 / total_ops as f64) * 100.0;
    let modify_hint_ratio = (modify_hint_count as f64 / total_ops as f64) * 100.0;
    println!(
        "Thread ({:?}) read count: {}({:.2}), modify hint count: {}({:.2})",
        std::thread::current().id(),
        read_count,
        read_ratio,
        modify_hint_count,
        modify_hint_ratio
    );
    histogram
}

pub fn main() {
    let params = SecBenchParams::parse();
    println!("{:?}", params);
    let bp = get_test_bp(params.bp_size);
    let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
    let total_num_keys = params.num_keys;
    println!("Loading the table with {} keys", total_num_keys);
    load_table(&params, &primary, 0..total_num_keys);
    let secondary = Arc::new(SecondaryIndex::new(&primary, 1));

    // Print the stats of primary, seocondary, and bp
    println!("Primary stats: \n{}", primary.page_stats(false));
    println!("Secondary stats: \n{}", secondary.stats());
    println!("BP stats: \n{}", bp.stats());

    // Check the correctness of the hints in the secondary index
    println!(
        "Initial Hint correctness: \n{}",
        secondary.check_hint_correctness()
    );

    let done = Arc::new(AtomicBool::new(false));
    let rwratio = ReadWriteRatio::new(100 - params.modification_ratio, params.modification_ratio);
    let repair_type = RepairType::from_u8(params.repair_type as u8);
    let hint = if params.with_hint == 1 {
        "hint"
    } else {
        "no_hint"
    };
    println!(
        "Warmup for {} seconds with {}, read-write ratio: {:?}, repair-type: {:?}",
        &hint, params.warmup_time, rwratio, repair_type
    );
    std::thread::scope(|s| {
        for _ in 0..1 {
            s.spawn(|| {
                do_work(
                    Arc::clone(&done),
                    &secondary,
                    &params,
                    &repair_type,
                    &rwratio,
                );
            });
        }

        std::thread::sleep(std::time::Duration::from_secs(params.warmup_time as u64));
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    // Print the stats of primary, seocondary, and bp
    // Hint correctness after warmup
    println!(
        "Hint correctness after warmup: \n{}",
        secondary.check_hint_correctness()
    );
    println!(
        "Execution for {} seconds with {}, read-write ratio: {:?}, repair-type: {:?}",
        &hint, params.exec_time, rwratio, repair_type
    );
    done.store(false, std::sync::atomic::Ordering::Relaxed);
    let num_threads = 1;
    let mut threads = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let done = Arc::clone(&done);
        let secondary = secondary.clone();
        let params = params.clone();
        let repair_type = repair_type.clone();
        let rwratio_exec = rwratio.clone();
        threads.push(std::thread::spawn(move || {
            do_work(done, &secondary, &params, &repair_type, &rwratio_exec)
        }));
    }

    std::thread::sleep(std::time::Duration::from_secs(params.exec_time as u64));
    done.store(true, std::sync::atomic::Ordering::Relaxed);

    let mut histogram = Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).unwrap();
    for t in threads {
        let h = t.join().unwrap();
        histogram.add(h).unwrap();
    }

    println!(
        "Hint correctness after execution: \n{}",
        secondary.check_hint_correctness()
    );

    // Print the histogram
    // print_histogram(&histogram);

    println!("Histogram summary");
    println!("Mean: {}", histogram.mean());
    println!("StdDev: {}", histogram.stdev());
    println!("Min: {}", histogram.min());
    println!("Max: {}", histogram.max());
    println!("1th percentile: {}", histogram.value_at_quantile(0.01));
    println!("2th percentile: {}", histogram.value_at_quantile(0.02));
    println!("5th percentile: {}", histogram.value_at_quantile(0.05));
    println!("10th percentile: {}", histogram.value_at_quantile(0.10));
    println!("25th percentile: {}", histogram.value_at_quantile(0.25));
    println!("50th percentile: {}", histogram.value_at_quantile(0.50));
    println!("75th percentile: {}", histogram.value_at_quantile(0.75));
    println!("90th percentile: {}", histogram.value_at_quantile(0.90));
    println!("95th percentile: {}", histogram.value_at_quantile(0.95));
    println!("98th percentile: {}", histogram.value_at_quantile(0.98));
    println!("99th percentile: {}", histogram.value_at_quantile(0.99));
    println!("99.9th percentile: {}", histogram.value_at_quantile(0.999));
    println!(
        "99.99th percentile: {}",
        histogram.value_at_quantile(0.9999)
    );
    println!("Total count: {}", histogram.len());

    let count = histogram
        .iter_recorded()
        .map(|v| v.count_at_value())
        .sum::<u64>();
    println!("Total count by iterating over the histogram: {}", count);

    plot_histogram(&histogram, 250, Some(6000)); // Step size is 1000 nanoseconds
}

pub fn plot_histogram(histogram: &Histogram<u64>, step: u64, threshold: Option<u64>) {
    let max_bar_width = 50;

    // Determine the maximum count in any bucket to scale the bars.
    let max_count = histogram
        .iter_recorded()
        .map(|v| v.count_at_value())
        .max()
        .unwrap_or(0);
    if max_count == 0 {
        panic!("No data in the histogram.");
    }

    // Print header.
    println!(
        "{:>10} | {:<50} | Count(Percentage)",
        "Value(ns)", "Histogram"
    );
    println!("{:-<10} | {:-<50} | {:-<20}", "", "", "");

    // Iterate through the histogram in linear steps.
    let mut prev_val = 0;
    let mut current_val = 0;
    loop {
        if let Some(threshold) = threshold {
            if current_val >= threshold {
                break;
            }
        }
        let percentage = histogram.percentile_below(current_val);
        if percentage >= 100.0 {
            break;
        }
        let percentage_diff = percentage - histogram.percentile_below(prev_val);
        let bar_len = ((percentage_diff / 100.0) * max_bar_width as f64).round() as usize;
        let bar = "*".repeat(bar_len);
        println!(
            "{:>10} | {:<50} | {:8.0}({:.2})",
            current_val,
            bar,
            percentage_diff * max_count as f64,
            percentage_diff
        );
        prev_val = current_val;
        current_val += step;
    }
    if let Some(thres_hold) = threshold {
        let percentage_diff = 100_f64 - histogram.percentile_below(thres_hold);
        let bar_len = ((percentage_diff / 100.0) * max_bar_width as f64).round() as usize;
        let bar = "*".repeat(bar_len);
        println!(
            "{:>9}- | {:<50} | {:8.0}({:.2})",
            thres_hold,
            bar,
            percentage_diff * max_count as f64,
            percentage_diff
        );
    }
}

// fn print_histogram(histogram: &Histogram<u64>) {
//     // Print header for the formatted table.
//     println!(
//         "{:>10} {:>15} {:>10} {:>16}",
//         "Value", "Percentile", "TotalCount", "1/(1-Percentile)"
//     );
//
//     let mut cumulative = 0;
//     // Iterate over each recorded bucket.
//     for v in histogram.iter_recorded() {
//         cumulative += v.count_at_value();
//         // The iterator’s percentile is given as a percentage (0.0 to 100.0);
//         // we divide by 100 to get a fraction.
//         let pct = v.quantile_iterated_to();
//         if pct < 1.0 {
//             println!(
//                 "{:10.3} {:15.12} {:10} {:16.2}",
//                 v.value_iterated_to(),
//                 pct,
//                 cumulative,
//                 1.0 / (1.0 - pct)
//             );
//         } else {
//             // For the final bucket (where percentile == 1), we omit the 1/(1-Percentile) column.
//             println!(
//                 "{:10.3} {:15.12} {:10}",
//                 v.value_iterated_to(),
//                 pct,
//                 cumulative
//             );
//         }
//     }
//
//     // Print a summary similar to your sample.
//     println!(
//         "#[Mean    = {:10.3}, StdDeviation   = {:10.3}]",
//         histogram.mean(),
//         histogram.stdev()
//     );
//     println!(
//         "#[Max     = {:10.3}, Total count    = {:10}]",
//         histogram.max(),
//         histogram.len()
//     );
//     println!("#[Buckets = {:10}]", histogram.buckets());
// }
