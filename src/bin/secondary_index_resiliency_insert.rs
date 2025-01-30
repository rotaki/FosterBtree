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
    bp::{get_test_bp, ContainerId, ContainerKey, MemPool, PageFrameKey},
    prelude::{FosterBtree, FosterBtreePage, PageId},
    utils::Permutation,
};

use clap::Parser;
use fbtree::{
    access_method::{AccessMethodError, UniqueKeyIndex},
    bp::BufferPool,
    random::gen_random_byte_vec,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub struct HintHitStats {
    pub page_hit: AtomicU64,
    pub page_miss: AtomicU64,
    pub frame_hit: AtomicU64,
    pub frame_miss: AtomicU64,
    pub slot_hit: AtomicU64,
    pub slot_miss: AtomicU64,
}

impl Default for HintHitStats {
    fn default() -> Self {
        Self::new()
    }
}

impl HintHitStats {
    pub const fn new() -> Self {
        Self {
            page_hit: AtomicU64::new(0),
            page_miss: AtomicU64::new(0),
            frame_hit: AtomicU64::new(0),
            frame_miss: AtomicU64::new(0),
            slot_hit: AtomicU64::new(0),
            slot_miss: AtomicU64::new(0),
        }
    }

    pub fn current(&self) -> HintHitStats {
        HintHitStats {
            page_hit: AtomicU64::new(self.page_hit.load(Ordering::Relaxed)),
            page_miss: AtomicU64::new(self.page_miss.load(Ordering::Relaxed)),
            frame_hit: AtomicU64::new(self.frame_hit.load(Ordering::Relaxed)),
            frame_miss: AtomicU64::new(self.frame_miss.load(Ordering::Relaxed)),
            slot_hit: AtomicU64::new(self.slot_hit.load(Ordering::Relaxed)),
            slot_miss: AtomicU64::new(self.slot_miss.load(Ordering::Relaxed)),
        }
    }

    pub fn diff(&self, other: &Self) -> Self {
        Self {
            page_hit: AtomicU64::new(
                self.page_hit.load(Ordering::Relaxed) - other.page_hit.load(Ordering::Relaxed),
            ),
            page_miss: AtomicU64::new(
                self.page_miss.load(Ordering::Relaxed) - other.page_miss.load(Ordering::Relaxed),
            ),
            frame_hit: AtomicU64::new(
                self.frame_hit.load(Ordering::Relaxed) - other.frame_hit.load(Ordering::Relaxed),
            ),
            frame_miss: AtomicU64::new(
                self.frame_miss.load(Ordering::Relaxed) - other.frame_miss.load(Ordering::Relaxed),
            ),
            slot_hit: AtomicU64::new(
                self.slot_hit.load(Ordering::Relaxed) - other.slot_hit.load(Ordering::Relaxed),
            ),
            slot_miss: AtomicU64::new(
                self.slot_miss.load(Ordering::Relaxed) - other.slot_miss.load(Ordering::Relaxed),
            ),
        }
    }
}

impl std::fmt::Display for HintHitStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Page: hit: {}, miss: {}, Frame: hit: {}, miss: {}, Slot: hit: {}, miss: {}",
            self.page_hit.load(Ordering::Relaxed),
            self.page_miss.load(Ordering::Relaxed),
            self.frame_hit.load(Ordering::Relaxed),
            self.frame_miss.load(Ordering::Relaxed),
            self.slot_hit.load(Ordering::Relaxed),
            self.slot_miss.load(Ordering::Relaxed),
        )
    }
}

static HINT_HINT_STATS: HintHitStats = HintHitStats::new();

pub trait SecondaryIndex<T: MemPool> {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError>;
    fn stats(&self) -> String;
}

pub struct SecondaryPageFrameSlotHint<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> SecondaryPageFrameSlotHint<T> {
    pub fn new(primary: &Arc<FosterBtree<T>>, c_id: ContainerId) -> SecondaryPageFrameSlotHint<T> {
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
}

impl<T: MemPool> SecondaryIndex<T> for SecondaryPageFrameSlotHint<T> {
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
            let (result, actual_slot_id) = if pri_page.low_fence_slot_id() < expected_slot_id
                && expected_slot_id < pri_page.high_fence_slot_id()
                && pri_page.get_raw_key(expected_slot_id) == p_key
            {
                HINT_HINT_STATS.slot_hit.fetch_add(1, Ordering::Relaxed);
                (
                    pri_page.get_val(expected_slot_id).to_vec(),
                    expected_slot_id,
                )
            } else {
                HINT_HINT_STATS.slot_miss.fetch_add(1, Ordering::Relaxed);
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
            if actual_page_id == expected_page_id {
                HINT_HINT_STATS.page_hit.fetch_add(1, Ordering::Relaxed);
            } else {
                HINT_HINT_STATS.page_miss.fetch_add(1, Ordering::Relaxed);
            }
            if actual_frame_id == expected_frame_id {
                HINT_HINT_STATS.frame_hit.fetch_add(1, Ordering::Relaxed);
            } else {
                HINT_HINT_STATS.frame_miss.fetch_add(1, Ordering::Relaxed);
            }
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
    /// Insert factor. The fraction of keys to insert out of the num_keys.
    #[clap(short, long, default_value = "0.0")]
    pub insert_factor: f64,
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

// Insert num_keys keys into the table
pub fn load_table(
    params: &SecBenchParams,
    table: &Arc<FosterBtree<BufferPool>>,
    perm: Permutation,
) {
    for key in perm.into_iter().take(params.num_keys) {
        let key = get_key_bytes(key, params.key_size);
        let value = get_new_value(params.record_size);
        table.insert(&key, &value).unwrap();
    }
}

fn insert_into_primary<M: MemPool>(
    params: &SecBenchParams,
    primary: &Arc<FosterBtree<M>>,
    perm: Permutation,
) {
    let insert_count = (params.insert_factor * params.num_keys as f64) as usize;
    for key in perm.into_iter().skip(params.num_keys).take(insert_count) {
        let key_bytes = get_key_bytes(key, params.key_size);
        let value = get_new_value(params.record_size);
        primary.insert(&key_bytes, &value).unwrap();
    }
}

fn run_secondary_lookups<M: MemPool, T: SecondaryIndex<M>>(
    params: &SecBenchParams,
    secondary: &T,
    bp: &Arc<M>,
    perm: Permutation,
) {
    println!("{}", secondary.stats());

    let bp_stats_pre = bp.stats();
    let hint_hit_stats_pre = HINT_HINT_STATS.current();
    let start = std::time::Instant::now();
    // Only read num_keys keys from the permutation
    for key in perm.into_iter().take(params.num_keys) {
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
    let hint_hit_stats_post = HINT_HINT_STATS.current();
    let bp_stats_diff = bp_stats_post.diff(&bp_stats_pre);
    let hint_hit_stats_diff = hint_hit_stats_post.diff(&hint_hit_stats_pre);
    println!(
        "time: {:?}, bp_stats: {}, hint_hit_stats: {}",
        elapsed, bp_stats_diff, hint_hit_stats_diff
    );
}

fn main() {
    println!("Secondary index resiliency insertion benchmark");
    let params = SecBenchParams::parse();
    println!("Params: {:?}", params);

    // First, we create a buffer pool that can hold the entire table (both primary and secondary indices)
    // We create a primary index on the buffer pool
    // We create a secondary index on the buffer pool with all the keys from the primary index
    // All the entries in the secondary index have the valid hint to the primary index entry
    // We then perform insertions into the primary index see how many hints are invalidated

    {
        println!("=========================================================================================");
        let bp = get_test_bp(params.bp_size);
        let primary = Arc::new(FosterBtree::new(ContainerKey::new(0, 0), Arc::clone(&bp)));
        let total_num_keys =
            params.num_keys + (params.insert_factor * params.num_keys as f64) as usize;
        let perm = Permutation::new(0, total_num_keys - 1);
        load_table(&params, &primary, perm.clone());
        // Print the page stats
        println!("BP stats: \n{}", bp.stats());
        println!("Tree stats: \n{}", primary.page_stats(false));
        println!("++++++++++++++++++++++++++++++++++++++++++++");
        println!("[Page, Frame, Slot] hint");
        let with_slot_hint = SecondaryPageFrameSlotHint::new(&primary, 40);
        insert_into_primary(&params, &primary, perm.clone());
        println!("BP stats: \n{}", bp.stats());
        run_secondary_lookups(&params, &with_slot_hint, &bp, perm.clone());
        println!("BP stats: \n{}", bp.stats());
        println!("=========================================================================================");
    }
}
