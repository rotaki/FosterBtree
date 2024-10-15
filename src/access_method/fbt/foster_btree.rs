#[allow(unused_imports)]
use crate::log;

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{
    access_method::{AccessMethodError, OrderedUniqueKeyIndex, UniqueKeyIndex},
    bp::prelude::*,
    log_debug, log_info, log_warn,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::foster_btree_page::{BTreeKey, FosterBtreePage};

#[derive(Clone, Copy, Debug)]
enum OpType {
    Split,
    Merge,
    LoadBalance,
    Adopt,
    AntiAdopt,
    AscendRoot,
    DescendRoot,
}

const SPLIT_FLAG: u8 = 0b1000_0000;
const MERGE_FLAG: u8 = 0b0100_0000;
const LOADBALANCE_FLAG: u8 = 0b0010_0000;
const ADOPT_FLAG: u8 = 0b0001_0000;
const ANTIADOPT_FLAG: u8 = 0b0000_1000;
const ASCENDROOT_FLAG: u8 = 0b0000_0100;
const DESCENDROOT_FLAG: u8 = 0b0000_0010;

struct OpByte(pub u8);

impl OpByte {
    pub fn new() -> Self {
        OpByte(0)
    }
    pub fn is_split_done(&self) -> bool {
        self.0 & SPLIT_FLAG != 0
    }
    pub fn split_done(&mut self) {
        self.0 |= SPLIT_FLAG;
    }
    pub fn is_merge_done(&self) -> bool {
        self.0 & MERGE_FLAG != 0
    }
    pub fn merge_done(&mut self) {
        self.0 |= MERGE_FLAG;
    }
    pub fn is_load_balance_done(&self) -> bool {
        self.0 & LOADBALANCE_FLAG != 0
    }
    pub fn load_balance_done(&mut self) {
        self.0 |= LOADBALANCE_FLAG;
    }
    pub fn is_adopt_done(&self) -> bool {
        self.0 & ADOPT_FLAG != 0
    }
    pub fn adopt_done(&mut self) {
        self.0 |= ADOPT_FLAG;
    }
    pub fn is_antiadopt_done(&self) -> bool {
        self.0 & ANTIADOPT_FLAG != 0
    }
    pub fn antiadopt_done(&mut self) {
        self.0 |= ANTIADOPT_FLAG;
    }
    pub fn is_ascend_root_done(&self) -> bool {
        self.0 & ASCENDROOT_FLAG != 0
    }
    pub fn ascend_root_done(&mut self) {
        self.0 |= ASCENDROOT_FLAG;
    }
    pub fn is_descend_root_done(&self) -> bool {
        self.0 & DESCENDROOT_FLAG != 0
    }
    pub fn descend_root_done(&mut self) {
        self.0 |= DESCENDROOT_FLAG;
    }
    pub fn reset(&mut self) {
        self.0 = 0;
    }
}

#[cfg(feature = "stat")]
mod stat {
    use super::*;
    use lazy_static::lazy_static;
    use std::{cell::UnsafeCell, sync::Mutex};

    pub struct FosterBTreeStat {
        sm_trigger: UnsafeCell<[usize; 7]>, // Number of times the structure modification is triggered
        sm_success: UnsafeCell<[usize; 7]>, // Number of times the structure modification is successful
        additional_traversals: UnsafeCell<[usize; 11]>, // Number of additional traversals for exclusive page latch
        shared_page_latch_failures: UnsafeCell<[usize; 3]>, // Number of times the read page fails [0] leaf failure, [1] inner failure, [2] total(#attempt \neq leaf + inner)
        exclusive_page_latch_failures: UnsafeCell<[usize; 3]>, // Number of times the exclusive page latch fails [0] leaf failure, [1] inner failure, [2] total(#attempt \neq leaf + inner)
    }

    impl FosterBTreeStat {
        pub fn new() -> Self {
            FosterBTreeStat {
                sm_trigger: UnsafeCell::new([0; 7]),
                sm_success: UnsafeCell::new([0; 7]),
                additional_traversals: UnsafeCell::new([0; 11]),
                shared_page_latch_failures: UnsafeCell::new([0; 3]),
                exclusive_page_latch_failures: UnsafeCell::new([0; 3]),
            }
        }

        pub fn inc_trigger(&self, op_type: OpType) {
            (unsafe { &mut *self.sm_trigger.get() })[op_type as usize] += 1;
        }

        pub fn inc_success(&self, op_type: OpType) {
            (unsafe { &mut *self.sm_success.get() })[op_type as usize] += 1;
        }

        pub fn to_string(&self) -> String {
            let mut result = String::new();
            let sm_trigger = unsafe { &*self.sm_trigger.get() };
            let sm_success = unsafe { &*self.sm_success.get() };
            result.push_str("Structure Modification Statistics\n");
            let mut sep = "";
            for i in 0..7 {
                result.push_str(sep);
                let op_type = match i {
                    0 => "SPLIT",
                    1 => "MERGE",
                    2 => "LOADBALANCE",
                    3 => "ADOPT",
                    4 => "ANTIADOPT",
                    5 => "ASCENDROOT",
                    6 => "DESCENDROOT",
                    _ => unreachable!(),
                };
                let success = sm_success[i];
                let trigger = sm_trigger[i];
                let rate = if trigger == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (success as f64) / (trigger as f64) * 100.0)
                };
                result.push_str(&format!(
                    "{:12}: {:6} / {:6} ({:6})",
                    op_type, success, trigger, rate
                ));
                sep = "\n";
            }
            result.push_str("\n\n");
            let mut sep = "";
            result.push_str("Additional Traversal For Exclusive Page Latch\n");
            let mut cumulative_count = 0;
            for i in 0..11 {
                result.push_str(sep);
                let count = unsafe { &*self.additional_traversals.get() }[i];
                cumulative_count += count;
                if i == 10 {
                    result.push_str(&format!("{:2}+: {:6} ({:6})", i, count, cumulative_count));
                } else {
                    result.push_str(&format!("{:3}: {:6} ({:6})", i, count, cumulative_count));
                }
                sep = "\n";
            }
            result.push_str("\n\n");
            let shared_page_latch_failures = unsafe { &*self.shared_page_latch_failures.get() };

            result.push_str("Shared Page Latch Failure by Node Type\n");
            let leaf_failure = shared_page_latch_failures[0];
            let inner_failure = shared_page_latch_failures[1];
            let total_failure = leaf_failure + inner_failure;
            let total = shared_page_latch_failures[2];
            result.push_str(&format!(
                "Leaf : {:6} / {:6} ({:6})",
                leaf_failure,
                total,
                if total == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (leaf_failure as f64) / (total as f64) * 100.0)
                }
            ));
            result.push_str("\n");
            result.push_str(&format!(
                "Inner: {:6} / {:6} ({:6})",
                inner_failure,
                total,
                if total == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (inner_failure as f64) / (total as f64) * 100.0)
                }
            ));
            result.push_str("\n");
            result.push_str(&format!(
                "Total: {:6} / {:6} ({:6})",
                total_failure,
                total,
                if total == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (total_failure as f64) / (total as f64) * 100.0)
                }
            ));

            result.push_str("\n\n");
            let exclusive_page_latch_failures =
                unsafe { &*self.exclusive_page_latch_failures.get() };
            result.push_str("Exclusive Page Latch Failure by Node Type\n");
            let leaf_failure = exclusive_page_latch_failures[0];
            let inner_failure = exclusive_page_latch_failures[1];
            let total_failure = leaf_failure + inner_failure;
            let total = exclusive_page_latch_failures[2];
            result.push_str(&format!(
                "Leaf : {:6} / {:6} ({:6})",
                leaf_failure,
                total,
                if total == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (leaf_failure as f64) / (total as f64) * 100.0)
                }
            ));
            result.push_str("\n");
            result.push_str(&format!(
                "Inner: {:6} / {:6} ({:6})",
                inner_failure,
                total,
                if total == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (inner_failure as f64) / (total as f64) * 100.0)
                }
            ));
            result.push_str("\n");
            result.push_str(&format!(
                "Total: {:6} / {:6} ({:6})",
                total_failure,
                total,
                if total == 0 {
                    "N/A".to_owned()
                } else {
                    format!("{:.2}%", (total_failure as f64) / (total as f64) * 100.0)
                }
            ));

            result
        }

        pub fn merge(&self, other: &FosterBTreeStat) {
            let sm_trigger = unsafe { &mut *self.sm_trigger.get() };
            let sm_success = unsafe { &mut *self.sm_success.get() };
            let other_sm_trigger = unsafe { &*other.sm_trigger.get() };
            let other_sm_success = unsafe { &*other.sm_success.get() };
            for i in 0..7 {
                sm_trigger[i] += other_sm_trigger[i];
                sm_success[i] += other_sm_success[i];
            }
            let additional_traversals = unsafe { &mut *self.additional_traversals.get() };
            let other_additional_traversals = unsafe { &*other.additional_traversals.get() };
            for i in 0..11 {
                additional_traversals[i] += other_additional_traversals[i];
            }
            let shared_page_latch_failures = unsafe { &mut *self.shared_page_latch_failures.get() };
            let other_shared_page_latch_failures =
                unsafe { &*other.shared_page_latch_failures.get() };
            let exclusive_page_latch_failures =
                unsafe { &mut *self.exclusive_page_latch_failures.get() };
            let other_exclusive_page_latch_failures =
                unsafe { &*other.exclusive_page_latch_failures.get() };
            for i in 0..3 {
                shared_page_latch_failures[i] += other_shared_page_latch_failures[i];
                exclusive_page_latch_failures[i] += other_exclusive_page_latch_failures[i];
            }
        }

        pub fn clear(&self) {
            let sm_trigger = unsafe { &mut *self.sm_trigger.get() };
            let sm_success = unsafe { &mut *self.sm_success.get() };
            let additional_traversals = unsafe { &mut *self.additional_traversals.get() };
            let shared_page_latch_failures = unsafe { &mut *self.shared_page_latch_failures.get() };
            let exclusive_page_latch_failures =
                unsafe { &mut *self.exclusive_page_latch_failures.get() };
            for i in 0..7 {
                sm_trigger[i] = 0;
                sm_success[i] = 0;
            }
            for i in 0..11 {
                additional_traversals[i] = 0;
            }
            for i in 0..3 {
                shared_page_latch_failures[i] = 0;
                exclusive_page_latch_failures[i] = 0;
            }
        }
    }

    pub struct LocalStat {
        pub stat: FosterBTreeStat,
    }

    impl Drop for LocalStat {
        fn drop(&mut self) {
            GLOBAL_STAT.lock().unwrap().merge(&self.stat);
        }
    }

    lazy_static! {
        pub static ref GLOBAL_STAT: Mutex<FosterBTreeStat> = Mutex::new(FosterBTreeStat::new());
    }

    thread_local! {
        pub static LOCAL_STAT: LocalStat = LocalStat {
            stat: FosterBTreeStat::new(),
        };
    }

    pub fn inc_local_stat_trigger(op_type: OpType) {
        LOCAL_STAT.with(|stat| {
            stat.stat.inc_trigger(op_type);
        });
    }

    pub fn inc_local_stat_success(op_type: OpType) {
        LOCAL_STAT.with(|stat| {
            stat.stat.inc_success(op_type);
        });
    }

    pub fn inc_local_additional_traversals(attempts: u32) {
        LOCAL_STAT.with(|stat| {
            let additional_traversals = unsafe { &mut *stat.stat.additional_traversals.get() };
            if attempts >= 10 {
                additional_traversals[10] += 1;
            } else {
                additional_traversals[attempts as usize] += 1;
            }
        });
    }

    pub fn inc_shared_page_latch_failures(is_leaf: bool, attempts: usize) {
        LOCAL_STAT.with(|stat| {
            let shared_page_latch_failures =
                unsafe { &mut *stat.stat.shared_page_latch_failures.get() };
            if is_leaf {
                shared_page_latch_failures[0] += attempts;
            } else {
                shared_page_latch_failures[1] += attempts;
            }
        });
    }

    pub fn inc_shared_page_latch_count() {
        LOCAL_STAT.with(|stat| {
            let shared_page_latch_failures =
                unsafe { &mut *stat.stat.shared_page_latch_failures.get() };
            shared_page_latch_failures[2] += 1;
        });
    }

    pub fn inc_exclusive_page_latch_failures(is_leaf: bool) {
        LOCAL_STAT.with(|stat| {
            let exclusive_page_latch_failures =
                unsafe { &mut *stat.stat.exclusive_page_latch_failures.get() };
            if is_leaf {
                exclusive_page_latch_failures[0] += 1;
            } else {
                exclusive_page_latch_failures[1] += 1;
            }
        });
    }

    pub fn inc_exclusive_page_latch_count() {
        LOCAL_STAT.with(|stat| {
            let exclusive_page_latch_failures =
                unsafe { &mut *stat.stat.exclusive_page_latch_failures.get() };
            exclusive_page_latch_failures[2] += 1;
        });
    }
}

use concurrent_queue::ConcurrentQueue;
#[cfg(feature = "stat")]
use stat::*;

// The threshold for page modification.
// [0, MIN_BYTES_USED) is small.
// [MIN_BYTES_USED, MAX_BYTES_USED) is normal.
// [MAX_BYTES_USED, PAGE_SIZE) is large.
// If the page has less than MIN_BYTES_USED, then we need to MERGE or LOADBALANCE.
pub const MIN_BYTES_USED: usize = AVAILABLE_PAGE_SIZE / 5;
// If the page has more than MAX_BYTES_USED, then we need to LOADBALANCE.
pub const MAX_BYTES_USED: usize = AVAILABLE_PAGE_SIZE * 4 / 5;

#[inline]
fn is_small(page: &Page) -> bool {
    (page.total_bytes_used() as usize) < MIN_BYTES_USED
}

#[inline]
fn is_large(page: &Page) -> bool {
    (page.total_bytes_used() as usize) >= MAX_BYTES_USED
}

/// A struct that defines the value stored in the inner page of the foster btree.
pub(crate) struct InnerVal {
    pub page_id: PageId,
    pub frame_id: u32,
}

impl InnerVal {
    pub fn new_with_frame_id(page_id: PageId, frame_id: u32) -> Self {
        InnerVal { page_id, frame_id }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        let mut bytes = [0; 8];
        bytes[0..4].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.frame_id.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut page_id_bytes = [0; 4];
        page_id_bytes.copy_from_slice(&bytes[0..4]);
        let page_id = u32::from_be_bytes(page_id_bytes);
        let mut frame_id_bytes = [0; 4];
        frame_id_bytes.copy_from_slice(&bytes[4..8]);
        let frame_id = u32::from_be_bytes(frame_id_bytes);
        InnerVal { page_id, frame_id }
    }
}

impl Default for InnerVal {
    fn default() -> Self {
        InnerVal {
            page_id: PageId::default(),
            frame_id: u32::MAX,
        }
    }
}

pub(crate) fn deserialize_page_id(bytes: &[u8]) -> Option<PageId> {
    if bytes.len() < 4 {
        return None;
    }
    let mut id_bytes = [0; 4];
    id_bytes.copy_from_slice(&bytes[0..4]);
    Some(u32::from_be_bytes(id_bytes))
}

/// Check if the parent page is the parent of the child page.
/// This includes the foster child relationship.
fn is_parent_and_child(parent: &Page, child: &Page) -> bool {
    let low_key = BTreeKey::new(child.get_raw_key(child.low_fence_slot_id()));
    // Find the slot id of the child page in the parent page.
    let slot_id = parent.upper_bound_slot_id(&low_key) - 1;
    // Check if the value of the slot id is the same as the child page id.
    deserialize_page_id(parent.get_val(slot_id)) == Some(child.get_id())
}

/// Check if the parent page is the foster parent of the child page.
fn is_foster_relationship(parent: &Page, child: &Page) -> bool {
    parent.level() == child.level()
        && parent.has_foster_child()
        && deserialize_page_id(parent.get_foster_val()) == Some(child.get_id())
}

fn should_split_this(this: &Page, op_byte: &mut OpByte) -> bool {
    if !op_byte.is_split_done() && should_split(this) {
        op_byte.split_done();
        return true;
    }
    false
}

fn should_modify_foster_relationship(
    this: &Page,
    child: &Page,
    op_byte: &mut OpByte,
) -> Option<OpType> {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    if !op_byte.is_merge_done() && should_merge(this, child) {
        op_byte.merge_done();
        return Some(OpType::Merge);
    }
    if !op_byte.is_load_balance_done() && should_load_balance(this, child) {
        op_byte.load_balance_done();
        return Some(OpType::LoadBalance);
    }
    if !op_byte.is_ascend_root_done() && should_root_ascend(this, child) {
        op_byte.ascend_root_done();
        return Some(OpType::AscendRoot);
    }
    None
}

fn should_modify_parent_child_relationship(
    this: &Page,
    child: &Page,
    op_byte: &mut OpByte,
) -> Option<OpType> {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    if !op_byte.is_adopt_done() && should_adopt(this, child) {
        op_byte.adopt_done();
        return Some(OpType::Adopt);
    }
    if !op_byte.is_antiadopt_done() && should_antiadopt(this, child) {
        op_byte.antiadopt_done();
        return Some(OpType::AntiAdopt);
    }
    if !op_byte.is_descend_root_done() && should_root_descend(this, child) {
        op_byte.descend_root_done();
        return Some(OpType::DescendRoot);
    }
    None
}

fn should_split(this: &Page) -> bool {
    if is_large(this) {
        return true;
    }
    false
}

/// There are 3 * 3 = 9 possible size
/// S: Small, N: Normal, L: Large
/// SS, NS, SN: Merge
/// SL, LS: Load balance
/// NN, LN, NL, LL: Do nothing
fn should_merge(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    // Check if this is small and child is not too large
    if is_small(this) && !is_large(child) {
        return true;
    }

    if !is_large(this) && is_small(child) {
        return true;
    }
    false
}

/// There are 3 * 3 = 9 possible size
/// S: Small, N: Normal, L: Large
/// SS, NS, SN: Merge
/// SL, LS: Load balance
/// NN, LN, NL, LL: Do nothing
fn should_load_balance(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    // Check if this is not too small and child is too large
    if (is_small(this) && is_large(child)) || (is_large(this) && is_small(child)) {
        return true;
    }
    false
}

fn should_adopt(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    // Check if the parent page has enough space to adopt the foster child of the child page.
    if !child.has_foster_child() {
        return false;
    }
    // We want to do anti-adoption as less as possible.
    // Therefore, if child will need merge or load balance, we prioritize them over adoption.
    // If this is large, there is a high chance that the parent will need to split when adopting the foster child.
    // If child is small, there is a high chance that the parent will need to merge or load balance.
    if is_large(this) || is_small(child) {
        return false;
    }
    if this.total_free_space()
        < this.bytes_needed(
            child.get_foster_key(),
            InnerVal::default().to_bytes().as_ref(),
        )
    {
        // If the parent page does not have enough space to adopt the foster child, then we do not adopt.
        return false;
    }
    true
}

fn should_antiadopt(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    if child.has_foster_child() {
        return false;
    }
    if !is_small(child) {
        // If child is not small, there is no need to antiadopt because the child will not need to merge or load balance.
        return false;
    }
    let low_key = BTreeKey::new(child.get_raw_key(child.low_fence_slot_id()));
    let slot_id = this.upper_bound_slot_id(&low_key);
    if this.has_foster_child() {
        if slot_id >= this.foster_child_slot_id() {
            return false;
        }
    } else {
        // If the slot_id is greater or equal to high fence, then slot_id + 1 does not exist.
        if slot_id >= this.high_fence_slot_id() {
            return false;
        }
    }

    true
}

fn should_root_ascend(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    if !this.is_root() {
        return false;
    }
    true
}

fn should_root_descend(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    if !this.is_root() {
        return false;
    }
    if this.active_slot_count() != 1 {
        return false;
    }
    if child.has_foster_child() {
        return false;
    }
    // Same low fence and high fence
    debug_assert_eq!(this.get_low_fence(), child.get_low_fence());
    debug_assert_eq!(this.get_high_fence(), child.get_high_fence());
    true
}

/// Opportunistically try to fix the child page frame id
fn fix_frame_id<'a>(
    this: FrameReadGuard<'a>,
    slot_id: u32,
    new_frame_key: &PageFrameKey,
) -> FrameReadGuard<'a> {
    match this.try_upgrade(true) {
        Ok(mut write_guard) => {
            let val = InnerVal::new_with_frame_id(
                new_frame_key.p_key().page_id,
                new_frame_key.frame_id(),
            );
            let res = write_guard.update_at(slot_id, None, &val.to_bytes());
            assert!(res);
            log_debug!("Fixed frame id of the child page");
            write_guard.downgrade()
        }
        Err(read_guard) => {
            log_debug!("Failed to fix frame id of the child page");
            read_guard
        }
    }
}

/// Split this page into two pages.
/// The foster child will be the right page of this page after the split.
/// Returns the foster key
fn split_even(this: &mut FrameWriteGuard, foster_child: &mut FrameWriteGuard) -> Vec<u8> {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::Split);

    // The page is full and we need to split the page.
    // First, we split the page into two pages with (almost) equal sizes.
    let total_size = this.total_bytes_used();
    let mut half_bytes = total_size / 2;
    let mut moving_slot_ids = Vec::with_capacity((this.active_slot_count() as usize) / 2); // Roughly half of the slots will be moved to the foster child.
    let mut moving_kvs = Vec::with_capacity((this.active_slot_count() as usize) / 2); // Roughly half of the slots will be moved to the foster child.
    for i in (1..this.high_fence_slot_id()).rev() {
        let key = this.get_raw_key(i);
        let val = this.get_val(i);
        let bytes_needed = this.bytes_needed(key, val);
        if half_bytes >= bytes_needed {
            half_bytes -= bytes_needed;
            moving_slot_ids.push(i);
            moving_kvs.push((key, val));
        } else {
            break;
        }
    }
    if moving_kvs.is_empty() {
        // Print this page
        // panic!("Page is full but cannot split because the slots are too large");
        return split_min_move(this, foster_child);
    }

    // Reverse the moving slots
    moving_kvs.reverse();
    moving_slot_ids.reverse();

    let foster_key = moving_kvs[0].0.to_vec();

    foster_child.init();
    foster_child.set_level(this.level());
    foster_child.set_low_fence(&foster_key);
    foster_child.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
    foster_child.set_right_most(this.is_right_most());
    let res = foster_child.append_sorted(&moving_kvs);
    assert!(res);
    foster_child.set_has_foster_child(this.has_foster_child());
    // Remove the moved slots from this
    let high_fence_slot_id = this.high_fence_slot_id();
    this.remove_range(moving_slot_ids[0], high_fence_slot_id);
    let foster_child_id =
        InnerVal::new_with_frame_id(foster_child.get_id(), foster_child.frame_id());
    let res = this.insert(&foster_key, &foster_child_id.to_bytes());
    assert!(res);
    this.set_has_foster_child(true);

    foster_key
}

/// Split this page into two pages with minimum moving slots
/// from this to foster child.
fn split_min_move(this: &mut FrameWriteGuard, foster_child: &mut FrameWriteGuard) -> Vec<u8> {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::Split);

    // Just move one slot from this to foster child.
    if this.active_slot_count() == 0 {
        panic!("Cannot split an empty page");
    }

    let moving_slot_id = this.high_fence_slot_id() - 1;
    let moving_key = this.get_raw_key(moving_slot_id);
    let moving_val = this.get_val(moving_slot_id);

    foster_child.init();
    foster_child.set_level(this.level());
    foster_child.set_low_fence(moving_key);
    foster_child.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
    foster_child.set_right_most(this.is_right_most());
    let res = foster_child.insert(moving_key, moving_val);
    assert!(res);
    foster_child.set_has_foster_child(this.has_foster_child());

    let foster_key = moving_key.to_vec();
    // Remove the moved slot from this
    this.remove_at(moving_slot_id);

    let foster_child_id =
        InnerVal::new_with_frame_id(foster_child.get_id(), foster_child.frame_id());
    let res = this.insert(&foster_key, &foster_child_id.to_bytes());
    assert!(res);
    this.set_has_foster_child(true);

    foster_key
}

/// Splitting the page and insert the key-value pair in the appropriate page.
///
/// We want to insert the key-value pair into the page.
/// However, the page does not have enough space to insert the key-value pair.
///
/// Some assumptions:
/// 1. Key must be in the range of the page.
/// 2. The foster child is a unused page. It will be initialized in this function.
fn split_insert(
    this: &mut FrameWriteGuard,
    foster_child: &mut FrameWriteGuard,
    key: &[u8],
    value: &[u8],
) -> bool {
    let foster_key = split_even(this, foster_child);

    // Now, we have two pages: this and foster_child.
    // We need to decide which page to insert the key-value pair.
    // If the key is less than the foster key, we insert the key into this.
    // Otherwise, we insert the key into the foster child.

    let res = if key < &foster_key {
        this.insert(key, value)
    } else {
        foster_child.insert(key, value)
    };
    if !res {
        // Need to split the page into three
        // There are some tricky cases where insertion causes the page to split into three pages.
        // E.g. Page has capacity of 6 and the current page looks like this:
        // [[xxx],[zzz]]
        // We want to insert [yyyy]
        // None of [[xxx],[yyyy]] or [[zzz],[yyyy]] can work.
        // We need to split the page into three pages.
        // [[xxx]], [[yyyy]], [[zzz]]

        // If we cannot insert the key into one of the pages, then we need to split the page into three.
        // This: [l, r1, r2, ..., rk, (f)x h]
        // Foster1: [l(x), x, (f)rk+1, h]
        // Foster2: [l(rk+1), rk+1, rk+2, ..., h]
        unimplemented!("Need to split the page into three")
    }
    res
}

/*  This is not a clean implementation. Will comment out for now to reduce the complexity.
    Split will panic if the page is required to split into three pages.
/// Assumptions:
/// * We want to insert the key-value pair into the page.
/// * However, the page does not have enough space to insert the key-value pair.
/// * Before: this: [[xxx], [zzz]] and we insert [yyyy]
/// * After: this: [[xxx]], new_page1: [[yyyy]], new_page2: [[zzz]]
/// Returns whether the new_page2 is used or not.
fn split_insert_triple(
    this: &mut FrameWriteGuard,
    new_page1: &mut FrameWriteGuard,
    new_page2: &mut FrameWriteGuard,
    inserting_key: &[u8],
    inserting_value: &[u8],
) -> bool {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::Split);

    let mut moving_slot_ids = Vec::new();
    let mut moving_kvs = Vec::new();
    for i in (1..this.high_fence_slot_id()).rev() {
        let key = this.get_raw_key(i);
        let val = this.get_val(i);
        if key >= inserting_key {
            moving_slot_ids.push(i);
            moving_kvs.push((key, val));
        } else {
            break;
        }
    }
    if moving_kvs.is_empty() {
        // We waste new_page2 but it is fine for now.
        new_page1.init();
        new_page1.set_level(this.level());
        new_page1.set_low_fence(inserting_key);
        new_page1.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
        new_page1.set_right_most(this.is_right_most());
        let res = new_page1.insert(inserting_key, inserting_value);
        assert!(res);
        new_page1.set_has_foster_child(false);

        // Connect this to new_page1
        let foster_child_id = InnerVal::new_with_frame_id(new_page1.get_id(), new_page1.frame_id());
        let res = this.insert(inserting_key, &foster_child_id.to_bytes());
        assert!(res);
        this.set_has_foster_child(true);
        false
    } else {
        // Reverse the moving slots
        moving_kvs.reverse();
        moving_slot_ids.reverse();

        let foster_key1 = inserting_key;
        let foster_key2 = moving_kvs[0].0.to_vec();

        new_page1.init();
        new_page1.set_level(this.level());
        new_page1.set_low_fence(foster_key1);
        new_page1.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
        new_page1.set_right_most(this.is_right_most());
        let res = new_page1.insert(inserting_key, inserting_value);
        assert!(res);
        new_page1.set_has_foster_child(true);

        new_page2.init();
        new_page2.set_level(this.level());
        new_page2.set_low_fence(&foster_key2);
        new_page2.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
        new_page2.set_right_most(this.is_right_most());
        let res = new_page2.append_sorted(&moving_kvs);
        assert!(res);
        new_page2.set_has_foster_child(this.has_foster_child());

        // Remove the moved slots from this
        let high_fence_slot_id = this.high_fence_slot_id();
        this.remove_range(moving_slot_ids[0], high_fence_slot_id);

        // Connect this to new_page1
        let foster_child_id = InnerVal::new_with_frame_id(new_page1.get_id(), new_page1.frame_id());
        let res = this.insert(foster_key1, &foster_child_id.to_bytes());
        assert!(res);
        this.set_has_foster_child(true);

        // Connect new_page1 to new_page2
        let foster_child_id = InnerVal::new_with_frame_id(new_page2.get_id(), new_page2.frame_id());
        let res = new_page1.insert(&foster_key2, &foster_child_id.to_bytes());
        assert!(res);
        new_page1.set_has_foster_child(true);
        true
    }
}
*/

/// Merge the foster child page into this page.
/// The foster child page will be deleted.
/// Before:
///   this [k0, k2) --> foster_child [k1, k2)
///
/// After:
///   this [k0, k2)
fn merge(this: &mut FrameWriteGuard, foster_child: &mut FrameWriteGuard) {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::Merge);

    debug_assert!(is_parent_and_child(this, foster_child));
    debug_assert!(is_foster_relationship(this, foster_child));

    let mut kvs = Vec::new();
    for i in 1..=foster_child.active_slot_count() {
        let key = foster_child.get_raw_key(i);
        let val = foster_child.get_val(i);
        kvs.push((key, val));
    }
    let foster_child_slot_id = this.foster_child_slot_id();
    this.remove_at(foster_child_slot_id);
    let res = this.append_sorted(&kvs);
    assert!(res);
    this.set_has_foster_child(foster_child.has_foster_child());
    this.set_right_most(foster_child.is_right_most());
    foster_child.init();
    foster_child.set_valid(false);

    #[cfg(debug_assertions)]
    {
        this.run_consistency_checks(false);
        foster_child.run_consistency_checks(false);
    }
}

/// Move some slots from one page to another page so that the two pages approximately
/// have the same number of bytes.
/// Assumptions:
/// 1. this page is the foster parent of the foster child.
/// 2. The two pages are initialized. (have low fence and high fence)
/// 3. Balancing does not move the foster key from one page to another.
/// 4. Balancing does not move the low fence and high fence.
fn balance(this: &mut FrameWriteGuard, foster_child: &mut FrameWriteGuard) {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::LoadBalance);

    debug_assert!(is_parent_and_child(this, foster_child));
    debug_assert!(is_foster_relationship(this, foster_child));

    // Calculate the total bytes of the two pages.
    let this_total = this.total_bytes_used();
    let foster_child_total = foster_child.total_bytes_used();
    match this_total.cmp(&foster_child_total) {
        std::cmp::Ordering::Equal => {
            // The two pages have the same number of bytes.
        }
        std::cmp::Ordering::Greater => {
            // Move some slots from this to foster child.
            let mut diff = (this_total - foster_child_total) / 2;
            let mut moving_slot_ids = Vec::new();
            let mut moving_kvs: Vec<(&[u8], &[u8])> = Vec::new();
            for i in (1..this.foster_child_slot_id()).rev() {
                let key = this.get_raw_key(i);
                let val = this.get_val(i);
                let bytes_needed = this.bytes_needed(key, val);
                if diff >= bytes_needed {
                    diff -= bytes_needed;
                    moving_slot_ids.push(i);
                    moving_kvs.push((key, val));
                } else {
                    break;
                }
            }
            if moving_kvs.is_empty() {
                // No slots to move
                return;
            }
            // Reverse the moving slots
            moving_kvs.reverse();
            moving_slot_ids.reverse();
            // Before:
            // this [l, k0, k1, k2, ..., f(kN), h) --> foster_child [l(kN), kN, kN+1, ..., h)
            // After:
            // this [l, k0, k1, ..., f(kN-m), h) --> foster_child [l(kN-m), kN-m, kN-m+1, ..., h)
            foster_child.set_low_fence(moving_kvs[0].0);
            for (key, val) in moving_kvs {
                let res = foster_child.insert(key, val);
                assert!(res);
            }
            // Remove the moved slots from this
            let high_fence_slot_id = this.high_fence_slot_id();
            this.remove_range(moving_slot_ids[0], high_fence_slot_id);
            let res = this.insert(
                foster_child.get_raw_key(0),
                &InnerVal::new_with_frame_id(foster_child.get_id(), foster_child.frame_id())
                    .to_bytes(),
            );
            assert!(res);
        }
        std::cmp::Ordering::Less => {
            // Move some slots from foster child to this.
            let mut diff = (foster_child_total - this_total) / 2;
            let mut moving_slot_ids = Vec::new();
            let mut moving_kvs = Vec::new();
            let end = if foster_child.has_foster_child() {
                // We do not move the foster key
                foster_child.foster_child_slot_id()
            } else {
                foster_child.high_fence_slot_id()
            };
            for i in 1..end {
                let key = foster_child.get_raw_key(i);
                let val = foster_child.get_val(i);
                let bytes_needed = foster_child.bytes_needed(key, val);
                if diff >= bytes_needed {
                    diff -= bytes_needed;
                    moving_slot_ids.push(i);
                    moving_kvs.push((key, val));
                } else {
                    break;
                }
            }
            if moving_kvs.is_empty() {
                // No slots to move
                return;
            }
            // Before:
            // this [l, k0, k1, k2, ..., f(kN), h) --> foster_child [l(kN), kN, kN+1, ..., h)
            // After:
            // this [l, k0, k1, ..., f(kN+m), h) --> foster_child [l(kN+m), kN+m, kN+m+1, ..., h)
            let foster_child_slot_id = this.foster_child_slot_id();
            this.remove_at(foster_child_slot_id);
            for (key, val) in moving_kvs {
                let res = this.insert(key, val);
                assert!(res);
            }
            // Remove the moved slots from foster child
            foster_child.remove_range(
                moving_slot_ids[0],
                moving_slot_ids[moving_slot_ids.len() - 1] + 1,
            );
            let foster_key = foster_child.get_raw_key(1).to_vec();
            foster_child.set_low_fence(&foster_key);
            let foster_child_id =
                InnerVal::new_with_frame_id(foster_child.get_id(), foster_child.frame_id());
            let res = this.insert(&foster_key, &foster_child_id.to_bytes());
            assert!(res);
        }
    }
}

/// Adopt the foster child of the child page.
/// The foster child of the child page is removed and becomes the child of the parent page.
///
/// Parent page
/// * insert the foster key and the foster child page id.
///
/// Child page
/// * remove the foster key from the child page.
/// * remove the foster child flag and set the high fence to the foster key.
///
/// Before:
///   parent [k0, k2)
///    |
///    v
///   child1 [k0, k2) --> child2 [k1, k2)
//
/// After:
///   parent [k0, k2)
///    +-------------------+
///    |                   |
///    v                   v
///   child1 [k0, k1)    child2 [k1, k2)
fn adopt(parent: &mut FrameWriteGuard, child1: &mut FrameWriteGuard) {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::Adopt);

    debug_assert!(is_parent_and_child(parent, child1));
    debug_assert!(!is_foster_relationship(parent, child1));
    debug_assert!(child1.has_foster_child());

    // Try insert the foster key and value into parent page.
    let res = parent.insert(
        child1.get_foster_key(),
        child1.get_val(child1.foster_child_slot_id()),
    );
    assert!(res);
    // Make the foster key the high fence of the child page.
    let high_fence_slot_id = child1.high_fence_slot_id();
    child1.remove_at(high_fence_slot_id); // Remove the high fence to make the foster key the high fence.
    child1.set_has_foster_child(false);
    child1.set_right_most(false);
}

/// Anti-adopt.
/// Make a child a foster parent. This operation is needed when we do load balancing between siblings.
///
/// Before:
/// parent [..., k0, k1, k2, ..., kN)
///  +-------------------+
///  |                   |
///  v                   v
/// child1 [k0, k1)   child2 [k1, k2)
///
/// After:
/// parent [..., k0, k2, ..., kN)
///  |
///  v
/// child1 [k0, k2) --> foster_child [k1, k2)
fn anti_adopt(parent: &mut FrameWriteGuard, child1: &mut FrameWriteGuard) {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::AntiAdopt);

    debug_assert!(is_parent_and_child(parent, child1));
    debug_assert!(!is_foster_relationship(parent, child1));
    debug_assert!(!child1.has_foster_child());

    // Identify child1 slot
    let low_key = BTreeKey::new(child1.get_raw_key(child1.low_fence_slot_id()));
    let slot_id = parent.upper_bound_slot_id(&low_key);
    if parent.has_foster_child() {
        debug_assert!(slot_id < parent.foster_child_slot_id());
    } else {
        debug_assert!(slot_id < parent.high_fence_slot_id());
    }
    // We remove k1 from the parent
    let k1 = parent.get_raw_key(slot_id);
    let child2_ptr = parent.get_val(slot_id);
    let k2 = parent.get_raw_key(slot_id + 1);
    child1.set_high_fence(k2);
    child1.set_has_foster_child(true);
    if parent.is_right_most() && slot_id + 1 == parent.high_fence_slot_id() {
        // If k2 is the high fence of the parent, then child1 also becomes the right most child
        // because k2 is the high fence of child1 as well.
        child1.set_right_most(true);
    }
    let res = child1.insert(k1, child2_ptr);
    if !res {
        panic!("Cannot insert the slot into the child page");
    }
    parent.remove_at(slot_id);
}

/// Descend the root page to the child page if the root page
/// contains only the page id of the child page.
/// Note that the root page is never deleted.
/// The child page will be deleted.
///
/// Before:
/// root [-inf, +inf)
///  |
///  v
/// child [-inf, +inf)
///
/// After:
/// root [-inf, +inf)
fn descend_root(root: &mut FrameWriteGuard, child: &mut FrameWriteGuard) {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::DescendRoot);

    assert!(root.is_root());
    // If the root contains only the page id of the child,
    // then we can push the child's content to the root and
    // descend the root level to the child level.
    assert_eq!(root.active_slot_count(), 1);
    assert_eq!(deserialize_page_id(root.get_val(1)), Some(child.get_id()));
    assert_eq!(child.get_low_fence(), BTreeKey::MinusInfty);
    assert_eq!(child.get_high_fence(), BTreeKey::PlusInfty);

    root.remove_at(1);
    root.decrement_level();

    let mut kvs = Vec::new();
    for i in 1..=child.active_slot_count() {
        let key = child.get_raw_key(i);
        let val = child.get_val(i);
        kvs.push((key, val));
    }
    let res = root.append_sorted(&kvs);
    assert!(res);
    child.init();
    child.set_valid(false);
}

/// Ascend the root page to the parent page if the root page
/// contains a foster child.
/// Note that the root page will still be the root page after
/// the ascend operation.
///
/// Before:
/// root [-inf, +inf) --> foster_child [k0, +inf)
///
/// After:
/// root [-inf, +inf)
///  +-------------------+
///  |                   |
///  v                   v
/// child [-inf, k0)    foster_child [k0, +inf)
fn ascend_root(root: &mut FrameWriteGuard, child: &mut FrameWriteGuard) {
    #[cfg(feature = "stat")]
    inc_local_stat_success(OpType::AscendRoot);

    assert!(root.is_root());
    assert!(root.has_foster_child());
    assert!(!root.empty());
    assert!(child.empty());

    // Copy the foster slot data to local variables.
    let foster_key = root.get_foster_key().to_owned();

    // Set level, has_foster_child flag, and fence keys
    child.init();
    child.set_level(root.level());
    child.set_low_fence(root.get_raw_key(root.low_fence_slot_id()));
    child.set_left_most(root.is_left_most());
    child.set_right_most(false); // Right most will be false for the child page
    child.set_high_fence(&foster_key);
    root.increment_level();
    root.set_has_foster_child(false);

    // Move the root's data to the child
    let mut kvs = Vec::new();
    for i in 1..root.foster_child_slot_id() {
        let key = root.get_raw_key(i);
        let val = root.get_val(i);
        kvs.push((key, val));
    }
    let res = child.append_sorted(&kvs);
    assert!(res);

    // Remove the moved slots from the root
    let foster_child_slot_id = root.foster_child_slot_id();
    root.remove_range(1, foster_child_slot_id);
    let child_id = InnerVal::new_with_frame_id(child.get_id(), child.frame_id());
    root.insert(&[], &child_id.to_bytes());

    #[cfg(debug_assertions)]
    {
        root.run_consistency_checks(false);
        child.run_consistency_checks(false);
    }
}

#[allow(dead_code)]
fn print_page(p: &Page) {
    println!(
        "----------------- Page ID: {} -----------------",
        p.get_id()
    );
    println!("Level: {}", p.level());
    println!("Size: {}", p.total_bytes_used());
    println!("Available space: {}", p.total_free_space());
    println!("Low fence: {:?}", p.get_low_fence());
    println!("High fence: {:?}", p.get_high_fence());
    println!("Slot count: {}", p.slot_count());
    println!("Active slot count: {}", p.active_slot_count());
    println!("Foster child: {}", p.has_foster_child());
    for i in 1..=p.active_slot_count() {
        let key = p.get_raw_key(i);
        let val = p.get_val(i);
        let key = if key.is_empty() {
            "[]".to_owned()
        } else {
            usize::from_be_bytes(key.try_into().unwrap()).to_string()
        };
        let val = if p.has_foster_child() && i == p.active_slot_count() {
            let page_id = u32::from_be_bytes(val.try_into().unwrap()).to_string();
            format!("PageID({})", page_id)
        } else {
            // Print the first 8 bytes of the value
            if p.is_leaf() {
                let size = val.len();
                let min_size = 8.min(size);
                let val = &val[..min_size];
                format!("{:?}...len={}", val, size)
            } else {
                let page_id = u32::from_be_bytes(val.try_into().unwrap()).to_string();
                format!("PageID({})", page_id)
            }
        };
        println!("Slot {}: Key: {}, Value: {}", i, key, val);
    }
    println!("----------------------------------------------");
}

struct RuntimeStats {
    num_keys: AtomicUsize,
}

impl RuntimeStats {
    fn new() -> Self {
        RuntimeStats {
            num_keys: AtomicUsize::new(0),
        }
    }

    fn inc_num_keys(&self) {
        self.num_keys.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_num_keys(&self) {
        self.num_keys.fetch_sub(1, Ordering::Relaxed);
    }

    fn get_num_keys(&self) -> usize {
        self.num_keys.load(Ordering::Relaxed)
    }
}

pub struct FosterBtree<T: MemPool> {
    pub c_key: ContainerKey,
    pub root_key: PageFrameKey,
    pub mem_pool: Arc<T>,
    stats: RuntimeStats,
    unused_pages: ConcurrentQueue<PageId>,
    // pub wal_buffer: LogBufferRef,
}

impl<T: MemPool> FosterBtree<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let root_key = {
            let mut root = mem_pool.create_new_page_for_write(c_key).unwrap();
            root.init_as_root();
            root.page_frame_key().unwrap()
        };

        FosterBtree {
            c_key,
            root_key,
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
            unused_pages: ConcurrentQueue::unbounded(),
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, root_page_id: PageId) -> Self {
        // Assumes that the root page is the first page in this container.
        FosterBtree {
            c_key,
            root_key: PageFrameKey::new(c_key, root_page_id),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
            unused_pages: ConcurrentQueue::unbounded(),
        }
    }

    pub fn bulk_insert_create<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let stats = RuntimeStats::new();

        let mut root = mem_pool.create_new_page_for_write(c_key).unwrap();
        root.init_as_root();
        let root_key = root.page_frame_key().unwrap();

        // Keep iterating the iter and appending the key-value pairs to the root page.
        let mut current_page = root;
        for (key, value) in iter {
            stats.inc_num_keys();
            if !current_page.insert(key.as_ref(), value.as_ref()) {
                // Create a new page and split the current page.
                let mut new_page = mem_pool.create_new_page_for_write(c_key).unwrap();
                let foster_key = split_min_move(&mut current_page, &mut new_page);
                assert!(foster_key.as_slice() < key.as_ref());
                // Insert it into new page. If it fails, panic.
                assert!(new_page.insert(key.as_ref(), value.as_ref()));

                mem_pool.fast_evict(current_page.frame_id()).unwrap();
                drop(current_page);

                current_page = new_page;
            }
        }

        FosterBtree {
            c_key,
            root_key,
            mem_pool: mem_pool.clone(),
            stats,
            unused_pages: ConcurrentQueue::unbounded(),
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.stats.get_num_keys()
    }

    /// Thread-unsafe traversal of the BTree
    pub fn page_traverser(&self) -> FosterBTreePageTraversal<T> {
        FosterBTreePageTraversal::new(self)
    }

    /// Check the consistency of the BTree
    /// This is a thread-unsafe operation
    pub fn check_consistency(&self) {
        let mut checker = ConsistencyChecker {};
        let traverser = self.page_traverser();
        traverser.visit(&mut checker);
    }

    /// Accumulate the statistics of the pages in the BTree
    /// This is a thread-unsafe operation
    pub fn page_stats(&self, verbose: bool) -> String {
        let mut stats = PageStatsGenerator::new();
        let traverser = self.page_traverser();
        traverser.visit(&mut stats);
        stats.to_string(verbose)
    }

    pub fn op_stats(&self) -> String {
        #[cfg(feature = "stat")]
        {
            let stats = GLOBAL_STAT.lock().unwrap();
            LOCAL_STAT.with(|local_stat| {
                stats.merge(&local_stat.stat);
                local_stat.stat.clear();
            });
            stats.to_string()
        }
        #[cfg(not(feature = "stat"))]
        {
            "Stat is disabled".to_string()
        }
    }

    /// System transaction that allocates a new page.
    fn allocate_page(&self) -> FrameWriteGuard {
        if let Ok(page_id) = self.unused_pages.pop() {
            let page = self
                .mem_pool
                .get_page_for_write(PageFrameKey::new(self.c_key, page_id));
            if let Ok(mut page) = page {
                assert!(!page.is_valid());
                page.init();
                return page;
            }
        }
        let mut new_page = loop {
            let page = self.mem_pool.create_new_page_for_write(self.c_key);
            match page {
                Ok(page) => break page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        };
        new_page.init();
        new_page
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        let base = 2;
        let mut attempts = 0;
        loop {
            #[cfg(feature = "stat")]
            inc_shared_page_latch_count();
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    #[cfg(feature = "stat")]
                    inc_shared_page_latch_failures(page.is_leaf(), attempts as usize);
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_info!(
                        "Read latch grant failed for page: {}, attempts: {}",
                        page_key,
                        attempts
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                    attempts += 1;
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    // sleep for a while
                    std::thread::sleep(Duration::from_millis(1)); // Backoff because there is a lot of contention in the buffer pool
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn insert_at_slot_or_split(
        &self,
        this: &mut FrameWriteGuard,
        slot: u32,
        key: &[u8],
        value: &[u8],
    ) {
        // Easy checks to see if we can insert the key-value pair into the page.
        if key.len() + value.len() > AVAILABLE_PAGE_SIZE {
            panic!(
                "Key-value pair (len: {}, {}) is too large to insert into the page of size: {}",
                key.len(),
                value.len(),
                AVAILABLE_PAGE_SIZE
            );
        }
        if !this.insert_at(slot, key, value) {
            #[cfg(feature = "stat")]
            inc_local_stat_trigger(OpType::Split);
            // Split the page
            let mut foster_child = self.allocate_page();
            if !split_insert(this, &mut foster_child, key, value) {
                panic!("Split triple required");
                // let mut new_page1 = self.allocate_page();
                // let mut new_page2 = self.allocate_page();
                // let foster_key = this.get_foster_key();
                // if key < foster_key {
                //     if !split_insert_triple(this, &mut new_page1, &mut new_page2, key, value) {
                //         let _ = self.unused_pages.push(new_page2.get_id());
                //     }
                // } else if !split_insert_triple(
                //     &mut foster_child,
                //     &mut new_page1,
                //     &mut new_page2,
                //     key,
                //     value,
                // ) {
                //     let _ = self.unused_pages.push(new_page2.get_id());
                // }
            }
        }
    }

    fn update_at_slot_or_split(
        &self,
        this: &mut FrameWriteGuard,
        slot: u32,
        key: &[u8],
        value: &[u8],
    ) {
        // Easy checks to see if we can insert the key-value pair into the page.
        if key.len() + value.len() > AVAILABLE_PAGE_SIZE {
            panic!(
                "Key-value pair (len: {}, {}) is too large to insert into the page of size: {}",
                key.len(),
                value.len(),
                AVAILABLE_PAGE_SIZE
            );
        }

        if !this.update_at(slot, None, value) {
            #[cfg(feature = "stat")]
            inc_local_stat_trigger(OpType::Split);
            // Remove the slot and split insert
            this.remove_at(slot);
            // Try inserting the key-value pair into this page again after removing the slot.
            if this.insert(key, value) {
            } else {
                let mut foster_child = self.allocate_page();
                if !split_insert(this, &mut foster_child, key, value) {
                    panic!("Split triple required");
                    // let mut new_page1 = self.allocate_page();
                    // let mut new_page2 = self.allocate_page();
                    // let foster_key = this.get_foster_key();
                    // if key < foster_key {
                    //     if !split_insert_triple(this, &mut new_page1, &mut new_page2, key, value) {
                    //         let _ = self.unused_pages.push(new_page2.get_id());
                    //     }
                    // } else if !split_insert_triple(
                    //     &mut foster_child,
                    //     &mut new_page1,
                    //     &mut new_page2,
                    //     key,
                    //     value,
                    // ) {
                    //     let _ = self.unused_pages.push(new_page2.get_id());
                    // }
                }
            }
        }
    }

    #[inline]
    fn modify_structure_if_needed_for_read<'a>(
        &self,
        is_foster_relationship: bool,
        this: FrameReadGuard<'a>,
        child: FrameReadGuard<'a>,
        op_byte: &mut OpByte,
    ) -> (Option<OpType>, FrameReadGuard<'a>, FrameReadGuard<'a>) {
        if should_split_this(&this, op_byte) {
            log_info!("Should split this page: {}", this.get_id());
            #[cfg(feature = "stat")]
            inc_local_stat_trigger(OpType::Split);
            let mut this = match this.try_upgrade(true) {
                Ok(this) => this,
                Err(this) => {
                    return (None, this, child);
                }
            };
            let mut foster_child = self.allocate_page();
            split_even(&mut this, &mut foster_child);
            return (Some(OpType::Split), this.downgrade(), child);
        }
        let op = if is_foster_relationship {
            should_modify_foster_relationship(&this, &child, op_byte)
        } else {
            should_modify_parent_child_relationship(&this, &child, op_byte)
        };
        if let Some(op) = op {
            log_info!(
                "Should modify structure: {:?}, This: {}, Child: {}",
                op,
                this.get_id(),
                child.get_id()
            );
            #[cfg(feature = "stat")]
            inc_local_stat_trigger(op.clone());
            let (mut this, mut child) = match this.try_upgrade(false) {
                Ok(this) => {
                    let child = child.try_upgrade(false);
                    match child {
                        Ok(child) => (this, child),
                        Err(child) => {
                            return (None, this.downgrade(), child);
                        }
                    }
                }
                Err(this) => {
                    return (None, this, child);
                }
            };
            log_info!(
                "Ready to modify structure: {:?}, This: {}, Child: {}",
                op,
                this.get_id(),
                child.get_id()
            );
            // make both pages dirty
            this.dirty().store(true, Ordering::Relaxed);
            child.dirty().store(true, Ordering::Relaxed);
            self.modify_structure(op, &mut this, &mut child);
            (Some(op), this.downgrade(), child.downgrade())
        } else {
            (None, this, child)
        }
    }

    fn modify_structure(
        &self,
        op: OpType,
        this: &mut FrameWriteGuard,
        child: &mut FrameWriteGuard,
    ) {
        match op {
            OpType::Merge => {
                // Merge the foster child into this page
                merge(this, child);
                self.unused_pages.push(child.get_id()).unwrap();
            }
            OpType::LoadBalance => {
                // Load balance between this and the foster child
                balance(this, child);
            }
            OpType::Adopt => {
                // Adopt the foster child of the child page
                adopt(this, child);
            }
            OpType::AntiAdopt => {
                // Anti-adopt the foster child of the child page
                anti_adopt(this, child);
            }
            OpType::AscendRoot => {
                let mut new_page = self.allocate_page();
                new_page.init();
                // Ascend the root page to the parent page
                ascend_root(this, &mut new_page);
            }
            OpType::DescendRoot => {
                // Descend the root page to the child page
                descend_root(this, child);
                self.unused_pages.push(child.get_id()).unwrap();
            }
            _ => {
                panic!("Unexpected operation");
            }
        }
    }

    pub(in crate::access_method) fn traverse_to_leaf_for_read(&self, key: &[u8]) -> FrameReadGuard {
        let mut current_page = self.read_page(self.root_key);
        let mut op_byte = OpByte::new();
        loop {
            let this_page = current_page;
            log_info!("Traversal for read, page: {}", this_page.get_id());
            if this_page.is_leaf() {
                if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                    // Check whether the foster child should be traversed.
                    let val = InnerVal::from_bytes(this_page.get_foster_val());
                    let foster_page_key =
                        PageFrameKey::new_with_frame_id(self.c_key, val.page_id, val.frame_id);
                    let foster_page = self.read_page(foster_page_key);

                    let this_page = if foster_page.frame_id() != val.frame_id {
                        log_debug!(
                            "Frame ID mismatch: Expected: {}, Actual: {}.",
                            val.frame_id,
                            foster_page.frame_id()
                        );
                        let foster_child_slot_id = this_page.foster_child_slot_id();
                        fix_frame_id(
                            this_page,
                            foster_child_slot_id,
                            &foster_page.page_frame_key().unwrap(),
                        )
                    } else {
                        this_page
                    };

                    // Now we have two locks. We need to release the lock of the current page.
                    let (op, this_page, foster_page) = self.modify_structure_if_needed_for_read(
                        true,
                        this_page,
                        foster_page,
                        &mut op_byte,
                    );
                    match op {
                        Some(OpType::Merge) | Some(OpType::LoadBalance) => {
                            current_page = this_page;
                            continue;
                        }
                        None | Some(OpType::Split) | Some(OpType::AscendRoot) => {
                            // Start from the child
                            op_byte.reset();
                            current_page = foster_page;
                            continue;
                        }
                        _ => {
                            panic!("Unexpected operation");
                        }
                    }
                } else {
                    return this_page;
                }
            }
            let slot_id = this_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
            let is_foster_relationship =
                this_page.has_foster_child() && slot_id == this_page.foster_child_slot_id();
            let val = InnerVal::from_bytes(this_page.get_val(slot_id));
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, val.page_id, val.frame_id);

            let next_page = self.read_page(page_key);

            // Check if the frame_id is the same
            let this_page = if next_page.frame_id() != val.frame_id {
                log_debug!(
                    "Frame ID mismatch: Expected: {}, Actual: {}.",
                    val.frame_id,
                    next_page.frame_id()
                );
                fix_frame_id(this_page, slot_id, &next_page.page_frame_key().unwrap())
            } else {
                this_page
            };

            // Now we have two locks. We need to release the lock of the current page.
            let (op, this_page, next_page) = self.modify_structure_if_needed_for_read(
                is_foster_relationship,
                this_page,
                next_page,
                &mut op_byte,
            );
            match op {
                Some(OpType::Merge)
                | Some(OpType::LoadBalance)
                | Some(OpType::DescendRoot)
                | Some(OpType::Adopt) => {
                    // Continue from the current page
                    current_page = this_page;
                    continue;
                }
                None | Some(OpType::Split) | Some(OpType::AscendRoot) | Some(OpType::AntiAdopt) => {
                    // Start from the child
                    op_byte.reset();
                    current_page = next_page;
                    continue;
                }
            }
        }
    }

    pub(in crate::access_method) fn try_traverse_to_leaf_for_write(
        &self,
        key: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let leaf_page = self.traverse_to_leaf_for_read(key);
        #[cfg(feature = "stat")]
        inc_exclusive_page_latch_count();
        match leaf_page.try_upgrade(true) {
            Ok(upgraded) => Ok(upgraded),
            Err(_) => {
                #[cfg(feature = "stat")]
                inc_exclusive_page_latch_failures(true);
                Err(AccessMethodError::PageWriteLatchFailed)
            }
        }
    }

    pub(in crate::access_method) fn traverse_to_leaf_for_write(
        &self,
        key: &[u8],
    ) -> FrameWriteGuard {
        let base = 2;
        let mut attempts = 0;
        let leaf_page = {
            loop {
                match self.try_traverse_to_leaf_for_write(key) {
                    Ok(leaf_page) => {
                        #[cfg(feature = "stat")]
                        inc_local_additional_traversals(attempts);
                        break leaf_page;
                    }
                    Err(AccessMethodError::PageWriteLatchFailed) => {
                        log_info!(
                            "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                            attempts,
                            base * attempts
                        );
                        std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                        attempts += 1;
                    }
                    Err(e) => {
                        panic!("Unexpected error: {:?}", e);
                    }
                }
            }
        };
        leaf_page
    }

    pub(in crate::access_method) fn get_with_physical_address(
        &self,
        key: &[u8],
    ) -> Result<(Vec<u8>, (PageId, u32)), AccessMethodError> {
        let leaf_page = self.traverse_to_leaf_for_read(key);
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else {
            // We can get the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                Ok((
                    leaf_page.get_val(slot_id).to_vec(),
                    (leaf_page.get_id(), leaf_page.frame_id()),
                ))
            } else {
                // Non-existent key
                Err(AccessMethodError::KeyNotFound)
            }
        }
    }
}

impl<T: MemPool> UniqueKeyIndex for FosterBtree<T> {
    type Iter = FosterBtreeRangeScanner<T>;

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let leaf_page = self.traverse_to_leaf_for_read(key);
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // Lower fence. Non-existent key
            Err(AccessMethodError::KeyNotFound)
        } else {
            // We can get the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                Ok(leaf_page.get_val(slot_id).to_vec())
            } else {
                // Non-existent key
                Err(AccessMethodError::KeyNotFound)
            }
        }
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let mut leaf_page = self.traverse_to_leaf_for_write(key);
        log_info!("Acquired write lock for page {}", leaf_page.get_id());
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // Lower fence so insert is ok. We insert the key-value at the next position of the lower fence.
            self.stats.inc_num_keys();
            self.insert_at_slot_or_split(&mut leaf_page, slot_id + 1, key, value);
            Ok(())
        } else {
            // We can insert the key if it does not exist
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                Err(AccessMethodError::KeyDuplicate)
            } else {
                self.stats.inc_num_keys();
                self.insert_at_slot_or_split(&mut leaf_page, slot_id + 1, key, value);
                Ok(())
            }
        }
    }

    fn update(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let mut leaf_page = self.traverse_to_leaf_for_write(key);
        log_info!("Acquired write lock for page {}", leaf_page.get_id());
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // We cannot update the lower fence
            Err(AccessMethodError::KeyNotFound)
        } else {
            // We can update the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                self.update_at_slot_or_split(&mut leaf_page, slot_id, key, value);
                Ok(())
            } else {
                // Non-existent key
                Err(AccessMethodError::KeyNotFound)
            }
        }
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let mut leaf_page = self.traverse_to_leaf_for_write(key);
        log_info!("Acquired write lock for page {}", leaf_page.get_id());
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // Lower fence so insert is ok. We insert the key-value at the next position of the lower fence.
            self.stats.inc_num_keys();
            self.insert_at_slot_or_split(&mut leaf_page, slot_id + 1, key, value);
        } else {
            // We can insert the key if it does not exist
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                self.update_at_slot_or_split(&mut leaf_page, slot_id, key, value);
            } else {
                // Non-existent key
                self.stats.inc_num_keys();
                self.insert_at_slot_or_split(&mut leaf_page, slot_id + 1, key, value);
            }
        }
        Ok(())
    }

    fn upsert_with_merge(
        &self,
        key: &[u8],
        value: &[u8],
        merge_func: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        let mut leaf_page = self.traverse_to_leaf_for_write(key);
        log_info!("Acquired write lock for page {}", leaf_page.get_id());
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // Lower fence so insert is ok. We insert the key-value at the next position of the lower fence.
            self.stats.inc_num_keys();
            self.insert_at_slot_or_split(&mut leaf_page, slot_id + 1, key, value);
        } else {
            // We can insert the key if it does not exist
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                let new_value = merge_func(leaf_page.get_val(slot_id), value);
                self.update_at_slot_or_split(&mut leaf_page, slot_id, key, &new_value);
            } else {
                // Non-existent key
                self.stats.inc_num_keys();
                self.insert_at_slot_or_split(&mut leaf_page, slot_id + 1, key, value);
            }
        }
        Ok(())
    }

    /// Physical deletion of a key
    fn delete(&self, key: &[u8]) -> Result<(), AccessMethodError> {
        let mut leaf_page = self.traverse_to_leaf_for_write(key);
        log_info!("Acquired write lock for page {}", leaf_page.get_id());
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
        if slot_id == 0 {
            // Lower fence cannot be deleted
            Err(AccessMethodError::KeyNotFound)
        } else {
            // We can delete the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                self.stats.dec_num_keys();
                leaf_page.remove_at(slot_id);
                Ok(())
            } else {
                // Non-existent key
                Err(AccessMethodError::KeyNotFound)
            }
        }
    }

    fn scan(self: &Arc<Self>) -> Self::Iter {
        FosterBtreeRangeScanner::new(self, &[], &[])
    }

    fn scan_with_filter(self: &Arc<Self>, filter: FilterFunc) -> Self::Iter {
        FosterBtreeRangeScanner::new_with_filter(self, &[], &[], filter)
    }
}

impl<T: MemPool> OrderedUniqueKeyIndex for FosterBtree<T> {
    type RangeIter = FosterBtreeRangeScanner<T>;

    fn scan_range(self: &Arc<Self>, start_key: &[u8], end_key: &[u8]) -> Self::RangeIter {
        FosterBtreeRangeScanner::new(self, start_key, end_key)
    }

    fn scan_range_with_filter(
        self: &Arc<Self>,
        start_key: &[u8],
        end_key: &[u8],
        filter: FilterFunc,
    ) -> Self::RangeIter {
        FosterBtreeRangeScanner::new_with_filter(self, start_key, end_key, filter)
    }
}

type FilterFunc = Arc<dyn Fn(&[u8], &[u8]) -> bool + Send + Sync>;

/// Scan the BTree in the range [l_key, r_key)
/// To specify all keys, use an empty slice.
/// (l_key, r_key) = (&[], &[]) means [-inf, +inf)
pub struct FosterBtreeRangeScanner<T: MemPool> {
    btree: Arc<FosterBtree<T>>, // Holds the reference to the btree

    // Scan parameters
    l_key: Vec<u8>,
    r_key: Vec<u8>,
    filter: Option<FilterFunc>,

    // States
    initialized: bool,
    finished: bool,
    current_high_fence: Option<Vec<u8>>,
    current_leaf_page: Option<FrameReadGuard<'static>>, // As long as btree is alive, bp is alive so the frame is alive
    current_slot_id: u32,
    parents: Vec<PageFrameKey>,
}

impl<T: MemPool> FosterBtreeRangeScanner<T> {
    fn new(btree: &Arc<FosterBtree<T>>, l_key: &[u8], r_key: &[u8]) -> Self {
        Self {
            btree: btree.clone(),

            l_key: l_key.to_vec(),
            r_key: r_key.to_vec(),
            filter: None,

            initialized: false,
            finished: false,
            current_high_fence: None,
            current_leaf_page: None,
            current_slot_id: 0,
            parents: Vec::new(),
        }
    }

    fn new_with_filter(
        btree: &Arc<FosterBtree<T>>,
        l_key: &[u8],
        r_key: &[u8],
        filter: FilterFunc,
    ) -> Self {
        Self {
            btree: btree.clone(),

            l_key: l_key.to_vec(),
            r_key: r_key.to_vec(),
            filter: Some(filter),

            initialized: false,
            finished: false,
            current_high_fence: None,
            current_leaf_page: None,
            current_slot_id: 0,
            parents: Vec::new(),
        }
    }

    fn l_key(&self) -> BTreeKey {
        if self.l_key.is_empty() {
            BTreeKey::MinusInfty
        } else {
            BTreeKey::new(&self.l_key)
        }
    }

    fn r_key(&self) -> BTreeKey {
        if self.r_key.is_empty() {
            BTreeKey::PlusInfty
        } else {
            BTreeKey::new(&self.r_key)
        }
    }

    fn high_fence(&self) -> BTreeKey {
        if self.current_high_fence.as_ref().unwrap().is_empty() {
            BTreeKey::PlusInfty
        } else {
            BTreeKey::new(self.current_high_fence.as_ref().unwrap())
        }
    }

    fn finish(&mut self) {
        self.finished = true;
        self.current_leaf_page = None;
    }

    #[cfg(feature = "old_iter")]
    fn initialize(&mut self) {
        // Traverse to the leaf page that contains the l_key
        let leaf_page = self.btree.traverse_to_leaf_for_read(self.l_key.as_slice());
        let leaf_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(leaf_page) };
        let mut slot = leaf_page.lower_bound_slot_id(&self.l_key());
        if slot == 0 {
            slot = 1; // Skip the lower fence
        }
        self.current_slot_id = slot;
        self.current_high_fence = Some(
            leaf_page
                .get_raw_key(leaf_page.high_fence_slot_id())
                .to_owned(),
        );
        self.current_leaf_page = Some(leaf_page);
    }

    #[cfg(feature = "old_iter")]
    fn go_to_next_leaf(&mut self) {
        // Traverse to the leaf page that contains the current key (high fence of the previous leaf page)
        // Current key should NOT be equal to the r_key. Otherwise, we are done.
        let leaf_page = self
            .btree
            .traverse_to_leaf_for_read(self.current_high_fence.as_ref().unwrap());
        let leaf_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(leaf_page) };
        self.current_slot_id = 1; // Skip the lower fence
        self.current_high_fence = Some(
            leaf_page
                .get_raw_key(leaf_page.high_fence_slot_id())
                .to_owned(),
        );
        self.current_leaf_page = Some(leaf_page);
    }

    /// Traverse to the leaf page to find the page with l_key.
    /// Creates a parents stack to keep track of the traversal path.
    /// At the end of the function, the stack will contain the path from the root to the leaf page
    /// including the foster pages.
    #[cfg(not(feature = "old_iter"))]
    fn initialize(&mut self) {
        // Push the root page to the stack
        self.parents.push(self.btree.root_key);
        let page_frame_key = self.parents.last().unwrap();
        let mut current_page = self.btree.read_page(*page_frame_key);

        // Loop for traversing from the current page to the leaf page
        // Once we reach this loop, we never go back to the outer loop.
        loop {
            let this_page = current_page;
            if this_page.is_leaf() {
                if this_page.has_foster_child()
                    && this_page.get_foster_key() <= self.l_key.as_slice()
                {
                    // Check whether the foster child should be traversed.
                    let val = InnerVal::from_bytes(this_page.get_foster_val());
                    let foster_page_key = PageFrameKey::new_with_frame_id(
                        self.btree.c_key,
                        val.page_id,
                        val.frame_id,
                    );
                    let foster_page = self.btree.read_page(foster_page_key);
                    current_page = foster_page;
                    self.parents.push(foster_page_key); // Push the visiting page to the stack
                    continue;
                } else {
                    let leaf_page = unsafe {
                        std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(this_page)
                    };

                    // Initialize the slot to the lower bound of the l_key
                    let mut slot = leaf_page.lower_bound_slot_id(&self.l_key());
                    if slot == 0 {
                        slot = 1; // Skip the lower fence
                    }
                    self.current_slot_id = slot;
                    self.current_high_fence = Some(
                        leaf_page
                            .get_raw_key(leaf_page.high_fence_slot_id())
                            .to_owned(),
                    );
                    self.current_leaf_page = Some(leaf_page);
                    return;
                }
            }
            let slot_id = this_page.upper_bound_slot_id(&self.l_key()) - 1;
            let val = InnerVal::from_bytes(this_page.get_val(slot_id));
            let page_key =
                PageFrameKey::new_with_frame_id(self.btree.c_key, val.page_id, val.frame_id);

            let next_page = self.btree.read_page(page_key);

            // Do a prefetch for the next next page
            let prefetch_slot_id = slot_id + 1;
            if prefetch_slot_id < this_page.high_fence_slot_id() {
                let prefetch_val = InnerVal::from_bytes(this_page.get_val(prefetch_slot_id));
                let prefetch_page_key = PageFrameKey::new_with_frame_id(
                    self.btree.c_key,
                    prefetch_val.page_id,
                    prefetch_val.frame_id,
                );
                let _ = self.btree.mem_pool.prefetch_page(prefetch_page_key);
            }

            current_page = next_page;
            self.parents.push(page_key); // Push the visiting page to the stack
        }
    }

    /// Traverse to the leaf page.
    /// This is done by checking the parent page and check if the page is valid.
    /// The validity is checked by checking the valid bit as well as whether the next leaf page
    /// is in the range of this parent page.
    /// If concurrent operations removes the parent page by merging or load balancing, it will
    /// set the valid bit to false before releasing the latch. Therefore, we can check the valid
    /// bit to see if the parent page is still valid. If valid bit is true, the page contains
    /// a fence keys that can be used for comparison. Even if the page is valid, it may be possible that
    /// the page is no longer the parent page of the current leaf page due to the recycling of the
    /// removed page. Therefore, we still need to check the range of the parent page to see if the
    /// current leaf page is still in the range of the parent page.
    /// In addition, the traversal happens from leaf to root. We need to ensure that no deadlocks occur
    /// during the traversal. Deadlocks are not a problem in this case because the traversal only
    /// takes a read latch. If taking the read latch fails, there is a concurrent write operation
    /// that is modifying the page. If we detect this situation, we can wait for a while.
    #[cfg(not(feature = "old_iter"))]
    fn go_to_next_leaf(&mut self) {
        // Pop the current page from the stack
        self.parents.pop().unwrap();

        let key = self.current_high_fence.as_ref().unwrap();

        // Loop for retrying traversals from internal nodes
        loop {
            let page_frame_key = self.parents.last().unwrap();
            let mut current_page = self.btree.read_page(*page_frame_key);
            if !current_page.is_valid() || !current_page.inside_range(&BTreeKey::Normal(key)) {
                self.parents.pop().unwrap(); // The previous page was invalidated by concurrent operations. Go one level up.
                continue;
            } else {
                // Loop for traversing from the current page to the leaf page
                // Once we reach this loop, we never go back to the outer loop.
                loop {
                    let this_page = current_page;
                    log_info!("Traversal for read, page: {}", this_page.get_id());
                    if this_page.is_leaf() {
                        if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                            // Check whether the foster child should be traversed.
                            let val = InnerVal::from_bytes(this_page.get_foster_val());
                            let foster_page_key = PageFrameKey::new_with_frame_id(
                                self.btree.c_key,
                                val.page_id,
                                val.frame_id,
                            );
                            let foster_page = self.btree.read_page(foster_page_key);
                            current_page = foster_page;
                            self.parents.push(foster_page_key); // Push the visiting page to the stack
                            continue;
                        } else {
                            let leaf_page = unsafe {
                                std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(
                                    this_page,
                                )
                            };
                            self.current_slot_id = 1; // Skip the lower fence
                            self.current_high_fence = Some(
                                leaf_page
                                    .get_raw_key(leaf_page.high_fence_slot_id())
                                    .to_owned(),
                            );
                            self.current_leaf_page = Some(leaf_page);
                            return;
                        }
                    }
                    let slot_id = this_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                    let val = InnerVal::from_bytes(this_page.get_val(slot_id));
                    let page_key = PageFrameKey::new_with_frame_id(
                        self.btree.c_key,
                        val.page_id,
                        val.frame_id,
                    );

                    let next_page = self.btree.read_page(page_key);

                    // Do a prefetch for the next next page
                    let prefetch_slot_id = slot_id + 1;
                    if prefetch_slot_id < this_page.high_fence_slot_id() {
                        let prefetch_val =
                            InnerVal::from_bytes(this_page.get_val(prefetch_slot_id));
                        let prefetch_page_key = PageFrameKey::new_with_frame_id(
                            self.btree.c_key,
                            prefetch_val.page_id,
                            prefetch_val.frame_id,
                        );
                        let _ = self.btree.mem_pool.prefetch_page(prefetch_page_key);
                    }

                    current_page = next_page;
                    self.parents.push(page_key); // Push the visiting page to the stack
                }
            }
        }
    }
}

impl<T: MemPool> Iterator for FosterBtreeRangeScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.finished {
            return None;
        }
        if !self.initialized {
            self.initialize();
            self.initialized = true;
        }
        loop {
            debug_assert!(self.current_slot_id > 0);
            if self.current_leaf_page.is_none() {
                // If the current leaf page is None, we have to re-traverse to the leaf page using the previous high fence.
                if self.high_fence() >= self.r_key() {
                    self.finish();
                    return None;
                } else {
                    self.go_to_next_leaf();
                }
            }
            debug_assert!(self.current_leaf_page.is_some());
            let leaf_page = self.current_leaf_page.as_ref().unwrap();
            let key = leaf_page.get_raw_key(self.current_slot_id);
            if BTreeKey::new(key) >= self.r_key() {
                // Evict the page as soon as possible
                let current_leaf_page = self.current_leaf_page.take().unwrap();
                // self.btree
                //     .mem_pool
                //     .fast_evict(current_leaf_page.frame_id())
                //     .unwrap();
                drop(current_leaf_page);

                self.finish();
                return None;
            }
            if self.current_slot_id == leaf_page.high_fence_slot_id() {
                // Evict the page as soon as possible
                let current_leaf_page = self.current_leaf_page.take().unwrap();
                // self.btree
                //     .mem_pool
                //     .fast_evict(current_leaf_page.frame_id())
                //     .unwrap();
                drop(current_leaf_page);
            } else if leaf_page.has_foster_child()
                && self.current_slot_id == leaf_page.foster_child_slot_id()
            {
                // If the current slot is the foster child slot, move to the foster child.
                // Before releasing the current page, we need to get the read-latch of the foster child.
                let current_page = self.current_leaf_page.take().unwrap();
                let val = InnerVal::from_bytes(current_page.get_foster_val());
                let foster_page_key =
                    PageFrameKey::new_with_frame_id(self.btree.c_key, val.page_id, val.frame_id);
                let foster_page = self
                    .btree
                    .mem_pool
                    .get_page_for_read(foster_page_key)
                    .unwrap();
                let foster_page = unsafe {
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(foster_page)
                };
                // Evict the current page as soon as possible
                // self.btree
                //     .mem_pool
                //     .fast_evict(current_page.frame_id())
                //     .unwrap();
                drop(current_page);

                self.current_slot_id = 1;
                self.current_high_fence = Some(
                    foster_page
                        .get_raw_key(foster_page.high_fence_slot_id())
                        .to_owned(),
                );
                self.current_leaf_page = Some(foster_page);
            } else {
                let val = leaf_page.get_val(self.current_slot_id);
                if self.filter.is_none() || (self.filter.as_mut().unwrap())(key, val) {
                    self.current_slot_id += 1;
                    return Some((key.to_owned(), val.to_owned()));
                } else {
                    self.current_slot_id += 1;
                }
            }
        }
    }
}

/// Scan the BTree in the range [l_key, r_key)
/// To specify all keys, use an empty slice.
/// (l_key, r_key) = (&[], &[]) means [-inf, +inf)
pub struct FosterBtreeRangeScannerWithPageId<T: MemPool> {
    btree: Arc<FosterBtree<T>>, // Holds the reference to the btree

    // Scan parameters
    l_key: Vec<u8>,
    r_key: Vec<u8>,
    filter: Option<FilterFunc>,

    // States
    initialized: bool,
    finished: bool,
    current_high_fence: Option<Vec<u8>>,
    current_leaf_page: Option<FrameReadGuard<'static>>, // As long as btree is alive, bp is alive so the frame is alive
    current_slot_id: u32,
    parents: Vec<PageFrameKey>,
}

impl<T: MemPool> FosterBtreeRangeScannerWithPageId<T> {
    pub fn new(btree: &Arc<FosterBtree<T>>, l_key: &[u8], r_key: &[u8]) -> Self {
        Self {
            btree: btree.clone(),

            l_key: l_key.to_vec(),
            r_key: r_key.to_vec(),
            filter: None,

            initialized: false,
            finished: false,
            current_high_fence: None,
            current_leaf_page: None,
            current_slot_id: 0,
            parents: Vec::new(),
        }
    }

    fn new_with_filter(
        btree: &Arc<FosterBtree<T>>,
        l_key: &[u8],
        r_key: &[u8],
        filter: FilterFunc,
    ) -> Self {
        Self {
            btree: btree.clone(),

            l_key: l_key.to_vec(),
            r_key: r_key.to_vec(),
            filter: Some(filter),

            initialized: false,
            finished: false,
            current_high_fence: None,
            current_leaf_page: None,
            current_slot_id: 0,
            parents: Vec::new(),
        }
    }

    fn l_key(&self) -> BTreeKey {
        if self.l_key.is_empty() {
            BTreeKey::MinusInfty
        } else {
            BTreeKey::new(&self.l_key)
        }
    }

    fn r_key(&self) -> BTreeKey {
        if self.r_key.is_empty() {
            BTreeKey::PlusInfty
        } else {
            BTreeKey::new(&self.r_key)
        }
    }

    fn high_fence(&self) -> BTreeKey {
        if self.current_high_fence.as_ref().unwrap().is_empty() {
            BTreeKey::PlusInfty
        } else {
            BTreeKey::new(self.current_high_fence.as_ref().unwrap())
        }
    }

    fn finish(&mut self) {
        self.finished = true;
        self.current_leaf_page = None;
    }

    #[cfg(feature = "old_iter")]
    fn initialize(&mut self) {
        // Traverse to the leaf page that contains the l_key
        let leaf_page = self.btree.traverse_to_leaf_for_read(self.l_key.as_slice());
        let leaf_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(leaf_page) };
        let mut slot = leaf_page.lower_bound_slot_id(&self.l_key());
        if slot == 0 {
            slot = 1; // Skip the lower fence
        }
        self.current_slot_id = slot;
        self.current_high_fence = Some(
            leaf_page
                .get_raw_key(leaf_page.high_fence_slot_id())
                .to_owned(),
        );
        self.current_leaf_page = Some(leaf_page);
    }

    #[cfg(feature = "old_iter")]
    fn go_to_next_leaf(&mut self) {
        // Traverse to the leaf page that contains the current key (high fence of the previous leaf page)
        // Current key should NOT be equal to the r_key. Otherwise, we are done.
        let leaf_page = self
            .btree
            .traverse_to_leaf_for_read(self.current_high_fence.as_ref().unwrap());
        let leaf_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(leaf_page) };
        self.current_slot_id = 1; // Skip the lower fence
        self.current_high_fence = Some(
            leaf_page
                .get_raw_key(leaf_page.high_fence_slot_id())
                .to_owned(),
        );
        self.current_leaf_page = Some(leaf_page);
    }

    /// Traverse to the leaf page to find the page with l_key.
    /// Creates a parents stack to keep track of the traversal path.
    /// At the end of the function, the stack will contain the path from the root to the leaf page
    /// including the foster pages.
    #[cfg(not(feature = "old_iter"))]
    fn initialize(&mut self) {
        // Push the root page to the stack
        self.parents.push(self.btree.root_key);
        let page_frame_key = self.parents.last().unwrap();
        let mut current_page = self.btree.read_page(*page_frame_key);

        // Loop for traversing from the current page to the leaf page
        // Once we reach this loop, we never go back to the outer loop.
        loop {
            let this_page = current_page;
            if this_page.is_leaf() {
                if this_page.has_foster_child()
                    && this_page.get_foster_key() <= self.l_key.as_slice()
                {
                    // Check whether the foster child should be traversed.
                    let val = InnerVal::from_bytes(this_page.get_foster_val());
                    let foster_page_key = PageFrameKey::new_with_frame_id(
                        self.btree.c_key,
                        val.page_id,
                        val.frame_id,
                    );
                    let foster_page = self.btree.read_page(foster_page_key);
                    current_page = foster_page;
                    self.parents.push(foster_page_key); // Push the visiting page to the stack
                    continue;
                } else {
                    let leaf_page = unsafe {
                        std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(this_page)
                    };

                    // Initialize the slot to the lower bound of the l_key
                    let mut slot = leaf_page.lower_bound_slot_id(&self.l_key());
                    if slot == 0 {
                        slot = 1; // Skip the lower fence
                    }
                    self.current_slot_id = slot;
                    self.current_high_fence = Some(
                        leaf_page
                            .get_raw_key(leaf_page.high_fence_slot_id())
                            .to_owned(),
                    );
                    self.current_leaf_page = Some(leaf_page);
                    return;
                }
            }
            let slot_id = this_page.upper_bound_slot_id(&self.l_key()) - 1;
            let val = InnerVal::from_bytes(this_page.get_val(slot_id));
            let page_key =
                PageFrameKey::new_with_frame_id(self.btree.c_key, val.page_id, val.frame_id);

            let next_page = self.btree.read_page(page_key);

            // Do a prefetch for the next next page
            let prefetch_slot_id = slot_id + 1;
            if prefetch_slot_id < this_page.high_fence_slot_id() {
                let prefetch_val = InnerVal::from_bytes(this_page.get_val(prefetch_slot_id));
                let prefetch_page_key = PageFrameKey::new_with_frame_id(
                    self.btree.c_key,
                    prefetch_val.page_id,
                    prefetch_val.frame_id,
                );
                let _ = self.btree.mem_pool.prefetch_page(prefetch_page_key);
            }

            current_page = next_page;
            self.parents.push(page_key); // Push the visiting page to the stack
        }
    }

    /// Traverse to the leaf page.
    /// This is done by checking the parent page and check if the page is valid.
    /// The validity is checked by checking the valid bit as well as whether the next leaf page
    /// is in the range of this parent page.
    /// If concurrent operations removes the parent page by merging or load balancing, it will
    /// set the valid bit to false before releasing the latch. Therefore, we can check the valid
    /// bit to see if the parent page is still valid. If valid bit is true, the page contains
    /// a fence keys that can be used for comparison. Even if the page is valid, it may be possible that
    /// the page is no longer the parent page of the current leaf page due to the recycling of the
    /// removed page. Therefore, we still need to check the range of the parent page to see if the
    /// current leaf page is still in the range of the parent page.
    /// In addition, the traversal happens from leaf to root. We need to ensure that no deadlocks occur
    /// during the traversal. Deadlocks are not a problem in this case because the traversal only
    /// takes a read latch. If taking the read latch fails, there is a concurrent write operation
    /// that is modifying the page. If we detect this situation, we can wait for a while.
    #[cfg(not(feature = "old_iter"))]
    fn go_to_next_leaf(&mut self) {
        // Pop the current page from the stack
        self.parents.pop().unwrap();

        let key = self.current_high_fence.as_ref().unwrap();

        // Loop for retrying traversals from internal nodes
        loop {
            let page_frame_key = self.parents.last().unwrap();
            let mut current_page = self.btree.read_page(*page_frame_key);
            if !current_page.is_valid() || !current_page.inside_range(&BTreeKey::Normal(key)) {
                self.parents.pop().unwrap(); // The previous page was invalidated by concurrent operations. Go one level up.
                continue;
            } else {
                // Loop for traversing from the current page to the leaf page
                // Once we reach this loop, we never go back to the outer loop.
                loop {
                    let this_page = current_page;
                    log_info!("Traversal for read, page: {}", this_page.get_id());
                    if this_page.is_leaf() {
                        if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                            // Check whether the foster child should be traversed.
                            let val = InnerVal::from_bytes(this_page.get_foster_val());
                            let foster_page_key = PageFrameKey::new_with_frame_id(
                                self.btree.c_key,
                                val.page_id,
                                val.frame_id,
                            );
                            let foster_page = self.btree.read_page(foster_page_key);
                            current_page = foster_page;
                            self.parents.push(foster_page_key); // Push the visiting page to the stack
                            continue;
                        } else {
                            let leaf_page = unsafe {
                                std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(
                                    this_page,
                                )
                            };
                            self.current_slot_id = 1; // Skip the lower fence
                            self.current_high_fence = Some(
                                leaf_page
                                    .get_raw_key(leaf_page.high_fence_slot_id())
                                    .to_owned(),
                            );
                            self.current_leaf_page = Some(leaf_page);
                            return;
                        }
                    }
                    let slot_id = this_page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                    let val = InnerVal::from_bytes(this_page.get_val(slot_id));
                    let page_key = PageFrameKey::new_with_frame_id(
                        self.btree.c_key,
                        val.page_id,
                        val.frame_id,
                    );

                    let next_page = self.btree.read_page(page_key);

                    // Do a prefetch for the next next page
                    let prefetch_slot_id = slot_id + 1;
                    if prefetch_slot_id < this_page.high_fence_slot_id() {
                        let prefetch_val =
                            InnerVal::from_bytes(this_page.get_val(prefetch_slot_id));
                        let prefetch_page_key = PageFrameKey::new_with_frame_id(
                            self.btree.c_key,
                            prefetch_val.page_id,
                            prefetch_val.frame_id,
                        );
                        let _ = self.btree.mem_pool.prefetch_page(prefetch_page_key);
                    }

                    current_page = next_page;
                    self.parents.push(page_key); // Push the visiting page to the stack
                }
            }
        }
    }
}

impl<T: MemPool> Iterator for FosterBtreeRangeScannerWithPageId<T> {
    type Item = (Vec<u8>, Vec<u8>, (PageId, u32));

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>, (PageId, u32))> {
        if self.finished {
            return None;
        }
        if !self.initialized {
            self.initialize();
            self.initialized = true;
        }
        loop {
            debug_assert!(self.current_slot_id > 0);
            if self.current_leaf_page.is_none() {
                // If the current leaf page is None, we have to re-traverse to the leaf page using the previous high fence.
                if self.high_fence() >= self.r_key() {
                    self.finish();
                    return None;
                } else {
                    self.go_to_next_leaf();
                }
            }
            debug_assert!(self.current_leaf_page.is_some());
            let leaf_page = self.current_leaf_page.as_ref().unwrap();
            let key = leaf_page.get_raw_key(self.current_slot_id);
            if BTreeKey::new(key) >= self.r_key() {
                // Evict the page as soon as possible
                let current_leaf_page = self.current_leaf_page.take().unwrap();
                // self.btree
                //     .mem_pool
                //     .fast_evict(current_leaf_page.frame_id())
                //     .unwrap();
                drop(current_leaf_page);

                self.finish();
                return None;
            }
            if self.current_slot_id == leaf_page.high_fence_slot_id() {
                // Evict the page as soon as possible
                let current_leaf_page = self.current_leaf_page.take().unwrap();
                // self.btree
                //     .mem_pool
                //     .fast_evict(current_leaf_page.frame_id())
                //     .unwrap();
                drop(current_leaf_page);
            } else if leaf_page.has_foster_child()
                && self.current_slot_id == leaf_page.foster_child_slot_id()
            {
                // If the current slot is the foster child slot, move to the foster child.
                // Before releasing the current page, we need to get the read-latch of the foster child.
                let current_page = self.current_leaf_page.take().unwrap();
                let val = InnerVal::from_bytes(current_page.get_foster_val());
                let foster_page_key =
                    PageFrameKey::new_with_frame_id(self.btree.c_key, val.page_id, val.frame_id);
                let foster_page = self
                    .btree
                    .mem_pool
                    .get_page_for_read(foster_page_key)
                    .unwrap();
                let foster_page = unsafe {
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(foster_page)
                };
                // Evict the current page as soon as possible
                // self.btree
                //     .mem_pool
                //     .fast_evict(current_page.frame_id())
                //     .unwrap();
                drop(current_page);

                self.current_slot_id = 1;
                self.current_high_fence = Some(
                    foster_page
                        .get_raw_key(foster_page.high_fence_slot_id())
                        .to_owned(),
                );
                self.current_leaf_page = Some(foster_page);
            } else {
                let val = leaf_page.get_val(self.current_slot_id);
                let page_id = leaf_page.get_id();
                let frame_id = leaf_page.frame_id();
                if self.filter.is_none() || (self.filter.as_mut().unwrap())(key, val) {
                    self.current_slot_id += 1;
                    return Some((key.to_owned(), val.to_owned(), (page_id, frame_id)));
                } else {
                    self.current_slot_id += 1;
                }
            }
        }
    }
}

/// Tree index for non-unique keys.
///
/// The key is appended with a suffix to make it unique.
/// The suffix is currently a 32-bit integer.
/// This tree panics in the following cases
/// 1. When you try to append a key for which the suffix has reached the maximum value.
/// 2. When you try to append a key whose prefix key (not equal) is already in the tree.
///
/// We prevent the second case because the appended suffix can change the order of the two keys.
/// For example, say the user inserts two keys "a" and "a:3". Here, "a" is a prefix of "a:3".
/// Inserting "a" will internally create "a:0" and inserting "a:3" will create "a:3:0" because the suffix(count) is 0.
/// As the user appends more "a" keys, the internal keys should be "a:1", "a:2", "a:3", "a:4".
/// In the tree, however, the values of "a" and "a:3" will be interleaved because the internal keys are sorted by byte order.
/// The sort order: "a:0", "a:1", "a:2", "a:3", "a:3:0", "a:4".
/// To prevent this, we panic when the user tries to append a key whose prefix key is already in the tree.
/// This will not happen when the keys are of fixed length.
pub struct FosterBtreeAppendOnly<T: MemPool> {
    pub fbt: Arc<FosterBtree<T>>,
}

impl<T: MemPool> FosterBtreeAppendOnly<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        FosterBtreeAppendOnly {
            fbt: Arc::new(FosterBtree::new(c_key, mem_pool)),
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, root_page_id: PageId) -> Self {
        FosterBtreeAppendOnly {
            fbt: Arc::new(FosterBtree::load(c_key, mem_pool, root_page_id)),
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.fbt.num_kvs()
    }

    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        // Adds a suffix to the key and insert the key-value pair.
        let mut suffixed_key = key.to_vec();
        let count = u32::MAX;
        suffixed_key.extend_from_slice(&count.to_be_bytes());
        // Search for the key
        let mut leaf_page = self.fbt.traverse_to_leaf_for_write(&suffixed_key);
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&suffixed_key)) - 1;

        // If value_before is prefixed with key, use lower_fence + 1
        // If value_before is not prefixed with key, use key + "0000"
        let key_before = leaf_page.get_raw_key(slot_id);
        let new_suffix = if key_before.starts_with(key) {
            if key_before.len() != suffixed_key.len() {
                panic!("Key with the same prefix already exists");
            }
            let old_suffix = u32::from_be_bytes(key_before[key.len()..].try_into().unwrap());
            if old_suffix == u32::MAX {
                panic!("Key is already appended with the maximum count");
            }
            old_suffix + 1
        } else {
            0
        };
        // Modify the suffix of the key
        suffixed_key[key.len()..].copy_from_slice(&new_suffix.to_be_bytes());
        self.fbt.stats.inc_num_keys();
        self.fbt
            .insert_at_slot_or_split(&mut leaf_page, slot_id + 1, &suffixed_key, value);
        Ok(())
    }

    pub fn scan_key(self: &Arc<Self>, key: &[u8]) -> FosterBtreeAppendOnlyRangeScanner<T> {
        let mut first_key = key.to_vec();
        first_key.extend(0_u32.to_be_bytes());

        let mut last_key = key.to_vec();
        last_key.extend(u32::MAX.to_be_bytes());

        let key_len = first_key.len();

        FosterBtreeAppendOnlyRangeScanner::new(FosterBtreeRangeScanner::new_with_filter(
            &self.fbt,
            &first_key,
            &last_key,
            Arc::new(move |k, _| k.len() == key_len),
        ))
    }

    pub fn scan_range(
        self: &Arc<Self>,
        l_key: &[u8],
        r_key: &[u8],
    ) -> FosterBtreeAppendOnlyRangeScanner<T> {
        let mut first_key = l_key.to_vec();
        first_key.extend(0_u32.to_be_bytes());

        let mut last_key = r_key.to_vec();
        last_key.extend(u32::MAX.to_be_bytes());

        FosterBtreeAppendOnlyRangeScanner::new(FosterBtreeRangeScanner::new(
            &self.fbt, &first_key, &last_key,
        ))
    }

    pub fn scan(self: &Arc<Self>) -> FosterBtreeAppendOnlyRangeScanner<T> {
        FosterBtreeAppendOnlyRangeScanner::new(FosterBtreeRangeScanner::new(&self.fbt, &[], &[]))
    }
}

pub struct FosterBtreeAppendOnlyRangeScanner<T: MemPool> {
    scanner: FosterBtreeRangeScanner<T>,
}

impl<T: MemPool> FosterBtreeAppendOnlyRangeScanner<T> {
    fn new(scanner: FosterBtreeRangeScanner<T>) -> Self {
        Self { scanner }
    }
}

impl<T: MemPool> Iterator for FosterBtreeAppendOnlyRangeScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.scanner.next().map(|(mut key, value)| {
            key.truncate(key.len() - 4);
            (key, value)
        })
    }
}

/// Thread-unsafe page traversal coordinator.
/// This can used for debugging, visualization, and collecting statistics.
pub struct FosterBTreePageTraversal<T: MemPool> {
    // BTree parameters
    c_key: ContainerKey,
    root_key: PageFrameKey,
    mem_pool: Arc<T>,
}

impl<T: MemPool> FosterBTreePageTraversal<T> {
    pub fn new(tree: &FosterBtree<T>) -> Self {
        Self {
            c_key: tree.c_key,
            root_key: tree.root_key,
            mem_pool: tree.mem_pool.clone(),
        }
    }
}

impl<T: MemPool> FosterBTreePageTraversal<T> {
    // Return the children to visit in the order of the traversal
    fn get_children_page_ids(&self, page: &Page) -> Vec<InnerVal> {
        let mut children = Vec::new();
        if page.is_leaf() {
            if page.has_foster_child() {
                let val = InnerVal::from_bytes(page.get_foster_val());
                children.push(val);
            }
            children
        } else {
            for i in 1..=page.active_slot_count() {
                let val = InnerVal::from_bytes(page.get_val(i));
                children.push(val);
            }
            children
        }
    }

    pub fn visit<V>(&self, visitor: &mut V)
    where
        V: PageVisitor,
    {
        let mut stack = vec![(self.root_key, false)]; // (page_key, pre_visited)
        while let Some((next_key, pre_visited)) = stack.last_mut() {
            let page = self.mem_pool.get_page_for_read(*next_key).unwrap();
            if *pre_visited {
                visitor.visit_post(&page);
                stack.pop();
                continue;
            } else {
                *pre_visited = true;
                visitor.visit_pre(&page);
                let children = self.get_children_page_ids(&page);
                for child_key in children.into_iter().rev() {
                    stack.push((
                        PageFrameKey::new_with_frame_id(
                            self.c_key,
                            child_key.page_id,
                            child_key.frame_id,
                        ),
                        false,
                    ));
                }
            }
        }
    }
}

pub trait PageVisitor {
    fn visit_pre(&mut self, page: &Page);
    fn visit_post(&mut self, page: &Page);
}

pub struct ConsistencyChecker {}

impl PageVisitor for ConsistencyChecker {
    fn visit_pre(&mut self, page: &Page) {
        page.run_consistency_checks(false);
    }

    fn visit_post(&mut self, _page: &Page) {}
}

#[derive(Debug)]
struct PerPageStats {
    level: u8,
    has_foster: bool,
    slot_count: usize,
    active_slot_count: usize,
    total_bytes_used: usize,
    total_free_space: usize,
}

impl std::fmt::Display for PerPageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        result.push_str(&format!("Level: {}\n", self.level));
        result.push_str(&format!("Has foster: {}\n", self.has_foster));
        result.push_str(&format!("Slot count: {}\n", self.slot_count));
        result.push_str(&format!("Active slot count: {}\n", self.active_slot_count));
        result.push_str(&format!("Total bytes used: {}\n", self.total_bytes_used));
        result.push_str(&format!("Total free space: {}\n", self.total_free_space));
        write!(f, "{}", result)
    }
}

struct PerLevelPageStats {
    count: usize,
    has_foster_count: usize,
    min_fillfactor: f64,
    max_fillfactor: f64,
    sum_fillfactor: f64,
    page_stats: BTreeMap<PageId, PerPageStats>,
}

impl PerLevelPageStats {
    fn new() -> Self {
        Self {
            count: 0,
            has_foster_count: 0,
            min_fillfactor: 1.0,
            max_fillfactor: 0.0,
            sum_fillfactor: 0.0,
            page_stats: BTreeMap::new(),
        }
    }
}

struct PageStatsGenerator {
    stats: BTreeMap<u8, PerLevelPageStats>, // level -> stats
}

impl PageStatsGenerator {
    fn new() -> Self {
        Self {
            stats: BTreeMap::new(),
        }
    }

    fn update(&mut self, page_id: PageId, stats: PerPageStats) {
        let level = stats.level;
        let per_level_stats = self.stats.entry(level).or_insert(PerLevelPageStats::new());
        per_level_stats.count += 1;
        per_level_stats.has_foster_count += if stats.has_foster { 1 } else { 0 };
        let fillfactor = stats.total_bytes_used as f64 / AVAILABLE_PAGE_SIZE as f64;
        per_level_stats.min_fillfactor = per_level_stats.min_fillfactor.min(fillfactor);
        per_level_stats.max_fillfactor = per_level_stats.max_fillfactor.max(fillfactor);
        per_level_stats.sum_fillfactor += fillfactor;
        per_level_stats.page_stats.insert(page_id, stats);
    }

    fn to_string(&self, verbose: bool) -> String {
        let mut result = String::new();
        for (level, stats) in self.stats.iter().rev() {
            result.push_str(&format!(
                "----------------- Level {} -----------------\n",
                level
            ));
            result.push_str(&format!("Page Created: {}\n", stats.count));
            result.push_str(&format!("Has foster count: {}\n", stats.has_foster_count));
            result.push_str(&format!("Min fillfactor: {:.4}\n", stats.min_fillfactor));
            result.push_str(&format!("Max fillfactor: {:.4}\n", stats.max_fillfactor));
            result.push_str(&format!(
                "Avg fillfactor: {:.4}\n",
                stats.sum_fillfactor / stats.count as f64
            ));
        }
        // Print the page with high fillfactor
        if verbose {
            result.push_str("Pages with high fillfactor (> 0.99):\n");
            for stats in self.stats.values().rev() {
                for (page_id, per_page_stats) in stats.page_stats.iter() {
                    let fillfactor =
                        per_page_stats.total_bytes_used as f64 / AVAILABLE_PAGE_SIZE as f64;
                    if fillfactor > 0.99 {
                        result.push_str(&format!(
                            "Page {} has high fillfactor: {:.2}\n",
                            page_id, fillfactor
                        ));
                        result.push_str(&per_page_stats.to_string());
                    }
                }
            }

            // Print the page stats
            result.push_str("Individual page stats:\n");
            for stats in self.stats.values().rev() {
                for (page_id, per_page_stats) in stats.page_stats.iter() {
                    result.push_str(&format!(
                        "----------------- Page {} -----------------\n",
                        page_id
                    ));
                    result.push_str(&per_page_stats.to_string());
                }
            }
        }
        result
    }
}

impl PageVisitor for PageStatsGenerator {
    fn visit_pre(&mut self, page: &Page) {
        let stats = PerPageStats {
            level: page.level(),
            has_foster: page.has_foster_child(),
            slot_count: page.slot_count() as usize,
            active_slot_count: page.active_slot_count() as usize,
            total_bytes_used: page.total_bytes_used() as usize,
            total_free_space: page.total_free_space() as usize,
        };
        self.update(page.get_id(), stats);
    }

    fn visit_post(&mut self, _page: &Page) {}
}

#[cfg(test)]
mod tests {
    use core::num;
    use std::collections::HashSet;
    use std::{fs::File, sync::Arc, thread};

    use crate::access_method::fbt::foster_btree::{deserialize_page_id, InnerVal};
    #[allow(unused_imports)]
    use crate::log;
    use crate::log_info;

    use crate::page::AVAILABLE_PAGE_SIZE;
    use crate::random::gen_random_permutation;
    use crate::{
        access_method::fbt::foster_btree::{
            adopt, anti_adopt, ascend_root, balance, descend_root, is_large, is_small, merge,
            should_adopt, should_antiadopt, should_load_balance, should_merge, should_root_ascend,
            should_root_descend, AccessMethodError, OrderedUniqueKeyIndex, UniqueKeyIndex,
            MIN_BYTES_USED,
        },
        bp::{get_in_mem_pool, get_test_bp},
        random::RandomKVs,
    };

    use super::{
        ContainerKey, FosterBtree, FosterBtreeAppendOnly, FosterBtreePage, MemPool, PageFrameKey,
        MAX_BYTES_USED,
    };

    fn to_bytes(num: usize) -> Vec<u8> {
        num.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> usize {
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    use rstest::rstest;

    #[rstest]
    #[case::bp(get_test_bp(1))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_page_setup<T: MemPool>(#[case] mp: Arc<T>) {
        let c_key = ContainerKey::new(0, 0);
        let mut p = mp.create_new_page_for_write(c_key).unwrap();
        p.init();
        let low_fence = to_bytes(0);
        let high_fence = to_bytes(20);
        p.set_low_fence(&low_fence);
        p.set_high_fence(&high_fence);
        assert_eq!(p.get_low_fence().as_ref(), low_fence);
        assert_eq!(p.get_high_fence().as_ref(), high_fence);
    }

    fn test_page_merge_detail<T: MemPool>(
        bp: Arc<T>,
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if !left.is_empty() {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if !right.is_empty() {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut p0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut p1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        p0.init();
        p0.set_level(0);
        p0.set_low_fence(&k0);
        p0.set_high_fence(&k2);
        p0.set_has_foster_child(true);
        p0.append_sorted(
            &left
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );
        let foster_val = InnerVal::new_with_frame_id(p1.get_id(), p1.frame_id());
        p0.insert(&k1, &foster_val.to_bytes());

        p1.init();
        p1.set_level(0);
        p1.set_low_fence(&k1);
        p1.set_high_fence(&k2);
        p1.append_sorted(
            &right
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );

        // Run consistency checks
        p0.run_consistency_checks(true);
        p1.run_consistency_checks(true);

        // Merge p1 into p0
        merge(&mut p0, &mut p1);

        // Run consistency checks
        p0.run_consistency_checks(false);
        p1.run_consistency_checks(false);
        // Check the contents of p0
        assert_eq!(p0.active_slot_count() as usize, left.len() + right.len());
        for i in 0..left.len() {
            let key = p0.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(left[i]));
            let val = p0.get_val((i + 1) as u32);
            assert_eq!(val, to_bytes(left[i]));
        }
        for i in 0..right.len() {
            let key = p0.get_raw_key((i + 1 + left.len()) as u32);
            assert_eq!(key, to_bytes(right[i]));
            let val = p0.get_val((i + 1 + left.len()) as u32);
            assert_eq!(val, to_bytes(right[i]));
        }
        assert_eq!(p0.get_low_fence().as_ref(), k0);
        assert_eq!(p0.get_high_fence().as_ref(), k2);
        assert!(!p0.has_foster_child());

        assert!(p1.empty());
    }

    #[rstest]
    #[case::bp(get_test_bp(2))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_page_merge<T: MemPool>(#[case] bp: Arc<T>) {
        test_page_merge_detail(bp.clone(), 10, 20, 30, vec![], vec![]);
        test_page_merge_detail(bp.clone(), 10, 20, 30, vec![10, 15], vec![20, 25, 29]);
        test_page_merge_detail(bp.clone(), 10, 20, 30, vec![], vec![20, 25]);
        test_page_merge_detail(bp, 10, 20, 30, vec![10, 15], vec![]);
    }

    fn test_page_balance_detail<T: MemPool>(
        bp: Arc<T>,
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if !left.is_empty() {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if !right.is_empty() {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut p0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut p1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        p0.init();
        p0.set_level(0);
        p0.set_low_fence(&k0);
        p0.set_high_fence(&k2);
        p0.set_has_foster_child(true);
        p0.append_sorted(
            &left
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );
        let foster_val = InnerVal::new_with_frame_id(p1.get_id(), p1.frame_id());
        p0.insert(&k1, &foster_val.to_bytes());

        p1.init();
        p1.set_level(0);
        p1.set_low_fence(&k1);
        p1.set_high_fence(&k2);
        p1.append_sorted(
            &right
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );

        // Run consistency checks
        p0.run_consistency_checks(true);
        p1.run_consistency_checks(true);

        let prev_diff = p0.total_bytes_used().abs_diff(p1.total_bytes_used());

        // Balance p0 and p1
        balance(&mut p0, &mut p1);

        // Run consistency checks
        p0.run_consistency_checks(false);
        p1.run_consistency_checks(false);

        // If balanced, half of the total keys should be in p0 and the other half should be in p1
        let total_keys = left.len() + right.len();
        let total_keys_in_pages = p0.active_slot_count() + p1.active_slot_count();
        assert_eq!(total_keys + 1, total_keys_in_pages as usize); // +1 because of the foster key

        let all = left.iter().chain(right.iter()).collect::<Vec<_>>();
        for i in 0..p0.active_slot_count() as usize - 1 {
            let key = p0.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(*all[i]));
        }
        for i in 0..p1.active_slot_count() as usize {
            let key = p1.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(*all[i + p0.active_slot_count() as usize - 1]));
        }
        assert_eq!(p0.get_low_fence().as_ref(), k0);
        assert_eq!(p0.get_high_fence().as_ref(), k2);
        assert!(p0.has_foster_child());
        assert_eq!(
            deserialize_page_id(p0.get_foster_val()).unwrap(),
            p1.get_id()
        );
        assert_eq!(p0.get_foster_key(), p1.get_raw_key(0));
        assert_eq!(p1.get_high_fence().as_ref(), k2);

        let new_diff = p0.total_bytes_used().abs_diff(p1.total_bytes_used());
        assert!(new_diff <= prev_diff);
    }

    #[rstest]
    #[case::bp(get_test_bp(2))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_page_balance<T: MemPool>(#[case] bp: Arc<T>) {
        test_page_balance_detail(bp.clone(), 10, 20, 30, vec![], vec![]);
        test_page_balance_detail(bp.clone(), 10, 20, 30, vec![10, 15], vec![]);
        test_page_balance_detail(bp.clone(), 10, 20, 30, vec![], vec![20, 25, 29]);
        test_page_balance_detail(
            bp.clone(),
            10,
            20,
            30,
            vec![10, 11, 12, 13, 14, 15],
            vec![20, 21],
        );
    }

    fn test_page_adopt_detail<T: MemPool>(
        bp: Arc<T>,
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if !left.is_empty() {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if !right.is_empty() {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        let val = InnerVal::new_with_frame_id(child0.get_id(), child0.frame_id());
        parent.insert(&k0, &val.to_bytes());

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_level(0);
        child0.append_sorted(
            &left
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );
        child0.set_has_foster_child(true);
        let foster_val = InnerVal::new_with_frame_id(child1.get_id(), child1.frame_id());
        child0.insert(&k1, &foster_val.to_bytes());

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);
        child1.append_sorted(
            &right
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);
        assert_eq!(parent.active_slot_count(), 1);
        assert_eq!(child0.active_slot_count() as usize, left.len() + 1);
        assert_eq!(child1.active_slot_count() as usize, right.len());
        assert_eq!(
            deserialize_page_id(parent.get_val(1)),
            Some(child0.get_id())
        );
        assert!(child0.has_foster_child());
        assert_eq!(child0.get_foster_key(), child1.get_raw_key(0));
        assert_eq!(
            deserialize_page_id(child0.get_foster_val()).unwrap(),
            child1.get_id()
        );

        // Adopt
        adopt(&mut parent, &mut child0);

        // Run consistency checks
        parent.run_consistency_checks(false);
        child0.run_consistency_checks(false);
        child1.run_consistency_checks(false);

        // Check the contents of parent
        assert_eq!(parent.active_slot_count(), 2);
        assert_eq!(
            deserialize_page_id(parent.get_val(1)),
            Some(child0.get_id())
        );
        assert_eq!(
            deserialize_page_id(parent.get_val(2)),
            Some(child1.get_id())
        );
        assert_eq!(child0.active_slot_count() as usize, left.len());
        assert!(!child0.has_foster_child());
        for i in 0..left.len() {
            let key = child0.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(left[i]));
            let val = child0.get_val((i + 1) as u32);
            assert_eq!(val, to_bytes(left[i]));
        }
        assert_eq!(child1.active_slot_count() as usize, right.len());
        for i in 0..right.len() {
            let key = child1.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(right[i]));
            let val = child1.get_val((i + 1) as u32);
            assert_eq!(val, to_bytes(right[i]));
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_page_adopt<T: MemPool>(#[case] bp: Arc<T>) {
        test_page_adopt_detail(bp.clone(), 10, 20, 30, vec![], vec![]);
        test_page_adopt_detail(bp.clone(), 10, 20, 30, vec![10, 15], vec![20, 25, 29]);
        test_page_adopt_detail(bp.clone(), 10, 20, 30, vec![], vec![20, 25]);
        test_page_adopt_detail(bp.clone(), 10, 20, 30, vec![10, 15], vec![]);
    }

    fn test_page_anti_adopt_detail<T: MemPool>(
        bp: Arc<T>,
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if !left.is_empty() {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if !right.is_empty() {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        let val = InnerVal::new_with_frame_id(child0.get_id(), child0.frame_id());
        parent.insert(&k0, &val.to_bytes());
        let val = InnerVal::new_with_frame_id(child1.get_id(), child1.frame_id());
        parent.insert(&k1, &val.to_bytes());

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k1);
        child0.set_level(0);
        child0.append_sorted(
            &left
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);
        child1.append_sorted(
            &right
                .iter()
                .map(|&x| (to_bytes(x), to_bytes(x)))
                .collect::<Vec<_>>(),
        );

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);
        assert_eq!(parent.active_slot_count(), 2);
        assert_eq!(child0.active_slot_count() as usize, left.len());
        assert_eq!(child1.active_slot_count() as usize, right.len());

        // Anti-adopt
        anti_adopt(&mut parent, &mut child0);

        // Run consistency checks
        parent.run_consistency_checks(false);
        child0.run_consistency_checks(false);
        child1.run_consistency_checks(false);

        // Check the contents of parent
        assert_eq!(parent.active_slot_count(), 1);
        assert_eq!(
            deserialize_page_id(parent.get_val(1)),
            Some(child0.get_id())
        );

        assert_eq!(child0.active_slot_count() as usize, left.len() + 1);
        assert_eq!(child0.get_low_fence().as_ref(), k0);
        assert_eq!(child0.get_high_fence().as_ref(), k2);
        assert!(child0.has_foster_child());
        assert_eq!(child0.get_foster_key(), child1.get_raw_key(0));
        assert_eq!(
            deserialize_page_id(child0.get_foster_val()).unwrap(),
            child1.get_id()
        );
        for i in 0..left.len() {
            let key = child0.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(left[i]));
            let val = child0.get_val((i + 1) as u32);
            assert_eq!(val, to_bytes(left[i]));
        }

        assert_eq!(child1.active_slot_count() as usize, right.len());
        assert_eq!(child1.get_low_fence().as_ref(), k1);
        assert_eq!(child1.get_high_fence().as_ref(), k2);
        for i in 0..right.len() {
            let key = child1.get_raw_key((i + 1) as u32);
            assert_eq!(key, to_bytes(right[i]));
            let val = child1.get_val((i + 1) as u32);
            assert_eq!(val, to_bytes(right[i]));
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_page_anti_adopt<T: MemPool>(#[case] bp: Arc<T>) {
        test_page_anti_adopt_detail(bp.clone(), 10, 20, 30, vec![], vec![]);
        test_page_anti_adopt_detail(bp.clone(), 10, 20, 30, vec![10, 15], vec![20, 25, 29]);
        test_page_anti_adopt_detail(bp.clone(), 10, 20, 30, vec![], vec![20, 25]);
        test_page_anti_adopt_detail(bp.clone(), 10, 20, 30, vec![10, 15], vec![]);
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_root_page_ascend<T: MemPool>(#[case] bp: Arc<T>) {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut root = bp.create_new_page_for_write(c_key).unwrap();
        let mut foster_child = bp.create_new_page_for_write(c_key).unwrap();
        let mut child = bp.create_new_page_for_write(c_key).unwrap();

        // Before:
        // root [-inf, +inf) --> foster_child [k0, +inf)
        //
        // After:
        // root [-inf, +inf)
        //  +-------------------+
        //  |                   |
        //  v                   v
        // child [-inf, k0)    foster_child [k0, +inf)

        root.init_as_root();
        // Insert 10 slots into the root. Add foster child at the end.
        for i in 1..=10 {
            let key = to_bytes(i);
            root.insert(&key, &key);
        }
        let foster_key = to_bytes(11);
        let val = InnerVal::new_with_frame_id(foster_child.get_id(), foster_child.frame_id());
        root.insert(&foster_key, &val.to_bytes());
        root.set_has_foster_child(true);

        foster_child.init();
        foster_child.set_low_fence(&foster_key);
        foster_child.set_high_fence(&[]);

        // Insert 10 slots into the foster child
        for i in 11..=20 {
            let key = to_bytes(i);
            foster_child.insert(&key, &key);
        }

        child.init();

        // Run consistency checks
        root.run_consistency_checks(true);
        foster_child.run_consistency_checks(true);
        child.run_consistency_checks(true);

        ascend_root(&mut root, &mut child);

        // Run consistency checks
        root.run_consistency_checks(false);
        foster_child.run_consistency_checks(true);
        child.run_consistency_checks(true);
        assert_eq!(root.active_slot_count(), 2);
        assert_eq!(root.level(), 1);
        assert!(!root.has_foster_child());
        assert!(root.get_raw_key(1).is_empty());
        assert_eq!(deserialize_page_id(root.get_val(1)), Some(child.get_id()));
        assert_eq!(root.get_raw_key(2), &foster_key);
        assert_eq!(
            deserialize_page_id(root.get_val(2)),
            Some(foster_child.get_id())
        );
        assert_eq!(child.active_slot_count(), 10);
        assert_eq!(child.level(), 0);
        assert!(!child.has_foster_child());
        assert!(child.is_left_most());
        for i in 1..=child.active_slot_count() {
            let key = child.get_raw_key(i);
            let val = child.get_val(i);
            assert_eq!(key, to_bytes(i as usize));
            assert_eq!(val, to_bytes(i as usize));
        }
        assert_eq!(foster_child.active_slot_count(), 10);
        assert_eq!(foster_child.level(), 0);
        assert!(!foster_child.has_foster_child());
        for i in 1..=foster_child.active_slot_count() {
            let key = foster_child.get_raw_key(i);
            let val = foster_child.get_val(i);
            assert_eq!(key, to_bytes(i as usize + 10));
            assert_eq!(val, to_bytes(i as usize + 10));
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_root_page_descend<T: MemPool>(#[case] bp: Arc<T>) {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut root = bp.create_new_page_for_write(c_key).unwrap();
        let mut child = bp.create_new_page_for_write(c_key).unwrap();

        // Before:
        //   root [k0, k1)
        //    |
        //    v
        //   child [k0, k1)
        //
        //
        // After:
        //   root [k0, k1)

        root.init_as_root();
        root.increment_level();
        let val = InnerVal::new_with_frame_id(child.get_id(), child.frame_id());
        root.insert(&[], &val.to_bytes());

        child.init();
        child.set_low_fence(&[]);
        child.set_high_fence(&[]);
        // Insert 10 slots
        for i in 1..=10 {
            let key = to_bytes(i);
            child.insert(&key, &key);
        }
        child.set_level(0);

        // Run consistency checks
        root.run_consistency_checks(false);
        child.run_consistency_checks(false);

        descend_root(&mut root, &mut child);

        // Run consistency checks
        root.run_consistency_checks(false);
        child.run_consistency_checks(false);
        assert_eq!(root.active_slot_count(), 10);
        assert_eq!(root.level(), 0);
        assert!(!root.has_foster_child());
        assert!(child.empty());
        assert_eq!(child.active_slot_count(), 0);
        for i in 1..=root.active_slot_count() {
            let key = root.get_raw_key(i);
            let val = root.get_val(i);
            assert_eq!(key, to_bytes(i as usize));
            assert_eq!(val, to_bytes(i as usize));
        }
    }

    // Foster relationship between two pages.
    // Returns (this_page_key, foster_page_key)
    // If is_root is true, then this_page is a root page.
    fn build_foster_relationship<T: MemPool>(
        bp: Arc<T>,
        is_root: bool,
        this_size: usize,
        foster_size: usize,
    ) -> (PageFrameKey, PageFrameKey) {
        let this_size = this_size as u32;
        let foster_size = foster_size as u32;
        // Create a foster relationship between two pages.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut this = bp.create_new_page_for_write(c_key).unwrap();
        let mut foster = bp.create_new_page_for_write(c_key).unwrap();

        // This [k0, k2) --> Foster [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        if is_root {
            this.init_as_root();
        } else {
            this.init();
        }
        this.set_low_fence(&k0);
        this.set_high_fence(&k2);
        this.set_has_foster_child(true);
        let val = InnerVal::new_with_frame_id(foster.get_id(), foster.frame_id());
        this.insert(&k1, &val.to_bytes());
        {
            // Insert a slot into this page so that the total size of the page is this_size
            let current_size = this.total_bytes_used();
            let val_size = this_size - current_size - this.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            this.insert(&k0, &val);
        }
        assert_eq!(this.total_bytes_used(), this_size);

        foster.init();
        foster.set_low_fence(&k1);
        foster.set_high_fence(&k2);
        foster.set_right_most(this.is_right_most());
        foster.set_has_foster_child(false);
        {
            // Insert a slot into foster page so that the total size of the page is foster_size
            let current_size = foster.total_bytes_used();
            let val_size = foster_size - current_size - this.bytes_needed(&k1, &[]);
            let val = vec![2_u8; val_size as usize];
            foster.insert(&k1, &val);
        }

        // Run consistency checks
        this.run_consistency_checks(true);
        foster.run_consistency_checks(true);

        let this_key = this.page_frame_key().unwrap();
        let foster_key = foster.page_frame_key().unwrap();

        (this_key, foster_key)
    }

    // In a foster relationship, MERGE, BALANCE, ROOT_ASCEND is allowed.
    // [0, MIN_BYTES_USED) is small
    // [MIN_BYTES_USED, MAX_BYTES_USED) is normal.
    // [MAX_BYTES_USED, PAGE_SIZE) is large
    // There are 3 * 3 = 9 possible size
    // S: Small, N: Normal, L: Large
    // SS, NS, SN: Merge
    // SL, LS: Load balance
    // NN, LN, NL, LL: Do nothing
    // Root ascend is allowed only when the foster page is the root page.
    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_foster_relationship_structure_modification_criteria<T: MemPool>(#[case] bp: Arc<T>) {
        {
            // Should merge, root_ascend
            // Should not balance
            {
                let (this_id, foster_id) =
                    build_foster_relationship(bp.clone(), true, MIN_BYTES_USED - 1, MIN_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_small(&this));
                assert!(!is_large(&foster));
                assert!(should_merge(&this, &foster));
                assert!(should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
            {
                let (this_id, foster_id) =
                    build_foster_relationship(bp.clone(), true, MIN_BYTES_USED, MIN_BYTES_USED - 1);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(!is_large(&this));
                assert!(is_small(&foster));
                assert!(should_merge(&this, &foster));
                assert!(should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
            {
                let (this_id, foster_id) = build_foster_relationship(
                    bp.clone(),
                    true,
                    MIN_BYTES_USED - 1,
                    MIN_BYTES_USED - 1,
                );
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_small(&this));
                assert!(is_small(&foster));
                assert!(should_merge(&this, &foster));
                assert!(should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
        }
        {
            // Should load balance
            // Should not merge, root_ascend
            {
                let (this_id, foster_id) = build_foster_relationship(
                    bp.clone(),
                    false,
                    MIN_BYTES_USED - 1,
                    MAX_BYTES_USED,
                );
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_small(&this));
                assert!(is_large(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(should_load_balance(&this, &foster));
            }
            {
                let (this_id, foster_id) = build_foster_relationship(
                    bp.clone(),
                    false,
                    MAX_BYTES_USED,
                    MIN_BYTES_USED - 1,
                );
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_large(&this));
                assert!(is_small(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(should_load_balance(&this, &foster));
            }
        }
        {
            // Should not merge, balance, root_ascend
            {
                let (this_id, foster_id) =
                    build_foster_relationship(bp.clone(), false, MAX_BYTES_USED, MAX_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_large(&this));
                assert!(is_large(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
            {
                let (this_id, foster_id) =
                    build_foster_relationship(bp.clone(), false, MIN_BYTES_USED, MIN_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(!is_small(&this));
                assert!(!is_small(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
        }
    }

    fn build_two_children_tree<T: MemPool>(
        bp: Arc<T>,
        child0_size: usize,
    ) -> (PageFrameKey, PageFrameKey) {
        let child0_size = child0_size as u32;

        // Create a parent with two children.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        // Parent [k0, k2)
        //  +-------------------+
        //  |                   |
        //  v                   v
        // Child0 [k0, k1)     Child1 [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        let val = InnerVal::new_with_frame_id(child0.get_id(), child0.frame_id());
        parent.insert(&k0, &val.to_bytes());
        let val = InnerVal::new_with_frame_id(child1.get_id(), child1.frame_id());
        parent.insert(&k1, &val.to_bytes());

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_level(0);
        {
            // Insert a slot into child0 page so that the total size of the page is child0_size
            let current_size = child0.total_bytes_used();
            let val_size = child0_size - current_size - child0.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            child0.insert(&k0, &val);
        }

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);

        let parent_key = parent.page_frame_key().unwrap();
        let child0_key = child0.page_frame_key().unwrap();

        (parent_key, child0_key)
    }

    fn build_single_child_with_foster_child_tree<T: MemPool>(
        bp: Arc<T>,
        child0_size: usize,
    ) -> (PageFrameKey, PageFrameKey) {
        let child0_size = child0_size as u32;

        // Create a parent with a child and a foster child.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        // Parent [k0, k2)
        //  |
        //  v
        // Child0 [k0, k2) --> Child1 [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        let val = InnerVal::new_with_frame_id(child0.get_id(), child0.frame_id());
        parent.insert(&k0, &val.to_bytes());

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_level(0);
        child0.set_has_foster_child(true);
        let val = InnerVal::new_with_frame_id(child1.get_id(), child1.frame_id());
        child0.insert(&k1, &val.to_bytes());
        {
            // Insert a slot into child0 page so that the total size of the page is child0_size
            let current_size = child0.total_bytes_used();
            let val_size = child0_size - current_size - child0.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            child0.insert(&k0, &val);
        }

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);

        let parent_key = parent.page_frame_key().unwrap();
        let child0_key = child0.page_frame_key().unwrap();

        (parent_key, child0_key)
    }

    fn build_single_child_tree<T: MemPool>(
        bp: Arc<T>,
        child0_size: usize,
    ) -> (PageFrameKey, PageFrameKey) {
        let child0_size = child0_size as u32;

        // Create a parent with a child.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();

        // Parent [k0, k2)
        //  |
        //  v
        // Child0 [k0, k2)

        let k0 = to_bytes(10);
        // let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init_as_root();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        let val = InnerVal::new_with_frame_id(child0.get_id(), child0.frame_id());
        parent.insert(&k0, &val.to_bytes());

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_left_most(true);
        child0.set_right_most(true);
        child0.set_level(0);
        {
            // Insert a slot into child0 page so that the total size of the page is child0_size
            let current_size = child0.total_bytes_used();
            let val_size = child0_size - current_size - child0.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            child0.insert(&k0, &val);
        }

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);

        let parent_key = parent.page_frame_key().unwrap();
        let child0_key = child0.page_frame_key().unwrap();

        (parent_key, child0_key)
    }

    // In a parent-child relationship, ADOPT, ANTI_ADOPT, ROOT_DESCEND is allowed.
    // [0, MIN_BYTES_USED) is small
    // [MIN_BYTES_USED, MAX_BYTES_USED) is normal.
    // [MAX_BYTES_USED, PAGE_SIZE) is large
    // There are 3 * 3 * 3 = 9 possible size
    // S: Small, N: Normal, L: Large
    // SS, NS, LS: AntiAdopt if parent has more than 1 child.
    // SN, SL, NN, NL: Adopt if child has foster child.
    // LN, LL: Nothing
    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_parent_child_relationship_structure_modification_criteria<T: MemPool>(
        #[case] bp: Arc<T>,
    ) {
        {
            // Parent [k0, k2)
            //  +-------------------+
            //  |                   |
            //  v                   v
            // Child0 [k0, k1)     Child1 [k1, k2)
            {
                // Should anti_adopt
                // Should not adopt, root descend
                let (parent_id, child0_id) =
                    build_two_children_tree(bp.clone(), MIN_BYTES_USED - 1);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(is_small(&child0));
                assert!(should_antiadopt(&parent, &child0));
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
            {
                // Should not adopt, anti_adopt, root_descend
                let (parent_id, child0_id) = build_two_children_tree(bp.clone(), MIN_BYTES_USED);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(!is_small(&child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
        }
        {
            // Parent [k0, k2)
            //  |
            //  v
            // Child0 [k0, k2) --> Child1 [k1, k2)
            {
                // Should adopt
                // Should not anti_adopt, root_descend
                let (parent_id, child0_id) =
                    build_single_child_with_foster_child_tree(bp.clone(), MIN_BYTES_USED);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(!is_small(&child0));
                assert!(should_adopt(&parent, &child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
            {
                // Should not adopt, anti_adopt, root_descend
                let (parent_id, child0_id) =
                    build_single_child_with_foster_child_tree(bp.clone(), MIN_BYTES_USED - 1);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(is_small(&child0));
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
        }
        {
            // Parent [k0, k2)
            //  |
            //  v
            // Child0 [k0, k2)
            {
                // Should not adopt, anti_adopt, root_descend
                let (parent_id, child0_id) =
                    build_single_child_tree(bp.clone(), MIN_BYTES_USED - 1);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(should_root_descend(&parent, &child0));
            }
        }
    }

    fn setup_btree_empty<T: MemPool>(bp: Arc<T>) -> FosterBtree<T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);

        FosterBtree::new(c_key, bp.clone())
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_sorted_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![2_u8; 1024];
        for i in 0..10 {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(i);
            btree.insert(&key, &val).unwrap();
        }
        for i in 0..10 {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_updates<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        let new_val = vec![4_u8; 128];
        for i in order.iter() {
            println!(
                "**************************** Updating key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.update(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
        let new_val = vec![5_u8; 512];
        for i in order.iter() {
            println!(
                "**************************** Updating key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.update(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_deletion<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Deleting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.delete(&key).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key);
            assert_eq!(current_val, Err(AccessMethodError::KeyNotFound))
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_upserts<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
        let new_val = vec![4_u8; 128];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
        let new_val = vec![5_u8; 512];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_upsert_with_merge<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        // Insert 1024 bytes
        let key = to_bytes(0);
        let vals = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in vals.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let val = to_bytes(*i);
            btree
                .upsert_with_merge(&key, &val, |old_val: &[u8], new_val: &[u8]| -> Vec<u8> {
                    // Deserialize old_val and new_val and add them.
                    let old_val = from_bytes(old_val);
                    let new_val = from_bytes(new_val);
                    to_bytes(old_val + new_val)
                })
                .unwrap();
        }
        let expected_val = vals.iter().sum::<usize>();
        let current_val = btree.get(&key).unwrap();
        assert_eq!(from_bytes(&current_val), expected_val);
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_scan<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_empty(bp.clone()));
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &val).unwrap();
        }
        let start_key = to_bytes(2);
        let end_key = to_bytes(7);
        let iter = btree.scan_range(&start_key, &end_key);
        let mut count = 0;
        for (key, current_val) in iter {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(current_val, val);
            count += 1;
        }
        assert_eq!(count, 5);

        let start_key = to_bytes(0);
        let end_key = to_bytes(10);
        let iter = btree.scan_range(&start_key, &end_key);
        let mut count = 0;
        for (key, current_val) in iter {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(current_val, val);
            count += 1;
        }
        assert_eq!(count, 10);

        let start_key = [];
        let end_key = [];
        let iter = btree.scan_range(&start_key, &end_key);
        let mut count = 0;
        for (key, current_val) in iter {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(current_val, val);
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[rstest]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn test_scan_heavy<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_empty(bp.clone()));
        let val = vec![1; 1024];
        // Insert num_keys values in random order
        let num_keys = 200000;
        let order = gen_random_permutation((0..num_keys).into_iter().collect::<Vec<_>>());
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &val).unwrap();
        }
        let iter = btree.scan();
        let mut count = 0;
        for (idx, (key, _)) in iter.enumerate() {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(key, idx);
            count += 1;
        }
        assert_eq!(count, num_keys);

        // Scan with range
        let start_key = to_bytes(1000);
        let end_key = to_bytes(2000);
        let iter = btree.scan_range(&start_key, &end_key);
        let mut count = 0;
        for (idx, (key, _)) in iter.enumerate() {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(key, idx + 1000);
            count += 1;
        }
        assert_eq!(count, 1000);
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_scan_with_filter<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_empty(bp.clone()));
        // Insert 512 and 1024 bytes. Odd keys have 512 bytes and even keys have 1024 bytes.
        let val1 = vec![3_u8; 512];
        let val2 = vec![4_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            if i % 2 == 0 {
                btree.upsert(&key, &val2).unwrap();
            } else {
                btree.upsert(&key, &val1).unwrap();
            }
        }
        let filter1 = Arc::new(|_: &[u8], value: &[u8]| -> bool {
            // Filter by value.
            value.len() == 512
        });
        let iter = btree.scan_with_filter(filter1);
        let mut count = 0;
        for (key, current_val) in iter {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(current_val, val1);
            count += 1;
        }
        assert_eq!(count, 5);
        let filter2 = Arc::new(|key: &[u8], _: &[u8]| -> bool {
            // Filter by key
            from_bytes(key) % 2 == 0
        });
        let iter = btree.scan_with_filter(filter2);
        let mut count = 0;
        for (key, current_val) in iter {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(current_val, val2);
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_insertion_stress<T: MemPool>(#[case] bp: Arc<T>) {
        let num_keys = 10000;
        let key_size = 8;
        let val_min_size = 50;
        let val_max_size = 100;
        let mut kvs = RandomKVs::new(
            true,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );
        let kvs = kvs.pop().unwrap();

        let btree = Arc::new(setup_btree_empty(bp.clone()));

        // Write kvs to file
        // let kvs_file = "kvs.dat";
        // // serde cbor to write to file
        // let mut file = File::create(kvs_file).unwrap();
        // let kvs_str = serde_cbor::to_vec(&kvs).unwrap();
        // file.write_all(&kvs_str).unwrap();

        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Inserting {} key={:?} **************************",
                i, key
            );
            btree.insert(key, val).unwrap();
        }

        let iter = btree.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {:?} **************************",
                key
            );
            let val = kvs.get(&key).unwrap();
            assert_eq!(&current_val, val);
            count += 1;
        }
        assert_eq!(count, num_keys);

        for (key, val) in kvs.iter() {
            println!(
                "**************************** Getting key {:?} **************************",
                key
            );
            let current_val = btree.get(key).unwrap();
            assert_eq!(current_val, *val);
        }

        let iter = btree.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {:?} **************************",
                key
            );
            let val = kvs.get(&key).unwrap();
            assert_eq!(&current_val, val);
            count += 1;
        }

        assert_eq!(count, num_keys);

        // println!("{}", btree.page_stats(false));

        println!("SUCCESS");
    }

    // skip default
    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn replay_stress<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());

        let kvs_file = "kvs.dat";
        let file = File::open(kvs_file).unwrap();
        let kvs: RandomKVs = serde_cbor::from_reader(file).unwrap();

        let bug_occurred_at = 1138;
        for (i, (key, val)) in kvs.iter().enumerate() {
            if i == bug_occurred_at {
                break;
            }
            println!(
                "**************************** Inserting {} key={:?} **************************",
                i, key
            );
            btree.insert(key, val).unwrap();
        }

        let (k, v) = &kvs[bug_occurred_at];
        println!(
            "BUG INSERT ************** Inserting {} key={:?} **************************",
            bug_occurred_at, k
        );
        btree.insert(k, v).unwrap();

        /*
        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Getting {} key={} **************************",
                i,
                key
            );
            let key = to_bytes(*key);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, *val);
        }
        */

        // let dot_string = btree.generate_dot();
        // let dot_file = "btree.dot";
        // let mut file = File::create(dot_file).unwrap();
        // // write dot_string as txt
        // file.write_all(dot_string.as_bytes()).unwrap();
    }

    #[rstest]
    #[case::in_mem(get_in_mem_pool())]
    fn test_insert_remove_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_empty(bp.clone()));
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let num_keys = 10000;
        let order = gen_random_permutation((0..num_keys).into_iter().collect::<Vec<_>>());
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Removing key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.delete(&key).unwrap();
        }

        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_bulk_insert_create<T: MemPool>(#[case] bp: Arc<T>) {
        let num_keys = 100000;
        let key_size = 8;
        let val_min_size = 50;
        let val_max_size = 100;
        let mut kvs = RandomKVs::new(
            true,
            true,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );
        let kvs = kvs.pop().unwrap();

        let btree = Arc::new(FosterBtree::bulk_insert_create(
            ContainerKey::new(0, 0),
            bp.clone(),
            kvs.iter(),
        ));

        // Print the page stats
        println!("{}", btree.page_stats(false));

        for (key, val) in kvs.iter() {
            let current_val = btree.get(key).unwrap();
            assert_eq!(current_val, *val);
        }

        // Print the page stats
        println!("{}", btree.page_stats(false));
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_parallel_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        // init_test_logger();
        let btree = Arc::new(setup_btree_empty(bp.clone()));
        let num_keys = 5000;
        let key_size = 100;
        let val_min_size = 50;
        let val_max_size = 100;
        let num_threads = 3;
        let kvs = RandomKVs::new(
            true,
            false,
            num_threads,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );
        let verify_kvs = kvs.clone();

        log_info!("Number of keys: {}", num_keys);

        // Use 3 threads to insert keys into the tree.
        // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
        thread::scope(
            // issue three threads to insert keys into the tree
            |s| {
                for kvs_i in kvs.iter() {
                    let btree = btree.clone();
                    s.spawn(move || {
                        log_info!("Spawned");
                        for (key, val) in kvs_i.iter() {
                            log_info!("Inserting key {:?}", key);
                            btree.insert(key, val).unwrap();
                        }
                    });
                }
            },
        );

        // Check if all keys have been inserted.
        for kvs_i in verify_kvs {
            for (key, val) in kvs_i.iter() {
                let current_val = btree.get(key).unwrap();
                assert_eq!(current_val, *val);
            }
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn test_page_split_triple<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = setup_btree_empty(bp.clone());
        let key0 = to_bytes(0);
        let val0 = vec![0; AVAILABLE_PAGE_SIZE * 2 / 5];
        let key2 = to_bytes(2);
        let val2 = vec![2; AVAILABLE_PAGE_SIZE * 2 / 5];
        btree.insert(&key0, &val0).unwrap();
        btree.insert(&key2, &val2).unwrap();

        let page_stats = btree.page_stats(true);
        println!("{}", page_stats);

        let key1 = to_bytes(1);
        let val1 = vec![1; AVAILABLE_PAGE_SIZE * 4 / 5]; // This should trigger a page split into 3 pages because (val0 + val1), (val1 + val2) are larger than a page.
        btree.insert(&key1, &val1).unwrap();

        let page_stats = btree.page_stats(true);
        println!("{}", page_stats);

        let current_val = btree.get(&key1).unwrap();
        assert_eq!(current_val, val1);

        let current_val = btree.get(&key0).unwrap();
        assert_eq!(current_val, val0);

        let current_val = btree.get(&key2).unwrap();
        assert_eq!(current_val, val2);
    }

    fn setup_btree_append_only_empty<T: MemPool>(bp: Arc<T>) -> FosterBtreeAppendOnly<T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);

        FosterBtreeAppendOnly::new(c_key, bp.clone())
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_append<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_append_only_empty(bp.clone()));
        // Insert 1024 bytes
        let inserting_key = to_bytes(0);
        for i in 0..10 {
            println!(
                "**************************** Appending key {} **************************",
                i
            );
            btree.append(&inserting_key, &to_bytes(i)).unwrap();
        }
        // Scan the tree
        let iter = btree.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {} **************************",
                count
            );
            // Check the prefix of the keys
            assert_eq!(key, inserting_key);
            assert_eq!(current_val, to_bytes(count));
            count += 1;
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_append_large<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_append_only_empty(bp.clone()));
        // Insert 1024 bytes
        let inserting_key = to_bytes(0);
        let val = vec![3_u8; 1024];
        for i in 0..10 {
            println!(
                "**************************** Appending key {} **************************",
                i
            );
            btree.append(&inserting_key, &val).unwrap();
        }
        // Scan the tree
        let iter = btree.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {} **************************",
                count
            );
            // Check the prefix of the keys
            assert_eq!(key, inserting_key);
            assert_eq!(current_val, val);
            count += 1;
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_concurrent_append<T: MemPool>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_btree_append_only_empty(bp.clone()));
        let num_keys = 1000;
        let key_size = 100;
        let val_min_size = 50;
        let val_max_size = 100;
        let num_threads = 3;
        let kvs = RandomKVs::new(
            true,
            false,
            num_threads,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );
        let mut verify_kvs = HashSet::new();
        for kvs_i in kvs.iter() {
            for (_key, val) in kvs_i.iter() {
                verify_kvs.insert(val.clone());
            }
        }

        log_info!("Number of keys: {}", num_keys);

        let key = to_bytes(0);

        // Use 3 threads to insert keys into the tree.
        // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
        thread::scope(
            // issue three threads to insert keys into the tree
            |s| {
                for kvs_i in kvs.iter() {
                    let btree = btree.clone();
                    let key = key.clone();
                    s.spawn(move || {
                        log_info!("Spawned");
                        for (_, val) in kvs_i.iter() {
                            log_info!("Appending key {:?}", key);
                            btree.append(&key, val).unwrap();
                        }
                    });
                }
            },
        );

        // Check if all keys have been inserted.
        let iter = btree.scan_key(&key);
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {:?} **************************",
                key
            );
            assert!(verify_kvs.remove(&current_val));
            count += 1;
        }
        assert_eq!(count, num_keys);
        assert!(verify_kvs.is_empty());
    }

    #[test]
    fn test_page_stat_generator() {
        let btree = setup_btree_empty(get_in_mem_pool());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        let page_stats = btree.page_stats(true);
        println!("{}", page_stats);

        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }

        let page_stats = btree.page_stats(true);
        println!("{}", page_stats);
    }
}
