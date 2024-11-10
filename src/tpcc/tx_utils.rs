use rand::Rng;
use std::cmp;
use std::hash::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::{Index, IndexMut, Shl};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::log_info;
use crate::prelude::{TxnOptions, TxnStorageStatus, TxnStorageTrait};
use crate::tpcc::record_definitions::Customer;
use clap::Parser;

use super::loader::TableInfo;
use super::record_definitions::urand_int;

#[macro_export]
macro_rules! write_fields {
    ($out:expr, $($field:expr),*) => {
        $(
            $out.write($field);
        )*
    };
}

/// Configuration settings parsed from command-line arguments.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct TPCCConfig {
    /// Number of warehouses.
    #[arg(short = 'w', long, default_value_t = 1)]
    pub num_warehouses: u16,

    /// Number of threads.
    #[arg(short = 't', long, default_value_t = 1)]
    pub num_threads: usize,

    /// Enable random aborts.
    #[arg(short = 'r', long, default_value_t = false)]
    pub random_abort: bool,

    /// Use fixed warehouse per thread.
    #[arg(short = 'f', long, default_value_t = false)]
    pub fixed_warehouse_per_thread: bool,
}

/// Enumeration representing the status of a transaction.
#[derive(Debug, PartialEq, Eq)]
pub enum TPCCStatus {
    Success,     // if all stages of transaction return Result::Success
    UserAbort,   // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SystemAbort, // if any stage of a transaction returns Result::Abort
    Bug,         // if any stage of a transaction returns unexpected Result::Fail
}

/// Struct to accumulate output data.
#[derive(Debug)]
pub struct Output {
    pub out: u64,
}

impl Output {
    /// Creates a new `Output` instance.
    pub fn new() -> Self {
        Output { out: 0 }
    }

    /// Writes any type `T` into the output by merging its bytes.
    pub fn write<T: Sized>(&mut self, t: &T) {
        let data = unsafe {
            std::slice::from_raw_parts((t as *const T) as *const u8, std::mem::size_of::<T>())
        };
        self.merge(data);
    }

    /// Merges the provided data into the output.
    fn merge(&mut self, data: &[u8]) {
        let mut size = data.len();
        let mut ptr = data.as_ptr();
        while size >= std::mem::size_of::<u64>() {
            let temp = unsafe { *(ptr as *const u64) };
            self.out = self.out.wrapping_add(temp);
            ptr = unsafe { ptr.add(std::mem::size_of::<u64>()) };
            size -= std::mem::size_of::<u64>();
        }
        if size > 0 {
            let mut temp: u64 = 0;
            unsafe {
                std::ptr::copy_nonoverlapping(ptr, &mut temp as *mut u64 as *mut u8, size);
            }
            self.out = self.out.wrapping_add(temp);
        }
    }

    /// Invalidates the output by resetting it.
    pub fn invalidate(&mut self) {
        self.out = 0;
    }
}

pub struct Permutation {
    perm: Vec<usize>,
}

impl Permutation {
    pub fn new(min: usize, max: usize) -> Permutation {
        assert!(min <= max);
        let size = max - min + 1;
        let mut perm: Vec<usize> = Vec::with_capacity(size);

        // Fill perm with values from min to max inclusive
        for i in min..=max {
            perm.push(i);
        }
        assert_eq!(perm.len(), size);

        // Now shuffle perm
        let s = perm.len();
        let mut rng = rand::thread_rng();
        for i in 0..(s - 1) {
            let j = rng.gen_range(0..=s - i - 1);
            assert!(i + j < s);
            if j != 0 {
                perm.swap(i, i + j);
            }
        }

        Permutation { perm }
    }
}

impl Index<usize> for Permutation {
    type Output = usize;

    fn index(&self, i: usize) -> &Self::Output {
        assert!(i < self.perm.len());
        &self.perm[i]
    }
}

/// Enumeration representing transaction profile IDs.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum TxnProfileID {
    NewOrderTxn = 0,
    PaymentTxn = 1,
    OrderStatusTxn = 2,
    DeliveryTxn = 3,
    StockLevelTxn = 4,
    Max = 5,
}

impl From<u8> for TxnProfileID {
    fn from(val: u8) -> Self {
        match val {
            0 => TxnProfileID::NewOrderTxn,
            1 => TxnProfileID::PaymentTxn,
            2 => TxnProfileID::OrderStatusTxn,
            3 => TxnProfileID::DeliveryTxn,
            4 => TxnProfileID::StockLevelTxn,
            _ => panic!("Invalid TxnProfileID"),
        }
    }
}

// Mapping from TxProfileID to transaction profile structs.
pub trait TxnProfile {
    fn new(config: &TPCCConfig, w_id: u16) -> Self;
    fn run<T: TxnStorageTrait>(
        &self,
        config: &TPCCConfig,
        txn_storage: &T,
        tbl_info: &TableInfo,
        stat: &mut Stat,
        out: &mut Output,
    ) -> TPCCStatus;
}

/// Statistics per transaction type.
#[derive(Debug)]
pub struct PerTxnType {
    pub num_commits: usize,
    pub num_usr_aborts: usize,
    pub num_sys_aborts: usize,
    pub abort_details: [usize; Stat::ABORT_DETAILS_SIZE],
    pub total_latency: u64,
    pub min_latency: u64,
    pub max_latency: u64,
}

impl PerTxnType {
    /// Creates a new `PerTxType` instance.
    pub fn new() -> Self {
        PerTxnType {
            num_commits: 0,
            num_usr_aborts: 0,
            num_sys_aborts: 0,
            abort_details: [0; Stat::ABORT_DETAILS_SIZE],
            total_latency: 0,
            min_latency: u64::MAX,
            max_latency: 0,
        }
    }

    /// Adds statistics from another `PerTxType`.
    pub fn add(&mut self, rhs: &PerTxnType, with_abort_details: bool) {
        self.num_commits += rhs.num_commits;
        self.num_usr_aborts += rhs.num_usr_aborts;
        self.num_sys_aborts += rhs.num_sys_aborts;

        if with_abort_details {
            for i in 0..Stat::ABORT_DETAILS_SIZE {
                self.abort_details[i] += rhs.abort_details[i];
            }
        }

        self.total_latency += rhs.total_latency;
        self.min_latency = cmp::min(self.min_latency, rhs.min_latency);
        self.max_latency = cmp::max(self.max_latency, rhs.max_latency);
    }
}

/// Overall statistics struct.

#[derive(Debug)]
pub struct Stat {
    pub per_type: [PerTxnType; TxnProfileID::Max as usize],
}

impl Stat {
    pub const ABORT_DETAILS_SIZE: usize = 20;

    /// Creates a new `Stat` instance.
    pub fn new() -> Self {
        Stat {
            per_type: [
                PerTxnType::new(),
                PerTxnType::new(),
                PerTxnType::new(),
                PerTxnType::new(),
                PerTxnType::new(),
            ],
        }
    }

    /// Gets a mutable reference to `PerTxType` based on `TxProfileID`.
    pub fn get_mut(&mut self, tx_type: TxnProfileID) -> &mut PerTxnType {
        &mut self.per_type[tx_type as usize]
    }

    /// Gets an immutable reference to `PerTxType` based on `TxProfileID`.
    pub fn get(&self, tx_type: TxnProfileID) -> &PerTxnType {
        &self.per_type[tx_type as usize]
    }

    /// Adds statistics from another `Stat`.
    pub fn add(&mut self, rhs: &Stat) {
        for i in 0..(TxnProfileID::Max as usize) {
            self.per_type[i].add(&rhs.per_type[i], true);
        }
    }

    /// Aggregates performance statistics across all transaction types.
    pub fn aggregate_perf(&self) -> PerTxnType {
        let mut out = PerTxnType::new();
        for i in 0..(TxnProfileID::Max as usize) {
            out.add(&self.per_type[i], false);
        }
        out
    }
}

impl std::fmt::Display for Stat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut out = String::new();
        for i in 0..(TxnProfileID::Max as usize) {
            out.push_str(&format!("TxnType: {:?}\n", TxnProfileID::from(i as u8)));
            out.push_str(&format!(
                "  num_commits: {}\n",
                self.per_type[i].num_commits
            ));
            out.push_str(&format!(
                "  num_usr_aborts: {}\n",
                self.per_type[i].num_usr_aborts
            ));
            out.push_str(&format!(
                "  num_sys_aborts: {}\n",
                self.per_type[i].num_sys_aborts
            ));
            out.push_str(&format!(
                "  total_latency: {}\n",
                self.per_type[i].total_latency
            ));
            out.push_str(&format!(
                "  min_latency: {}\n",
                self.per_type[i].min_latency
            ));
            out.push_str(&format!(
                "  max_latency: {}\n",
                self.per_type[i].max_latency
            ));
            out.push_str("  abort_details: [");
            for j in 0..Stat::ABORT_DETAILS_SIZE {
                out.push_str(&format!("{} ", self.per_type[i].abort_details[j]));
            }
            out.push_str("]\n");
        }
        write!(f, "{}", out)
    }
}

impl Index<TxnProfileID> for Stat {
    type Output = PerTxnType;

    fn index(&self, index: TxnProfileID) -> &Self::Output {
        &self.per_type[index as usize]
    }
}

impl IndexMut<TxnProfileID> for Stat {
    fn index_mut(&mut self, index: TxnProfileID) -> &mut Self::Output {
        &mut self.per_type[index as usize]
    }
}

/// Thread-local data struct.
pub struct ThreadLocalData {
    pub stat: Stat,
    pub out: Output,
}

impl ThreadLocalData {
    pub fn new() -> Self {
        ThreadLocalData {
            stat: Stat::new(),
            out: Output::new(),
        }
    }
}

// Non-uniform random number generator.
// Only used for generating customer numbers, customer last names,
// and item numbers, means an independently selected and non-uniformly
// distributed random number over the specified range of values [x .. y].
// This number must be generated by using the function NURand which
// produces positions within the range [x .. y].
// The results of NURand might have to be converted to produce a name or a
// number valid for the implementation
// NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y -x + 1)) + x
// where exp-1 | exp-2 stands for the bitwise logical OR operation between exp-1 and exp-2
// exp-1 % exp-2 stands for exp-1 modulo exp-2
// random(x, y) stands for ramdomly selected within [x .. y]
// A is a constant chosen according to the size of the range [x .. y]
//   for C_LAST, the range is [0 .. 999] and A = 255
//   for C_ID, the range is [1 .. 3000] and A = 1023
//   for OL_I_ID, the range is [1 .. 100000] and A = 8191
// C is a run-time constant randomly chosen within [0 .. A] that can be varied without altering performance.
// The same C value, per field (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals
// Define constants as in the original code

/// Function to check if the transaction did not succeed.
pub fn not_successful<K>(config: &TPCCConfig, res: &Result<K, TxnStorageStatus>) -> bool {
    if config.random_abort && res.is_ok() && urand_int(1, 100) == 1 {
        return true; // Randomized system abort
    } else if res.is_err() {
        return true; // System abort
    } else {
        return false; // Success
    }
}

/// Helper struct for transaction management.
/// This struct keeps track of transaction statistics and
/// commits and aborts transactions based on the result
/// returned by the transactional storage.
pub struct TxHelper<'a, T: TxnStorageTrait> {
    txn_storage: &'a T,
    per_type: &'a mut PerTxnType,
}

impl<'a, T: TxnStorageTrait> TxHelper<'a, T> {
    /// Creates a new `TxHelper` instance.
    pub fn new(txn_storage: &'a T, per_type: &'a mut PerTxnType) -> Self {
        TxHelper {
            txn_storage,
            per_type,
        }
    }

    /// Handles a failed transaction and updates statistics.
    pub fn kill<K>(
        &mut self,
        handler: &T::TxnHandle,
        res: &Result<K, TxnStorageStatus>,
        abort_id: u8,
    ) -> TPCCStatus {
        match res {
            Err(e) => {
                self.per_type.num_sys_aborts += 1;
                self.per_type.abort_details[abort_id as usize] += 1;
                self.txn_storage.abort_txn(handler).unwrap();
                TPCCStatus::SystemAbort
            }
            _ => panic!("wrong TransactionResult"),
        }
    }

    /// Attempts to commit the transaction and updates statistics.
    /// If the transaction fails to commit, it is aborted and statistics are updated.
    pub fn commit(&mut self, handler: &T::TxnHandle, abort_id: u8, time: u64) -> TPCCStatus {
        match self.txn_storage.commit_txn(handler, false) {
            Ok(_) => {
                self.per_type.total_latency += time;
                self.per_type.min_latency = cmp::min(self.per_type.min_latency, time);
                self.per_type.max_latency = cmp::max(self.per_type.max_latency, time);
                self.per_type.num_commits += 1;
                TPCCStatus::Success
            }
            Err(TxnStorageStatus::Aborted) => {
                self.txn_storage.abort_txn(handler).unwrap();
                self.per_type.num_sys_aborts += 1;
                self.per_type.abort_details[abort_id as usize] += 1;
                TPCCStatus::SystemAbort
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    /// Handles a user abort and updates statistics.
    pub fn usr_abort(&mut self, handler: &T::TxnHandle) -> TPCCStatus {
        self.txn_storage.abort_txn(handler).unwrap();
        self.per_type.num_usr_aborts += 1;
        TPCCStatus::UserAbort
    }
}

pub fn run<T, P>(
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TableInfo,
    stat: &mut Stat,
    out: &mut Output,
) -> TPCCStatus
where
    T: TxnStorageTrait,
    P: TxnProfile,
{
    // Begin a transaction
    let w_id = if config.fixed_warehouse_per_thread {
        // A temporary workaround to avoid using nightly features
        // to convert thread id to u16
        let thread_id = std::thread::current().id();
        let mut hasher = DefaultHasher::new();
        thread_id.hash(&mut hasher);
        let thread_id_u64 = hasher.finish();
        ((thread_id_u64 % config.num_warehouses as u64) + 1) as u16
    } else {
        urand_int(1, config.num_warehouses as u64) as u16
    };

    let p = P::new(config, w_id);
    p.run(config, txn_storage, tbl_info, stat, out)
}

pub fn run_with_retry<T, P>(
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TableInfo,
    stat: &mut Stat,
    out: &mut Output,
) -> bool
where
    T: TxnStorageTrait,
    P: TxnProfile,
{
    loop {
        let status = run::<T, P>(config, txn_storage, tbl_info, stat, out);
        match status {
            TPCCStatus::Success => {
                log_info!("Success");
                return true;
            }
            TPCCStatus::UserAbort => {
                log_info!("UserAbort");
                return false; // Stop retrying since the user initiated the abort
            }
            TPCCStatus::SystemAbort => {
                log_info!("SystemAbort");
                // Retry the transaction
            }
            TPCCStatus::Bug => {
                panic!("Unexpected result");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permutation_elements() {
        for _ in 0..100 {
            let min = 0;
            let max = 9;
            let perm = Permutation::new(min, max);
            let mut elements = perm.perm.clone();
            elements.sort_unstable();
            let expected: Vec<usize> = (min..=max).collect();
            assert_eq!(elements, expected);
        }
    }
}
