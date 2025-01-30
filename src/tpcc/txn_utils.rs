use core::panic;
use std::cmp;
use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicBool, Ordering};

// do not warn about unused imports
#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::{TxnStorageStatus, TxnStorageTrait};
use clap::Parser;

use super::loader::TPCCTableInfo;
use super::prelude::{DeliveryTxn, NewOrderTxn, OrderStatusTxn, PaymentTxn, StockLevelTxn};
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
#[derive(Parser, Debug, Clone)]
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
    #[arg(short = 'f', long, default_value_t = true)]
    pub fixed_warehouse_per_thread: bool,

    /// Warmup duration in seconds.
    #[arg(short = 'd', long, default_value_t = 3)]
    pub warmup_time: u64,

    /// Test duration in seconds.
    #[arg(short = 'D', long, default_value_t = 10)]
    pub exec_time: u64,
}

/// Enumeration representing the status of a transaction.
#[derive(Debug, PartialEq, Eq)]
pub enum TPCCStatus {
    Success,     // if all stages of transaction return Result::Success
    UserAbort,   // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SystemAbort, // if any stage of a transaction returns Result::Abort
    Bug(String), // if any stage of a transaction returns unexpected Result::Fail
}

/// Struct to accumulate output data.
#[derive(Debug)]
pub struct TPCCOutput {
    pub out: u64,
}

impl Default for TPCCOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl TPCCOutput {
    /// Creates a new `Output` instance.
    pub fn new() -> Self {
        TPCCOutput { out: 0 }
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

/// Enumeration representing transaction profile IDs.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum TPCCTxnProfileID {
    NewOrderTxn = 0,
    PaymentTxn = 1,
    OrderStatusTxn = 2,
    DeliveryTxn = 3,
    StockLevelTxn = 4,
    Max = 5,
}

impl From<u8> for TPCCTxnProfileID {
    fn from(val: u8) -> Self {
        match val {
            0 => TPCCTxnProfileID::NewOrderTxn,
            1 => TPCCTxnProfileID::PaymentTxn,
            2 => TPCCTxnProfileID::OrderStatusTxn,
            3 => TPCCTxnProfileID::DeliveryTxn,
            4 => TPCCTxnProfileID::StockLevelTxn,
            _ => panic!("Invalid TxnProfileID"),
        }
    }
}

impl std::fmt::Display for TPCCTxnProfileID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TPCCTxnProfileID::NewOrderTxn => write!(f, "NewOrderTxn"),
            TPCCTxnProfileID::PaymentTxn => write!(f, "PaymentTxn"),
            TPCCTxnProfileID::OrderStatusTxn => write!(f, "OrderStatusTxn"),
            TPCCTxnProfileID::DeliveryTxn => write!(f, "DeliveryTxn"),
            TPCCTxnProfileID::StockLevelTxn => write!(f, "StockLevelTxn"),
            TPCCTxnProfileID::Max => write!(f, "Max"),
        }
    }
}

// Mapping from TxProfileID to transaction profile structs.
pub trait TPCCTxnProfile {
    fn new(config: &TPCCConfig, w_id: u16) -> Self;
    fn run<T: TxnStorageTrait>(
        &self,
        config: &TPCCConfig,
        txn_storage: &T,
        tbl_info: &TPCCTableInfo,
        stat: &mut TPCCStat,
        out: &mut TPCCOutput,
    ) -> TPCCStatus;
}

/// Statistics per transaction type.
#[derive(Debug)]
pub struct TPCCPerTxnType {
    pub num_commits: usize,
    pub num_usr_aborts: usize,
    pub num_sys_aborts: usize,
    pub abort_details: [usize; TPCCStat::ABORT_DETAILS_SIZE],
    pub total_latency: u64,
    pub min_latency: u64,
    pub max_latency: u64,
}

impl Default for TPCCPerTxnType {
    fn default() -> Self {
        Self::new()
    }
}

impl TPCCPerTxnType {
    /// Creates a new `PerTxType` instance.
    pub fn new() -> Self {
        TPCCPerTxnType {
            num_commits: 0,
            num_usr_aborts: 0,
            num_sys_aborts: 0,
            abort_details: [0; TPCCStat::ABORT_DETAILS_SIZE],
            total_latency: 0,
            min_latency: u64::MAX,
            max_latency: 0,
        }
    }

    /// Adds statistics from another `PerTxType`.
    pub fn add(&mut self, rhs: &TPCCPerTxnType, with_abort_details: bool) {
        self.num_commits += rhs.num_commits;
        self.num_usr_aborts += rhs.num_usr_aborts;
        self.num_sys_aborts += rhs.num_sys_aborts;

        if with_abort_details {
            for i in 0..TPCCStat::ABORT_DETAILS_SIZE {
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
pub struct TPCCStat {
    pub per_type: [TPCCPerTxnType; TPCCTxnProfileID::Max as usize],
}

impl Default for TPCCStat {
    fn default() -> Self {
        Self::new()
    }
}

impl TPCCStat {
    pub const ABORT_DETAILS_SIZE: usize = 20;

    /// Creates a new `Stat` instance.
    pub fn new() -> Self {
        TPCCStat {
            per_type: [
                TPCCPerTxnType::new(),
                TPCCPerTxnType::new(),
                TPCCPerTxnType::new(),
                TPCCPerTxnType::new(),
                TPCCPerTxnType::new(),
            ],
        }
    }

    /// Gets a mutable reference to `PerTxType` based on `TxProfileID`.
    pub fn get_mut(&mut self, tx_type: TPCCTxnProfileID) -> &mut TPCCPerTxnType {
        &mut self.per_type[tx_type as usize]
    }

    /// Gets an immutable reference to `PerTxType` based on `TxProfileID`.
    pub fn get(&self, tx_type: TPCCTxnProfileID) -> &TPCCPerTxnType {
        &self.per_type[tx_type as usize]
    }

    /// Adds statistics from another `Stat`.
    pub fn add(&mut self, rhs: &TPCCStat) {
        for i in 0..(TPCCTxnProfileID::Max as usize) {
            self.per_type[i].add(&rhs.per_type[i], true);
        }
    }

    /// Aggregates performance statistics across all transaction types.
    pub fn aggregate_perf(&self) -> TPCCPerTxnType {
        let mut out = TPCCPerTxnType::new();
        for i in 0..(TPCCTxnProfileID::Max as usize) {
            out.add(&self.per_type[i], false);
        }
        out
    }
}

impl std::fmt::Display for TPCCStat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut out = String::new();
        for i in 0..(TPCCTxnProfileID::Max as usize) {
            out.push_str(&format!("TxnType: {:?}\n", TPCCTxnProfileID::from(i as u8)));
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
            for j in 0..TPCCStat::ABORT_DETAILS_SIZE {
                out.push_str(&format!("{} ", self.per_type[i].abort_details[j]));
            }
            out.push_str("]\n");
        }
        write!(f, "{}", out)
    }
}

impl Index<TPCCTxnProfileID> for TPCCStat {
    type Output = TPCCPerTxnType;

    fn index(&self, index: TPCCTxnProfileID) -> &Self::Output {
        &self.per_type[index as usize]
    }
}

impl IndexMut<TPCCTxnProfileID> for TPCCStat {
    fn index_mut(&mut self, index: TPCCTxnProfileID) -> &mut Self::Output {
        &mut self.per_type[index as usize]
    }
}

/// Thread-local data struct.
pub struct ThreadLocalData {
    pub stat: TPCCStat,
    pub out: TPCCOutput,
}

impl Default for ThreadLocalData {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadLocalData {
    pub fn new() -> Self {
        ThreadLocalData {
            stat: TPCCStat::new(),
            out: TPCCOutput::new(),
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
    match res {
        Err(_e) => {
            log_info!("Error: {:?}", e);
            true // System abort
        }
        Ok(_) => {
            if config.random_abort && urand_int(1, 100) == 1 {
                log_info!("Result OK but random abort");
                true // Randomized system abort
            } else {
                log_info!("Result OK");
                false // Success
            }
        }
    }
}

/// Helper struct for transaction management.
/// This struct keeps track of transaction statistics and
/// commits and aborts transactions based on the result
/// returned by the transactional storage.
pub struct TxHelper<'a, T: TxnStorageTrait> {
    txn_storage: &'a T,
    per_type: &'a mut TPCCPerTxnType,
}

impl<'a, T: TxnStorageTrait> TxHelper<'a, T> {
    /// Creates a new `TxHelper` instance.
    pub fn new(txn_storage: &'a T, per_type: &'a mut TPCCPerTxnType) -> Self {
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
            Err(_e) => {
                self.per_type.num_sys_aborts += 1;
                self.per_type.abort_details[abort_id as usize] += 1;
                self.txn_storage.abort_txn(handler).unwrap();
                TPCCStatus::SystemAbort
            }
            Ok(_) => {
                panic!("Not a failed transaction");
            }
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

fn run<T, P>(
    thread_id: usize,
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    stat: &mut TPCCStat,
    out: &mut TPCCOutput,
) -> TPCCStatus
where
    T: TxnStorageTrait,
    P: TPCCTxnProfile,
{
    // Begin a transaction
    let w_id = if config.fixed_warehouse_per_thread {
        ((thread_id % config.num_warehouses as usize) + 1) as u16
    } else {
        urand_int(1, config.num_warehouses as u64) as u16
    };

    let p = P::new(config, w_id);
    p.run(config, txn_storage, tbl_info, stat, out)
}

fn run_with_retry<T, P>(
    thread_id: usize,
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    stat: &mut TPCCStat,
    out: &mut TPCCOutput,
) -> bool
where
    T: TxnStorageTrait,
    P: TPCCTxnProfile,
{
    let mut retry_count: u32 = 0;
    let base: u64 = 2;
    loop {
        let status = run::<T, P>(thread_id, config, txn_storage, tbl_info, stat, out);
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
                // Sleep for base^retry_count nanoseconds
                let sleep_time = base.pow(retry_count);
                std::thread::sleep(std::time::Duration::from_nanos(sleep_time));
                retry_count += 1;
                // Retry the transaction
            }
            TPCCStatus::Bug(_reason) => {
                log_info!("Other: {}", _reason);
                return false;
            }
        }
    }
}

pub fn run_tpcc_for_thread<T>(
    thread_id: usize,
    config: &TPCCConfig,
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    flag: &AtomicBool, // while flag is true, keep running transactions
) -> (TPCCStat, TPCCOutput)
where
    T: TxnStorageTrait,
{
    let mut stat = TPCCStat::new();
    let mut out = TPCCOutput::new();
    while flag.load(Ordering::Acquire) {
        let x = urand_int(1, 100);
        if x <= 4 {
            run_with_retry::<T, StockLevelTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        } else if x <= 4 + 4 {
            run_with_retry::<T, DeliveryTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        } else if x <= 4 + 4 + 4 {
            run_with_retry::<T, OrderStatusTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        } else if x <= 4 + 4 + 4 + 43 {
            run_with_retry::<T, PaymentTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        } else {
            run_with_retry::<T, NewOrderTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        }
    }
    (stat, out)
}
