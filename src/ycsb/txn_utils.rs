use std::{
    ops::{Index, IndexMut},
    sync::atomic::{AtomicBool, Ordering},
};

use clap::Parser;

use crate::{
    prelude::{urand_int, TxnStorageStatus, TxnStorageTrait},
    random::{gen_random_byte_vec, small_thread_rng},
    zipfan::FastZipf,
};

// do not warn about unused imports
#[allow(unused_imports)]
use crate::log;
use crate::log_info;

use super::{loader::YCSBTableInfo, read_txn::ReadTxn, update_txn::UpdateTxn};

#[derive(Debug, Parser, Clone)]
pub struct YCSBConfig {
    /// Workload type
    #[clap(short = 'w', long, default_value = "A")]
    pub workload_type: char,

    // Number of threads
    #[clap(short = 't', long, default_value = "1")]
    pub num_threads: usize,

    /// Number of records. Default 10 M
    #[clap(short = 'n', long, default_value = "1000")]
    pub num_keys: usize,

    /// Key size
    #[clap(short = 'k', long, default_value = "8")]
    pub key_size: usize,

    /// Record size
    #[clap(short = 'v', long, default_value = "100")]
    pub value_size: usize,

    /// Skew factor
    #[clap(short = 's', long, default_value = "0.0")]
    pub skew_factor: f64,

    /// Warmup time in seconds
    #[clap(short = 'd', long, default_value = "3")]
    pub warmup_time: u64,

    /// Execution time in seconds
    #[clap(short = 'D', long, default_value = "10")]
    pub exec_time: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum YCSBStatus {
    Success,
    SystemAbort,
    Bug(String),
}

/// Struct to accumulate output data.
#[derive(Debug)]
pub struct YCSBOutput {
    pub out: u64,
}

impl Default for YCSBOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl YCSBOutput {
    /// Creates a new `Output` instance.
    pub fn new() -> Self {
        YCSBOutput { out: 0 }
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
pub enum YCSBTxnProfileID {
    ReadTxn = 0,
    UpdateTxn = 1,
    Max = 2,
}

impl From<u8> for YCSBTxnProfileID {
    fn from(val: u8) -> Self {
        match val {
            0 => YCSBTxnProfileID::ReadTxn,
            1 => YCSBTxnProfileID::UpdateTxn,
            _ => panic!("Invalid TxnProfileID"),
        }
    }
}

impl std::fmt::Display for YCSBTxnProfileID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            YCSBTxnProfileID::ReadTxn => write!(f, "ReadTxn"),
            YCSBTxnProfileID::UpdateTxn => write!(f, "WriteTxn"),
            YCSBTxnProfileID::Max => write!(f, "Max"),
        }
    }
}

// Mapping from TxProfileID to transaction profile structs.
pub trait YCSBTxnProfile {
    fn new(config: &YCSBConfig) -> Self;
    fn run<T: TxnStorageTrait>(
        &self,
        config: &YCSBConfig,
        txn_storage: &T,
        tbl_info: &YCSBTableInfo,
        stat: &mut YCSBStat,
        out: &mut YCSBOutput,
    ) -> YCSBStatus;
}

/// Statistics per transaction type.
#[derive(Debug)]
pub struct YCSBPerTxnType {
    pub num_commits: usize,
    pub num_sys_aborts: usize,
    pub total_latency: u64,
    pub min_latency: u64,
    pub max_latency: u64,
}

impl Default for YCSBPerTxnType {
    fn default() -> Self {
        Self::new()
    }
}

impl YCSBPerTxnType {
    /// Creates a new `PerTxType` instance.
    pub fn new() -> Self {
        YCSBPerTxnType {
            num_commits: 0,
            num_sys_aborts: 0,
            total_latency: 0,
            min_latency: u64::MAX,
            max_latency: 0,
        }
    }

    /// Adds statistics from another `PerTxType`.
    pub fn add(&mut self, rhs: &YCSBPerTxnType) {
        self.num_commits += rhs.num_commits;
        self.num_sys_aborts += rhs.num_sys_aborts;

        self.total_latency += rhs.total_latency;
        self.min_latency = std::cmp::min(self.min_latency, rhs.min_latency);
        self.max_latency = std::cmp::max(self.max_latency, rhs.max_latency);
    }
}

/// Overall statistics struct.

#[derive(Debug)]
pub struct YCSBStat {
    pub per_type: [YCSBPerTxnType; YCSBTxnProfileID::Max as usize],
}

impl Default for YCSBStat {
    fn default() -> Self {
        Self::new()
    }
}

impl YCSBStat {
    /// Creates a new `Stat` instance.
    pub fn new() -> Self {
        YCSBStat {
            per_type: [YCSBPerTxnType::new(), YCSBPerTxnType::new()],
        }
    }

    /// Gets a mutable reference to `PerTxType` based on `TxProfileID`.
    pub fn get_mut(&mut self, tx_type: YCSBTxnProfileID) -> &mut YCSBPerTxnType {
        &mut self.per_type[tx_type as usize]
    }

    /// Gets an immutable reference to `PerTxType` based on `TxProfileID`.
    pub fn get(&self, tx_type: YCSBTxnProfileID) -> &YCSBPerTxnType {
        &self.per_type[tx_type as usize]
    }

    /// Adds statistics from another `Stat`.
    pub fn add(&mut self, rhs: &YCSBStat) {
        for i in 0..(YCSBTxnProfileID::Max as usize) {
            self.per_type[i].add(&rhs.per_type[i]);
        }
    }

    /// Aggregates performance statistics across all transaction types.
    pub fn aggregate_perf(&self) -> YCSBPerTxnType {
        let mut out = YCSBPerTxnType::new();
        for i in 0..(YCSBTxnProfileID::Max as usize) {
            out.add(&self.per_type[i]);
        }
        out
    }
}

impl std::fmt::Display for YCSBStat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut out = String::new();
        for i in 0..(YCSBTxnProfileID::Max as usize) {
            out.push_str(&format!("TxnType: {:?}\n", YCSBTxnProfileID::from(i as u8)));
            out.push_str(&format!(
                "  num_commits: {}\n",
                self.per_type[i].num_commits
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
            out.push_str("]\n");
        }
        write!(f, "{}", out)
    }
}

impl Index<YCSBTxnProfileID> for YCSBStat {
    type Output = YCSBPerTxnType;

    fn index(&self, index: YCSBTxnProfileID) -> &Self::Output {
        &self.per_type[index as usize]
    }
}

impl IndexMut<YCSBTxnProfileID> for YCSBStat {
    fn index_mut(&mut self, index: YCSBTxnProfileID) -> &mut Self::Output {
        &mut self.per_type[index as usize]
    }
}

// ---------- Functions for generating keys and values ----------

pub fn get_key_bytes(key: usize, key_size: usize) -> Vec<u8> {
    if key_size < std::mem::size_of::<usize>() {
        panic!("Key size is less than the size of usize");
    }
    let mut key_vec = vec![0u8; key_size];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[key_size - bytes.len()..].copy_from_slice(&bytes);
    key_vec
}

pub fn from_key_bytes(key: &[u8]) -> usize {
    // The last 8 bytes of the key is the key

    usize::from_be_bytes(
        key[key.len() - std::mem::size_of::<usize>()..]
            .try_into()
            .unwrap(),
    )
}

pub fn get_key(num_keys: usize, skew_factor: f64) -> usize {
    let rng = small_thread_rng();
    FastZipf::new(rng, skew_factor, num_keys).sample() as usize
}

pub fn get_new_value(value_size: usize) -> Vec<u8> {
    gen_random_byte_vec(value_size, value_size)
}

/// Helper struct for transaction management.
/// This struct keeps track of transaction statistics and
/// commits and aborts transactions based on the result
/// returned by the transactional storage.
pub(super) struct TxHelper<'a, T: TxnStorageTrait> {
    txn_storage: &'a T,
    per_type: &'a mut YCSBPerTxnType,
}

impl<'a, T: TxnStorageTrait> TxHelper<'a, T> {
    /// Creates a new `TxHelper` instance.
    pub fn new(txn_storage: &'a T, per_type: &'a mut YCSBPerTxnType) -> Self {
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
    ) -> YCSBStatus {
        match res {
            Err(_e) => {
                self.per_type.num_sys_aborts += 1;
                self.txn_storage.abort_txn(handler).unwrap();
                YCSBStatus::SystemAbort
            }
            Ok(_) => {
                panic!("Not a failed transaction");
            }
        }
    }

    /// Attempts to commit the transaction and updates statistics.
    /// If the transaction fails to commit, it is aborted and statistics are updated.
    pub fn commit(&mut self, handler: &T::TxnHandle, time: u64) -> YCSBStatus {
        match self.txn_storage.commit_txn(handler, false) {
            Ok(_) => {
                self.per_type.total_latency += time;
                self.per_type.min_latency = std::cmp::min(self.per_type.min_latency, time);
                self.per_type.max_latency = std::cmp::max(self.per_type.max_latency, time);
                self.per_type.num_commits += 1;
                YCSBStatus::Success
            }
            Err(TxnStorageStatus::Aborted) => {
                self.txn_storage.abort_txn(handler).unwrap();
                self.per_type.num_sys_aborts += 1;
                YCSBStatus::SystemAbort
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}

pub fn run_with_retry<T, P>(
    thread_id: usize,
    config: &YCSBConfig,
    txn_storage: &T,
    tbl_info: &YCSBTableInfo,
    stat: &mut YCSBStat,
    out: &mut YCSBOutput,
) -> bool
where
    T: TxnStorageTrait,
    P: YCSBTxnProfile,
{
    let mut retry_count: u32 = 0;
    let base: u64 = 2;
    loop {
        let status = run::<T, P>(thread_id, config, txn_storage, tbl_info, stat, out);
        match status {
            YCSBStatus::Success => {
                log_info!("Success");
                return true;
            }
            YCSBStatus::SystemAbort => {
                log_info!("SystemAbort");
                // Sleep for base^retry_count nanoseconds
                let sleep_time = base.pow(retry_count);
                std::thread::sleep(std::time::Duration::from_nanos(sleep_time));
                retry_count += 1;
                // Retry the transaction
            }
            YCSBStatus::Bug(_reason) => {
                log_info!("Other: {}", _reason);
                return false;
            }
        }
    }
}

fn run<T, P>(
    _thread_id: usize,
    config: &YCSBConfig,
    txn_storage: &T,
    tbl_info: &YCSBTableInfo,
    stat: &mut YCSBStat,
    out: &mut YCSBOutput,
) -> YCSBStatus
where
    T: TxnStorageTrait,
    P: YCSBTxnProfile,
{
    let p = P::new(config);
    p.run(config, txn_storage, tbl_info, stat, out)
}

// Returns the proportion of each workload type
// (read, update, scan, insert, read-modify-write)
fn workload_proportion(workload_type: char) -> (usize, usize, usize, usize, usize) {
    match workload_type {
        'A' => (50, 50, 0, 0, 0), // Update heavy
        'B' => (95, 5, 0, 0, 0),  // Read mostly
        'C' => (100, 0, 0, 0, 0), // Read only
        'D' => {
            panic!("Workload type D not supported yet");
            // (5, 0, 0, 95, 0),  // Read latest workload
        }
        'E' => {
            panic!("Workload type E not supported yet");
            // (0, 0, 95, 5, 0) // Scan workload
        }
        'F' => {
            panic!("Workload type F not supported yet");
            // (50, 0, 0, 0, 50) // Read-modify-write workload
        }
        'X' => {
            // My workload. Scan only.
            (0, 0, 100, 0, 0)
        }
        _ => panic!("Invalid workload type. Choose from A, B, C, D, E, F"),
    }
}

pub fn run_ycsb_for_thread<T>(
    thread_id: usize,
    config: &YCSBConfig,
    txn_storage: &T,
    tbl_info: &YCSBTableInfo,
    flag: &AtomicBool, // while flag is true, keep running transactions
) -> (YCSBStat, YCSBOutput)
where
    T: TxnStorageTrait,
{
    let mut stat = YCSBStat::new();
    let mut out = YCSBOutput::new();
    let (read, update, _scan, _insert, _rmw) = workload_proportion(config.workload_type);

    while flag.load(Ordering::Acquire) {
        let x = urand_int(1, 100);
        if x <= read {
            run_with_retry::<T, ReadTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        } else if x <= read + update {
            run_with_retry::<T, UpdateTxn>(
                thread_id,
                config,
                txn_storage,
                tbl_info,
                &mut stat,
                &mut out,
            );
        } else {
            unimplemented!("Not implemented right now")
        }
    }
    (stat, out)
}
