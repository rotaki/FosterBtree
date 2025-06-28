use std::cmp;

// do not warn about unused imports
#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::{TxnStorageStatus, TxnStorageTrait};
use clap::Parser;

/// Enum representing different abort reasons for each transaction type
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum AbortID {
    // NewOrder abort reasons
    NewOrderGetWarehouse = 0,
    NewOrderUpdateDistrict = 1,
    NewOrderGetCustomer = 2,
    NewOrderInsertNewOrder = 3,
    NewOrderInsertOrder = 4,
    NewOrderInsertOrderSecondary = 5,
    NewOrderGetItem = 6,
    NewOrderUpdateStock = 7,
    NewOrderInsertOrderLine = 8,
    NewOrderPrecommit = 9,

    // Payment abort reasons
    PaymentUpdateWarehouse = 10,
    PaymentUpdateDistrict = 11,
    PaymentScanCustomerByLastName = 12,
    PaymentUpdateCustomer = 13,
    PaymentPrecommit = 14,

    // OrderStatus abort reasons
    OrderStatusGetCustomerByLastName = 20,
    OrderStatusGetCustomer = 21,
    OrderStatusGetOrderByCustomerId = 22,
    OrderStatusRangeGetOrderLine = 23,
    OrderStatusPrecommit = 24,

    // Delivery abort reasons
    DeliveryGetNewOrderWithSmallestKey = 30,
    DeliveryDeleteNewOrder = 31,
    DeliveryFinishDeleteNewOrder = 32,
    DeliveryPrepareUpdateOrder = 33,
    DeliveryFinishUpdateOrder = 34,
    DeliveryRangeUpdateOrderLine = 35,
    DeliveryPrepareUpdateCustomer = 36,
    DeliveryFinishUpdateCustomer = 37,
    DeliveryPrecommit = 38,

    // StockLevel abort reasons
    StockLevelGetDistrict = 40,
    StockLevelRangeGetOrderLine = 41,
    StockLevelGetStock = 42,
    StockLevelPrecommit = 43,
}

impl AbortID {
    /// Get the string name for this abort ID
    pub fn name(&self) -> &'static str {
        match self {
            // NewOrder
            AbortID::NewOrderGetWarehouse => "NewOrder_GetWarehouse",
            AbortID::NewOrderUpdateDistrict => "NewOrder_UpdateDistrict",
            AbortID::NewOrderGetCustomer => "NewOrder_GetCustomer",
            AbortID::NewOrderInsertNewOrder => "NewOrder_InsertNewOrder",
            AbortID::NewOrderInsertOrder => "NewOrder_InsertOrder",
            AbortID::NewOrderInsertOrderSecondary => "NewOrder_InsertOrderSecondary",
            AbortID::NewOrderGetItem => "NewOrder_GetItem",
            AbortID::NewOrderUpdateStock => "NewOrder_UpdateStock",
            AbortID::NewOrderInsertOrderLine => "NewOrder_InsertOrderLine",
            AbortID::NewOrderPrecommit => "NewOrder_Precommit",

            // Payment
            AbortID::PaymentUpdateWarehouse => "Payment_UpdateWarehouse",
            AbortID::PaymentUpdateDistrict => "Payment_UpdateDistrict",
            AbortID::PaymentScanCustomerByLastName => "Payment_ScanCustomerByLastName",
            AbortID::PaymentUpdateCustomer => "Payment_UpdateCustomer",
            AbortID::PaymentPrecommit => "Payment_Precommit",

            // OrderStatus
            AbortID::OrderStatusGetCustomerByLastName => "OrderStatus_GetCustomerByLastName",
            AbortID::OrderStatusGetCustomer => "OrderStatus_GetCustomer",
            AbortID::OrderStatusGetOrderByCustomerId => "OrderStatus_GetOrderByCustomerId",
            AbortID::OrderStatusRangeGetOrderLine => "OrderStatus_RangeGetOrderLine",
            AbortID::OrderStatusPrecommit => "OrderStatus_Precommit",

            // Delivery
            AbortID::DeliveryGetNewOrderWithSmallestKey => "Delivery_GetNewOrderWithSmallestKey",
            AbortID::DeliveryDeleteNewOrder => "Delivery_DeleteNewOrder",
            AbortID::DeliveryFinishDeleteNewOrder => "Delivery_FinishDeleteNewOrder",
            AbortID::DeliveryPrepareUpdateOrder => "Delivery_PrepareUpdateOrder",
            AbortID::DeliveryFinishUpdateOrder => "Delivery_FinishUpdateOrder",
            AbortID::DeliveryRangeUpdateOrderLine => "Delivery_RangeUpdateOrderLine",
            AbortID::DeliveryPrepareUpdateCustomer => "Delivery_PrepareUpdateCustomer",
            AbortID::DeliveryFinishUpdateCustomer => "Delivery_FinishUpdateCustomer",
            AbortID::DeliveryPrecommit => "Delivery_Precommit",

            // StockLevel
            AbortID::StockLevelGetDistrict => "StockLevel_GetDistrict",
            AbortID::StockLevelRangeGetOrderLine => "StockLevel_RangeGetOrderLine",
            AbortID::StockLevelGetStock => "StockLevel_GetStock",
            AbortID::StockLevelPrecommit => "StockLevel_Precommit",
        }
    }

    /// Get the transaction type this abort belongs to
    pub fn txn_type(&self) -> &'static str {
        match *self as u8 {
            0..=9 => "NewOrderTxn",
            10..=19 => "PaymentTxn",
            20..=29 => "OrderStatusTxn",
            30..=39 => "DeliveryTxn",
            40..=49 => "StockLevelTxn",
            _ => "Unknown",
        }
    }
}

/// Configuration settings parsed from command-line arguments.
#[derive(Parser, Debug, Clone, Default)]
#[command(version, about, long_about = None)]
pub struct TPCCConfig {
    /// BP size in GB. 0 means 1GB per warehouse.
    #[arg(short = 'b', long, default_value_t = 0)]
    pub bp_size: usize,

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

// Stats struct for transaction types
#[derive(Debug, Clone)]
pub struct TxnTypeStats {
    pub num_commits: u64,
    pub num_user_aborts: u64,
    pub num_system_aborts: u64,
    pub abort_counts: [u64; 53],
    pub total_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
}

impl Default for TxnTypeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnTypeStats {
    pub fn new() -> Self {
        Self {
            num_commits: 0,
            num_user_aborts: 0,
            num_system_aborts: 0,
            abort_counts: [0; 53],
            total_latency_ns: 0,
            min_latency_ns: u64::MAX,
            max_latency_ns: 0,
        }
    }

    pub fn record_commit(&mut self, latency_ns: u64) {
        self.num_commits += 1;
        self.total_latency_ns += latency_ns;
        self.min_latency_ns = self.min_latency_ns.min(latency_ns);
        self.max_latency_ns = self.max_latency_ns.max(latency_ns);
    }

    pub fn record_user_abort(&mut self) {
        self.num_user_aborts += 1;
    }

    pub fn record_system_abort(&mut self, abort_id: u8) {
        self.num_system_aborts += 1;
        if (abort_id as usize) < self.abort_counts.len() {
            self.abort_counts[abort_id as usize] += 1;
        }
    }
}

// Non-uniform random number generator is handled in record_definitions.rs

/// Function to check if the transaction did not succeed.
pub fn not_successful<K>(res: &Result<K, TxnStorageStatus>) -> bool {
    match res {
        Err(_e) => {
            log_info!("Error: {:?}", _e);
            true // System abort
        }
        Ok(_) => {
            log_info!("Result OK");
            false // Success
        }
    }
}

/// Helper struct for transaction management.
/// This struct keeps track of transaction statistics and
/// commits and aborts transactions based on the result
/// returned by the transactional storage.
pub struct TxHelper<'a, T: TxnStorageTrait> {
    txn_storage: &'a T,
    per_type: &'a mut TxnTypeStats,
}

impl<'a, T: TxnStorageTrait> TxHelper<'a, T> {
    /// Creates a new `TxHelper` instance.
    pub fn new(txn_storage: &'a T, per_type: &'a mut TxnTypeStats) -> Self {
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
                self.per_type.num_system_aborts += 1;
                self.per_type.abort_counts[abort_id as usize] += 1;
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
                self.per_type.total_latency_ns += time;
                self.per_type.min_latency_ns = cmp::min(self.per_type.min_latency_ns, time);
                self.per_type.max_latency_ns = cmp::max(self.per_type.max_latency_ns, time);
                self.per_type.num_commits += 1;
                TPCCStatus::Success
            }
            Err(TxnStorageStatus::Aborted) => {
                self.txn_storage.abort_txn(handler).unwrap();
                self.per_type.num_system_aborts += 1;
                self.per_type.abort_counts[abort_id as usize] += 1;
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
        self.per_type.num_user_aborts += 1;
        TPCCStatus::UserAbort
    }
}

// Define a constant for invalid item ID
pub const INVALID_ITEM_ID: u32 = 99999;

pub fn print_abort_details(txn_name: &str, stats: &TxnTypeStats) {
    if stats.num_system_aborts == 0 {
        return;
    }

    println!("    {} Abort Details:", txn_name);

    // Get the range of abort IDs for this transaction type
    let (start, end) = match txn_name {
        "NewOrderTxn" => (0, 10),
        "PaymentTxn" => (10, 15),
        "OrderStatusTxn" => (20, 25),
        "DeliveryTxn" => (30, 39),
        "StockLevelTxn" => (40, 44),
        _ => return,
    };

    // Print abort counts for this transaction type
    for i in start..end {
        if i < stats.abort_counts.len() && stats.abort_counts[i] > 0 {
            if let Ok(abort_id) = AbortID::try_from(i as u8) {
                println!("        {}: {}", abort_id.name(), stats.abort_counts[i]);
            }
        }
    }
}

impl TryFrom<u8> for AbortID {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AbortID::NewOrderGetWarehouse),
            1 => Ok(AbortID::NewOrderUpdateDistrict),
            2 => Ok(AbortID::NewOrderGetCustomer),
            3 => Ok(AbortID::NewOrderInsertNewOrder),
            4 => Ok(AbortID::NewOrderInsertOrder),
            5 => Ok(AbortID::NewOrderInsertOrderSecondary),
            6 => Ok(AbortID::NewOrderGetItem),
            7 => Ok(AbortID::NewOrderUpdateStock),
            8 => Ok(AbortID::NewOrderInsertOrderLine),
            9 => Ok(AbortID::NewOrderPrecommit),

            10 => Ok(AbortID::PaymentUpdateWarehouse),
            11 => Ok(AbortID::PaymentUpdateDistrict),
            12 => Ok(AbortID::PaymentScanCustomerByLastName),
            13 => Ok(AbortID::PaymentUpdateCustomer),
            14 => Ok(AbortID::PaymentPrecommit),

            20 => Ok(AbortID::OrderStatusGetCustomerByLastName),
            21 => Ok(AbortID::OrderStatusGetCustomer),
            22 => Ok(AbortID::OrderStatusGetOrderByCustomerId),
            23 => Ok(AbortID::OrderStatusRangeGetOrderLine),
            24 => Ok(AbortID::OrderStatusPrecommit),

            30 => Ok(AbortID::DeliveryGetNewOrderWithSmallestKey),
            31 => Ok(AbortID::DeliveryDeleteNewOrder),
            32 => Ok(AbortID::DeliveryFinishDeleteNewOrder),
            33 => Ok(AbortID::DeliveryPrepareUpdateOrder),
            34 => Ok(AbortID::DeliveryFinishUpdateOrder),
            35 => Ok(AbortID::DeliveryRangeUpdateOrderLine),
            36 => Ok(AbortID::DeliveryPrepareUpdateCustomer),
            37 => Ok(AbortID::DeliveryFinishUpdateCustomer),
            38 => Ok(AbortID::DeliveryPrecommit),

            40 => Ok(AbortID::StockLevelGetDistrict),
            41 => Ok(AbortID::StockLevelRangeGetOrderLine),
            42 => Ok(AbortID::StockLevelGetStock),
            43 => Ok(AbortID::StockLevelPrecommit),

            _ => Err(()),
        }
    }
}
