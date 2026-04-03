use std::sync::Arc;
use std::time::Instant;

use crate::{
    bp::{DatabaseId, MemPool},
    txn_storage2::{
        field_level_storage_trait::{FieldLeveLStorageTrait, TxnStorageStatus},
        transactional_storage::TransactionalStorage,
    },
};

/// Enumeration representing the status of a transaction
#[derive(Debug, PartialEq, Eq)]
pub enum TPCCStatus {
    Success,     // if all stages of transaction return successfully
    UserAbort,   // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SystemAbort, // if any stage of a transaction returns an error
}

/// Enum representing different abort reasons for each transaction type
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum AbortID {
    // NewOrder abort reasons
    NewOrderGetWarehouse = 0,
    NewOrderUpdateDistrict = 1,
    NewOrderGetCustomer = 2,
    NewOrderGetItem = 3,
    NewOrderGetStock = 4,
    NewOrderUpdateStock = 5,
    NewOrderInsertOrder = 6,
    NewOrderInsertOrderSecondary = 7,
    NewOrderInsertNewOrder = 8,
    NewOrderInsertOrderLine = 9,
    NewOrderCommit = 10,

    // Payment abort reasons
    PaymentUpdateWarehouse = 11,
    PaymentGetWarehouse = 12,
    PaymentUpdateDistrict = 13,
    PaymentGetDistrict = 14,
    PaymentScanCustomerSecondary = 15,
    PaymentGetCustomer = 16,
    PaymentUpdateCustomer = 17,
    PaymentInsertHistory = 18,
    PaymentCommit = 19,

    // OrderStatus abort reasons
    OrderStatusScanCustomerSecondary = 21,
    OrderStatusGetCustomer = 22,
    OrderStatusScanOrderSecondary = 23,
    OrderStatusGetOrder = 24,
    OrderStatusGetOrderLine = 25,
    OrderStatusCommit = 26,

    // Delivery abort reasons
    DeliveryScanNewOrder = 31,
    DeliveryDeleteNewOrder = 32,
    DeliveryGetOrder = 33,
    DeliveryUpdateOrder = 34,
    DeliveryGetOrderLine = 35,
    DeliveryUpdateOrderLine = 36,
    DeliveryUpdateCustomer = 37,
    DeliveryCommit = 38,

    // StockLevel abort reasons
    StockLevelGetDistrict = 41,
    StockLevelScanOrderLine = 42,
    StockLevelGetOrderLine = 43,
    StockLevelGetStock = 44,
    StockLevelCommit = 45,

    // Generic abort reasons
    InvalidInput = 51,
    UnexpectedError = 52,
}

/// Statistics for a single transaction type
#[derive(Debug)]
pub struct TxnTypeStats {
    pub num_commits: u64,
    pub num_user_aborts: u64,
    pub num_system_aborts: u64,
    pub abort_counts: [u64; 53], // Sized for all AbortID variants
    pub total_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
}

impl Default for TxnTypeStats {
    fn default() -> Self {
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
}

impl TxnTypeStats {
    pub fn new() -> Self {
        Self::default()
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

    pub fn record_system_abort(&mut self, abort_id: AbortID) {
        self.num_system_aborts += 1;
        self.abort_counts[abort_id as usize] += 1;
    }
}

/// Function to check if a transaction operation was not successful
pub fn not_successful<T>(result: &Result<T, TxnStorageStatus>) -> bool {
    result.is_err()
}

/// Helper struct for transaction management
pub struct TxHelper<'a, M: MemPool> {
    storage: &'a Arc<TransactionalStorage<M>>,
    _db_id: DatabaseId,
    stats: &'a mut TxnTypeStats,
    start_time: Instant,
}

impl<'a, M: MemPool> TxHelper<'a, M> {
    pub fn new(
        storage: &'a Arc<TransactionalStorage<M>>,
        db_id: DatabaseId,
        stats: &'a mut TxnTypeStats,
    ) -> Self {
        Self {
            storage,
            _db_id: db_id,
            stats,
            start_time: Instant::now(),
        }
    }

    /// Handles a failed transaction, aborts it, and updates statistics
    pub fn kill<T>(
        &mut self,
        txn: &<TransactionalStorage<M> as FieldLeveLStorageTrait>::TxnHandle,
        _result: &Result<T, TxnStorageStatus>,
        abort_id: AbortID,
    ) -> TPCCStatus {
        // Abort the transaction
        self.storage.abort_txn(txn).unwrap();

        // Update statistics
        self.stats.record_system_abort(abort_id);

        // Return system abort status
        TPCCStatus::SystemAbort
    }

    /// Attempts to commit the transaction and updates statistics
    pub fn commit(
        &mut self,
        txn: &<TransactionalStorage<M> as FieldLeveLStorageTrait>::TxnHandle,
        abort_id: AbortID,
    ) -> TPCCStatus {
        match self.storage.commit_txn(txn, false) {
            Ok(_) => {
                let latency_ns = self.start_time.elapsed().as_nanos() as u64;
                self.stats.record_commit(latency_ns);
                TPCCStatus::Success
            }
            Err(_) => {
                // Try to abort the transaction if commit failed
                let _ = self.storage.abort_txn(txn);
                self.stats.record_system_abort(abort_id);
                TPCCStatus::SystemAbort
            }
        }
    }

    /// Handles a user-initiated abort (e.g., 1% rollback in NewOrder)
    pub fn user_abort(
        &mut self,
        txn: &<TransactionalStorage<M> as FieldLeveLStorageTrait>::TxnHandle,
    ) -> TPCCStatus {
        let _ = self.storage.abort_txn(txn);
        self.stats.record_user_abort();
        TPCCStatus::UserAbort
    }
}

/// Print abort details for a transaction type
pub fn print_abort_details(txn_name: &str, stats: &TxnTypeStats) {
    println!("{} Abort Details:", txn_name);

    // Define abort ID ranges and names for each transaction type
    let abort_ids: Vec<(AbortID, &str)> = match txn_name {
        "NewOrderTxn" => vec![
            (AbortID::NewOrderGetWarehouse, "GET_WAREHOUSE"),
            (AbortID::NewOrderUpdateDistrict, "UPDATE_DISTRICT"),
            (AbortID::NewOrderGetCustomer, "GET_CUSTOMER"),
            (AbortID::NewOrderGetItem, "GET_ITEM"),
            (AbortID::NewOrderGetStock, "GET_STOCK"),
            (AbortID::NewOrderUpdateStock, "UPDATE_STOCK"),
            (AbortID::NewOrderInsertOrder, "INSERT_ORDER"),
            (
                AbortID::NewOrderInsertOrderSecondary,
                "INSERT_ORDER_SECONDARY",
            ),
            (AbortID::NewOrderInsertNewOrder, "INSERT_NEWORDER"),
            (AbortID::NewOrderInsertOrderLine, "INSERT_ORDERLINE"),
            (AbortID::NewOrderCommit, "COMMIT"),
        ],
        "PaymentTxn" => vec![
            (AbortID::PaymentUpdateWarehouse, "UPDATE_WAREHOUSE"),
            (AbortID::PaymentGetWarehouse, "GET_WAREHOUSE"),
            (AbortID::PaymentUpdateDistrict, "UPDATE_DISTRICT"),
            (AbortID::PaymentGetDistrict, "GET_DISTRICT"),
            (
                AbortID::PaymentScanCustomerSecondary,
                "SCAN_CUSTOMER_SECONDARY",
            ),
            (AbortID::PaymentGetCustomer, "GET_CUSTOMER"),
            (AbortID::PaymentUpdateCustomer, "UPDATE_CUSTOMER"),
            (AbortID::PaymentInsertHistory, "INSERT_HISTORY"),
            (AbortID::PaymentCommit, "COMMIT"),
        ],
        "OrderStatusTxn" => vec![
            (
                AbortID::OrderStatusScanCustomerSecondary,
                "SCAN_CUSTOMER_SECONDARY",
            ),
            (AbortID::OrderStatusGetCustomer, "GET_CUSTOMER"),
            (
                AbortID::OrderStatusScanOrderSecondary,
                "SCAN_ORDER_SECONDARY",
            ),
            (AbortID::OrderStatusGetOrder, "GET_ORDER"),
            (AbortID::OrderStatusGetOrderLine, "GET_ORDERLINE"),
            (AbortID::OrderStatusCommit, "COMMIT"),
        ],
        "DeliveryTxn" => vec![
            (AbortID::DeliveryScanNewOrder, "SCAN_NEWORDER"),
            (AbortID::DeliveryDeleteNewOrder, "DELETE_NEWORDER"),
            (AbortID::DeliveryGetOrder, "GET_ORDER"),
            (AbortID::DeliveryUpdateOrder, "UPDATE_ORDER"),
            (AbortID::DeliveryGetOrderLine, "GET_ORDERLINE"),
            (AbortID::DeliveryUpdateOrderLine, "UPDATE_ORDERLINE"),
            (AbortID::DeliveryUpdateCustomer, "UPDATE_CUSTOMER"),
            (AbortID::DeliveryCommit, "COMMIT"),
        ],
        "StockLevelTxn" => vec![
            (AbortID::StockLevelGetDistrict, "GET_DISTRICT"),
            (AbortID::StockLevelScanOrderLine, "SCAN_ORDERLINE"),
            (AbortID::StockLevelGetOrderLine, "GET_ORDERLINE"),
            (AbortID::StockLevelGetStock, "GET_STOCK"),
            (AbortID::StockLevelCommit, "COMMIT"),
        ],
        _ => return,
    };

    for (abort_id, name) in abort_ids {
        let count = stats.abort_counts[abort_id as usize];
        println!("        {:<45}: {}", name, count);
    }
}
