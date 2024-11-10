use crate::{
    access_method::AccessMethodError,
    bp::prelude::{ContainerId, DatabaseId},
};
use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum TxnStorageStatus {
    // Not found
    DBNotFound,
    ContainerNotFound,
    TxnNotFound,
    KeyNotFound,

    // Already exists
    DBExists,
    ContainerExists,
    KeyExists,

    // Transaction errors
    TxnConflict,

    Aborted,
    AbortFailed,

    // Other errors
    Error,

    // Access method errrors
    AccessError(AccessMethodError),
}

// To String conversion
impl From<TxnStorageStatus> for String {
    fn from(status: TxnStorageStatus) -> String {
        match status {
            TxnStorageStatus::DBNotFound => "DB not found".to_string(),
            TxnStorageStatus::ContainerNotFound => "Container not found".to_string(),
            TxnStorageStatus::TxnNotFound => "Tx not found".to_string(),
            TxnStorageStatus::KeyNotFound => "Key not found".to_string(),
            TxnStorageStatus::DBExists => "DB already exists".to_string(),
            TxnStorageStatus::ContainerExists => "Container already exists".to_string(),
            TxnStorageStatus::KeyExists => "Key already exists".to_string(),
            TxnStorageStatus::TxnConflict => "Txn conflict".to_string(),
            TxnStorageStatus::Aborted => "Aborted".to_string(),
            TxnStorageStatus::AbortFailed => "Abort failed".to_string(),
            TxnStorageStatus::Error => "Error".to_string(),
            TxnStorageStatus::AccessError(status) => format!("Access error: {:?}", status),
        }
    }
}

impl From<AccessMethodError> for TxnStorageStatus {
    fn from(status: AccessMethodError) -> TxnStorageStatus {
        match status {
            AccessMethodError::KeyNotFound => TxnStorageStatus::KeyNotFound,
            AccessMethodError::KeyDuplicate => TxnStorageStatus::KeyExists,
            _ => TxnStorageStatus::AccessError(status),
        }
    }
}

pub struct DBOptions {
    name: String,
}

impl DBOptions {
    pub fn new(name: &str) -> Self {
        DBOptions {
            name: String::from(name),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Container data structure
#[derive(Clone)]
pub enum ContainerDS {
    Hash,
    BTree,
    AppendOnly,
}

impl ContainerDS {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ContainerDS::Hash => vec![0],
            ContainerDS::BTree => vec![1],
            ContainerDS::AppendOnly => vec![2],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        match bytes[0] {
            0 => ContainerDS::Hash,
            1 => ContainerDS::BTree,
            2 => ContainerDS::AppendOnly,
            _ => panic!("Invalid container type"),
        }
    }
}

#[derive(Clone)]
pub enum ContainerType {
    Primary,
    Secondary(ContainerId), // Secondary container with primary container id
}

pub struct ContainerOptions {
    name: String,
    c_ds: ContainerDS,
    c_type: ContainerType,
}

impl ContainerOptions {
    pub fn primary(name: &str, c_ds: ContainerDS) -> Self {
        ContainerOptions {
            name: String::from(name),
            c_ds,
            c_type: ContainerType::Primary,
        }
    }

    pub fn secondary(name: &str, c_ds: ContainerDS, primary_c_id: ContainerId) -> Self {
        ContainerOptions {
            name: String::from(name),
            c_ds,
            c_type: ContainerType::Secondary(primary_c_id),
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_structure(&self) -> ContainerDS {
        self.c_ds.clone()
    }

    pub fn container_type(&self) -> ContainerType {
        self.c_type.clone()
    }
}

#[derive(Default)]
pub struct TxnOptions {}

#[derive(Default)]
pub struct ScanOptions {
    pub lower: Vec<u8>,
    pub upper: Vec<u8>,
    pub limit: usize,
}

impl ScanOptions {
    pub fn new() -> Self {
        ScanOptions::default()
    }
}

pub trait TxnStorageTrait: Send + Sync {
    type TxnHandle;
    type IteratorHandle;

    // Open connection with the db
    fn open_db(&self, options: DBOptions) -> Result<DatabaseId, TxnStorageStatus>;

    // Close connection with the db
    fn close_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus>;

    // Delete the db
    fn delete_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus>;

    // Create a container in the db
    fn create_container(
        &self,
        db_id: DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus>;

    // Delete a container from the db
    fn delete_container(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus>;

    // List all container names in the db
    fn list_containers(&self, db_id: DatabaseId) -> Result<HashSet<ContainerId>, TxnStorageStatus>;

    // Insert value without transaction support
    fn raw_insert_value(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus>;

    // Transactional operations

    // Begin a transaction with the database.
    fn begin_txn(
        &self,
        db_id: DatabaseId,
        options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus>;

    // Commit a transaction.
    // If transaction has been committed safely, it returns Ok(()).
    // If transaction aborted, it returns Err(TxnStorageStatus::Aborted).
    // If transaction is not committed safely, it returns Err(TxnStorageStatus::AbortFailed).
    fn commit_txn(&self, txn: &Self::TxnHandle, async_commit: bool)
        -> Result<(), TxnStorageStatus>;

    // Abort a transaction
    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus>;

    // Wait for a transaction to finish
    fn wait_for_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus>;

    // Drop a transaction handle
    fn drop_txn(&self, txn: Self::TxnHandle) -> Result<(), TxnStorageStatus>;

    fn num_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus>;

    // Check if value exists
    fn check_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<bool, TxnStorageStatus>;

    // Get value
    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus>;

    // Insert value
    fn insert_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus>;

    // Insert values
    fn insert_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), TxnStorageStatus>;

    // Update value
    fn update_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus>;

    // Update value based on a function
    // On in-memory systems, &mut [u8] can point to the actual value in memory or the entry in the read-write set.
    // On on-disk systems with immediate modifications, the original value is copied and modified with this function and then written back.
    // On on-disk systems with deferred modifications, the original value is copied into the read-write set and modified there.
    fn update_value_with_func<K: AsRef<[u8]>, F: FnOnce(&mut [u8])>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
        func: F,
    ) -> Result<(), TxnStorageStatus>;

    // Delete value
    fn delete_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<(), TxnStorageStatus>;

    // Scan range. While iterating, the container should be alive.
    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus>;

    // Iterate next
    fn iter_next(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus>;

    // Drop an iterator handle.
    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus>;
}
