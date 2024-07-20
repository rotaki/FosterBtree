use crate::{
    bp::prelude::{ContainerId, DatabaseId},
    prelude::TreeStatus,
};
use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum TxnStorageStatus {
    // Not found
    DBNotFound,
    ContainerNotFound,
    TxNotFound,
    KeyNotFound,

    // Already exists
    DBExists,
    ContainerExists,
    KeyExists,

    // Transaction errors
    TxnConflict,

    // System errors
    SystemAbort,

    // Other errors
    Error,

    // Access method errrors
    AccessError(TreeStatus),
}

// To String conversion
impl From<TxnStorageStatus> for String {
    fn from(status: TxnStorageStatus) -> String {
        match status {
            TxnStorageStatus::DBNotFound => "DB not found".to_string(),
            TxnStorageStatus::ContainerNotFound => "Container not found".to_string(),
            TxnStorageStatus::TxNotFound => "Tx not found".to_string(),
            TxnStorageStatus::KeyNotFound => "Key not found".to_string(),
            TxnStorageStatus::DBExists => "DB already exists".to_string(),
            TxnStorageStatus::ContainerExists => "Container already exists".to_string(),
            TxnStorageStatus::KeyExists => "Key already exists".to_string(),
            TxnStorageStatus::TxnConflict => "Txn conflict".to_string(),
            TxnStorageStatus::SystemAbort => "System abort".to_string(),
            TxnStorageStatus::Error => "Error".to_string(),
            TxnStorageStatus::AccessError(status) => format!("Access error: {:?}", status),
        }
    }
}

impl From<TreeStatus> for TxnStorageStatus {
    fn from(status: TreeStatus) -> TxnStorageStatus {
        match status {
            TreeStatus::NotFound => TxnStorageStatus::KeyNotFound,
            TreeStatus::Duplicate => TxnStorageStatus::KeyExists,
            other => TxnStorageStatus::AccessError(other),
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

#[derive(Clone)]
pub enum ContainerType {
    Hash,
    BTree,
}

pub struct ContainerOptions {
    name: String,
    c_type: ContainerType,
}

impl ContainerOptions {
    pub fn new(name: &str, c_type: ContainerType) -> Self {
        ContainerOptions {
            name: String::from(name),
            c_type,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn get_type(&self) -> ContainerType {
        self.c_type.clone()
    }
}

#[derive(Default)]
pub struct TxnOptions {}

#[derive(Default)]
pub struct ScanOptions {
    // currently scans all keys
}

impl ScanOptions {
    pub fn new() -> Self {
        ScanOptions::default()
    }
}

pub trait TxnStorageTrait {
    type TxnHandle;
    type IteratorHandle;

    // Open connection with the db
    fn open_db(&self, options: DBOptions) -> Result<DatabaseId, TxnStorageStatus>;

    // Close connection with the db
    fn close_db(&self, db_id: &DatabaseId) -> Result<(), TxnStorageStatus>;

    // Delete the db
    fn delete_db(&self, db_id: &DatabaseId) -> Result<(), TxnStorageStatus>;

    // Create a container in the db
    fn create_container(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus>;

    // Delete a container from the db
    fn delete_container(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        c_id: &ContainerId,
    ) -> Result<(), TxnStorageStatus>;

    // List all container names in the db
    fn list_containers(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
    ) -> Result<HashSet<ContainerId>, TxnStorageStatus>;

    // Begin a transaction
    fn begin_txn(
        &self,
        db_id: &DatabaseId,
        options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus>;

    // Commit a transaction
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
        c_id: &ContainerId,
    ) -> Result<usize, TxnStorageStatus>;

    // Check if value exists
    fn check_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<bool, TxnStorageStatus>;

    // Get value
    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus>;

    // Insert value
    fn insert_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus>;

    // Insert values
    fn insert_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), TxnStorageStatus>;

    // Update value
    fn update_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus>;

    // Delete value
    fn delete_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<(), TxnStorageStatus>;

    // Scan range. While iterating, the container should be alive.
    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus>;

    // Iterate next
    fn iter_next(
        &self,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus>;

    // Drop an iterator handle.
    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus>;
}
