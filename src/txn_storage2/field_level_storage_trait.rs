use crate::{
    access_method::AccessMethodError,
    bp::prelude::{ContainerId, DatabaseId},
    txn_storage2::{
        field::{Field, Record, RecordPointer},
        schema::Schema,
    },
};

#[derive(Debug, PartialEq, Clone, Copy)]
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
        }
    }
}

impl From<AccessMethodError> for TxnStorageStatus {
    fn from(status: AccessMethodError) -> TxnStorageStatus {
        match status {
            AccessMethodError::KeyNotFound => TxnStorageStatus::KeyNotFound,
            AccessMethodError::KeyDuplicate => TxnStorageStatus::KeyExists,
            AccessMethodError::PageReadLatchFailed
            | AccessMethodError::PageWriteLatchFailed
            | AccessMethodError::NotEnoughMemory => TxnStorageStatus::TxnConflict,
            other => {
                panic!("Unexpected AccessMethodError: {:?}", other)
            }
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerOptions {
    name: String,
    c_ds: ContainerDS,
    schema: Schema,
}

impl ContainerOptions {
    pub fn new(name: &str, c_ds: ContainerDS, schema: Schema) -> Self {
        ContainerOptions {
            name: String::from(name),
            c_ds,
            schema,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_structure(&self) -> ContainerDS {
        self.c_ds
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.c_ds.to_bytes();
        bytes.extend_from_slice(self.name.as_bytes());
        bytes.extend_from_slice(&self.schema.to_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let c_ds = ContainerDS::from_bytes(&bytes[0..1]); // 1 byte
        let name = String::from_utf8(bytes[1..].to_vec()).expect("Invalid container name");
        let schema_bytes = &bytes[1 + name.len()..];
        let schema = Schema::from_bytes(schema_bytes);
        ContainerOptions { name, c_ds, schema }
    }
}

#[derive(Default)]
pub struct TxnOptions {}

#[derive(Clone, Default)]
pub struct ScanOptions {
    pub lower_inc: Vec<u8>,
    pub upper_exc: Vec<u8>,
    pub cols: Vec<usize>, // Columns to scan
}

impl ScanOptions {
    pub fn new(cols: &[usize]) -> Self {
        ScanOptions {
            lower_inc: vec![],
            upper_exc: vec![],
            cols: cols.to_vec(),
        }
    }
    // lower: inclusive, upper: exclusive
    // [lower, upper)
    pub fn with_bounds(self, lower: Vec<Field>, upper: Vec<Field>) -> Self {
        use crate::txn_storage2::to_normalized_key;

        let lower_indices: Vec<_> = lower
            .iter()
            .enumerate()
            .map(|(i, _)| (i, true, false))
            .collect();
        let upper_indices: Vec<_> = upper
            .iter()
            .enumerate()
            .map(|(i, _)| (i, true, false))
            .collect();

        ScanOptions {
            lower_inc: to_normalized_key(&lower, &lower_indices),
            upper_exc: to_normalized_key(&upper, &upper_indices),
            cols: self.cols,
        }
    }
}

pub trait FieldLeveLStorageTrait: Send + Sync {
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

    fn get_container_stats(
        &self,
        _db_id: DatabaseId,
        _c_id: ContainerId,
    ) -> Result<String, TxnStorageStatus> {
        Ok("Stats disabled".to_string())
    }

    // List all container names in the db
    fn list_containers(
        &self,
        db_id: DatabaseId,
    ) -> Result<Vec<(ContainerId, ContainerOptions)>, TxnStorageStatus>;

    // Insert records without transaction support
    // Raw insert without transaction support
    // This method bypasses all transaction mechanisms and directly inserts the record
    // Use with caution as it provides no ACID guarantees
    fn raw_insert_record(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
    ) -> Result<RecordPointer, TxnStorageStatus>;

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

    fn num_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus>;

    // Get field
    fn get_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        hint: Option<RecordPointer>,
    ) -> Result<(Field, RecordPointer), TxnStorageStatus>;

    fn get_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idxs: &[usize],
        hint: Option<RecordPointer>,
    ) -> Result<(Vec<Field>, RecordPointer), TxnStorageStatus>;

    // Update field
    fn update_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        field: Field,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus>;

    fn update_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        fields: Vec<(usize, Field)>,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus>;

    fn update_field_with_func<F: FnOnce(&mut Field)>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        func: F,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus>;

    // Insert a record
    fn insert_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        record: Record,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus>;

    // Insert records
    fn insert_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        records: Vec<(Record, Option<RecordPointer>)>,
    ) -> Result<Vec<RecordPointer>, TxnStorageStatus>;

    // Delete a record
    fn delete_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        hint: Option<RecordPointer>,
    ) -> Result<(), TxnStorageStatus>;

    // Scan range. While iterating, the container should be alive.
    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus>;

    // Iterate next
    #[allow(clippy::type_complexity)]
    fn iter_next(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<Field>, RecordPointer)>, TxnStorageStatus>;

    // Drop an iterator handle.
    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus>;

    // Create multiple partitioned iterators for parallel scanning
    // Returns a vector of iterator handles, each covering a disjoint partition of the data
    fn create_partitioned_scan(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        num_partitions: usize,
        columns: Vec<usize>,
    ) -> Result<Vec<Self::IteratorHandle>, TxnStorageStatus>;
}
