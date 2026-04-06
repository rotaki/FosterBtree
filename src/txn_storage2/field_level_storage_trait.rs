use crate::{
    access_method::AccessMethodError,
    bp::prelude::{ContainerId, DatabaseId},
    txn_storage2::{
        field::{DataType, Field, Record},
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

/// Describes how a container relates to other containers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContainerType {
    /// A standalone primary container.
    Primary,
    /// A secondary index that references a primary container.
    ///
    /// `secondary_key_columns` lists primary schema column indices that form
    /// the secondary key, in order. The secondary schema is derived from the
    /// primary schema at `create_container` time.
    ///
    /// After creation, the derived fields are populated:
    /// - `primary_key_col_indices`: positions within the secondary key that
    ///   correspond to the primary table's key columns.
    /// - `sec_to_pri_col_map`: maps each secondary column position to its
    ///   primary schema column index (same as `secondary_key_columns`).
    Secondary {
        primary_c_id: ContainerId,
        /// Primary schema column indices forming the secondary key, in order.
        secondary_key_columns: Vec<usize>,
        /// Positions in secondary key that form the primary key. Derived at
        /// create_container time.
        primary_key_col_indices: Vec<usize>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerOptions {
    name: String,
    c_ds: ContainerDS,
    schema: Schema,
    c_type: ContainerType,
}

impl ContainerOptions {
    pub fn new(name: &str, c_ds: ContainerDS, schema: Schema) -> Self {
        ContainerOptions {
            name: String::from(name),
            c_ds,
            schema,
            c_type: ContainerType::Primary,
        }
    }

    /// Create options for a secondary index container.
    ///
    /// `primary_c_id` is the container this index references.
    /// `secondary_key_columns` lists primary schema column indices that form
    /// the secondary key, in the desired key order. The secondary schema is
    /// derived automatically from the primary schema at `create_container` time.
    ///
    /// **Design invariant**: All primary key columns must appear in
    /// `secondary_key_columns`. This ensures each secondary entry is unique
    /// (non-unique secondary columns + PK = unique composite key).
    pub fn secondary(
        name: &str,
        c_ds: ContainerDS,
        primary_c_id: ContainerId,
        secondary_key_columns: Vec<usize>,
    ) -> Self {
        // Schema will be derived from the primary at create_container time.
        // Use an empty placeholder for now.
        let placeholder_schema = Schema::with_primary_key(vec![], vec![]);
        ContainerOptions {
            name: String::from(name),
            c_ds,
            schema: placeholder_schema,
            c_type: ContainerType::Secondary {
                primary_c_id,
                secondary_key_columns,
                primary_key_col_indices: vec![], // derived at create_container time
            },
        }
    }

    /// Called by the storage layer at create_container time to derive the
    /// secondary schema from the primary and compute primary_key_col_indices.
    pub fn resolve_secondary_schema(&mut self, primary_schema: &Schema) {
        if let ContainerType::Secondary {
            ref secondary_key_columns,
            ref mut primary_key_col_indices,
            ..
        } = self.c_type
        {
            // Derive secondary schema: column types from primary, all are key columns
            let cols: Vec<(bool, DataType)> = secondary_key_columns
                .iter()
                .map(|&pri_col| primary_schema.cols()[pri_col])
                .collect();
            let key_indices: Vec<usize> = (0..cols.len()).collect();
            self.schema = Schema::with_primary_key(cols, key_indices);

            // Compute primary_key_col_indices: which positions in the secondary
            // key correspond to the primary table's key columns.
            let pri_key_set: std::collections::HashSet<usize> =
                primary_schema.key_indices().iter().copied().collect();
            *primary_key_col_indices = secondary_key_columns
                .iter()
                .enumerate()
                .filter(|(_, &pri_col)| pri_key_set.contains(&pri_col))
                .map(|(sec_pos, _)| sec_pos)
                .collect();

            // Validate: all primary key columns must be present
            assert_eq!(
                primary_key_col_indices.len(),
                primary_schema.key_indices().len(),
                "secondary_key_columns must contain all primary key columns. \
                 Primary key indices: {:?}, found in secondary: {:?}",
                primary_schema.key_indices(),
                primary_key_col_indices,
            );
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

    pub fn container_type(&self) -> &ContainerType {
        &self.c_type
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
        ContainerOptions {
            name,
            c_ds,
            schema,
            c_type: ContainerType::Primary,
        }
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
    /// Opaque hint returned by read/write operations to accelerate subsequent accesses
    /// to the same record. Implementations define what this contains (e.g. page location,
    /// partition id, or `()` if hints are not supported).
    type Hint: Clone + Copy + PartialEq + Eq + std::fmt::Debug + Send + Sync;

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
    // Raw insert without transaction support.
    // Bypasses all transaction mechanisms and directly inserts the record.
    // Use with caution as it provides no ACID guarantees.
    fn raw_insert_record(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
    ) -> Result<Self::Hint, TxnStorageStatus>;

    // Raw insert into a secondary index container.
    // `primary_hint` is the pointer to the primary record, embedded as the
    // last 8 bytes of the secondary value for prefetch and direct lookup.
    fn raw_insert_secondary_record(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
        primary_hint: Self::Hint,
    ) -> Result<Self::Hint, TxnStorageStatus>;

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
        hint: Option<Self::Hint>,
    ) -> Result<(Field, Self::Hint), TxnStorageStatus>;

    fn get_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idxs: &[usize],
        hint: Option<Self::Hint>,
    ) -> Result<(Vec<Field>, Self::Hint), TxnStorageStatus>;

    // Update field
    fn update_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        field: Field,
        hint: Option<Self::Hint>,
    ) -> Result<Self::Hint, TxnStorageStatus>;

    fn update_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        fields: Vec<(usize, Field)>,
        hint: Option<Self::Hint>,
    ) -> Result<Self::Hint, TxnStorageStatus>;

    fn update_field_with_func<F: FnOnce(&mut Field)>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        func: F,
        hint: Option<Self::Hint>,
    ) -> Result<Self::Hint, TxnStorageStatus>;

    // Insert a record
    fn insert_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        record: Record,
        hint: Option<Self::Hint>,
    ) -> Result<Self::Hint, TxnStorageStatus>;

    // Insert records
    fn insert_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        records: Vec<(Record, Option<Self::Hint>)>,
    ) -> Result<Vec<Self::Hint>, TxnStorageStatus>;

    // Delete a record
    fn delete_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        hint: Option<Self::Hint>,
    ) -> Result<(), TxnStorageStatus>;

    // Scan range. While iterating, the container should be alive.
    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus>;

    /// Zero-copy scan: calls the closure with raw (&[u8], &[u8], Hint) for each
    /// KV pair. No allocation for key/value bytes. The closure returns `true` to
    /// continue or `false` to stop early. Returns total tuples processed.
    fn iter_for_each(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
        f: &mut dyn FnMut(&[u8], &[u8], Self::Hint) -> bool,
    ) -> Result<u64, TxnStorageStatus>;

    /// Field-level scan: deserializes each KV pair inside the storage layer and
    /// calls the closure with (&[Field], &[Field], Hint) — key fields, projected
    /// value fields (per ScanOptions::cols), and the record hint. The closure
    /// returns `true` to continue or `false` to stop early. Returns total tuples
    /// processed.
    fn iter_for_each_fields(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
        f: &mut dyn FnMut(&[Field], Self::Hint) -> bool,
    ) -> Result<u64, TxnStorageStatus>;

    // Drop an iterator handle.
    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus>;
}
