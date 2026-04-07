use crate::{
    access_method::AccessMethodError,
    bp::prelude::{ContainerId, DatabaseId},
    txn_storage2::{
        field::{DataType, Field, NormalizedKeyFieldRange, Record},
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
    Primary {
        schema: Schema,
        /// Key Column types in key order, derived from schema for convenience.
        key_col_types: Vec<(usize, DataType)>,
        /// Value column types in column order, derived from schema for convenience.
        value_col_types: Vec<(usize, DataType)>,
        /// Key byte ranges for each key column in the normalized key.
        /// None unless every key column is fixed-width and non-nullable.
        key_field_ranges: Option<Vec<NormalizedKeyFieldRange>>,
        /// Value byte ranges for the serialized record value.
        /// None unless every value column is fixed-width and non-nullable.
        value_field_ranges: Option<Vec<NormalizedKeyFieldRange>>,
    },
    /// A secondary index that references a primary container.
    ///
    /// All fields except `primary_c_id` and `col_to_primary` are derived
    /// from the primary schema at `create_container` time via `resolve_secondary_info`.
    Secondary {
        primary_c_id: ContainerId,
        /// DataType of each secondary key column, in key order.
        key_col_types: Vec<(usize, DataType)>,
        /// Precomputed byte ranges in the normalized key.
        /// None unless every key column is fixed-width and non-nullable.
        key_field_ranges: Option<Vec<NormalizedKeyFieldRange>>,
        /// Which positions in the secondary key form the primary key.
        primary_key_positions: Vec<usize>,
    },
}

impl ContainerType {
    /// Extract projected fields from normalized key bytes.
    /// `wanted` is a sorted slice of key-column indices to extract.
    pub fn fields_from_key(&self, key_bytes: &[u8], wanted: &[usize]) -> Vec<Field> {
        let (key_col_types, key_field_ranges) = match self {
            ContainerType::Primary {
                key_col_types,
                key_field_ranges,
                ..
            }
            | ContainerType::Secondary {
                key_col_types,
                key_field_ranges,
                ..
            } => (key_col_types, key_field_ranges.as_deref()),
        };

        // When key layout is fully fixed, decode each requested field by slicing directly.
        if let Some(ranges) = key_field_ranges {
            return wanted
                .iter()
                .map(|&key_idx| {
                    let (offset, len) = ranges[key_idx];
                    crate::txn_storage2::field::field_from_normalized_key_bytes(
                        &key_bytes[offset..offset + len],
                        key_col_types[key_idx].1,
                    )
                })
                .collect();
        }

        crate::txn_storage2::field::fields_from_normalized_key_projected(
            key_bytes,
            key_col_types,
            wanted,
        )
    }

    /// Extract fields from a primary record's value bytes at the given column indices.
    /// If `cols` is empty, returns all columns.
    /// Returns one Field per entry in `cols`, in the same order (duplicates allowed).
    /// Panics on secondary containers.
    pub fn fields_from_value(&self, value_bytes: &[u8], cols: &[usize]) -> Vec<Field> {
        let ContainerType::Primary {
            schema,
            value_col_types,
            value_field_ranges,
            ..
        } = self
        else {
            panic!("fields_from_value called on secondary container")
        };
        let value_field_ranges = value_field_ranges.as_deref();

        // Stable value offsets let us decode by direct slicing instead of scanning.
        if cols.is_empty() {
            if let Some(ranges) = value_field_ranges {
                return value_col_types
                    .iter()
                    .map(|&(col_idx, data_type)| {
                        let (offset, len) = ranges[col_idx];
                        Field::from_bytes(
                            &value_bytes[offset..offset + len],
                            schema.cols()[col_idx].0,
                            data_type,
                        )
                    })
                    .collect();
            }

            let mut result = Vec::with_capacity(value_col_types.len());
            let mut offset = 0;
            for &(col_idx, data_type) in value_col_types {
                let is_nullable = schema.cols()[col_idx].0;
                let field = Field::from_bytes(&value_bytes[offset..], is_nullable, data_type);
                offset += field.size(is_nullable);
                result.push(field);
            }
            return result;
        }

        let is_sorted_dedup = cols.windows(2).all(|w| w[0] < w[1]);
        let mut sorted = Vec::new();
        let sorted_cols = if is_sorted_dedup {
            cols
        } else {
            sorted = cols.to_vec();
            sorted.sort();
            sorted.dedup();
            &sorted
        };

        let decoded = if let Some(ranges) = value_field_ranges {
            sorted_cols
                .iter()
                .map(|&col_idx| {
                    let (offset, len) = ranges[col_idx];
                    Field::from_bytes(
                        &value_bytes[offset..offset + len],
                        schema.cols()[col_idx].0,
                        value_col_types[col_idx].1,
                    )
                })
                .collect()
        } else {
            // Fallback: single forward scan, skipping non-projected columns cheaply.
            let mut result = Vec::with_capacity(sorted_cols.len());
            let mut offset = 0;
            let mut proj_idx = 0;
            for &(col_idx, data_type) in value_col_types {
                if proj_idx >= sorted_cols.len() {
                    break;
                }
                let is_nullable = schema.cols()[col_idx].0;
                if col_idx == sorted_cols[proj_idx] {
                    let field = Field::from_bytes(&value_bytes[offset..], is_nullable, data_type);
                    offset += field.size(is_nullable);
                    result.push(field);
                    proj_idx += 1;
                } else {
                    offset += crate::txn_storage2::field::skip_field_bytes(
                        &value_bytes[offset..],
                        is_nullable,
                        data_type,
                    );
                }
            }
            result
        };

        if is_sorted_dedup {
            return decoded;
        }

        cols.iter()
            .map(|&col_idx| decoded[sorted.binary_search(&col_idx).unwrap()].clone())
            .collect()
    }

    /// Extract primary key bytes from a secondary normalized key.
    /// Uses precomputed byte ranges (fast) or falls back to field deserialization.
    pub fn extract_primary_key_from_sec_key(&self, key_bytes: &[u8]) -> Vec<u8> {
        let ContainerType::Secondary {
            primary_key_positions,
            key_field_ranges: field_ranges,
            ..
        } = self
        else {
            panic!("extract_primary_key called on primary container")
        };

        if let Some(ranges) = field_ranges {
            let total_len: usize = primary_key_positions.iter().map(|&i| ranges[i].1).sum();
            let mut pk_bytes = Vec::with_capacity(total_len);
            for &i in primary_key_positions {
                let (offset, len) = ranges[i];
                pk_bytes.extend_from_slice(&key_bytes[offset..offset + len]);
            }
            return pk_bytes;
        }

        let pk_fields = self.fields_from_key(key_bytes, primary_key_positions);
        crate::txn_storage2::field::key_to_bytes(&pk_fields)
    }

    /// Serialize fields into value bytes.
    /// Panics on secondary containers.
    pub fn fields_to_value(&self, fields: &[Field]) -> Vec<u8> {
        let ContainerType::Primary { schema, .. } = self else {
            panic!("fields_to_value called on secondary container")
        };
        crate::txn_storage2::field::record_to_bytes(fields, schema)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerOptions {
    name: String,
    c_ds: ContainerDS,
    c_type: ContainerType,
}

impl ContainerOptions {
    pub fn primary(name: &str, c_ds: ContainerDS, schema: Schema) -> Self {
        let key_col_types: Vec<(usize, DataType)> = schema
            .key_indices()
            .iter()
            .map(|&i| (i, schema.cols()[i].1))
            .collect();
        let value_col_types: Vec<(usize, DataType)> = schema
            .cols()
            .iter()
            .enumerate()
            .map(|(i, &(_, dt))| (i, dt))
            .collect();
        let key_field_ranges = crate::txn_storage2::field::precompute_normalized_key_field_ranges(
            schema.key_indices().iter().map(|&i| schema.cols()[i]),
        );
        let value_field_ranges = crate::txn_storage2::field::precompute_value_field_ranges(&schema);
        ContainerOptions {
            name: String::from(name),
            c_ds,
            c_type: ContainerType::Primary {
                schema,
                key_col_types,
                value_col_types,
                key_field_ranges,
                value_field_ranges,
            },
        }
    }

    /// Create options for a secondary index container.
    ///
    /// `primary_c_id` is the container this index references.
    /// `secondary_key_columns` lists primary schema column indices that form
    /// the secondary key, in the desired key order.
    /// `primary_schema` is used to derive column types and primary key positions.
    ///
    /// **Design invariant**: All primary key columns must appear in
    /// `secondary_key_columns`. This ensures each secondary entry is unique
    /// (non-unique secondary columns + PK = unique composite key).
    pub fn secondary(
        name: &str,
        c_ds: ContainerDS,
        primary_c_id: ContainerId,
        secondary_key_columns: Vec<usize>,
        primary_schema: &Schema,
    ) -> Self {
        let key_col_types: Vec<(usize, DataType)> = secondary_key_columns
            .iter()
            .map(|&pri_col| (pri_col, primary_schema.cols()[pri_col].1))
            .collect();

        let pri_key_set: std::collections::HashSet<usize> =
            primary_schema.key_indices().iter().copied().collect();
        let primary_key_positions: Vec<usize> = secondary_key_columns
            .iter()
            .enumerate()
            .filter(|(_, &pri_col)| pri_key_set.contains(&pri_col))
            .map(|(sec_pos, _)| sec_pos)
            .collect();

        assert_eq!(
            primary_key_positions.len(),
            primary_schema.key_indices().len(),
            "secondary_key_columns must contain all primary key columns. \
             Primary key indices: {:?}, found in secondary: {:?}",
            primary_schema.key_indices(),
            primary_key_positions,
        );

        let key_field_ranges = crate::txn_storage2::field::precompute_normalized_key_field_ranges(
            secondary_key_columns
                .iter()
                .map(|&pri_col| primary_schema.cols()[pri_col]),
        );

        ContainerOptions {
            name: String::from(name),
            c_ds,
            c_type: ContainerType::Secondary {
                primary_c_id,
                key_col_types,
                primary_key_positions,
                key_field_ranges,
            },
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_structure(&self) -> ContainerDS {
        self.c_ds
    }

    pub fn container_type(&self) -> &ContainerType {
        &self.c_type
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.c_ds.to_bytes();
        bytes.extend_from_slice(self.name.as_bytes());
        match &self.c_type {
            ContainerType::Primary { schema, .. } => bytes.extend_from_slice(&schema.to_bytes()),
            ContainerType::Secondary { .. } => {}
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let c_ds = ContainerDS::from_bytes(&bytes[0..1]); // 1 byte
        let name = String::from_utf8(bytes[1..].to_vec()).expect("Invalid container name");
        let schema_bytes = &bytes[1 + name.len()..];
        let schema = Schema::from_bytes(schema_bytes);
        ContainerOptions::primary(&name, c_ds, schema)
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

    /// Atomically read and update selected fields under a single exclusive lock.
    ///
    /// Reads the full record, extracts the fields at `col_indices` into a temporary
    /// slice, passes `&mut [Field]` (indexed 0..col_indices.len()) to the closure,
    /// then writes modified fields back into the full record. The closure can both
    /// read and modify the selected fields.
    ///
    /// This avoids the shared→exclusive lock upgrade conflict that occurs with
    /// separate `get_fields` + `update_fields` calls.
    fn update_fields_with_func<F: FnOnce(&mut [Field])>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_indices: &[usize],
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
