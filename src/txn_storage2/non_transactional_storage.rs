use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    access_method::{
        fbt::{FosterBtree, FosterBtreeRangeScanner},
        prelude::*,
    },
    bp::{ContainerId, ContainerKey, DatabaseId, MemPool},
    txn_storage2::{
        field::{Field, Record, RecordPointer},
        field_level_storage_trait::{
            ContainerDS, ContainerOptions, DBOptions, FieldLeveLStorageTrait, ScanOptions,
            TxnOptions, TxnStorageStatus,
        },
        schema::Schema,
        to_normalized_key,
    },
};

// ============================================================================
// Public Types
// ============================================================================

/// A simple transaction handle for non-transactional operations
#[derive(Debug, Clone, Copy)]
pub struct NonTxnHandle {
    db_id: DatabaseId,
}

/// Iterator handle for scanning operations
pub struct NonTxnIterator<M: MemPool> {
    scanner: Mutex<FosterBtreeRangeScanner<M>>,
    c_id: ContainerId,
}

/// Non-transactional field-level storage implementation using Foster B-trees
pub struct NonTransactionalStorage<M: MemPool> {
    mem_pool: Arc<M>,
    databases: RwLock<HashMap<DatabaseId, DatabaseInfo<M>>>,
    next_db_id: RwLock<DatabaseId>,
}

// ============================================================================
// Internal Types
// ============================================================================

struct DatabaseInfo<M: MemPool> {
    containers: HashMap<ContainerId, ContainerInfo<M>>,
    next_container_id: ContainerId,
}

struct ContainerInfo<M: MemPool> {
    options: ContainerOptions,
    btree: Arc<FosterBtree<M>>,
}

// ============================================================================
// Core Implementation
// ============================================================================

impl<M: MemPool> NonTransactionalStorage<M> {
    /// Create a new non-transactional storage instance
    pub fn new(mem_pool: Arc<M>) -> Self {
        Self {
            mem_pool,
            databases: RwLock::new(HashMap::new()),
            next_db_id: RwLock::new(1),
        }
    }
}

// ============================================================================
// Serialization Utilities
// ============================================================================

impl<M: MemPool> NonTransactionalStorage<M> {
    fn key_to_bytes(&self, key: &[Field]) -> Vec<u8> {
        to_normalized_key(
            key,
            &key.iter()
                .enumerate()
                .map(|(i, _)| (i, true, false))
                .collect::<Vec<_>>(),
        )
    }

    fn record_to_key_bytes(&self, record: &Record, schema: &Schema) -> Vec<u8> {
        to_normalized_key(
            &record.fields,
            &schema
                .key_indices()
                .iter()
                .map(|&i| (i, true, false))
                .collect::<Vec<_>>(),
        )
    }

    /// Convert a record to byte representation for B-tree storage
    fn record_to_bytes(&self, record: &Record, schema: &Schema) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (i, field) in record.fields.iter().enumerate() {
            let (is_nullable, _) = &schema.cols()[i];
            let field_bytes = field.to_bytes(*is_nullable);
            bytes.extend_from_slice(&field_bytes);
        }
        bytes
    }

    /// Convert bytes back to fields based on schema
    fn bytes_to_fields(
        &self,
        bytes: &[u8],
        schema: &Schema,
        field_indices: &[usize],
    ) -> Result<Vec<Field>, TxnStorageStatus> {
        // First, decode all fields from the record
        let all_fields = self.decode_all_fields(bytes, schema)?;

        // Extract requested fields
        let mut fields = Vec::new();
        for &idx in field_indices {
            if idx >= all_fields.len() {
                return Err(TxnStorageStatus::AbortFailed);
            }
            fields.push(all_fields[idx].clone());
        }

        Ok(fields)
    }

    /// Decode all fields from a byte array according to schema
    fn decode_all_fields(
        &self,
        bytes: &[u8],
        schema: &Schema,
    ) -> Result<Vec<Field>, TxnStorageStatus> {
        let mut all_fields = Vec::new();
        let mut offset = 0;

        for (is_nullable, data_type) in schema.cols().iter() {
            if offset >= bytes.len() {
                return Err(TxnStorageStatus::AbortFailed);
            }

            // Calculate the remaining bytes from current offset
            let remaining_bytes = &bytes[offset..];

            // Use Field::from_bytes to deserialize the field
            let field = Field::from_bytes(remaining_bytes, *is_nullable, *data_type);

            // Calculate how many bytes were consumed
            let consumed_bytes = field.size(*is_nullable);

            offset += consumed_bytes;
            all_fields.push(field);
        }

        Ok(all_fields)
    }

    /// Update a record by applying a modifier function to its fields
    fn update_record_with_modifier<F>(
        &self,
        txn: &NonTxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        modifier: F,
    ) -> Result<RecordPointer, TxnStorageStatus>
    where
        F: FnOnce(&mut Vec<Field>) -> Result<(), TxnStorageStatus>,
    {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        let key_bytes = self.key_to_bytes(&key);

        // Get current record
        let current_bytes = match container.btree.get(&key_bytes) {
            Ok(bytes) => bytes,
            Err(AccessMethodError::KeyNotFound) => return Err(TxnStorageStatus::KeyNotFound),
            Err(e) => return Err(TxnStorageStatus::from(e)),
        };

        // Convert to all fields
        let all_indices: Vec<usize> = (0..container.options.schema().cols().len()).collect();
        let mut current_fields =
            self.bytes_to_fields(&current_bytes, container.options.schema(), &all_indices)?;

        // Apply modifier function
        modifier(&mut current_fields)?;

        // Convert back to record and store
        let record = Record {
            fields: current_fields,
        };
        let new_bytes = self.record_to_bytes(&record, container.options.schema());

        container
            .btree
            .upsert(&key_bytes, &new_bytes)
            .map_err(TxnStorageStatus::from)?;

        // TODO: Get actual page_id and frame_id from btree
        Ok(RecordPointer::new(0, 0))
    }
}

// ============================================================================
// Database and Container Management
// ============================================================================

impl<M: MemPool> FieldLeveLStorageTrait for NonTransactionalStorage<M> {
    type TxnHandle = NonTxnHandle;
    type IteratorHandle = NonTxnIterator<M>;

    // ========================================================================
    // Database Management
    // ========================================================================

    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        let mut next_id = self.next_db_id.write().unwrap();
        let db_id = *next_id;
        *next_id += 1;

        let db_info = DatabaseInfo {
            containers: HashMap::new(),
            next_container_id: 1,
        };

        self.databases.write().unwrap().insert(db_id, db_info);
        Ok(db_id)
    }

    fn close_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        self.databases.write().unwrap().remove(&db_id);
        Ok(())
    }

    fn delete_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        self.close_db(db_id)
    }

    // ========================================================================
    // Container Management
    // ========================================================================

    fn create_container(
        &self,
        db_id: DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        let mut databases = self.databases.write().unwrap();
        let db = databases
            .get_mut(&db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;

        // Only support B-tree containers for now
        if options.data_structure() != ContainerDS::BTree {
            return Err(TxnStorageStatus::AbortFailed);
        }

        let c_id = db.next_container_id;
        db.next_container_id += 1;

        // Create Foster B-tree
        // TODO: Check options.schema().is_unique() and create FosterBtreeAppendOnly if false
        let container_key = ContainerKey::new(db_id, c_id);
        let btree = Arc::new(FosterBtree::new(container_key, self.mem_pool.clone()));

        let container_info = ContainerInfo {
            options: options.clone(),
            btree,
        };

        db.containers.insert(c_id, container_info);
        Ok(c_id)
    }

    fn delete_container(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        let mut databases = self.databases.write().unwrap();
        let db = databases
            .get_mut(&db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;

        db.containers
            .remove(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        Ok(())
    }

    fn list_containers(
        &self,
        db_id: DatabaseId,
    ) -> Result<Vec<(ContainerId, ContainerOptions)>, TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases.get(&db_id).ok_or(TxnStorageStatus::DBNotFound)?;

        let containers = db
            .containers
            .iter()
            .map(|(&c_id, container)| (c_id, container.options.clone()))
            .collect();

        Ok(containers)
    }

    // ========================================================================
    // Bulk Operations
    // ========================================================================

    fn raw_insert_record(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases.get(&db_id).ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        let key_bytes = self.record_to_key_bytes(&record, container.options.schema());
        let value_bytes = self.record_to_bytes(&record, container.options.schema());

        container
            .btree
            .insert(&key_bytes, &value_bytes)
            .map_err(TxnStorageStatus::from)?;

        // TODO: Get actual page_id and frame_id from btree
        Ok(RecordPointer::new(0, 0))
    }

    // ========================================================================
    // Transaction Management
    // ========================================================================

    fn begin_txn(
        &self,
        db_id: DatabaseId,
        _options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        // Check if database exists
        let databases = self.databases.read().unwrap();
        if !databases.contains_key(&db_id) {
            return Err(TxnStorageStatus::DBNotFound);
        }

        Ok(NonTxnHandle { db_id })
    }

    fn commit_txn(
        &self,
        _txn: &Self::TxnHandle,
        _async_commit: bool,
    ) -> Result<(), TxnStorageStatus> {
        // Non-transactional, so always succeeds
        Ok(())
    }

    fn abort_txn(&self, _txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        // Non-transactional, so always succeeds
        Ok(())
    }

    fn wait_for_txn(&self, _txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        // Non-transactional, so always succeeds
        Ok(())
    }

    fn drop_txn(&self, _txn: Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        // Non-transactional, so always succeeds
        Ok(())
    }

    // ========================================================================
    // Record Operations
    // ========================================================================

    fn num_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        // For simplicity, we'll scan the entire B-tree to count records
        // In a real implementation, you might want to maintain a separate counter
        let mut count = 0;
        let mut scanner = container.btree.scan_range(&[], &[]);

        while scanner.next().is_some() {
            count += 1;
        }

        Ok(count)
    }

    // ========================================================================
    // Field Access Operations
    // ========================================================================

    fn get_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        hint: Option<RecordPointer>,
    ) -> Result<(Field, RecordPointer), TxnStorageStatus> {
        let (fields, ptr) = self.get_fields(txn, c_id, key, &[col_idx], hint)?;
        Ok((fields.into_iter().next().unwrap(), ptr))
    }

    fn get_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_indices: &[usize],
        _hint: Option<RecordPointer>,
    ) -> Result<(Vec<Field>, RecordPointer), TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        let key_bytes = self.key_to_bytes(&key);

        match container.btree.get(&key_bytes) {
            Ok(value_bytes) => {
                let fields =
                    self.bytes_to_fields(&value_bytes, container.options.schema(), col_indices)?;
                // TODO: Get actual page_id and frame_id from btree
                let ptr = RecordPointer::new(0, 0);
                Ok((fields, ptr))
            }
            Err(AccessMethodError::KeyNotFound) => Err(TxnStorageStatus::KeyNotFound),
            Err(e) => Err(TxnStorageStatus::from(e)),
        }
    }

    // ========================================================================
    // Field Update Operations
    // ========================================================================

    fn update_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        field: Field,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        self.update_fields(txn, c_id, key, vec![(col_idx, field)], hint)
    }

    fn update_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        fields: Vec<(usize, Field)>,
        _hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        self.update_record_with_modifier(txn, c_id, key, |current_fields| {
            for (idx, field) in fields {
                if idx >= current_fields.len() {
                    return Err(TxnStorageStatus::AbortFailed);
                }
                current_fields[idx] = field;
            }
            Ok(())
        })
    }

    fn update_field_with_func<F: FnOnce(&mut Field)>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        func: F,
        _hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        self.update_record_with_modifier(txn, c_id, key, |current_fields| {
            func(&mut current_fields[col_idx]);
            Ok(())
        })
    }

    // ========================================================================
    // Record Insertion and Deletion
    // ========================================================================

    fn insert_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        record: Record,
        _hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        let key_bytes = self.record_to_key_bytes(&record, container.options.schema());
        let value_bytes = self.record_to_bytes(&record, container.options.schema());

        container
            .btree
            .insert(&key_bytes, &value_bytes)
            .map_err(TxnStorageStatus::from)?;

        // TODO: Get actual page_id and frame_id from btree
        Ok(RecordPointer::new(0, 0))
    }

    fn insert_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        records: Vec<(Record, Option<RecordPointer>)>,
    ) -> Result<Vec<RecordPointer>, TxnStorageStatus> {
        let mut pointers = Vec::new();
        for (record, hint) in records {
            let ptr = self.insert_record(txn, c_id, record, hint)?;
            pointers.push(ptr);
        }
        Ok(pointers)
    }

    fn delete_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        _hint: Option<RecordPointer>,
    ) -> Result<(), TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        let key_bytes = self.key_to_bytes(&key);

        container
            .btree
            .delete(&key_bytes)
            .map_err(TxnStorageStatus::from)?;

        Ok(())
    }

    // ========================================================================
    // Range Scanning and Iteration
    // ========================================================================

    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        _options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        let scanner = container.btree.scan_range(&[], &[]);
        Ok(NonTxnIterator {
            scanner: Mutex::new(scanner),
            c_id,
        })
    }

    fn iter_next(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<Field>, Vec<Field>, RecordPointer)>, TxnStorageStatus> {
        let databases = self.databases.read().unwrap();
        let db = databases
            .get(&txn.db_id)
            .ok_or(TxnStorageStatus::DBNotFound)?;
        let container = db
            .containers
            .get(&iter.c_id)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;

        if let Some((_key_bytes, value_bytes)) = iter.scanner.lock().unwrap().next() {
            // Convert value bytes to all fields first
            let all_indices: Vec<usize> = (0..container.options.schema().cols().len()).collect();
            let all_fields =
                self.bytes_to_fields(&value_bytes, container.options.schema(), &all_indices)?;

            // Extract primary key fields from the full record
            let pk_indices = container.options.schema().key_indices();
            let mut key_fields = Vec::new();
            for &idx in pk_indices {
                if idx < all_fields.len() {
                    key_fields.push(all_fields[idx].clone());
                }
            }

            // TODO: Get actual page_id and frame_id from btree
            let ptr = RecordPointer::new(0, 0);
            Ok(Some((key_fields, all_fields, ptr)))
        } else {
            Ok(None)
        }
    }

    fn drop_iterator_handle(&self, _iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus> {
        // Iterator will be dropped automatically
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_test_bp;
    use crate::txn_storage2::test_utils::{setup_db_with_schema, setup_simple_db};
    use crate::txn_storage2::DataType;
    use crate::{assert_field, field, record, schema};

    #[test]
    fn test_basic_operations() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),  // id (primary key)
            (true, DataType::String),  // name (nullable)
            (false, DataType::Int32),  // age (not nullable)
        ]);

        let (storage, db_id, container_id) = setup_db_with_schema(schema);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a record using macro
        let record = record![field!(Int32 1), field!(String "Alice"), field!(Int32 25)];
        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        // Get and verify fields using macro
        let key = vec![field!(Int32 1)];
        let (fields, _ptr) = storage
            .get_fields(&txn, container_id, key.clone(), &[1, 2], None)
            .unwrap();
        assert_eq!(fields.len(), 2);
        assert_field!(&fields[0], String("Alice"));
        assert_field!(&fields[1], Int32(25));

        // Update and verify field
        storage
            .update_field(&txn, container_id, key.clone(), 2, field!(Int32 26), None)
            .unwrap();
        let (updated_fields, _ptr) = storage
            .get_fields(&txn, container_id, key.clone(), &[2], None)
            .unwrap();
        assert_field!(&updated_fields[0], Int32(26));

        // Delete and verify deletion
        storage
            .delete_record(&txn, container_id, key.clone(), None)
            .unwrap();
        let result = storage.get_fields(&txn, container_id, key, &[1], None);
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        storage.commit_txn(&txn, false).unwrap();
        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_multi_field_primary_key() {
        let schema = schema!(pk: [0, 1], cols: [
            (false, DataType::Int32),  // department_id (part of primary key)
            (false, DataType::Int32),  // employee_id (part of primary key)
            (true, DataType::String),  // name (nullable)
            (false, DataType::Int32),  // salary (not nullable)
        ]);

        let (storage, db_id, container_id) = setup_db_with_schema(schema);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert records with multi-field primary key using macro
        let record1 = record![
            field!(Int32 10),       // department_id
            field!(Int32 1),        // employee_id
            field!(String "Alice"), // name
            field!(Int32 50000)     // salary
        ];

        let record2 = record![
            field!(Int32 10),     // department_id
            field!(Int32 2),      // employee_id
            field!(String "Bob"), // name
            field!(Int32 60000)   // salary
        ];

        storage
            .insert_record(&txn, container_id, record1, None)
            .unwrap();
        storage
            .insert_record(&txn, container_id, record2, None)
            .unwrap();

        // Retrieve and verify records using multi-field primary key
        let key1 = vec![field!(Int32 10), field!(Int32 1)];
        let key2 = vec![field!(Int32 10), field!(Int32 2)];

        let (fields1, _ptr) = storage
            .get_fields(&txn, container_id, key1, &[2, 3], None)
            .unwrap();
        let (fields2, _ptr) = storage
            .get_fields(&txn, container_id, key2, &[2, 3], None)
            .unwrap();

        assert_field!(&fields1[0], String("Alice"));
        assert_field!(&fields1[1], Int32(50000));
        assert_field!(&fields2[0], String("Bob"));
        assert_field!(&fields2[1], Int32(60000));

        storage.commit_txn(&txn, false).unwrap();
        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_database_management() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);

        // Test opening multiple databases
        let db1_options = DBOptions::new("test_db1");
        let db2_options = DBOptions::new("test_db2");

        let db1_id = storage.open_db(db1_options).unwrap();
        let db2_id = storage.open_db(db2_options).unwrap();

        // Database IDs should be different
        assert_ne!(db1_id, db2_id);

        // Test closing databases
        storage.close_db(db1_id).unwrap();
        storage.close_db(db2_id).unwrap();

        // Test deleting databases
        let db3_options = DBOptions::new("test_db3");
        let db3_id = storage.open_db(db3_options).unwrap();
        storage.delete_db(db3_id).unwrap();
    }

    #[test]
    fn test_container_management() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);

        let db_options = DBOptions::new("test_db");
        let db_id = storage.open_db(db_options).unwrap();

        // Create schema
        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );

        // Test creating containers
        let container1_options =
            ContainerOptions::new("container1", ContainerDS::BTree, schema.clone());
        let container2_options =
            ContainerOptions::new("container2", ContainerDS::BTree, schema.clone());

        let container1_id = storage
            .create_container(db_id, container1_options.clone())
            .unwrap();
        let container2_id = storage
            .create_container(db_id, container2_options.clone())
            .unwrap();

        // Container IDs should be different
        assert_ne!(container1_id, container2_id);

        // Test listing containers
        let containers = storage.list_containers(db_id).unwrap();
        assert_eq!(containers.len(), 2);

        let container_ids: Vec<_> = containers.iter().map(|(id, _)| *id).collect();
        assert!(container_ids.contains(&container1_id));
        assert!(container_ids.contains(&container2_id));

        // Test deleting containers
        storage.delete_container(db_id, container1_id).unwrap();
        let containers_after_delete = storage.list_containers(db_id).unwrap();
        assert_eq!(containers_after_delete.len(), 1);

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_error_handling() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);

        let db_options = DBOptions::new("test_db");
        let db_id = storage.open_db(db_options).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_options = ContainerOptions::new("test_container", ContainerDS::BTree, schema);
        let container_id = storage.create_container(db_id, container_options).unwrap();

        // Test transaction with invalid database ID
        let invalid_db_id = 999;
        let txn_result = storage.begin_txn(invalid_db_id, TxnOptions::default());
        assert!(matches!(txn_result, Err(TxnStorageStatus::DBNotFound)));

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Test operations with invalid container ID
        let invalid_container_id = 999;
        let key = vec![Field::Int32(Some(1))];
        let field_result = storage.get_field(&txn, invalid_container_id, key.clone(), 0, None);
        assert!(matches!(
            field_result,
            Err(TxnStorageStatus::ContainerNotFound)
        ));

        // Test getting non-existent key
        let non_existent_key = vec![Field::Int32(Some(999))];
        let field_result = storage.get_field(&txn, container_id, non_existent_key, 0, None);
        assert!(matches!(field_result, Err(TxnStorageStatus::KeyNotFound)));

        // Test deleting non-existent key
        let delete_result =
            storage.delete_record(&txn, container_id, vec![Field::Int32(Some(999))], None);
        assert!(matches!(delete_result, Err(TxnStorageStatus::KeyNotFound)));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_serialization_deserialization() {
        // Test with various data types
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),     // Primary key
            (true, DataType::String),     // Nullable string
            (false, DataType::Float64),   // Float
            (true, DataType::Bool),       // Nullable bool
            (false, DataType::FixedBytes8), // Fixed bytes
            (true, DataType::VarBytes),   // Variable bytes
        ]);

        let (storage, db_id, container_id) = setup_db_with_schema(schema);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert record with all data types
        let record = record![
            field!(Int32 1),
            field!(String "test_string"),
            field!(Float64 std::f64::consts::PI),
            field!(Bool true),
            field!(FixedBytes8 [1, 2, 3, 4, 5, 6, 7, 8]),
            field!(VarBytes vec![10, 20, 30, 40, 50])
        ];

        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        // Retrieve and verify all fields
        let key = vec![field!(Int32 1)];
        let (all_fields, _ptr) = storage
            .get_fields(&txn, container_id, key, &[0, 1, 2, 3, 4, 5], None)
            .unwrap();

        assert_eq!(all_fields.len(), 6);

        // Verify each field using macros
        assert_field!(&all_fields[0], Int32(1));
        assert_field!(&all_fields[1], String("test_string"));
        assert_field!(&all_fields[2], Float64(std::f64::consts::PI));
        assert_field!(&all_fields[3], Bool(true));
        assert_field!(&all_fields[4], FixedBytes8([1, 2, 3, 4, 5, 6, 7, 8]));
        assert_field!(&all_fields[5], VarBytes(vec![10, 20, 30, 40, 50]));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_nullable_fields() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),  // Primary key (non-nullable)
            (true, DataType::String),  // Nullable
            (true, DataType::Int32),   // Nullable
        ]);

        let (storage, db_id, container_id) = setup_db_with_schema(schema);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert record with null values
        let record = record![
            field!(Int32 1),
            field!(Null String), // Null string
            field!(Null Int32)   // Null int
        ];

        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        // Retrieve and verify null fields
        let key = vec![field!(Int32 1)];
        let (fields, _ptr) = storage
            .get_fields(&txn, container_id, key, &[1, 2], None)
            .unwrap();

        assert_eq!(fields.len(), 2);
        assert_field!(&fields[0], Null);
        assert_field!(&fields[1], Null);

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_field_updates() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (true, DataType::String),
            (false, DataType::Int32),
        ]);

        let (storage, db_id, container_id) = setup_db_with_schema(schema);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert initial record
        let record = record![field!(Int32 1), field!(String "initial"), field!(Int32 100)];

        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        let key = vec![field!(Int32 1)];

        // Test single field update
        storage
            .update_field(
                &txn,
                container_id,
                key.clone(),
                1,
                field!(String "updated"),
                None,
            )
            .unwrap();

        let (updated_field, _ptr) = storage
            .get_field(&txn, container_id, key.clone(), 1, None)
            .unwrap();
        assert_field!(&updated_field, String("updated"));

        // Test multiple field update
        storage
            .update_fields(
                &txn,
                container_id,
                key.clone(),
                vec![(1, field!(String "multi_updated")), (2, field!(Int32 200))],
                None,
            )
            .unwrap();

        let (updated_fields, _ptr) = storage
            .get_fields(&txn, container_id, key.clone(), &[1, 2], None)
            .unwrap();
        assert_field!(&updated_fields[0], String("multi_updated"));
        assert_field!(&updated_fields[1], Int32(200));

        // Test functional update
        storage
            .update_field_with_func(
                &txn,
                container_id,
                key.clone(),
                2,
                |field| {
                    if let Field::Int32(Some(ref mut val)) = field {
                        *val += 50;
                    }
                },
                None,
            )
            .unwrap();

        let (final_field, _ptr) = storage.get_field(&txn, container_id, key, 2, None).unwrap();
        assert_field!(&final_field, Int32(250));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_bulk_operations() {
        let (storage, db_id, container_id) = setup_simple_db();

        // Test bulk insert
        let records = (1..=100)
            .map(|i| record![field!(Int32 i), field!(String format!("value_{}", i))])
            .collect::<Vec<_>>();

        for record in records {
            storage
                .raw_insert_record(db_id, container_id, record)
                .unwrap();
        }

        // Test inserting multiple records via insert_records
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let additional_records = (101..=150)
            .map(|i| record![field!(Int32 i), field!(String format!("additional_{}", i))])
            .collect::<Vec<_>>();

        let additional_records_with_hints: Vec<(Record, Option<RecordPointer>)> =
            additional_records.into_iter().map(|r| (r, None)).collect();
        storage
            .insert_records(&txn, container_id, additional_records_with_hints)
            .unwrap();

        // Verify record count
        let count = storage.num_records(&txn, container_id).unwrap();
        assert_eq!(count, 150);

        // Verify some records
        let key_50 = vec![field!(Int32 50)];
        let (field_50, _ptr) = storage
            .get_field(&txn, container_id, key_50, 1, None)
            .unwrap();
        assert_field!(&field_50, String("value_50"));

        let key_125 = vec![field!(Int32 125)];
        let (field_125, _ptr) = storage
            .get_field(&txn, container_id, key_125, 1, None)
            .unwrap();
        assert_field!(&field_125, String("additional_125"));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_iteration_and_scanning() {
        let (storage, db_id, container_id) = setup_simple_db();
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert test data
        for i in 1..=10 {
            let record = record![field!(Int32 i), field!(String format!("item_{}", i))];
            storage
                .insert_record(&txn, container_id, record, None)
                .unwrap();
        }

        // Test scanning
        let iter = storage
            .scan_range(&txn, container_id, ScanOptions::new())
            .unwrap();

        let mut count = 0;
        let mut collected_keys = Vec::new();
        let mut collected_values = Vec::new();

        while let Ok(Some((key_fields, value_fields, _ptr))) = storage.iter_next(&txn, &iter) {
            count += 1;
            collected_keys.push(key_fields);
            collected_values.push(value_fields);
        }

        assert_eq!(count, 10);
        assert_eq!(collected_keys.len(), 10);
        assert_eq!(collected_values.len(), 10);

        // Verify some collected data
        for i in 0..count {
            // Keys should contain the primary key field
            assert_eq!(collected_keys[i].len(), 1);

            // Values should contain all fields
            assert_eq!(collected_values[i].len(), 2);
        }

        storage.drop_iterator_handle(iter).unwrap();
        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_complex_primary_key_operations() {
        // Test with three-field primary key
        let schema = schema!(pk: [0, 1, 2], cols: [
            (false, DataType::Int32),  // part of PK
            (false, DataType::String), // part of PK
            (false, DataType::Int32),  // part of PK
            (true, DataType::String),  // data field
        ]);

        let (storage, db_id, container_id) = setup_db_with_schema(schema);
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert records with composite keys
        let test_data = vec![
            (1, "A", 100, Some("data1")),
            (1, "A", 200, Some("data2")),
            (1, "B", 100, Some("data3")),
            (2, "A", 100, Some("data4")),
        ];

        for (id1, str_part, id2, data) in &test_data {
            let record = record![
                field!(Int32 * id1),
                field!(String str_part),
                field!(Int32 * id2),
                match data {
                    Some(s) => field!(String s),
                    None => field!(Null String),
                }
            ];

            storage
                .insert_record(&txn, container_id, record, None)
                .unwrap();
        }

        // Test retrieval with composite keys
        for (id1, str_part, id2, expected_data) in &test_data {
            let key = vec![
                field!(Int32 * id1),
                field!(String str_part),
                field!(Int32 * id2),
            ];

            let (fields, _ptr) = storage.get_field(&txn, container_id, key, 3, None).unwrap();

            match expected_data {
                Some(expected) => assert_field!(&fields, String(expected)),
                None => assert_field!(&fields, Null),
            }
        }

        // Test that partial keys don't work
        let partial_key = vec![field!(Int32 1)];
        let result = storage.get_field(&txn, container_id, partial_key, 3, None);
        assert!(result.is_err()); // Should fail because key is incomplete

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_transaction_operations() {
        let (storage, db_id, container_id) = setup_simple_db();

        // Test transaction lifecycle
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert some data
        let record = record![field!(Int32 1), field!(String "test")];

        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        // Test commit
        storage.commit_txn(&txn, false).unwrap();

        // Test async commit
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage.commit_txn(&txn2, true).unwrap();

        // Test abort
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage.abort_txn(&txn3).unwrap();

        // Test wait
        let txn4 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage.wait_for_txn(&txn4).unwrap();

        // Test drop
        let txn5 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage.drop_txn(txn5).unwrap();

        storage.close_db(db_id).unwrap();
    }
}
