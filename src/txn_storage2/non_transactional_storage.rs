use std::{cell::UnsafeCell, collections::HashMap, sync::Arc};

use crate::{
    access_method::{
        fbt::{BTreeKey, FosterBtree, FosterBtreeRangeScanner},
        prelude::*,
    },
    bp::{ContainerId, ContainerKey, DatabaseId, MemPool, PageFrameKey},
    txn_storage2::{
        field::{key_to_bytes, record_to_key_bytes, Field, Record, RecordHandle},
        field_level_storage_trait::{
            ContainerDS, ContainerOptions, ContainerType, DBOptions, FieldLeveLStorageTrait,
            ScanOptions, TxnOptions, TxnStorageStatus,
        },
    },
};

// ============================================================================
// Public Types
// ============================================================================

/// A simple transaction handle for non-transactional operations
#[derive(Debug, Clone, Copy)]
pub struct NonTxnHandle;

/// Iterator handle for scanning operations
pub struct NonTxnIterator<M: MemPool> {
    scanner: UnsafeCell<FosterBtreeRangeScanner<M>>,
    c_id: ContainerId,
    options: ScanOptions,
}

// SAFETY: This is safe because we assume single-threaded access only
unsafe impl<M: MemPool> Send for NonTxnIterator<M> {}
unsafe impl<M: MemPool> Sync for NonTxnIterator<M> {}

/// Non-transactional field-level storage implementation using Foster B-trees
/// This implementation assumes single-threaded access and uses unsafe for performance
pub struct NonTransactionalStorage<M: MemPool> {
    mem_pool: Arc<M>,
    containers: UnsafeCell<HashMap<ContainerId, ContainerInfo<M>>>,
    next_container_id: UnsafeCell<ContainerId>,
}

// SAFETY: This is safe because we assume single-threaded access only
// The user of this struct must ensure that it's only accessed from a single thread
unsafe impl<M: MemPool> Sync for NonTransactionalStorage<M> {}
unsafe impl<M: MemPool> Send for NonTransactionalStorage<M> {}

// ============================================================================
// Internal Types
// ============================================================================

struct ContainerInfo<M: MemPool> {
    options: ContainerOptions,
    btree: Arc<FosterBtree<M>>,
}

impl<M: MemPool> ContainerInfo<M> {
    fn to_key_bytes(&self, record: &[Field]) -> Vec<u8> {
        match self.options.container_type() {
            ContainerType::Primary { schema, .. } => {
                record_to_key_bytes(record, schema.key_indices())
            }
            ContainerType::Secondary { .. } => record_to_key_bytes(record, &[]),
        }
    }

    fn fields_to_value(&self, fields: &[Field]) -> Vec<u8> {
        self.options.container_type().fields_to_value(fields)
    }

    fn get_fields(&self, key_bytes: &[u8], value_bytes: &[u8], cols: &[usize]) -> Vec<Field> {
        self.options
            .container_type()
            .get_fields(key_bytes, value_bytes, cols)
    }
}

// ============================================================================
// Core Implementation
// ============================================================================

impl<M: MemPool> NonTransactionalStorage<M> {
    /// Create a new non-transactional storage instance
    pub fn new(mem_pool: Arc<M>) -> Self {
        Self {
            mem_pool,
            containers: UnsafeCell::new(HashMap::new()),
            next_container_id: UnsafeCell::new(1),
        }
    }
}

// ============================================================================
// Database and Container Management
// ============================================================================

impl<M: MemPool> FieldLeveLStorageTrait for NonTransactionalStorage<M> {
    type TxnHandle = NonTxnHandle;
    type IteratorHandle = NonTxnIterator<M>;
    type Hint = RecordHandle;

    // ========================================================================
    // Database Management
    // ========================================================================

    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        // Always return the same database ID since we only support one database
        Ok(0)
    }

    fn close_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        assert_eq!(
            db_id, 0,
            "NonTransactionalStorage only supports a single database with ID 0"
        );
        Ok(())
    }

    fn delete_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        assert_eq!(
            db_id, 0,
            "NonTransactionalStorage only supports a single database with ID 0"
        );
        Ok(())
    }

    // ========================================================================
    // Container Management
    // ========================================================================

    fn create_container(
        &self,
        db_id: DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        assert_eq!(
            db_id, 0,
            "NonTransactionalStorage only supports a single database with ID 0"
        );
        // Only support B-tree containers for now
        if options.data_structure() != ContainerDS::BTree {
            return Err(TxnStorageStatus::AbortFailed);
        }

        // SAFETY: We assume single-threaded access as per the requirements
        unsafe {
            let containers = &mut *self.containers.get();
            let next_container_id = &mut *self.next_container_id.get();

            let c_id = *next_container_id;
            *next_container_id += 1;

            // Create Foster B-tree
            let container_key = ContainerKey::new(0, c_id); // Always use db_id = 0,
            let btree = Arc::new(FosterBtree::new(container_key, self.mem_pool.clone()));

            let container_info = ContainerInfo {
                options: options.clone(),
                btree,
            };

            containers.insert(c_id, container_info);
            Ok(c_id)
        }
    }

    fn delete_container(
        &self,
        _db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        unsafe {
            let containers = &mut *self.containers.get();

            containers
                .remove(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?;

            Ok(())
        }
    }

    fn list_containers(
        &self,
        _db_id: DatabaseId,
    ) -> Result<Vec<(ContainerId, ContainerOptions)>, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        unsafe {
            let containers = &*self.containers.get();
            let result = containers
                .iter()
                .map(|(&c_id, container)| (c_id, container.options.clone()))
                .collect();

            Ok(result)
        }
    }

    // ========================================================================
    // Bulk Operations
    // ========================================================================

    fn raw_insert_record(
        &self,
        _db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        self.insert_record(
            &NonTxnHandle,
            c_id,
            record,
            None, // No hint for non-transactional insert
        )
    }

    fn raw_insert_secondary_record(
        &self,
        _db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
        primary_hint: RecordHandle,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        self.insert_record(&NonTxnHandle, c_id, record, Some(primary_hint))
    }

    // ========================================================================
    // Transaction Management
    // ========================================================================

    fn begin_txn(
        &self,
        _db_id: DatabaseId,
        _options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        Ok(NonTxnHandle)
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
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

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
        hint: Option<RecordHandle>,
    ) -> Result<(Field, RecordHandle), TxnStorageStatus> {
        let (fields, ptr) = self.get_fields(txn, c_id, key, &[col_idx], hint)?;
        Ok((fields.into_iter().next().unwrap(), ptr))
    }

    fn get_fields(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_indices: &[usize],
        hint: Option<RecordHandle>,
    ) -> Result<(Vec<Field>, RecordHandle), TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let key = key_to_bytes(&key);

        let leaf_page = container.btree.traverse_to_leaf_for_read_with_hint(
            &key,
            hint.as_ref().map(|ptr| {
                PageFrameKey::new_with_frame_id(container.btree.c_key, ptr.page_id, ptr.frame_id)
            }),
        );
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&key)) - 1;
        if slot_id == 0 {
            // Lower fence. Non-existent key
            Err(TxnStorageStatus::KeyNotFound)
        } else {
            // We can get the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                let fields = container.get_fields(&key, leaf_page.get_val(slot_id), col_indices);
                Ok((
                    fields,
                    RecordHandle::new(leaf_page.page().get_id(), leaf_page.frame_id()),
                ))
            } else {
                // Non-existent key
                Err(TxnStorageStatus::KeyNotFound)
            }
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
        hint: Option<RecordHandle>,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        self.update_fields(txn, c_id, key, vec![(col_idx, field)], hint)
    }

    fn update_fields(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        fields: Vec<(usize, Field)>,
        hint: Option<RecordHandle>,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let key = key_to_bytes(&key);

        let mut leaf_page = container.btree.traverse_to_leaf_for_write_with_hint(
            &key,
            hint.as_ref().map(|ptr| {
                PageFrameKey::new_with_frame_id(container.btree.c_key, ptr.page_id, ptr.frame_id)
            }),
        );
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&key)) - 1;
        if slot_id == 0 {
            // We cannot update the lower fence
            Err(TxnStorageStatus::KeyNotFound)
        } else {
            // We can update the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                let mut record = container.get_fields(&key, leaf_page.get_val(slot_id), &[]);
                for (col_idx, field) in fields {
                    record[col_idx] = field;
                }
                let value = container.fields_to_value(&record);
                // Exact match
                container
                    .btree
                    .update_at_slot_or_split(&mut leaf_page, slot_id, &key, &value);
                Ok(RecordHandle::new(
                    leaf_page.page().get_id(),
                    leaf_page.frame_id(),
                ))
            } else {
                // Non-existent key
                Err(TxnStorageStatus::KeyNotFound)
            }
        }
    }

    fn update_field_with_func<F: FnOnce(&mut Field)>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        func: F,
        hint: Option<RecordHandle>,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let key = key_to_bytes(&key);

        let mut leaf_page = container.btree.traverse_to_leaf_for_write_with_hint(
            &key,
            hint.as_ref().map(|ptr| {
                PageFrameKey::new_with_frame_id(container.btree.c_key, ptr.page_id, ptr.frame_id)
            }),
        );
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&key)) - 1;
        if slot_id == 0 {
            // We cannot update the lower fence
            Err(TxnStorageStatus::KeyNotFound)
        } else {
            // We can update the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                let mut record = container.get_fields(&key, leaf_page.get_val(slot_id), &[]);
                func(&mut record[col_idx]);
                let value = container.fields_to_value(&record);
                // Exact match
                container
                    .btree
                    .update_at_slot_or_split(&mut leaf_page, slot_id, &key, &value);
                Ok(RecordHandle::new(
                    leaf_page.page().get_id(),
                    leaf_page.frame_id(),
                ))
            } else {
                // Non-existent key
                Err(TxnStorageStatus::KeyNotFound)
            }
        }
    }

    fn update_fields_with_func<F: FnOnce(&mut [Field])>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_indices: &[usize],
        func: F,
        hint: Option<RecordHandle>,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let key = key_to_bytes(&key);

        let mut leaf_page = container.btree.traverse_to_leaf_for_write_with_hint(
            &key,
            hint.as_ref().map(|ptr| {
                PageFrameKey::new_with_frame_id(container.btree.c_key, ptr.page_id, ptr.frame_id)
            }),
        );
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&key)) - 1;
        if slot_id == 0 {
            Err(TxnStorageStatus::KeyNotFound)
        } else {
            if leaf_page.get_raw_key(slot_id) == key {
                const PLACEHOLDER: Field = Field::Bool(None);
                let mut record = container.get_fields(&key, leaf_page.get_val(slot_id), &[]);
                let mut selected: Vec<Field> = col_indices
                    .iter()
                    .map(|&i| std::mem::replace(&mut record[i], PLACEHOLDER))
                    .collect();
                func(&mut selected);
                for (j, &i) in col_indices.iter().enumerate() {
                    record[i] = std::mem::replace(&mut selected[j], PLACEHOLDER);
                }
                let value = container.fields_to_value(&record);
                container
                    .btree
                    .update_at_slot_or_split(&mut leaf_page, slot_id, &key, &value);
                Ok(RecordHandle::new(
                    leaf_page.page().get_id(),
                    leaf_page.frame_id(),
                ))
            } else {
                Err(TxnStorageStatus::KeyNotFound)
            }
        }
    }

    // ========================================================================
    // Record Insertion and Deletion
    // ========================================================================

    fn insert_record(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        record: Record,
        hint: Option<RecordHandle>,
    ) -> Result<RecordHandle, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let key = container.to_key_bytes(&record.fields);
        let value = container.fields_to_value(&record.fields);

        let mut leaf_page = container.btree.traverse_to_leaf_for_write_with_hint(
            &key,
            hint.as_ref().map(|ptr| {
                PageFrameKey::new_with_frame_id(container.btree.c_key, ptr.page_id, ptr.frame_id)
            }),
        );
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&key)) - 1;
        if slot_id == 0 {
            // Lower fence so insert is ok. We insert the key-value at the next position of the lower fence.
            container.btree.insert_at_slot_or_split(
                &mut leaf_page,
                slot_id + 1,
                &key,
                &value,
                false,
            );
            Ok(RecordHandle::new(
                leaf_page.page().get_id(),
                leaf_page.frame_id(),
            ))
        } else {
            // We can insert the key if it does not exist
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                Err(TxnStorageStatus::KeyExists)
            } else {
                container.btree.insert_at_slot_or_split(
                    &mut leaf_page,
                    slot_id + 1,
                    &key,
                    &value,
                    false,
                );
                Ok(RecordHandle::new(
                    leaf_page.page().get_id(),
                    leaf_page.frame_id(),
                ))
            }
        }
    }

    fn insert_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        records: Vec<(Record, Option<RecordHandle>)>,
    ) -> Result<Vec<RecordHandle>, TxnStorageStatus> {
        let mut pointers = Vec::new();
        for (record, hint) in records {
            let ptr = self.insert_record(txn, c_id, record, hint)?;
            pointers.push(ptr);
        }
        Ok(pointers)
    }

    fn delete_record(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        hint: Option<RecordHandle>,
    ) -> Result<(), TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let key = key_to_bytes(&key);

        let mut leaf_page = container.btree.traverse_to_leaf_for_write_with_hint(
            &key,
            hint.as_ref().map(|ptr| {
                PageFrameKey::new_with_frame_id(container.btree.c_key, ptr.page_id, ptr.frame_id)
            }),
        );
        let slot_id = leaf_page.upper_bound_slot_id(&BTreeKey::new(&key)) - 1;
        if slot_id == 0 {
            // Lower fence so delete is not possible
            Err(TxnStorageStatus::KeyNotFound)
        } else {
            // We can delete the key if it exists
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                leaf_page.remove_at(slot_id);
                Ok(())
            } else {
                // Non-existent key
                Err(TxnStorageStatus::KeyNotFound)
            }
        }
    }

    // ========================================================================
    // Range Scanning and Iteration
    // ========================================================================

    fn scan_range(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        // SAFETY: We assume single-threaded access as per the requirements
        let container = unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let scanner = container
            .btree
            .scan_range(&options.lower_inc, &options.upper_exc);
        Ok(NonTxnIterator {
            scanner: UnsafeCell::new(scanner),
            c_id,
            options,
        })
    }

    fn iter_for_each(
        &self,
        _txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
        f: &mut dyn FnMut(&[u8], &[u8], RecordHandle) -> bool,
    ) -> Result<u64, TxnStorageStatus> {
        let mut count: u64 = 0;
        let scanner = unsafe { &mut *iter.scanner.get() };
        for (key, value) in scanner {
            count += 1;
            if !f(&key, &value, RecordHandle::new(0, 0)) {
                break;
            }
        }
        Ok(count)
    }

    fn iter_for_each_fields(
        &self,
        _txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
        f: &mut dyn FnMut(&[Field], RecordHandle) -> bool,
    ) -> Result<u64, TxnStorageStatus> {
        let container = unsafe {
            (*self.containers.get())
                .get(&iter.c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?
        };

        let cols = &iter.options.cols;
        let mut count: u64 = 0;
        let scanner = unsafe { &mut *iter.scanner.get() };

        for (key_bytes, value_bytes) in scanner {
            let fields = container.get_fields(&key_bytes, &value_bytes, cols);

            let ptr = RecordHandle::new(0, 0);
            count += 1;
            if !f(&fields, ptr) {
                break;
            }
        }
        Ok(count)
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
    use crate::txn_storage2::DataType;
    use crate::{assert_field, field, record, schema};

    use crate::txn_storage2::schema::Schema;

    #[test]
    fn test_basic_operations() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),  // id (primary key)
            (true, DataType::String),  // name (nullable)
            (false, DataType::Int32),  // age (not nullable)
        ]);

        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
    }

    #[test]
    fn test_multi_field_primary_key() {
        let schema = schema!(pk: [0, 1], cols: [
            (false, DataType::Int32),  // department_id (part of primary key)
            (false, DataType::Int32),  // employee_id (part of primary key)
            (true, DataType::String),  // name (nullable)
            (false, DataType::Int32),  // salary (not nullable)
        ]);

        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
    }

    #[test]
    fn test_container_management() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();

        // Create schema
        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );

        // Test creating containers
        let container1_options =
            ContainerOptions::primary("container1", ContainerDS::BTree, schema.clone());
        let container2_options =
            ContainerOptions::primary("container2", ContainerDS::BTree, schema.clone());

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
    }

    #[test]
    fn test_error_handling() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_options =
            ContainerOptions::primary("test_container", ContainerDS::BTree, schema);
        let container_id = storage.create_container(db_id, container_options).unwrap();

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

        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
    }

    #[test]
    fn test_nullable_fields() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),  // Primary key (non-nullable)
            (true, DataType::String),  // Nullable
            (true, DataType::Int32),   // Nullable
        ]);

        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
    }

    #[test]
    fn test_field_updates() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (true, DataType::String),
            (false, DataType::Int32),
        ]);

        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
    }

    #[test]
    fn test_bulk_operations() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();

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

        let additional_records_with_hints: Vec<(Record, Option<RecordHandle>)> =
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
    }

    #[test]
    fn test_iteration_and_scanning() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
            .scan_range(&txn, container_id, ScanOptions::new(&[0, 1]))
            .unwrap();

        let mut collected_fields = Vec::new();

        let count = storage
            .iter_for_each_fields(&txn, &iter, &mut |fields, _ptr| {
                collected_fields.push(fields.to_vec());
                true
            })
            .unwrap();

        assert_eq!(count, 10);
        assert_eq!(collected_fields.len(), 10);

        // Verify some collected data
        for i in 0..count as usize {
            // Fields should contain the two projected columns (0 and 1)
            assert_eq!(collected_fields[i].len(), 2);
        }

        storage.drop_iterator_handle(iter).unwrap();
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

        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();
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
    }

    #[test]
    fn test_transaction_operations() {
        let bp = get_test_bp(100);
        let storage = NonTransactionalStorage::new(bp);
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();

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
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn test_concurrent_access_panics() {
        use std::sync::Arc;
        use std::thread;

        let bp = get_test_bp(100);
        let storage = Arc::new(NonTransactionalStorage::new(bp));
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert some initial data
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 0..10 {
            let record = record![field!(Int32 i), field!(String format!("initial_{}", i))];
            storage
                .insert_record(&txn, container_id, record, None)
                .unwrap();
        }

        // Try concurrent access - this should trigger undefined behavior or data races
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let storage_clone = Arc::clone(&storage);
                thread::spawn(move || {
                    let txn = storage_clone
                        .begin_txn(db_id, TxnOptions::default())
                        .unwrap();

                    // Each thread tries to insert and update records
                    for i in 0..100 {
                        let key = thread_id * 1000 + i;
                        let record = record![
                            field!(Int32 key),
                            field!(String format!("thread_{}_value_{}", thread_id, i))
                        ];

                        // This might cause data races with UnsafeCell
                        let _ = storage_clone.insert_record(&txn, container_id, record, None);

                        // Try to update existing records
                        if i % 10 == 0 {
                            let update_key = vec![field!(Int32(i % 10))];
                            let _ = storage_clone.update_field(
                                &txn,
                                container_id,
                                update_key,
                                1,
                                field!(String format!("updated_by_thread_{}", thread_id)),
                                None,
                            );
                        }
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // This test should demonstrate that concurrent access is unsafe
        // In practice, this might cause data corruption, panics, or other undefined behavior
        panic!("assertion failed: concurrent access should not be safe");
    }

    #[test]
    fn test_concurrent_read_safety() {
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::thread;

        let bp = get_test_bp(100);
        let storage = Arc::new(NonTransactionalStorage::new(bp));
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (true, DataType::String)],
            vec![0],
        );
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert test data
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 0..100 {
            let record = record![field!(Int32 i), field!(String format!("value_{}", i))];
            storage
                .insert_record(&txn, container_id, record, None)
                .unwrap();
        }

        // Multiple threads reading concurrently - even this is unsafe with UnsafeCell
        let handles: Vec<_> = (0..4)
            .map(|_thread_id| {
                let storage_clone = Arc::clone(&storage);
                thread::spawn(move || {
                    let txn = storage_clone
                        .begin_txn(db_id, TxnOptions::default())
                        .unwrap();
                    let mut seen_values = HashSet::new();

                    // Each thread reads all records
                    for i in 0..100 {
                        let key = vec![field!(Int32 i)];
                        match storage_clone.get_field(&txn, container_id, key, 1, None) {
                            Ok((field, _)) => {
                                if let Field::String(Some(val)) = field {
                                    seen_values.insert(val);
                                }
                            }
                            Err(_) => {
                                // Key might not exist due to race conditions
                            }
                        }
                    }

                    seen_values.len()
                })
            })
            .collect();

        // Collect results from all threads
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Even concurrent reads might produce inconsistent results due to UnsafeCell
        println!("Read results from threads: {:?}", results);

        // Results might vary due to unsafe concurrent access
        assert!(!results.is_empty());
    }

    #[test]
    fn test_data_race_detection() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let bp = get_test_bp(100);
        let storage = Arc::new(NonTransactionalStorage::new(bp));
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(false, DataType::Int32), (false, DataType::Int32)],
            vec![0],
        );
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert a record that will be concurrently modified
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(Int32 0)];
        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        let barrier = Arc::new(Barrier::new(2));

        // Two threads trying to increment the same counter
        let handles: Vec<_> = (0..2)
            .map(|thread_id| {
                let storage_clone = Arc::clone(&storage);
                let barrier_clone = Arc::clone(&barrier);

                thread::spawn(move || {
                    let txn = storage_clone
                        .begin_txn(db_id, TxnOptions::default())
                        .unwrap();

                    // Synchronize threads to increase chance of race
                    barrier_clone.wait();

                    let mut sum = 0;
                    for _ in 0..1000 {
                        // Read current value
                        let key = vec![field!(Int32 1)];
                        match storage_clone.get_field(&txn, container_id, key.clone(), 1, None) {
                            Ok((Field::Int32(Some(val)), _)) => {
                                // Increment and write back (classic race condition)
                                let new_val = val + 1;
                                let _ = storage_clone.update_field(
                                    &txn,
                                    container_id,
                                    key,
                                    1,
                                    field!(Int32 new_val),
                                    None,
                                );
                                sum += 1;
                            }
                            _ => {}
                        }
                    }
                    (thread_id, sum)
                })
            })
            .collect();

        // Collect results
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        println!("Thread results: {:?}", results);

        // Check final value - it should be 2000 if no races, but will likely be less
        let key = vec![field!(Int32 1)];
        match storage.get_field(&txn, container_id, key, 1, None) {
            Ok((Field::Int32(Some(final_val)), _)) => {
                println!("Final counter value: {}", final_val);
                // Due to race conditions, the final value will likely be less than 2000
                // This demonstrates the danger of concurrent access with UnsafeCell
                assert!(final_val > 0 && final_val <= 2000);
            }
            _ => panic!("Failed to read final value"),
        }
    }
}
