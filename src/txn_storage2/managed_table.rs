use crate::bp::prelude::DatabaseId;
use crate::txn_storage2::{
    catalog::TableMeta,
    field::{Field, Record},
    field_level_storage_trait::{FieldLeveLStorageTrait, ScanOptions, TxnStorageStatus},
};

/// A handle to a table with automatic index maintenance.
///
/// All DML operations (insert, delete, update) go through this type.
/// Secondary indexes are maintained automatically.
pub struct ManagedTable<'a, S: FieldLeveLStorageTrait> {
    storage: &'a S,
    db_id: DatabaseId,
    meta: &'a TableMeta,
}

impl<'a, S: FieldLeveLStorageTrait> ManagedTable<'a, S> {
    pub fn new(storage: &'a S, db_id: DatabaseId, meta: &'a TableMeta) -> Self {
        ManagedTable {
            storage,
            db_id,
            meta,
        }
    }

    pub fn meta(&self) -> &TableMeta {
        self.meta
    }

    pub fn storage(&self) -> &S {
        self.storage
    }

    pub fn db_id(&self) -> DatabaseId {
        self.db_id
    }

    /// Extract primary key fields from a full record.
    fn extract_pk(&self, record: &[Field]) -> Vec<Field> {
        self.meta
            .physical_schema
            .primary_index
            .key_columns
            .iter()
            .map(|&i| record[i].clone())
            .collect()
    }

    /// Build a secondary index record from a full table record and a hint (RecordHandle).
    ///
    /// Layout: [secondary_key_fields..., pk_fields_not_in_sec_key..., Pointer]
    fn build_secondary_record(
        &self,
        sec_idx_pos: usize,
        full_record: &[Field],
        hint: S::Hint,
    ) -> Record {
        let sec_def = &self.meta.physical_schema.secondary_indexes[sec_idx_pos];
        let pk_cols = &self.meta.physical_schema.primary_index.key_columns;

        let mut rec = Record::new();

        // Secondary key columns
        for &col_idx in &sec_def.key_columns {
            rec.push(full_record[col_idx].clone());
        }

        // Primary key columns not already in the secondary key
        for &pk_col in pk_cols {
            if !sec_def.key_columns.contains(&pk_col) {
                rec.push(full_record[pk_col].clone());
            }
        }

        // Pointer to primary record
        // We need to convert the Hint to a RecordHandle for the Pointer field.
        // The Hint type for transactional storage IS RecordHandle.
        // We store it via Field::Pointer.
        rec.push(Self::hint_to_pointer_field(hint));

        rec
    }

    /// Extract the key fields of a secondary index record (for deletion).
    /// The key is: [secondary_key_fields..., pk_fields_not_in_sec_key...]
    fn build_secondary_key(&self, sec_idx_pos: usize, full_record: &[Field]) -> Vec<Field> {
        let sec_def = &self.meta.physical_schema.secondary_indexes[sec_idx_pos];
        let pk_cols = &self.meta.physical_schema.primary_index.key_columns;

        let mut key = Vec::new();

        for &col_idx in &sec_def.key_columns {
            key.push(full_record[col_idx].clone());
        }

        for &pk_col in pk_cols {
            if !sec_def.key_columns.contains(&pk_col) {
                key.push(full_record[pk_col].clone());
            }
        }

        key
    }

    /// Convert a Hint to Field::Pointer.
    ///
    /// This works because for the transactional storage, Hint = RecordHandle.
    /// For other backends where Hint might be (), we store a null pointer.
    fn hint_to_pointer_field(hint: S::Hint) -> Field {
        let size = std::mem::size_of::<S::Hint>();
        if size == std::mem::size_of::<crate::txn_storage2::field::RecordHandle>() {
            // RecordHandle is 8 bytes (page_id: u32, frame_id: u32)
            let mut bytes = [0u8; 8];
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &hint as *const S::Hint as *const u8,
                    bytes.as_mut_ptr(),
                    8,
                );
            }
            let page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let frame_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
            Field::Pointer(Some(crate::txn_storage2::field::RecordHandle::new(
                page_id, frame_id,
            )))
        } else {
            Field::Pointer(None)
        }
    }

    // =========================================================================
    // DML Operations
    // =========================================================================

    /// Insert a record. Automatically inserts into all secondary indexes.
    ///
    /// `fields` must be in the same order as the LogicalSchema columns.
    pub fn insert(
        &self,
        txn: &S::TxnHandle,
        fields: Vec<Field>,
    ) -> Result<S::Hint, TxnStorageStatus> {
        // Build primary record
        let mut primary_record = Record::new();
        for f in &fields {
            primary_record.push(f.clone());
        }

        // Insert into primary index
        let hint = self.storage.insert_record(
            txn,
            self.meta.primary_container_id,
            primary_record,
            None,
        )?;

        // Insert into each secondary index
        for (i, _sec_def) in self
            .meta
            .physical_schema
            .secondary_indexes
            .iter()
            .enumerate()
        {
            let sec_record = self.build_secondary_record(i, &fields, hint);
            let sec_cid = self.meta.secondary_container_ids[i];
            self.storage.insert_record(txn, sec_cid, sec_record, None)?;
        }

        Ok(hint)
    }

    /// Insert a record without transaction support. Used for bulk loading.
    pub fn raw_insert(&self, fields: Vec<Field>) -> Result<S::Hint, TxnStorageStatus> {
        let mut primary_record = Record::new();
        for f in &fields {
            primary_record.push(f.clone());
        }

        let hint = self.storage.raw_insert_record(
            self.db_id,
            self.meta.primary_container_id,
            primary_record,
        )?;

        for (i, _sec_def) in self
            .meta
            .physical_schema
            .secondary_indexes
            .iter()
            .enumerate()
        {
            let sec_record = self.build_secondary_record(i, &fields, hint);
            let sec_cid = self.meta.secondary_container_ids[i];
            self.storage
                .raw_insert_record(self.db_id, sec_cid, sec_record)?;
        }

        Ok(hint)
    }

    /// Delete a record by primary key. Automatically removes from all secondary indexes.
    ///
    /// Reads the full record first (needed to compute secondary keys).
    pub fn delete(
        &self,
        txn: &S::TxnHandle,
        primary_key: Vec<Field>,
    ) -> Result<(), TxnStorageStatus> {
        // Read the full record to compute secondary keys
        let all_cols: Vec<usize> = (0..self.meta.logical_schema.num_columns()).collect();
        let (full_fields, _hint) = self.storage.get_fields(
            txn,
            self.meta.primary_container_id,
            primary_key.clone(),
            &all_cols,
            None,
        )?;

        // Delete from each secondary index
        for (i, _sec_def) in self
            .meta
            .physical_schema
            .secondary_indexes
            .iter()
            .enumerate()
        {
            let sec_key = self.build_secondary_key(i, &full_fields);
            let sec_cid = self.meta.secondary_container_ids[i];
            self.storage.delete_record(txn, sec_cid, sec_key, None)?;
        }

        // Delete from primary
        self.storage
            .delete_record(txn, self.meta.primary_container_id, primary_key, None)?;

        Ok(())
    }

    /// Update specific fields of a record identified by primary key.
    ///
    /// If any updated field is part of a secondary index key, that secondary
    /// index entry is deleted and re-inserted.
    pub fn update_fields(
        &self,
        txn: &S::TxnHandle,
        primary_key: Vec<Field>,
        updates: &[(usize, Field)],
    ) -> Result<S::Hint, TxnStorageStatus> {
        // If there are secondary indexes, we need to check if any updated column
        // affects a secondary index key.
        if !self.meta.physical_schema.secondary_indexes.is_empty() {
            // Read the full record before update (for old secondary keys)
            let all_cols: Vec<usize> = (0..self.meta.logical_schema.num_columns()).collect();
            let (old_fields, _) = self.storage.get_fields(
                txn,
                self.meta.primary_container_id,
                primary_key.clone(),
                &all_cols,
                None,
            )?;

            // Apply updates to get new record
            let mut new_fields = old_fields.clone();
            for (col_idx, new_val) in updates {
                new_fields[*col_idx] = new_val.clone();
            }

            // For each secondary index, check if any key column changed
            for (i, sec_def) in self
                .meta
                .physical_schema
                .secondary_indexes
                .iter()
                .enumerate()
            {
                let affected = sec_def
                    .key_columns
                    .iter()
                    .any(|kc| updates.iter().any(|(uc, _)| uc == kc));

                if affected {
                    let sec_cid = self.meta.secondary_container_ids[i];
                    // Delete old secondary entry
                    let old_sec_key = self.build_secondary_key(i, &old_fields);
                    self.storage
                        .delete_record(txn, sec_cid, old_sec_key, None)?;

                    // After primary update, re-insert with new key
                    // (we'll insert after the primary update below)
                }
            }

            // Update primary record
            let hint = self.storage.update_fields(
                txn,
                self.meta.primary_container_id,
                primary_key,
                updates.to_vec(),
                None,
            )?;

            // Re-insert affected secondary index entries
            for (i, sec_def) in self
                .meta
                .physical_schema
                .secondary_indexes
                .iter()
                .enumerate()
            {
                let affected = sec_def
                    .key_columns
                    .iter()
                    .any(|kc| updates.iter().any(|(uc, _)| uc == kc));

                if affected {
                    let sec_cid = self.meta.secondary_container_ids[i];
                    let new_sec_record = self.build_secondary_record(i, &new_fields, hint);
                    self.storage
                        .insert_record(txn, sec_cid, new_sec_record, None)?;
                }
            }

            Ok(hint)
        } else {
            // No secondary indexes — just update primary
            self.storage.update_fields(
                txn,
                self.meta.primary_container_id,
                primary_key,
                updates.to_vec(),
                None,
            )
        }
    }

    /// Get specific fields by primary key.
    pub fn get_by_pk(
        &self,
        txn: &S::TxnHandle,
        primary_key: Vec<Field>,
        col_idxs: &[usize],
        hint: Option<S::Hint>,
    ) -> Result<(Vec<Field>, S::Hint), TxnStorageStatus> {
        self.storage.get_fields(
            txn,
            self.meta.primary_container_id,
            primary_key,
            col_idxs,
            hint,
        )
    }

    /// Get a single field by primary key.
    pub fn get_field(
        &self,
        txn: &S::TxnHandle,
        primary_key: Vec<Field>,
        col_idx: usize,
        hint: Option<S::Hint>,
    ) -> Result<(Field, S::Hint), TxnStorageStatus> {
        self.storage.get_field(
            txn,
            self.meta.primary_container_id,
            primary_key,
            col_idx,
            hint,
        )
    }

    /// Update a single field by primary key. Handles secondary index maintenance.
    pub fn update_field(
        &self,
        txn: &S::TxnHandle,
        primary_key: Vec<Field>,
        col_idx: usize,
        value: Field,
        hint: Option<S::Hint>,
    ) -> Result<S::Hint, TxnStorageStatus> {
        // Check if any secondary index is affected
        let sec_affected = self
            .meta
            .physical_schema
            .secondary_indexes
            .iter()
            .any(|sec| sec.key_columns.contains(&col_idx));

        if sec_affected {
            self.update_fields(txn, primary_key, &[(col_idx, value)])
        } else {
            // Fast path: no secondary index maintenance needed
            self.storage.update_field(
                txn,
                self.meta.primary_container_id,
                primary_key,
                col_idx,
                value,
                hint,
            )
        }
    }

    /// Scan using a specific named index.
    ///
    /// This is the "explicit access-path selection" — the caller picks which index.
    /// Returns the low-level iterator handle for use with `iter_for_each_fields`.
    pub fn scan_index(
        &self,
        txn: &S::TxnHandle,
        index_name: &str,
        options: ScanOptions,
    ) -> Result<S::IteratorHandle, TxnStorageStatus> {
        let c_id = self
            .meta
            .container_for_index(index_name)
            .ok_or(TxnStorageStatus::ContainerNotFound)?;
        self.storage.scan_range(txn, c_id, options)
    }

    /// Scan the primary index.
    pub fn scan_primary(
        &self,
        txn: &S::TxnHandle,
        options: ScanOptions,
    ) -> Result<S::IteratorHandle, TxnStorageStatus> {
        self.storage
            .scan_range(txn, self.meta.primary_container_id, options)
    }

    /// Get the container ID for a named index (useful for low-level access).
    pub fn container_id(&self, index_name: &str) -> Option<u16> {
        self.meta.container_for_index(index_name)
    }
}
