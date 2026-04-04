use crate::bp::prelude::ContainerId;
use crate::txn_storage2::{
    catalog::TableMeta,
    field::Field,
    field_level_storage_trait::{FieldLeveLStorageTrait, ScanOptions, TxnStorageStatus},
    index_def::{IndexDef, IndexKind},
    managed_table::ManagedTable,
    to_normalized_key,
};

/// Compute the successor of a field value for exclusive upper-bound prefix scans.
/// E.g., successor of Uint32(1) is Uint32(2).
fn successor_field(field: &Field) -> Field {
    match field {
        Field::Int8(Some(v)) => Field::Int8(Some(v.wrapping_add(1))),
        Field::Int16(Some(v)) => Field::Int16(Some(v.wrapping_add(1))),
        Field::Int32(Some(v)) => Field::Int32(Some(v.wrapping_add(1))),
        Field::Int64(Some(v)) => Field::Int64(Some(v.wrapping_add(1))),
        Field::Uint8(Some(v)) => Field::Uint8(Some(v.wrapping_add(1))),
        Field::Uint16(Some(v)) => Field::Uint16(Some(v.wrapping_add(1))),
        Field::Uint32(Some(v)) => Field::Uint32(Some(v.wrapping_add(1))),
        Field::Uint64(Some(v)) => Field::Uint64(Some(v.wrapping_add(1))),
        Field::String(Some(s)) => {
            // Append a null byte to get the next string after all strings with this prefix
            let mut next = s.clone();
            next.push('\0');
            Field::String(Some(next))
        }
        other => {
            // For types where increment doesn't make sense, this is a best-effort.
            // In practice, prefix scans are most useful on integer and string keys.
            panic!("successor_field not supported for {:?}", other);
        }
    }
}

/// Fluent query builder for scans with explicit index selection.
///
/// # Example
/// ```ignore
/// let results = QueryBuilder::new(&table, &txn)
///     .using("by_user")
///     .prefix_eq("user_id", field!(Uint32 4))
///     .lower_bound("ts", field!(Uint64 100))
///     .upper_bound("ts", field!(Uint64 200))
///     .select(&["ts", "reason"])
///     .execute()?;
/// ```
pub struct QueryBuilder<'a, S: FieldLeveLStorageTrait> {
    table: &'a ManagedTable<'a, S>,
    txn: &'a S::TxnHandle,
    index_name: Option<String>,
    /// Prefix equalities: (column_name, exact_value)
    prefix: Vec<(String, Field)>,
    /// Range lower bound (inclusive): (column_name, value)
    range_lower: Option<(String, Field)>,
    /// Range upper bound (exclusive): (column_name, value)
    range_upper: Option<(String, Field)>,
    /// Columns to return (empty = all from the target)
    select_cols: Vec<String>,
}

impl<'a, S: FieldLeveLStorageTrait> QueryBuilder<'a, S> {
    pub fn new(table: &'a ManagedTable<'a, S>, txn: &'a S::TxnHandle) -> Self {
        QueryBuilder {
            table,
            txn,
            index_name: None,
            prefix: Vec::new(),
            range_lower: None,
            range_upper: None,
            select_cols: Vec::new(),
        }
    }

    /// Select which index to use. If not called, uses the primary index.
    pub fn using(mut self, index_name: &str) -> Self {
        self.index_name = Some(index_name.to_string());
        self
    }

    /// Fix a prefix column to an exact value.
    /// Must be called in index key order (first key column, then second, etc.).
    pub fn prefix_eq(mut self, column: &str, value: Field) -> Self {
        self.prefix.push((column.to_string(), value));
        self
    }

    /// Set inclusive lower bound on a range column (the column after the prefix columns).
    pub fn lower_bound(mut self, column: &str, value: Field) -> Self {
        self.range_lower = Some((column.to_string(), value));
        self
    }

    /// Set exclusive upper bound on a range column.
    pub fn upper_bound(mut self, column: &str, value: Field) -> Self {
        self.range_upper = Some((column.to_string(), value));
        self
    }

    /// Limit which columns are returned. Column names refer to the logical schema.
    /// If not called, returns all columns that the scan produces.
    pub fn select(mut self, cols: &[&str]) -> Self {
        self.select_cols = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Build and execute the scan, collecting all results into a Vec.
    pub fn execute(self) -> Result<Vec<Vec<Field>>, TxnStorageStatus> {
        let meta = self.table.meta();

        // Resolve index
        let (index_def, container_id) = self.resolve_index(meta)?;

        // Build the scan key fields and scan options
        let options = self.build_scan_options(index_def, meta, container_id)?;

        // Execute scan
        let iter = self
            .table
            .storage()
            .scan_range(self.txn, container_id, options)?;

        let mut results = Vec::new();
        let mut lookup_err: Option<TxnStorageStatus> = None;
        self.table.storage().iter_for_each_fields(
            self.txn,
            &iter,
            &mut |key_fields, value_fields, _hint| {
                let row = match index_def.kind {
                    IndexKind::Primary => {
                        // value_fields are the requested columns from the primary record
                        value_fields.to_vec()
                    }
                    IndexKind::Secondary => {
                        // For secondary index scans, we need to do a back-lookup
                        // to get the full record from the primary index.
                        match self.secondary_back_lookup(index_def, meta, value_fields, key_fields)
                        {
                            Ok(fields) => fields,
                            Err(e) => {
                                lookup_err = Some(e);
                                return false;
                            }
                        }
                    }
                };
                results.push(row);
                true
            },
        )?;

        self.table.storage().drop_iterator_handle(iter)?;
        if let Some(e) = lookup_err {
            return Err(e);
        }
        Ok(results)
    }

    /// Execute the scan and return a buffered iterator over the results.
    pub fn execute_iter(self) -> Result<QueryIter<'a, S>, TxnStorageStatus> {
        let table = self.table;
        let rows = self.execute()?;
        Ok(QueryIter {
            table,
            rows,
            pos: 0,
        })
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    fn resolve_index<'m>(
        &self,
        meta: &'m TableMeta,
    ) -> Result<(&'m IndexDef, ContainerId), TxnStorageStatus> {
        match &self.index_name {
            None => Ok((
                &meta.physical_schema.primary_index,
                meta.primary_container_id,
            )),
            Some(name) => {
                if meta.physical_schema.primary_index.name == *name {
                    return Ok((
                        &meta.physical_schema.primary_index,
                        meta.primary_container_id,
                    ));
                }
                for (i, idx) in meta.physical_schema.secondary_indexes.iter().enumerate() {
                    if idx.name == *name {
                        return Ok((idx, meta.secondary_container_ids[i]));
                    }
                }
                Err(TxnStorageStatus::ContainerNotFound)
            }
        }
    }

    fn build_scan_options(
        &self,
        index_def: &IndexDef,
        meta: &TableMeta,
        _container_id: ContainerId,
    ) -> Result<ScanOptions, TxnStorageStatus> {
        let logical = &meta.logical_schema;

        // Determine which columns in the *container schema* correspond to our prefix/range.
        // For primary indexes, container column indices == logical column indices.
        // For secondary indexes, we need to map through the index_def.

        // Build lower and upper bound field vectors for the scan key.
        // The scan key is the B-tree key of the container.
        let pk_cols = &meta.physical_schema.primary_index.key_columns;

        // Map column names to positions in the index key.
        // For a secondary index, the container key is:
        //   [sec_key_cols..., pk_cols_not_in_sec...]
        // We need to figure out which position each named column maps to.

        // Build the key column mapping: logical_col_idx -> container_key_position
        let key_col_order: Vec<usize> = match index_def.kind {
            IndexKind::Primary => index_def.key_columns.clone(),
            IndexKind::Secondary => {
                let mut order = Vec::new();
                for &col_idx in &index_def.key_columns {
                    order.push(col_idx);
                }
                for &pk_col in pk_cols {
                    if !index_def.key_columns.contains(&pk_col) {
                        order.push(pk_col);
                    }
                }
                order
            }
        };

        // Resolve prefix column names to index key positions and field values
        let mut prefix_fields: Vec<Field> = Vec::new();

        for (col_name, value) in &self.prefix {
            let logical_idx = logical
                .column_index(col_name)
                .ok_or(TxnStorageStatus::ContainerNotFound)?;

            // Verify this column is at the expected position in the key
            let expected_pos = prefix_fields.len();
            if expected_pos >= key_col_order.len() || key_col_order[expected_pos] != logical_idx {
                return Err(TxnStorageStatus::ContainerNotFound);
            }

            prefix_fields.push(value.clone());
        }

        // Build lower and upper bounds.
        // For a prefix-only scan (no range bounds), lower = prefix, upper = successor(prefix).
        // For prefix + range: lower = prefix ++ [range_lower], upper = prefix ++ [range_upper].
        let mut lower_fields = prefix_fields.clone();
        let mut upper_fields: Vec<Field>;

        if let Some((col_name, value)) = &self.range_lower {
            let _logical_idx = logical
                .column_index(col_name)
                .ok_or(TxnStorageStatus::ContainerNotFound)?;
            lower_fields.push(value.clone());
        }

        if let Some((_, value)) = &self.range_upper {
            upper_fields = prefix_fields.clone();
            upper_fields.push(value.clone());
        } else if !prefix_fields.is_empty() {
            // No explicit upper bound: compute successor of the prefix.
            // Increment the last prefix field to get exclusive upper bound.
            upper_fields = prefix_fields.clone();
            let last = upper_fields.last_mut().unwrap();
            *last = successor_field(last);
        } else {
            // No prefix and no upper bound — full scan
            upper_fields = Vec::new();
        }

        // Determine which columns to return from the container.
        // For primary: return the user-requested logical columns.
        // For secondary: return all columns (we'll do back-lookup).
        let return_cols: Vec<usize> = match index_def.kind {
            IndexKind::Primary => {
                if self.select_cols.is_empty() {
                    (0..logical.num_columns()).collect()
                } else {
                    self.select_cols
                        .iter()
                        .map(|name| logical.column_index(name).expect("Unknown column name"))
                        .collect()
                }
            }
            IndexKind::Secondary => {
                // Return all columns from the secondary index container
                // (we need the PK fields for back-lookup)
                let container_schema = index_def.to_container_schema(logical, pk_cols);
                (0..container_schema.cols().len()).collect()
            }
        };

        // Build ScanOptions with normalized key bounds
        let mut options = ScanOptions::new(&return_cols);

        if !lower_fields.is_empty() {
            let lower_indices: Vec<(usize, bool, bool)> = lower_fields
                .iter()
                .enumerate()
                .map(|(i, _)| (i, true, false))
                .collect();
            options.lower_inc = to_normalized_key(&lower_fields, &lower_indices);
        }

        if !upper_fields.is_empty() {
            let upper_indices: Vec<(usize, bool, bool)> = upper_fields
                .iter()
                .enumerate()
                .map(|(i, _)| (i, true, false))
                .collect();
            options.upper_exc = to_normalized_key(&upper_fields, &upper_indices);
        }

        Ok(options)
    }

    /// For a secondary index scan result, do the back-lookup to the primary index.
    fn secondary_back_lookup(
        &self,
        index_def: &IndexDef,
        meta: &TableMeta,
        value_fields: &[Field],
        _key_fields: &[Field],
    ) -> Result<Vec<Field>, TxnStorageStatus> {
        let logical = &meta.logical_schema;
        let pk_cols = &meta.physical_schema.primary_index.key_columns;

        // The secondary container record is: [sec_key_cols..., pk_extra_cols..., Pointer]
        // The value_fields from the scan are the cols we asked for (all of them).
        // Extract PK fields for back-lookup.

        // Figure out where PK fields are in the secondary record
        let sec_key_len = index_def.key_columns.len();
        let mut pk_fields = Vec::with_capacity(pk_cols.len());

        for &pk_col in pk_cols {
            if let Some(pos) = index_def.key_columns.iter().position(|&c| c == pk_col) {
                // PK column is in the secondary key
                pk_fields.push(value_fields[pos].clone());
            } else {
                // PK column is after the secondary key columns
                // Find its position: it's after all sec_key columns, in the order of pk_cols
                let mut extra_pos = sec_key_len;
                for &other_pk in pk_cols {
                    if other_pk == pk_col {
                        break;
                    }
                    if !index_def.key_columns.contains(&other_pk) {
                        extra_pos += 1;
                    }
                }
                pk_fields.push(value_fields[extra_pos].clone());
            }
        }

        // Extract the Pointer hint from the last field
        let pointer_field = &value_fields[value_fields.len() - 1];
        let hint = match pointer_field {
            Field::Pointer(Some(ptr)) => {
                // Convert RecordPointer back to Hint
                // This is safe because for TransactionalStorage, Hint = RecordPointer
                if std::mem::size_of::<S::Hint>() == 8 {
                    let mut bytes = [0u8; 8];
                    bytes[0..4].copy_from_slice(&ptr.page_id.to_le_bytes());
                    bytes[4..8].copy_from_slice(&ptr.frame_id.to_le_bytes());
                    Some(unsafe { std::mem::transmute_copy(&bytes) })
                } else {
                    None
                }
            }
            _ => None,
        };

        // Determine which columns to fetch from primary
        let fetch_cols: Vec<usize> = if self.select_cols.is_empty() {
            (0..logical.num_columns()).collect()
        } else {
            self.select_cols
                .iter()
                .map(|name| logical.column_index(name).expect("Unknown column"))
                .collect()
        };

        let (fields, _) = self.table.storage().get_fields(
            self.txn,
            meta.primary_container_id,
            pk_fields,
            &fetch_cols,
            hint,
        )?;

        Ok(fields)
    }
}

/// Buffered iterator for query results.  Results are eagerly collected via
/// `iter_for_each_fields` and then yielded one at a time from the buffer.
pub struct QueryIter<'a, S: FieldLeveLStorageTrait> {
    table: &'a ManagedTable<'a, S>,
    rows: Vec<Vec<Field>>,
    pos: usize,
}

impl<'a, S: FieldLeveLStorageTrait> QueryIter<'a, S> {
    /// Get the next result row.
    pub fn next(&mut self) -> Result<Option<Vec<Field>>, TxnStorageStatus> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = std::mem::take(&mut self.rows[self.pos]);
        self.pos += 1;
        Ok(Some(row))
    }

    /// No-op for API compatibility — resources were already released after
    /// the eager scan completed.
    pub fn finish(self) -> Result<(), TxnStorageStatus> {
        let _ = self.table;
        Ok(())
    }
}
