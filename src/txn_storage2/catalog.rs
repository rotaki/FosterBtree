use std::collections::HashMap;
use std::sync::Arc;

use crate::bp::prelude::{ContainerId, DatabaseId};
use crate::txn_storage2::{
    field::{DataType, Field, Record},
    field_level_storage_trait::{
        ContainerDS, ContainerOptions, FieldLeveLStorageTrait, TxnStorageStatus,
    },
    index_def::PhysicalSchema,
    logical_schema::LogicalSchema,
    schema::Schema,
};

/// Runtime representation of a registered table.
#[derive(Clone, Debug)]
pub struct TableMeta {
    pub name: String,
    pub logical_schema: LogicalSchema,
    pub physical_schema: PhysicalSchema,
    /// ContainerId of the primary index container.
    pub primary_container_id: ContainerId,
    /// ContainerId for each secondary index, same order as physical_schema.secondary_indexes.
    pub secondary_container_ids: Vec<ContainerId>,
}

impl TableMeta {
    /// Get the container ID for a named index.
    pub fn container_for_index(&self, index_name: &str) -> Option<ContainerId> {
        if self.physical_schema.primary_index.name == index_name {
            return Some(self.primary_container_id);
        }
        for (i, idx) in self.physical_schema.secondary_indexes.iter().enumerate() {
            if idx.name == index_name {
                return Some(self.secondary_container_ids[i]);
            }
        }
        None
    }

    /// Serialize table metadata to bytes for catalog storage.
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Table name
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(name_bytes);

        // Logical schema
        let ls_bytes = self.logical_schema.to_bytes();
        bytes.extend_from_slice(&(ls_bytes.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(&ls_bytes);

        // Physical schema
        let ps_bytes = self.physical_schema.to_bytes();
        bytes.extend_from_slice(&(ps_bytes.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(&ps_bytes);

        // Primary container ID
        bytes.extend_from_slice(&self.primary_container_id.to_ne_bytes());

        // Secondary container IDs
        bytes.extend_from_slice(&(self.secondary_container_ids.len() as u32).to_ne_bytes());
        for &cid in &self.secondary_container_ids {
            bytes.extend_from_slice(&cid.to_ne_bytes());
        }

        bytes
    }

    /// Deserialize table metadata from bytes.
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;

        // Table name
        let name_len =
            u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let name = std::str::from_utf8(&bytes[offset..offset + name_len])
            .expect("Invalid UTF-8")
            .to_string();
        offset += name_len;

        // Logical schema
        let ls_len =
            u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let logical_schema = LogicalSchema::from_bytes(&bytes[offset..offset + ls_len]);
        offset += ls_len;

        // Physical schema
        let ps_len =
            u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let physical_schema = PhysicalSchema::from_bytes(&bytes[offset..offset + ps_len]);
        offset += ps_len;

        // Primary container ID
        let primary_container_id = ContainerId::from_ne_bytes(
            bytes[offset..offset + std::mem::size_of::<ContainerId>()]
                .try_into()
                .unwrap(),
        );
        offset += std::mem::size_of::<ContainerId>();

        // Secondary container IDs
        let sec_count =
            u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let mut secondary_container_ids = Vec::with_capacity(sec_count);
        for _ in 0..sec_count {
            let cid = ContainerId::from_ne_bytes(
                bytes[offset..offset + std::mem::size_of::<ContainerId>()]
                    .try_into()
                    .unwrap(),
            );
            secondary_container_ids.push(cid);
            offset += std::mem::size_of::<ContainerId>();
        }

        TableMeta {
            name,
            logical_schema,
            physical_schema,
            primary_container_id,
            secondary_container_ids,
        }
    }
}

/// The catalog manages table metadata, backed by the storage layer.
///
/// The catalog itself is stored in a special container: a B-tree keyed by table name.
/// Each entry stores the serialized `TableMeta`.
pub struct Catalog<S: FieldLeveLStorageTrait> {
    storage: Arc<S>,
    db_id: DatabaseId,
    /// ContainerId of the catalog's own metadata container.
    catalog_container_id: ContainerId,
    /// In-memory cache: table_name -> TableMeta.
    tables: HashMap<String, TableMeta>,
}

impl<S: FieldLeveLStorageTrait> Catalog<S> {
    /// Open or create the catalog.
    ///
    /// Looks for an existing `__catalog__` container. If not found, creates one.
    /// Then loads all table metadata into memory.
    pub fn open(storage: Arc<S>, db_id: DatabaseId) -> Result<Self, TxnStorageStatus> {
        // Try to find existing catalog container
        let containers = storage.list_containers(db_id)?;
        let catalog_cid = containers
            .iter()
            .find(|(_, opts)| opts.name() == "__catalog__")
            .map(|(cid, _)| *cid);

        let catalog_container_id = match catalog_cid {
            Some(cid) => cid,
            None => {
                // Create the catalog container.
                // Key is a String (table name), value is VarBytes (serialized TableMeta).
                let schema = Schema::with_primary_key(
                    vec![
                        (false, DataType::String),   // table name (key)
                        (false, DataType::VarBytes),  // serialized TableMeta
                    ],
                    vec![0],
                );
                let opts =
                    ContainerOptions::new("__catalog__", ContainerDS::BTree, schema);
                storage.create_container(db_id, opts)?
            }
        };

        let mut catalog = Catalog {
            storage,
            db_id,
            catalog_container_id,
            tables: HashMap::new(),
        };

        // Load existing table metadata
        catalog.load_all()?;

        Ok(catalog)
    }

    /// Load all table metadata from the catalog container into memory.
    fn load_all(&mut self) -> Result<(), TxnStorageStatus> {
        self.tables.clear();

        let txn = self
            .storage
            .begin_txn(self.db_id, Default::default())?;

        let scan = self.storage.scan_range(
            &txn,
            self.catalog_container_id,
            crate::txn_storage2::ScanOptions::new(&[0, 1]),
        )?;

        loop {
            match self.storage.iter_next(&txn, &scan)? {
                Some((_key_fields, value_fields, _hint)) => {
                    // value_fields[0] = table name (String), value_fields[1] = serialized meta (VarBytes)
                    let meta_bytes = match &value_fields[1] {
                        Field::VarBytes(Some(b)) => b.clone(),
                        _ => panic!("Catalog entry has invalid meta field"),
                    };
                    let meta = TableMeta::from_bytes(&meta_bytes);
                    self.tables.insert(meta.name.clone(), meta);
                }
                None => break,
            }
        }

        self.storage.drop_iterator_handle(scan)?;
        self.storage.commit_txn(&txn, false)?;
        self.storage.drop_txn(txn)?;

        Ok(())
    }

    /// Register a new table: creates all containers (primary + secondaries),
    /// stores metadata in the catalog container.
    pub fn create_table(
        &mut self,
        name: &str,
        logical: LogicalSchema,
        physical: PhysicalSchema,
    ) -> Result<&TableMeta, TxnStorageStatus> {
        if self.tables.contains_key(name) {
            return Err(TxnStorageStatus::ContainerExists);
        }

        let pk_cols = &physical.primary_index.key_columns;

        // Create primary container
        let primary_schema = physical
            .primary_index
            .to_container_schema(&logical, pk_cols);
        let primary_opts = ContainerOptions::new(
            &format!("{}__pk__{}", name, physical.primary_index.name),
            ContainerDS::BTree,
            primary_schema,
        );
        let primary_cid = self.storage.create_container(self.db_id, primary_opts)?;

        // Create secondary containers
        let mut secondary_cids = Vec::with_capacity(physical.secondary_indexes.len());
        for sec_idx in &physical.secondary_indexes {
            let sec_schema = sec_idx.to_container_schema(&logical, pk_cols);
            let sec_opts = ContainerOptions::new(
                &format!("{}__idx__{}", name, sec_idx.name),
                ContainerDS::BTree,
                sec_schema,
            );
            let sec_cid = self.storage.create_container(self.db_id, sec_opts)?;
            secondary_cids.push(sec_cid);
        }

        let meta = TableMeta {
            name: name.to_string(),
            logical_schema: logical,
            physical_schema: physical,
            primary_container_id: primary_cid,
            secondary_container_ids: secondary_cids,
        };

        // Persist to catalog container
        let meta_bytes = meta.to_bytes();
        let mut record = Record::new();
        record.push(Field::String(Some(name.to_string())));
        record.push(Field::VarBytes(Some(meta_bytes)));
        self.storage
            .raw_insert_record(self.db_id, self.catalog_container_id, record)?;

        self.tables.insert(name.to_string(), meta);
        Ok(self.tables.get(name).unwrap())
    }

    /// Look up a table by name.
    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name)
    }

    /// List all registered table names.
    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Drop a table and all its containers.
    pub fn drop_table(&mut self, name: &str) -> Result<(), TxnStorageStatus> {
        let meta = match self.tables.remove(name) {
            Some(m) => m,
            None => return Err(TxnStorageStatus::ContainerNotFound),
        };

        // Delete all index containers
        self.storage
            .delete_container(self.db_id, meta.primary_container_id)?;
        for &sec_cid in &meta.secondary_container_ids {
            self.storage.delete_container(self.db_id, sec_cid)?;
        }

        // Remove from catalog container
        let txn = self
            .storage
            .begin_txn(self.db_id, Default::default())?;
        self.storage.delete_record(
            &txn,
            self.catalog_container_id,
            vec![Field::String(Some(name.to_string()))],
            None,
        )?;
        self.storage.commit_txn(&txn, false)?;
        self.storage.drop_txn(txn)?;

        Ok(())
    }

    /// Get a reference to the underlying storage.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get the database ID.
    pub fn db_id(&self) -> DatabaseId {
        self.db_id
    }
}
