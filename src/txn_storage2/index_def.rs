use crate::txn_storage2::{field::DataType, logical_schema::LogicalSchema, schema::Schema};

/// What role an index plays.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexKind {
    /// Clustered: the B-tree stores the full record. One per table.
    Primary,
    /// Non-clustered: the B-tree stores secondary key fields + primary key fields + Pointer.
    Secondary,
}

impl IndexKind {
    pub fn to_byte(&self) -> u8 {
        match self {
            IndexKind::Primary => 0,
            IndexKind::Secondary => 1,
        }
    }

    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => IndexKind::Primary,
            1 => IndexKind::Secondary,
            _ => panic!("Invalid IndexKind byte: {}", b),
        }
    }
}

/// Defines a single index over a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDef {
    pub name: String,
    pub kind: IndexKind,
    /// Column indices (into LogicalSchema) that form this index's sort key.
    pub key_columns: Vec<usize>,
    /// If true, duplicate key values are disallowed.
    pub unique: bool,
}

impl IndexDef {
    pub fn primary(name: &str, key_columns: Vec<usize>) -> Self {
        IndexDef {
            name: name.to_string(),
            kind: IndexKind::Primary,
            key_columns,
            unique: true, // primary is always unique
        }
    }

    pub fn secondary(name: &str, key_columns: Vec<usize>, unique: bool) -> Self {
        IndexDef {
            name: name.to_string(),
            kind: IndexKind::Secondary,
            key_columns,
            unique,
        }
    }

    /// Build the low-level `Schema` that the underlying container needs.
    ///
    /// - **Primary**: schema has all table columns; key_indices = self.key_columns.
    /// - **Secondary**: schema has [secondary_key_cols, primary_key_cols, Pointer];
    ///   key_indices covers all columns except Pointer (so the B-tree key is
    ///   secondary_key + primary_key, ensuring uniqueness even for non-unique indexes).
    pub fn to_container_schema(
        &self,
        logical: &LogicalSchema,
        primary_key_columns: &[usize],
    ) -> Schema {
        match self.kind {
            IndexKind::Primary => {
                let cols: Vec<(bool, DataType)> = logical
                    .columns()
                    .iter()
                    .map(|c| (c.nullable, c.data_type))
                    .collect();
                Schema::with_primary_key(cols, self.key_columns.clone())
            }
            IndexKind::Secondary => {
                let mut cols: Vec<(bool, DataType)> = Vec::new();
                let mut key_indices: Vec<usize> = Vec::new();

                // Secondary key columns
                for &col_idx in &self.key_columns {
                    let col = &logical.columns()[col_idx];
                    key_indices.push(cols.len());
                    cols.push((col.nullable, col.data_type));
                }

                // Primary key columns (for uniqueness + back-lookup)
                // Skip any PK column already in the secondary key
                for &pk_col in primary_key_columns {
                    if !self.key_columns.contains(&pk_col) {
                        let col = &logical.columns()[pk_col];
                        key_indices.push(cols.len());
                        cols.push((col.nullable, col.data_type));
                    }
                }

                // Pointer column (not part of the key)
                cols.push((false, DataType::Pointer));

                Schema::with_primary_key(cols, key_indices)
            }
        }
    }

    // --- Serialization ---

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Name
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(name_bytes);
        // Kind
        bytes.push(self.kind.to_byte());
        // Unique
        bytes.push(if self.unique { 1 } else { 0 });
        // Key columns
        bytes.extend_from_slice(&(self.key_columns.len() as u32).to_ne_bytes());
        for &col in &self.key_columns {
            bytes.extend_from_slice(&(col as u32).to_ne_bytes());
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
        let mut offset = 0;

        let name_len = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let name = std::str::from_utf8(&bytes[offset..offset + name_len])
            .expect("Invalid UTF-8")
            .to_string();
        offset += name_len;

        let kind = IndexKind::from_byte(bytes[offset]);
        offset += 1;

        let unique = bytes[offset] != 0;
        offset += 1;

        let key_count = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut key_columns = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            let col = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            key_columns.push(col);
            offset += 4;
        }

        (
            IndexDef {
                name,
                kind,
                key_columns,
                unique,
            },
            offset,
        )
    }
}

/// Groups a primary index with zero or more secondary indexes — the physical schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalSchema {
    pub primary_index: IndexDef,
    pub secondary_indexes: Vec<IndexDef>,
}

impl PhysicalSchema {
    pub fn new(primary: IndexDef, secondaries: Vec<IndexDef>) -> Self {
        assert_eq!(
            primary.kind,
            IndexKind::Primary,
            "First index must be Primary"
        );
        for s in &secondaries {
            assert_eq!(
                s.kind,
                IndexKind::Secondary,
                "Non-primary indexes must be Secondary"
            );
        }
        PhysicalSchema {
            primary_index: primary,
            secondary_indexes: secondaries,
        }
    }

    /// Iterate over all indexes (primary first, then secondaries).
    pub fn all_indexes(&self) -> impl Iterator<Item = &IndexDef> {
        std::iter::once(&self.primary_index).chain(self.secondary_indexes.iter())
    }

    /// Find an index by name.
    pub fn find_index(&self, name: &str) -> Option<&IndexDef> {
        self.all_indexes().find(|idx| idx.name == name)
    }

    // --- Serialization ---

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Primary
        let primary_bytes = self.primary_index.to_bytes();
        bytes.extend_from_slice(&(primary_bytes.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(&primary_bytes);
        // Secondary count + each
        bytes.extend_from_slice(&(self.secondary_indexes.len() as u32).to_ne_bytes());
        for idx in &self.secondary_indexes {
            let idx_bytes = idx.to_bytes();
            bytes.extend_from_slice(&(idx_bytes.len() as u32).to_ne_bytes());
            bytes.extend_from_slice(&idx_bytes);
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let primary_len =
            u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let (primary_index, _) = IndexDef::from_bytes(&bytes[offset..offset + primary_len]);
        offset += primary_len;

        let sec_count = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut secondary_indexes = Vec::with_capacity(sec_count);
        for _ in 0..sec_count {
            let idx_len =
                u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let (idx, _) = IndexDef::from_bytes(&bytes[offset..offset + idx_len]);
            secondary_indexes.push(idx);
            offset += idx_len;
        }

        PhysicalSchema {
            primary_index,
            secondary_indexes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn_storage2::logical_schema::{ColumnDef, LogicalSchema};

    fn sample_logical() -> LogicalSchema {
        LogicalSchema::new(vec![
            ColumnDef::new("w_id", DataType::Uint16, false),
            ColumnDef::new("d_id", DataType::Uint8, false),
            ColumnDef::new("c_id", DataType::Uint32, false),
            ColumnDef::new("c_last", DataType::String, false),
            ColumnDef::new("c_balance", DataType::Float64, false),
        ])
    }

    #[test]
    fn test_primary_container_schema() {
        let logical = sample_logical();
        let primary = IndexDef::primary("pk", vec![0, 1, 2]);
        let schema = primary.to_container_schema(&logical, &[0, 1, 2]);

        // Should have all 5 columns, key on [0,1,2]
        assert_eq!(schema.cols().len(), 5);
        assert_eq!(schema.key_indices(), &[0, 1, 2]);
    }

    #[test]
    fn test_secondary_container_schema() {
        let logical = sample_logical();
        let primary_key_cols = vec![0, 1, 2]; // w_id, d_id, c_id
        let secondary = IndexDef::secondary("by_last", vec![0, 1, 3], false); // w_id, d_id, c_last

        let schema = secondary.to_container_schema(&logical, &primary_key_cols);

        // Secondary key: w_id(0), d_id(1), c_last(2)
        // PK cols not in secondary key: c_id -> index 3
        // Pointer -> index 4
        // Key indices: [0, 1, 2, 3] (everything except Pointer)
        assert_eq!(schema.cols().len(), 5);
        assert_eq!(schema.key_indices(), &[0, 1, 2, 3]);
        // Last column should be Pointer
        assert_eq!(schema.cols()[4].1, DataType::Pointer);
    }

    #[test]
    fn test_physical_schema_serde() {
        let primary = IndexDef::primary("pk", vec![0, 1]);
        let sec = IndexDef::secondary("by_name", vec![2], false);
        let physical = PhysicalSchema::new(primary.clone(), vec![sec.clone()]);

        let bytes = physical.to_bytes();
        let decoded = PhysicalSchema::from_bytes(&bytes);
        assert_eq!(physical, decoded);
    }

    #[test]
    fn test_find_index() {
        let primary = IndexDef::primary("pk", vec![0]);
        let sec1 = IndexDef::secondary("by_user", vec![1], false);
        let sec2 = IndexDef::secondary("by_reason", vec![2], true);
        let physical = PhysicalSchema::new(primary, vec![sec1, sec2]);

        assert!(physical.find_index("pk").is_some());
        assert!(physical.find_index("by_user").is_some());
        assert!(physical.find_index("by_reason").is_some());
        assert!(physical.find_index("missing").is_none());
    }
}
