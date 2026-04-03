use crate::txn_storage2::field::DataType;

/// A named column definition — the user-facing description of a field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }
}

/// Describes *what data* a table holds — column names and types.
/// No mention of indexes or physical access paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalSchema {
    columns: Vec<ColumnDef>,
}

impl LogicalSchema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        LogicalSchema { columns }
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Look up a column index by name. Returns None if not found.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    // --- Serialization ---

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Column count
        bytes.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in &self.columns {
            // Name: length-prefixed
            let name_bytes = col.name.as_bytes();
            bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(name_bytes);
            // DataType: 1 byte
            bytes.push(col.data_type.as_byte());
            // Nullable: 1 byte
            bytes.push(if col.nullable { 1 } else { 0 });
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let col_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let name_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            let name = std::str::from_utf8(&bytes[offset..offset + name_len])
                .expect("Invalid UTF-8 in column name")
                .to_string();
            offset += name_len;

            let data_type = DataType::from_bytes(&bytes[offset..offset + 1]);
            offset += 1;

            let nullable = bytes[offset] != 0;
            offset += 1;

            columns.push(ColumnDef {
                name,
                data_type,
                nullable,
            });
        }

        LogicalSchema { columns }
    }
}

/// Convenience macro for building a LogicalSchema.
///
/// Usage:
/// ```ignore
/// logical_schema! {
///     "ts" => Uint64,
///     "id" => Uint32,
///     "name" => String nullable,
/// }
/// ```
#[macro_export]
macro_rules! logical_schema {
    ($($name:literal => $dtype:ident $($nullable:ident)?),* $(,)?) => {{
        use $crate::txn_storage2::logical_schema::ColumnDef;
        use $crate::txn_storage2::field::DataType;
        $crate::txn_storage2::logical_schema::LogicalSchema::new(vec![
            $(
                ColumnDef::new(
                    $name,
                    DataType::$dtype,
                    logical_schema!(@nullable $($nullable)?),
                )
            ),*
        ])
    }};
    (@nullable nullable) => { true };
    (@nullable) => { false };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_schema_basics() {
        let schema = LogicalSchema::new(vec![
            ColumnDef::new("id", DataType::Uint32, false),
            ColumnDef::new("name", DataType::String, true),
            ColumnDef::new("balance", DataType::Float64, false),
        ]);

        assert_eq!(schema.num_columns(), 3);
        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("missing"), None);
        assert_eq!(schema.column("name").unwrap().nullable, true);
    }

    #[test]
    fn test_logical_schema_serde() {
        let schema = LogicalSchema::new(vec![
            ColumnDef::new("ts", DataType::Uint64, false),
            ColumnDef::new("user_id", DataType::Uint32, false),
            ColumnDef::new("reason", DataType::String, true),
        ]);
        let bytes = schema.to_bytes();
        let decoded = LogicalSchema::from_bytes(&bytes);
        assert_eq!(schema, decoded);
    }

    #[test]
    fn test_logical_schema_macro() {
        let schema = logical_schema! {
            "id" => Uint32,
            "name" => String nullable,
        };
        assert_eq!(schema.num_columns(), 2);
        assert_eq!(schema.columns()[0].nullable, false);
        assert_eq!(schema.columns()[1].nullable, true);
    }
}
