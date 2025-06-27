use crate::txn_storage2::field::DataType;

/// Create a schema with column definitions
#[macro_export]
macro_rules! schema {
    (pk: [$($pk:expr),*], cols: [$(($nullable:expr, $dtype:expr)),* $(,)?]) => {
        $crate::txn_storage2::schema::Schema::with_primary_key(
            vec![$(($nullable, $dtype)),*],
            vec![$($pk),*],
        )
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    cols: Vec<(bool, DataType)>, // (is nullable, data type)
    key_indices: Vec<usize>,     // Indices of fields that form the key
}

impl Schema {
    pub fn new(cols: Vec<(bool, DataType)>) -> Self {
        Schema {
            cols,
            key_indices: vec![0], // Default to first field as key
        }
    }

    pub fn cols(&self) -> &[(bool, DataType)] {
        &self.cols
    }

    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }

    pub fn with_primary_key(cols: Vec<(bool, DataType)>, primary_key_indices: Vec<usize>) -> Self {
        Schema {
            cols,
            key_indices: primary_key_indices,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Serialize column count
        bytes.extend_from_slice(&(self.cols.len() as u32).to_ne_bytes());

        // Serialize columns
        for (is_nullable, data_type) in &self.cols {
            bytes.push(if *is_nullable { 1 } else { 0 });
            bytes.extend_from_slice(&data_type.to_bytes());
        }

        // Serialize key indices count
        bytes.extend_from_slice(&(self.key_indices.len() as u32).to_ne_bytes());

        // Serialize key indices
        for &idx in &self.key_indices {
            bytes.extend_from_slice(&(idx as u32).to_ne_bytes());
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;

        // Deserialize column count
        let col_count = u32::from_ne_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        // Deserialize columns
        let mut cols = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let is_nullable = bytes[offset] != 0;
            offset += 1;
            let data_type = DataType::from_bytes(&bytes[offset..]);
            cols.push((is_nullable, data_type));
            offset += 1;
        }

        // Deserialize key indices count
        let key_count = u32::from_ne_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        // Deserialize key indices
        let mut key_indices = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            let idx = u32::from_ne_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            key_indices.push(idx);
            offset += 4;
        }

        Schema { cols, key_indices }
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Schema {{")?;
        writeln!(f, "  Key Indices: {:?}", self.key_indices)?;
        writeln!(f, "  Columns:")?;

        for (idx, (is_nullable, data_type)) in self.cols.iter().enumerate() {
            let key_marker = if self.key_indices.contains(&idx) {
                " [KEY]"
            } else {
                ""
            };
            let nullable_str = if *is_nullable { "NULL" } else { "NOT NULL" };
            writeln!(
                f,
                "    {}: {:?} {}{}",
                idx, data_type, nullable_str, key_marker
            )?;
        }

        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::txn_storage2::field::DataType;

    #[test]
    fn test_schema_to_from_bytes() {
        let schema = Schema {
            cols: vec![(true, DataType::Int8), (false, DataType::String)],
            key_indices: vec![0],
        };
        let bytes = schema.to_bytes();
        let decoded_schema = Schema::from_bytes(&bytes);
        assert_eq!(schema, decoded_schema);
    }
}
