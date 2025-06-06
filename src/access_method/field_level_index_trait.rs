use super::AccessMethodError;

pub enum LogicalField {}

pub struct Schema {}

pub trait FieldLevelIndex {
    type Iter: Iterator<Item = (Vec<u8>, Vec<LogicalField>)>;
    fn get_schema(&self) -> &Schema;

    fn insert(&self, key: &[u8], fields: Vec<LogicalField>) -> Result<(), AccessMethodError>;

    fn delete(&self, key: &[u8]) -> Result<(), AccessMethodError>;

    fn get_field(&self, key: &[u8], col_idx: usize) -> Result<LogicalField, AccessMethodError>;

    fn get_fields(
        &self,
        key: &[u8],
        col_idxs: &[usize],
    ) -> Result<Vec<LogicalField>, AccessMethodError>;

    fn update_field(
        &self,
        key: &[u8],
        col_idx: usize,
        field: LogicalField,
    ) -> Result<(), AccessMethodError>;

    fn update_fields(
        &self,
        key: &[u8],
        fields: Vec<(usize, LogicalField)>,
    ) -> Result<(), AccessMethodError>;

    fn scan(&self, col_idxs: &[usize]) -> Self::Iter;

    fn scan_range(&self, start_key: &[u8], end_key: &[u8], col_idxs: &[usize]) -> Self::Iter;
}
