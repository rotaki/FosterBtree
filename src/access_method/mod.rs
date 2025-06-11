use std::sync::Arc;

use crate::bp::MemPoolStatus;

pub mod append_only_store;
pub mod chain;
pub mod fbt;
pub mod field_level_index_trait;
pub mod fixed_size_store;
pub mod hash_fbt;
pub mod hashindex;
pub mod heapstore;

#[derive(Debug, PartialEq)]
pub enum AccessMethodError {
    KeyNotFound,
    KeyDuplicate,
    NotEnoughMemory,
    PageReadLatchFailed,
    PageWriteLatchFailed,
    RecordTooLarge,
    OutOfSpace, // For ReadOptimizedPage
    OutOfSpaceForUpdate(Vec<u8>),
    Other(String),
}

impl From<MemPoolStatus> for AccessMethodError {
    fn from(status: MemPoolStatus) -> AccessMethodError {
        match status {
            MemPoolStatus::CannotEvictPage => AccessMethodError::NotEnoughMemory,
            MemPoolStatus::FrameReadLatchGrantFailed => AccessMethodError::PageReadLatchFailed,
            MemPoolStatus::FrameWriteLatchGrantFailed => AccessMethodError::PageWriteLatchFailed,
            e => {
                panic!("Unexpected MemPoolStatus: {:?}", e)
            }
        }
    }
}

pub mod prelude {
    pub use super::chain::prelude::*;
    pub use super::fbt::prelude::*;
    pub use super::field_level_index_trait::*;
    pub use super::hash_fbt::prelude::*;
    pub use super::hashindex::prelude::*;
    pub use super::heapstore::prelude::*;
    pub use super::AccessMethodError;
    pub use super::{NonUniqueKeyIndex, OrderedUniqueKeyIndex, UniqueKeyIndex};
}

pub type FilterType = Arc<dyn Fn(&[u8], &[u8]) -> bool + Send + Sync>;

pub trait UniqueKeyIndex {
    type Iter: Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError>;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError>;
    fn delete(&self, key: &[u8]) -> Result<(), AccessMethodError>;
    fn update(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError>;
    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError>;
    fn upsert_with_merge(
        &self,
        key: &[u8],
        value: &[u8],
        merge_fn: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), AccessMethodError>;
    fn scan(self: &Arc<Self>) -> Self::Iter;
    fn scan_with_filter(self: &Arc<Self>, filter: FilterType) -> Self::Iter;
}

pub trait OrderedUniqueKeyIndex: UniqueKeyIndex {
    type RangeIter: Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn scan_range(self: &Arc<Self>, start_key: &[u8], end_key: &[u8]) -> Self::RangeIter;
    fn scan_range_with_filter(
        self: &Arc<Self>,
        start_key: &[u8],
        end_key: &[u8],
        filter: FilterType,
    ) -> Self::RangeIter;
}

pub trait NonUniqueKeyIndex {
    type RangeIter: Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn append(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError>;
    fn scan(self: &Arc<Self>) -> Self::RangeIter;
    fn scan_key(self: &Arc<Self>, key: &[u8]) -> Self::RangeIter;
}
