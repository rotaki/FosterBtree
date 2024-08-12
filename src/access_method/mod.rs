use std::sync::Arc;

use crate::bp::MemPoolStatus;

pub mod append_only_store;
pub mod fbt;
pub mod hash_fbt;
pub mod hashindex;

#[derive(Debug, PartialEq)]
pub enum AccessMethodError {
    KeyNotFound,
    KeyDuplicate,
    KeyNotInPageRange, // For Btree
    PageReadLatchFailed,
    PageWriteLatchFailed,
    RecordTooLarge,
    MemPoolStatus(MemPoolStatus),
    Other(String),
}

pub mod prelude {
    pub use super::append_only_store::prelude::*;
    pub use super::fbt::prelude::*;
    pub use super::hash_fbt::prelude::*;
    pub use super::hashindex::prelude::*;
    pub use super::AccessMethodError;
    pub use super::{NonUniqueKeyIndex, OrderedUniqueKeyIndex, UniqueKeyIndex};
}

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
    fn scan_with_filter(
        self: &Arc<Self>,
        filter: Box<dyn FnMut(&[u8], &[u8]) -> bool>,
    ) -> Self::Iter;
}

pub trait OrderedUniqueKeyIndex: UniqueKeyIndex {
    type RangeIter: Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn scan_range(self: &Arc<Self>, start_key: &[u8], end_key: &[u8]) -> Self::RangeIter;
    fn scan_range_with_filter(
        self: &Arc<Self>,
        start_key: &[u8],
        end_key: &[u8],
        filter: Box<dyn FnMut(&[u8], &[u8]) -> bool>,
    ) -> Self::RangeIter;
}

pub trait NonUniqueKeyIndex {
    type Iter: Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError>;
    fn scan(self: &Arc<Self>) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)>;
    fn scan_key(self: &Arc<Self>, key: &[u8]) -> Self::Iter;
}
