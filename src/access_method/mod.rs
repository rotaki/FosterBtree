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
}
