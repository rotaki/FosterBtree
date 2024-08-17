use crate::bp::{FrameWriteGuard, MemPoolStatus};

pub mod append_only_store;
pub mod chain;
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
    OutOfSpace, // For ReadOptimizedPage
    OutOfSpaceForUpdate(Vec<u8>),
    Other(String),
}

pub mod prelude {
    pub use super::append_only_store::prelude::*;
    pub use super::fbt::prelude::*;
    pub use super::hash_fbt::prelude::*;
    pub use super::hashindex::prelude::*;
}
