use append_only_store::AppendOnlyStoreError;
use fbt::TreeStatus;
use prelude::PagedHashMapError;

pub mod append_only_store;
pub mod fbt;
pub mod hashindex;

pub enum AccessMethodError {
    HashIdx(PagedHashMapError),
    BTreeIdx(TreeStatus),
    AppendOnlyStore(AppendOnlyStoreError),
}

impl From<PagedHashMapError> for AccessMethodError {
    fn from(err: PagedHashMapError) -> Self {
        AccessMethodError::HashIdx(err)
    }
}

impl From<TreeStatus> for AccessMethodError {
    fn from(err: TreeStatus) -> Self {
        AccessMethodError::BTreeIdx(err)
    }
}

impl From<AppendOnlyStoreError> for AccessMethodError {
    fn from(err: AppendOnlyStoreError) -> Self {
        AccessMethodError::AppendOnlyStore(err)
    }
}

pub mod prelude {
    pub use super::append_only_store::prelude::*;
    pub use super::fbt::prelude::*;
    pub use super::hashindex::prelude::*;
}
