use std::sync::Arc;

use crate::{
    bp::MemPool,
    prelude::{AccessMethodError, FosterBtree, UniqueKeyIndex},
};

pub struct FbtSecondaryLogical<T: MemPool> {
    primary_fbt: Arc<FosterBtree<T>>,
    secondary_fbt: Arc<FosterBtree<T>>,
    mempool: Arc<T>,
}

impl<T: MemPool> FbtSecondaryLogical<T> {
    pub fn new(
        primary_fbt: Arc<FosterBtree<T>>,
        secondary_fbt: Arc<FosterBtree<T>>,
        mempool: Arc<T>,
    ) -> Self {
        Self {
            primary_fbt,
            secondary_fbt,
            mempool,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let val = self.secondary_fbt.get(key)?;
        self.primary_fbt.get(&val)
    }
}
