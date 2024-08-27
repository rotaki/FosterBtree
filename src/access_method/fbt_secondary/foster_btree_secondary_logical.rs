use std::sync::Arc;

use crate::{
    bp::MemPool,
    prelude::{AccessMethodError, FosterBtree, UniqueKeyIndex},
};

pub struct FbtSecondaryLogical<T: MemPool> {
    pub primary: Arc<FosterBtree<T>>,
    pub secondary: Arc<FosterBtree<T>>,
}

impl<T: MemPool> FbtSecondaryLogical<T> {
    pub fn new(primary: Arc<FosterBtree<T>>, secondary: Arc<FosterBtree<T>>) -> Self {
        Self { primary, secondary }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let val = self.secondary.get(key)?;
        self.primary.get(&val)
    }
}
