use std::{
    cell::UnsafeCell,
    collections::HashSet,
    sync::{Arc, Mutex, RwLock},
};

use super::{
    ContainerDS, ContainerOptions, DBOptions, ScanOptions, TxnOptions, TxnStorageStatus,
    TxnStorageTrait,
};
use crate::{
    access_method::fbt::FosterBtreeRangeScanner,
    bp::prelude::{ContainerId, DatabaseId},
    prelude::{ContainerKey, FosterBtree},
};
use crate::{
    access_method::prelude::{AppendOnlyStore, AppendOnlyStoreScanner},
    access_method::UniqueKeyIndex,
    bp::MemPool,
};

pub enum Storage<M: MemPool> {
    HashMap(),
    BTreeMap(Arc<FosterBtree<M>>),
    AppendOnly(Arc<AppendOnlyStore<M>>),
}

unsafe impl<M: MemPool> Sync for Storage<M> {}

impl<M: MemPool> Storage<M> {
    fn new(db_id: DatabaseId, c_id: ContainerId, c_type: ContainerDS, bp: Arc<M>) -> Self {
        match c_type {
            ContainerDS::Hash => {
                unimplemented!("Hash container not implemented")
            }
            ContainerDS::BTree => Storage::BTreeMap(Arc::new(FosterBtree::<M>::new(
                ContainerKey::new(db_id, c_id),
                bp,
            ))),
            ContainerDS::AppendOnly => Storage::AppendOnly(Arc::new(AppendOnlyStore::<M>::new(
                ContainerKey::new(db_id, c_id),
                bp,
            ))),
        }
    }

    fn load(db_id: DatabaseId, c_id: ContainerId, c_type: ContainerDS, bp: Arc<M>) -> Self {
        match c_type {
            ContainerDS::Hash => {
                unimplemented!("Hash container not implemented")
            }
            ContainerDS::BTree => Storage::BTreeMap(Arc::new(FosterBtree::<M>::load(
                ContainerKey::new(db_id, c_id),
                bp,
                0,
            ))),
            ContainerDS::AppendOnly => Storage::AppendOnly(Arc::new(AppendOnlyStore::<M>::load(
                ContainerKey::new(db_id, c_id),
                bp,
                0,
            ))),
        }
    }

    fn clear(&self) {
        unimplemented!("clear not implemented")
    }

    fn insert(&self, key: Vec<u8>, val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.insert(&key, &val)?,
            Storage::AppendOnly(v) => v.append(&key, &val)?,
        };
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, TxnStorageStatus> {
        let result = match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.get(key)?,
            Storage::AppendOnly(v) => {
                unimplemented!("get by key is not supported for append only container")
            }
        };
        Ok(result)
    }

    fn update(&self, key: &[u8], val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.update(key, &val)?,
            Storage::AppendOnly(v) => {
                unimplemented!("update by key is not supported for append only container")
            }
        };
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<(), TxnStorageStatus> {
        match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.delete(key)?,
            Storage::AppendOnly(v) => {
                unimplemented!("remove by key is not supported for append only container")
            }
        };
        Ok(())
    }

    /*
    fn iter(self: &Arc<Self>) -> OnDiskIterator<M> {
        match self.as_ref() {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => OnDiskIterator::btree(b.scan()),
            Storage::AppendOnly(v) => OnDiskIterator::vec(v.scan()),
        }
    }
    */

    fn num_values(&self) -> usize {
        match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.num_kvs(),
            Storage::AppendOnly(v) => v.num_kvs(),
        }
    }
}

pub struct OnDiskSiloStorage<T: MemPool> {
    bp: Arc<T>,
    metadata: Arc<FosterBtree<T>>,
    containers: UnsafeCell<Vec<Arc<Storage<T>>>>,
}
