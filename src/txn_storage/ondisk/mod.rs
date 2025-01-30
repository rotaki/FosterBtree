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
            Storage::BTreeMap(b) => b.insert(&key, &val)?,
            Storage::AppendOnly(v) => v.append(&key, &val)?,
        };
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, TxnStorageStatus> {
        let result = match self {
            Storage::BTreeMap(b) => b.get(key)?,
            Storage::AppendOnly(_v) => {
                unimplemented!("get by key is not supported for append only container")
            }
        };
        Ok(result)
    }

    fn update(&self, key: &[u8], val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        match self {
            Storage::BTreeMap(b) => b.update(key, &val)?,
            Storage::AppendOnly(_v) => {
                unimplemented!("update by key is not supported for append only container")
            }
        };
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<(), TxnStorageStatus> {
        match self {
            Storage::BTreeMap(b) => b.delete(key)?,
            Storage::AppendOnly(_v) => {
                unimplemented!("remove by key is not supported for append only container")
            }
        };
        Ok(())
    }

    fn iter(self: &Arc<Self>) -> OnDiskIterator<M> {
        match self.as_ref() {
            Storage::BTreeMap(b) => OnDiskIterator::btree(b.scan()),
            Storage::AppendOnly(v) => OnDiskIterator::vec(v.scan()),
        }
    }

    fn num_values(&self) -> usize {
        match self {
            Storage::BTreeMap(b) => b.num_kvs(),
            Storage::AppendOnly(v) => v.num_kvs(),
        }
    }
}

pub enum OnDiskIterator<M: MemPool> {
    // Storage and the iterator
    Hash(),
    BTree(Mutex<FosterBtreeRangeScanner<M>>),
    Vec(Mutex<AppendOnlyStoreScanner<M>>),
}

impl<M: MemPool> OnDiskIterator<M> {
    fn btree(iter: FosterBtreeRangeScanner<M>) -> Self {
        OnDiskIterator::BTree(Mutex::new(iter))
    }

    fn vec(iter: AppendOnlyStoreScanner<M>) -> Self {
        OnDiskIterator::Vec(Mutex::new(iter))
    }

    fn next(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            OnDiskIterator::Hash() => {
                unimplemented!("Hash container not implemented")
            }
            OnDiskIterator::BTree(iter) => iter.lock().unwrap().next(),
            OnDiskIterator::Vec(iter) => iter.lock().unwrap().next(),
        }
    }
}

/// Assumptions of OnDiskStorage:
/// 1. Creation and deletion of the database is not thread-safe. This means, you can't create
///    or delete a database while other threads are accessing the database.
/// 2. Creation and deletion of a container is thread-safe with respect to other containers.
///    However, deletion of a container is not thread-safe with respect to other threads accessing
///    the same container that is being deleted. You have to make sure that no other threads are
///    accessing the container while you are deleting. You also have to make sure that before you
///    access the container, the container is already created (the create_container() has returned
///    without error). If you try to access a container that is not created, it will panic as
///    there is no container at that index in the containers vector.
/// 3. Accessing the container must be thread-safe. This means, you can concurrently access
///    the container from multiple threads. insert, get, update, remove, scan_range, iter_next
///    should be thread-safe. In the case of InMemStorage, while iterator is alive, insert,
///    update, remove should be blocked. get and scan_range should be allowed because they are
///    read-only operations.
/// 4. For simplicity, a single database can be created. If you try to create multiple databases,
///    it will return DBExists error.
/// 5. The iterator next() must not be called using multiple threads. next() is not thread-safe with
///    respect to other next() calls of the same iterator. However, next() is thread-safe with respect
///    to other operations on the same container including next() of other iterators.
/// 6. Only a single database can be created. If you try to open_db() will always return DatabaseId 0.
pub struct OnDiskStorage<M: MemPool> {
    bp: Arc<M>,
    metadata: Arc<FosterBtree<M>>, // Database metadata. Stored in DatabaseId::MAX, ContainerId::0
    container_lock: RwLock<()>,    // lock for container operations
    containers: UnsafeCell<Vec<Arc<Storage<M>>>>, // Storage is in a Box in order to prevent moving when resizing the vector
}

unsafe impl<M: MemPool> Sync for OnDiskStorage<M> {}
unsafe impl<M: MemPool> Send for OnDiskStorage<M> {}

impl<M: MemPool> OnDiskStorage<M> {
    /// Assumes bp_directory is already created.
    /// Any database created will be created in the bp_directory.
    pub fn new(bp: &Arc<M>) -> Self {
        OnDiskStorage {
            bp: bp.clone(),
            metadata: Arc::new(FosterBtree::<M>::new(
                ContainerKey::new(DatabaseId::MAX, 0),
                bp.clone(),
            )),
            container_lock: RwLock::new(()),
            containers: UnsafeCell::new(Vec::new()),
        }
    }

    pub fn load(bp: &Arc<M>) -> Self {
        let metadata = Arc::new(FosterBtree::<M>::load(
            ContainerKey::new(DatabaseId::MAX, 0),
            bp.clone(),
            0,
        ));
        // Scans the metadata to get all the containers
        let mut containers = Vec::new();
        let iter = metadata.scan();
        for (k, v) in iter {
            let c_id = ContainerId::from_be_bytes(k.try_into().unwrap());
            let c_type = ContainerDS::from_bytes(&v);
            let storage = Storage::load(0, c_id, c_type, bp.clone());
            containers.push(Arc::new(storage));
        }

        OnDiskStorage {
            bp: bp.clone(),
            metadata,
            container_lock: RwLock::new(()),
            containers: UnsafeCell::new(containers),
        }
    }
}

pub struct OnDiskDummyTxnHandle {
    db_id: DatabaseId,
}

impl OnDiskDummyTxnHandle {
    pub fn new(db_id: DatabaseId) -> Self {
        OnDiskDummyTxnHandle { db_id }
    }

    pub fn db_id(&self) -> DatabaseId {
        self.db_id
    }
}

impl<M: MemPool> TxnStorageTrait for OnDiskStorage<M> {
    type TxnHandle = OnDiskDummyTxnHandle;
    type IteratorHandle = OnDiskIterator<M>;

    // Open connection with the db
    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        Ok(0)
    }

    // Close connection with the db
    fn close_db(&self, _db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        // Do nothing
        Ok(())
    }

    // Delete the db
    fn delete_db(&self, _db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        // Clear all the containers
        let containers = unsafe { &mut *self.containers.get() };
        containers.clear();
        Ok(())
    }

    // Create a container in the db
    fn create_container(
        &self,
        db_id: DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        if db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let _guard = self.container_lock.write().unwrap();
        let containers = unsafe { &mut *self.containers.get() };
        let c_id = containers.len();
        let storage = Arc::new(Storage::new(
            db_id,
            c_id as ContainerId,
            options.data_structure(),
            self.bp.clone(),
        ));
        self.metadata
            .insert(
                &(c_id as ContainerId).to_be_bytes(),
                &options.data_structure().to_bytes(),
            )
            .unwrap();
        containers.push(storage);
        Ok(c_id as ContainerId)
    }

    // Delete a container from the db
    // This function does not remove the container from the containers vector.
    // It just clears the container. Hence the container_id can be reused.
    // TODO: Make list_containers return only non-empty containers
    fn delete_container(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        if db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let _guard = self.container_lock.write().unwrap();
        let containers = unsafe { &mut *self.containers.get() };
        self.metadata.delete(&c_id.to_be_bytes()).unwrap();
        containers[c_id as usize].clear();
        Ok(())
    }

    // List all container names in the db
    fn list_containers(&self, db_id: DatabaseId) -> Result<HashSet<ContainerId>, TxnStorageStatus> {
        if db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let _guard = self.container_lock.read().unwrap();
        let containers = unsafe { &mut *self.containers.get() };
        Ok((0..containers.len() as ContainerId).collect())
    }

    fn raw_insert_value(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }

    // Begin a transaction
    fn begin_txn(
        &self,
        db_id: DatabaseId,
        _options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        Ok(OnDiskDummyTxnHandle::new(db_id))
    }

    // Commit a transaction
    fn commit_txn(
        &self,
        _txn: &Self::TxnHandle,
        _async_commit: bool,
    ) -> Result<(), TxnStorageStatus> {
        Ok(())
    }

    // Abort a transaction
    fn abort_txn(&self, _txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        Ok(())
    }

    // Wait for a transaction to finish
    fn wait_for_txn(&self, _txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        Ok(())
    }

    // Drop a transaction handle
    fn drop_txn(&self, _txn: Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        Ok(())
    }

    fn num_values(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        Ok(storage.num_values())
    }

    // Check if value exists
    fn check_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<bool, TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        match storage.get(key.as_ref()) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    // Get value
    fn get_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        storage.get(key.as_ref())
    }

    // Insert value
    fn insert_value(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        storage.insert(key, value)
    }

    // Insert values
    fn insert_values(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        for (k, v) in kvs {
            storage.insert(k, v)?;
        }
        Ok(())
    }

    // Update value
    fn update_value<K>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus>
    where
        K: AsRef<[u8]>,
    {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        storage.update(key.as_ref(), value)
    }

    fn update_value_with_func<K: AsRef<[u8]>, F: FnOnce(&mut [u8])>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
        func: F,
    ) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }

    // Delete value
    fn delete_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<(), TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[c_id as usize].as_ref();
        storage.remove(key.as_ref())
    }

    // Scan range
    fn scan_range(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        _options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        Ok(containers[c_id as usize].iter())
    }

    // Iterate next
    fn iter_next(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        Ok(iter.next())
    }

    // Drop an iterator handle
    fn drop_iterator_handle(&self, _iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus> {
        // Do nothing
        Ok(())
    }
}
