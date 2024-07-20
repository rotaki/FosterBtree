use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
};

use super::{
    ContainerOptions, ContainerType, DBOptions, ScanOptions, TxnOptions, TxnStorageStatus,
    TxnStorageTrait,
};
use crate::bp::prelude::{ContainerId, DatabaseId};
use crate::rwlatch::RwLatch;

pub enum Storage {
    HashMap(RwLatch, UnsafeCell<HashMap<Vec<u8>, Vec<u8>>>),
    BTreeMap(RwLatch, UnsafeCell<BTreeMap<Vec<u8>, Vec<u8>>>),
}

unsafe impl Sync for Storage {}

impl Storage {
    fn new(c_type: ContainerType) -> Self {
        match c_type {
            ContainerType::Hash => {
                Storage::HashMap(RwLatch::default(), UnsafeCell::new(HashMap::new()))
            }
            ContainerType::BTree => {
                Storage::BTreeMap(RwLatch::default(), UnsafeCell::new(BTreeMap::new()))
            }
        }
    }

    fn shared(&self) {
        match self {
            Storage::HashMap(latch, _) => latch.shared(),
            Storage::BTreeMap(latch, _) => latch.shared(),
        }
    }

    fn exclusive(&self) {
        match self {
            Storage::HashMap(latch, _) => latch.exclusive(),
            Storage::BTreeMap(latch, _) => latch.exclusive(),
        }
    }

    fn release_shared(&self) {
        match self {
            Storage::HashMap(latch, _) => latch.release_shared(),
            Storage::BTreeMap(latch, _) => latch.release_shared(),
        }
    }

    fn release_exclusive(&self) {
        match self {
            Storage::HashMap(latch, _) => latch.release_exclusive(),
            Storage::BTreeMap(latch, _) => latch.release_exclusive(),
        }
    }

    fn clear(&self) {
        self.exclusive();
        match self {
            Storage::HashMap(_, h) => {
                let h = unsafe { &mut *h.get() };
                h.clear();
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &mut *b.get() };
                b.clear();
            }
        }
        self.release_exclusive();
    }

    fn insert(&self, key: Vec<u8>, val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        self.exclusive();
        let result = match self {
            Storage::HashMap(_, h) => {
                let h = unsafe { &mut *h.get() };
                match h.entry(key) {
                    std::collections::hash_map::Entry::Occupied(_) => {
                        Err(TxnStorageStatus::KeyExists)
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(val);
                        Ok(())
                    }
                }
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &mut *b.get() };
                match b.entry(key) {
                    std::collections::btree_map::Entry::Occupied(_) => {
                        Err(TxnStorageStatus::KeyExists)
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(val);
                        Ok(())
                    }
                }
            }
        };
        self.release_exclusive();
        result
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, TxnStorageStatus> {
        self.shared();
        let result = match self {
            Storage::HashMap(_, h) => {
                let h = unsafe { &*h.get() };
                match h.get(key) {
                    Some(val) => Ok(val.clone()),
                    None => Err(TxnStorageStatus::KeyNotFound),
                }
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &*b.get() };
                match b.get(key) {
                    Some(val) => Ok(val.clone()),
                    None => Err(TxnStorageStatus::KeyNotFound),
                }
            }
        };
        self.release_shared();
        result
    }

    fn update(&self, key: &[u8], val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        self.exclusive();
        let result = match self {
            Storage::HashMap(_, h) => {
                let h = unsafe { &mut *h.get() };
                match h.get_mut(key) {
                    Some(v) => {
                        *v = val;
                        Ok(())
                    }
                    None => Err(TxnStorageStatus::KeyNotFound),
                }
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &mut *b.get() };
                match b.get_mut(key) {
                    Some(v) => {
                        *v = val;
                        Ok(())
                    }
                    None => Err(TxnStorageStatus::KeyNotFound),
                }
            }
        };
        self.release_exclusive();
        result
    }

    fn remove(&self, key: &[u8]) -> Result<(), TxnStorageStatus> {
        self.shared();
        let result = match self {
            Storage::HashMap(_, h) => {
                let h = unsafe { &mut *h.get() };
                match h.remove(key) {
                    Some(_) => Ok(()),
                    None => Err(TxnStorageStatus::KeyNotFound),
                }
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &mut *b.get() };
                match b.remove(key) {
                    Some(_) => Ok(()),
                    None => Err(TxnStorageStatus::KeyNotFound),
                }
            }
        };
        self.release_shared();
        result
    }

    fn iter(self: &Arc<Self>) -> InMemIterator {
        self.shared(); // Latch the storage while iterator is alive. When iterator is dropped, the latch must be released.
        match self.as_ref() {
            Storage::HashMap(_, h) => {
                let h = unsafe { &*h.get() };
                InMemIterator::hash(Arc::clone(self), h.iter())
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &*b.get() };
                InMemIterator::btree(Arc::clone(self), b.iter())
            }
        }
    }

    fn num_values(&self) -> usize {
        self.shared();
        let result = match self {
            Storage::HashMap(_, h) => {
                let h = unsafe { &*h.get() };
                h.len()
            }
            Storage::BTreeMap(_, b) => {
                let b = unsafe { &*b.get() };
                b.len()
            }
        };
        self.release_shared();
        result
    }
}

pub enum InMemIterator {
    // Storage and the iterator
    Hash(
        Arc<Storage>,
        Mutex<std::collections::hash_map::Iter<'static, Vec<u8>, Vec<u8>>>,
    ),
    BTree(
        Arc<Storage>,
        Mutex<std::collections::btree_map::Iter<'static, Vec<u8>, Vec<u8>>>,
    ),
}

impl Drop for InMemIterator {
    fn drop(&mut self) {
        match self {
            InMemIterator::Hash(storage, _) => storage.release_shared(),
            InMemIterator::BTree(storage, _) => storage.release_shared(),
        }
    }
}

impl InMemIterator {
    fn hash(
        storage: Arc<Storage>,
        iter: std::collections::hash_map::Iter<'static, Vec<u8>, Vec<u8>>,
    ) -> Self {
        InMemIterator::Hash(storage, Mutex::new(iter))
    }

    fn btree(
        storage: Arc<Storage>,
        iter: std::collections::btree_map::Iter<'static, Vec<u8>, Vec<u8>>,
    ) -> Self {
        InMemIterator::BTree(storage, Mutex::new(iter))
    }

    fn next(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            InMemIterator::Hash(_, iter) => {
                let mut iter = iter.lock().unwrap();
                iter.next().map(|(k, v)| (k.clone(), v.clone()))
            }
            InMemIterator::BTree(_, iter) => {
                let mut iter = iter.lock().unwrap();
                iter.next().map(|(k, v)| (k.clone(), v.clone()))
            }
        }
    }
}

/// Assumptions of InMemStorage:
/// 1. Creation and deletion of the database is not thread-safe. This means, you can't create
/// or delete a database while other threads are accessing the database.
/// 2. Creation and deletion of a container is thread-safe with respect to other containers.
/// However, deletion of a container is not thread-safe with respect to other threads accessing
/// the same container that is being deleted. You have to make sure that no other threads are
/// accessing the container while you are deleting. You also have to make sure that before you
/// access the container, the container is already created (the create_container() has returned
/// without error). If you try to access a container that is not created, it will panic as
/// there is no container at that index in the containers vector.
/// 3. Accessing the container must be thread-safe. This means, you can concurrently access
/// the container from multiple threads. insert, get, update, remove, scan_range, iter_next
/// should be thread-safe. In the case of InMemStorage, while iterator is alive, insert,
/// update, remove should be blocked. get and scan_range should be allowed because they are
/// read-only operations.
/// 4. For simplicity, a single database can be created. If you try to create multiple databases,
/// it will return DBExists error.
/// 5. The iterator next() must not be called using multiple threads. next() is not thread-safe with
/// respect to other next() calls of the same iterator. However, next() is thread-safe with respect
/// to other operations on the same container including next() of other iterators.
pub struct InMemStorage {
    db_created: UnsafeCell<bool>,
    container_lock: RwLock<()>, // lock for container operations
    containers: UnsafeCell<Vec<Arc<Storage>>>, // Storage is in a Box in order to prevent moving when resizing the vector
}

unsafe impl Sync for InMemStorage {}

impl Default for InMemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemStorage {
    pub fn new() -> Self {
        InMemStorage {
            db_created: UnsafeCell::new(false),
            container_lock: RwLock::new(()),
            containers: UnsafeCell::new(Vec::new()),
        }
    }
}

pub struct InMemDummyTxnHandle {
    db_id: DatabaseId,
}

impl InMemDummyTxnHandle {
    pub fn new(db_id: DatabaseId) -> Self {
        InMemDummyTxnHandle { db_id }
    }

    pub fn db_id(&self) -> DatabaseId {
        self.db_id
    }
}

impl TxnStorageTrait for InMemStorage {
    type TxnHandle = InMemDummyTxnHandle;
    type IteratorHandle = InMemIterator;

    // Open connection with the db
    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        let guard = unsafe { &mut *self.db_created.get() };
        if *guard {
            return Err(TxnStorageStatus::DBExists);
        }
        *guard = true;
        Ok(0)
    }

    // Close connection with the db
    fn close_db(&self, _db_id: &DatabaseId) -> Result<(), TxnStorageStatus> {
        // Do nothing
        Ok(())
    }

    // Delete the db
    fn delete_db(&self, db_id: &DatabaseId) -> Result<(), TxnStorageStatus> {
        if *db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let guard = unsafe { &mut *self.db_created.get() };
        *guard = false;
        // Clear all the containers
        let containers = unsafe { &mut *self.containers.get() };
        containers.clear();
        Ok(())
    }

    // Create a container in the db
    fn create_container(
        &self,
        _txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        if *db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let _guard = self.container_lock.write().unwrap();
        let containers = unsafe { &mut *self.containers.get() };
        let storage = Arc::new(Storage::new(options.get_type()));
        containers.push(storage);
        Ok((containers.len() - 1) as ContainerId)
    }

    // Delete a container from the db
    // This function does not remove the container from the containers vector.
    // It just clears the container. Hence the container_id can be reused.
    // TODO: Make list_containers return only non-empty containers
    fn delete_container(
        &self,
        _txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        c_id: &ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        if *db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let _guard = self.container_lock.write().unwrap();
        let containers = unsafe { &mut *self.containers.get() };
        containers[*c_id as usize].clear();
        Ok(())
    }

    // List all container names in the db
    fn list_containers(
        &self,
        _txn: &Self::TxnHandle,
        db_id: &DatabaseId,
    ) -> Result<HashSet<ContainerId>, TxnStorageStatus> {
        if *db_id != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let _guard = self.container_lock.read().unwrap();
        let containers = unsafe { &mut *self.containers.get() };
        Ok((0..containers.len() as ContainerId).collect())
    }

    // Begin a transaction
    fn begin_txn(
        &self,
        db_id: &DatabaseId,
        _options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        Ok(InMemDummyTxnHandle::new(*db_id))
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
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        if txn.db_id() != 0 {
            return Err(TxnStorageStatus::DBNotFound);
        }
        let containers = unsafe { &*self.containers.get() };
        Ok(containers[*c_id as usize].num_values())
    }

    // Check if value exists
    fn check_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<bool, TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[*c_id as usize].as_ref();
        match storage.get(key.as_ref()) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    // Get value
    fn get_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[*c_id as usize].as_ref();
        storage.get(key.as_ref())
    }

    // Insert value
    fn insert_value(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[*c_id as usize].as_ref();
        storage.insert(key, value)
    }

    // Insert values
    fn insert_values(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[*c_id as usize].as_ref();
        for (k, v) in kvs {
            storage.insert(k, v)?;
        }
        Ok(())
    }

    // Update value
    fn update_value<K>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
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
        let storage = containers[*c_id as usize].as_ref();
        storage.update(key.as_ref(), value)
    }

    // Delete value
    fn delete_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<(), TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        let storage = containers[*c_id as usize].as_ref();
        storage.remove(key.as_ref())
    }

    // Scan range
    fn scan_range(
        &self,
        _txn: &Self::TxnHandle,
        c_id: &ContainerId,
        _options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        // Access the container with the container_id. No guard
        // is required because we assume that container is
        // already created.
        let containers = unsafe { &*self.containers.get() };
        Ok(containers[*c_id as usize].iter())
    }

    // Iterate next
    fn iter_next(
        &self,
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
