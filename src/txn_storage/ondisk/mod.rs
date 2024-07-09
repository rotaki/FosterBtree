use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};

use super::{
    ContainerOptions, ContainerType, DBOptions, ScanOptions, TxnOptions, TxnStorageStatus,
    TxnStorageTrait,
};
use crate::rwlatch::RwLatch;
use crate::{
    bp::prelude::{ContainerId, DatabaseId},
    fbt::FosterBtreeRangeScanner,
    prelude::{BufferPool, ContainerKey, FosterBtree, LRUEvictionPolicy},
};

type BufferPoolType = BufferPool<LRUEvictionPolicy>;
type FosterBtreeType = FosterBtree<LRUEvictionPolicy, BufferPoolType>;

pub enum Storage {
    HashMap(),
    BTreeMap(Arc<FosterBtreeType>),
}

unsafe impl Sync for Storage {}

impl Storage {
    fn new(
        db_id: DatabaseId,
        c_id: ContainerId,
        c_type: ContainerType,
        bp: Arc<BufferPoolType>,
    ) -> Self {
        match c_type {
            ContainerType::Hash => {
                unimplemented!("Hash container not implemented")
            }
            ContainerType::BTree => Storage::BTreeMap(Arc::new(FosterBtreeType::new(
                ContainerKey::new(db_id, c_id),
                bp,
            ))),
        }
    }

    fn clear(&self) {
        unimplemented!("clear not implemented")
    }

    fn insert(&self, key: Vec<u8>, val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        let result = match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.insert(&key, &val)?,
        };
        Ok(result)
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, TxnStorageStatus> {
        let result = match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.get(key)?,
        };
        Ok(result)
    }

    fn update(&self, key: &[u8], val: Vec<u8>) -> Result<(), TxnStorageStatus> {
        let result = match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.update(key, &val)?,
        };
        Ok(result)
    }

    fn remove(&self, key: &[u8]) -> Result<(), TxnStorageStatus> {
        let result = match self {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => b.delete(key)?,
        };
        Ok(result)
    }

    fn iter(self: &Arc<Self>) -> OnDiskIterator {
        match self.as_ref() {
            Storage::HashMap() => {
                unimplemented!("Hash container not implemented")
            }
            Storage::BTreeMap(b) => OnDiskIterator::btree(b.scan(&[], &[])),
        }
    }
}

pub enum OnDiskIterator {
    // Storage and the iterator
    Hash(),
    BTree(Mutex<FosterBtreeRangeScanner<LRUEvictionPolicy, BufferPoolType>>),
}

impl OnDiskIterator {
    fn btree(iter: FosterBtreeRangeScanner<LRUEvictionPolicy, BufferPoolType>) -> Self {
        OnDiskIterator::BTree(Mutex::new(iter))
    }

    fn next(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            OnDiskIterator::Hash() => {
                unimplemented!("Hash container not implemented")
            }
            OnDiskIterator::BTree(iter) => iter.lock().unwrap().next(),
        }
    }
}

/// Assumptions of InMemStorage:
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
pub struct OnDiskStorage {
    bp_dir: PathBuf,
    bp: Arc<BufferPoolType>,
    db_created: UnsafeCell<bool>,
    container_lock: RwLock<()>, // lock for container operations
    containers: UnsafeCell<Vec<Arc<Storage>>>, // Storage is in a Box in order to prevent moving when resizing the vector
}

impl Drop for OnDiskStorage {
    fn drop(&mut self) {
        // Clear the directory including folders
        std::fs::remove_dir_all(&self.bp_dir).unwrap();
    }
}

unsafe impl Sync for OnDiskStorage {}

impl OnDiskStorage {
    pub fn new(num_frames: usize) -> Self {
        let ts_in_nanoseconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir_name = "BP_TEST_DIR_".to_string() + &ts_in_nanoseconds.to_string();
        // Create dir if it doesn't exist
        if !std::path::Path::new(&dir_name).exists() {
            println!(
                "Buffer pool directory ({}) does not exist. Creating the directory.",
                dir_name
            );
            std::fs::create_dir(&dir_name).unwrap();
        } else {
            println!(
                "Buffer pool directory ({}) already exists. Clearing the directory.",
                dir_name
            );
            // Clear the directory including folders
            std::fs::remove_dir_all(&dir_name).unwrap();
            std::fs::create_dir(&dir_name).unwrap();
        }
        OnDiskStorage {
            bp_dir: PathBuf::from(&dir_name),
            bp: Arc::new(BufferPoolType::new(&dir_name, num_frames).unwrap()),
            db_created: UnsafeCell::new(false),
            container_lock: RwLock::new(()),
            containers: UnsafeCell::new(Vec::new()),
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

impl TxnStorageTrait for OnDiskStorage {
    type TxnHandle = OnDiskDummyTxnHandle;
    type IteratorHandle = OnDiskIterator;

    // Open connection with the db
    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        let guard = unsafe { &mut *self.db_created.get() };
        if *guard {
            return Err(TxnStorageStatus::DBExists);
        }
        let db_id = 0;
        // Create db folder
        let db_path = self.bp_dir.join(db_id.to_string());
        std::fs::create_dir(&db_path).unwrap();
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
        let c_id = containers.len();
        let storage = Arc::new(Storage::new(
            *db_id,
            c_id as ContainerId,
            options.get_type(),
            self.bp.clone(),
        ));
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
        Ok(OnDiskDummyTxnHandle::new(*db_id))
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
