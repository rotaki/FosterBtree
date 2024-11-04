use core::panic;
use std::cell::UnsafeCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use clap::ArgAction;
use locktable::SimpleLockTable;

use crate::access_method::fbt::{BTreeKey, FosterBtreeAppendOnly, FosterBtreeRangeScannerWithPageId};
use crate::bp::{FrameReadGuard, FrameWriteGuard, PageFrameKey};
use crate::page::PageId;
use crate::prelude::{FosterBtreePage, UniqueKeyIndex};
use crate::txn_storage::TxnStorageStatus;
use crate::{
    bp::prelude::{ContainerId, DatabaseId, MemPool},
    prelude::{ContainerKey, FosterBtree},
};

use super::txn_storage_trait::ContainerType;
use super::{ContainerOptions, TxnOptions, TxnStorageTrait};

// Each transaction has a read-write set

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalAddress {
    page_id: PageId,
    frame_id: u32,
}

impl PhysicalAddress {
    pub fn new(page_id: PageId, frame_id: u32) -> Self {
        PhysicalAddress { page_id, frame_id }
    }

    pub fn to_pf_key(&self, c_id: ContainerId) -> PageFrameKey {
        PageFrameKey::new_with_frame_id(ContainerKey::new(0, c_id), self.page_id, self.frame_id)
    }
}

impl Display for PhysicalAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(p_id: {}, f_id: {})", self.page_id, self.frame_id)
    }
}

pub enum RWEntry {
    Read(bool, PhysicalAddress), // inserted_as_ghost, physical_address
    Update(bool, PhysicalAddress, Vec<u8>), // inserted_as_ghost, physical_address, value
    Insert(bool, PhysicalAddress, Vec<u8>), // inserted_as_ghost, physical_address, value
    Delete(bool, PhysicalAddress), // inserted_as_ghost, physical_address
}

impl Display for RWEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RWEntry::Read(inserted_as_ghost, pa) => {
                write!(f, "Read(is_ghost({}), {})", inserted_as_ghost, pa)
            }
            RWEntry::Update(inserted_as_ghost, pa, value) => {
                write!(
                    f,
                    "Update(is_ghost({}), {}, {:?})",
                    inserted_as_ghost, pa, value
                )
            }
            RWEntry::Insert(inserted_as_ghost, pa, value) => {
                write!(
                    f,
                    "Insert(is_ghost({}), {}, {:?})",
                    inserted_as_ghost, pa, value
                )
            }
            RWEntry::Delete(inserted_as_ghost, pa) => {
                write!(f, "Delete(is_ghost({}), {})", inserted_as_ghost, pa)
            }
        }
    }
}

impl RWEntry {
    pub fn update_physical_address(&mut self, new_pa: PhysicalAddress) {
        match self {
            RWEntry::Read(_, pa) => {
                *pa = new_pa;
            }
            RWEntry::Update(_, pa, _) => {
                *pa = new_pa;
            }
            RWEntry::Insert(_, pa, _) => {
                *pa = new_pa;
            }
            RWEntry::Delete(_, pa) => {
                *pa = new_pa;
            }
        }
    }

    pub fn update_value(&mut self, value: Vec<u8>) {
        match self {
            RWEntry::Read(_, _) => {
                panic!("Cannot update value of a read element")
            }
            RWEntry::Update(_, _, v) => {
                *v = value;
            }
            RWEntry::Insert(_, _, v) => {
                *v = value;
            }
            RWEntry::Delete(_, _) => {
                panic!("Cannot update value of a delete element")
            }
        }
    }

    pub fn ghost_inserted(&self) -> bool {
        match self {
            RWEntry::Read(inserted_as_ghost, _) => *inserted_as_ghost,
            RWEntry::Update(inserted_as_ghost, _, _) => *inserted_as_ghost,
            RWEntry::Insert(inserted_as_ghost, _, _) => *inserted_as_ghost,
            RWEntry::Delete(inserted_as_ghost, _) => *inserted_as_ghost,
        }
    }

    pub fn physical_address(&self) -> PhysicalAddress {
        match self {
            RWEntry::Read(_, pa) => pa.clone(),
            RWEntry::Update(_, pa, _) => pa.clone(),
            RWEntry::Insert(_, pa, _) => pa.clone(),
            RWEntry::Delete(_, pa) => pa.clone(),
        }
    }
}

pub struct ReadWriteSet {
    pub rwset: UnsafeCell<HashMap<Vec<u8>, RWEntry>>,
}

impl Display for ReadWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rwset = unsafe { &*self.rwset.get() };
        for (key, e) in rwset.iter() {
            write!(f, "{}: {}\n", String::from_utf8_lossy(key), e)?;
        }
        Ok(())
    }
}

impl ReadWriteSet {
    pub fn new() -> Self {
        ReadWriteSet {
            rwset: UnsafeCell::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&RWEntry> {
        let rwset = unsafe { &*self.rwset.get() };
        rwset.get(key)
    }

    pub fn get_mut(&self, key: &[u8]) -> Option<&mut RWEntry> {
        let rwset = unsafe { &mut *self.rwset.get() };
        rwset.get_mut(key)
    }

    pub fn insert(&self, key: Vec<u8>, value: RWEntry) {
        let rwset = unsafe { &mut *self.rwset.get() };
        rwset.insert(key, value);
    }

    pub fn remove(&self, key: &[u8]) {
        let rwset = unsafe { &mut *self.rwset.get() };
        rwset.remove(key);
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<Vec<u8>, RWEntry> {
        let rwset = unsafe { &*self.rwset.get() };
        rwset.iter()
    }
}

const CONTAINER_ID_COUNTER: AtomicU16 = AtomicU16::new(0);

// UniqueKeyIndex
pub struct PrimaryStorage<M: MemPool> {
    pub btree: Arc<FosterBtree<M>>,
    pub locktable: SimpleLockTable,
}

pub struct PrimaryStorages<M: MemPool> {
    pub map: UnsafeCell<HashMap<ContainerId, Arc<PrimaryStorage<M>>>>,
}

impl<M: MemPool> PrimaryStorages<M> {
    pub fn new() -> Self {
        PrimaryStorages {
            map: UnsafeCell::new(HashMap::new()),
        }
    }

    pub fn get(&self, c_id: ContainerId) -> Option<&Arc<PrimaryStorage<M>>> {
        let map = unsafe { &*self.map.get() };
        map.get(&c_id)
    }

    pub fn create_new(&self, bp: &Arc<M>) -> ContainerId {
        let map: &mut HashMap<u16, Arc<PrimaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let c_id = CONTAINER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let btree = Arc::new(FosterBtree::new(ContainerKey::new(0, c_id), bp.clone()));
        let locktable = SimpleLockTable::new();
        map.insert(c_id, Arc::new(PrimaryStorage { btree, locktable }));
        c_id
    }
}

unsafe impl<M: MemPool> Sync for PrimaryStorages<M> {}

pub struct SecondaryStorage<M: MemPool> {
    pub btree: Arc<FosterBtreeAppendOnly<M>>,
    pub ps: Arc<PrimaryStorage<M>>,
}

pub struct SecondaryStorages<M: MemPool> {
    map: UnsafeCell<HashMap<ContainerId, Arc<SecondaryStorage<M>>>>,
}

impl<M: MemPool> SecondaryStorages<M> {
    pub fn new() -> Self {
        SecondaryStorages {
            map: UnsafeCell::new(HashMap::new()),
        }
    }

    pub fn get(&self, c_id: ContainerId) -> Option<&Arc<SecondaryStorage<M>>> {
        let map = unsafe { &*self.map.get() };
        map.get(&c_id)
    }

    pub fn create_new(&self, bp: &Arc<M>, ps: &Arc<PrimaryStorage<M>>) -> ContainerId {
        let map: &mut HashMap<u16, Arc<SecondaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let c_id = CONTAINER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let btree = Arc::new(FosterBtreeAppendOnly::new(
            ContainerKey::new(0, c_id),
            bp.clone(),
        ));
        map.insert(
            c_id,
            Arc::new(SecondaryStorage {
                btree,
                ps: ps.clone(),
            }),
        );
        c_id
    }
}

unsafe impl<M: MemPool> Sync for SecondaryStorages<M> {}

pub struct NoWaitTxn {
    rwset: UnsafeCell<HashMap<ContainerId, ReadWriteSet>>, // Read-write set
}

impl Display for NoWaitTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rwset_all = unsafe { &*self.rwset.get() };
        for (c_id, rwset) in rwset_all.iter() {
            write!(f, "ContainerId: {}\n", c_id)?;
            write!(f, "{}\n", rwset)?;
        }
        Ok(())
    }
}

// State transitions of read-write set
// Current state -> Operation: Next state

// None -> Read: Read
// Read -> Read: Read
// Update -> Read: Update
// Insert -> Read: Insert
// Delete -> Read: panic

// None -> Update: Update
// Read -> Update: Update
// Update -> Update: Update
// Insert -> Update: Update (since ghost is inserted)
// Delete -> Update: panic

// None -> Insert: Insert(ghost record inserted)
// Read -> Insert: panic
// Update -> Insert: panic
// Insert -> Insert: panic
// Delete -> Insert: Update

// None -> Delete: Delete
// Read -> Delete: Delete
// Update -> Delete: Delete
// Insert -> Delete: Delete (since ghost is inserted)
// Delete -> Delete: panic

// Commit:
// 1. Release all read locks
// 2. Write all updates and inserts to storage
// 3. Physically delete all deletes from storage
// 4. Release all write locks

// Abort:
// 1. Revert all failed inserts
// 2. Release all locks

impl NoWaitTxn {
    pub fn read<M: MemPool, K: AsRef<[u8]>>(
        &self,
        c_id: ContainerId,
        ps: &PrimaryStorage<M>,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        // Get from rwset or insert a new entry
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(_, pa) => {
                    let storage = &ps.btree;
                    let page = storage.traverse_to_leaf_for_read_with_hint(
                        key.as_ref(),
                        Some(&pa.to_pf_key(c_id)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                        // Lower fence or non-existent key
                        panic!("Key should exist in storage if it is in rwset")
                    } else {
                        // Update the physical address
                        let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                        e.update_physical_address(new_pa);
                        return Ok(page.get_val(slot_id).to_vec());
                    }
                }
                RWEntry::Update(_, _, value) | RWEntry::Insert(_, _, value) => {
                    return Ok(value.clone())
                }
                RWEntry::Delete(_, _) => return Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Find from index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let page = storage.traverse_to_leaf_for_read_with_hint(key.as_ref(), None);
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                // Lower fence or non-existent key
                return Err(TxnStorageStatus::KeyNotFound);
            } else {
                // Lock the key
                if !locktable.try_shared(key.as_ref().to_vec()) {
                    println!("TxnConflict triggered");
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(key.as_ref().to_vec(), RWEntry::Read(false, new_pa));
                return Ok(page.get_val(slot_id).to_vec());
            }
        }
    }

    pub fn read_for_update<K: AsRef<[u8]>>(
        &self,
        c_id: ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        unimplemented!()
    }

    pub fn insert<M: MemPool, K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        c_id: ContainerId,
        ps: &PrimaryStorage<M>,
        key: K,
        value: V,
    ) -> Result<(), TxnStorageStatus> {
        // Get from rwset or insert a new entry
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(_, _) | RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) => {
                    return Err(TxnStorageStatus::KeyExists)
                }
                RWEntry::Delete(inserted_as_ghost, pa) => {
                    // Change to update
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(())
                }
            }
        } else {
            // Find from index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let mut page = storage.traverse_to_leaf_for_write_with_hint(key.as_ref(), None);
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                // Lower fence or non-existent key
                let next_key_slot_id = slot_id + 1; // This exists because of the upper fence
                let next_key = page.get_raw_key(next_key_slot_id).to_vec();
                // Lock the next key and then this key. The next_key might be infty which will be mapped to [].
                // The smallest key (-infty) is also mapped to [] but it should not be a problem if
                // we never lock the smallest key.
                //
                // First, check if next_key is in read-write set.
                // Case1. Next key is in rwset and is READ
                // => Upgrade next-key lock to write lock and then lock this key. Downgrade next-key lock after inserting this key.
                // Case2. Next key is in rwset and not READ
                // => Next-key is already locked. Lock this key and insert this key.
                // Case3. Next key is not in rwset
                // => Lock the next key and then this key. Insert this key and then release the next-key lock.
                match rwset.get(&next_key) {
                    Some(RWEntry::Read(_, _)) => {
                        // Upgrade next-key lock to write lock
                        if !locktable.try_upgrade(next_key.to_vec()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Lock this key
                        if !locktable.try_exclusive(key.as_ref().to_vec()) {
                            locktable.downgrade(&next_key); // Downgrade the next key lock
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Insert the key-value as ghost record
                        storage.insert_at_slot_or_split(
                            &mut page,
                            slot_id + 1,
                            key.as_ref(),
                            value.as_ref(),
                            true,
                        );

                        // Downgrade the next key lock
                        locktable.downgrade(&next_key);
                    }
                    Some(_) => {
                        // Lock this key
                        if !locktable.try_exclusive(key.as_ref().to_vec()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Insert the key-value as ghost record
                        storage.insert_at_slot_or_split(
                            &mut page,
                            slot_id + 1,
                            key.as_ref(),
                            value.as_ref(),
                            true,
                        );
                    }
                    None => {
                        // Lock the next key
                        if !locktable.try_exclusive(next_key.to_vec()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Lock this key
                        if !locktable.try_exclusive(key.as_ref().to_vec()) {
                            locktable.release_exclusive(next_key.to_vec());
                            return Err(TxnStorageStatus::TxnConflict);
                        }

                        // Insert the key-value as ghost record
                        storage.insert_at_slot_or_split(
                            &mut page,
                            slot_id + 1,
                            key.as_ref(),
                            value.as_ref(),
                            true,
                        );

                        // Release the next key lock
                        locktable.release_exclusive(next_key.to_vec());
                    }
                }
                // Insert this key into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(
                    key.as_ref().to_vec(),
                    RWEntry::Insert(true, new_pa, value.as_ref().to_vec()),
                );
                Ok(())
            } else {
                return Err(TxnStorageStatus::KeyExists);
            }
        }
    }

    pub fn update<M: MemPool, K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        c_id: ContainerId,
        ps: &PrimaryStorage<M>,
        key: K,
        value: V,
    ) -> Result<(), TxnStorageStatus> {
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(inserted_as_ghost, pa) => {
                    // Upgrade lock
                    let locktable = &ps.locktable;
                    if !locktable.try_upgrade(key.as_ref().to_vec()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    // Insert UPDATE entry into rwset
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(())
                }
                RWEntry::Update(..) => {
                    e.update_value(value.as_ref().to_vec());
                    Ok(())
                }
                RWEntry::Insert(inserted_as_ghost, pa, _) => {
                    // Insert UPDATE entry into rwset
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(())
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Abort if not found in index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let page = storage.traverse_to_leaf_for_read_with_hint(key.as_ref(), None);
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                return Err(TxnStorageStatus::KeyNotFound);
            } else {
                // Lock the key
                if !locktable.try_exclusive(key.as_ref().to_vec()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(
                    key.as_ref().to_vec(),
                    RWEntry::Update(false, new_pa, value.as_ref().to_vec()),
                );
                Ok(())
            }
        }
    }

    pub fn delete<M: MemPool, K: AsRef<[u8]>>(
        &self,
        c_id: ContainerId,
        ps: &PrimaryStorage<M>,
        key: K,
    ) -> Result<(), TxnStorageStatus> {
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(inserted_as_ghost, pa) => {
                    // Upgrade lock
                    let locktable = &ps.locktable;
                    if !locktable.try_upgrade(key.as_ref().to_vec()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    // Insert DELETE entry into rwset
                    *e = RWEntry::Delete(*inserted_as_ghost, pa.clone());
                    Ok(())
                }
                RWEntry::Update(inserted_as_ghost, pa, _)
                | RWEntry::Insert(inserted_as_ghost, pa, _) => {
                    // Insert DELETE entry into rwset.
                    // This should physically delete the record from storage on commit.
                    *e = RWEntry::Delete(*inserted_as_ghost, pa.clone());
                    Ok(())
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Abort if not found in index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let page = storage.traverse_to_leaf_for_read_with_hint(key.as_ref(), None);
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                return Err(TxnStorageStatus::KeyNotFound);
            } else {
                // Lock the key
                if !locktable.try_exclusive(key.as_ref().to_vec()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(key.as_ref().to_vec(), RWEntry::Delete(false, new_pa));
                Ok(())
            }
        }
    }

    pub fn read_scan<M: MemPool>(
        &self, 
        c_id: ContainerId,
        ps: &PrimaryStorage<M>,
        l_key: &[u8],
        u_key: &[u8],
        count: usize,
        rev: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        if rev {
            panic!("Reverse scan not supported yet")
        }

        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);

        let storage = &ps.btree;
        let locktable = &ps.locktable;
        let mut current_count = 0;

        let scanner = FosterBtreeRangeScannerWithPageId::new(storage, l_key, u_key);
        let mut result = Vec::new();
        for (key, value, (page_id, frame_id)) in scanner {
            if current_count >= count {
                break;
            }
            current_count += 1;
            if let Some(e) = rwset.get_mut(&key) {
                match e {
                    RWEntry::Read(_, _) => {
                        result.push((key, value)); 
                    }
                    RWEntry::Update(_, _, new_val) | RWEntry::Insert(_, _, new_val) => {
                        result.push((key, new_val.clone()));
                    }
                    RWEntry::Delete(_, _) => {
                        // Skip the record
                    }
                }
                continue;
            } else {
                // Lock the key
                if !locktable.try_shared(key.clone()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                rwset.insert(key.clone(), RWEntry::Read(false, PhysicalAddress::new(page_id, frame_id)));
            }
        }
        Ok(result)
    }

    pub fn update_scan<M: MemPool>(
        &self, 
        c_id: ContainerId,
        ps: &PrimaryStorage<M>,
        l_key: &[u8],
        u_key: &[u8],
        count: usize,
        rev: bool,
        mut func: impl FnMut(&mut [u8]),
    ) -> Result<(), TxnStorageStatus> {
        if rev {
            panic!("Reverse scan not supported yet")
        }

        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);

        let storage = &ps.btree;
        let locktable = &ps.locktable;
        let mut current_count = 0;

        let scanner = FosterBtreeRangeScannerWithPageId::new(storage, l_key, u_key);
        for (key, mut value, (page_id, frame_id)) in scanner {
            if current_count >= count {
                break;
            }
            current_count += 1;
            if let Some(e) = rwset.get_mut(&key) {
                match e {
                    RWEntry::Read(inserted_as_ghost, pa) => {
                        // Upgrade lock
                        if !locktable.try_upgrade(key.clone()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        func(&mut value);
                        *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value);
                    }
                    RWEntry::Update(_, _, value) => {
                        func(value);
                    }
                    RWEntry::Insert(inserted_as_ghost, pa, value) => {
                        func(value);
                        *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.clone());
                    }
                    RWEntry::Delete(_, _) => {
                        // Skip the record
                    }
                }
                continue;
            } else {
                // Lock the key
                if !locktable.try_exclusive(key.clone()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                func(&mut value);
                // Insert into rwset
                rwset.insert(key.clone(), RWEntry::Update(false, PhysicalAddress::new(page_id, frame_id), value));
            }
        }

        Ok(())
    }

    pub fn commit<M: MemPool>(&self, pss: &PrimaryStorages<M>) -> Result<(), TxnStorageStatus> {
        // Release read locks first
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let locktable = &pss.get(*c_id).unwrap().locktable;
            for (key, e) in rwset.iter() {
                match e {
                    RWEntry::Read(_, _) => {
                        locktable.release_shared(key.clone());
                    }
                    RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) | RWEntry::Delete(_, _) => {
                    }
                }
            }
        }

        // Write
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let ps = pss.get(*c_id).unwrap();
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            for (key, e) in rwset.iter() {
                match e {
                    RWEntry::Read(_, _) => {
                        continue;
                    }
                    RWEntry::Update(inserted_as_ghost, pa, value) => {
                        let mut page = storage
                            .traverse_to_leaf_for_write_with_hint(key, Some(&pa.to_pf_key(*c_id)));
                        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                        if slot_id == 0 || page.get_raw_key(slot_id) != key {
                            panic!("Key should exist in storage if it is in rwset")
                        } else {
                            // Update the record
                            if *inserted_as_ghost {
                                // Insert(g) -[Delete Op]-> Delete(g) -[Insert Op]-> Update(g)
                                // Update ghost bit
                                page.unghostify_at(slot_id);
                            }
                            storage.update_at_slot_or_split(&mut page, slot_id, key, value);
                        }
                    }
                    RWEntry::Insert(inserted_as_ghost, pa, _) => {
                        // Insert is always a ghost record insertion
                        assert!(inserted_as_ghost);
                        let mut page = storage
                            .traverse_to_leaf_for_write_with_hint(key, Some(&pa.to_pf_key(*c_id)));
                        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                        if slot_id == 0 || page.get_raw_key(slot_id) != key {
                            panic!("Key should exist in storage if it is in rwset")
                        } else {
                            page.unghostify_at(slot_id);
                        }
                    }
                    RWEntry::Delete(_, pa) => {
                        let mut page = storage
                            .traverse_to_leaf_for_write_with_hint(key, Some(&pa.to_pf_key(*c_id)));
                        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                        if slot_id == 0 || page.get_raw_key(slot_id) != key {
                            panic!("Key should exist in storage if it is in rwset")
                        } else {
                            // Physicall delete the record
                            page.remove_at(slot_id);
                        }
                    }
                }
                locktable.release_exclusive(key.clone());
            }
        }

        Ok(())
    }

    pub fn abort<M: MemPool>(&self, pss: &PrimaryStorages<M>) -> Result<(), TxnStorageStatus> {
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let ps = pss.get(*c_id).unwrap();
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            // Revert failed inserts
            for (key, e) in rwset.iter() {
                if e.ghost_inserted() {
                    let mut page = storage.traverse_to_leaf_for_write_with_hint(
                        key,
                        Some(&e.physical_address().to_pf_key(*c_id)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key {
                        panic!("Key should exist in storage if it is in rwset")
                    } else {
                        page.remove_at(slot_id);
                    }
                }
            }

            // Release locks
            for (key, e) in rwset.iter() {
                match e {
                    RWEntry::Read(_, _) => {
                        locktable.release_shared(key.clone());
                    }
                    RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) | RWEntry::Delete(_, _) => {
                        locktable.release_exclusive(key.clone());
                    }
                }
            }
        }

        Ok(())
    }
}

pub struct NoWaitTxnStorage<M: MemPool> {
    bp: Arc<M>,
    ps: PrimaryStorages<M>,
    ss: SecondaryStorages<M>,
}

impl<M: MemPool> NoWaitTxnStorage<M> {
    pub fn new(bp: Arc<M>) -> Self {
        NoWaitTxnStorage {
            bp,
            ps: PrimaryStorages::new(),
            ss: SecondaryStorages::new(),
        }
    }
}

impl<M: MemPool> TxnStorageTrait for NoWaitTxnStorage<M> {
    type TxnHandle = NoWaitTxn;
    type IteratorHandle = u32;

    // Only a single database supported right now.
    fn open_db(&self, options: super::DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        Ok(0)
    }

    fn close_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        Ok(())
    }

    fn delete_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        panic!("Delete db not supported")
    }

    // Creating a container is not transactional
    fn create_container(
        &self,
        db_id: DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        debug_assert_eq!(db_id, 0);
        match options.container_type() {
            ContainerType::Primary => Ok(self.ps.create_new(&self.bp)),
            ContainerType::Secondary(primary_c_id) => {
                let ps = self.ps.get(primary_c_id).unwrap();
                Ok(self.ss.create_new(&self.bp, ps))
            }
        }
    }

    // Deleting a container is not transactional
    fn delete_container(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        unimplemented!();
    }

    fn list_containers(&self, db_id: DatabaseId) -> Result<HashSet<ContainerId>, TxnStorageStatus> {
        unimplemented!();
    }

    fn raw_insert_value(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => {
                ps.btree.insert(&key, &value).unwrap();
                Ok(())
            }
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    ss.btree.append(&key, &value).unwrap();
                    Ok(())
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn begin_txn(
        &self,
        db_id: DatabaseId,
        options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        assert_eq!(db_id, 0);
        Ok(NoWaitTxn {
            rwset: UnsafeCell::new(HashMap::new()),
        })
    }

    fn commit_txn(
        &self,
        txn: &Self::TxnHandle,
        async_commit: bool,
    ) -> Result<(), TxnStorageStatus> {
        assert!(!async_commit);
        if let Ok(_) = txn.commit(&self.ps) {
            return Ok(());
        }
        if let Ok(_) = txn.abort(&self.ps) {
            Err(TxnStorageStatus::SystemAbort)
        } else {
            Err(TxnStorageStatus::AbortFailed)
        }
    }

    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        if let Ok(_) = txn.abort(&self.ps) {
            return Ok(());
        } else {
            Err(TxnStorageStatus::AbortFailed)
        }
    }

    fn wait_for_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }

    fn drop_txn(&self, txn: Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }

    fn num_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        unimplemented!()
    }

    fn check_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<bool, TxnStorageStatus> {
        unimplemented!()
    }

    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => txn.read(c_id, ps, key),
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    unimplemented!()
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn insert_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => txn.insert(c_id, ps, key, value),
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    unimplemented!()
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn insert_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => {
                for (key, value) in kvs {
                    txn.insert(c_id, ps, key, value)?;
                }
            }
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    unimplemented!()
                }
                None => return Err(TxnStorageStatus::ContainerNotFound),
            },
        }
        Ok(())
    }

    fn update_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => txn.update(c_id, ps, key, value),
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    unimplemented!()
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn update_value_with_func<K: AsRef<[u8]>, F: FnOnce(&mut [u8])>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
        func: F,
    ) -> Result<(), TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => {
                let mut value = txn.read(c_id, ps, key.as_ref())?;
                func(&mut value);
                txn.update(c_id, ps, key, value)
            }
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    unimplemented!()
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn delete_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<(), TxnStorageStatus> {
        match self.ps.get(c_id) {
            Some(ps) => txn.delete(c_id, ps, key),
            None => match self.ss.get(c_id) {
                Some(ss) => {
                    unimplemented!()
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: super::ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        unimplemented!()
    }

    fn iter_next(
        &self,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        unimplemented!()
    }

    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bp::{get_test_bp, BufferPool},
        prelude::{ContainerDS, ContainerOptions, DBOptions, TxnOptions},
    };

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_insert_and_read_back() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp(10);
        let storage = NoWaitTxnStorage {
            bp: bp.clone(),
            ps: PrimaryStorages::new(),
            ss: SecondaryStorages::new(),
        };

        // Open a database
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Create a container
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Begin a transaction
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a key-value pair
        storage
            .insert_value(&txn, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        println!("txn {}", &txn);

        // Commit the transaction
        storage.commit_txn(&txn, false).unwrap();

        // Begin another transaction
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Read the key-value pair
        let value = storage.get_value(&txn2, c_id, b"key1").unwrap();

        // Check that the value matches
        assert_eq!(value, b"value1".to_vec());

        // Commit the transaction
        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_insert_and_update_in_same_txn() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp(10);
        let storage = NoWaitTxnStorage {
            bp: bp.clone(),
            ps: PrimaryStorages::new(),
            ss: SecondaryStorages::new(),
        };

        // Open a database
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Create a container
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Begin a transaction
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a key-value pair
        storage
            .insert_value(&txn, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Update the key-value pair
        storage
            .update_value(&txn, c_id, b"key1", b"value2".to_vec())
            .unwrap();

        // Read the key-value pair
        let value = storage.get_value(&txn, c_id, b"key1").unwrap();

        // Check that the value matches the updated value
        assert_eq!(value, b"value2".to_vec());

        println!("txn {}", &txn);

        // Commit the transaction
        storage.commit_txn(&txn, false).unwrap();

        // Begin another transaction
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Read the key-value pair
        let value = storage.get_value(&txn2, c_id, b"key1").unwrap();

        // Check that the value matches the updated value
        assert_eq!(value, b"value2".to_vec());
    }

    #[test]
    fn test_insert_and_delete_in_same_txn() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp(10);
        let storage = NoWaitTxnStorage {
            bp: bp.clone(),
            ps: PrimaryStorages::new(),
            ss: SecondaryStorages::new(),
        };

        // Open a database
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Create a container
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();
        // Begin a transaction
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a key-value pair
        storage
            .insert_value(&txn, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Delete the key-value pair
        storage.delete_value(&txn, c_id, b"key1").unwrap();

        // Try to read the key-value pair, should get KeyNotFound
        let result = storage.get_value(&txn, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        println!("txn {}", &txn);

        // Commit the transaction
        storage.commit_txn(&txn, false).unwrap();

        // Begin another transaction to verify deletion
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Try to read the key-value pair, should still get KeyNotFound
        let result = storage.get_value(&txn2, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        println!("txn2 {}", &txn2);

        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_conflicting_transactions() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp(10);
        let storage = NoWaitTxnStorage {
            bp: bp.clone(),
            ps: PrimaryStorages::new(),
            ss: SecondaryStorages::new(),
        };

        // Open a database
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Begin first transaction
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Create a container
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert a key-value pair in txn1
        storage
            .insert_value(&txn1, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Begin second transaction
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Try to update the same key in txn2, should cause a conflict
        let result = storage.update_value(&txn2, c_id, b"key1", b"value2".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Commit txn1
        println!("txn1 {}", &txn1);
        storage.commit_txn(&txn1, false).unwrap();

        // Retry update in txn2, should succeed now
        let result = storage.update_value(&txn2, c_id, b"key1", b"value2".to_vec());
        assert!(result.is_ok());

        // Commit txn2
        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_transaction_abort() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp(10);
        let storage = NoWaitTxnStorage {
            bp: bp.clone(),
            ps: PrimaryStorages::new(),
            ss: SecondaryStorages::new(),
        };

        // Open a database
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        // Create a container
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Begin a transaction
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a key-value pair
        storage
            .insert_value(&txn, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Abort the transaction
        storage.abort_txn(&txn).unwrap();

        // Begin another transaction to verify that the key doesn't exist
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Try to read the key-value pair, should get KeyNotFound
        let result = storage.get_value(&txn2, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        storage.commit_txn(&txn2, false).unwrap();
    }
}
