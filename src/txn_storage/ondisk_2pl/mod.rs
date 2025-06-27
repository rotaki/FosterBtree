use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use crate::access_method::fbt::{BTreeKey, FosterBtreeCursor};
use crate::bp::PageFrameKey;
use crate::event_tracer::trace_secidx;
use crate::page::PageId;
use crate::prelude::{FosterBtreePage, ScanOptions, UniqueKeyIndex};
use crate::txn_storage::TxnStorageStatus;
use crate::{
    bp::prelude::{ContainerId, DatabaseId, MemPool},
    prelude::{ContainerKey, FosterBtree},
};

use super::locktable::ConcurrentLockTable as LockTable;
// use super::locktable::SingleThreadLockTable as LockTable;
use super::txn_storage_trait::ContainerType;
use super::{ContainerOptions, DBOptions, TxnOptions, TxnStorageTrait};

#[allow(unused_imports)]
use crate::log;
#[allow(unused_imports)]
use crate::{log_error, log_info};

// Each transaction has a read-write set

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalAddress {
    page_id: PageId,
    frame_id: u32,
}

impl Default for PhysicalAddress {
    fn default() -> Self {
        PhysicalAddress {
            page_id: 0,
            frame_id: u32::MAX,
        }
    }
}

impl PhysicalAddress {
    pub fn new(page_id: PageId, frame_id: u32) -> Self {
        PhysicalAddress { page_id, frame_id }
    }

    pub fn to_pf_key(&self, c_id: ContainerId) -> PageFrameKey {
        PageFrameKey::new_with_frame_id(ContainerKey::new(0, c_id), self.page_id, self.frame_id)
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let page_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let frame_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        PhysicalAddress { page_id, frame_id }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        let mut bytes = [0; 8];
        bytes[0..4].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.frame_id.to_be_bytes());
        bytes
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
            writeln!(f, "{}: {}", String::from_utf8_lossy(key), e)?;
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

    pub fn iter(&self) -> std::collections::hash_map::Iter<Vec<u8>, RWEntry> {
        let rwset = unsafe { &*self.rwset.get() };
        rwset.iter()
    }
}

// ContainerId starts from 1. 0 is reserved for metadata.
static CONTAINER_ID_COUNTER: AtomicU16 = AtomicU16::new(1);

// UniqueKeyIndex
pub struct PrimaryStorage<M: MemPool> {
    pub c_id: ContainerId,
    pub btree: Arc<FosterBtree<M>>,
    pub locktable: Arc<LockTable>,
}

pub struct PrimaryStorages<M: MemPool> {
    pub map: UnsafeCell<HashMap<ContainerId, Arc<PrimaryStorage<M>>>>,
}

impl<M: MemPool> Default for PrimaryStorages<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: MemPool> PrimaryStorages<M> {
    pub fn new() -> Self {
        PrimaryStorages {
            map: UnsafeCell::new(HashMap::new()),
        }
    }

    pub fn print_lock_tables(&self) {
        let map = unsafe { &*self.map.get() };
        for (c_id, ps) in map.iter() {
            println!("PrimaryStorage {}: LockTable\n{}", c_id, ps.locktable);
        }
    }

    pub fn get(&self, c_id: ContainerId) -> Option<&Arc<PrimaryStorage<M>>> {
        let map = unsafe { &*self.map.get() };
        map.get(&c_id)
    }

    pub fn create_new(&self, bp: &Arc<M>) -> ContainerId {
        let map: &mut HashMap<u16, Arc<PrimaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let c_id = CONTAINER_ID_COUNTER.fetch_add(1, Ordering::AcqRel);
        let btree = Arc::new(FosterBtree::new(ContainerKey::new(0, c_id), bp.clone()));
        let locktable = Arc::new(LockTable::new());
        map.insert(
            c_id,
            Arc::new(PrimaryStorage {
                c_id,
                btree,
                locktable,
            }),
        );
        c_id
    }

    pub fn load(&self, bp: &Arc<M>, c_id: ContainerId) {
        let map: &mut HashMap<u16, Arc<PrimaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let btree = Arc::new(FosterBtree::load(ContainerKey::new(0, c_id), bp.clone(), 0));
        let locktable = Arc::new(LockTable::new());
        map.insert(
            c_id,
            Arc::new(PrimaryStorage {
                c_id,
                btree,
                locktable,
            }),
        );
    }
}

unsafe impl<M: MemPool> Sync for PrimaryStorages<M> {}

pub struct SecondaryStorage<M: MemPool> {
    pub c_id: ContainerId,
    pub btree: Arc<FosterBtree<M>>,
    pub locktable: Arc<LockTable>,
    pub ps: Arc<PrimaryStorage<M>>,
}

pub struct SecondaryStorages<M: MemPool> {
    map: UnsafeCell<HashMap<ContainerId, Arc<SecondaryStorage<M>>>>,
}

impl<M: MemPool> Default for SecondaryStorages<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: MemPool> SecondaryStorages<M> {
    pub fn new() -> Self {
        SecondaryStorages {
            map: UnsafeCell::new(HashMap::new()),
        }
    }

    pub fn print_lock_tables(&self) {
        let map = unsafe { &*self.map.get() };
        for (c_id, ss) in map.iter() {
            println!("SecondaryStorage {}: LockTable\n{}", c_id, ss.locktable);
        }
    }

    pub fn get(&self, c_id: ContainerId) -> Option<&Arc<SecondaryStorage<M>>> {
        let map = unsafe { &*self.map.get() };
        map.get(&c_id)
    }

    pub fn create_new(&self, bp: &Arc<M>, ps: &Arc<PrimaryStorage<M>>) -> ContainerId {
        let map: &mut HashMap<u16, Arc<SecondaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let c_id = CONTAINER_ID_COUNTER.fetch_add(1, Ordering::AcqRel);
        let btree = Arc::new(FosterBtree::new(ContainerKey::new(0, c_id), bp.clone()));
        let locktable = Arc::new(LockTable::new());
        map.insert(
            c_id,
            Arc::new(SecondaryStorage {
                c_id,
                btree,
                locktable,
                ps: ps.clone(),
            }),
        );
        c_id
    }

    pub fn load(&self, bp: &Arc<M>, c_id: ContainerId, ps: &Arc<PrimaryStorage<M>>) {
        let map: &mut HashMap<u16, Arc<SecondaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let btree = Arc::new(FosterBtree::load(ContainerKey::new(0, c_id), bp.clone(), 0));
        let locktable = Arc::new(LockTable::new());
        map.insert(
            c_id,
            Arc::new(SecondaryStorage {
                c_id,
                btree,
                locktable,
                ps: ps.clone(),
            }),
        );
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
            writeln!(f, "ContainerId: {}", c_id)?;
            writeln!(f, "{}", rwset)?;
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
// 2. Update: apply the update to storage
// 3. Insert: flip the ghost bit in storage
// 4. Delete: remove the record from storage
// 5. Release all write locks

// Abort:
// 1. Revert all failed inserts
// 2. Release all locks

impl NoWaitTxn {
    pub fn print_read_write_sets(&self) {
        let rwset_all = unsafe { &*self.rwset.get() };
        for (c_id, rwset) in rwset_all.iter() {
            println!("ContainerId: {}", c_id);
            println!("{}", rwset);
        }
    }

    pub fn read<M: MemPool, K: AsRef<[u8]>>(
        &self,
        ps: &PrimaryStorage<M>,
        key: K,
        hint: Option<PhysicalAddress>,
    ) -> Result<(Vec<u8>, PhysicalAddress), TxnStorageStatus> {
        let c_id = ps.c_id;
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);

        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(_, pa) => {
                    // Prioritize the hint in rwset over the hint from the caller.
                    let storage = &ps.btree;
                    let page = storage.traverse_to_leaf_for_read_with_hint(
                        key.as_ref(),
                        Some(pa.to_pf_key(c_id)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                        // Lower fence or non-existent key
                        panic!("Key should exist in storage if it is in rwset")
                    } else {
                        // Update the physical address
                        let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                        e.update_physical_address(new_pa.clone());
                        Ok((page.get_val(slot_id).to_vec(), new_pa))
                    }
                }
                RWEntry::Update(_, pa, value) | RWEntry::Insert(_, pa, value) => {
                    Ok((value.clone(), pa.clone()))
                }
                RWEntry::Delete(_, _) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Find from index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let page = storage
                .traverse_to_leaf_for_read_with_hint(key.as_ref(), hint.map(|h| h.to_pf_key(c_id)));
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                // Lower fence or non-existent key
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_shared(key.as_ref().to_vec()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(key.as_ref().to_vec(), RWEntry::Read(false, new_pa.clone()));
                Ok((page.get_val(slot_id).to_vec(), new_pa))
            }
        }
    }

    pub fn insert<M: MemPool, K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        ps: &PrimaryStorage<M>,
        key: K,
        value: V,
        hint: Option<PhysicalAddress>,
    ) -> Result<PhysicalAddress, TxnStorageStatus> {
        // Get from rwset or insert a new entry
        let c_id = ps.c_id;
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(_, _) | RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) => {
                    Err(TxnStorageStatus::KeyExists)
                }
                RWEntry::Delete(inserted_as_ghost, pa) => {
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(pa)
                }
            }
        } else {
            // Find from index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let mut page = storage.traverse_to_leaf_for_write_with_hint(
                key.as_ref(),
                hint.map(|h| h.to_pf_key(c_id)),
            );
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
                    RWEntry::Insert(true, new_pa.clone(), value.as_ref().to_vec()),
                );
                Ok(new_pa)
            } else {
                Err(TxnStorageStatus::KeyExists)
            }
        }
    }

    pub fn insert_secondary<M: MemPool, K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        ss: &SecondaryStorage<M>,
        key: K,
        value: V,
        hint: Option<PhysicalAddress>,
    ) -> Result<PhysicalAddress, TxnStorageStatus> {
        // Get from rwset or insert a new entry
        let c_id = ss.c_id;
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(_, _) | RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) => {
                    Err(TxnStorageStatus::KeyExists)
                }
                RWEntry::Delete(inserted_as_ghost, pa) => {
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(pa)
                }
            }
        } else {
            // Find from index
            let storage = &ss.btree;
            let locktable = &ss.locktable;
            let mut page = storage.traverse_to_leaf_for_write_with_hint(
                key.as_ref(),
                hint.map(|h| h.to_pf_key(c_id)),
            );
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
                    RWEntry::Insert(true, new_pa.clone(), value.as_ref().to_vec()),
                );
                Ok(new_pa)
            } else {
                Err(TxnStorageStatus::KeyExists)
            }
        }
    }

    pub fn update<M: MemPool, K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        ps: &PrimaryStorage<M>,
        key: K,
        value: V,
        hint: Option<PhysicalAddress>,
    ) -> Result<PhysicalAddress, TxnStorageStatus> {
        let c_id = ps.c_id;
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
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(pa)
                }
                RWEntry::Update(_, pa, _) => {
                    let pa = pa.clone();
                    e.update_value(value.as_ref().to_vec());
                    Ok(pa)
                }
                RWEntry::Insert(inserted_as_ghost, pa, _) => {
                    // Insert UPDATE entry into rwset
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), value.as_ref().to_vec());
                    Ok(pa)
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Abort if not found in index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let page = storage
                .traverse_to_leaf_for_read_with_hint(key.as_ref(), hint.map(|h| h.to_pf_key(c_id)));
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_exclusive(key.as_ref().to_vec()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(
                    key.as_ref().to_vec(),
                    RWEntry::Update(false, new_pa.clone(), value.as_ref().to_vec()),
                );
                Ok(new_pa)
            }
        }
    }

    pub fn delete<M: MemPool, K: AsRef<[u8]>>(
        &self,
        ps: &PrimaryStorage<M>,
        key: K,
        hint: Option<PhysicalAddress>,
    ) -> Result<PhysicalAddress, TxnStorageStatus> {
        let c_id = ps.c_id;
        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some(e) = rwset.get_mut(key.as_ref()) {
            match e {
                RWEntry::Read(inserted_as_ghost, pa) => {
                    // Upgrade lock
                    let locktable: &Arc<LockTable> = &ps.locktable;
                    if !locktable.try_upgrade(key.as_ref().to_vec()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    // Insert DELETE entry into rwset
                    let pa = pa.clone();
                    *e = RWEntry::Delete(*inserted_as_ghost, pa.clone());
                    Ok(pa)
                }
                RWEntry::Update(inserted_as_ghost, pa, _)
                | RWEntry::Insert(inserted_as_ghost, pa, _) => {
                    // Insert DELETE entry into rwset.
                    // This should physically delete the record from storage on commit.
                    let pa = pa.clone();
                    *e = RWEntry::Delete(*inserted_as_ghost, pa.clone());
                    Ok(pa)
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Abort if not found in index
            let storage = &ps.btree;
            let locktable = &ps.locktable;
            let page = storage
                .traverse_to_leaf_for_read_with_hint(key.as_ref(), hint.map(|h| h.to_pf_key(c_id)));
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_exclusive(key.as_ref().to_vec()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(
                    key.as_ref().to_vec(),
                    RWEntry::Delete(false, new_pa.clone()),
                );
                Ok(new_pa)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn iter_next<M: MemPool>(
        &self,
        pi: &mut PrimaryIterator<M>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        if pi.finished {
            return Ok(None);
        }

        let c_id = pi.ps.c_id;
        let locktable = &pi.ps.locktable;

        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        loop {
            if let Some((key, value)) = pi.cursor.get_kv() {
                if !pi.options.upper_exc.is_empty() && key >= pi.options.upper_exc {
                    // Lock the upper key for phantom protection
                    if rwset.get(&key).is_none() {
                        // If the key is not in rwset, we need to lock it
                        if !locktable.try_shared(key.clone()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        rwset.insert(
                            key.clone(),
                            RWEntry::Read(false, PhysicalAddress::default()),
                        );
                    }
                    pi.finished = true; // We reached the upper bound
                    return Ok(None);
                }

                let (page_id, frame_id, _) = pi.cursor.get_physical_address();
                pi.cursor.go_to_next_kv();
                if let Some(e) = rwset.get_mut(&key) {
                    e.update_physical_address(PhysicalAddress::new(page_id, frame_id));
                    match e {
                        RWEntry::Read(_, _) => {
                            return Ok(Some((key, value)));
                        }
                        RWEntry::Update(_, _, new_val) | RWEntry::Insert(_, _, new_val) => {
                            return Ok(Some((key, new_val.clone())));
                        }
                        RWEntry::Delete(_, _) => {
                            continue; // Skip deleted entries
                        }
                    }
                } else {
                    // Lock the key
                    if !locktable.try_shared(key.clone()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    // Insert into rwset
                    rwset.insert(
                        key.clone(),
                        RWEntry::Read(false, PhysicalAddress::new(page_id, frame_id)),
                    );
                    return Ok(Some((key, value)));
                }
            } else {
                // No more entries in the primary index. Lock the &[] key for phantom protection.
                if rwset.get(&[]).is_none() {
                    // If the key is not in rwset, we need to lock it
                    if !locktable.try_shared([].to_vec()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    rwset.insert(
                        [].to_vec(),
                        RWEntry::Read(false, PhysicalAddress::default()),
                    );
                }
                pi.finished = true; // We reached the end of the primary index
                return Ok(None);
            }
        }
    }

    // Iterates over the secondary index and returns the value in the primary index
    #[allow(clippy::type_complexity)]
    pub fn iter_next_sec<M: MemPool>(
        &self,
        si: &mut SecondaryIterator<M>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        if si.finished {
            return Ok(None);
        }

        let c_id = si.ss.c_id;
        let locktable = &si.ss.locktable;

        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        let (s_key, s_value) = loop {
            if let Some((key, value)) = si.cursor.get_kv() {
                if !si.options.upper_exc.is_empty() && key >= si.options.upper_exc {
                    // Lock the upper key for phantom protection
                    if rwset.get(&key).is_none() {
                        // If the key is not in rwset, we need to lock it
                        if !locktable.try_shared(key.clone()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        rwset.insert(
                            key.clone(),
                            RWEntry::Read(false, PhysicalAddress::default()),
                        );
                    }
                    si.finished = true; // We reached the upper bound
                    return Ok(None);
                }

                let (page_id, frame_id, _) = si.cursor.get_physical_address();
                if let Some(e) = rwset.get_mut(&key) {
                    e.update_physical_address(PhysicalAddress::new(page_id, frame_id));
                    match e {
                        RWEntry::Read(_, _) => break (key, value),
                        RWEntry::Update(_, _, new_val) | RWEntry::Insert(_, _, new_val) => {
                            break (key, new_val.clone())
                        }
                        RWEntry::Delete(_, _) => {
                            si.cursor.go_to_next_kv();
                            continue; // Skip deleted entries
                        }
                    }
                } else {
                    // Lock the key
                    if !locktable.try_shared(key.clone()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    // Insert into rwset
                    rwset.insert(
                        key.clone(),
                        RWEntry::Read(false, PhysicalAddress::new(page_id, frame_id)),
                    );
                    break (key, value);
                }
            } else {
                // No more entries in the primary index. Lock the &[] key for phantom protection.
                if rwset.get(&[]).is_none() {
                    // If the key is not in rwset, we need to lock it
                    if !locktable.try_shared([].to_vec()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    rwset.insert(
                        [].to_vec(),
                        RWEntry::Read(false, PhysicalAddress::default()),
                    );
                }
                si.finished = true; // We reached the end of the primary index
                return Ok(None);
            }
        };

        // We got the primary key from the secondary index. Now, we need to get the value from the primary index.
        let ps = &si.ss.ps;
        let (p_key, p_hint) = s_value.split_at(s_value.len() - 8);
        let p_hint = PhysicalAddress::from_bytes(p_hint);
        let (p_value, _p_addr) = self.read(ps, p_key, Some(p_hint.clone()))?;
        // println!("Hint: {}, Actual: {}", p_hint, p_addr);
        // Rewrite the physical address in the secondary index. The last 8 bytes should be updated with the new physical address.
        #[cfg(not(feature = "no_tree_hint"))]
        if p_hint != _p_addr {
            // Update the physical address in the secondary index
            // println!("Sec Update From: {}, To: {}", p_hint, p_addr);
            let new_p_addr = _p_addr.to_bytes();
            let new_s_val = [p_key, &new_p_addr].concat();
            if p_hint.page_id != _p_addr.page_id {
                si.cursor.opportunistic_update(&new_s_val, true);
                si.page_hint_failed += 1;
            } else if p_hint.frame_id != _p_addr.frame_id {
                si.cursor.opportunistic_update(&new_s_val, false);
                si.frame_hint_failed += 1;
            }
        } else {
            si.hint_worked += 1;
        }
        si.cursor.go_to_next_kv();

        Ok(Some((s_key, p_value)))
    }

    pub fn commit<M: MemPool>(
        &self,
        pss: &PrimaryStorages<M>,
        sss: &SecondaryStorages<M>,
    ) -> Result<(), TxnStorageStatus> {
        // Release read locks first
        log_info!("NO-WAIT-TXN: Committing transaction");
        log_info!(" 1. Releasing read locks");
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let locktable = match pss.get(*c_id) {
                Some(ps) => &ps.locktable,
                None => {
                    let ss = sss.get(*c_id).unwrap(); // unwrap is safe because we only consider updates of secondary storages
                    &ss.locktable
                }
            };
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
        log_info!(" 2. Releasing write locks and applying updates");
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let (storage, locktable) = match pss.get(*c_id) {
                Some(ps) => (&ps.btree, &ps.locktable),
                None => {
                    let ss = sss.get(*c_id).unwrap(); // unwrap is safe because we only consider updates of secondary storages
                    (&ss.btree, &ss.locktable)
                }
            };
            for (key, e) in rwset.iter() {
                match e {
                    RWEntry::Read(_, _) => {
                        continue;
                    }
                    RWEntry::Update(inserted_as_ghost, pa, value) => {
                        let mut page = storage
                            .traverse_to_leaf_for_write_with_hint(key, Some(pa.to_pf_key(*c_id)));
                        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                        if slot_id == 0 || page.get_raw_key(slot_id) != key {
                            panic!("Key: {:?} of container: {} should exist in storage at slot_id: {} if it is in rwset", key, c_id, slot_id);
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
                            .traverse_to_leaf_for_write_with_hint(key, Some(pa.to_pf_key(*c_id)));
                        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                        if slot_id == 0 || page.get_raw_key(slot_id) != key {
                            panic!("Key: {:?} of container: {} should exist in storage at slot_id: {} if it is in rwset", key, c_id, slot_id);
                        } else {
                            page.unghostify_at(slot_id);
                        }
                    }
                    RWEntry::Delete(_, pa) => {
                        let mut page = storage
                            .traverse_to_leaf_for_write_with_hint(key, Some(pa.to_pf_key(*c_id)));
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

        log_info!("  Done committing transaction");

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
                        Some(e.physical_address().to_pf_key(*c_id)),
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

pub enum KVIterator<M: MemPool> {
    Primary(UnsafeCell<PrimaryIterator<M>>),
    Secondary(UnsafeCell<SecondaryIterator<M>>),
}

pub struct PrimaryIterator<M: MemPool> {
    options: ScanOptions,
    finished: bool,
    cursor: FosterBtreeCursor<M>,
    ps: Arc<PrimaryStorage<M>>,
}

impl<M: MemPool> PrimaryIterator<M> {
    pub fn new(options: ScanOptions, ps: Arc<PrimaryStorage<M>>) -> Self {
        let cursor = FosterBtreeCursor::new(&ps.btree, &options.lower_inc, &[]);
        PrimaryIterator {
            options,
            finished: false,
            cursor,
            ps,
        }
    }
}

pub struct SecondaryIterator<M: MemPool> {
    hint_worked: usize,
    page_hint_failed: usize,
    frame_hint_failed: usize,
    options: ScanOptions,
    finished: bool,
    cursor: FosterBtreeCursor<M>,
    ss: Arc<SecondaryStorage<M>>,
}

impl<M: MemPool> SecondaryIterator<M> {
    pub fn new(options: ScanOptions, ss: Arc<SecondaryStorage<M>>) -> Self {
        let cursor = FosterBtreeCursor::new(&ss.btree, &options.lower_inc, &[]);
        SecondaryIterator {
            hint_worked: 0,
            page_hint_failed: 0,
            frame_hint_failed: 0,
            options,
            finished: false,
            cursor,
            ss,
        }
    }
}

impl<M: MemPool> Drop for SecondaryIterator<M> {
    fn drop(&mut self) {
        trace_secidx(
            self.cursor.c_key().c_id() as u8,
            self.hint_worked as u32,
            self.page_hint_failed as u32,
            self.frame_hint_failed as u32,
        );
    }
}

pub struct NoWaitTxnStorage<M: MemPool> {
    metadata: Arc<FosterBtree<M>>,
    bp: Arc<M>,
    pss: PrimaryStorages<M>,
    sss: SecondaryStorages<M>,
}

impl<M: MemPool> NoWaitTxnStorage<M> {
    pub fn new(bp: &Arc<M>) -> Self {
        NoWaitTxnStorage {
            metadata: Arc::new(FosterBtree::new(
                ContainerKey::new(0, 0), // Metadata container id is 0
                bp.clone(),
            )),
            bp: bp.clone(),
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
        }
    }

    pub fn print_lock_tables(&self) {
        println!("NO-WAIT-TXN: Lock tables for primary storages:");
        self.pss.print_lock_tables();
        println!("NO-WAIT-TXN: Lock tables for secondary storages:");
        self.sss.print_lock_tables();
    }

    pub fn load(bp: &Arc<M>) -> Self {
        let metadata = Arc::new(FosterBtree::<M>::load(
            ContainerKey::new(0, 0),
            bp.clone(),
            0,
        ));
        let iter = metadata.scan();
        let mut primary_storages = Vec::new();
        let mut secondary_storages = Vec::new();
        for (k, v) in iter {
            let c_id = ContainerId::from_be_bytes(k.try_into().unwrap());
            let c_type = ContainerOptions::from_bytes(&v);
            match c_type.container_type() {
                ContainerType::Primary => {
                    primary_storages.push(c_id);
                }
                ContainerType::Secondary(primary_c_id) => {
                    secondary_storages.push((c_id, primary_c_id));
                }
            }
        }
        // Load the primary storages first.
        let pss = PrimaryStorages::new();
        for c_id in primary_storages {
            pss.load(bp, c_id);
        }
        // Load the secondary storages after.
        let sss = SecondaryStorages::new();
        for (c_id, primary_c_id) in secondary_storages {
            sss.load(bp, c_id, pss.get(primary_c_id).unwrap());
        }

        NoWaitTxnStorage {
            metadata,
            bp: bp.clone(),
            pss,
            sss,
        }
    }
}

impl<M: MemPool> TxnStorageTrait for NoWaitTxnStorage<M> {
    type TxnHandle = NoWaitTxn;
    type IteratorHandle = KVIterator<M>;

    // Only a single database supported right now.
    fn open_db(&self, _: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        Ok(0)
    }

    fn close_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        assert_eq!(db_id, 0);
        Ok(())
    }

    fn delete_db(&self, db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        assert_eq!(db_id, 0);
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
            ContainerType::Primary => {
                let c_id = self.pss.create_new(&self.bp);
                self.metadata
                    .insert(&(c_id as ContainerId).to_be_bytes(), &options.to_bytes())
                    .unwrap();
                Ok(c_id)
            }
            ContainerType::Secondary(primary_c_id) => {
                let ps = self.pss.get(primary_c_id).unwrap();
                let c_id = self.sss.create_new(&self.bp, ps);
                self.metadata
                    .insert(&(c_id as ContainerId).to_be_bytes(), &options.to_bytes())
                    .unwrap();
                Ok(c_id)
            }
        }
    }

    // Deleting a container is not transactional
    fn delete_container(
        &self,
        _db_id: DatabaseId,
        _c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        unimplemented!();
    }

    fn get_container_stats(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<String, TxnStorageStatus> {
        debug_assert_eq!(db_id, 0);
        match self.pss.get(c_id) {
            Some(ps) => Ok(ps.btree.page_stats(false)),
            None => match self.sss.get(c_id) {
                Some(ss) => Ok(ss.btree.page_stats(false)),
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn list_containers(
        &self,
        db_id: DatabaseId,
    ) -> Result<Vec<(ContainerId, ContainerOptions)>, TxnStorageStatus> {
        debug_assert_eq!(db_id, 0);
        let mut containers = Vec::new();
        for (k, v) in self.metadata.scan() {
            let c_id = ContainerId::from_be_bytes(k.try_into().unwrap());
            let c_type = ContainerOptions::from_bytes(&v);
            containers.push((c_id, c_type));
        }
        Ok(containers)
    }

    fn raw_insert_value(
        &self,
        _db_id: DatabaseId,
        c_id: ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        match self.pss.get(c_id) {
            Some(ps) => {
                ps.btree.insert(&key, &value).unwrap();
                Ok(())
            }
            None => match self.sss.get(c_id) {
                Some(ss) => {
                    let value = [value, PhysicalAddress::default().to_bytes().to_vec()].concat();
                    ss.btree.insert(&key, &value).unwrap();
                    Ok(())
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn begin_txn(
        &self,
        db_id: DatabaseId,
        _options: TxnOptions,
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
        if txn.commit(&self.pss, &self.sss).is_ok() {
            return Ok(());
        }
        if txn.abort(&self.pss).is_ok() {
            Err(TxnStorageStatus::Aborted)
        } else {
            Err(TxnStorageStatus::AbortFailed)
        }
    }

    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        if txn.abort(&self.pss).is_ok() {
            Ok(())
        } else {
            Err(TxnStorageStatus::AbortFailed)
        }
    }

    fn wait_for_txn(&self, _txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }

    fn drop_txn(&self, _txn: Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        unimplemented!()
    }

    fn num_values(
        &self,
        _txn: &Self::TxnHandle,
        _c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        unimplemented!()
    }

    fn check_value<K: AsRef<[u8]>>(
        &self,
        _txn: &Self::TxnHandle,
        _c_id: ContainerId,
        _key: K,
    ) -> Result<bool, TxnStorageStatus> {
        unimplemented!()
    }

    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        match self.pss.get(c_id) {
            Some(ps) => txn.read(ps, key, None).map(|(v, _)| v),
            None => match self.sss.get(c_id) {
                Some(_ss) => {
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
        match self.pss.get(c_id) {
            Some(ps) => {
                txn.insert(ps, key, value, None)?;
                Ok(())
            }
            None => match self.sss.get(c_id) {
                Some(_ss) => {
                    let value = [value, PhysicalAddress::default().to_bytes().to_vec()].concat();
                    txn.insert_secondary(_ss, key, value, None)?;
                    Ok(())
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
        match self.pss.get(c_id) {
            Some(ps) => {
                for (key, value) in kvs {
                    txn.insert(ps, key, value, None)?;
                }
            }
            None => match self.sss.get(c_id) {
                Some(_ss) => {
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
        match self.pss.get(c_id) {
            Some(ps) => txn.update(ps, key, value, None).map(|_| ()),
            None => match self.sss.get(c_id) {
                Some(_ss) => {
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
        match self.pss.get(c_id) {
            Some(ps) => {
                let (mut value, addr) = txn.read(ps, key.as_ref(), None)?;
                func(&mut value);
                txn.update(ps, key, value, Some(addr)).map(|_| ())
            }
            None => match self.sss.get(c_id) {
                Some(_ss) => {
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
        match self.pss.get(c_id) {
            Some(ps) => txn.delete(ps, key, None).map(|_| ()),
            None => match self.sss.get(c_id) {
                Some(_ss) => {
                    unimplemented!()
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn scan_range(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: super::ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        match self.pss.get(c_id) {
            Some(ps) => {
                let iter = PrimaryIterator::new(options, ps.clone());
                Ok(KVIterator::Primary(UnsafeCell::new(iter)))
            }
            None => match self.sss.get(c_id) {
                Some(ss) => {
                    let iter = SecondaryIterator::new(options, ss.clone());
                    Ok(KVIterator::Secondary(UnsafeCell::new(iter)))
                }
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
    }

    fn iter_next(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        match iter {
            KVIterator::Primary(iter) => {
                let iter = unsafe { &mut *iter.get() };
                txn.iter_next(iter)
            }
            KVIterator::Secondary(iter) => {
                let iter = unsafe { &mut *iter.get() };
                txn.iter_next_sec(iter)
            }
        }
    }

    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus> {
        drop(iter);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bp::get_test_bp_lru,
        prelude::{ContainerDS, ContainerOptions, DBOptions, TxnOptions},
    };

    use super::*;

    #[test]
    fn test_insert_and_read_back() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);

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
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);

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
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);

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
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);

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
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);

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

    #[test]
    fn test_container_durability() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);

        // Open a database
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        let mut options = vec![];
        let option = ContainerOptions::primary("testtable1", ContainerDS::BTree);
        // Create a container
        let c_id1 = storage.create_container(db_id, option.clone()).unwrap();
        options.push((c_id1, option));

        // Create a secondary container
        let option = ContainerOptions::secondary("testtable2", ContainerDS::BTree, c_id1);
        let s_id1 = storage.create_container(db_id, option.clone()).unwrap();
        options.push((s_id1, option));

        // Create another container
        let option = ContainerOptions::primary("testtable3", ContainerDS::BTree);
        let c_id2 = storage.create_container(db_id, option.clone()).unwrap();
        options.push((c_id2, option));

        // Load the storage again to verify durability
        let new_storage = NoWaitTxnStorage::load(&bp);
        let mut all_containers = new_storage.list_containers(0).unwrap();

        // Check that all containers are present
        options.sort_by(|a, b| a.0.cmp(&b.0));
        all_containers.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(options, all_containers);
    }

    #[test]
    fn test_phantom_protection_insert() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert some initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key3".to_vec(), b"value3".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Scan range [key1, key3]
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let iter = storage
            .scan_range(
                &txn1,
                c_id,
                ScanOptions {
                    lower_inc: b"key1".to_vec(),
                    upper_exc: b"key4".to_vec(),
                },
            )
            .unwrap();

        // Read first value to establish locks
        let result = storage.iter_next(&txn1, &iter).unwrap();
        assert_eq!(result, Some((b"key1".to_vec(), b"value1".to_vec())));
        let result = storage.iter_next(&txn1, &iter).unwrap();
        assert_eq!(result, Some((b"key3".to_vec(), b"value3".to_vec())));
        let result = storage.iter_next(&txn1, &iter).unwrap();
        assert_eq!(result, None);
        storage.drop_iterator_handle(iter).unwrap();

        // Transaction 2: Try to insert key2 (should fail due to phantom protection)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.insert_value(&txn2, c_id, b"key2".to_vec(), b"value2".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));
        storage.abort_txn(&txn2).unwrap();

        storage.commit_txn(&txn1, false).unwrap();
    }

    #[test]
    fn test_range_scan_with_updates() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 1..=5 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            storage
                .insert_value(&txn_init, c_id, key.into_bytes(), value.into_bytes())
                .unwrap();
        }
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction: Update some values and then scan
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Update key02 and key04
        storage
            .update_value(&txn, c_id, b"key02", b"updated2".to_vec())
            .unwrap();
        storage
            .update_value(&txn, c_id, b"key04", b"updated4".to_vec())
            .unwrap();

        // Scan the range
        let iter = storage
            .scan_range(
                &txn,
                c_id,
                ScanOptions {
                    lower_inc: b"key01".to_vec(),
                    upper_exc: b"key06".to_vec(),
                },
            )
            .unwrap();

        // Verify we see the updated values
        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key01".to_vec(), b"value1".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key02".to_vec(), b"updated2".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key03".to_vec(), b"value3".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key04".to_vec(), b"updated4".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key05".to_vec(), b"value5".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, None);

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_scan_with_deletes() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 1..=5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            storage
                .insert_value(&txn_init, c_id, key.into_bytes(), value.into_bytes())
                .unwrap();
        }
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction: Delete some keys and scan
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Delete key2 and key4
        storage.delete_value(&txn, c_id, b"key2").unwrap();
        storage.delete_value(&txn, c_id, b"key4").unwrap();

        // Scan should skip deleted entries
        let iter = storage
            .scan_range(
                &txn,
                c_id,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key1".to_vec(), b"value1".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key3".to_vec(), b"value3".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"key5".to_vec(), b"value5".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, None);

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_empty_scan_phantom_protection() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Transaction 1: Scan empty range
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let iter = storage
            .scan_range(
                &txn1,
                c_id,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();

        // Read to establish phantom lock on empty key
        let result = storage.iter_next(&txn1, &iter).unwrap();
        assert_eq!(result, None);

        // Transaction 2: Try to insert (should fail due to phantom protection)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.insert_value(&txn2, c_id, b"key1".to_vec(), b"value1".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        storage.commit_txn(&txn1, false).unwrap();
    }

    #[test]
    fn test_secondary_index_basic() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Create primary container
        let primary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::primary("primary_table", ContainerDS::BTree),
            )
            .unwrap();

        // Create secondary container
        let secondary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::secondary("secondary_index", ContainerDS::BTree, primary_cid),
            )
            .unwrap();

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert data into primary table
        storage
            .insert_value(&txn, primary_cid, b"pk1".to_vec(), b"John Doe".to_vec())
            .unwrap();
        storage
            .insert_value(&txn, primary_cid, b"pk2".to_vec(), b"Jane Smith".to_vec())
            .unwrap();

        // Insert corresponding entries in secondary index (last name -> primary key)
        storage
            .insert_value(&txn, secondary_cid, b"Doe".to_vec(), b"pk1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn, secondary_cid, b"Smith".to_vec(), b"pk2".to_vec())
            .unwrap();

        // Test scan within same transaction
        let iter = storage
            .scan_range(
                &txn,
                secondary_cid,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"Doe".to_vec(), b"John Doe".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"Smith".to_vec(), b"Jane Smith".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, None);

        drop(iter);

        storage.commit_txn(&txn, false).unwrap();

        // Test secondary index scan in a new transaction
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let iter = storage
            .scan_range(
                &txn2,
                secondary_cid,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();

        // Should return values from primary storage
        let result = storage.iter_next(&txn2, &iter).unwrap();
        assert_eq!(result, Some((b"Doe".to_vec(), b"John Doe".to_vec())));

        let result = storage.iter_next(&txn2, &iter).unwrap();
        assert_eq!(result, Some((b"Smith".to_vec(), b"Jane Smith".to_vec())));

        let result = storage.iter_next(&txn2, &iter).unwrap();
        assert_eq!(result, None);

        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_secondary_index_with_updates() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Create primary and secondary containers
        let primary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::primary("primary_table", ContainerDS::BTree),
            )
            .unwrap();

        let secondary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::secondary("secondary_index", ContainerDS::BTree, primary_cid),
            )
            .unwrap();

        // Initial data setup
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, primary_cid, b"pk1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, secondary_cid, b"sk1".to_vec(), b"pk1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Update primary record and verify secondary index still works
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value(&txn, primary_cid, b"pk1", b"updated_value".to_vec())
            .unwrap();

        // Scan through secondary index should return updated value
        let iter = storage
            .scan_range(
                &txn,
                secondary_cid,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"sk1".to_vec(), b"updated_value".to_vec())));

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_secondary_index_conflict() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Create primary and secondary containers
        let primary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::primary("primary_table", ContainerDS::BTree),
            )
            .unwrap();

        let secondary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::secondary("secondary_index", ContainerDS::BTree, primary_cid),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, primary_cid, b"pk1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, secondary_cid, b"sk1".to_vec(), b"pk1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Lock through secondary index scan
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let iter = storage
            .scan_range(
                &txn1,
                secondary_cid,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();
        let result = storage.iter_next(&txn1, &iter).unwrap();
        assert_eq!(result, Some((b"sk1".to_vec(), b"value1".to_vec())));

        // Transaction 2: Try to update the primary record (should conflict)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.update_value(&txn2, primary_cid, b"pk1", b"new_value".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        storage.commit_txn(&txn1, false).unwrap();
    }

    #[test]
    fn test_update_value_with_func() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial value
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"counter".to_vec(), b"0000".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Update value using function
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value_with_func(&txn, c_id, b"counter", |value| {
                // Increment counter
                let current = std::str::from_utf8(value).unwrap().parse::<u32>().unwrap();
                let new_value = format!("{:04}", current + 1);
                value.copy_from_slice(new_value.as_bytes());
            })
            .unwrap();

        // Verify the update
        let value = storage.get_value(&txn, c_id, b"counter").unwrap();
        assert_eq!(value, b"0001");

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_concurrent_read_write() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Read the value
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn1, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");

        // Transaction 2: Try to write (should conflict)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.update_value(&txn2, c_id, b"key1", b"value2".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Transaction 3: Another reader should succeed
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn3, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");

        storage.commit_txn(&txn1, false).unwrap();
        storage.commit_txn(&txn3, false).unwrap();
    }

    #[test]
    fn test_write_write_conflict() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Update the value
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value(&txn1, c_id, b"key1", b"value2".to_vec())
            .unwrap();

        // Transaction 2: Try to update same key (should conflict)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.update_value(&txn2, c_id, b"key1", b"value3".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Transaction 2: Try to delete same key (should also conflict)
        let result = storage.delete_value(&txn2, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        storage.commit_txn(&txn1, false).unwrap();
    }

    #[test]
    fn test_read_then_upgrade_conflict() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Read the value
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn1, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");

        // Transaction 2: Also read the value
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn2, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");

        // Transaction 1: Try to upgrade to write lock (should fail)
        let result = storage.update_value(&txn1, c_id, b"key1", b"value2".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_multiple_keys_transaction() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key2".to_vec(), b"value2".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key3".to_vec(), b"value3".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Lock key1 and key3
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value(&txn1, c_id, b"key1", b"updated1".to_vec())
            .unwrap();
        storage
            .update_value(&txn1, c_id, b"key3", b"updated3".to_vec())
            .unwrap();

        // Transaction 2: Can work with key2
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value(&txn2, c_id, b"key2", b"updated2".to_vec())
            .unwrap();

        // But cannot access key1 or key3
        let result = storage.get_value(&txn2, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        let result = storage.get_value(&txn2, c_id, b"key3");
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Both transactions can commit
        storage.commit_txn(&txn1, false).unwrap();
        storage.commit_txn(&txn2, false).unwrap();

        // Verify final state
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        assert_eq!(
            storage.get_value(&txn3, c_id, b"key1").unwrap(),
            b"updated1"
        );
        assert_eq!(
            storage.get_value(&txn3, c_id, b"key2").unwrap(),
            b"updated2"
        );
        assert_eq!(
            storage.get_value(&txn3, c_id, b"key3").unwrap(),
            b"updated3"
        );
        storage.commit_txn(&txn3, false).unwrap();
    }

    #[test]
    fn test_deadlock_avoidance() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key2".to_vec(), b"value2".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Lock key1
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value(&txn1, c_id, b"key1", b"updated1".to_vec())
            .unwrap();

        // Transaction 2: Lock key2
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_value(&txn2, c_id, b"key2", b"updated2".to_vec())
            .unwrap();

        // Transaction 1: Try to lock key2 (would cause deadlock in wait-die, but fails immediately here)
        let result = storage.update_value(&txn1, c_id, b"key2", b"updated2_by_txn1".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Transaction 2: Try to lock key1 (would cause deadlock in wait-die, but fails immediately here)
        let result = storage.update_value(&txn2, c_id, b"key1", b"updated1_by_txn2".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Abort txn1 and commit txn2
        storage.abort_txn(&txn1).unwrap();
        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_ghost_record_insert_abort() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Transaction 1: Insert a ghost record but abort
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn1, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Before abort, the key should be visible to the same transaction
        let value = storage.get_value(&txn1, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");

        // Abort the transaction
        storage.abort_txn(&txn1).unwrap();

        // Transaction 2: The key should not exist after abort
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.get_value(&txn2, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));
        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_ghost_record_visibility() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Transaction 1: Insert a ghost record
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn1, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Transaction 2: Should not see the ghost record
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.get_value(&txn2, c_id, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Transaction 2: Scan will conflict when trying to lock the ghost record
        let iter = storage
            .scan_range(
                &txn2,
                c_id,
                ScanOptions {
                    lower_inc: vec![],
                    upper_exc: vec![],
                },
            )
            .unwrap();
        let result = storage.iter_next(&txn2, &iter);
        // Should get a conflict because txn1 holds exclusive lock on key1
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Commit txn1 - ghost record becomes visible
        storage.commit_txn(&txn1, false).unwrap();

        // Transaction 3: Should now see the record
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn3, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");
        storage.commit_txn(&txn3, false).unwrap();
    }

    #[test]
    fn test_delete_then_insert_same_key() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction: Delete then insert same key
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Delete the key
        storage.delete_value(&txn, c_id, b"key1").unwrap();

        // Insert the same key with new value (should work as update)
        let result = storage.insert_value(&txn, c_id, b"key1".to_vec(), b"value2".to_vec());
        assert!(result.is_ok());

        // Read should return the new value
        let value = storage.get_value(&txn, c_id, b"key1").unwrap();
        assert_eq!(value, b"value2");

        storage.commit_txn(&txn, false).unwrap();

        // Verify final state
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn2, c_id, b"key1").unwrap();
        assert_eq!(value, b"value2");
        storage.commit_txn(&txn2, false).unwrap();
    }

    #[test]
    fn test_lock_upgrade_in_rwset() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction: Read then update (lock upgrade)
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // First read the value (acquires read lock)
        let value = storage.get_value(&txn, c_id, b"key1").unwrap();
        assert_eq!(value, b"value1");

        // Then update it (upgrades to write lock)
        storage
            .update_value(&txn, c_id, b"key1", b"value2".to_vec())
            .unwrap();

        // Read again should see the updated value
        let value = storage.get_value(&txn, c_id, b"key1").unwrap();
        assert_eq!(value, b"value2");

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_next_key_locking() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"key3".to_vec(), b"value3".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Transaction 1: Read key3
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn1, c_id, b"key3").unwrap();
        assert_eq!(value, b"value3");

        // Transaction 2: Try to insert key2 (requires next-key lock on key3)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.insert_value(&txn2, c_id, b"key2".to_vec(), b"value2".to_vec());
        // Should fail because txn1 holds read lock on key3
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        storage.commit_txn(&txn1, false).unwrap();
    }

    #[test]
    fn test_insert_values_batch() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Transaction: Insert multiple values
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let kvs = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        storage.insert_values(&txn, c_id, kvs).unwrap();

        // Verify all values are inserted
        assert_eq!(storage.get_value(&txn, c_id, b"key1").unwrap(), b"value1");
        assert_eq!(storage.get_value(&txn, c_id, b"key2").unwrap(), b"value2");
        assert_eq!(storage.get_value(&txn, c_id, b"key3").unwrap(), b"value3");

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_empty_key_operations() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Transaction: Work with empty keys
        // Note: Empty keys are treated specially in B+tree as infinity keys,
        // so we'll test with a minimal non-empty key instead
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert with minimal key
        let min_key = vec![0u8];
        storage
            .insert_value(&txn, c_id, min_key.clone(), b"min_key_value".to_vec())
            .unwrap();

        // Read minimal key
        let value = storage.get_value(&txn, c_id, &min_key).unwrap();
        assert_eq!(value, b"min_key_value");

        // Update minimal key
        storage
            .update_value(&txn, c_id, &min_key, b"updated_min".to_vec())
            .unwrap();

        // Delete minimal key
        storage.delete_value(&txn, c_id, &min_key).unwrap();

        // Should not exist anymore
        let result = storage.get_value(&txn, c_id, &min_key);
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_container_not_found() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Try operations on non-existent container
        let invalid_cid = 9999;

        let result = storage.get_value(&txn, invalid_cid, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::ContainerNotFound)));

        let result = storage.insert_value(&txn, invalid_cid, b"key1".to_vec(), b"value1".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::ContainerNotFound)));

        let result = storage.update_value(&txn, invalid_cid, b"key1", b"value1".to_vec());
        assert!(matches!(result, Err(TxnStorageStatus::ContainerNotFound)));

        let result = storage.delete_value(&txn, invalid_cid, b"key1");
        assert!(matches!(result, Err(TxnStorageStatus::ContainerNotFound)));

        let result = storage.scan_range(
            &txn,
            invalid_cid,
            ScanOptions {
                lower_inc: vec![],
                upper_exc: vec![],
            },
        );
        assert!(matches!(result, Err(TxnStorageStatus::ContainerNotFound)));
    }

    #[test]
    fn test_large_key_value() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Create large key and value
        let large_key = vec![b'k'; 1000];
        let large_value = vec![b'v'; 10000];

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert large key-value
        storage
            .insert_value(&txn, c_id, large_key.clone(), large_value.clone())
            .unwrap();

        // Read it back
        let value = storage.get_value(&txn, c_id, &large_key).unwrap();
        assert_eq!(value, large_value);

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_raw_insert_value() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        // Test primary container
        let primary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::primary("primary_table", ContainerDS::BTree),
            )
            .unwrap();

        // Raw insert into primary
        storage
            .raw_insert_value(db_id, primary_cid, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Verify with transaction
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let value = storage.get_value(&txn, primary_cid, b"key1").unwrap();
        assert_eq!(value, b"value1");
        storage.commit_txn(&txn, false).unwrap();

        // Test secondary container
        let secondary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::secondary("secondary_index", ContainerDS::BTree, primary_cid),
            )
            .unwrap();

        // Raw insert into secondary
        storage
            .raw_insert_value(db_id, secondary_cid, b"sk1".to_vec(), b"key1".to_vec())
            .unwrap();
    }

    #[test]
    fn test_physical_address_serialization() {
        let pa = PhysicalAddress::new(12345, 67890);
        let bytes = pa.to_bytes();
        let pa2 = PhysicalAddress::from_bytes(&bytes);
        assert_eq!(pa, pa2);

        // Test default
        let default_pa = PhysicalAddress::default();
        assert_eq!(default_pa.page_id, 0);
        assert_eq!(default_pa.frame_id, u32::MAX);
    }

    #[test]
    fn test_container_stats() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();

        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert some data
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            storage
                .insert_value(&txn, c_id, key.into_bytes(), value.into_bytes())
                .unwrap();
        }
        storage.commit_txn(&txn, false).unwrap();

        // Get stats
        let stats = storage.get_container_stats(db_id, c_id).unwrap();
        assert!(!stats.is_empty());

        // Invalid container
        let result = storage.get_container_stats(db_id, 9999);
        assert!(matches!(result, Err(TxnStorageStatus::ContainerNotFound)));
    }

    #[test]
    fn test_scan_boundary_conditions() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        // Insert data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn_init, c_id, b"a".to_vec(), b"value_a".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"b".to_vec(), b"value_b".to_vec())
            .unwrap();
        storage
            .insert_value(&txn_init, c_id, b"c".to_vec(), b"value_c".to_vec())
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Scan with exact boundary match
        let iter = storage
            .scan_range(
                &txn,
                c_id,
                ScanOptions {
                    lower_inc: b"b".to_vec(),
                    upper_exc: b"b".to_vec(),
                },
            )
            .unwrap();

        // Should return nothing (upper bound is exclusive)
        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, None);

        // Scan from b to c
        let iter = storage
            .scan_range(
                &txn,
                c_id,
                ScanOptions {
                    lower_inc: b"b".to_vec(),
                    upper_exc: b"c".to_vec(),
                },
            )
            .unwrap();

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, Some((b"b".to_vec(), b"value_b".to_vec())));

        let result = storage.iter_next(&txn, &iter).unwrap();
        assert_eq!(result, None);

        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_rwset_display() {
        let bp = get_test_bp_lru(10);
        let storage = NoWaitTxnStorage::new(&bp);
        let db_id = storage.open_db(DBOptions::new("testdb")).unwrap();
        let c_id = storage
            .create_container(
                db_id,
                ContainerOptions::primary("testtable", ContainerDS::BTree),
            )
            .unwrap();

        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Perform various operations to populate rwset
        storage
            .insert_value(&txn, c_id, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .update_value(&txn, c_id, b"key1", b"value2".to_vec())
            .unwrap();

        // Test Display implementation
        let txn_str = format!("{}", txn);
        assert!(txn_str.contains("key1"));

        storage.commit_txn(&txn, false).unwrap();
    }
}
