use core::panic;
use std::cell::UnsafeCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use crate::access_method::fbt::{
    BTreeKey, FosterBtreeAppendOnly, FosterBtreeAppendOnlyCursor, FosterBtreeCursor,
};
use crate::bp::PageFrameKey;
use crate::page::PageId;
use crate::prelude::{FosterBtreePage, UniqueKeyIndex};
use crate::txn_storage::TxnStorageStatus;
use crate::{
    bp::prelude::{ContainerId, DatabaseId, MemPool},
    prelude::{ContainerKey, FosterBtree},
};

use super::txn_storage_trait::ContainerType;
use super::{ContainerOptions, DBOptions, TxnOptions, TxnStorageTrait};

#[allow(unused_imports)]
use crate::log;
use crate::log_info;

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
    Read(bool, PhysicalAddress, u64), // inserted_as_ghost, physical_address
    Update(bool, PhysicalAddress, u64, Vec<u8>), // inserted_as_ghost, physical_address, value
    Insert(bool, PhysicalAddress, u64, Vec<u8>), // inserted_as_ghost, physical_address, value
    Delete(bool, PhysicalAddress, u64), // inserted_as_ghost, physical_address
}

impl Display for RWEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RWEntry::Read(inserted_as_ghost, pa, version) => {
                write!(f, "Read(is_ghost({}), version({}), {})", inserted_as_ghost, version, pa)
            }
            RWEntry::Update(inserted_as_ghost, pa, version, value) => {
                write!(
                    f,
                    "Update(is_ghost({}), version({}), {}, {:?})",
                    inserted_as_ghost, version, pa, value
                )
            }
            RWEntry::Insert(inserted_as_ghost, pa, version, value) => {
                write!(
                    f,
                    "Insert(is_ghost({}), version({}), {}, {:?})",
                    inserted_as_ghost, version, pa, value
                )
            }
            RWEntry::Delete(inserted_as_ghost, version, pa) => {
                write!(f, "Delete(is_ghost({}), version({}), {})", inserted_as_ghost, version, pa)
            }
        }
    }
}

impl RWEntry {
    pub fn update_physical_address(&mut self, new_pa: PhysicalAddress) {
        match self {
            RWEntry::Read(_, pa, ..) => {
                *pa = new_pa;
            }
            RWEntry::Update(_, pa, ..) => {
                *pa = new_pa;
            }
            RWEntry::Insert(_, pa, ..) => {
                *pa = new_pa;
            }
            RWEntry::Delete(_, pa, ..) => {
                *pa = new_pa;
            }
        }
    }

    pub fn update_value(&mut self, value: Vec<u8>) {
        match self {
            RWEntry::Read(..) => {
                panic!("Cannot update value of a read element")
            }
            RWEntry::Update(.., v) => {
                *v = value;
            }
            RWEntry::Insert(.., v) => {
                *v = value;
            }
            RWEntry::Delete(..) => {
                panic!("Cannot update value of a delete element")
            }
        }
    }

    pub fn ghost_inserted(&self) -> bool {
        match self {
            RWEntry::Read(inserted_as_ghost, ..) => *inserted_as_ghost,
            RWEntry::Update(inserted_as_ghost, ..) => *inserted_as_ghost,
            RWEntry::Insert(inserted_as_ghost, ..) => *inserted_as_ghost,
            RWEntry::Delete(inserted_as_ghost, ..) => *inserted_as_ghost,
        }
    }

    pub fn physical_address(&self) -> PhysicalAddress {
        match self {
            RWEntry::Read(_, pa, ..) => pa.clone(),
            RWEntry::Update(_, pa, .., ) => pa.clone(),
            RWEntry::Insert(_, pa, .., ) => pa.clone(),
            RWEntry::Delete(_, pa, ..) => pa.clone(),
        }
    }

    pub fn version(&self) -> u64 {
        match self {
            RWEntry::Read(_, _, version) => *version,
            RWEntry::Update(_, _, version, _) => *version,
            RWEntry::Insert(_, _, version, _) => *version,
            RWEntry::Delete(_, _, version) => *version,
        }
    }
}

pub struct ReadWriteSet {
    pub rwset: UnsafeCell<BTreeMap<Vec<u8>, RWEntry>>,
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
            rwset: UnsafeCell::new(BTreeMap::new()),
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

    pub fn iter(&self) -> std::collections::btree_map::Iter<Vec<u8>, RWEntry> {
        let rwset = unsafe { &*self.rwset.get() };
        rwset.iter()
    }
}

static CONTAINER_ID_COUNTER: AtomicU16 = AtomicU16::new(0);

// UniqueKeyIndex
pub struct PrimaryStorage<M: MemPool> {
    pub c_id: ContainerId,
    pub btree: Arc<FosterBtree<M>>,
}

pub struct PrimaryStorages<M: MemPool> {
    pub map: UnsafeCell<BTreeMap<ContainerId, Arc<PrimaryStorage<M>>>>,
}

impl<M: MemPool> Default for PrimaryStorages<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: MemPool> PrimaryStorages<M> {
    pub fn new() -> Self {
        PrimaryStorages {
            map: UnsafeCell::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, c_id: ContainerId) -> Option<&Arc<PrimaryStorage<M>>> {
        let map = unsafe { &*self.map.get() };
        map.get(&c_id)
    }

    pub fn create_new(&self, bp: &Arc<M>) -> ContainerId {
        let map: &mut BTreeMap<u16, Arc<PrimaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let c_id = CONTAINER_ID_COUNTER.fetch_add(1, Ordering::AcqRel);
        let btree = Arc::new(FosterBtree::new(ContainerKey::new(0, c_id), bp.clone()));
        map.insert(
            c_id,
            Arc::new(PrimaryStorage {
                c_id,
                btree,
            }),
        );
        c_id
    }
}

unsafe impl<M: MemPool> Sync for PrimaryStorages<M> {}

pub struct SecondaryStorage<M: MemPool> {
    pub c_id: ContainerId,
    pub btree: Arc<FosterBtreeAppendOnly<M>>,
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

    pub fn get(&self, c_id: ContainerId) -> Option<&Arc<SecondaryStorage<M>>> {
        let map = unsafe { &*self.map.get() };
        map.get(&c_id)
    }

    pub fn create_new(&self, bp: &Arc<M>, ps: &Arc<PrimaryStorage<M>>) -> ContainerId {
        let map: &mut HashMap<u16, Arc<SecondaryStorage<M>>> = unsafe { &mut *self.map.get() };
        let c_id = CONTAINER_ID_COUNTER.fetch_add(1, Ordering::AcqRel);
        let btree = Arc::new(FosterBtreeAppendOnly::new(
            ContainerKey::new(0, c_id),
            bp.clone(),
        ));
        map.insert(
            c_id,
            Arc::new(SecondaryStorage {
                c_id,
                btree,
                ps: ps.clone(),
            }),
        );
        c_id
    }
}

unsafe impl<M: MemPool> Sync for SecondaryStorages<M> {}

pub struct SiloTxn {
    rwset: UnsafeCell<HashMap<ContainerId, ReadWriteSet>>, // Read-write set
}

impl Display for SiloTxn {
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


// Method to extract versions from the value bytes
fn get_version(versioned_value: &[u8]) -> u64 {
    let version_with_lock = u64::from_be_bytes(versioned_value[0..8].try_into().unwrap());
    // A version is a 64-bit integer. The first 1 bit is reserved for lock.
    version_with_lock & 0xFFFFFFFFFFFFFFFE
}

fn get_value(versioned_value: &[u8]) -> &[u8] {
    &versioned_value[8..]
}

// If return value is true, the record is locked.
fn is_locked(versioned_value: &[u8]) -> bool {
    let version_with_lock = u64::from_be_bytes(versioned_value[0..8].try_into().unwrap());
    (version_with_lock & 1) == 1
}

// Method to update versions in the value bytes
fn set_version(versioned_value: &[u8], version: u64, lock: bool){
    let version_with_lock = version | (lock as u64);
    let value_ptr: *const u8 = versioned_value.as_ptr();
    unsafe {
        let value_ptr = value_ptr as *mut u8;
        std::ptr::copy_nonoverlapping(&version_with_lock.to_be_bytes()[0], value_ptr, 8);
    }
}

fn set_lock(versioned_value: &mut [u8], lock: bool) {
    let version_with_lock = u64::from_be_bytes(versioned_value[0..8].try_into().unwrap());
    let new_version_with_lock = version_with_lock | (lock as u64);
    let value_ptr: *const u8 = versioned_value.as_ptr();
    unsafe {
        let value_ptr = value_ptr as *mut u8;
        std::ptr::copy_nonoverlapping(&new_version_with_lock.to_be_bytes()[0], value_ptr, 8);
    }
}

impl SiloTxn {
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
                RWEntry::Read(_, pa, old_version, ..) => {
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
                        let versioned_value = page.get_val(slot_id);
                        if is_locked(versioned_value) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        let new_version = get_version(versioned_value);
                        if new_version != *old_version {
                            // Version mismatch
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        let value = get_value(versioned_value);
                        // Update the physical address
                        let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                        e.update_physical_address(new_pa.clone());
                        Ok((value.to_vec(), new_pa))
                    }
                }
                RWEntry::Update(_, pa, _, value) | RWEntry::Insert(_, pa, _, value) => {
                    Ok((value.clone(), pa.clone()))
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Find from index
            let storage = &ps.btree;
            let page = storage
                .traverse_to_leaf_for_read_with_hint(key.as_ref(), hint.map(|h| h.to_pf_key(c_id)));
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                // Lower fence or non-existent key
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Insert into rwset
                let versioned_value = page.get_val(slot_id);
                if is_locked(versioned_value) {
                    return Err(TxnStorageStatus::TxnConflict);
                } else {
                    let version = get_version(versioned_value);
                    let value = get_value(versioned_value);
                    let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                    rwset.insert(key.as_ref().to_vec(), RWEntry::Read(false, new_pa.clone(), version));
                    Ok((value.to_vec(), new_pa))
                }
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
                RWEntry::Read(..) | RWEntry::Update(..) | RWEntry::Insert(..) => {
                    Err(TxnStorageStatus::KeyExists)
                }
                RWEntry::Delete(inserted_as_ghost, pa, version) => {
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), *version, value.as_ref().to_vec());
                    Ok(pa)
                }
            }
        } else {
            // Find from index
            let storage = &ps.btree;
            let mut page = storage.traverse_to_leaf_for_write_with_hint(
                key.as_ref(),
                hint.map(|h| h.to_pf_key(c_id)),
            );
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                let mut versioned_value = [0u8; 8].to_vec();
                versioned_value.extend_from_slice(value.as_ref());
                let version = 0;
                let lock = true;
                set_version(&mut versioned_value, version, lock);
                
                // Insert the key-value as ghost record
                storage.insert_at_slot_or_split(
                    &mut page,
                    slot_id + 1,
                    key.as_ref(),
                    &versioned_value,
                    true,
                );

                // Insert this key into rwset
                let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                rwset.insert(
                    key.as_ref().to_vec(),
                    RWEntry::Insert(true, new_pa.clone(), version, value.as_ref().to_vec())
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
                RWEntry::Read(inserted_as_ghost, pa, version) => {
                    // Insert UPDATE entry into rwset
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), *version, value.as_ref().to_vec());
                    Ok(pa)
                }
                RWEntry::Update(_, pa, ..) => {
                    let pa = pa.clone();
                    e.update_value(value.as_ref().to_vec());
                    Ok(pa)
                }
                RWEntry::Insert(inserted_as_ghost, pa, version, ..) => {
                    // Insert UPDATE entry into rwset
                    let pa = pa.clone();
                    *e = RWEntry::Update(*inserted_as_ghost, pa.clone(), *version, value.as_ref().to_vec());
                    Ok(pa)
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Abort if not found in index
            let storage = &ps.btree;
            let page = storage
                .traverse_to_leaf_for_read_with_hint(key.as_ref(), hint.map(|h| h.to_pf_key(c_id)));
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                let versioned_value = page.get_val(slot_id);
                if is_locked(versioned_value) {
                    return Err(TxnStorageStatus::TxnConflict);
                } else {
                    let version = get_version(versioned_value);
                    let value = get_value(versioned_value);
                    // Insert into rwset
                    let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                    rwset.insert(
                        key.as_ref().to_vec(),
                        RWEntry::Update(false, new_pa.clone(), version, value.to_vec()),
                    );
                    Ok(new_pa)
                }
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
                RWEntry::Read(inserted_as_ghost, pa, version) => {
                    // Insert DELETE entry into rwset
                    let pa = pa.clone();
                    *e = RWEntry::Delete(*inserted_as_ghost, pa.clone(), *version);
                    Ok(pa)
                }
                RWEntry::Update(inserted_as_ghost, pa, version, ..)
                | RWEntry::Insert(inserted_as_ghost, pa, version, ..) => {
                    // Insert DELETE entry into rwset.
                    // This should physically delete the record from storage on commit.
                    let pa = pa.clone();
                    *e = RWEntry::Delete(*inserted_as_ghost, pa.clone(), *version);
                    Ok(pa)
                }
                RWEntry::Delete(..) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Abort if not found in index
            let storage = &ps.btree;
            let page = storage
                .traverse_to_leaf_for_read_with_hint(key.as_ref(), hint.map(|h| h.to_pf_key(c_id)));
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key.as_ref())) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key.as_ref() {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                let versioned_value = page.get_val(slot_id);
                if is_locked(versioned_value) {
                    return Err(TxnStorageStatus::TxnConflict);
                } else {
                    let version = get_version(versioned_value);
                    // Insert into rwset
                    let new_pa = PhysicalAddress::new(page.get_id(), page.frame_id());
                    rwset.insert(
                        key.as_ref().to_vec(),
                        RWEntry::Delete(false, new_pa.clone(), version),
                    );
                    Ok(new_pa)
                }
            }
        }
    }

    pub fn iter_next<M: MemPool>(
        &self,
        pi: &mut PrimaryIterator<M>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        let c_id = pi.ps.c_id;

        let rwset_all = unsafe { &mut *self.rwset.get() };
        let rwset = rwset_all.entry(c_id).or_insert_with(ReadWriteSet::new);
        if let Some((key, versioned_value)) = pi.cursor.get_kv() {
            if is_locked(&versioned_value) {
                return Err(TxnStorageStatus::TxnConflict);
            }
            let version = get_version(&versioned_value);
            let value = get_value(&versioned_value);
            let (page_id, frame_id, _) = pi.cursor.get_physical_address();
            pi.cursor.go_to_next_kv();
            if let Some(e) = rwset.get_mut(&key) {
                e.update_physical_address(PhysicalAddress::new(page_id, frame_id));
                match e {
                    RWEntry::Read(_, _, old_version) => {
                        if *old_version != version {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        return Ok(Some((key, value.to_vec())));
                    }
                    RWEntry::Update(_, _, old_version, new_val) | RWEntry::Insert(_, _, old_version, new_val) => {
                        if *old_version != version {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        return Ok(Some((key, new_val.clone())));
                    }
                    RWEntry::Delete(..) => {
                        // Skip the record
                    }
                }
            } else {
                // Insert into rwset
                rwset.insert(
                    key.clone(),
                    RWEntry::Read(false, PhysicalAddress::new(page_id, frame_id), version),
                );
            }
            Ok(Some((key, value.to_vec())))
        } else {
            Ok(None)
        }
    }

    // Iterates over the secondary index and returns the value in the primary index
    pub fn iter_next_sec<M: MemPool>(
        &self,
        si: &mut SecondaryIterator<M>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        if let Some(((s_key, _id), s_value)) = si.cursor.get_kv() {
            // We got the primary key from the secondary index. Now, we need to get the value from the primary index.
            let ps = &si.ss.ps;
            let (p_key, p_hint) = s_value.split_at(s_value.len() - 8);
            let p_hint = PhysicalAddress::from_bytes(p_hint);
            let (p_value, p_addr) = self.read(ps, p_key, Some(p_hint.clone()))?;
            // println!("Hint: {}, Actual: {}", p_hint, p_addr);
            // Rewrite the physical address in the secondary index. The last 8 bytes should be updated with the new physical address.
            if p_hint != p_addr {
                // Update the physical address in the secondary index
                // println!("Sec Update From: {}, To: {}", p_hint, p_addr);
                let new_p_addr = p_addr.to_bytes();
                let new_s_val = [p_key, &new_p_addr].concat();
                si.cursor.opportunistic_update(&new_s_val, false);
            }
            si.cursor.go_to_next_kv();

            Ok(Some((s_key, p_value)))
        } else {
            Ok(None)
        }
    }

    pub fn commit<M: MemPool>(
        &self,
        pss: &PrimaryStorages<M>,
        sss: &SecondaryStorages<M>,
    ) -> Result<(), TxnStorageStatus> {
        // Release read locks first
        log_info!("SILO-TXN: Committing transaction");
        log_info!(" 1. Lock Write Set");
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let ps = pss.get(*c_id).unwrap(); // unwrap is safe because we only buffer updates of primary storages.
            let storage = &ps.btree;
            for (key, e) in rwset.iter() {
                match e {
                    RWEntry::Read(..) | RWEntry::Insert(..) => {
                        // Do nothing
                    }
                    RWEntry::Update(_, pa, old_version, _) | 
                    RWEntry::Delete(_, pa, old_version,) => {
                        // Lock the record
                        let mut page = storage
                            .traverse_to_leaf_for_write_with_hint(key, Some(pa.to_pf_key(*c_id)));
                        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                        

                    }
                }
            }
        }
        log_info!(" 2.1 Validate Read Set");
        log_info!(" 2.2 Validate Node Set");
        log_info!(" 3. Apply Write Set");

        log_info!(" 1. Releasing read locks");
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let ps = pss.get(*c_id).unwrap(); // unwrap is safe because we only buffer updates of primary storages.
            for (key, e) in rwset.iter() {
                match e {
                    RWEntry::Read(_, _) => {
                    }
                    RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) | RWEntry::Delete(_, _) => {
                    }
                }
            }
        }

        // Write
        log_info!(" 2. Releasing write locks and applying updates");
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let ps = pss.get(*c_id).unwrap(); // unwrap is safe because we only consider updates of primary storages
            let storage = &ps.btree;
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
            }
        }

        log_info!("  Done committing transaction");

        Ok(())
    }

    pub fn abort<M: MemPool>(&self, pss: &PrimaryStorages<M>) -> Result<(), TxnStorageStatus> {
        for (c_id, rwset) in unsafe { &*self.rwset.get() } {
            let ps = pss.get(*c_id).unwrap();
            let storage = &ps.btree;
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
                    }
                    RWEntry::Update(_, _, _) | RWEntry::Insert(_, _, _) | RWEntry::Delete(_, _) => {
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
    cursor: FosterBtreeCursor<M>,
    ps: Arc<PrimaryStorage<M>>,
}

impl<M: MemPool> PrimaryIterator<M> {
    pub fn new(cursor: FosterBtreeCursor<M>, ps: Arc<PrimaryStorage<M>>) -> Self {
        PrimaryIterator { cursor, ps }
    }
}

pub struct SecondaryIterator<M: MemPool> {
    cursor: FosterBtreeAppendOnlyCursor<M>,
    ss: Arc<SecondaryStorage<M>>,
}

impl<M: MemPool> SecondaryIterator<M> {
    pub fn new(cursor: FosterBtreeAppendOnlyCursor<M>, ss: Arc<SecondaryStorage<M>>) -> Self {
        SecondaryIterator { cursor, ss }
    }
}

pub struct NoWaitTxnStorage<M: MemPool> {
    bp: Arc<M>,
    pss: PrimaryStorages<M>,
    sss: SecondaryStorages<M>,
}

impl<M: MemPool> NoWaitTxnStorage<M> {
    pub fn new(bp: &Arc<M>) -> Self {
        NoWaitTxnStorage {
            bp: bp.clone(),
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
        }
    }
}

impl<M: MemPool> TxnStorageTrait for NoWaitTxnStorage<M> {
    type TxnHandle = SiloTxn;
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
            ContainerType::Primary => Ok(self.pss.create_new(&self.bp)),
            ContainerType::Secondary(primary_c_id) => {
                let ps = self.pss.get(primary_c_id).unwrap();
                Ok(self.sss.create_new(&self.bp, ps))
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

    fn get_container_stats(
        &self,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<String, TxnStorageStatus> {
        debug_assert_eq!(db_id, 0);
        match self.pss.get(c_id) {
            Some(ps) => Ok(ps.btree.page_stats(false)),
            None => match self.sss.get(c_id) {
                Some(ss) => Ok(ss.btree.fbt.page_stats(false)),
                None => Err(TxnStorageStatus::ContainerNotFound),
            },
        }
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
        match self.pss.get(c_id) {
            Some(ps) => {
                ps.btree.insert(&key, &value).unwrap();
                Ok(())
            }
            None => match self.sss.get(c_id) {
                Some(ss) => {
                    let value = [value, PhysicalAddress::default().to_bytes().to_vec()].concat();
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
        Ok(SiloTxn {
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
        match self.pss.get(c_id) {
            Some(ps) => txn.read(ps, key, None).map(|(v, _)| v),
            None => match self.sss.get(c_id) {
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
        match self.pss.get(c_id) {
            Some(ps) => txn.insert(ps, key, value, None).map(|_| ()),
            None => match self.sss.get(c_id) {
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
        match self.pss.get(c_id) {
            Some(ps) => {
                for (key, value) in kvs {
                    txn.insert(ps, key, value, None)?;
                }
            }
            None => match self.sss.get(c_id) {
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
        match self.pss.get(c_id) {
            Some(ps) => txn.update(ps, key, value, None).map(|_| ()),
            None => match self.sss.get(c_id) {
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
        match self.pss.get(c_id) {
            Some(ps) => {
                let (mut value, addr) = txn.read(ps, key.as_ref(), None)?;
                func(&mut value);
                txn.update(ps, key, value, Some(addr)).map(|_| ())
            }
            None => match self.sss.get(c_id) {
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
        match self.pss.get(c_id) {
            Some(ps) => txn.delete(ps, key, None).map(|_| ()),
            None => match self.sss.get(c_id) {
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
        match self.pss.get(c_id) {
            Some(ps) => {
                let cursor = FosterBtreeCursor::new(&ps.btree, &options.lower, &options.upper);
                let iter = PrimaryIterator::new(cursor, ps.clone());
                Ok(KVIterator::Primary(UnsafeCell::new(iter)))
            }
            None => match self.sss.get(c_id) {
                Some(ss) => {
                    let cursor =
                        FosterBtreeAppendOnlyCursor::new(&ss.btree, &options.lower, &options.upper);
                    let iter = SecondaryIterator::new(cursor, ss.clone());
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
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bp::get_test_bp,
        prelude::{ContainerDS, ContainerOptions, DBOptions, TxnOptions},
    };

    use super::*;

    #[test]
    fn test_insert_and_read_back() {
        // Create a NoWaitTxnStorage
        let bp = get_test_bp(10);
        let storage = NoWaitTxnStorage {
            bp: bp.clone(),
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
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
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
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
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
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
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
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
            pss: PrimaryStorages::new(),
            sss: SecondaryStorages::new(),
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
