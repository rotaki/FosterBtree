use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    access_method::{
        fbt::{BTreeKey, FosterBtree, FosterBtreeCursor, FosterBtreePage},
        prelude::*,
    },
    bp::{ContainerId, ContainerKey, DatabaseId, MemPool, PageFrameKey},
    txn_storage::locktable::ConcurrentLockTable as LockTable,
    txn_storage2::{
        field::{
            bytes_to_record, key_to_bytes, record_to_bytes, record_to_key_bytes, Field, Record,
            RecordPointer,
        },
        field_level_storage_trait::{
            ContainerDS, ContainerOptions, DBOptions, FieldLeveLStorageTrait, ScanOptions,
            TxnOptions, TxnStorageStatus,
        },
    },
};

// ============================================================================
// Read-Write Set Entry
// ============================================================================

#[derive(Clone, Debug)]
pub enum RWEntry {
    Read(RecordPointer, bool), // Physical address, inserted_as_ghost
    Update(Vec<Field>, RecordPointer, bool), // Updated fields, physical address, inserted_as_ghost
    Insert(Vec<Field>, RecordPointer, bool), // Inserted fields, physical address, inserted_as_ghost
    Delete(RecordPointer, bool), // Deleted record, physical address, inserted_as_ghost
}

impl RWEntry {
    fn get_pointer(&self) -> &RecordPointer {
        match self {
            RWEntry::Read(ptr, _)
            | RWEntry::Update(_, ptr, _)
            | RWEntry::Insert(_, ptr, _)
            | RWEntry::Delete(ptr, _) => ptr,
        }
    }

    fn is_ghost_inserted(&self) -> bool {
        match self {
            RWEntry::Read(_, ghost)
            | RWEntry::Update(_, _, ghost)
            | RWEntry::Insert(_, _, ghost)
            | RWEntry::Delete(_, ghost) => *ghost,
        }
    }
}

// ============================================================================
// Read-Write Set for a Container
// ============================================================================

pub struct ReadWriteSet {
    entries: UnsafeCell<HashMap<Vec<u8>, RWEntry>>, // key_bytes -> entry
}

impl Default for ReadWriteSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadWriteSet {
    pub fn new() -> Self {
        ReadWriteSet {
            entries: UnsafeCell::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&RWEntry> {
        unsafe { (*self.entries.get()).get(key) }
    }

    pub fn get_mut(&self, key: &[u8]) -> Option<&mut RWEntry> {
        unsafe { (*self.entries.get()).get_mut(key) }
    }

    pub fn insert(&self, key: Vec<u8>, entry: RWEntry) {
        unsafe {
            (*self.entries.get()).insert(key, entry);
        }
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<Vec<u8>, RWEntry> {
        unsafe { (*self.entries.get()).iter() }
    }
}

unsafe impl Send for ReadWriteSet {}
unsafe impl Sync for ReadWriteSet {}

// ============================================================================
// Transaction Handle
// ============================================================================

pub struct TxnHandle {
    rwsets: UnsafeCell<HashMap<ContainerId, ReadWriteSet>>, // Container-specific read-write sets
    committed: std::sync::atomic::AtomicBool,
}

impl TxnHandle {
    fn new() -> Self {
        TxnHandle {
            rwsets: UnsafeCell::new(HashMap::new()),
            committed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn get_or_create_rwset(&self, c_id: ContainerId) -> &ReadWriteSet {
        unsafe {
            let rwsets = &mut *self.rwsets.get();
            rwsets.entry(c_id).or_default()
        }
    }

    fn rwsets(&self) -> std::collections::hash_map::Iter<ContainerId, ReadWriteSet> {
        unsafe { (*self.rwsets.get()).iter() }
    }
}

unsafe impl Send for TxnHandle {}
unsafe impl Sync for TxnHandle {}

// ============================================================================
// Iterator Handle
// ============================================================================

pub struct TxnIterator<M: MemPool> {
    options: ScanOptions,
    scanner: KVCursor<M>,
    c_id: ContainerId,
    finished: AtomicBool,
}

impl<M: MemPool> TxnIterator<M> {
    fn new(options: ScanOptions, scanner: KVCursor<M>, c_id: ContainerId) -> Self {
        TxnIterator {
            options,
            scanner,
            c_id,
            finished: AtomicBool::new(false),
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::Acquire)
    }

    fn finish(&self) {
        self.finished
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

// ============================================================================
// Container Information
// ============================================================================

struct KVCursor<M: MemPool> {
    cursor: UnsafeCell<FosterBtreeCursor<M>>,
}

impl<M: MemPool> KVCursor<M> {
    fn next(&self) -> Option<(Vec<u8>, Vec<u8>, RecordPointer)> {
        unsafe {
            let cursor_ref = &mut *self.cursor.get();
            let (key, value) = cursor_ref.get_kv()?;
            let (page_id, frame_id, _) = cursor_ref.get_physical_address();
            cursor_ref.go_to_next_kv();
            Some((key, value, RecordPointer::new(page_id, frame_id)))
        }
    }

    /// Zero-copy scan over raw key/value bytes.
    /// The closure receives borrowed slices directly from the page — no allocation.
    /// Returns the number of tuples processed.
    /// Closure returns `true` to continue, `false` to stop early.
    fn for_each_raw(&self, mut f: impl FnMut(&[u8], &[u8], RecordPointer) -> bool) -> u64 {
        unsafe {
            let cursor_ref = &mut *self.cursor.get();
            cursor_ref.for_each_with_ptr(|key, val, (page_id, frame_id)| {
                f(key, val, RecordPointer::new(page_id, frame_id))
            })
        }
    }
}

struct ContainerInfo<M: MemPool> {
    options: ContainerOptions,
    btree: Arc<FosterBtree<M>>,
    locktable: Arc<LockTable>,
    c_key: ContainerKey,
}

impl<M: MemPool> ContainerInfo<M> {
    fn hint_to_page_frame_key(&self, hint: Option<RecordPointer>) -> Option<PageFrameKey> {
        hint.map(|h| PageFrameKey::new_with_frame_id(self.c_key, h.page_id, h.frame_id))
    }

    #[allow(dead_code)]
    fn print_locks(&self) {
        println!(
            "==========================\nContainer ({}) Locks:\n==========================\n{}",
            self.c_key, self.locktable,
        );
    }

    fn insert_with_hint(
        &self,
        key: &[u8],
        value: &[u8],
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, AccessMethodError> {
        let mut page = self
            .btree
            .traverse_to_leaf_for_write_with_hint(key, self.hint_to_page_frame_key(hint));
        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;

        if slot_id > 0 && page.get_raw_key(slot_id) == key {
            Err(AccessMethodError::KeyDuplicate)
        } else {
            self.btree
                .insert_at_slot_or_split(&mut page, slot_id + 1, key, value, false);
            let pointer = RecordPointer::new(page.page().get_id(), page.frame_id());
            Ok(pointer)
        }
    }

    fn delete_with_hint(
        &self,
        key: &[u8],
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, AccessMethodError> {
        let mut page = self
            .btree
            .traverse_to_leaf_for_write_with_hint(key, self.hint_to_page_frame_key(hint));
        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;

        if slot_id > 0 && page.get_raw_key(slot_id) == key {
            let pointer = RecordPointer::new(page.page().get_id(), page.frame_id());
            page.remove_at(slot_id);
            Ok(pointer)
        } else {
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> KVCursor<M> {
        KVCursor {
            cursor: UnsafeCell::new(FosterBtreeCursor::new(&self.btree.clone(), start, end)),
        }
    }

    fn release_shared_locks(&self, rwset: &ReadWriteSet) {
        let locktable = &self.locktable;
        for (key, entry) in rwset.iter() {
            if let RWEntry::Read(..) = entry {
                // Release shared lock
                locktable.release_shared(key.clone());
            }
        }
    }

    fn apply_updates_and_release_exclusive_locks(&self, rwset: &ReadWriteSet) {
        let locktable = &self.locktable;
        for (key, entry) in rwset.iter() {
            match entry {
                RWEntry::Read(..) => continue,
                RWEntry::Update(record, ptr, ghost) => {
                    let mut page = self.btree.traverse_to_leaf_for_write_with_hint(
                        key,
                        self.hint_to_page_frame_key(Some(*ptr)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key {
                        panic!(
                            "Key: {:?} of container {} not found for update",
                            key, self.c_key
                        );
                    } else {
                        // Update the record
                        if *ghost {
                            page.unghostify_at(slot_id);
                        }
                        self.btree.update_at_slot_or_split(
                            &mut page,
                            slot_id,
                            key,
                            &record_to_bytes(record, self.options.schema()),
                        );
                    }
                }
                RWEntry::Insert(_record, ptr, ghost) => {
                    assert!(ghost, "Insert entry should be ghost");
                    let mut page = self.btree.traverse_to_leaf_for_write_with_hint(
                        key,
                        self.hint_to_page_frame_key(Some(*ptr)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;

                    if slot_id == 0 || page.get_raw_key(slot_id) != key {
                        panic!(
                            "Key: {:?} of container {} not found for insert",
                            key, self.c_key
                        );
                    } else {
                        // Insert the record
                        page.unghostify_at(slot_id);
                    }
                }
                RWEntry::Delete(ptr, _) => {
                    let mut page = self.btree.traverse_to_leaf_for_write_with_hint(
                        key,
                        self.hint_to_page_frame_key(Some(*ptr)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(key)) - 1;

                    if slot_id == 0 || page.get_raw_key(slot_id) != key {
                        panic!(
                            "Key: {:?} of container {} not found for delete",
                            key, self.c_key
                        );
                    } else {
                        // Delete the record
                        page.remove_at(slot_id);
                    }
                }
            }
            locktable.release_exclusive(key.clone());
        }
    }

    fn revert_failed_inserts_and_release_locks(&self, rwset: &ReadWriteSet) {
        let locktable = &self.locktable;
        for (key, entry) in rwset.iter() {
            if entry.is_ghost_inserted() {
                self.delete_with_hint(key, Some(*entry.get_pointer()))
                    .expect("Failed to revert ghost insert");
            }
        }

        for (key, entry) in rwset.iter() {
            match entry {
                RWEntry::Read(..) => {
                    // Release shared lock
                    locktable.release_shared(key.clone());
                }
                RWEntry::Update(..) | RWEntry::Insert(..) | RWEntry::Delete(..) => {
                    // Release exclusive lock
                    locktable.release_exclusive(key.clone());
                }
            }
        }
    }
}

// ============================================================================
// Transactional Storage Implementation
// ============================================================================

pub struct TransactionalStorage<M: MemPool> {
    mem_pool: Arc<M>,
    containers: UnsafeCell<HashMap<ContainerId, ContainerInfo<M>>>,
    next_container_id: UnsafeCell<ContainerId>,
}

impl<M: MemPool> TransactionalStorage<M> {
    /// Create a new transactional storage instance
    pub fn new(mem_pool: Arc<M>) -> Self {
        Self {
            mem_pool,
            containers: UnsafeCell::new(HashMap::new()),
            next_container_id: UnsafeCell::new(1),
        }
    }

    /// Get a container by ID (internal helper)
    fn get_container(&self, c_id: ContainerId) -> Result<&ContainerInfo<M>, TxnStorageStatus> {
        unsafe {
            (*self.containers.get())
                .get(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)
        }
    }

    #[allow(dead_code)]
    fn print_locks(&self) {
        unsafe {
            for container in (*self.containers.get()).values() {
                container.print_locks();
            }
        }
    }

    /// Check if all lock tables are empty (no locks held)
    /// Returns true if all containers have empty lock tables
    ///
    /// This is useful for debugging and testing to ensure that:
    /// - All locks are properly released after commit/abort
    /// - No lock leaks occur in the system
    /// - Transaction isolation is properly maintained
    pub fn are_lock_tables_empty(&self) -> bool {
        unsafe {
            for container in (*self.containers.get()).values() {
                if !container.locktable.is_empty() {
                    return false;
                }
            }
            true
        }
    }

    /// Get detailed lock information for debugging
    /// Returns a vector of (container_id, lock_count) tuples
    pub fn get_lock_counts(&self) -> Vec<(ContainerId, usize)> {
        unsafe {
            (*self.containers.get())
                .iter()
                .map(|(c_id, container)| (*c_id, container.locktable.lock_count()))
                .collect()
        }
    }
}

unsafe impl<M: MemPool> Send for TransactionalStorage<M> {}
unsafe impl<M: MemPool> Sync for TransactionalStorage<M> {}

// ============================================================================
// FieldLeveLStorageTrait Implementation
// ============================================================================

impl<M: MemPool> FieldLeveLStorageTrait for TransactionalStorage<M> {
    type TxnHandle = TxnHandle;
    type IteratorHandle = TxnIterator<M>;
    type Hint = RecordPointer;

    // ========================================================================
    // Database Management (Single database with ID 0)
    // ========================================================================

    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        // Always return database ID 0
        Ok(0)
    }

    fn close_db(&self, _db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        // No-op for single database
        Ok(())
    }

    fn delete_db(&self, _db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
        // No-op for single database
        Ok(())
    }

    // ========================================================================
    // Container Management
    // ========================================================================

    fn create_container(
        &self,
        _db_id: DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        // Only support B-tree containers for now
        if options.data_structure() != ContainerDS::BTree {
            return Err(TxnStorageStatus::AbortFailed);
        }

        unsafe {
            let c_id = *self.next_container_id.get();
            *self.next_container_id.get() += 1;

            // Create Foster B-tree
            let container_key = ContainerKey::new(0, c_id); // Always use db_id 0
            let btree = Arc::new(FosterBtree::new(container_key, self.mem_pool.clone()));
            let locktable = Arc::new(LockTable::new());

            let container_info = ContainerInfo {
                options: options.clone(),
                btree,
                locktable,
                c_key: container_key,
            };

            (*self.containers.get()).insert(c_id, container_info);
            Ok(c_id)
        }
    }

    fn delete_container(
        &self,
        _db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        unsafe {
            (*self.containers.get())
                .remove(&c_id)
                .ok_or(TxnStorageStatus::ContainerNotFound)?;
            Ok(())
        }
    }

    fn list_containers(
        &self,
        _db_id: DatabaseId,
    ) -> Result<Vec<(ContainerId, ContainerOptions)>, TxnStorageStatus> {
        unsafe {
            let containers = (*self.containers.get())
                .iter()
                .map(|(&c_id, container)| (c_id, container.options.clone()))
                .collect();
            Ok(containers)
        }
    }

    // ========================================================================
    // Bulk Operations
    // ========================================================================

    fn raw_insert_record(
        &self,
        _db_id: DatabaseId,
        c_id: ContainerId,
        record: Record,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        let container = self.get_container(c_id)?;

        // Extract primary key from record
        let key_bytes = record_to_key_bytes(&record.fields, container.options.schema());

        // Serialize the record's fields
        let value_bytes = record_to_bytes(&record.fields, container.options.schema());

        // Directly insert into the container without any locking or transaction tracking
        let pointer = container
            .insert_with_hint(&key_bytes, &value_bytes, None)
            .map_err(|e| match e {
                AccessMethodError::KeyDuplicate => TxnStorageStatus::KeyExists,
                _ => TxnStorageStatus::from(e),
            })?;

        Ok(pointer)
    }

    // ========================================================================
    // Transaction Management
    // ========================================================================

    fn begin_txn(
        &self,
        _db_id: DatabaseId,
        _options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        Ok(TxnHandle::new())
    }

    fn commit_txn(
        &self,
        txn: &Self::TxnHandle,
        _async_commit: bool,
    ) -> Result<(), TxnStorageStatus> {
        // Check if already committed
        if txn.committed.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        // For each container in the transaction, release the shared locks
        for (&c_id, rwset) in txn.rwsets() {
            if let Ok(container) = self.get_container(c_id) {
                container.release_shared_locks(rwset);
            }
        }

        // Apply updates and release exclusive locks
        for (&c_id, rwset) in txn.rwsets() {
            if let Ok(container) = self.get_container(c_id) {
                container.apply_updates_and_release_exclusive_locks(rwset);
            }
        }

        // Mark as committed
        txn.committed
            .store(true, std::sync::atomic::Ordering::Release);

        Ok(())
    }

    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        // Check if already committed
        if txn.committed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TxnStorageStatus::Aborted);
        }

        // Abort all container changes
        for (&c_id, rwset) in txn.rwsets() {
            if let Ok(container) = self.get_container(c_id) {
                // Revert any ghost inserts and release locks
                container.revert_failed_inserts_and_release_locks(rwset);
            }
        }

        Ok(())
    }

    fn wait_for_txn(&self, _txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        // In this implementation, commit is synchronous, so no waiting needed
        Ok(())
    }

    fn drop_txn(&self, txn: Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        // If not committed, abort the transaction
        if !txn.committed.load(std::sync::atomic::Ordering::Acquire) {
            self.abort_txn(&txn)?;
        }
        Ok(())
    }

    // ========================================================================
    // Record Operations
    // ========================================================================

    fn num_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        let container = self.get_container(c_id)?;

        // This is a simplified implementation - in production you might want to
        // maintain counters or use more efficient methods
        let mut count = 0;
        let scanner = container.scan_range(&[], &[]);

        while scanner.next().is_some() {
            count += 1;
        }

        // Adjust count based on uncommitted changes in this transaction
        let rwset = txn.get_or_create_rwset(c_id);
        for (_key, entry) in rwset.iter() {
            match entry {
                RWEntry::Insert(_, _, _) => count += 1,
                RWEntry::Delete(_, _) => count -= 1,
                _ => {}
            }
        }

        Ok(count)
    }

    // ========================================================================
    // Field Access Operations
    // ========================================================================

    fn get_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        _hint: Option<RecordPointer>,
    ) -> Result<(Field, RecordPointer), TxnStorageStatus> {
        let (fields, ptr) = self.get_fields(txn, c_id, key, &[col_idx], _hint)?;
        Ok((fields.into_iter().next().unwrap(), ptr))
    }

    fn get_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_indices: &[usize],
        hint: Option<RecordPointer>,
    ) -> Result<(Vec<Field>, RecordPointer), TxnStorageStatus> {
        let rwset = txn.get_or_create_rwset(c_id);
        let container = self.get_container(c_id)?;
        let key_bytes = key_to_bytes(&key);

        if let Some(entry) = rwset.get_mut(&key_bytes) {
            match entry {
                RWEntry::Read(ptr, _) => {
                    let page = container.btree.traverse_to_leaf_for_read_with_hint(
                        &key_bytes,
                        container.hint_to_page_frame_key(Some(*ptr)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                        panic!("Key should exist in storage if in rwset");
                    }
                    *ptr = RecordPointer::new(page.page().get_id(), page.frame_id());
                    let record = bytes_to_record(page.get_val(slot_id), container.options.schema());
                    let fields = col_indices.iter().map(|&idx| record[idx].clone()).collect();
                    Ok((fields, *ptr))
                }
                RWEntry::Update(fields, ptr, _) | RWEntry::Insert(fields, ptr, _) => {
                    // Return the fields directly from rwset
                    let result = col_indices.iter().map(|&idx| fields[idx].clone()).collect();
                    Ok((result, *ptr))
                }
                RWEntry::Delete(_, _) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Find from index
            let locktable = &container.locktable;
            let page = container.btree.traverse_to_leaf_for_read_with_hint(
                &key_bytes,
                container.hint_to_page_frame_key(hint),
            );
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_shared(key_bytes.clone()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset as a read entry
                let ptr = RecordPointer::new(page.page().get_id(), page.frame_id());
                let record = bytes_to_record(page.get_val(slot_id), container.options.schema());
                rwset.insert(key_bytes.clone(), RWEntry::Read(ptr, false));
                let fields = col_indices.iter().map(|&idx| record[idx].clone()).collect();
                Ok((fields, ptr))
            }
        }
    }

    // ========================================================================
    // Field Update Operations
    // ========================================================================

    fn update_field(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        field: Field,
        _hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        self.update_fields(txn, c_id, key, vec![(col_idx, field)], _hint)
    }

    fn update_fields(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        fields: Vec<(usize, Field)>,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        let rwset = txn.get_or_create_rwset(c_id);
        let container = self.get_container(c_id)?;
        let key_bytes = key_to_bytes(&key);

        // Get current fields and pointer (either from rwset or storage)
        if let Some(e) = rwset.get_mut(&key_bytes) {
            match e {
                RWEntry::Delete(_, _) => Err(TxnStorageStatus::KeyNotFound),
                RWEntry::Read(ptr, ghost) => {
                    let page = container.btree.traverse_to_leaf_for_read_with_hint(
                        &key_bytes,
                        container.hint_to_page_frame_key(Some(*ptr)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                        panic!("Key should exist in storage if in rwset");
                    }

                    let locktable = &container.locktable;
                    if !locktable.try_upgrade(key_bytes.clone()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }

                    let mut record =
                        bytes_to_record(page.get_val(slot_id), container.options.schema());
                    fields.into_iter().for_each(|(idx, new_field)| {
                        record[idx] = new_field;
                    });

                    let ptr = *ptr;
                    *e = RWEntry::Update(record, ptr, *ghost);
                    Ok(ptr)
                }
                RWEntry::Update(record, ptr, _) => {
                    fields
                        .into_iter()
                        .for_each(|(idx, new_field)| record[idx] = new_field);
                    Ok(*ptr)
                }
                RWEntry::Insert(record, ptr, ghost) => {
                    let ptr = *ptr;
                    fields
                        .into_iter()
                        .for_each(|(idx, new_field)| record[idx] = new_field);
                    *e = RWEntry::Update(record.clone(), ptr, *ghost);
                    Ok(ptr)
                }
            }
        } else {
            // Abort if not found in index
            let locktable = &container.locktable;
            let page = container.btree.traverse_to_leaf_for_read_with_hint(
                &key_bytes,
                container.hint_to_page_frame_key(hint),
            );
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_exclusive(key_bytes.clone()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let ptr = RecordPointer::new(page.page().get_id(), page.frame_id());
                let mut record = bytes_to_record(page.get_val(slot_id), container.options.schema());
                // Update fields
                fields.into_iter().for_each(|(idx, new_field)| {
                    record[idx] = new_field;
                });
                rwset.insert(
                    key_bytes.clone(),
                    RWEntry::Update(record.clone(), ptr, false),
                );
                Ok(ptr)
            }
        }
    }

    fn update_field_with_func<F: FnOnce(&mut Field)>(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        col_idx: usize,
        func: F,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        let rwset = txn.get_or_create_rwset(c_id);
        let container = self.get_container(c_id)?;
        let key_bytes = key_to_bytes(&key);

        // Get current fields and pointer (either from rwset or storage)
        if let Some(e) = rwset.get_mut(&key_bytes) {
            match e {
                RWEntry::Delete(_, _) => Err(TxnStorageStatus::KeyNotFound),
                RWEntry::Read(ptr, ghost) => {
                    let page = container.btree.traverse_to_leaf_for_read_with_hint(
                        &key_bytes,
                        container.hint_to_page_frame_key(Some(*ptr)),
                    );
                    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
                    if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                        panic!("Key should exist in storage if in rwset");
                    }

                    let ptr = *ptr;
                    let locktable = &container.locktable;
                    if !locktable.try_upgrade(key_bytes.clone()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    let mut record =
                        bytes_to_record(page.get_val(slot_id), container.options.schema());
                    func(&mut record[col_idx]);
                    *e = RWEntry::Update(record, ptr, *ghost);
                    Ok(ptr)
                }
                RWEntry::Update(record, ptr, _) => {
                    func(&mut record[col_idx]);
                    Ok(*ptr)
                }
                RWEntry::Insert(record, ptr, ghost) => {
                    let ptr = *ptr;
                    func(&mut record[col_idx]);
                    *e = RWEntry::Update(record.clone(), ptr, *ghost);
                    Ok(ptr)
                }
            }
        } else {
            // Abort if not found in index
            let locktable = &container.locktable;
            let page = container.btree.traverse_to_leaf_for_read_with_hint(
                &key_bytes,
                container.hint_to_page_frame_key(hint),
            );
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_exclusive(key_bytes.clone()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset
                let ptr = RecordPointer::new(page.page().get_id(), page.frame_id());
                let mut record = bytes_to_record(page.get_val(slot_id), container.options.schema());
                // Update fields
                func(&mut record[col_idx]);
                rwset.insert(
                    key_bytes.clone(),
                    RWEntry::Update(record.clone(), ptr, false),
                );
                Ok(ptr)
            }
        }
    }

    // ========================================================================
    // Record Insertion and Deletion
    // ========================================================================

    fn insert_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        record: Record,
        hint: Option<RecordPointer>,
    ) -> Result<RecordPointer, TxnStorageStatus> {
        let rwset = txn.get_or_create_rwset(c_id);
        let container = self.get_container(c_id)?;
        let key_bytes = record_to_key_bytes(&record.fields, container.options.schema());

        // Check if key already exists in rwset
        if let Some(entry) = rwset.get_mut(&key_bytes) {
            match entry {
                RWEntry::Delete(ptr, ghost) => {
                    // Was deleted, now inserting - this becomes an update
                    let ptr = *ptr;
                    *entry = RWEntry::Update(record.fields, ptr, *ghost);
                    Ok(ptr)
                }
                _ => Err(TxnStorageStatus::KeyExists),
            }
        } else {
            // Not in rwset, check storage
            let locktable = &container.locktable;
            let mut page = container.btree.traverse_to_leaf_for_write_with_hint(
                &key_bytes,
                container.hint_to_page_frame_key(hint),
            );
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
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
                    Some(RWEntry::Read(..)) => {
                        // Upgrade next-key lock to write lock
                        if !locktable.try_upgrade(next_key.to_vec()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Lock this key
                        if !locktable.try_exclusive(key_bytes.clone()) {
                            locktable.downgrade(&next_key); // Downgrade the next key lock
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Insert the key-value as ghost record
                        container.btree.insert_at_slot_or_split(
                            &mut page,
                            slot_id + 1,
                            &key_bytes,
                            &record_to_bytes(&record.fields, container.options.schema()),
                            true,
                        );

                        // Downgrade the next key lock
                        locktable.downgrade(&next_key);
                    }
                    Some(_) => {
                        // Exclusive lock on next key is already held.
                        // Lock this key
                        if !locktable.try_exclusive(key_bytes.clone()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Insert the key-value as ghost record
                        container.btree.insert_at_slot_or_split(
                            &mut page,
                            slot_id + 1,
                            &key_bytes,
                            &record_to_bytes(&record.fields, container.options.schema()),
                            true,
                        );
                    }
                    None => {
                        // Lock the next key
                        if !locktable.try_exclusive(next_key.to_vec()) {
                            // println!("lock table: {}", locktable);
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        // Lock this key
                        if !locktable.try_exclusive(key_bytes.clone()) {
                            locktable.release_exclusive(next_key.to_vec());
                            return Err(TxnStorageStatus::TxnConflict);
                        }

                        // Insert the key-value as ghost record
                        container.btree.insert_at_slot_or_split(
                            &mut page,
                            slot_id + 1,
                            &key_bytes,
                            &record_to_bytes(&record.fields, container.options.schema()),
                            true,
                        );

                        // Release the next key lock
                        locktable.release_exclusive(next_key.to_vec());
                    }
                }

                let ptr = RecordPointer::new(page.page().get_id(), page.frame_id());
                rwset.insert(
                    key_bytes.clone(),
                    RWEntry::Insert(record.fields, ptr, true), // Mark as ghost
                );
                Ok(ptr)
            } else {
                Err(TxnStorageStatus::KeyExists)
            }
        }
    }

    fn insert_records(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        records: Vec<(Record, Option<RecordPointer>)>,
    ) -> Result<Vec<RecordPointer>, TxnStorageStatus> {
        let mut pointers = Vec::new();
        for (record, hint) in records {
            let ptr = self.insert_record(txn, c_id, record, hint)?;
            pointers.push(ptr);
        }
        Ok(pointers)
    }

    fn delete_record(
        &self,
        txn: &Self::TxnHandle,
        c_id: ContainerId,
        key: Vec<Field>,
        hint: Option<RecordPointer>,
    ) -> Result<(), TxnStorageStatus> {
        let rwset = txn.get_or_create_rwset(c_id);
        let container = self.get_container(c_id)?;
        let key_bytes = key_to_bytes(&key);

        // Check rwset first
        if let Some(entry) = rwset.get_mut(&key_bytes) {
            match entry {
                RWEntry::Read(ptr, ghost) => {
                    // Upgrade lock
                    let locktable = &container.locktable;
                    if !locktable.try_upgrade(key_bytes.clone()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    *entry = RWEntry::Delete(*ptr, *ghost);
                    Ok(())
                }
                RWEntry::Update(_, ptr, ghost) | RWEntry::Insert(_, ptr, ghost) => {
                    *entry = RWEntry::Delete(*ptr, *ghost);
                    Ok(())
                }
                RWEntry::Delete(_, _) => Err(TxnStorageStatus::KeyNotFound),
            }
        } else {
            // Not in rwset, need to check storage
            let locktable = &container.locktable;
            let page = container.btree.traverse_to_leaf_for_read_with_hint(
                &key_bytes,
                container.hint_to_page_frame_key(hint),
            );
            let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&key_bytes)) - 1;
            if slot_id == 0 || page.get_raw_key(slot_id) != key_bytes {
                Err(TxnStorageStatus::KeyNotFound)
            } else {
                // Lock the key
                if !locktable.try_exclusive(key_bytes.clone()) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                // Insert into rwset as a delete entry
                let ptr = RecordPointer::new(page.page().get_id(), page.frame_id());
                rwset.insert(
                    key_bytes.clone(),
                    RWEntry::Delete(ptr, false), // Not ghost since read from storage
                );
                Ok(())
            }
        }
    }

    // ========================================================================
    // Range Scanning and Iteration
    // ========================================================================

    fn scan_range(
        &self,
        _txn: &Self::TxnHandle,
        c_id: ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        let container = self.get_container(c_id)?;

        let scanner = container.scan_range(&options.lower_inc, &[]);

        Ok(TxnIterator::new(options, scanner, c_id))
    }

    fn iter_next(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<Field>, Vec<Field>, RecordPointer)>, TxnStorageStatus> {
        if iter.is_finished() {
            return Ok(None); // Iterator already finished
        }

        let container = self.get_container(iter.c_id)?;
        let rwset = txn.get_or_create_rwset(iter.c_id);

        loop {
            if let Some((key_bytes, value_bytes, ptr)) = iter.scanner.next() {
                if !iter.options.upper_exc.is_empty() && key_bytes >= iter.options.upper_exc {
                    // For phantom protection, we need to lock the upper bound key
                    if rwset.get(&key_bytes).is_none() {
                        let locktable = &container.locktable;
                        if !locktable.try_shared(key_bytes.clone()) {
                            return Err(TxnStorageStatus::TxnConflict);
                        }
                        rwset.insert(key_bytes, RWEntry::Read(ptr, false));
                    }
                    iter.finish();
                    return Ok(None);
                }

                // Check if in rwset
                let record = if let Some(entry) = rwset.get(&key_bytes) {
                    match entry {
                        RWEntry::Read(..) => {
                            bytes_to_record(&value_bytes, container.options.schema())
                        }
                        RWEntry::Update(record, _, _) | RWEntry::Insert(record, _, _) => {
                            record.clone()
                        }
                        RWEntry::Delete(_, _) => {
                            // Deleted in this transaction. Skip this entry.
                            continue;
                        }
                    }
                } else {
                    // Lock the key
                    let locktable = &container.locktable;
                    if !locktable.try_shared(key_bytes.clone()) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    let record = bytes_to_record(&value_bytes, container.options.schema());
                    rwset.insert(key_bytes.clone(), RWEntry::Read(ptr, false));
                    record
                };

                let key = container
                    .options
                    .schema()
                    .key_indices()
                    .iter()
                    .map(|&i| record[i].clone())
                    .collect();
                let fields = iter
                    .options
                    .cols
                    .iter()
                    .map(|&i| record[i].clone())
                    .collect();
                return Ok(Some((key, fields, ptr)));
            } else {
                // Last entry reached. Lock the &[] key to ensure no new entries are added
                if rwset.get(&[]).is_none() {
                    let locktable = &container.locktable;
                    if !locktable.try_shared(vec![]) {
                        return Err(TxnStorageStatus::TxnConflict);
                    }
                    rwset.insert(vec![], RWEntry::Read(RecordPointer::new(0, 0), false));
                }
                iter.finish();
                return Ok(None);
            }
        }
    }

    fn iter_for_each(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
        f: &mut dyn FnMut(&[u8], &[u8], RecordPointer) -> bool,
    ) -> Result<u64, TxnStorageStatus> {
        if iter.is_finished() {
            return Ok(0);
        }

        let container = self.get_container(iter.c_id)?;
        let rwset = txn.get_or_create_rwset(iter.c_id);
        let mut count: u64 = 0;
        let mut err: Option<TxnStorageStatus> = None;

        iter.scanner.for_each_raw(|key_bytes, value_bytes, ptr| {
            // Upper bound check
            if !iter.options.upper_exc.is_empty() && key_bytes >= &*iter.options.upper_exc {
                if rwset.get(key_bytes).is_none() {
                    let locktable = &container.locktable;
                    if !locktable.try_shared(key_bytes.to_vec()) {
                        err = Some(TxnStorageStatus::TxnConflict);
                        return false;
                    }
                    rwset.insert(key_bytes.to_vec(), RWEntry::Read(ptr, false));
                }
                iter.finish();
                return false;
            }

            // Check rwset for deleted entries
            if let Some(entry) = rwset.get(key_bytes) {
                if matches!(entry, RWEntry::Delete(_, _)) {
                    return true; // Skip deleted, continue
                }
            } else {
                // Lock the key
                let locktable = &container.locktable;
                if !locktable.try_shared(key_bytes.to_vec()) {
                    err = Some(TxnStorageStatus::TxnConflict);
                    return false;
                }
                rwset.insert(key_bytes.to_vec(), RWEntry::Read(ptr, false));
            }

            count += 1;
            f(key_bytes, value_bytes, ptr)
        });

        // Lock the end-of-range for phantom protection
        if err.is_none() && !iter.is_finished() {
            if rwset.get(&[]).is_none() {
                let locktable = &container.locktable;
                if !locktable.try_shared(vec![]) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                rwset.insert(vec![], RWEntry::Read(RecordPointer::new(0, 0), false));
            }
            iter.finish();
        }

        match err {
            Some(e) => Err(e),
            None => Ok(count),
        }
    }

    fn iter_for_each_fields(
        &self,
        txn: &Self::TxnHandle,
        iter: &Self::IteratorHandle,
        f: &mut dyn FnMut(&[Field], &[Field], RecordPointer) -> bool,
    ) -> Result<u64, TxnStorageStatus> {
        if iter.is_finished() {
            return Ok(0);
        }

        let container = self.get_container(iter.c_id)?;
        let schema = container.options.schema();
        let rwset = txn.get_or_create_rwset(iter.c_id);
        let mut count: u64 = 0;
        let mut err: Option<TxnStorageStatus> = None;

        iter.scanner.for_each_raw(|key_bytes, value_bytes, ptr| {
            // Upper bound check
            if !iter.options.upper_exc.is_empty() && key_bytes >= &*iter.options.upper_exc {
                if rwset.get(key_bytes).is_none() {
                    let locktable = &container.locktable;
                    if !locktable.try_shared(key_bytes.to_vec()) {
                        err = Some(TxnStorageStatus::TxnConflict);
                        return false;
                    }
                    rwset.insert(key_bytes.to_vec(), RWEntry::Read(ptr, false));
                }
                iter.finish();
                return false;
            }

            // Check rwset
            let record = if let Some(entry) = rwset.get(key_bytes) {
                match entry {
                    RWEntry::Delete(_, _) => return true, // Skip deleted, continue
                    RWEntry::Read(..) => bytes_to_record(value_bytes, schema),
                    RWEntry::Update(record, _, _) | RWEntry::Insert(record, _, _) => record.clone(),
                }
            } else {
                // Lock the key
                let locktable = &container.locktable;
                if !locktable.try_shared(key_bytes.to_vec()) {
                    err = Some(TxnStorageStatus::TxnConflict);
                    return false;
                }
                let record = bytes_to_record(value_bytes, schema);
                rwset.insert(key_bytes.to_vec(), RWEntry::Read(ptr, false));
                record
            };

            let key_fields: Vec<Field> = schema
                .key_indices()
                .iter()
                .map(|&i| record[i].clone())
                .collect();
            let val_fields: Vec<Field> = iter
                .options
                .cols
                .iter()
                .map(|&i| record[i].clone())
                .collect();

            count += 1;
            f(&key_fields, &val_fields, ptr)
        });

        // Lock the end-of-range for phantom protection
        if err.is_none() && !iter.is_finished() {
            if rwset.get(&[]).is_none() {
                let locktable = &container.locktable;
                if !locktable.try_shared(vec![]) {
                    return Err(TxnStorageStatus::TxnConflict);
                }
                rwset.insert(vec![], RWEntry::Read(RecordPointer::new(0, 0), false));
            }
            iter.finish();
        }

        match err {
            Some(e) => Err(e),
            None => Ok(count),
        }
    }

    fn drop_iterator_handle(&self, _iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus> {
        // Iterator will be dropped automatically
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_test_bp;

    use crate::txn_storage2::DataType;
    use crate::{assert_field, field, record, schema};

    #[test]
    fn test_transactional_basic_operations() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),  // id (primary key)
            (true, DataType::String),  // name (nullable)
            (false, DataType::Int32),  // age (not nullable)
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("users", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Start transaction
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a record
        let record = record![field!(Int32 1), field!(String "Alice"), field!(Int32 25)];
        storage
            .insert_record(&txn, container_id, record, None)
            .unwrap();

        // Read within same transaction (should see uncommitted data)
        let key = vec![field!(Int32 1)];
        let (fields, _) = storage
            .get_fields(&txn, container_id, key.clone(), &[1, 2], None)
            .unwrap();
        assert_eq!(fields.len(), 2);
        assert_field!(&fields[0], String("Alice"));
        assert_field!(&fields[1], Int32(25));

        // Update within transaction
        storage
            .update_field(&txn, container_id, key.clone(), 2, field!(Int32 26), None)
            .unwrap();

        // Verify update
        let (updated_fields, _) = storage
            .get_fields(&txn, container_id, key.clone(), &[2], None)
            .unwrap();
        assert_field!(&updated_fields[0], Int32(26));

        // Commit transaction
        storage.commit_txn(&txn, false).unwrap();

        // Start new transaction to verify committed data
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let (committed_fields, _) = storage
            .get_fields(&txn2, container_id, key, &[1, 2], None)
            .unwrap();
        assert_field!(&committed_fields[0], String("Alice"));
        assert_field!(&committed_fields[1], Int32(26));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_transactional_abort() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (true, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial data
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(String "initial")];
        storage
            .insert_record(&txn1, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn1, false).unwrap();

        // Start transaction that will be aborted
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Update record
        let key = vec![field!(Int32 1)];
        storage
            .update_field(
                &txn2,
                container_id,
                key.clone(),
                1,
                field!(String "aborted"),
                None,
            )
            .unwrap();

        // Abort transaction
        storage.abort_txn(&txn2).unwrap();

        // Verify data is unchanged
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let (field, _) = storage
            .get_field(&txn3, container_id, key, 1, None)
            .unwrap();
        assert_field!(&field, String("initial"));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_transactional_conflicts() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::Int32),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(Int32 100)];
        storage
            .insert_record(&txn_init, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Start two concurrent transactions
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let key = vec![field!(Int32 1)];

        // txn1 reads the record (acquires shared lock)
        let (_fields1, _) = storage
            .get_field(&txn1, container_id, key.clone(), 1, None)
            .unwrap();

        // txn2 tries to update (needs exclusive lock) - should fail
        let result =
            storage.update_field(&txn2, container_id, key.clone(), 1, field!(Int32 200), None);
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // txn1 commits
        storage.commit_txn(&txn1, false).unwrap();

        // Now txn2 can update (after starting fresh)
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_field(&txn3, container_id, key.clone(), 1, field!(Int32 200), None)
            .unwrap();
        storage.commit_txn(&txn3, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_isolation_read_uncommitted_changes() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
            (false, DataType::Int32),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![
            field!(Int32 1),
            field!(String "original"),
            field!(Int32 100)
        ];
        storage
            .insert_record(&txn_init, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Start two transactions
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let key = vec![field!(Int32 1)];

        // txn2 reads first (acquires shared lock)
        let (fields, _) = storage
            .get_fields(&txn2, container_id, key.clone(), &[1], None)
            .unwrap();
        assert_field!(&fields[0], String("original"));

        // txn1 tries to update - should fail due to txn2's shared lock
        let result = storage.update_field(
            &txn1,
            container_id,
            key.clone(),
            1,
            field!(String "modified"),
            None,
        );
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Commit txn2
        storage.commit_txn(&txn2, false).unwrap();

        // Now txn1 can update
        let txn1_new = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_field(
                &txn1_new,
                container_id,
                key.clone(),
                1,
                field!(String "modified"),
                None,
            )
            .unwrap();
        storage.commit_txn(&txn1_new, false).unwrap();

        // New transaction sees the modified value
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let (fields, _) = storage
            .get_fields(&txn3, container_id, key, &[1], None)
            .unwrap();
        assert_field!(&fields[0], String("modified"));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_write_write_conflict() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::Int32),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(Int32 100)];
        storage
            .insert_record(&txn_init, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Start two concurrent transactions
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let key = vec![field!(Int32 1)];

        // txn1 updates the record
        storage
            .update_field(&txn1, container_id, key.clone(), 1, field!(Int32 200), None)
            .unwrap();

        // txn2 tries to update the same record - should fail
        let result =
            storage.update_field(&txn2, container_id, key.clone(), 1, field!(Int32 300), None);
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // txn2 tries to delete the same record - should also fail
        let result = storage.delete_record(&txn2, container_id, key.clone(), None);
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        storage.commit_txn(&txn1, false).unwrap();
        storage.abort_txn(&txn2).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_multiple_readers_single_writer() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(String "shared_data")];
        storage
            .insert_record(&txn_init, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Start multiple reader transactions
        let txn_r1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn_r2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn_r3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let key = vec![field!(Int32 1)];

        // All readers can read simultaneously
        let (field1, _) = storage
            .get_field(&txn_r1, container_id, key.clone(), 1, None)
            .unwrap();
        let (field2, _) = storage
            .get_field(&txn_r2, container_id, key.clone(), 1, None)
            .unwrap();
        let (field3, _) = storage
            .get_field(&txn_r3, container_id, key.clone(), 1, None)
            .unwrap();

        assert_field!(&field1, String("shared_data"));
        assert_field!(&field2, String("shared_data"));
        assert_field!(&field3, String("shared_data"));

        // Start a writer transaction
        let txn_w = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Writer cannot acquire lock while readers hold shared locks
        let result = storage.update_field(
            &txn_w,
            container_id,
            key.clone(),
            1,
            field!(String "modified"),
            None,
        );
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Commit all readers
        storage.commit_txn(&txn_r1, false).unwrap();
        storage.commit_txn(&txn_r2, false).unwrap();
        storage.commit_txn(&txn_r3, false).unwrap();

        // Now writer can proceed
        let txn_w2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_field(
                &txn_w2,
                container_id,
                key,
                1,
                field!(String "modified"),
                None,
            )
            .unwrap();
        storage.commit_txn(&txn_w2, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_delete_and_reinsert() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(String "initial")];
        storage
            .insert_record(&txn1, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn1, false).unwrap();

        // Delete and reinsert in same transaction
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let key = vec![field!(Int32 1)];

        // Delete the record
        storage
            .delete_record(&txn2, container_id, key.clone(), None)
            .unwrap();

        // Try to read - should not exist
        let result = storage.get_field(&txn2, container_id, key.clone(), 1, None);
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        // Reinsert with new value
        let new_record = record![field!(Int32 1), field!(String "reinserted")];
        storage
            .insert_record(&txn2, container_id, new_record, None)
            .unwrap();

        // Read should now succeed with new value
        let (field, _) = storage
            .get_field(&txn2, container_id, key.clone(), 1, None)
            .unwrap();
        assert_field!(&field, String("reinserted"));

        storage.commit_txn(&txn2, false).unwrap();

        // Verify in new transaction
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let (field, _) = storage
            .get_field(&txn3, container_id, key, 1, None)
            .unwrap();
        assert_field!(&field, String("reinserted"));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_scan_with_uncommitted_changes() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Test scan sees committed data only
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 1..=3 {
            let record = record![field!(Int32 i), field!(String format!("record_{}", i))];
            storage
                .insert_record(&txn1, container_id, record, None)
                .unwrap();
        }
        storage.commit_txn(&txn1, false).unwrap();

        // Start transaction and verify scan sees all records
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let iter = storage
            .scan_range(&txn2, container_id, ScanOptions::new(&[]))
            .unwrap();
        let mut count = 0;
        let mut keys = Vec::new();

        while let Ok(Some((key, _fields, _))) = storage.iter_next(&txn2, &iter) {
            assert_eq!(key.len(), 1);
            if let Field::Int32(Some(k)) = &key[0] {
                keys.push(*k);
            }
            count += 1;
        }

        assert_eq!(count, 3);
        assert_eq!(keys, vec![1, 2, 3]);

        // Test that modifications within transaction are visible to point queries
        storage
            .delete_record(&txn2, container_id, vec![field!(Int32 2)], None)
            .unwrap();

        // Point query should not find deleted record
        let result = storage.get_field(&txn2, container_id, vec![field!(Int32 2)], 1, None);
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        // But can still read other records
        let (field, _) = storage
            .get_field(&txn2, container_id, vec![field!(Int32 1)], 1, None)
            .unwrap();
        assert_field!(&field, String("record_1"));

        storage.commit_txn(&txn2, false).unwrap();

        // New transaction should see the deletion
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.get_field(&txn3, container_id, vec![field!(Int32 2)], 1, None);
        assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_update_with_function() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::Int32),
            (false, DataType::Int32),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("counters", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(Int32 10), field!(Int32 20)];
        storage
            .insert_record(&txn1, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn1, false).unwrap();

        // Update using function
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let key = vec![field!(Int32 1)];

        storage
            .update_field_with_func(
                &txn2,
                container_id,
                key.clone(),
                1,
                |field| {
                    // Increment both counters
                    if let Field::Int32(Some(val)) = field {
                        *val += 5;
                    }
                },
                None,
            )
            .unwrap();

        storage
            .update_field_with_func(
                &txn2,
                container_id,
                key.clone(),
                2,
                |field| {
                    // Increment both counters
                    if let Field::Int32(Some(val)) = field {
                        *val += 10;
                    }
                },
                None,
            )
            .unwrap();

        // Verify the update
        let (fields, _) = storage
            .get_fields(&txn2, container_id, key.clone(), &[1, 2], None)
            .unwrap();
        assert_field!(&fields[0], Int32(15));
        assert_field!(&fields[1], Int32(30));

        storage.commit_txn(&txn2, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_concurrent_inserts_different_keys() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Start multiple transactions
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Each transaction inserts different keys - should all succeed
        let record1 = record![field!(Int32 1), field!(String "txn1")];
        let record2 = record![field!(Int32 2), field!(String "txn2")];
        let record3 = record![field!(Int32 3), field!(String "txn3")];

        storage
            .insert_record(&txn1, container_id, record1, None)
            .unwrap();
        storage
            .insert_record(&txn2, container_id, record2, None)
            .unwrap();
        storage
            .insert_record(&txn3, container_id, record3, None)
            .unwrap();

        // All commits should succeed
        storage.commit_txn(&txn1, false).unwrap();
        storage.commit_txn(&txn2, false).unwrap();
        storage.commit_txn(&txn3, false).unwrap();

        // Verify all records exist
        let txn_verify = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let (field1, _) = storage
            .get_field(&txn_verify, container_id, vec![field!(Int32 1)], 1, None)
            .unwrap();
        assert_field!(&field1, String("txn1"));

        let (field2, _) = storage
            .get_field(&txn_verify, container_id, vec![field!(Int32 2)], 1, None)
            .unwrap();
        assert_field!(&field2, String("txn2"));

        let (field3, _) = storage
            .get_field(&txn_verify, container_id, vec![field!(Int32 3)], 1, None)
            .unwrap();
        assert_field!(&field3, String("txn3"));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_deadlock_prevention() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert two records
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record1 = record![field!(Int32 1), field!(String "record1")];
        let record2 = record![field!(Int32 2), field!(String "record2")];
        storage
            .insert_record(&txn_init, container_id, record1, None)
            .unwrap();
        storage
            .insert_record(&txn_init, container_id, record2, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Start two transactions
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // txn1 locks record 1
        let (_val1, _) = storage
            .get_field(&txn1, container_id, vec![field!(Int32 1)], 1, None)
            .unwrap();

        // txn2 locks record 2
        let (_val2, _) = storage
            .get_field(&txn2, container_id, vec![field!(Int32 2)], 1, None)
            .unwrap();

        // txn1 tries to update record 2 - should fail (would cause deadlock)
        let result = storage.update_field(
            &txn1,
            container_id,
            vec![field!(Int32 2)],
            1,
            field!(String "updated"),
            None,
        );
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // txn2 tries to update record 1 - should also fail
        let result = storage.update_field(
            &txn2,
            container_id,
            vec![field!(Int32 1)],
            1,
            field!(String "updated"),
            None,
        );
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Both transactions can still commit their reads
        storage.commit_txn(&txn1, false).unwrap();
        storage.commit_txn(&txn2, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_multi_field_update() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),   // id
            (false, DataType::String),  // name
            (false, DataType::Int32),   // age
            (true, DataType::String),   // email (nullable)
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("users", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![
            field!(Int32 1),
            field!(String "John"),
            field!(Int32 25),
            field!(Null String)
        ];
        storage
            .insert_record(&txn1, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn1, false).unwrap();

        // Update multiple fields at once
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let key = vec![field!(Int32 1)];

        storage
            .update_fields(
                &txn2,
                container_id,
                key.clone(),
                vec![
                    (1, field!(String "Jonathan")),
                    (2, field!(Int32 26)),
                    (3, field!(String "jonathan@example.com")),
                ],
                None,
            )
            .unwrap();

        // Verify all updates
        let (fields, _) = storage
            .get_fields(&txn2, container_id, key.clone(), &[1, 2, 3], None)
            .unwrap();
        assert_field!(&fields[0], String("Jonathan"));
        assert_field!(&fields[1], Int32(26));
        assert_field!(&fields[2], String("jonathan@example.com"));

        storage.commit_txn(&txn2, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_lock_upgrade() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(String "initial")];
        storage
            .insert_record(&txn_init, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Start transaction and read (acquires shared lock)
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let key = vec![field!(Int32 1)];

        let (field, _) = storage
            .get_field(&txn, container_id, key.clone(), 1, None)
            .unwrap();
        assert_field!(&field, String("initial"));

        // Same transaction can upgrade to exclusive lock for update
        storage
            .update_field(
                &txn,
                container_id,
                key.clone(),
                1,
                field!(String "updated"),
                None,
            )
            .unwrap();

        // Verify update
        let (field, _) = storage.get_field(&txn, container_id, key, 1, None).unwrap();
        assert_field!(&field, String("updated"));

        storage.commit_txn(&txn, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_bulk_operations() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Raw insert without transaction overhead
        let records = vec![
            record![field!(Int32 1), field!(String "record1")],
            record![field!(Int32 2), field!(String "record2")],
            record![field!(Int32 3), field!(String "record3")],
        ];
        for record in records {
            storage
                .raw_insert_record(db_id, container_id, record)
                .unwrap();
        }

        // Verify all records were inserted
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        for i in 1..=3 {
            let (field, _) = storage
                .get_field(&txn, container_id, vec![field!(Int32 i)], 1, None)
                .unwrap();
            let expected = format!("record{}", i);
            assert_field!(&field, String(&expected));
        }

        storage.commit_txn(&txn, false).unwrap();
        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_concurrent_read_write_same_key() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::Int32),
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert initial record
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let record = record![field!(Int32 1), field!(Int32 100)];
        storage
            .insert_record(&txn_init, container_id, record, None)
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Multiple readers and one writer
        let readers: Vec<_> = (0..5)
            .map(|_| storage.begin_txn(db_id, TxnOptions::default()).unwrap())
            .collect();
        let writer = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        let key = vec![field!(Int32 1)];

        // All readers can read simultaneously
        for reader in &readers {
            let (field, _) = storage
                .get_field(reader, container_id, key.clone(), 1, None)
                .unwrap();
            assert_field!(&field, Int32(100));
        }

        // Writer cannot update while readers hold locks
        let result = storage.update_field(
            &writer,
            container_id,
            key.clone(),
            1,
            field!(Int32 200),
            None,
        );
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

        // Commit all readers
        for reader in readers {
            storage.commit_txn(&reader, false).unwrap();
        }

        // Now writer can update
        let writer_new = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .update_field(
                &writer_new,
                container_id,
                key.clone(),
                1,
                field!(Int32 200),
                None,
            )
            .unwrap();
        storage.commit_txn(&writer_new, false).unwrap();

        // Verify update
        let txn_verify = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let (field, _) = storage
            .get_field(&txn_verify, container_id, key, 1, None)
            .unwrap();
        assert_field!(&field, Int32(200));

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_basic_ghost_record() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),   // id
            (false, DataType::String),  // name
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Test basic ghost record behavior
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert a ghost record
        storage
            .insert_record(
                &txn1,
                container_id,
                record![field!(Int32 1), field!(String "Ghost")],
                None,
            )
            .unwrap();

        // Another transaction shouldn't see it
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.get_field(&txn2, container_id, vec![field!(Int32 1)], 1, None);
        assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));
        storage.abort_txn(&txn2).unwrap();

        // Commit txn1 - record should now be visible
        storage.commit_txn(&txn1, false).unwrap();

        // New transaction should see it
        let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let (fields, _) = storage
            .get_field(&txn3, container_id, vec![field!(Int32 1)], 1, None)
            .unwrap();
        assert_field!(&fields, String("Ghost"));
        storage.commit_txn(&txn3, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_simple_phantom_prevention() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),   // id
            (false, DataType::String),  // name
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert some initial records
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_record(
                &txn_init,
                container_id,
                record![field!(Int32 1), field!(String "One")],
                None,
            )
            .unwrap();
        storage
            .insert_record(
                &txn_init,
                container_id,
                record![field!(Int32 3), field!(String "Three")],
                None,
            )
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Test: Can we insert between 1 and 3?
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.insert_record(
            &txn1,
            container_id,
            record![field!(Int32 2), field!(String "Two")],
            None,
        );
        assert!(result.is_ok());
        storage.commit_txn(&txn1, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_scan_insert_interaction_with_phantom_protection() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),   // id
            (false, DataType::String),  // name
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert some initial records
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        storage
            .insert_record(
                &txn_init,
                container_id,
                record![field!(Int32 1), field!(String "One")],
                None,
            )
            .unwrap();
        storage
            .insert_record(
                &txn_init,
                container_id,
                record![field!(Int32 3), field!(String "Three")],
                None,
            )
            .unwrap();
        storage.commit_txn(&txn_init, false).unwrap();

        // Txn1: Start a scan
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let scan_options =
            ScanOptions::new(&[]).with_bounds(vec![field!(Int32 1)], vec![field!(Int32 5)]);
        let iter = storage
            .scan_range(&txn1, container_id, scan_options.clone())
            .unwrap();

        storage.iter_next(&txn1, &iter).unwrap().unwrap(); // Read first record (key 1)
        storage.iter_next(&txn1, &iter).unwrap().unwrap(); // Read second record (key 3)
        assert_eq!(storage.iter_next(&txn1, &iter).unwrap(), None);
        drop(iter); // Explicitly drop the iterator to release page latches.

        // Txn2: Try to insert key 2 (between 1 and 3 -- fails because 1, 3, [inf] are locked)
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let result = storage.insert_record(
            &txn2,
            container_id,
            record![field!(Int32 2), field!(String "Two")],
            None,
        );
        assert!(
            matches!(result, Err(TxnStorageStatus::TxnConflict)),
            "Expected conflict when inserting between scan range"
        );

        // Txn2: Try to insert key 4 (within scan range -- fails because 1, 3, [inf] are locked)
        let result = storage.insert_record(
            &txn2,
            container_id,
            record![field!(Int32 4), field!(String "Four")],
            None,
        );
        assert!(
            matches!(result, Err(TxnStorageStatus::TxnConflict)),
            "Expected conflict when inserting within scan range"
        );

        let iter = storage
            .scan_range(&txn1, container_id, scan_options)
            .unwrap();
        storage.iter_next(&txn1, &iter).unwrap(); // Consume first record (key 1)
        storage.iter_next(&txn1, &iter).unwrap(); // Consume second record (key 3)
        assert_eq!(storage.iter_next(&txn1, &iter).unwrap(), None);

        storage.commit_txn(&txn1, false).unwrap();

        storage.close_db(db_id).unwrap();
    }

    /*
    #[test]
    fn test_phantom_prevention() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),   // id
            (false, DataType::String),  // name
        ]);

        let bp = get_test_bp(100);
        let storage = Arc::new(TransactionalStorage::new(bp));

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert some initial records
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let records = vec![
            record![field!(Int32 1), field!(String "Alice")],
            record![field!(Int32 3), field!(String "Carol")],
            record![field!(Int32 5), field!(String "Eve")],
        ];
        for record in records {
            storage
                .insert_record(&txn_init, container_id, record, None)
                .unwrap();
        }
        storage.commit_txn(&txn_init, false).unwrap();

        // Test 1: Phantom prevention with concurrent insert during scan
        {
            // Txn1: Start a scan that will read keys 1-5
            let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let scan_options =
                ScanOptions::with_bounds(vec![field!(Int32 1)], vec![field!(Int32 5)]);
            let iter = storage
                .scan_range(&txn1, container_id, scan_options)
                .unwrap();

            // Read first record (id=1)
            let (key1, value1, _) = storage.iter_next(&txn1, &iter).unwrap().unwrap();
            assert_field!(&key1[0], Int32(1));
            assert_field!(&value1[1], String("Alice")); // name is at index 1

            // Txn2: Try to insert id=2 (within scan range)
            let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let result = storage.insert_record(
                &txn2,
                container_id,
                record![field!(Int32 2), field!(String "Bob")],
                None,
            );

            // This should conflict due to next-key locking
            assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

            // Txn2: Try to insert id=4 (also within scan range)
            let result = storage.insert_record(
                &txn2,
                container_id,
                record![field!(Int32 4), field!(String "David")],
                None,
            );

            // This should also conflict
            assert!(matches!(result, Err(TxnStorageStatus::TxnConflict)));

            // Continue scan in Txn1
            let (key2, value2, _) = storage.iter_next(&txn1, &iter).unwrap().unwrap();
            assert_field!(&key2[0], Int32(3));
            assert_field!(&value2[1], String("Carol")); // name is at index 1

            let (key3, value3, _) = storage.iter_next(&txn1, &iter).unwrap().unwrap();
            assert_field!(&key3[0], Int32(5));
            assert_field!(&value3[1], String("Eve")); // name is at index 1

            // End of scan
            assert!(storage.iter_next(&txn1, &iter).unwrap().is_none());

            storage.commit_txn(&txn1, false).unwrap();

            // Now Txn2 can insert successfully
            storage
                .insert_record(
                    &txn2,
                    container_id,
                    record![field!(Int32 2), field!(String "Bob")],
                    None,
                )
                .unwrap();
            storage
                .insert_record(
                    &txn2,
                    container_id,
                    record![field!(Int32 4), field!(String "David")],
                    None,
                )
                .unwrap();
            storage.commit_txn(&txn2, false).unwrap();
        }

        // Test 2: Verify no phantoms occurred
        {
            let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let scan_options =
                ScanOptions::with_bounds(vec![field!(Int32 1)], vec![field!(Int32 5)]);
            let iter = storage
                .scan_range(&txn3, container_id, scan_options)
                .unwrap();

            // Now we should see all 5 records in order
            let expected = vec![
                (1, "Alice"),
                (2, "Bob"),
                (3, "Carol"),
                (4, "David"),
                (5, "Eve"),
            ];

            for (expected_id, expected_name) in expected {
                let (key, value, _) = storage.iter_next(&txn3, &iter).unwrap().unwrap();
                assert_field!(&key[0], Int32(expected_id));
                assert_field!(&value[1], String(expected_name)); // name is at index 1
            }

            assert!(storage.iter_next(&txn3, &iter).unwrap().is_none());
            storage.commit_txn(&txn3, false).unwrap();
        }

        // Test 3: Phantom prevention with point reads
        {
            // Txn1 reads key 6 (doesn't exist)
            let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let result = storage.get_field(&txn1, container_id, vec![field!(Int32 6)], 1, None);
            assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

            // Txn2 tries to insert key 6
            let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let result = storage.insert_record(
                &txn2,
                container_id,
                record![field!(Int32 6), field!(String "Frank")],
                None,
            );

            // This should succeed because Txn1 only did a point read, not a range scan
            assert!(result.is_ok());
            storage.commit_txn(&txn2, false).unwrap();

            // Txn1 still doesn't see key 6 (repeatable read)
            let result = storage.get_field(&txn1, container_id, vec![field!(Int32 6)], 1, None);
            assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));

            storage.commit_txn(&txn1, false).unwrap();
        }

        // Test 4: Ghost record cleanup on abort
        {
            let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

            // Insert a ghost record
            storage
                .insert_record(
                    &txn1,
                    container_id,
                    record![field!(Int32 7), field!(String "Ghost")],
                    None,
                )
                .unwrap();

            // Another transaction shouldn't see it
            let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let result = storage.get_field(&txn2, container_id, vec![field!(Int32 7)], 1, None);
            assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));
            storage.commit_txn(&txn2, false).unwrap();

            // Abort txn1 - ghost record should be removed
            storage.abort_txn(&txn1).unwrap();

            // Verify ghost record is gone
            let txn3 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
            let result = storage.get_field(&txn3, container_id, vec![field!(Int32 7)], 1, None);
            assert!(matches!(result, Err(TxnStorageStatus::KeyNotFound)));
            storage.commit_txn(&txn3, false).unwrap();
        }

        storage.close_db(db_id).unwrap();
    }
    */

    #[test]
    fn test_lock_table_empty_after_commit() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Verify lock tables are initially empty
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty initially"
        );

        // Start transaction and perform various operations
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert records
        for i in 1..=5 {
            let record = record![field!(Int32 i), field!(String format!("record_{}", i))];
            storage
                .insert_record(&txn, container_id, record, None)
                .unwrap();
        }

        // Update some records
        storage
            .update_field(
                &txn,
                container_id,
                vec![field!(Int32 2)],
                1,
                field!(String "updated"),
                None,
            )
            .unwrap();

        // Read some records
        let _ = storage.get_field(&txn, container_id, vec![field!(Int32 3)], 1, None);

        // During transaction, locks should be held
        assert!(
            !storage.are_lock_tables_empty(),
            "Lock tables should not be empty during transaction"
        );

        // Commit transaction
        storage.commit_txn(&txn, false).unwrap();

        // After commit, all locks should be released
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty after commit"
        );

        // Verify with detailed lock counts
        let lock_counts = storage.get_lock_counts();
        for (container_id, count) in lock_counts {
            assert_eq!(
                count, 0,
                "Container {} should have 0 locks after commit",
                container_id
            );
        }

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_lock_table_empty_after_abort() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Verify lock tables are initially empty
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty initially"
        );

        // Start transaction and perform various operations
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Insert records
        for i in 1..=5 {
            let record = record![field!(Int32 i), field!(String format!("record_{}", i))];
            storage
                .insert_record(&txn, container_id, record, None)
                .unwrap();
        }

        // Update some records
        storage
            .update_field(
                &txn,
                container_id,
                vec![field!(Int32 2)],
                1,
                field!(String "updated"),
                None,
            )
            .unwrap();

        // Delete a record
        storage
            .delete_record(&txn, container_id, vec![field!(Int32 1)], None)
            .unwrap();

        // During transaction, locks should be held
        assert!(
            !storage.are_lock_tables_empty(),
            "Lock tables should not be empty during transaction"
        );

        // Abort transaction
        storage.abort_txn(&txn).unwrap();

        // After abort, all locks should be released
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty after abort"
        );

        // Verify with detailed lock counts
        let lock_counts = storage.get_lock_counts();
        for (container_id, count) in lock_counts {
            assert_eq!(
                count, 0,
                "Container {} should have 0 locks after abort",
                container_id
            );
        }

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_lock_table_consistency_multiple_transactions() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert some initial data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 1..=10 {
            let record = record![field!(Int32 i), field!(String format!("initial_{}", i))];
            storage
                .insert_record(&txn_init, container_id, record, None)
                .unwrap();
        }
        storage.commit_txn(&txn_init, false).unwrap();

        // Verify locks are clean after initial setup
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty after initial commit"
        );

        // Start multiple transactions
        let txn1 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Txn1 reads and updates
        let _ = storage.get_field(&txn1, container_id, vec![field!(Int32 1)], 1, None);
        storage
            .update_field(
                &txn1,
                container_id,
                vec![field!(Int32 1)],
                1,
                field!(String "txn1_update"),
                None,
            )
            .unwrap();

        // Txn2 reads different records
        let _ = storage.get_field(&txn2, container_id, vec![field!(Int32 5)], 1, None);
        let _ = storage.get_field(&txn2, container_id, vec![field!(Int32 6)], 1, None);

        // Locks should be held by both transactions
        assert!(
            !storage.are_lock_tables_empty(),
            "Lock tables should not be empty with active transactions"
        );

        // Commit txn1
        storage.commit_txn(&txn1, false).unwrap();

        // Still have locks from txn2
        assert!(
            !storage.are_lock_tables_empty(),
            "Lock tables should not be empty with txn2 still active"
        );

        // Abort txn2
        storage.abort_txn(&txn2).unwrap();

        // Now all locks should be released
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty after all transactions complete"
        );

        storage.close_db(db_id).unwrap();
    }

    #[test]
    fn test_lock_table_consistency_with_scans() {
        let schema = schema!(pk: [0], cols: [
            (false, DataType::Int32),
            (false, DataType::String),
        ]);

        let bp = get_test_bp(100);
        let storage = TransactionalStorage::new(bp);

        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test", ContainerDS::BTree, schema),
            )
            .unwrap();

        // Insert test data
        let txn_init = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for i in 1..=20 {
            let record = record![field!(Int32 i), field!(String format!("data_{}", i))];
            storage
                .insert_record(&txn_init, container_id, record, None)
                .unwrap();
        }
        storage.commit_txn(&txn_init, false).unwrap();

        // Start transaction with scan
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

        // Perform range scan
        let iter = storage
            .scan_range(
                &txn,
                container_id,
                ScanOptions::new(&[0, 1])
                    .with_bounds(vec![field!(Int32 5)], vec![field!(Int32 15)]),
            )
            .unwrap();

        // Read some results
        let mut count = 0;
        while count < 5 {
            if let Ok(Some(_)) = storage.iter_next(&txn, &iter) {
                count += 1;
            } else {
                break;
            }
        }

        // Locks should be held during scan
        assert!(
            !storage.are_lock_tables_empty(),
            "Lock tables should not be empty during scan"
        );

        // Drop iterator handle
        storage.drop_iterator_handle(iter).unwrap();

        // Commit transaction
        storage.commit_txn(&txn, false).unwrap();

        // All locks should be released after commit
        assert!(
            storage.are_lock_tables_empty(),
            "Lock tables should be empty after commit with scan"
        );

        storage.close_db(db_id).unwrap();
    }
}
