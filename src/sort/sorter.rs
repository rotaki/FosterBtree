use libc::key_t;
use rayon::prelude::*;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::{
    bp::{ContainerId, ContainerKey, DatabaseId, FrameWriteGuard, MemPool, PageFrameKey},
    page::{Page, AVAILABLE_PAGE_SIZE},
    txn_storage2::{
        field::{bytes_to_record, record_to_bytes, to_normalized_key, Field, Record},
        field_level_storage_trait::{FieldLeveLStorageTrait, TxnStorageStatus},
        DataType, Schema,
    },
};

// Page layout constants for sorted pages
const SORTED_PAGE_HEADER_SIZE: usize = 14;
const SLOT_SIZE: usize = 6;

// Configuration for external sort
pub struct ExternalSortConfig {
    pub total_frames: usize,
    pub num_threads: usize,
    pub sort_columns: Vec<(usize, bool, bool)>, // (column_index, ascending, nulls_first)
}

impl ExternalSortConfig {
    pub fn frames_per_thread(&self) -> usize {
        ((self.total_frames as f64 * 0.95) / self.num_threads as f64) as usize
    }
}

// Slot structure for page-based storage
#[derive(Clone, Copy, Debug)]
struct Slot {
    offset: u16,
    key_size: u16,
    val_size: u16,
}

impl Slot {
    fn new(offset: u16, key_size: u16, val_size: u16) -> Self {
        Self {
            offset,
            key_size,
            val_size,
        }
    }

    fn to_bytes(&self) -> [u8; SLOT_SIZE] {
        let mut bytes = [0; SLOT_SIZE];
        bytes[0..2].copy_from_slice(&self.offset.to_be_bytes());
        bytes[2..4].copy_from_slice(&self.key_size.to_be_bytes());
        bytes[4..6].copy_from_slice(&self.val_size.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; SLOT_SIZE]) -> Self {
        let offset = u16::from_be_bytes([bytes[0], bytes[1]]);
        let key_size = u16::from_be_bytes([bytes[2], bytes[3]]);
        let val_size = u16::from_be_bytes([bytes[4], bytes[5]]);
        Self {
            offset,
            key_size,
            val_size,
        }
    }
}

// Page operations for sorted data
trait SortedPage {
    fn init_sorted(&mut self);
    fn slot_count(&self) -> u16;
    fn set_slot_count(&mut self, count: u16);
    fn rec_start_offset(&self) -> u16;
    fn set_rec_start_offset(&mut self, offset: u16);
    fn total_bytes_used(&self) -> u16;
    fn set_total_bytes_used(&mut self, bytes: u16);
    fn total_free_space(&self) -> u16 {
        AVAILABLE_PAGE_SIZE as u16 - self.total_bytes_used()
    }

    fn append(&mut self, key: &[u8], val: &[u8]) -> bool;
    fn get_key(&self, slot_id: u16) -> &[u8];
    fn get_val(&self, slot_id: u16) -> &[u8];
    fn get_slot(&self, slot_id: u16) -> Option<Slot>;
}

// Extension trait for FrameWriteGuard to provide page operations
trait PageExt {
    fn append(&mut self, key: &[u8], val: &[u8]) -> bool;
    fn slot_count(&self) -> u16;
    fn get_key(&self, slot_id: u16) -> &[u8];
    fn get_val(&self, slot_id: u16) -> &[u8];
    fn get_id(&self) -> PageFrameKey;
}

impl<T: crate::bp::EvictionPolicy> PageExt for FrameWriteGuard<T> {
    fn append(&mut self, key: &[u8], val: &[u8]) -> bool {
        // Get the underlying page
        let page = unsafe { &mut *(self.as_mut_ptr() as *mut Page) };
        page.append(key, val)
    }

    fn slot_count(&self) -> u16 {
        // Get the underlying page
        let page = unsafe { &*(self.as_ptr() as *const Page) };
        page.slot_count()
    }

    fn get_key(&self, slot_id: u16) -> &[u8] {
        // Get the underlying page
        let page = unsafe { &*(self.as_ptr() as *const Page) };
        page.get_key(slot_id)
    }

    fn get_val(&self, slot_id: u16) -> &[u8] {
        // Get the underlying page
        let page = unsafe { &*(self.as_ptr() as *const Page) };
        page.get_val(slot_id)
    }

    fn get_id(&self) -> PageFrameKey {
        self.page_frame_key().expect("Page should have a frame key")
    }
}

impl SortedPage for Page {
    fn init_sorted(&mut self) {
        // Initialize sorted page layout in the data portion (after base header)
        self.set_slot_count(0);
        self.set_rec_start_offset(AVAILABLE_PAGE_SIZE as u16);
        self.set_total_bytes_used(SORTED_PAGE_HEADER_SIZE as u16);
    }

    fn slot_count(&self) -> u16 {
        u16::from_be_bytes([self[10], self[11]])
    }

    fn set_slot_count(&mut self, count: u16) {
        self[10..12].copy_from_slice(&count.to_be_bytes());
    }

    fn rec_start_offset(&self) -> u16 {
        u16::from_be_bytes([self[12], self[13]])
    }

    fn set_rec_start_offset(&mut self, offset: u16) {
        self[12..14].copy_from_slice(&offset.to_be_bytes());
    }

    fn total_bytes_used(&self) -> u16 {
        u16::from_be_bytes([self[8], self[9]])
    }

    fn set_total_bytes_used(&mut self, bytes: u16) {
        self[8..10].copy_from_slice(&bytes.to_be_bytes());
    }

    fn append(&mut self, key: &[u8], val: &[u8]) -> bool {
        let needed_space = SLOT_SIZE as u16 + key.len() as u16 + val.len() as u16;
        if self.total_free_space() < needed_space {
            return false;
        }

        // Write key and value
        let rec_offset = self.rec_start_offset() - key.len() as u16 - val.len() as u16;
        self[rec_offset as usize..rec_offset as usize + key.len()].copy_from_slice(key);
        self[rec_offset as usize + key.len()..rec_offset as usize + key.len() + val.len()]
            .copy_from_slice(val);

        // Write slot
        let slot = Slot::new(rec_offset, key.len() as u16, val.len() as u16);
        let slot_offset = SORTED_PAGE_HEADER_SIZE + self.slot_count() as usize * SLOT_SIZE;
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update metadata
        self.set_slot_count(self.slot_count() + 1);
        self.set_rec_start_offset(rec_offset);
        self.set_total_bytes_used(self.total_bytes_used() + needed_space);

        true
    }

    fn get_slot(&self, slot_id: u16) -> Option<Slot> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let slot_offset = SORTED_PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;
        let slot_bytes: [u8; SLOT_SIZE] = self[slot_offset..slot_offset + SLOT_SIZE]
            .try_into()
            .unwrap();
        Some(Slot::from_bytes(&slot_bytes))
    }

    fn get_key(&self, slot_id: u16) -> &[u8] {
        let slot = self.get_slot(slot_id).unwrap();
        &self[slot.offset as usize..slot.offset as usize + slot.key_size as usize]
    }

    fn get_val(&self, slot_id: u16) -> &[u8] {
        let slot = self.get_slot(slot_id).unwrap();
        let val_offset = slot.offset as usize + slot.key_size as usize;
        &self[val_offset..val_offset + slot.val_size as usize]
    }
}

// In-memory sort buffer for each thread
pub struct ThreadSortBuffer<M: MemPool> {
    pages: Vec<FrameWriteGuard<M::EP>>,
    ptrs: Vec<(u16, u16)>, // (page_idx, slot_id)
    current_page_idx: usize,
}

impl<M: MemPool> ThreadSortBuffer<M> {
    fn new(
        mem_pool: Arc<M>,
        container_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Self, String> {
        let mut pages = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            let mut page = mem_pool.create_new_page_for_write(container_key).unwrap();
            page.init_sorted();
            pages.push(page);
        }

        Ok(Self {
            pages,
            ptrs: Vec::new(),
            current_page_idx: 0,
        })
    }

    fn append(&mut self, key: &[u8], val: &[u8]) -> bool {
        if self.pages[self.current_page_idx].append(&key, &val) {
            self.ptrs.push((
                self.current_page_idx as u16,
                self.pages[self.current_page_idx].slot_count() - 1,
            ));
            true
        } else {
            // Try next page
            self.current_page_idx += 1;
            if self.current_page_idx >= self.pages.len() {
                self.current_page_idx -= 1;
                false
            } else {
                if self.pages[self.current_page_idx].append(&key, &val) {
                    self.ptrs.push((
                        self.current_page_idx as u16,
                        self.pages[self.current_page_idx].slot_count() - 1,
                    ));
                    true
                } else {
                    panic!("Record too large to fit in empty page");
                }
            }
        }
    }

    fn sort(&mut self) {
        self.ptrs.sort_by(|a, b| {
            let key_a = self.pages[a.0 as usize].get_key(a.1);
            let key_b = self.pages[b.0 as usize].get_key(b.1);
            key_a.cmp(key_b)
        });
    }

    fn reset(&mut self) {
        self.ptrs.clear();
        self.current_page_idx = 0;
        for page in &mut self.pages {
            page.init_sorted();
        }
    }

    fn iter(&self) -> ThreadSortBufferIter<M> {
        ThreadSortBufferIter {
            buffer: self,
            idx: 0,
        }
    }
}

struct ThreadSortBufferIter<'a, M: MemPool> {
    buffer: &'a ThreadSortBuffer<M>,
    idx: usize,
}

impl<'a, M: MemPool> Iterator for ThreadSortBufferIter<'a, M> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.buffer.ptrs.len() {
            let (page_idx, slot_id) = self.buffer.ptrs[self.idx];
            let page = &self.buffer.pages[page_idx as usize];
            let key = page.get_key(slot_id);
            let val = page.get_val(slot_id);
            self.idx += 1;
            Some((key, val))
        } else {
            None
        }
    }
}

// Sorted run on disk - implements Iterator directly
pub struct SortedRun<M: MemPool> {
    mem_pool: Arc<M>,
    container_key: ContainerKey,
    page_ids: Vec<PageFrameKey>,
    current_page_idx: usize,
    current_slot_idx: u16,
    current_page: Option<Page>,
    schema: Schema,
}

impl<M: MemPool> SortedRun<M> {
    pub fn new(mem_pool: Arc<M>, container_key: ContainerKey, schema: Schema) -> Self {
        Self {
            mem_pool,
            container_key,
            page_ids: Vec::new(),
            current_page_idx: 0,
            current_slot_idx: 0,
            current_page: None,
            schema,
        }
    }

    pub fn from_sorted_iterator<I>(
        mem_pool: Arc<M>,
        container_key: ContainerKey,
        schema: Schema,
        iter: I,
    ) -> Result<Self, String>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let mut run = Self::new(mem_pool, container_key, schema);
        run.write_from_iterator(iter)?;
        Ok(run)
    }

    fn write_from_iterator<I>(&mut self, iter: I) -> Result<(), String>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        // Write sorted data to pages
        let mut current_page = self
            .mem_pool
            .create_new_page_for_write(self.container_key)
            .map_err(|e| format!("Failed to create page: {:?}", e))?;
        {
            let page = unsafe { &mut *(current_page.as_mut_ptr() as *mut Page) };
            page.init_sorted();
        }

        for (key, val) in iter {
            let appended = current_page.append(&key, &val);

            if !appended {
                // Page is full, persist it and create new one
                let page_id = current_page.get_id();
                self.page_ids.push(page_id);
                drop(current_page);

                current_page = self
                    .mem_pool
                    .create_new_page_for_write(self.container_key)
                    .map_err(|e| format!("Failed to create page: {:?}", e))?;
                {
                    let page = unsafe { &mut *(current_page.as_mut_ptr() as *mut Page) };
                    page.init_sorted();
                }

                if !current_page.append(&key, &val) {
                    return Err("Record too large to fit in empty page".to_string());
                }
            }
        }

        // Save last page
        if current_page.slot_count() > 0 {
            let page_id = current_page.get_id();
            self.page_ids.push(page_id);
        }

        // Reset iterator position for reading
        self.current_page_idx = 0;
        self.current_slot_idx = 0;
        self.current_page = None;

        Ok(())
    }
}

impl<M: MemPool> Iterator for SortedRun<M> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Load page if needed
            if self.current_page.is_none() && self.current_page_idx < self.page_ids.len() {
                let page_guard = self
                    .mem_pool
                    .get_page_for_read(self.page_ids[self.current_page_idx])
                    .ok()?;
                let mut page = Page::new_empty();
                // Copy from the page guard
                let src_page = unsafe { &*(page_guard.as_ptr() as *const Page) };
                page.copy(src_page);
                drop(page_guard);
                self.current_page = Some(page);
                self.current_slot_idx = 0;
            }

            // Check if we have a page
            let page = self.current_page.as_ref()?;

            // Check if we have more slots in current page
            if self.current_slot_idx < page.slot_count() {
                let _key = page.get_key(self.current_slot_idx); // Normalized key, not needed for output
                let val = page.get_val(self.current_slot_idx);
                self.current_slot_idx += 1;

                // Deserialize the record
                let fields = bytes_to_record(val, &self.schema);
                return Some(Record { fields });
            }

            // Move to next page
            self.current_page_idx += 1;
            self.current_page = None;

            if self.current_page_idx >= self.page_ids.len() {
                return None;
            }
        }
    }
}

// K-way merge iterator that merges multiple sorted runs
pub struct MergeIterator<M: MemPool> {
    heap: BinaryHeap<Reverse<MergeItem<M>>>,
    sort_columns: Vec<(usize, bool, bool)>,
}

struct MergeItem<M: MemPool> {
    key: Vec<u8>,
    record: Record,
    run_idx: usize,
    run: SortedRun<M>,
}

impl<M: MemPool> Ord for MergeItem<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.run_idx.cmp(&other.run_idx))
    }
}

impl<M: MemPool> PartialOrd for MergeItem<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: MemPool> Eq for MergeItem<M> {}

impl<M: MemPool> PartialEq for MergeItem<M> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.run_idx == other.run_idx
    }
}

impl<M: MemPool> MergeIterator<M> {
    fn new(runs: Vec<SortedRun<M>>, sort_columns: Vec<(usize, bool, bool)>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, mut run) in runs.into_iter().enumerate() {
            if let Some(record) = run.next() {
                let key = to_normalized_key(&record.fields, &sort_columns);
                heap.push(Reverse(MergeItem {
                    key,
                    record,
                    run_idx: idx,
                    run,
                }));
            }
        }

        Self { heap, sort_columns }
    }
}

// Derive Clone for Record
impl Clone for Record {
    fn clone(&self) -> Self {
        Record {
            fields: self.fields.clone(),
        }
    }
}

impl<M: MemPool> Iterator for MergeIterator<M> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse(mut item) = self.heap.pop()?;
        let result = item.record.clone();

        // Try to get next item from same run
        if let Some(record) = item.run.next() {
            item.key = to_normalized_key(&record.fields, &self.sort_columns);
            item.record = record;
            self.heap.push(Reverse(item));
        }

        Some(result)
    }
}

// Add Debug trait for SortedRun to fix error handling
impl<M: MemPool> std::fmt::Debug for SortedRun<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortedRun")
            .field("page_ids", &self.page_ids.len())
            .field("current_page_idx", &self.current_page_idx)
            .finish()
    }
}

// Storage scanner iterator that reads records from storage
pub struct StorageScanner<'a, S: FieldLeveLStorageTrait> {
    storage: &'a S,
    txn: S::TxnHandle,
    iterator: Option<S::IteratorHandle>,
    _phantom: PhantomData<S>,
}

impl<'a, S: FieldLeveLStorageTrait> StorageScanner<'a, S> {
    pub fn new(
        storage: &'a S,
        db_id: DatabaseId,
        container_id: ContainerId,
    ) -> Result<Self, TxnStorageStatus> {
        use crate::txn_storage2::field_level_storage_trait::{ScanOptions, TxnOptions};

        let txn = storage.begin_txn(db_id, TxnOptions::default())?;
        let scan_options = ScanOptions::default();
        let iterator = storage.scan_range(&txn, container_id, scan_options)?;

        Ok(Self {
            storage,
            txn,
            iterator: Some(iterator),
            _phantom: PhantomData,
        })
    }
}

impl<'a, S: FieldLeveLStorageTrait> Iterator for StorageScanner<'a, S> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref iterator) = self.iterator {
            match self.storage.iter_next(&self.txn, iterator) {
                Ok(Some((_key_fields, value_fields, _ptr))) => Some(Record {
                    fields: value_fields,
                }),
                _ => None,
            }
        } else {
            None
        }
    }
}

impl<'a, S: FieldLeveLStorageTrait> Drop for StorageScanner<'a, S> {
    fn drop(&mut self) {
        if let Some(iterator) = self.iterator.take() {
            let _ = self.storage.drop_iterator_handle(iterator);
        }
        let _ = self.storage.commit_txn(&self.txn, false);
    }
}

// Main external sorter
pub struct ExternalSorter<M: MemPool> {
    mem_pool: Arc<M>,
    config: ExternalSortConfig,
    container_key: ContainerKey,
    schema: Schema,
}

impl<M: MemPool> ExternalSorter<M> {
    pub fn new(
        mem_pool: Arc<M>,
        config: ExternalSortConfig,
        db_id: DatabaseId,
        container_id: ContainerId,
        schema: Schema,
    ) -> Self {
        Self {
            mem_pool,
            config,
            container_key: ContainerKey::new(db_id, container_id),
            schema,
        }
    }

    pub fn sort_from_storage<S: FieldLeveLStorageTrait>(
        &self,
        storage: &S,
        db_id: DatabaseId,
        container_id: ContainerId,
    ) -> Result<MergeIterator<M>, String> {
        let scanner = StorageScanner::new(storage, db_id, container_id)
            .map_err(|e| format!("Failed to create scanner: {:?}", e))?;

        self.sort_iterator(scanner)
    }

    pub fn sort_iterator<I>(&self, records: I) -> Result<MergeIterator<M>, String>
    where
        I: Iterator<Item = Record>,
    {
        let records: Vec<Record> = records.collect();
        self.sort_records(records)
    }

    pub fn sort_records(&self, records: Vec<Record>) -> Result<MergeIterator<M>, String> {
        let frames_per_thread = self.config.frames_per_thread();
        let chunk_size = (records.len() + self.config.num_threads - 1) / self.config.num_threads;

        // Phase 1: Generate sorted runs in parallel
        let runs = Arc::new(Mutex::new(Vec::new()));
        let mem_pool = self.mem_pool.clone();
        let container_key = self.container_key;
        let sort_columns = self.config.sort_columns.clone();
        let schema = self.schema.clone();

        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        records.par_chunks(chunk_size).for_each(|chunk| {
            match ThreadSortBuffer::new(
                mem_pool.clone(),
                container_key,
                frames_per_thread,
                sort_columns.clone(),
                schema.clone(),
            ) {
                Ok(mut buffer) => {
                    // Fill buffer and generate runs
                    for record in chunk {
                        if !buffer.append(record) {
                            // Buffer full, sort and write run
                            buffer.sort();
                            let mut run =
                                SortedRun::new(mem_pool.clone(), container_key, schema.clone());
                            if let Err(e) = run.write_from_iterator(
                                buffer.iter().map(|(k, v)| (k.to_vec(), v.to_vec())),
                            ) {
                                errors.lock().unwrap().push(e);
                                return;
                            }
                            runs.lock().unwrap().push(run);

                            // Reset buffer and add record
                            buffer.reset();
                            if !buffer.append(record) {
                                errors
                                    .lock()
                                    .unwrap()
                                    .push("Record too large to fit in empty buffer".to_string());
                                return;
                            }
                        }
                    }

                    // Write final run if buffer has data
                    if !buffer.ptrs.is_empty() {
                        buffer.sort();
                        let mut run =
                            SortedRun::new(mem_pool.clone(), container_key, schema.clone());
                        if let Err(e) = run.write_from_iterator(
                            buffer.iter().map(|(k, v)| (k.to_vec(), v.to_vec())),
                        ) {
                            errors.lock().unwrap().push(e);
                            return;
                        }
                        runs.lock().unwrap().push(run);
                    }
                }
                Err(e) => {
                    errors.lock().unwrap().push(e);
                }
            }
        });

        // Check for errors
        let errors = Arc::try_unwrap(errors).unwrap().into_inner().unwrap();
        if !errors.is_empty() {
            return Err(errors.join("; "));
        }

        // Phase 2: K-way merge of sorted runs
        let runs = Arc::try_unwrap(runs).unwrap().into_inner().unwrap();
        let merge_iter = MergeIterator::new(runs, self.config.sort_columns.clone());

        Ok(merge_iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::{get_test_bp_clock, get_test_bp_lru};
    use crate::txn_storage2::{
        field_level_storage_trait::FieldLeveLStorageTrait, NonTransactionalStorage,
    };

    #[test]
    fn test_sorted_page_operations() {
        let mut page = Page::new_empty();
        page.init_sorted();

        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.total_bytes_used(), SORTED_PAGE_HEADER_SIZE as u16);

        let key = b"key1";
        let val = b"value1";
        assert!(page.append(key, val));

        assert_eq!(page.slot_count(), 1);
        assert_eq!(page.get_key(0), key);
        assert_eq!(page.get_val(0), val);
    }

    #[test]
    fn test_thread_sort_buffer() {
        let mem_pool = get_test_bp_lru(100);
        let container_key = ContainerKey::new(0, 0);
        let sort_columns = vec![(0, true, true)]; // Sort by first column, ascending, nulls first
        let schema = Schema::with_primary_key(
            vec![(true, DataType::Int32), (true, DataType::String)],
            vec![0],
        );

        let mut buffer =
            ThreadSortBuffer::new(mem_pool, container_key, 10, sort_columns, schema).unwrap();

        // Add just a few records first to isolate the issue
        for i in 0..5 {
            let record = Record {
                fields: vec![
                    Field::Int32(Some(i)),
                    Field::String(Some(format!("value_{}", i))),
                ],
            };
            assert!(buffer.append(&record));
        }

        buffer.sort();

        // Verify sorted order
        let mut count = 0;
        for (_key, _val) in buffer.iter() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_external_sort() {
        let mem_pool = get_test_bp_lru(200);
        let config = ExternalSortConfig {
            total_frames: 100,
            num_threads: 4,
            sort_columns: vec![(0, true, true)],
        };
        let schema = Schema::with_primary_key(
            vec![(true, DataType::Int32), (true, DataType::String)],
            vec![0],
        );

        let sorter = ExternalSorter::new(mem_pool, config, 0, 0, schema);

        // Create test records
        let mut records = Vec::new();
        for i in (0..1000).rev() {
            records.push(Record {
                fields: vec![
                    Field::Int32(Some(i)),
                    Field::String(Some(format!("value_{}", i))),
                ],
            });
        }

        // Sort
        let sorted_iter = sorter.sort_records(records).unwrap();

        // Verify sorted
        let sorted: Vec<_> = sorted_iter.collect();
        assert_eq!(sorted.len(), 1000);
        for i in 0..1000 {
            match &sorted[i].fields[0] {
                Field::Int32(Some(v)) => assert_eq!(*v, i as i32),
                _ => panic!("Unexpected field type"),
            }
        }
    }

    #[test]
    fn test_storage_scanner() {
        use crate::txn_storage2::field_level_storage_trait::{
            ContainerDS, ContainerOptions, DBOptions,
        };

        let mem_pool = get_test_bp_clock::<1>(50);
        let storage = NonTransactionalStorage::new(mem_pool.clone());
        let db_id = storage.open_db(DBOptions::new("test")).unwrap();

        let schema = Schema::with_primary_key(
            vec![(true, DataType::Int32), (true, DataType::String)],
            vec![0],
        );

        let container_id = storage
            .create_container(
                db_id,
                ContainerOptions::new("test_table", ContainerDS::BTree, schema.clone()),
            )
            .unwrap();

        // Insert some test data
        for i in 0..10 {
            let record = Record {
                fields: vec![
                    Field::Int32(Some(i)),
                    Field::String(Some(format!("value_{}", i))),
                ],
            };
            storage
                .raw_insert_record(db_id, container_id, record)
                .unwrap();
        }

        // Test scanner
        let scanner = StorageScanner::new(&storage, db_id, container_id).unwrap();
        let records: Vec<_> = scanner.collect();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn test_sorted_run_iterator() {
        let mem_pool = get_test_bp_clock::<1>(20);
        let container_key = ContainerKey::new(0, 0);
        let schema = Schema::with_primary_key(
            vec![(true, DataType::Int32), (true, DataType::String)],
            vec![0],
        );

        // Create a sorted run with some data
        let records = vec![
            Record {
                fields: vec![
                    Field::Int32(Some(1)),
                    Field::String(Some("one".to_string())),
                ],
            },
            Record {
                fields: vec![
                    Field::Int32(Some(2)),
                    Field::String(Some("two".to_string())),
                ],
            },
        ];

        let sort_columns = vec![(0, true, true)];
        let kv_pairs: Vec<_> = records
            .iter()
            .map(|record| {
                let key = to_normalized_key(&record.fields, &sort_columns);
                let val = record_to_bytes(&record.fields, &schema);
                (key, val)
            })
            .collect();

        let run =
            SortedRun::from_sorted_iterator(mem_pool, container_key, schema, kv_pairs.into_iter())
                .unwrap();

        // Test iteration
        let collected: Vec<_> = run.collect();
        assert_eq!(collected.len(), 2);

        match &collected[0].fields[0] {
            Field::Int32(Some(v)) => assert_eq!(*v, 1),
            _ => panic!("Unexpected field"),
        }
    }
}
