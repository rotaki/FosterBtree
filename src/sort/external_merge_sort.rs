use std::sync::{Arc, Mutex};
use std::thread;
use std::marker::PhantomData;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::bp::{ContainerId, ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, PageFrameKey};
use crate::page::{Page, AVAILABLE_PAGE_SIZE};
use super::sorter::{SortStrategy, SortInput};

// Page layout constants for sorted pages
const SORTED_PAGE_HEADER_SIZE: usize = 14;
const SLOT_SIZE: usize = 6;

// ============================================================================
// External merge sort strategy
// ============================================================================
pub struct ExternalMergeSort<M: MemPool + 'static> {
    _phantom: PhantomData<M>,
}

impl<M: MemPool + 'static> ExternalMergeSort<M> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<M: MemPool + 'static> SortStrategy<M> for ExternalMergeSort<M> {
    fn sort(
        &self,
        input: SortInput<M>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        let frames_per_thread = input.config.frames_per_thread();
        let runs = Arc::new(Mutex::new(Vec::new()));
        
        // Process each iterator in parallel using scoped threads
        thread::scope(|s| {
            let mut handles = Vec::new();
            
            for (idx, iterator) in input.iterators.into_iter().enumerate() {
                let mem_pool = input.mem_pool.clone();
                let container_key = ContainerKey::new(0, idx as ContainerId);
                let runs_clone = runs.clone();
                
                let handle = s.spawn(move || {
                    let mut buffer = ThreadSortBuffer::new(
                        mem_pool.clone(),
                        container_key,
                        frames_per_thread
                    ).unwrap();
                    
                    // Fill buffer and create sorted runs
                    let mut thread_runs = Vec::new();
                    
                    for (key, val) in iterator {
                        if !buffer.append(&key, &val) {
                            // Buffer full, sort and write run
                            buffer.sort();
                            let run = SortedRun::write_from_iterator(
                                mem_pool.clone(),
                                container_key,
                                buffer.iter().map(|(k, v)| (k.to_vec(), v.to_vec())),
                            ).unwrap();
                            thread_runs.push(run);
                            
                            // Reset buffer and add current record
                            buffer.reset();
                            buffer.append(&key, &val);
                        }
                    }
                    
                    // Handle remaining records in buffer
                    if buffer.ptrs.len() > 0 {
                        buffer.sort();
                        let run = SortedRun::write_from_iterator(
                            mem_pool.clone(),
                            container_key,
                            buffer.iter().map(|(k, v)| (k.to_vec(), v.to_vec())),
                        ).unwrap();
                        thread_runs.push(run);
                    }
                    
                    // Add runs to shared list
                    runs_clone.lock().unwrap().extend(thread_runs);
                });
                
                handles.push(handle);
            }
            
            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }
        });
        
        // Extract runs and create merge iterator
        let runs = Arc::try_unwrap(runs).unwrap().into_inner().unwrap();
        
        if runs.is_empty() {
            return Box::new(std::iter::empty());
        }
        
        // Create iterators from runs
        let run_iters: Vec<_> = runs.into_iter()
            .map(|run| SortedRunIterator::new(run, vec![], vec![]))
            .collect();
        
        Box::new(MergeIterator::new(run_iters))
    }
    
    fn name(&self) -> &str {
        "External Merge Sort"
    }
}

// ============================================================================
// Page-based structures for external merge sort
// ============================================================================

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
trait DataPage {
    fn init_data_page(&mut self);
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

impl DataPage for Page {
    fn init_data_page(&mut self) {
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

        let rec_offset = self.rec_start_offset() - key.len() as u16 - val.len() as u16;
        self[rec_offset as usize..rec_offset as usize + key.len()].copy_from_slice(key);
        self[rec_offset as usize + key.len()..rec_offset as usize + key.len() + val.len()]
            .copy_from_slice(val);

        let slot = Slot::new(rec_offset, key.len() as u16, val.len() as u16);
        let slot_offset = SORTED_PAGE_HEADER_SIZE + self.slot_count() as usize * SLOT_SIZE;
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

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
            page.init_data_page();
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
            page.init_data_page();
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

pub struct SortedRun<M: MemPool> {
    mem_pool: Arc<M>,
    c_key: ContainerKey,
    stats: Vec<(Vec<u8>, PageFrameKey)>,
}

impl<M: MemPool> SortedRun<M> {
    pub fn write_from_iterator<I>(
        mem_pool: Arc<M>,
        container_key: ContainerKey,
        iter: I,
    ) -> Result<Self, String>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let mut stats = Vec::new();
        let mut page = mem_pool.create_new_page_for_write(container_key).unwrap();
        page.init_data_page();

        for (count, (key, val)) in iter.enumerate() {
            if page.append(&key, &val) {
                if count == 0 {
                    stats.push((
                        key.clone(),
                        PageFrameKey::new_with_frame_id(
                            container_key,
                            page.page_id(),
                            page.frame_id(),
                        ),
                    ));
                }
            } else {
                page = mem_pool.create_new_page_for_write(container_key).unwrap();
                page.init_data_page();
                if page.append(&key, &val) {
                    stats.push((
                        key.clone(),
                        PageFrameKey::new_with_frame_id(
                            container_key,
                            page.page_id(),
                            page.frame_id(),
                        ),
                    ));
                } else {
                    panic!("Record too large to fit in empty page");
                }
            }
        }

        Ok(Self {
            mem_pool,
            c_key: container_key,
            stats,
        })
    }

    pub fn get_page(&self, index: usize) -> FrameReadGuard<M::EP> {
        let page_key = &self.stats[index].1;
        self.mem_pool.get_page_for_read(*page_key).unwrap()
    }

    fn find_largest_page_key_less_than(&self, key: &[u8]) -> usize {
        let mut ok = 0;
        let mut ng = self.stats.len() - 1;

        if self.stats[ok].0.as_slice() >= key {
            return ok;
        }

        if self.stats[ng].0.as_slice() < key {
            return ng;
        }

        while ok + 1 < ng {
            let mid = ok + (ng - ok) / 2;
            if self.stats[mid].0.as_slice() < key {
                ok = mid;
            } else {
                ng = mid;
            }
        }

        ok
    }

    fn find_smallest_page_key_greater_than_or_equal(&self, key: &[u8]) -> usize {
        let mut ok = self.stats.len() - 1;
        let mut ng = 0;

        if self.stats[ok].0.as_slice() < key {
            return ok + 1;
        }

        if self.stats[ng].0.as_slice() >= key {
            return ng;
        }

        while ng + 1 < ok {
            let mid = ng + (ok - ng) / 2;
            if self.stats[mid].0.as_slice() < key {
                ng = mid;
            } else {
                ok = mid;
            }
        }

        ok
    }
}

struct SortedRunIterator<M: MemPool> {
    sorted_run: SortedRun<M>,
    lower_inc: Vec<u8>,
    upper_exc: Vec<u8>,
    start_idx: usize,
    end_idx: usize,
    current_page: Option<FrameReadGuard<M::EP>>,
    current_page_idx: usize,
    current_slot_id: u16,
}

impl<M: MemPool> SortedRunIterator<M> {
    pub fn new(sorted_run: SortedRun<M>, lower_inc: Vec<u8>, upper_exc: Vec<u8>) -> Self {
        let start_idx = if lower_inc.is_empty() { 
            0 
        } else { 
            sorted_run.find_largest_page_key_less_than(&lower_inc) 
        };
        
        let end_idx = if upper_exc.is_empty() { 
            sorted_run.stats.len() 
        } else { 
            sorted_run.find_smallest_page_key_greater_than_or_equal(&upper_exc) 
        };
        
        Self {
            sorted_run,
            lower_inc,
            upper_exc,
            start_idx,
            end_idx,
            current_page: None,
            current_page_idx: start_idx,
            current_slot_id: 0,
        }
    }
}

impl<M: MemPool> Iterator for SortedRunIterator<M> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_idx >= self.end_idx {
            return None;
        }

        if self.current_page.is_none() {
            self.current_page = Some(self.sorted_run.get_page(self.current_page_idx));
            self.current_slot_id = 0;
        }

        let page = self.current_page.as_ref().unwrap();

        if self.current_slot_id >= page.slot_count() {
            self.current_page = None;
            self.current_page_idx += 1;
            self.current_slot_id = 0;
            return self.next();
        }

        let key = page.get_key(self.current_slot_id);

        if !self.lower_inc.is_empty() && key < self.lower_inc.as_slice() {
            self.current_slot_id += 1;
            return self.next();
        }

        if !self.upper_exc.is_empty() && key >= self.upper_exc.as_slice() {
            self.current_page = None;
            self.current_page_idx = self.end_idx;
            self.current_slot_id = 0;
            return None;
        }

        let val = page.get_val(self.current_slot_id);
        self.current_slot_id += 1;

        Some((key.to_vec(), val.to_vec()))
    }
}

// K-way merge iterator that merges multiple sorted runs
pub struct MergeIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)> + Send,
{
    heap: BinaryHeap<Reverse<MergeItem<I>>>,
}

struct MergeItem<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
{
    key: Vec<u8>,
    val: Vec<u8>,
    run_idx: usize,
    run: I,
}

impl<I> Ord for MergeItem<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.run_idx.cmp(&other.run_idx))
    }
}

impl<I> PartialOrd for MergeItem<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Eq for MergeItem<I> where I: Iterator<Item = (Vec<u8>, Vec<u8>)> {}

impl<I> PartialEq for MergeItem<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.run_idx == other.run_idx
    }
}

impl<I> MergeIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)> + Send,
{
    pub fn new(runs: Vec<I>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, mut run) in runs.into_iter().enumerate() {
            if let Some((key, val)) = run.next() {
                heap.push(Reverse(MergeItem {
                    key,
                    val,
                    run_idx: idx,
                    run,
                }));
            }
        }

        Self { heap }
    }
}

impl<I> Iterator for MergeIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)> + Send,
{
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse(mut item) = self.heap.pop()?;

        // Return the key-value pair
        let result = (item.key.clone(), item.val);

        // Try to get next item from same run
        if let Some((next_key, next_val)) = item.run.next() {
            item.key = next_key;
            item.val = next_val;
            self.heap.push(Reverse(item));
        }

        Some(result)
    }
}