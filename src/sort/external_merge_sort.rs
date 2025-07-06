use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::thread;

use super::sorter::{SortInput, SortStrategy};
use crate::bp::{
    ContainerId, ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, PageFrameKey,
};
use crate::page::{Page, AVAILABLE_PAGE_SIZE};

// Page layout constants for sorted pages
const SORTED_PAGE_HEADER_SIZE: usize = 14;
const SLOT_SIZE: usize = 6;

pub struct ExternalMergeSortConfig {
    pub num_threads: usize,
    pub total_frames: usize,
}

impl ExternalMergeSortConfig {
    pub fn frames_per_thread(&self) -> usize {
        self.total_frames / self.num_threads
    }
}

// ============================================================================
// External merge sort strategy
// ============================================================================
pub struct ExternalMergeSort<M: MemPool> {
    config: ExternalMergeSortConfig,
    mem_pool: Arc<M>,
}

impl<M: MemPool> ExternalMergeSort<M> {
    pub fn new(num_threads: usize, total_frames: usize, mem_pool: Arc<M>) -> Self {
        Self {
            config: ExternalMergeSortConfig {
                num_threads,
                total_frames,
            },
            mem_pool,
        }
    }
}

impl<M: MemPool + 'static> SortStrategy for ExternalMergeSort<M> {
    fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        let frames_per_thread = self.config.frames_per_thread();

        let iterators = input.create_iterators(self.config.num_threads);

        // Process each iterator in parallel using scoped threads
        let mut run_iters = Vec::new();

        thread::scope(|s| {
            let mut handles = Vec::new();

            for (_thread_id, mut iterator) in iterators.into_iter().enumerate() {
                let handle = s.spawn(move || {
                    // Currently employ a simple implementation
                    let mut run = Vec::new();
                    while let Some((key, val)) = iterator.next() {
                        run.push((key, val));
                    }

                    // Sort the run in memory
                    run.sort_by(|a, b| a.0.cmp(&b.0));

                    // Return a iterator over the sorted run
                    Box::new(VecRunIter::new(run))
                        as Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>
                });
                handles.push(handle);
            }

            for handle in handles {
                let run_iter = handle.join().unwrap();
                run_iters.push(run_iter);
            }
        });

        Box::new(MergeIterator::new(run_iters))
    }

    fn name(&self) -> &str {
        "External Merge Sort"
    }
}

struct VecRunIter {
    data: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl VecRunIter {
    fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { data, index: 0 }
    }
}

impl Iterator for VecRunIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data.len() {
            let item = self.data[self.index].clone();
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}
/*
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
*/

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::{get_test_bp_lru, ContainerKey};
    use crate::sort::sorter::{SortInput, Sorter};

    #[test]
    fn test_external_merge_sort_unique_keys() {
        let mem_pool = get_test_bp_lru(100);
        let sorter = Sorter::new_external_merge_sort(4, 100, mem_pool);

        // Create key-value pairs where key is sort key and value is primary key
        let mut original_kv_pairs = Vec::new();
        for i in (0..1000).rev() {
            let key = i as i32;
            let value = (i * 2) as i32;
            let key = key.to_be_bytes().to_vec();
            let value = value.to_be_bytes().to_vec();
            original_kv_pairs.push((key, value));
        }

        let result = sorter
            .sort(SortInput::new_from_vec(original_kv_pairs.clone()))
            .collect::<Vec<_>>();

        original_kv_pairs.sort_by(|a, b| a.0.cmp(&b.0)); // Sort by key to ensure uniqueness

        // Check that the sorted iterator matches the expected sorted order
        assert_eq!(result.len(), original_kv_pairs.len());

        for (i, (key, value)) in result.iter().enumerate() {
            assert_eq!(key, &original_kv_pairs[i].0);
            assert_eq!(value, &original_kv_pairs[i].1);
        }
    }

    #[test]
    fn test_external_merge_sort_with_duplicates() {
        let mem_pool = get_test_bp_lru(100);
        let sorter = Sorter::new_external_merge_sort(4, 100, mem_pool);

        // Create key-value pairs with duplicate keys
        let mut original_kv_pairs = Vec::new();
        for i in 0..1000 {
            let key = (i % 100) as i32;
            let value = key * 2;
            let key = key.to_be_bytes().to_vec(); // Duplicate keys every 100
            let value = value.to_be_bytes().to_vec();
            original_kv_pairs.push((key, value));
        }

        let result = sorter
            .sort(SortInput::new_from_vec(original_kv_pairs.clone()))
            .collect::<Vec<_>>();
        original_kv_pairs.sort_by(|a, b| a.0.cmp(&b.0)); // Sort by key to ensure duplicates are handled

        // Check that the sorted iterator matches the expected sorted order
        assert_eq!(result.len(), original_kv_pairs.len());
        for (i, (key, value)) in result.iter().enumerate() {
            assert_eq!(key, &original_kv_pairs[i].0);
            assert_eq!(value, &original_kv_pairs[i].1);
        }
    }

    #[test]
    fn test_external_merge_sort_empty() {
        let mem_pool = get_test_bp_lru(100);
        let sorter = Sorter::new_external_merge_sort(4, 100, mem_pool);
        let result = sorter
            .sort(SortInput::new_from_vec(vec![]))
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_external_merge_sort_single_element() {
        let mem_pool = get_test_bp_lru(100);
        let sorter = Sorter::new_external_merge_sort(4, 100, mem_pool);

        let key = 42i32.to_be_bytes().to_vec();
        let value = 84i32.to_be_bytes().to_vec();
        let kv_pairs = vec![(key.clone(), value.clone())];

        let result = sorter
            .sort(SortInput::new_from_vec(kv_pairs))
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, key);
        assert_eq!(result[0].1, value);
    }

    #[test]
    fn test_external_merge_sort_multi_threaded() {
        // Test with different thread counts to ensure correctness
        for num_threads in vec![1, 2, 4, 8] {
            let mem_pool = get_test_bp_lru(200);
            let sorter = Sorter::new_external_merge_sort(num_threads, 200, mem_pool);

            // Create larger dataset to ensure multi-threading is utilized
            let mut original_kv_pairs = Vec::new();
            for i in (0..5000).rev() {
                let key = i as i32;
                let value = (i * 3) as i32;
                let key = key.to_be_bytes().to_vec();
                let value = value.to_be_bytes().to_vec();
                original_kv_pairs.push((key, value));
            }

            let result = sorter
                .sort(SortInput::new_from_vec(original_kv_pairs.clone()))
                .collect::<Vec<_>>();

            original_kv_pairs.sort_by(|a, b| a.0.cmp(&b.0));

            // Verify correctness
            assert_eq!(
                result.len(),
                original_kv_pairs.len(),
                "Failed with {} threads",
                num_threads
            );

            for (i, (key, value)) in result.iter().enumerate() {
                assert_eq!(
                    key, &original_kv_pairs[i].0,
                    "Key mismatch at index {} with {} threads",
                    i, num_threads
                );
                assert_eq!(
                    value, &original_kv_pairs[i].1,
                    "Value mismatch at index {} with {} threads",
                    i, num_threads
                );
            }
        }
    }

    #[test]
    fn test_external_merge_sort_large_values() {
        let mem_pool = get_test_bp_lru(100);
        let sorter = Sorter::new_external_merge_sort(2, 100, mem_pool);

        // Create key-value pairs with larger values
        let mut original_kv_pairs = Vec::new();
        for i in 0..100 {
            let key = i as i32;
            let key_bytes = key.to_be_bytes().to_vec();

            // Create a larger value (e.g., 100 bytes)
            let mut value = vec![0u8; 100];
            value[0..4].copy_from_slice(&key.to_be_bytes());

            original_kv_pairs.push((key_bytes, value));
        }

        let result = sorter
            .sort(SortInput::new_from_vec(original_kv_pairs.clone()))
            .collect::<Vec<_>>();

        original_kv_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        // Check that the sorted iterator matches the expected sorted order
        assert_eq!(result.len(), original_kv_pairs.len());
        for (i, (key, value)) in result.iter().enumerate() {
            assert_eq!(key, &original_kv_pairs[i].0);
            assert_eq!(value, &original_kv_pairs[i].1);
        }
    }

    #[test]
    fn test_merge_iterator_correctness() {
        // Test the MergeIterator directly with multiple sorted runs
        let run1 = vec![
            (vec![1u8], vec![10u8]),
            (vec![4u8], vec![40u8]),
            (vec![7u8], vec![70u8]),
        ];

        let run2 = vec![
            (vec![2u8], vec![20u8]),
            (vec![5u8], vec![50u8]),
            (vec![8u8], vec![80u8]),
        ];

        let run3 = vec![
            (vec![3u8], vec![30u8]),
            (vec![6u8], vec![60u8]),
            (vec![9u8], vec![90u8]),
        ];

        let runs: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> = vec![
            Box::new(run1.into_iter()),
            Box::new(run2.into_iter()),
            Box::new(run3.into_iter()),
        ];

        let merge_iter = MergeIterator::new(runs);
        let result: Vec<_> = merge_iter.collect();

        // Verify the merged result is sorted
        assert_eq!(result.len(), 9);
        for i in 0..9 {
            assert_eq!(result[i].0, vec![(i + 1) as u8]);
            assert_eq!(result[i].1, vec![((i + 1) * 10) as u8]);
        }
    }

    #[test]
    fn test_external_merge_sort_tpch_lineitem() {
        use crate::sort::sorter::DeserializeRecord;
        use crate::sort::tpch_loader::{lineitem_schema, TpchLoader};
        use crate::tpcc2::txn_utils::get_date_field;
        use crate::tpcc2::txn_utils::{get_f64_field, get_i32_field, get_string_field};
        use crate::txn_storage2::DataType;
        use chrono::NaiveDate;

        // Setup with a small scale factor
        let mem_pool = get_test_bp_lru(1000);
        let loader = TpchLoader::new(mem_pool.clone());

        // Load a very small scale factor for testing
        let scale_factor = 0.001; // This should generate ~6000 lineitem records
        let num_threads = 2;
        loader.load_lineitem(scale_factor, num_threads);

        let sorter = Sorter::new_external_merge_sort(4, 1000, mem_pool.clone());

        // Test 1: Sort by l_shipdate (column 10)
        println!("\nTest 1: Sorting by l_shipdate");
        let sort_cols = vec![(10, true, true)]; // Sort by shipdate ascending
        let payload_cols = vec![0, 1, 4, 5]; // l_orderkey, l_partkey, l_quantity, l_extendedprice

        let sort_input = SortInput::new_from_storage(
            loader.get_storage(),
            ContainerKey::new(loader.get_db_id(), loader.get_lineitem_cid()),
            lineitem_schema(),
            sort_cols.clone(),
            payload_cols.clone(),
        );

        let deserializer = DeserializeRecord::new(
            vec![(0, true, true)],    // Adjusted sort columns
            vec![DataType::DateTime], // l_shipdate
            vec![
                (false, DataType::Int32),   // l_orderkey
                (false, DataType::Int32),   // l_partkey
                (false, DataType::Float64), // l_quantity
                (false, DataType::Float64), // l_extendedprice
            ],
        );

        let sorted: Vec<_> = sorter
            .sort_with_deserialization(sort_input, deserializer)
            .collect();

        // Verify sorting order
        let mut prev_shipdate = NaiveDate::MIN;
        for (i, (key_fields, _)) in sorted.iter().enumerate().take(100) {
            let shipdate = get_date_field(&key_fields, 0);
            assert!(
                prev_shipdate <= shipdate,
                "Shipdate not in ascending order at index {}",
                i
            );
            prev_shipdate = shipdate;
        }
        println!("Sorted {} records by shipdate", sorted.len());

        // Test 2: Sort by l_extendedprice descending
        println!("\nTest 2: Sorting by l_extendedprice descending");
        let sort_cols = vec![(5, false, true)]; // Sort by extended price descending
        let payload_cols = vec![0, 3, 4]; // l_orderkey, l_linenumber, l_quantity

        let sort_input = SortInput::new_from_storage(
            loader.get_storage(),
            ContainerKey::new(loader.get_db_id(), loader.get_lineitem_cid()),
            lineitem_schema(),
            sort_cols.clone(),
            payload_cols.clone(),
        );

        let deserializer = DeserializeRecord::new(
            vec![(0, false, true)],  // Adjusted sort columns (descending)
            vec![DataType::Float64], // l_extendedprice
            vec![
                (false, DataType::Int32),   // l_orderkey
                (false, DataType::Int32),   // l_linenumber
                (false, DataType::Float64), // l_quantity
            ],
        );

        let sorted: Vec<_> = sorter
            .sort_with_deserialization(sort_input, deserializer)
            .collect();

        // Verify top 10 highest prices are in descending order
        println!("Top 10 highest extended prices:");
        for i in 0..sorted.len() {
            let price = get_f64_field(&sorted[i].0, 0);
            let orderkey = get_i32_field(&sorted[i].1, 0);
            let linenumber = get_i32_field(&sorted[i].1, 1);
            let quantity = get_f64_field(&sorted[i].1, 2);
            println!(
                "  Price: {:.2}, OrderKey: {}, LineNumber: {}, Quantity: {:.2}",
                price, orderkey, linenumber, quantity
            );

            if i > 0 {
                let prev_price = get_f64_field(&sorted[i - 1].0, 0);
                assert!(
                    prev_price >= price,
                    "Extended price not in descending order at index {}",
                    i
                );
            }
        }

        // Test 3: Multi-column sort - l_returnflag ASC, l_linestatus ASC, l_shipdate ASC
        println!("\nTest 3: Multi-column sort (returnflag, linestatus, shipdate)");
        let sort_cols = vec![
            (8, true, true),  // l_returnflag ascending
            (9, true, true),  // l_linestatus ascending
            (10, true, true), // l_shipdate ascending
        ];
        let payload_cols = vec![0, 3, 5]; // l_orderkey, l_linenumber, l_extendedprice

        let sort_input = SortInput::new_from_storage(
            loader.get_storage(),
            ContainerKey::new(loader.get_db_id(), loader.get_lineitem_cid()),
            lineitem_schema(),
            sort_cols.clone(),
            payload_cols.clone(),
        );

        let deserializer = DeserializeRecord::new(
            vec![(0, true, true), (1, true, true), (2, true, true)], // Adjusted sort columns
            vec![DataType::String, DataType::String, DataType::DateTime], // returnflag, linestatus, shipdate
            vec![
                (false, DataType::Int32),   // l_orderkey
                (false, DataType::Int32),   // l_linenumber
                (false, DataType::Float64), // l_extendedprice
            ],
        );

        let sorted: Vec<_> = sorter
            .sort_with_deserialization(sort_input, deserializer)
            .collect();

        // Verify multi-column sort order
        for i in 1..sorted.len() {
            let curr_rf = get_string_field(&sorted[i].0, 0);
            let curr_ls = get_string_field(&sorted[i].0, 1);
            let curr_shipdate = get_date_field(&sorted[i].0, 2);
            let prev_rf = get_string_field(&sorted[i - 1].0, 0);
            let prev_ls = get_string_field(&sorted[i - 1].0, 1);
            let prev_shipdate = get_date_field(&sorted[i - 1].0, 2);

            // Check sort order
            match prev_rf.cmp(&curr_rf) {
                std::cmp::Ordering::Less => {} // Correct order
                std::cmp::Ordering::Equal => {
                    // If returnflag is equal, check linestatus
                    match prev_ls.cmp(&curr_ls) {
                        std::cmp::Ordering::Less => {
                            // Correct order
                        }
                        std::cmp::Ordering::Equal => {
                            // If both returnflag and linestatus are equal, check shipdate
                            if prev_shipdate > curr_shipdate {
                                panic!(
                                    "Shipdate not in ascending order at index {}: {} > {}",
                                    i, prev_shipdate, curr_shipdate
                                );
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            panic!(
                                "Linestatus not in ascending order at index {}: {} > {}",
                                i, prev_ls, curr_ls
                            );
                        }
                    }
                }
                std::cmp::Ordering::Greater => {
                    panic!(
                        "Returnflag not in ascending order at index {}: {} > {}",
                        i, prev_rf, curr_rf
                    );
                }
            }
        }

        println!(
            "Successfully sorted {} lineitem records with multi-column sort",
            sorted.len()
        );
    }
}
