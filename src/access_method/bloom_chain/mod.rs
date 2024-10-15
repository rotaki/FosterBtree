mod hash_bloom_chain;

use std::{
    cell::UnsafeCell,
    fmt::write,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    u32,
};

use fastbloom::BloomFilter;

use super::{AccessMethodError, UniqueKeyIndex};
use crate::{access_method::chain::prelude::*, page::Page};
use crate::{
    bp::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    hybrid_latch::HybridLatchGuardedStructure,
    log_info,
    page::{PageId, AVAILABLE_PAGE_SIZE},
    rwlatch::RwLatch,
};

pub use hash_bloom_chain::HashBloomChain;

pub mod prelude {
    pub use super::BloomChain;
    pub use super::HashBloomChain;
    pub use super::PageSketch;
}

#[derive(Clone)]
pub struct PageSketch {
    page_id: PageId,
    frame_id: u32,
    inserted: BloomFilter, // Currently only keeps inserted keys. To be extended to keep deleted keys as well by using Cuckoo filter.
    free_space: u32,
    next: *const HybridLatchGuardedStructure<PageSketch>,
}

impl PageSketch {
    pub fn new(page_id: PageId, frame_id: u32, free_space: u32) -> Self {
        Self {
            page_id,
            frame_id,
            inserted: BloomFilter::with_num_bits(1024).expected_items(AVAILABLE_PAGE_SIZE / 100),
            free_space,
            next: std::ptr::null(),
        }
    }

    pub fn page_frame_key(&self, c_key: &ContainerKey) -> PageFrameKey {
        PageFrameKey::new_with_frame_id(c_key.clone(), self.page_id, self.frame_id)
    }

    pub fn get_frame_id(&self) -> u32 {
        self.frame_id
    }

    pub fn set_frame_id(&mut self, frame_id: u32) {
        self.frame_id = frame_id;
    }

    pub fn add_to_filter(&mut self, key: &[u8]) {
        self.inserted.insert(key);
    }

    pub fn set_free_space(&mut self, free_space: u32) {
        self.free_space = free_space;
    }

    pub fn set_next(&mut self, next: *const HybridLatchGuardedStructure<PageSketch>) {
        self.next = next;
    }

    pub fn likely_contains(&self, key: &[u8]) -> bool {
        // We are sure that the key is not present in the page if it is not in the inserted bloom filter.
        self.inserted.contains(key)
    }

    pub fn likely_has_enough_space(&self, key: &[u8], value: &[u8]) -> bool {
        self.free_space >= (key.len() + value.len() + 20) as u32 // We assume that metadata for each slot is around 20 bytes
    }
}

pub struct BloomChain<T: MemPool> {
    c_key: ContainerKey,
    bp: Arc<T>,
    page_chain: HybridLatchGuardedStructure<PageSketch>,
}

unsafe impl<T: MemPool> Sync for BloomChain<T> {}
unsafe impl<T: MemPool> Send for BloomChain<T> {}

impl<T: MemPool> BloomChain<T> {
    pub fn new(c_key: ContainerKey, bp: Arc<T>) -> Self {
        let mut first_page = bp.create_new_page_for_write(c_key).unwrap();
        first_page.init();
        let page_chain = HybridLatchGuardedStructure::new(PageSketch::new(
            first_page.get_id(),
            first_page.frame_id(),
            first_page.total_free_space(),
        ));

        Self {
            c_key,
            bp: bp.clone(),
            page_chain,
        }
    }

    pub fn load(c_key: ContainerKey, bp: Arc<T>, first_page_id: PageId) -> Self {
        unimplemented!();
    }

    pub fn page_stats(&self, _verbose: bool) -> String {
        let mut generator = PageStatsGenerator::new();
        let traversal = BloomChainPageTraversal::new(self);
        traversal.visit(&mut generator);
        generator.to_string()
    }

    pub fn first_key(&self) -> PageFrameKey {
        loop {
            let guard = self.page_chain.optimistic_read();
            let page_key = unsafe { guard.get() }.page_frame_key(&self.c_key);
            if guard.validate() {
                return page_key;
            }
        }
    }

    fn read_page(&self, page_key: &PageFrameKey) -> FrameReadGuard {
        let base = 2;
        let mut attempts = 0;
        loop {
            let page = self.bp.get_page_for_read(*page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_info!(
                        "Failed to acquire read latch (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                    attempts += 1;
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn write_page(&self, page_key: &PageFrameKey) -> FrameWriteGuard {
        let base = 2;
        let mut attempts = 0;
        loop {
            let page = self.bp.get_page_for_write(*page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    log_info!(
                        "Failed to acquire write latch (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                    attempts += 1;
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let base = 2;
        let mut attempt = 0;
        while !self.insert_inner(key, value)? {
            // Exponential backoff
            std::thread::sleep(std::time::Duration::from_nanos(u64::pow(base, attempt)));
            attempt += 1;
        }
        Ok(())
    }

    fn insert_inner(&self, key: &[u8], value: &[u8]) -> Result<bool, AccessMethodError> {
        // Traverse to the end of the chain and insert the key-value pair
        let mut current_guard = self.page_chain.optimistic_read();
        loop {
            let vals = unsafe { current_guard.get() }.clone(); // Cloning is important
            if !current_guard.validate() {
                return Ok(false); // Retry
            }
            // If key is likely present in the page, read the page and check if the key is present
            if vals.likely_contains(key) {
                // Read the page and check if the key is present
                let page = self.read_page(&vals.page_frame_key(&self.c_key));
                // Validate the guard
                if !current_guard.validate() {
                    return Ok(false); // Retry
                }
                if page.binary_search(key).0 {
                    return Err(AccessMethodError::KeyDuplicate);
                }
            }
            // Go to the next entry in the chain
            let next = vals.next;
            if next.is_null() {
                // Next is null. Either we insert the key-value pair in the current page or
                // we create a new page and insert the key-value pair in the new page.
                // Either way, we need to upgrade the guard to exclusive mode.
                // Upgrading the guard will fail if the version of the guarded struct has changed.
                if let Some(mut write_guard) = current_guard.try_upgrade() {
                    // Write guard is created. We no longer need to care about the version.
                    // Check if the page has enough space to insert the key-value pair
                    let mut last_page = self.write_page(&vals.page_frame_key(&self.c_key));
                    if write_guard.likely_has_enough_space(key, value) {
                        // Insert into the page
                        last_page.insert(key, value).unwrap();
                        write_guard.set_free_space(last_page.total_free_space());
                        write_guard.add_to_filter(key);
                    } else {
                        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                        new_page.init();
                        new_page.insert(key, value).unwrap();
                        last_page.set_next_page(new_page.get_id(), new_page.frame_id());

                        let new_page_sketch = HybridLatchGuardedStructure::new(PageSketch::new(
                            new_page.get_id(),
                            new_page.frame_id(),
                            new_page.total_free_space(),
                        ));
                        new_page_sketch.write().add_to_filter(key);

                        // Leak the new page sketch and set the next pointer in the current page sketch
                        let new_page_sketch = Box::leak(Box::new(new_page_sketch));
                        write_guard.set_next(new_page_sketch);
                    }
                    return Ok(true);
                } else {
                    // Retry
                    return Ok(false);
                }
            } else {
                let next = unsafe { &*next };
                let next_guard = next.optimistic_read();
                // Validate the guard
                if !current_guard.validate() {
                    return Ok(false); // Retry
                }
                // Loop
                current_guard = next_guard;
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let base = 2;
        let mut attempt = 0;
        loop {
            match self.get_inner(key) {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => {
                    // Exponential backoff
                    std::thread::sleep(std::time::Duration::from_nanos(u64::pow(base, attempt)));
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn get_inner(&self, key: &[u8]) -> Result<Option<Vec<u8>>, AccessMethodError> {
        // Traverse the chain and get the value
        let mut current_guard = self.page_chain.optimistic_read();
        loop {
            let vals = unsafe { current_guard.get() }.clone(); // Cloning is important
            if !current_guard.validate() {
                return Ok(None);
            }
            // If key is likely present in the page, read the page and check if the key is present
            if vals.likely_contains(key) {
                // Read the page and check if the key is present
                let page = self.read_page(&vals.page_frame_key(&self.c_key));
                // Validate the guard
                if !current_guard.validate() {
                    return Ok(None);
                }
                // Page binary search
                if let Ok(value) = page.get(key) {
                    return Ok(Some(value.to_vec()));
                }
            }
            // Go to the next entry in the chain
            let next = vals.next;
            if next.is_null() {
                return Err(AccessMethodError::KeyNotFound);
            } else {
                let next = unsafe { &*next };
                let next_guard = next.optimistic_read();
                // Validate the guard
                if !current_guard.validate() {
                    return Ok(None);
                }
                // Loop
                current_guard = next_guard;
            }
        }
    }

    pub fn update(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let base = 2;
        let mut attempt = 0;
        while !self.update_inner(key, value)? {
            // Exponential backoff
            std::thread::sleep(std::time::Duration::from_nanos(u64::pow(base, attempt)));
            attempt += 1;
        }
        Ok(())
    }

    fn update_inner(&self, key: &[u8], value: &[u8]) -> Result<bool, AccessMethodError> {
        // Traverse the chain and update the value
        let mut current_guard = self.page_chain.optimistic_read();
        loop {
            let vals = unsafe { current_guard.get() }.clone(); // Cloning is important
            if !current_guard.validate() {
                return Ok(false);
            }
            // If key is likely present in the page, read the page and check if the key is present
            if vals.likely_contains(key) {
                // Update the current_guard to exclusive mode
                if let Some(mut write_guard) = current_guard.try_upgrade() {
                    // Read the page and check if the key is present
                    let mut page = self.write_page(&vals.page_frame_key(&self.c_key));
                    // Page binary search
                    let (found, slot_id) = page.binary_search(key);
                    if found {
                        page.update_at_slot_id(key, value, slot_id).unwrap(); // Currently simply unwraps it.
                        write_guard.set_frame_id(page.frame_id());
                        write_guard.set_free_space(page.total_free_space());
                        return Ok(true);
                    } else {
                        // This case is annoying. We got a false positive from the bloom filter.
                        // Simply go to the next entry in the chain. We do not need to validate
                        // since we have already upgraded the guard to exclusive mode.
                        let next = vals.next;
                        if next.is_null() {
                            return Err(AccessMethodError::KeyNotFound);
                        } else {
                            let next = unsafe { &*next };
                            let next_guard = next.optimistic_read();
                            current_guard = next_guard;
                        }
                    }
                } else {
                    // Retry
                    return Ok(false);
                }
            } else {
                // Go to the next entry in the chain
                let next = vals.next;
                if next.is_null() {
                    return Err(AccessMethodError::KeyNotFound);
                } else {
                    let next = unsafe { &*next };
                    let next_guard = next.optimistic_read();
                    // Validate the guard
                    if !current_guard.validate() {
                        return Ok(false);
                    }
                    // Loop
                    current_guard = next_guard;
                }
            }
        }
    }
}

impl<T: MemPool> UniqueKeyIndex for BloomChain<T> {
    type Iter = std::vec::IntoIter<(Vec<u8>, Vec<u8>)>;

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        BloomChain::insert(self, key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        BloomChain::get(self, key)
    }

    fn update(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        BloomChain::update(self, key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), AccessMethodError> {
        unimplemented!()
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        unimplemented!()
    }

    fn upsert_with_merge(
        &self,
        key: &[u8],
        value: &[u8],
        merge_fn: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        unimplemented!()
    }

    fn scan(self: &Arc<Self>) -> Self::Iter {
        unimplemented!()
    }

    fn scan_with_filter(
        self: &Arc<Self>,
        filter: Arc<dyn Fn(&[u8], &[u8]) -> bool + Send + Sync>,
    ) -> Self::Iter {
        unimplemented!()
    }
}

pub struct BloomChainPageTraversal<T: MemPool> {
    c_key: ContainerKey,
    first_page: PageFrameKey,
    mem_pool: Arc<T>,
}

impl<T: MemPool> BloomChainPageTraversal<T> {
    pub fn new(chain: &BloomChain<T>) -> Self {
        let c_key = chain.c_key.clone();
        let first_page = chain.first_key();
        Self {
            c_key,
            first_page,
            mem_pool: chain.bp.clone(),
        }
    }
}

pub trait PageVisitor {
    fn visit_pre(&mut self, page: &Page);
    fn visit_post(&mut self, page: &Page);
}

impl<T: MemPool> BloomChainPageTraversal<T> {
    pub fn visit<V>(&self, visitor: &mut V)
    where
        V: PageVisitor,
    {
        let mut stack = vec![(self.first_page, false)]; // (page_key, pre_visited)
        while let Some((next_key, pre_visited)) = stack.last_mut() {
            let page = self.mem_pool.get_page_for_read(*next_key).unwrap();
            if *pre_visited {
                visitor.visit_post(&page);
                stack.pop();
                continue;
            } else {
                *pre_visited = true;
                visitor.visit_pre(&page);
                if let Some((next_page_id, next_frame_id)) = page.next_page() {
                    stack.push((
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                        false,
                    ));
                }
            }
        }
    }
}

#[derive(Debug)]
struct PerPageStats {
    slot_count: usize,
    bytes_used: usize,
    free_space: usize,
}

impl PerPageStats {
    fn new(slot_count: usize, bytes_used: usize, free_space: usize) -> Self {
        Self {
            slot_count,
            bytes_used,
            free_space,
        }
    }
}

impl std::fmt::Display for PerPageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        result.push_str(&format!("Slot count: {}\n", self.slot_count));
        result.push_str(&format!("Total bytes used: {}\n", self.bytes_used));
        result.push_str(&format!("Total free space: {}\n", self.free_space));
        write!(f, "{}", result)
    }
}

struct PageStatsGenerator {
    stats: Vec<PerPageStats>,
}

impl PageStatsGenerator {
    fn new() -> Self {
        Self { stats: Vec::new() }
    }

    fn to_string(&self) -> String {
        // Print the min, avg, max of slot count, total bytes used, total free space

        let mut slot_count = Vec::new();
        let mut bytes_used = Vec::new();
        let mut free_space = Vec::new();

        for stat in &self.stats {
            slot_count.push(stat.slot_count);
            bytes_used.push(stat.bytes_used);
            free_space.push(stat.free_space);
        }

        let min_slot_count = slot_count.iter().min().unwrap();
        let avg_slot_count = slot_count.iter().sum::<usize>() / slot_count.len();
        let max_slot_count = slot_count.iter().max().unwrap();

        let min_bytes_used = bytes_used.iter().min().unwrap();
        let avg_bytes_used = bytes_used.iter().sum::<usize>() / bytes_used.len();
        let max_bytes_used = bytes_used.iter().max().unwrap();

        let min_free_space = free_space.iter().min().unwrap();
        let avg_free_space = free_space.iter().sum::<usize>() / free_space.len();
        let max_free_space = free_space.iter().max().unwrap();

        let mut result = String::new();
        result.push_str(&format!("Page count: {}\n", self.stats.len()));
        result.push_str(&format!(
            "Slot count: min={}, avg={}, max={}\n",
            min_slot_count, avg_slot_count, max_slot_count
        ));
        result.push_str(&format!(
            "Total bytes used: min={}, avg={}, max={}\n",
            min_bytes_used, avg_bytes_used, max_bytes_used
        ));
        result.push_str(&format!(
            "Total free space: min={}, avg={}, max={}\n",
            min_free_space, avg_free_space, max_free_space
        ));
        result
    }
}

impl PageVisitor for PageStatsGenerator {
    fn visit_pre(&mut self, page: &Page) {
        let slot_count = page.slot_count();
        let bytes_used = page.total_bytes_used();
        let free_space = page.total_free_space();
        let stats = PerPageStats::new(
            slot_count as usize,
            bytes_used as usize,
            free_space as usize,
        );
        self.stats.push(stats);
    }

    fn visit_post(&mut self, _page: &Page) {}
}

#[cfg(test)]
mod tests {

    use std::{collections::HashSet, fs::File, sync::Arc};

    use crate::{
        access_method::AccessMethodError,
        bp::{get_in_mem_pool, get_test_bp, BufferPool},
        log_info,
        prelude::UniqueKeyIndex,
        random::RandomKVs,
    };

    use super::{BloomChain, ContainerKey, HashReadOptimize, MemPool};

    fn to_bytes(num: usize) -> Vec<u8> {
        num.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> usize {
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    use rstest::rstest;

    fn setup_bloom_chain<T: MemPool>(bp: Arc<T>) -> BloomChain<T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);

        BloomChain::new(c_key, bp.clone())
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = setup_bloom_chain(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = chain.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_updates<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = setup_bloom_chain(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.insert(&key, &val).unwrap();
        }
        let new_val = vec![4_u8; 128];
        for i in order.iter() {
            println!(
                "**************************** Updating key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.update(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = chain.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
        let new_val = vec![5_u8; 512];
        for i in order.iter() {
            println!(
                "**************************** Updating key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.update(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = chain.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
    }

    // #[rstest]
    // #[case::bp(get_test_bp(20))]
    // #[case::in_mem(get_in_mem_pool())]
    // fn test_random_deletion<T: MemPool>(#[case] bp: Arc<T>) {
    //     let chain = setup_hashchain_empty(bp.clone());
    //     // Insert 1024 bytes
    //     let val = vec![3_u8; 1024];
    //     let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Inserting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         chain.insert(&key, &val).unwrap();
    //     }
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Deleting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         chain.delete(&key).unwrap();
    //     }
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Getting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         let current_val = chain.get(&key);
    //         assert_eq!(current_val, Err(AccessMethodError::KeyNotFound))
    //     }
    // }

    // #[rstest]
    // #[case::bp(get_test_bp(20))]
    // #[case::in_mem(get_in_mem_pool())]
    // fn test_random_upserts<T: MemPool>(#[case] bp: Arc<T>) {
    //     let chain = setup_bloom_chain(bp.clone());
    //     // Insert 1024 bytes
    //     let val = vec![3_u8; 1024];
    //     let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Upserting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         chain.upsert(&key, &val).unwrap();
    //     }
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Getting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         let current_val = chain.get(&key).unwrap();
    //         assert_eq!(current_val, val);
    //     }
    //     let new_val = vec![4_u8; 128];
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Upserting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         chain.upsert(&key, &new_val).unwrap();
    //     }
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Getting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         let current_val = chain.get(&key).unwrap();
    //         assert_eq!(current_val, new_val);
    //     }
    //     let new_val = vec![5_u8; 512];
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Upserting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         chain.upsert(&key, &new_val).unwrap();
    //     }
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Getting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         let current_val = chain.get(&key).unwrap();
    //         assert_eq!(current_val, new_val);
    //     }
    // }

    // #[rstest]
    // #[case::bp(get_test_bp(3))]
    // #[case::in_mem(get_in_mem_pool())]
    // fn test_upsert_with_merge<T: MemPool>(#[case] bp: Arc<T>) {
    //     let chain = setup_hashchain_empty(bp.clone());
    //     // Insert 1024 bytes
    //     let key = to_bytes(0);
    //     let vals = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
    //     for i in vals.iter() {
    //         println!(
    //             "**************************** Upserting key {} **************************",
    //             i
    //         );
    //         let val = to_bytes(*i);
    //         chain
    //             .upsert_with_merge(&key, &val, |old_val: &[u8], new_val: &[u8]| -> Vec<u8> {
    //                 // Deserialize old_val and new_val and add them.
    //                 let old_val = from_bytes(old_val);
    //                 let new_val = from_bytes(new_val);
    //                 to_bytes(old_val + new_val)
    //             })
    //             .unwrap();
    //     }
    //     let expected_val = vals.iter().sum::<usize>();
    //     let current_val = chain.get(&key).unwrap();
    //     assert_eq!(from_bytes(&current_val), expected_val);
    // }

    // #[rstest]
    // #[case::bp(get_test_bp(20))]
    // #[case::in_mem(get_in_mem_pool())]
    // fn test_scan<T: MemPool>(#[case] bp: Arc<T>) {
    //     let chain = Arc::new(setup_bloom_chain(bp.clone()));
    //     // Insert 1024 bytes
    //     let val = vec![3_u8; 1024];
    //     let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
    //     for i in order.iter() {
    //         println!(
    //             "**************************** Upserting key {} **************************",
    //             i
    //         );
    //         let key = to_bytes(*i);
    //         chain.upsert(&key, &val).unwrap();
    //     }

    //     let iter = chain.scan();
    //     let mut count = 0;
    //     for (key, current_val) in iter {
    //         let key = from_bytes(&key);
    //         println!(
    //             "**************************** Scanning key {} **************************",
    //             key
    //         );
    //         assert_eq!(current_val, val);
    //         count += 1;
    //     }
    //     assert_eq!(count, 10);
    // }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn test_insertion_stress<T: MemPool>(#[case] bp: Arc<T>) {
        let num_keys = 100000;
        let key_size = 8;
        let val_min_size = 50;
        let val_max_size = 100;
        let mut kvs = RandomKVs::new(
            true,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );
        let kvs = kvs.pop().unwrap();

        let chain = Arc::new(setup_bloom_chain(bp.clone()));

        // Write kvs to file
        // let kvs_file = "kvs.dat";
        // // serde cbor to write to file
        // let mut file = File::create(kvs_file).unwrap();
        // let kvs_str = serde_cbor::to_vec(&kvs).unwrap();
        // file.write_all(&kvs_str).unwrap();

        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Inserting {} key={:?} **************************",
                i, key
            );
            chain.insert(key, val).unwrap();
        }

        // let iter = chain.scan();
        // let mut count = 0;
        // for (key, current_val) in iter {
        //     println!(
        //         "**************************** Scanning key {:?} **************************",
        //         key
        //     );
        //     let val = kvs.get(&key).unwrap();
        //     assert_eq!(&current_val, val);
        //     count += 1;
        // }
        // assert_eq!(count, num_keys);

        for (key, val) in kvs.iter() {
            println!(
                "**************************** Getting key {:?} **************************",
                key
            );
            let current_val = chain.get(key).unwrap();
            assert_eq!(current_val, *val);
        }

        // let iter = chain.scan();
        // let mut count = 0;
        // for (key, current_val) in iter {
        //     println!(
        //         "**************************** Scanning key {:?} **************************",
        //         key
        //     );
        //     let val = kvs.get(&key).unwrap();
        //     assert_eq!(&current_val, val);
        //     count += 1;
        // }

        // assert_eq!(count, num_keys);

        // println!("{}", chain.page_stats(false));

        println!("SUCCESS");
    }

    // skip default
    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn replay_stress<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = setup_bloom_chain(bp.clone());

        let kvs_file = "kvs.dat";
        let file = File::open(kvs_file).unwrap();
        let kvs: RandomKVs = serde_cbor::from_reader(file).unwrap();

        let bug_occurred_at = 1138;
        for (i, (key, val)) in kvs.iter().enumerate() {
            if i == bug_occurred_at {
                break;
            }
            println!(
                "**************************** Inserting {} key={:?} **************************",
                i, key
            );
            chain.insert(key, val).unwrap();
        }

        let (k, v) = &kvs[bug_occurred_at];
        println!(
            "BUG INSERT ************** Inserting {} key={:?} **************************",
            bug_occurred_at, k
        );
        chain.insert(k, v).unwrap();

        /*
        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Getting {} key={} **************************",
                i,
                key
            );
            let key = to_bytes(*key);
            let current_val = chain.get_key(&key).unwrap();
            assert_eq!(current_val, *val);
        }
        */

        // let dot_string = chain.generate_dot();
        // let dot_file = "chain.dot";
        // let mut file = File::create(dot_file).unwrap();
        // // write dot_string as txt
        // file.write_all(dot_string.as_bytes()).unwrap();
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn test_parallel_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        // init_test_logger();
        let chain = Arc::new(setup_bloom_chain(bp.clone()));
        let num_keys = 50000;
        let key_size = 100;
        let val_min_size = 50;
        let val_max_size = 100;
        let num_threads = 3;
        let kvs = RandomKVs::new(
            true,
            false,
            num_threads,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        );
        let verify_kvs = kvs.clone();

        log_info!("Number of keys: {}", num_keys);

        // Use 3 threads to insert keys into the tree.
        // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
        std::thread::scope(
            // issue three threads to insert keys into the tree
            |s| {
                for kvs_i in kvs.iter() {
                    let chain = chain.clone();
                    s.spawn(move || {
                        log_info!("Spawned");
                        for (key, val) in kvs_i.iter() {
                            log_info!("Inserting key {:?}", key);
                            chain.insert(key, val).unwrap();
                        }
                    });
                }
            },
        );

        println!("page stats: {}", chain.page_stats(false));

        // Check if all keys have been inserted.
        for kvs_i in verify_kvs {
            for (key, val) in kvs_i.iter() {
                let current_val = chain.get(key).unwrap();
                assert_eq!(current_val, *val);
            }
        }
    }

    /*
        #[test]
        fn test_durability() {
            let temp_dir = tempfile::tempdir().unwrap();

            let num_keys = 10000;
            let key_size = 50;
            let val_min_size = 50;
            let val_max_size = 100;
            let vals = RandomKVs::new(
                true,
                false,
                1,
                num_keys,
                key_size,
                val_min_size,
                val_max_size,
            )
            .pop()
            .unwrap();

            let mut expected_vals = HashSet::new();
            for (key, val) in vals.iter() {
                expected_vals.insert((key.to_vec(), val.to_vec()));
            }

            // Create a store and insert some values.
            // Drop the store and buffer pool
            {
                let bp = Arc::new(BufferPool::new(&temp_dir, 20, false).unwrap());

                let c_key = ContainerKey::new(0, 0);
                let store = Arc::new(HashReadOptimize::new(c_key, bp.clone(), 10));

                for (key, val) in vals.iter() {
                    store.insert(key, val).unwrap();
                }

                drop(store);
                drop(bp);
            }

            {
                let bp = Arc::new(BufferPool::new(&temp_dir, 10, false).unwrap());

                let c_key = ContainerKey::new(0, 0);
                let store = Arc::new(HashReadOptimize::load(c_key, bp.clone(), 0));

                let mut scanner = store.scan();
                // Remove the keys from the expected_vals set as they are scanned.
                while let Some((key, val)) = scanner.next() {
                    let key = key.to_vec();
                    let val = val.to_vec();
                    assert!(expected_vals.remove(&(key, val)));
                }

                assert!(expected_vals.is_empty());
            }
        }

        #[test]
        fn test_page_stat_generator() {
            let btree = setup_bloom_chain(get_in_mem_pool());
            // Insert 1024 bytes
            let val = vec![3_u8; 1024];
            let order = [6, 3, 8, 1, 5, 7, 2];
            for i in order.iter() {
                println!(
                    "**************************** Inserting key {} **************************",
                    i
                );
                let key = to_bytes(*i);
                btree.insert(&key, &val).unwrap();
            }
            let page_stats = btree.page_stats(true);
            println!("{}", page_stats);

            for i in order.iter() {
                println!(
                    "**************************** Getting key {} **************************",
                    i
                );
                let key = to_bytes(*i);
                let current_val = btree.get(&key).unwrap();
                assert_eq!(current_val, val);
            }

            let page_stats = btree.page_stats(true);
            println!("{}", page_stats);
        }
    */
}
