use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::thread;

use super::sorter::{SortInput, SortStrategy};
use crate::access_method::fbt::{FosterBtree, FosterBtreeRangeScanner};
use crate::access_method::UniqueKeyIndex;
use crate::bp::{ContainerKey, MemPool};

// ============================================================================
// Configuration for Foster B-tree sort
// ============================================================================
#[derive(Clone, Debug)]
pub struct FosterBtreeSortConfig {
    pub num_threads: usize,
}

impl Default for FosterBtreeSortConfig {
    fn default() -> Self {
        Self {
            num_threads: 4,
        }
    }
}

// ============================================================================
// Foster B-tree based sorting strategy
// ============================================================================
pub struct FosterBtreeSort<M: MemPool> {
    config: FosterBtreeSortConfig,
    mem_pool: Arc<M>,
}

impl<M: MemPool> FosterBtreeSort<M> {
    pub fn new(num_threads: usize, mem_pool: Arc<M>) -> Self {
        Self {
            config: FosterBtreeSortConfig {
                num_threads,
            },
            mem_pool,
        }
    }
}

impl<M: MemPool + 'static> SortStrategy for FosterBtreeSort<M> {
    fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        // Get iterators from input
        let iterators = input.create_iterators(self.config.num_threads);
        
        // Create a single Foster B-tree that will be shared by all threads
        let container_key = ContainerKey::new(0, 0);
        let btree = Arc::new(FosterBtree::new(container_key, self.mem_pool.clone()));

        // Counter for generating unique IDs (shared across threads)
        let counter = Arc::new(Mutex::new(0u64));

        // Process each iterator in parallel using scoped threads
        thread::scope(|s| {
            let mut handles = Vec::new();

            for iterator in iterators.into_iter() {
                let btree_clone = btree.clone();
                let counter_clone = counter.clone();

                let handle = s.spawn(move || {
                    for (key, value) in iterator {
                        // Get a unique ID for this record
                        let unique_id = {
                            let mut counter_guard = counter_clone.lock().unwrap();
                            let id = *counter_guard;
                            *counter_guard += 1;
                            id
                        };

                        // Append the unique ID to make the key unique even for duplicate values
                        let mut unique_key = key;
                        unique_key.extend_from_slice(&unique_id.to_be_bytes());

                        // Insert into the B-tree
                        btree_clone.insert(&unique_key, &value).unwrap();
                    }
                });

                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }
        });

        Box::new(FosterBtreeKvIterator::new(btree))
    }

    fn name(&self) -> &str {
        "Foster B-tree Sort"
    }
}

// Iterator that reads from a Foster B-tree in sorted order
pub struct FosterBtreeKvIterator<M: MemPool> {
    scanner: FosterBtreeRangeScanner<M>,
}

impl<M: MemPool> FosterBtreeKvIterator<M> {
    pub fn new(btree: Arc<FosterBtree<M>>) -> Self {
        let scanner = btree.scan();
        Self { scanner }
    }
}

impl<M: MemPool> Iterator for FosterBtreeKvIterator<M> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.scanner.next().map(|(mut key, value)| {
            key.truncate(key.len() - 8); // Remove the unique ID part from the key
            (key, value)
        })
    }
}

unsafe impl<M: MemPool> Send for FosterBtreeKvIterator<M> {}

#[cfg(test)]
mod tests {
    use crate::bp::get_test_bp_lru;
    use crate::sort::sorter::{SortInput, Sorter};
    use crate::txn_storage2::field::{to_normalized_key, Field};

    #[test]
    fn test_foster_btree_sort() {
        let mem_pool = get_test_bp_lru(200);
        let num_threads = 4;

        let sorter = Sorter::new_tree_sort(num_threads, mem_pool);

        // Create key-value pairs where key is sort key and value is primary key
        let mut kv_pairs = Vec::new();
        for i in (0..1000).rev() {
            // Create a normalized sort key
            let fields = vec![Field::Int32(Some(i))];
            let sort_key = to_normalized_key(&fields, &vec![(0, true, true)]);

            // Value could be primary key or full record
            let value = i.to_be_bytes().to_vec();

            kv_pairs.push((sort_key, value));
        }

        let sorted_iter = sorter.sort(SortInput::new_from_vec(kv_pairs));

        let sorted: Vec<_> = sorted_iter.collect();
        assert_eq!(sorted.len(), 1000);


    }
}
