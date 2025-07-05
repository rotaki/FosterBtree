use std::sync::Arc;

use crate::bp::MemPool;
use crate::txn_storage2::{fields_to_bytes, DataType};
use crate::txn_storage2::{
    field::to_normalized_key, field_level_storage_trait::FieldLeveLStorageTrait,
};

use super::foster_btree_sort::FosterBtreeSort;
use super::in_memory_sort::InMemorySort;
// use super::external_merge_sort::ExternalMergeSort;

// ============================================================================
// Trait for different sorting strategies
// ============================================================================
pub trait SortStrategy: Send + Sync {
    /// Sort key-value pairs and return an iterator over sorted results
    fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>;

    /// Get a descriptive name for this sorting strategy
    fn name(&self) -> &str;
}

// ============================================================================
// Input abstraction for sorting (key-value pairs)
// ============================================================================
pub enum SortInput {
    /// Directly from a vector of key-value pairs
    Vec(Vec<(Vec<u8>, Vec<u8>)>),
    
    /// From a function that generates partitioned iterators
    Storage(Box<dyn FnOnce(usize) -> Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> + Send>),
}

impl SortInput {
    /// Create from a vector of key-value pairs
    pub fn new_from_vec(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        SortInput::Vec(kvs)
    }

    /// Create from storage parameters with a closure that captures everything needed
    pub fn new_from_storage<F>(create_fn: F) -> Self 
    where
        F: FnOnce(usize) -> Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> + Send + 'static,
    {
        SortInput::Storage(Box::new(create_fn))
    }

    /// Create iterators based on the input type
    pub fn create_iterators(self, num_partitions: usize) -> Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> {
        match self {
            SortInput::Vec(data) => {
                // Partition the vector into chunks
                if num_partitions <= 1 || data.is_empty() {
                    return vec![Box::new(data.into_iter())];
                }
                
                let chunk_size = (data.len() + num_partitions - 1) / num_partitions;
                let mut iterators: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> = Vec::new();
                
                // Split data into chunks
                let chunks: Vec<Vec<_>> = data.into_iter()
                    .collect::<Vec<_>>()
                    .chunks(chunk_size)
                    .map(|chunk| chunk.to_vec())
                    .collect();
                
                for chunk in chunks {
                    iterators.push(Box::new(chunk.into_iter()));
                }
                
                iterators
            }
            SortInput::Storage(create_fn) => {
                // Use the provided function to create iterators
                create_fn(num_partitions)
            }
        }
    }
}

// ============================================================================
// Main external sorter with strategy pattern
// ============================================================================
pub struct Sorter {
    strategy: Box<dyn SortStrategy>,
}

impl Sorter {
    /// Create with a specific sorting strategy
    pub fn new_with_strategy(
        strategy: Box<dyn SortStrategy>,
    ) -> Self {
        Self {
            strategy,
        }
    }

    /// Create with Foster B-tree strategy and config
    pub fn new_tree_sort(num_threads: usize, mem_pool: Arc<impl MemPool + 'static>) -> Self {
        Self::new_with_strategy(Box::new(FosterBtreeSort::new(num_threads, mem_pool)))
    }

    /// Create with in-memory sorting strategy
    pub fn new_in_mem_sort() -> Self {
        Self::new_with_strategy(Box::new(InMemorySort::new()))
    }

    pub fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        self.strategy.sort(input)
    }
}


// ============================================================================
// Partitioned storage iterator that converts records to key-value pairs
// ============================================================================
struct PartitionedStorageKvIterator<'a, S: FieldLeveLStorageTrait> {
    storage: &'a S,
    txn: &'a S::TxnHandle,
    iterator: S::IteratorHandle,
    sort_cols: Vec<(usize, bool, bool)>,
    payload_cols: Vec<(bool, DataType)>,
}

impl<'a, S: FieldLeveLStorageTrait> Iterator for PartitionedStorageKvIterator<'a, S> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.storage.iter_next(self.txn, &self.iterator) {
            Ok(Some((fields, _))) => {
                // The returned fields are in the order: sort columns, then primary key columns
                // Create the sort key from the first num_sort_cols fields
                let sort_key = to_normalized_key(&fields, &self.sort_cols);
                let value = fields_to_bytes(&fields[self.sort_cols.len()..], &self.payload_cols);

                Some((sort_key, value))
            }
            Ok(None) => None,
            Err(_) => None,
        }
    }
}

// Ensure the iterator is Send
unsafe impl<'a, S: FieldLeveLStorageTrait> Send for PartitionedStorageKvIterator<'a, S>
where
    S::IteratorHandle: Send,
    S::TxnHandle: Sync,
{
}

// ============================================================================
// Tests
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::{get_test_bp_lru, BufferPool, ContainerId, DatabaseId};
    use crate::txn_storage2::{
        field::{Field, Record, to_normalized_key}, 
        ContainerDS, ContainerOptions, DataType, NonTransactionalStorage, Schema, TxnOptions
    };
    
    // Test data structure to hold storage setup
    struct TestStorage {
        storage: NonTransactionalStorage<BufferPool>,
        db_id: DatabaseId,
        c_id: ContainerId,
    }
    
    // Helper to create storage with multi-column schema
    fn setup_multi_column_storage(bp: Arc<BufferPool>) -> TestStorage {
        let storage = NonTransactionalStorage::new(bp.clone());
        
        // Columns: id (primary key), value1 (sort column), value2, value3
        let schema = Schema::with_primary_key(
            vec![
                (false, DataType::Int32), // id - primary key
                (false, DataType::Int32), // value1 - sort column
                (false, DataType::Int32), // value2
                (false, DataType::Int32), // value3
            ],
            vec![0], // primary key is column 0
        );
        
        let db_id = 0;
        let container_options = ContainerOptions::new("test_container", ContainerDS::BTree, schema.clone());
        let c_id = storage.create_container(db_id, container_options).unwrap();
        
        TestStorage { storage, db_id, c_id }
    }
    
    // Helper to insert multi-column test data
    fn insert_multi_column_data(test_storage: &TestStorage, count: i32) {
        let txn = test_storage.storage.begin_txn(test_storage.db_id, TxnOptions::default()).unwrap();
        
        for i in 0..count {
            let record = Record {
                fields: vec![
                    Field::Int32(Some(i)),            // id
                    Field::Int32(Some((i * 7) % 10)), // value1 (to be sorted)
                    Field::Int32(Some(i * 100)),      // value2
                    Field::Int32(Some(i * 1000)),     // value3
                ],
            };
            test_storage.storage.insert_record(&txn, test_storage.c_id, record, None).unwrap();
        }
        
        test_storage.storage.commit_txn(&txn, false).unwrap();
    }
    
    #[test]
    fn test_read_from_storage() {
        // Setup
        let bp = get_test_bp_lru(1000);
        let test_storage = setup_multi_column_storage(bp.clone());
        insert_multi_column_data(&test_storage, 100);
        
        // Create a simple test using in-memory sort
        let sorter = Sorter::new_in_mem_sort();
        
        // Read data from storage and create key-value pairs
        let txn = test_storage.storage.begin_txn(test_storage.db_id, TxnOptions::default()).unwrap();
        let sort_cols = vec![(1, true, true)]; // Sort by column 1 (value) ascending
        let payload_cols = vec![(false, DataType::Int32)]; // Just the primary key
        
        // Create a single iterator manually
        let iter_handles = test_storage.storage.create_partitioned_scan(&txn, test_storage.c_id, 1, vec![0, 1]).unwrap();
        let iter_handle = iter_handles.into_iter().next().unwrap();
        let storage_iter = PartitionedStorageKvIterator {
            storage: &test_storage.storage,
            txn: &txn,
            iterator: iter_handle,
            sort_cols,
            payload_cols,
        };
        
        // Collect all pairs
        let all_pairs: Vec<_> = storage_iter.collect();
        test_storage.storage.commit_txn(&txn, false).unwrap();
        
        // Verify we read data
        assert!(all_pairs.len() > 0);
        println!("Successfully read {} key-value pairs from storage", all_pairs.len());
    }

    #[test]
    fn test_sort_with_fields_from_storage() {
        // Setup
        let bp = get_test_bp_lru(1000);
        let test_storage = setup_multi_column_storage(bp.clone());
        insert_multi_column_data(&test_storage, 10);
        
        // Create sorter
        let sorter = Sorter::new_tree_sort(2, bp.clone());
        
        // Read data from storage and create key-value pairs
        let txn = test_storage.storage.begin_txn(test_storage.db_id, TxnOptions::default()).unwrap();
        let sort_cols = vec![(1, true, true)]; // Sort by column 1
        let payload_cols = vec![(false, DataType::Int32), (false, DataType::Int32), (false, DataType::Int32)]; // id, value2, value3
        
        // Create an iterator
        let iter_handles = test_storage.storage.create_partitioned_scan(&txn, test_storage.c_id, 1, vec![0, 1, 2, 3]).unwrap();
        let iter_handle = iter_handles.into_iter().next().unwrap();
        let storage_iter = PartitionedStorageKvIterator {
            storage: &test_storage.storage,
            txn: &txn,
            iterator: iter_handle,
            sort_cols,
            payload_cols,
        };
        
        // Collect data into a vector
        let kvs: Vec<_> = storage_iter.collect();
        test_storage.storage.commit_txn(&txn, false).unwrap();
        
        // Sort the data
        let sorted_iter = sorter.sort(SortInput::new_from_vec(kvs));
        let sorted: Vec<_> = sorted_iter.collect();
        
        // Verify we got sorted data
        assert_eq!(sorted.len(), 10);
        
        // Verify sort order by checking keys are in ascending order
        let mut prev_key: Option<Vec<u8>> = None;
        for (key, _value) in &sorted {
            if let Some(prev) = prev_key {
                assert!(prev <= *key, "Keys should be in sorted order");
            }
            prev_key = Some(key.clone());
        }
        
        println!("Successfully sorted {} records from storage", sorted.len());
    }
    
    #[test]
    fn test_sort_from_vec() {
        // Setup
        let bp = get_test_bp_lru(100);
        
        // Create test data
        let mut kv_pairs = Vec::new();
        for i in vec![5, 2, 8, 1, 9, 3, 7, 4, 6, 0] {
            let fields = vec![Field::Int32(Some(i))];
            let sort_key = to_normalized_key(&fields, &vec![(0, true, true)]);
            let value = i.to_be_bytes().to_vec();
            kv_pairs.push((sort_key, value));
        }
        
        // Test with Foster B-tree sort
        let sorter_btree = Sorter::new_tree_sort(2, bp.clone());
        let sorted_iter = sorter_btree.sort(SortInput::new_from_vec(kv_pairs.clone()));
        let sorted: Vec<_> = sorted_iter.collect();
        
        assert_eq!(sorted.len(), 10);
        for i in 0..10 {
            let value = i32::from_be_bytes(sorted[i].1[0..4].try_into().unwrap());
            assert_eq!(value, i as i32);
        }
        
        // Test with in-memory sort
        let sorter_mem = Sorter::new_in_mem_sort();
        let sorted_iter = sorter_mem.sort(SortInput::new_from_vec(kv_pairs));
        let sorted: Vec<_> = sorted_iter.collect();
        
        assert_eq!(sorted.len(), 10);
        for i in 0..10 {
            let value = i32::from_be_bytes(sorted[i].1[0..4].try_into().unwrap());
            assert_eq!(value, i as i32);
        }
        
        println!("Successfully sorted from vec with both strategies");
    }
    

    // #[test]
    // fn test_external_merge_sort() {
    //     let mem_pool = get_test_bp_lru(100);
    //     let config = ExternalSortConfig {
    //         total_frames: 50,
    //         num_threads: 2,
    //     };

    //     let sorter = ExternalSorter::new_external_merge(mem_pool, config);

    //     let mut kv_pairs = Vec::new();
    //     for i in (0..100).rev() {
    //         let record = Record {
    //             fields: vec![Field::Int32(Some(i))],
    //         };
    //         let sort_key = to_normalized_key(&record.fields, &vec![(0, true, true)]);
    //         let value = i.to_be_bytes().to_vec();
    //         kv_pairs.push((sort_key, value));
    //     }

    //     let sorted_iter = sorter.sort_from_iterators(
    //         vec![kv_pairs.into_iter()],
    //     );

    //     let sorted: Vec<_> = sorted_iter.collect();
    //     assert_eq!(sorted.len(), 100);
    //     for i in 0..100 {
    //         let value = i32::from_be_bytes(sorted[i].1[0..4].try_into().unwrap());
    //         assert_eq!(value, i as i32);
    //     }
    // }
}