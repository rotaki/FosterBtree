use std::sync::Arc;

use crate::bp::{BufferPool, ContainerKey, MemPool};
use crate::txn_storage2::{
    bytes_to_fields, fields_to_bytes, from_normalized_key, DataType, Field,
    NonTransactionalStorage, Schema, TxnOptions,
};
use crate::txn_storage2::{
    field::to_normalized_key, field_level_storage_trait::FieldLeveLStorageTrait,
};

use super::external_merge_sort::ExternalMergeSort;
use super::foster_btree_sort::FosterBtreeSort;
use super::in_memory_sort::InMemorySort;

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
    Storage {
        s: Arc<NonTransactionalStorage<BufferPool>>,
        c_key: ContainerKey,
        schema: Schema,
        sort_cols: Vec<(usize, bool, bool)>,
        payload_cols: Vec<usize>,
    },
}

impl SortInput {
    /// Create from a vector of key-value pairs
    pub fn new_from_vec(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        SortInput::Vec(kvs)
    }

    /// Create from storage parameters with a closure that captures everything needed
    pub fn new_from_storage(
        s: Arc<NonTransactionalStorage<BufferPool>>,
        c_key: ContainerKey,
        schema: Schema,
        sort_cols: Vec<(usize, bool, bool)>,
        payload_cols: Vec<usize>,
    ) -> Self {
        SortInput::Storage {
            s,
            c_key,
            schema,
            sort_cols,
            payload_cols,
        }
    }

    /// Create iterators based on the input type
    pub fn create_iterators(
        self,
        num_partitions: usize,
    ) -> Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> {
        match self {
            SortInput::Vec(data) => {
                // Partition the vector into chunks
                if num_partitions <= 1 || data.is_empty() {
                    return vec![Box::new(data.into_iter())];
                }

                let chunk_size = (data.len() + num_partitions - 1) / num_partitions;
                let mut iterators: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> =
                    Vec::new();

                // Split data into chunks
                let chunks: Vec<Vec<_>> = data
                    .into_iter()
                    .collect::<Vec<_>>()
                    .chunks(chunk_size)
                    .map(|chunk| chunk.to_vec())
                    .collect();

                for chunk in chunks {
                    iterators.push(Box::new(chunk.into_iter()));
                }

                iterators
            }
            SortInput::Storage {
                s,
                c_key,
                schema,
                sort_cols,
                payload_cols,
            } => {
                let txn = s
                    .begin_txn(c_key.db_id(), TxnOptions::default())
                    .expect("Failed to begin transaction");
                let columns = sort_cols
                    .iter()
                    .map(|(col_idx, _, _)| *col_idx)
                    .chain(payload_cols.iter().cloned())
                    .collect::<Vec<_>>();
                let iters = s
                    .create_partitioned_scan(&txn, c_key.c_id(), num_partitions, columns)
                    .expect("Failed to create partitioned scan");

                let txn_handle_wrapper = Arc::new(TxnHandleWrapper::new(s.clone(), txn));

                let sort_cols_adjusted = sort_cols
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (_, asc, nulls_first))| (idx, asc, nulls_first))
                    .collect::<Vec<_>>();

                let payload_cols = payload_cols
                    .iter()
                    .map(|&col_idx| schema.cols()[col_idx].clone())
                    .collect::<Vec<_>>();

                let mut iterators: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>> =
                    Vec::with_capacity(iters.len());
                for iter_handle in iters {
                    let storage_iter = PartitionedStorageKvIterator {
                        storage: s.clone(),
                        txn: txn_handle_wrapper.clone(),
                        iterator: iter_handle,
                        sort_cols: sort_cols_adjusted.clone(),
                        payload_cols: payload_cols.clone(),
                    };
                    iterators.push(Box::new(storage_iter));
                }
                iterators
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
    pub fn new_with_strategy(strategy: Box<dyn SortStrategy>) -> Self {
        Self { strategy }
    }

    /// Create with Foster B-tree strategy and config
    pub fn new_tree_sort(num_threads: usize, mem_pool: Arc<impl MemPool + 'static>) -> Self {
        Self::new_with_strategy(Box::new(FosterBtreeSort::new(num_threads, mem_pool)))
    }

    /// Create with in-memory sorting strategy
    pub fn new_in_mem_sort() -> Self {
        Self::new_with_strategy(Box::new(InMemorySort::new()))
    }

    /// Create with external merge sort strategy
    pub fn new_external_merge_sort(
        num_threads: usize,
        total_frames: usize,
        mem_pool: Arc<impl MemPool + 'static>,
    ) -> Self {
        Self::new_with_strategy(Box::new(ExternalMergeSort::new(
            num_threads,
            total_frames,
            mem_pool,
        )))
    }

    pub fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        self.strategy.sort(input)
    }

    /// Sort and return an iterator that deserializes the results into Field vectors
    pub fn sort_with_deserialization(
        &self,
        input: SortInput,
        deserializer: DeserializeRecord,
    ) -> Box<dyn Iterator<Item = (Vec<Field>, Vec<Field>)> + Send> {
        Box::new(self.sort(input).deserialize(deserializer))
    }
}

pub struct TxnHandleWrapper<S: FieldLeveLStorageTrait> {
    pub storage: Arc<S>,
    pub txn: S::TxnHandle,
}

impl<S: FieldLeveLStorageTrait> TxnHandleWrapper<S> {
    pub fn new(storage: Arc<S>, txn: S::TxnHandle) -> Self {
        Self { storage, txn }
    }
}

impl<S: FieldLeveLStorageTrait> Drop for TxnHandleWrapper<S> {
    fn drop(&mut self) {
        // Commit the transaction when the handle is dropped
        self.storage
            .commit_txn(&self.txn, false)
            .expect(&format!("Failed to commit transaction for storage",));
    }
}

// ============================================================================
// Partitioned storage iterator that converts records to key-value pairs
// ============================================================================
struct PartitionedStorageKvIterator<S: FieldLeveLStorageTrait> {
    storage: Arc<S>,
    txn: Arc<TxnHandleWrapper<S>>,
    iterator: S::IteratorHandle,
    sort_cols: Vec<(usize, bool, bool)>,
    payload_cols: Vec<(bool, DataType)>,
}

impl<S: FieldLeveLStorageTrait> Iterator for PartitionedStorageKvIterator<S> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.storage.iter_next(&self.txn.txn, &self.iterator) {
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
unsafe impl<S: FieldLeveLStorageTrait> Send for PartitionedStorageKvIterator<S>
where
    S::IteratorHandle: Send,
    S::TxnHandle: Sync,
{
}

#[derive(Clone)]
pub struct DeserializeRecord {
    sort_cols: Vec<(usize, bool, bool)>,
    sort_field_types: Vec<DataType>,
    payload_cols: Vec<(bool, DataType)>,
}

impl DeserializeRecord {
    pub fn new(
        sort_cols: Vec<(usize, bool, bool)>,
        sort_field_types: Vec<DataType>,
        payload_cols: Vec<(bool, DataType)>,
    ) -> Self {
        Self {
            sort_cols,
            sort_field_types,
            payload_cols,
        }
    }

    pub fn deserialize(&self, key: Vec<u8>, value: Vec<u8>) -> (Vec<Field>, Vec<Field>) {
        let key_fields =
            from_normalized_key(&key, &self.sort_cols, &self.sort_field_types).unwrap();
        let value_fields = bytes_to_fields(&value, &self.payload_cols);
        (key_fields, value_fields)
    }
}

// ============================================================================
// Iterator Extension for Deserialization
// ============================================================================

/// Extension trait to add deserialization capability to iterators over (Vec<u8>, Vec<u8>)
pub trait DeserializeExt: Iterator<Item = (Vec<u8>, Vec<u8>)> + Sized {
    /// Attach a deserializing adapter that converts (Vec<u8>, Vec<u8>) to (Vec<Field>, Vec<Field>)
    fn deserialize(self, deserializer: DeserializeRecord) -> DeserializingIterator<Self> {
        DeserializingIterator::new(self, deserializer)
    }
}

// Implement the trait for all iterators that yield (Vec<u8>, Vec<u8>)
impl<I> DeserializeExt for I where I: Iterator<Item = (Vec<u8>, Vec<u8>)> {}

/// Iterator adapter that deserializes byte pairs into field pairs
pub struct DeserializingIterator<I> {
    inner: I,
    deserializer: DeserializeRecord,
}

impl<I> DeserializingIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
{
    pub fn new(inner: I, deserializer: DeserializeRecord) -> Self {
        Self {
            inner,
            deserializer,
        }
    }
}

impl<I> Iterator for DeserializingIterator<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
{
    type Item = (Vec<Field>, Vec<Field>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(key, value)| self.deserializer.deserialize(key, value))
    }
}

// Make it Send if the inner iterator is Send
unsafe impl<I> Send for DeserializingIterator<I> where I: Iterator<Item = (Vec<u8>, Vec<u8>)> + Send {}

// ============================================================================
// Tests
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::{get_test_bp_lru, BufferPool, ContainerId, DatabaseId};
    use crate::tpcc2::txn_utils::{get_i32_field, get_string_field};
    use crate::txn_storage2::{
        field::{to_normalized_key, Field, Record},
        fields_to_bytes, ContainerDS, ContainerOptions, DataType, NonTransactionalStorage, Schema,
        TxnOptions,
    };

    #[test]
    fn test_sort_from_vec() {
        // Setup
        let sorter = Sorter::new_in_mem_sort();

        // Create test data
        let mut original_vec: Vec<i32> = vec![5, 0, 8, 0, 4, 3, 5, 8, 2, 1, 9, 6, 7];
        let mut kv_pairs = Vec::with_capacity(original_vec.len());

        for i in &original_vec {
            let key = i.to_be_bytes().to_vec();
            let value = i.to_be_bytes().to_vec();
            kv_pairs.push((key, value));
        }

        // Sort the data
        let sorted_iter = sorter.sort(SortInput::new_from_vec(kv_pairs));
        let sorted: Vec<_> = sorted_iter.collect();

        assert_eq!(sorted.len(), original_vec.len());
        original_vec.sort();
        for (i, expected) in original_vec.iter().enumerate() {
            let key = sorted[i].0.clone();
            let value = sorted[i].1.clone();
            assert_eq!(key, expected.to_be_bytes().to_vec());
            assert_eq!(value, expected.to_be_bytes().to_vec());
        }

        println!("Successfully sorted from vec");
    }

    #[test]
    fn test_deserialize_iterator() {
        // Setup
        let sorter = Sorter::new_in_mem_sort();

        // Create test data with normalized keys
        let mut kv_pairs = Vec::new();
        let mut original_vec = vec![5, 2, 8, 1, 9, 9, 1, 3, 2, 5, 8, 1, 3];
        for i in &original_vec {
            let fields = vec![Field::Int32(Some(*i))];
            let key = to_normalized_key(&fields, &vec![(0, true, true)]);
            let value_fields = vec![Field::Int32(Some(*i * 100)), Field::Int32(Some(*i * 1000))];
            let value = fields_to_bytes(
                &value_fields,
                &vec![(false, DataType::Int32), (false, DataType::Int32)],
            );
            kv_pairs.push((key, value));
        }

        // Create deserializer
        let deserializer = DeserializeRecord::new(
            vec![(0, true, true)],                                    // sort columns config
            vec![DataType::Int32],                                    // sort field types
            vec![(false, DataType::Int32), (false, DataType::Int32)], // payload columns
        );

        // Method 1: Using the extension trait directly
        let sorted_iter = sorter.sort(SortInput::new_from_vec(kv_pairs.clone()));
        let deserialized: Vec<_> = sorted_iter.deserialize(deserializer.clone()).collect();

        // Verify deserialized results
        assert_eq!(deserialized.len(), original_vec.len());
        original_vec.sort();

        for (i, (key_fields, value_fields)) in deserialized.iter().enumerate() {
            let key_val = get_i32_field(key_fields, 0);
            assert_eq!(key_val, original_vec[i]);
            let val1 = get_i32_field(value_fields, 0);
            let val2 = get_i32_field(value_fields, 1);
            assert_eq!(val1, original_vec[i] * 100);
            assert_eq!(val2, original_vec[i] * 1000);
        }

        // Method 2: Using the convenience method
        let deserialized2: Vec<_> = sorter
            .sort_with_deserialization(SortInput::new_from_vec(kv_pairs), deserializer)
            .collect();

        for (i, (key_fields, value_fields)) in deserialized2.iter().enumerate() {
            let key_val = get_i32_field(key_fields, 0);
            assert_eq!(key_val, original_vec[i]);
            let val1 = get_i32_field(value_fields, 0);
            let val2 = get_i32_field(value_fields, 1);
            assert_eq!(val1, original_vec[i] * 100);
            assert_eq!(val2, original_vec[i] * 1000);
        }
        println!("Successfully tested deserializing iterator");
    }

    #[test]
    fn test_sort_from_storage() {
        use crate::bp::get_test_bp_lru;

        // Setup
        let bp = get_test_bp_lru(10);
        let storage = Arc::new(NonTransactionalStorage::new(bp.clone()));

        // Create schema with multiple columns including composite sort key
        let schema = Schema::with_primary_key(
            vec![
                (false, DataType::Int32), // id - primary key
                (false, DataType::Int32), // dept_id - first sort column
                (false, DataType::Int32), // salary - second sort column
                (true, DataType::String), // name - payload
            ],
            vec![0], // primary key is column 0
        );

        let db_id = 0;
        let container_options =
            ContainerOptions::new("test_multi_sort", ContainerDS::BTree, schema.clone());
        let c_id = storage.create_container(db_id, container_options).unwrap();

        let mut inserting_records = (0..20)
            .map(|i| {
                Record {
                    fields: vec![
                        Field::Int32(Some(i)),                          // id
                        Field::Int32(Some(i % 3)),                      // dept_id (0, 1, 2)
                        Field::Int32(Some(100 - (i * 5) % 30)), // salary (varies within dept)
                        Field::String(Some(format!("Employee_{}", i))), // name
                    ],
                }
            })
            .collect::<Vec<_>>();

        // Insert test data
        let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
        for record in &inserting_records {
            storage
                .insert_record(&txn, c_id, record.clone(), None)
                .unwrap();
        }
        storage.commit_txn(&txn, false).unwrap();

        // Create sorter with multi-column sort
        let sorter = Sorter::new_in_mem_sort();

        // Sort by dept_id ascending, then salary descending
        let sort_cols = vec![
            (1, true, true),  // dept_id ascending
            (2, false, true), // salary descending
        ];
        let payload_cols = vec![0, 3]; // Return id and name

        inserting_records.sort_by_key(|r| {
            let dept_id = get_i32_field(&r.fields, 1);
            let salary = get_i32_field(&r.fields, 2);
            (dept_id, -salary) // Sort by dept_id ascending, salary descending
        });

        let sort_input = SortInput::new_from_storage(
            storage.clone(),
            ContainerKey::new(db_id, c_id),
            schema.clone(),
            sort_cols.clone(),
            payload_cols.clone(),
        );

        // Create deserializer for multi-column sort key
        let deserializer = DeserializeRecord::new(
            vec![(0, true, true), (1, false, true)], // Adjusted sort columns
            vec![DataType::Int32, DataType::Int32],  // dept_id, salary
            vec![
                (false, DataType::Int32), // id
                (true, DataType::String), // name
            ],
        );

        // Sort and deserialize
        let sorted_deserialized: Vec<_> = sorter
            .sort_with_deserialization(sort_input, deserializer)
            .collect();

        // Verify results
        assert_eq!(sorted_deserialized.len(), 20);

        for (i, (key_fields, value_fields)) in sorted_deserialized.iter().enumerate() {
            // println!("Key: {:?}, Value: {:?}", key_fields, value_fields);
            let dept_id = get_i32_field(key_fields, 0);
            let salary = get_i32_field(key_fields, 1);
            let id = get_i32_field(value_fields, 0);
            let name = get_string_field(value_fields, 1);

            // Check if the sort order matches the inserting records
            let expected_record = &inserting_records[i];
            let expected_dept_id = get_i32_field(&expected_record.fields, 1);
            let expected_salary = get_i32_field(&expected_record.fields, 2);
            let expected_id = get_i32_field(&expected_record.fields, 0);
            let expected_name = get_string_field(&expected_record.fields, 3);

            assert_eq!(dept_id, expected_dept_id, "Dept ID mismatch at index {}", i);
            assert_eq!(salary, expected_salary, "Salary mismatch at index {}", i);
            assert_eq!(id, expected_id, "ID mismatch at index {}", i);
            assert_eq!(name, expected_name, "Name mismatch at index {}", i);
        }

        println!("Successfully tested multi-column sort from storage with deserialization");
    }
}
