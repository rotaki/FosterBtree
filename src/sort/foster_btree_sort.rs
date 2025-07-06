use std::sync::Arc;
use std::thread;

use super::sorter::{SortInput, SortStrategy};
use crate::access_method::fbt::{FosterBtree, FosterBtreeRangeScanner};
use crate::access_method::UniqueKeyIndex;
use crate::bp::{ContainerKey, MemPool};

const TEMP_DB_ID: u16 = 1;

// Least significat bits are
pub struct GlobalUniqueCounter {
    counter: u32,
    thread_id: u32,
}

impl GlobalUniqueCounter {
    pub fn new(thread_id: u32) -> Self {
        Self {
            counter: 0,
            thread_id,
        }
    }

    // Uniqueness is guaranteed by combining thread ID and a counter
    pub fn next(&mut self) -> u64 {
        // Least significant bits are thread ID
        // Most significant bits are a counter that increments per thread
        let unique_id = (self.thread_id as u64) << 32 | (self.counter as u64);
        self.counter += 1;
        unique_id
    }
}

// ============================================================================
// Configuration for Foster B-tree sort
// ============================================================================
#[derive(Clone, Debug)]
pub struct FosterBtreeSortConfig {
    pub num_threads: usize,
}

impl Default for FosterBtreeSortConfig {
    fn default() -> Self {
        Self { num_threads: 4 }
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
            config: FosterBtreeSortConfig { num_threads },
            mem_pool,
        }
    }
}

impl<M: MemPool + 'static> SortStrategy for FosterBtreeSort<M> {
    fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        // Get iterators from input
        let iterators = input.create_iterators(self.config.num_threads);

        // Create a single Foster B-tree that will be shared by all threads
        let container_key = ContainerKey::new(TEMP_DB_ID, 0);
        let btree = Arc::new(FosterBtree::new(container_key, self.mem_pool.clone()));

        // Process each iterator in parallel using scoped threads
        thread::scope(|s| {
            let mut handles = Vec::new();

            for (thread_id, iterator) in iterators.into_iter().enumerate() {
                let btree_clone = btree.clone();

                let handle = s.spawn(move || {
                    let mut counter = GlobalUniqueCounter::new(thread_id as u32);

                    for (key, value) in iterator {
                        // Get a unique ID for this record
                        let unique_id = counter.next();

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
    use crate::bp::{get_test_bp_lru, ContainerKey};
    use crate::sort::sorter::{DeserializeRecord, SortInput, Sorter};
    use crate::txn_storage2::DataType;
    use chrono::NaiveDate;
    use std::sync::Arc;

    #[test]
    fn test_foster_btree_sort_unique_keys() {
        let mem_pool = get_test_bp_lru(200);
        let num_threads = 4;

        let sorter = Sorter::new_tree_sort(num_threads, mem_pool);

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
            println!("Key: {:?}, Value: {:?}", key, value);
            assert_eq!(key, &original_kv_pairs[i].0);
            assert_eq!(value, &original_kv_pairs[i].1);
        }
    }

    #[test]
    fn test_foster_btree_sort_with_duplicates() {
        let mem_pool = get_test_bp_lru(200);
        let num_threads = 4;
        let sorter = Sorter::new_tree_sort(num_threads, mem_pool);
        // Create key-value pairs with duplicate keys
        let mut original_kv_pairs = Vec::new();
        for i in 0..1000 {
            let key = (i % 100) as i32;
            let value = key * 2;
            let key = key.to_be_bytes().to_vec(); // Duplicate keys every 100
            let value = value.to_be_bytes().to_vec(); // Value is just the index
            original_kv_pairs.push((key, value));
        }

        let result = sorter
            .sort(SortInput::new_from_vec(original_kv_pairs.clone()))
            .collect::<Vec<_>>();
        original_kv_pairs.sort_by(|a, b| a.0.cmp(&b.0)); // Sort by key to ensure duplicates are handled

        // Check that the sorted iterator matches the expected sorted order
        assert_eq!(result.len(), original_kv_pairs.len());
        for (i, (key, value)) in result.iter().enumerate() {
            println!("Key: {:?}, Value: {:?}", key, value);
            assert_eq!(key, &original_kv_pairs[i].0);
            assert_eq!(value, &original_kv_pairs[i].1);
        }
    }

    #[test]
    fn test_foster_btree_sort_empty() {
        let mem_pool = get_test_bp_lru(1);
        let num_threads = 1;
        let sorter = Sorter::new_tree_sort(num_threads, mem_pool);

        let result = sorter
            .sort(SortInput::new_from_vec(vec![]))
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_foster_btree_sort_single_element() {
        let mem_pool = get_test_bp_lru(3);
        let num_threads = 1;
        let sorter = Sorter::new_tree_sort(num_threads, mem_pool);

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
    fn test_foster_btree_sort_tpch_lineitem() {
        use crate::sort::tpch_loader::{lineitem_schema, TpchLoader};
        use crate::tpcc2::txn_utils::{
            get_date_field, get_f64_field, get_i32_field, get_string_field,
        };

        // Setup with a small scale factor
        let mem_pool = get_test_bp_lru(1000);
        let loader = TpchLoader::new(mem_pool.clone());

        // Load a very small scale factor for testing
        let scale_factor = 0.001; // This should generate ~6000 lineitem records
        let num_threads = 2;
        loader.load_lineitem(scale_factor, num_threads);

        // Use Foster B-tree sort with multiple threads
        let sorter = Sorter::new_tree_sort(4, mem_pool.clone());

        // Test 1: Sort by l_shipdate (column 10)
        println!("\nTest 1: Foster B-tree sorting by l_shipdate");
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
        println!("Foster B-tree sorted {} records by shipdate", sorted.len());

        // Test 2: Sort by l_extendedprice descending
        println!("\nTest 2: Foster B-tree sorting by l_extendedprice descending");
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
        println!("Top 10 highest extended prices (Foster B-tree):");
        for i in 0..sorted.len() {
            let price = get_f64_field(&sorted[i].0, 0);
            let orderkey = get_i32_field(&sorted[i].1, 0);
            let linenumber = get_i32_field(&sorted[i].1, 1);
            let quantity = get_f64_field(&sorted[i].1, 2);
            if i < 10 {
                println!(
                    "  Price: {:.2}, OrderKey: {}, LineNumber: {}, Quantity: {:.2}",
                    price, orderkey, linenumber, quantity
                );
            }

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
        println!("\nTest 3: Foster B-tree multi-column sort (returnflag, linestatus, shipdate)");
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

            if i < 10 {
                println!("  Record {}: ReturnFlag: {}, LineStatus: {}, ShipDate: {}, OrderKey: {}, LineNumber: {}, ExtendedPrice: {:.2}",
                    i, curr_rf, curr_ls, curr_shipdate,
                    get_i32_field(&sorted[i].1, 0),
                    get_i32_field(&sorted[i].1, 1),
                    get_f64_field(&sorted[i].1, 2)
                );
            }

            // Check sort order
            match prev_rf.cmp(&curr_rf) {
                std::cmp::Ordering::Less => {} // Correct order
                std::cmp::Ordering::Equal => {
                    // If returnflag is equal, check linestatus
                    match prev_ls.cmp(&curr_ls) {
                        std::cmp::Ordering::Less => {} // Correct
                        std::cmp::Ordering::Equal => {
                            // If both are equal, check shipdate
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
    }
}
