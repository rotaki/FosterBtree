use super::sorter::{SortInput, SortStrategy};

// ============================================================================
// In-memory sorting strategy (for smaller datasets)
// ============================================================================
pub struct InMemorySort {}

impl InMemorySort {
    pub fn new() -> Self {
        Self {}
    }
}

impl SortStrategy for InMemorySort {
    fn sort(&self, input: SortInput) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> {
        // Get a single iterator (in-memory sort doesn't benefit from partitioning)
        let iterators = input.create_iterators(1);

        // Collect all key-value pairs into memory
        let mut all_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for iterator in iterators {
            all_pairs.extend(iterator);
        }

        // Sort by keys
        all_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        Box::new(all_pairs.into_iter())
    }

    fn name(&self) -> &str {
        "In-Memory Sort"
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::bp::{get_test_bp_lru, ContainerKey};
    use crate::sort::sorter::{DeserializeRecord, SortInput, Sorter};
    use crate::tpcc2::txn_utils::get_date_field;
    use crate::txn_storage2::{field::to_normalized_key, fields_to_bytes, DataType};
    use std::sync::Arc;

    #[test]
    fn test_in_memory_sort_unique_keys() {
        let sorter = Sorter::new_in_mem_sort();

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
    fn test_in_memory_sort_with_duplicates() {
        let sorter = Sorter::new_in_mem_sort();

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
    fn test_in_memory_sort_empty() {
        let sorter = Sorter::new_in_mem_sort();
        let result = sorter
            .sort(SortInput::new_from_vec(vec![]))
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_in_memory_sort_single_element() {
        let sorter = Sorter::new_in_mem_sort();

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
    fn test_in_memory_sort_tpch_lineitem() {
        use crate::sort::tpch_loader::{lineitem_schema, TpchLoader};
        use crate::tpcc2::txn_utils::{get_f64_field, get_i32_field, get_string_field};

        // Setup with a small scale factor
        let mem_pool = get_test_bp_lru(1000);
        let loader = TpchLoader::new(mem_pool.clone());

        // Load a very small scale factor for testing
        let scale_factor = 0.001; // This should generate ~6000 lineitem records
        let num_threads = 2;
        loader.load_lineitem(scale_factor, num_threads);

        let sorter = Sorter::new_in_mem_sort();

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

            // Print current record for debugging
            println!("{:?}, {:?}", sorted[i].0, sorted[i].1);

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
