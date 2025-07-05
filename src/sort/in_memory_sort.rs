use super::sorter::{SortInput, SortStrategy};


// ============================================================================
// In-memory sorting strategy (for smaller datasets)
// ============================================================================
pub struct InMemorySort {
}

impl InMemorySort {
    pub fn new() -> Self {
        Self {
        }
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
    use crate::sort::sorter::{SortInput, Sorter};
    use crate::txn_storage2::field::{to_normalized_key, Field};

    #[test]
    fn test_in_memory_sort() {
        let sorter = Sorter::new_in_mem_sort();

        let mut kv_pairs = Vec::new();
        for i in vec![5, 2, 8, 1, 9, 3, 7, 4, 6, 0] {
            let fields = vec![Field::Int32(Some(i))];
            let sort_key = to_normalized_key(&fields, &vec![(0, true, true)]);
            let value = i.to_be_bytes().to_vec();
            kv_pairs.push((sort_key, value));
        }

        let sorted_iter = sorter.sort(SortInput::new_from_vec(kv_pairs));

        let sorted: Vec<_> = sorted_iter.collect();
        assert_eq!(sorted.len(), 10);
        for i in 0..10 {
            let value = i32::from_be_bytes(sorted[i].1[0..4].try_into().unwrap());
            assert_eq!(value, i as i32);
        }
    }
}
