mod read_optimized_chain;
mod read_optimized_page;

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    page::{PageId, AVAILABLE_PAGE_SIZE},
    random::gen_random_int,
};

use super::{
    chain::read_optimized_chain::{ReadOptimizedChain, ReadOptimizedChainRangeScanner},
    AccessMethodError, FilterType, UniqueKeyIndex,
};

pub mod prelude {
    pub use super::read_optimized_page::ReadOptimizedPage;
    pub use super::HashReadOptimize;
    pub use super::HashReadOptimizedChainIter;
}

pub struct HashReadOptimize<T: MemPool> {
    pub mem_pool: Arc<T>,
    _c_key: ContainerKey,
    num_buckets: usize,
    _meta_page_id: PageId, // Stores the number of buckets and all the page ids of the first of the chain
    buckets: Vec<Arc<ReadOptimizedChain<T>>>,
}

impl<T: MemPool> HashReadOptimize<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        if num_buckets == 0 {
            panic!("Number of buckets cannot be 0");
        }
        // If number of buckets does not fit in the meta_page, panic
        if num_buckets * std::mem::size_of::<PageId>() + std::mem::size_of::<usize>()
            > AVAILABLE_PAGE_SIZE
        {
            panic!("Number of buckets too large to fit in the meta_page page");
        }

        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();

        let mut offset = 0;
        let num_buckets_bytes = num_buckets.to_be_bytes();
        meta_page[offset..offset + num_buckets_bytes.len()].copy_from_slice(&num_buckets_bytes);
        offset += num_buckets_bytes.len();

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            // Create a new chain
            let chain = Arc::new(ReadOptimizedChain::new(c_key, mem_pool.clone()));

            let root_p_id = chain.first_key().p_key().page_id.to_be_bytes();
            meta_page[offset..offset + root_p_id.len()].copy_from_slice(&root_p_id);
            offset += root_p_id.len();

            buckets.push(chain);
        }

        let meta_page_id = meta_page.get_id();

        Self {
            mem_pool: mem_pool.clone(),
            _c_key: c_key,
            num_buckets,
            _meta_page_id: meta_page_id,
            buckets,
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, meta_page_id: PageId) -> Self {
        let meta_page = mem_pool
            .get_page_for_read(PageFrameKey::new(c_key, meta_page_id))
            .unwrap();

        let mut offset = 0;
        let num_buckets_bytes = &meta_page[offset..offset + std::mem::size_of::<usize>()];
        offset += num_buckets_bytes.len();
        let num_buckets = usize::from_be_bytes(num_buckets_bytes.try_into().unwrap());

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            let root_page_id_bytes = &meta_page[offset..offset + std::mem::size_of::<PageId>()];
            offset += root_page_id_bytes.len();
            let root_page_id = PageId::from_be_bytes(root_page_id_bytes.try_into().unwrap());
            let chain = Arc::new(ReadOptimizedChain::load(
                c_key,
                mem_pool.clone(),
                root_page_id,
            ));
            buckets.push(chain);
        }

        Self {
            mem_pool: mem_pool.clone(),
            _c_key: c_key,
            num_buckets,
            _meta_page_id: meta_page_id,
            buckets,
        }
    }

    pub fn num_kvs(&self) -> usize {
        // self.buckets.iter().map(|b| b.num_kvs()).sum()
        unimplemented!()
    }

    pub fn page_stats(&self, verbose: bool) -> String {
        let mut stats = String::new();
        // Randomly select stats from 5 buckets
        let mut bucket_idx = Vec::new();
        for _ in 0..5 {
            bucket_idx.push(gen_random_int(0, self.num_buckets - 1));
        }
        stats.push_str(&format!("Number of buckets: {}\n", self.num_buckets));
        stats.push_str("Randomly getting stats from 5 buckets\n");
        for idx in bucket_idx.iter() {
            stats.push_str(&format!("Bucket {}:\n", idx));
            stats.push_str(&self.buckets[*idx].page_stats(verbose));
        }
        stats
    }

    fn get_bucket(&self, key: &[u8]) -> &Arc<ReadOptimizedChain<T>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = hasher.finish() % self.num_buckets as u64;
        &self.buckets[idx as usize]
    }
}

impl<T: MemPool> UniqueKeyIndex for HashReadOptimize<T> {
    type Iter = HashReadOptimizedChainIter<T>;

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        self.get_bucket(key).get(key)
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.get_bucket(key).insert(key, value)
    }

    fn update(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.get_bucket(key).update(key, value)
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.get_bucket(key).upsert(key, value)
    }

    fn upsert_with_merge(
        &self,
        _key: &[u8],
        _value: &[u8],
        _merge_fn: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        unimplemented!()
        // self.get_bucket(key).upsert_with_merge(key, value, merge_fn)
    }

    fn delete(&self, _key: &[u8]) -> Result<(), AccessMethodError> {
        unimplemented!()
        // self.get_bucket(key).delete(key)
    }

    fn scan(self: &Arc<Self>) -> Self::Iter {
        // Chain the iterators from all the buckets
        let mut scanners = Vec::with_capacity(self.num_buckets);
        for bucket in self.buckets.iter() {
            scanners.push(bucket.scan(&[], &[]));
        }
        HashReadOptimizedChainIter::new(scanners)
    }

    fn scan_with_filter(self: &Arc<Self>, _filter: FilterType) -> Self::Iter {
        unimplemented!()
    }
}

pub struct HashReadOptimizedChainIter<T: MemPool> {
    scanners: Vec<ReadOptimizedChainRangeScanner<T>>,
    current: usize,
}

impl<T: MemPool> HashReadOptimizedChainIter<T> {
    pub fn new(scanners: Vec<ReadOptimizedChainRangeScanner<T>>) -> Self {
        Self {
            scanners,
            current: 0,
        }
    }
}

impl<T: MemPool> Iterator for HashReadOptimizedChainIter<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current >= self.scanners.len() {
                return None;
            }
            if let Some((key, value)) = self.scanners[self.current].next() {
                return Some((key, value));
            }
            self.current += 1;
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashSet, fs::File, sync::Arc};

    use crate::{
        bp::{get_in_mem_pool, get_test_bp, BufferPool},
        prelude::UniqueKeyIndex,
        random::RandomKVs,
    };

    use super::{ContainerKey, HashReadOptimize, MemPool};

    fn to_bytes(num: usize) -> Vec<u8> {
        num.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> usize {
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    use rstest::rstest;

    fn setup_hashchain_empty<T: MemPool>(bp: Arc<T>) -> HashReadOptimize<T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);

        HashReadOptimize::new(c_key, bp.clone(), 10)
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = setup_hashchain_empty(bp.clone());
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
        let chain = setup_hashchain_empty(bp.clone());
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

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_upserts<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = setup_hashchain_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.upsert(&key, &val).unwrap();
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
        let new_val = vec![4_u8; 128];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.upsert(&key, &new_val).unwrap();
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
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.upsert(&key, &new_val).unwrap();
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

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_scan<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = Arc::new(setup_hashchain_empty(bp.clone()));
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            chain.upsert(&key, &val).unwrap();
        }

        let iter = chain.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            let key = from_bytes(&key);
            println!(
                "**************************** Scanning key {} **************************",
                key
            );
            assert_eq!(current_val, val);
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_insertion_stress<T: MemPool>(#[case] bp: Arc<T>) {
        let num_keys = 10000;
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

        let chain = Arc::new(setup_hashchain_empty(bp.clone()));

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

        let iter = chain.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {:?} **************************",
                key
            );
            let val = kvs.get(&key).unwrap();
            assert_eq!(&current_val, val);
            count += 1;
        }
        assert_eq!(count, num_keys);

        for (key, val) in kvs.iter() {
            println!(
                "**************************** Getting key {:?} **************************",
                key
            );
            let current_val = chain.get(key).unwrap();
            assert_eq!(current_val, *val);
        }

        let iter = chain.scan();
        let mut count = 0;
        for (key, current_val) in iter {
            println!(
                "**************************** Scanning key {:?} **************************",
                key
            );
            let val = kvs.get(&key).unwrap();
            assert_eq!(&current_val, val);
            count += 1;
        }

        assert_eq!(count, num_keys);

        // println!("{}", chain.page_stats(false));

        println!("SUCCESS");
    }

    // skip default
    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn replay_stress<T: MemPool>(#[case] bp: Arc<T>) {
        let chain = setup_hashchain_empty(bp.clone());

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
    fn test_parallel_insertion<T: MemPool>(#[case] bp: Arc<T>) {
        // init_test_logger();
        let chain = Arc::new(setup_hashchain_empty(bp.clone()));
        let num_keys = 5000;
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

        println!("Number of keys: {}", num_keys);

        // Use 3 threads to insert keys into the tree.
        // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
        std::thread::scope(
            // issue three threads to insert keys into the tree
            |s| {
                for kvs_i in kvs.iter() {
                    let chain = chain.clone();
                    s.spawn(move || {
                        println!("Spawned");
                        for (key, val) in kvs_i.iter() {
                            println!("Inserting key {:?}", key);
                            chain.insert(key, val).unwrap();
                        }
                    });
                }
            },
        );

        // Check if all keys have been inserted.
        for kvs_i in verify_kvs {
            for (key, val) in kvs_i.iter() {
                let current_val = chain.get(key).unwrap();
                assert_eq!(current_val, *val);
            }
        }
    }

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

            let scanner = store.scan();
            // Remove the keys from the expected_vals set as they are scanned.
            for (key, val) in scanner {
                let key = key.to_vec();
                let val = val.to_vec();
                assert!(expected_vals.remove(&(key, val)));
            }

            assert!(expected_vals.is_empty());
        }
    }

    #[test]
    fn test_page_stat_generator() {
        let btree = setup_hashchain_empty(get_in_mem_pool());
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
}
