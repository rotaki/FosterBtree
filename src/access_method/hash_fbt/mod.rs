use core::num;
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::{
    bp::{ContainerKey, EvictionPolicy, MemPool, PageFrameKey},
    page::{PageId, AVAILABLE_PAGE_SIZE},
};

use super::fbt::{FosterBtree, FosterBtreeRangeScanner, TreeStatus};

pub mod prelude {
    pub use super::HashFosterBtree;
}

pub struct HashFosterBtree<E: EvictionPolicy, T: MemPool<E>> {
    pub mem_pool: Arc<T>,
    c_key: ContainerKey,
    num_buckets: usize,
    meta_page_id: PageId, // Stores the number of buckets and all the page ids of the root of the foster btrees
    buckets: Vec<Arc<FosterBtree<E, T>>>,
    phantom: std::marker::PhantomData<E>,
}

impl<E: EvictionPolicy, T: MemPool<E>> HashFosterBtree<E, T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        if num_buckets == 0 {
            panic!("Number of buckets cannot be 0");
        }
        // If number of buckets does not fit in the root_of_root page, panic
        if num_buckets * std::mem::size_of::<PageId>() + std::mem::size_of::<usize>()
            > AVAILABLE_PAGE_SIZE
        {
            panic!("Number of buckets too large to fit in the root_of_root page");
        }

        let mut root_of_root = mem_pool.create_new_page_for_write(c_key.clone()).unwrap();

        let mut offset = 0;
        let num_buckets_bytes = num_buckets.to_be_bytes();
        root_of_root[offset..offset + num_buckets_bytes.len()].copy_from_slice(&num_buckets_bytes);
        offset += num_buckets_bytes.len();

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            // Create a new foster btree
            let tree = Arc::new(FosterBtree::new(c_key, mem_pool.clone()));

            let root_p_id = tree.root_key.p_key().page_id.to_be_bytes();
            root_of_root[offset..offset + root_p_id.len()].copy_from_slice(&root_p_id);
            offset += root_p_id.len();

            buckets.push(tree);
        }

        let meta_page_id = root_of_root.get_id();

        Self {
            mem_pool: mem_pool.clone(),
            c_key,
            num_buckets,
            meta_page_id,
            buckets,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, meta_page_id: PageId) -> Self {
        let root_of_root = mem_pool
            .get_page_for_read(PageFrameKey::new(c_key, meta_page_id))
            .unwrap();

        let mut offset = 0;
        let num_buckets_bytes = &root_of_root[offset..offset + std::mem::size_of::<usize>()];
        offset += num_buckets_bytes.len();
        let num_buckets = usize::from_be_bytes(num_buckets_bytes.try_into().unwrap());

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            let root_page_id_bytes = &root_of_root[offset..offset + std::mem::size_of::<PageId>()];
            offset += root_page_id_bytes.len();
            let root_page_id = PageId::from_be_bytes(root_page_id_bytes.try_into().unwrap());
            let tree = Arc::new(FosterBtree::load(
                c_key.clone(),
                mem_pool.clone(),
                root_page_id,
            ));
            buckets.push(tree);
        }

        Self {
            mem_pool: mem_pool.clone(),
            c_key,
            num_buckets,
            meta_page_id,
            buckets,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.buckets.iter().map(|b| b.num_kvs()).sum()
    }

    fn get_bucket(&self, key: &[u8]) -> &Arc<FosterBtree<E, T>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = hasher.finish() % self.num_buckets as u64;
        &self.buckets[idx as usize]
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, TreeStatus> {
        self.get_bucket(key).get(key)
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        self.get_bucket(key).insert(key, value)
    }

    pub fn update(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        self.get_bucket(key).update(key, value)
    }

    pub fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        self.get_bucket(key).upsert(key, value)
    }

    pub fn upsert_with_merge(
        &self,
        key: &[u8],
        value: &[u8],
        merge_fn: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), TreeStatus> {
        self.get_bucket(key).upsert_with_merge(key, value, merge_fn)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), TreeStatus> {
        self.get_bucket(key).delete(key)
    }

    pub fn scan(self: &Arc<Self>) -> HashFosterBtreeIter<E, T> {
        // Chain the iterators from all the buckets
        let mut scanners = Vec::with_capacity(self.num_buckets);
        for bucket in self.buckets.iter() {
            scanners.push(bucket.scan(&[], &[]));
        }
        HashFosterBtreeIter::new(scanners)
    }
}

pub struct HashFosterBtreeIter<E: EvictionPolicy + 'static, T: MemPool<E>> {
    scanners: Vec<FosterBtreeRangeScanner<E, T>>,
    current: usize,
}

impl<E: EvictionPolicy, T: MemPool<E>> HashFosterBtreeIter<E, T> {
    pub fn new(scanners: Vec<FosterBtreeRangeScanner<E, T>>) -> Self {
        Self {
            scanners,
            current: 0,
        }
    }
}

impl<E: EvictionPolicy, T: MemPool<E>> Iterator for HashFosterBtreeIter<E, T> {
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
        bp::{get_in_mem_pool, get_test_bp, BufferPool, LRUEvictionPolicy},
        log_trace,
        prelude::TreeStatus,
        random::RandomKVs,
    };

    use super::{ContainerKey, EvictionPolicy, HashFosterBtree, MemPool};

    fn to_bytes(num: usize) -> Vec<u8> {
        num.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> usize {
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    use rstest::rstest;

    fn setup_hashbtree_empty<E: EvictionPolicy, T: MemPool<E>>(
        bp: Arc<T>,
    ) -> HashFosterBtree<E, T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);

        HashFosterBtree::new(c_key, bp.clone(), 10)
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_insertion<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = setup_hashbtree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_updates<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = setup_hashbtree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        let new_val = vec![4_u8; 128];
        for i in order.iter() {
            println!(
                "**************************** Updating key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.update(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
        let new_val = vec![5_u8; 512];
        for i in order.iter() {
            println!(
                "**************************** Updating key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.update(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_deletion<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = setup_hashbtree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Deleting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.delete(&key).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key);
            assert_eq!(current_val, Err(TreeStatus::NotFound))
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_random_upserts<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = setup_hashbtree_empty(bp.clone());
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, val);
        }
        let new_val = vec![4_u8; 128];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
        let new_val = vec![5_u8; 512];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &new_val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get(&key).unwrap();
            assert_eq!(current_val, new_val);
        }
    }

    #[rstest]
    #[case::bp(get_test_bp(3))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_upsert_with_merge<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = setup_hashbtree_empty(bp.clone());
        // Insert 1024 bytes
        let key = to_bytes(0);
        let vals = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in vals.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let val = to_bytes(*i);
            btree
                .upsert_with_merge(&key, &val, |old_val: &[u8], new_val: &[u8]| -> Vec<u8> {
                    // Deserialize old_val and new_val and add them.
                    let old_val = from_bytes(old_val);
                    let new_val = from_bytes(new_val);
                    to_bytes(old_val + new_val)
                })
                .unwrap();
        }
        let expected_val = vals.iter().sum::<usize>();
        let current_val = btree.get(&key).unwrap();
        assert_eq!(from_bytes(&current_val), expected_val);
    }

    #[rstest]
    #[case::bp(get_test_bp(20))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_scan<E: EvictionPolicy + 'static, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = Arc::new(setup_hashbtree_empty(bp.clone()));
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Upserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.upsert(&key, &val).unwrap();
        }

        let iter = btree.scan();
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
    fn test_insertion_stress<E: EvictionPolicy + 'static, T: MemPool<E>>(#[case] bp: Arc<T>) {
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

        let btree = Arc::new(setup_hashbtree_empty(bp.clone()));

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
            btree.insert(key, val).unwrap();
        }

        let iter = btree.scan();
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
            let current_val = btree.get(key).unwrap();
            assert_eq!(current_val, *val);
        }

        let iter = btree.scan();
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

        // println!("{}", btree.page_stats(false));

        println!("SUCCESS");
    }

    // skip default
    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    #[ignore]
    fn replay_stress<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        let btree = setup_hashbtree_empty(bp.clone());

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
            btree.insert(key, val).unwrap();
        }

        let (k, v) = &kvs[bug_occurred_at];
        println!(
            "BUG INSERT ************** Inserting {} key={:?} **************************",
            bug_occurred_at, k
        );
        btree.insert(k, v).unwrap();

        /*
        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Getting {} key={} **************************",
                i,
                key
            );
            let key = to_bytes(*key);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, *val);
        }
        */

        // let dot_string = btree.generate_dot();
        // let dot_file = "btree.dot";
        // let mut file = File::create(dot_file).unwrap();
        // // write dot_string as txt
        // file.write_all(dot_string.as_bytes()).unwrap();
    }

    #[rstest]
    #[case::bp(get_test_bp(100))]
    #[case::in_mem(get_in_mem_pool())]
    fn test_parallel_insertion<E: EvictionPolicy, T: MemPool<E>>(#[case] bp: Arc<T>) {
        // init_test_logger();
        let btree = Arc::new(setup_hashbtree_empty(bp.clone()));
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

        log_trace!("Number of keys: {}", num_keys);

        // Use 3 threads to insert keys into the tree.
        // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
        std::thread::scope(
            // issue three threads to insert keys into the tree
            |s| {
                for kvs_i in kvs.iter() {
                    let btree = btree.clone();
                    s.spawn(move || {
                        log_trace!("Spawned");
                        for (key, val) in kvs_i.iter() {
                            log_trace!("Inserting key {:?}", key);
                            btree.insert(key, val).unwrap();
                        }
                    });
                }
            },
        );

        // Check if all keys have been inserted.
        for kvs_i in verify_kvs {
            for (key, val) in kvs_i.iter() {
                let current_val = btree.get(key).unwrap();
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
            let bp = Arc::new(BufferPool::<LRUEvictionPolicy>::new(&temp_dir, 20, false).unwrap());

            let c_key = ContainerKey::new(0, 0);
            let store = Arc::new(HashFosterBtree::new(c_key, bp.clone(), 10));

            for (key, val) in vals.iter() {
                store.insert(key, val).unwrap();
            }

            drop(store);
            drop(bp);
        }

        {
            let bp = Arc::new(BufferPool::<LRUEvictionPolicy>::new(&temp_dir, 10, false).unwrap());

            let c_key = ContainerKey::new(0, 0);
            let store = Arc::new(HashFosterBtree::load(c_key, bp.clone(), 0));

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
}
