use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    page::{PageId, AVAILABLE_PAGE_SIZE},
    prelude::{AccessMethodError, UniqueKeyIndex},
    random::gen_random_int,
};

use super::BloomChain;

pub struct HashBloomChain<T: MemPool> {
    pub mem_pool: Arc<T>,
    c_key: ContainerKey,
    num_buckets: usize,
    meta_page_id: PageId,
    buckets: Vec<Arc<BloomChain<T>>>,
}

impl<T: MemPool> HashBloomChain<T> {
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
            let chain = Arc::new(BloomChain::new(c_key, mem_pool.clone()));

            let root_p_id = chain.first_key().p_key().page_id.to_be_bytes();
            meta_page[offset..offset + root_p_id.len()].copy_from_slice(&root_p_id);
            offset += root_p_id.len();

            buckets.push(chain);
        }

        let meta_page_id = meta_page.get_id();

        Self {
            mem_pool: mem_pool.clone(),
            c_key,
            num_buckets,
            meta_page_id,
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
            let chain = Arc::new(BloomChain::load(c_key, mem_pool.clone(), root_page_id));
            buckets.push(chain);
        }

        Self {
            mem_pool: mem_pool.clone(),
            c_key,
            num_buckets,
            meta_page_id,
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

    fn get_bucket(&self, key: &[u8]) -> &Arc<BloomChain<T>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = hasher.finish() % self.num_buckets as u64;
        &self.buckets[idx as usize]
    }
}

impl<T: MemPool> UniqueKeyIndex for HashBloomChain<T> {
    type Iter = std::vec::IntoIter<(Vec<u8>, Vec<u8>)>;

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
        key: &[u8],
        value: &[u8],
        merge_fn: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        unimplemented!()
        // self.get_bucket(key).upsert_with_merge(key, value, merge_fn)
    }

    fn delete(&self, key: &[u8]) -> Result<(), AccessMethodError> {
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
