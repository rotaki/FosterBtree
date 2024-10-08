use std::{
    hash::{Hash, Hasher}, 
    collections::hash_map::DefaultHasher,
    sync::{Arc, atomic::AtomicU32}, 
    time::Duration,
    error::Error,
    fmt::Debug,
};
use crate::{
    access_method::fbt::FosterBtreeRangeScanner, bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey}, log_debug, log_trace, log_warn, mvcc_index::{Delta, MvccIndex}, page::{Page, PageId, AVAILABLE_PAGE_SIZE}, prelude::AccessMethodError
};

use super::{
    mvcc_hash_join_history_chain::MvccHashJoinHistoryChain, 
    mvcc_hash_join_history_page::MvccHashJoinHistoryPage, 
    mvcc_hash_join_recent_chain::MvccHashJoinRecentChain, 
    mvcc_hash_join_recent_page::MvccHashJoinRecentPage, 
    Timestamp,
    TxId,
};

use rand::seq::index;
use serde::{Serialize, Deserialize};

pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const BUCKET_ENTRY_SIZE: usize = std::mem::size_of::<BucketEntry>();
pub const BUCKET_NUM_SIZE: usize = std::mem::size_of::<u64>(); // Size of bucket_num (u64)

pub const DEAFAULT_NUM_BUCKETS: usize = 16;

pub struct HashJoinTable<T: MemPool> {
    // txid: u64,
    // ts: Timestamp,

    mem_pool: Arc<T>,
    c_key: ContainerKey,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,
    
    num_buckets: usize,
    bucket_entries: Vec<(Arc<MvccHashJoinRecentChain<T>>, Arc<MvccHashJoinHistoryChain<T>>)>
}

impl<T: MemPool> MvccIndex for HashJoinTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = AccessMethodError;
    type MemPoolType = T;

    fn create(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(c_key, mem_pool))
    }

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        HashJoinTable::insert(self, key, pkey, ts, tx_id, value)
    }

    fn get(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        match self.get(key, pkey, ts) {
            Ok(val) => Ok(Some(val)),
            Err(AccessMethodError::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn get_all(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        self.get_all(key, ts)
    }

    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        HashJoinTable::update(&self, key, pkey, ts, tx_id, value)
    }

    fn delete(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), Self::Error> {
        HashJoinTable::delete(&self, key, pkey, ts, tx_id)
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error> {
        self.scan(ts)
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        self.delta_scan(from_ts, to_ts)
    }

    fn garbage_collect(
        &self,
        safe_ts: Timestamp,
    ) -> Result<(), Self::Error> {
        self.garbage_collect(safe_ts)
    }
}

impl<T: MemPool> HashJoinTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_NUM_BUCKETS)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        MvccHashJoinMetaPage::init(&mut *meta_page, num_buckets);
        MvccHashJoinMetaPage::set_bucket_num(&mut *meta_page, num_buckets);
        
        let mut bucket_entries: Vec<(Arc<MvccHashJoinRecentChain<T>>, Arc<MvccHashJoinHistoryChain<T>>)> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let recent_chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());
            let recent_page_id = recent_chain.first_page_id();
            // let recent_frame_id = recent_chain.first_frame_id();
            
            let history_chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());
            let history_page_id = history_chain.first_page_id();
            // let history_frame_id = history_chain.first_frame_id();

            let entry = BucketEntry::new(recent_page_id, history_page_id);
            MvccHashJoinMetaPage::set_bucket_entry(&mut *meta_page, i, &entry);
            bucket_entries.push((Arc::new(recent_chain), Arc::new(history_chain)));
        }
        drop(meta_page);
        
        Self {
            mem_pool,
            c_key,
            meta_page_id,
            meta_frame_id,
            num_buckets,
            bucket_entries,
        }
    }

    /// Creates a new hash join table using a scanner to populate the table.
    pub fn new_with_scanner(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        scanner: impl Iterator<Item = (Vec<u8>, Timestamp, Vec<u8>)>,
        tx_id: TxId,
    ) -> Self {
        Self::new_with_scanner_and_bucket_num(c_key, mem_pool, DEAFAULT_NUM_BUCKETS, scanner, tx_id)
    }

    /// Creates a new hash join table with a specified number of buckets, using a scanner.
    pub fn new_with_scanner_and_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
        scanner: impl Iterator<Item = (Vec<u8>, Timestamp, Vec<u8>)>,
        tx_id: TxId,
    ) -> Self {
        let mut table = Self::new_with_bucket_num(c_key, mem_pool, num_buckets);
        for (key, ts, val) in scanner {
            let pkey = key.clone();
            table.insert(key, pkey, ts, tx_id, val).unwrap();
        }
        table
    }

    /// Constructs a hash join table from an existing meta page.
    pub fn new_from_page(
        c_key: ContainerKey, 
        mem_pool: Arc<T>,
        meta_pid: PageId,
    ) -> Self {
        let temp_table = HashJoinTable {
            mem_pool: mem_pool.clone(),
            c_key,
            meta_page_id: meta_pid,
            meta_frame_id: AtomicU32::new(0),
            num_buckets: 0,      
            bucket_entries: Vec::new(),
        };

        let page_key = PageFrameKey::new(c_key, meta_pid);
        let meta_page = temp_table.read_page(page_key);

        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        let num_buckets = meta_page.get_bucket_num();
        
        let mut bucket_entries = Vec::with_capacity(num_buckets);

        for i in 0..num_buckets {
            let entry = meta_page.get_bucket_entry(i);

            let recent_chain =
                MvccHashJoinRecentChain::new_from_page(c_key, mem_pool.clone(), entry.first_recent_pid);
            let history_chain =
                MvccHashJoinHistoryChain::new_from_page(c_key, mem_pool.clone(), entry.first_history_pid);

            bucket_entries.push((Arc::new(recent_chain), Arc::new(history_chain)));
        }
        drop(meta_page);

        HashJoinTable {
            mem_pool,
            c_key,
            meta_page_id: meta_pid,
            meta_frame_id,
            num_buckets,
            bucket_entries,
        }
    }

    /// Inserts a key-value pair into the hash join table.
    pub fn insert(
        &self,
        key: Vec<u8>,
        pkey: Vec<u8>,
        ts: Timestamp,
        tx_id: TxId,
        val: Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(&key);
        let (recent_chian, _) = &self.bucket_entries[index];

        recent_chian.insert(&key, &pkey, ts, &val)
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let (recent_chain, history_chain) = &self.bucket_entries[index];

        let recent_val = recent_chain.get(key, pkey, ts);
        match recent_val {
            Ok(val) => Ok(val),
            Err(AccessMethodError::KeyNotFound) => Err(AccessMethodError::KeyNotFound),
            Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_val = history_chain.get(key, pkey, ts);
                match history_val {
                    Ok(val) => Ok(val),
                    Err(AccessMethodError::KeyNotFound) => Err(AccessMethodError::KeyNotFound),
                    Err(e) => Err(e),
                }
            },
            Err(e) => Err(e),
        }
    }

    /// Updates an existing key-value pair in the hash join table.
    pub fn update(
        &self,
        key: Vec<u8>,
        pkey: Vec<u8>,
        ts: Timestamp,
        tx_id: TxId,
        val: Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(&key);
        let (recent_chain, history_chain) = &self.bucket_entries[index];

        let old_result = recent_chain.update(&key, &pkey, ts, &val);
        match old_result {
            Ok((old_ts, old_val)) => {
                history_chain.insert(&key, &pkey, old_ts, ts, &old_val)
            },
            Err(e) => Err(e),
        }
    }

    /// Deletes a key-value pair from the hash join table.
    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), AccessMethodError> {
        todo!()
    }

    /// Flushes the in-memory bucket entries back to the meta page.
    fn flush_bucket_entries(&self) -> Result<(), AccessMethodError> {
        todo!()
    }

    /// Read page with given PageFrameKey
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_buckets
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BucketEntry {
    first_recent_pid: PageId,
    first_history_pid: PageId,
}

impl BucketEntry {
    pub fn new(first_recent_pid: PageId, first_history_pid: PageId) -> Self {
        Self {
            first_recent_pid,
            first_history_pid,
        }
    }
}
pub trait MvccHashJoinMetaPage {
    /// Initializes the meta page with the specified number of buckets.
    fn init(&mut self, num_buckets: usize);

    /// Retrieves the number of buckets from the meta page.
    fn get_bucket_num(&self) -> usize;

    /// Sets the number of buckets in the meta page.
    fn set_bucket_num(&mut self, num_buckets: usize);

    /// Gets the bucket entry at the specified index.
    fn get_bucket_entry(&self, index: usize) -> BucketEntry;

    /// Sets the bucket entry at the specified index.
    fn set_bucket_entry(&mut self, index: usize, entry: &BucketEntry);

    /// Reads all bucket entries from the meta page.
    fn read_all_entries(&self) -> Vec<BucketEntry>;

    /// Writes all bucket entries to the meta page.
    fn write_all_entries(&mut self, entries: &[BucketEntry]);
}

impl MvccHashJoinMetaPage for Page {
    fn init(&mut self, num_buckets: usize) {
        let required_size = BUCKET_NUM_SIZE + num_buckets * BUCKET_ENTRY_SIZE;
        assert!(
            required_size <= AVAILABLE_PAGE_SIZE,
            "Page size is insufficient for the number of buckets"
        );

        self.set_bucket_num(num_buckets);
        // only set bucket num here cause we need mem_pool to allocate pages
    }

    fn get_bucket_num(&self) -> usize {
        let bytes = &self[..BUCKET_NUM_SIZE];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn set_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[..BUCKET_NUM_SIZE];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn get_bucket_entry(&self, index: usize) -> BucketEntry {
        let num_buckets = self.get_bucket_num();
        assert!(index < num_buckets, "Bucket index out of bounds");

        let offset = BUCKET_NUM_SIZE + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let first_recent_pid = PageId::from_be_bytes(
            bytes[0..PAGE_ID_SIZE].try_into().unwrap(),
        );
        let first_history_pid = PageId::from_be_bytes(
            bytes[PAGE_ID_SIZE..2 * PAGE_ID_SIZE].try_into().unwrap(),
        );
        BucketEntry {
            first_recent_pid,
            first_history_pid,
        }
    }

    fn set_bucket_entry(&mut self, index: usize, entry: &BucketEntry) {
        let num_buckets = self.get_bucket_num();
        assert!(index < num_buckets, "Bucket index out of bounds");

        let offset = BUCKET_NUM_SIZE + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE]
            .copy_from_slice(&entry.first_recent_pid.to_be_bytes());
        bytes[PAGE_ID_SIZE..2 * PAGE_ID_SIZE]
            .copy_from_slice(&entry.first_history_pid.to_be_bytes());
    }

    fn read_all_entries(&self) -> Vec<BucketEntry> {
        let num_buckets = self.get_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries(&mut self, entries: &[BucketEntry]) {
        let num_buckets = self.get_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_bucket_entry(index, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_page_init() {
        let mut page = Page::new_empty();
        let num_buckets = 10;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        let stored_num_buckets = page.get_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets);
    }

    #[test]
    fn test_meta_page_set_and_get_bucket_num() {
        let mut page = Page::new_empty();
        let num_buckets = 15;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        page.set_bucket_num(num_buckets);
        let stored_num_buckets = page.get_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets);
    }

    #[test]
    fn test_meta_page_set_and_get_bucket_entry() {
        let mut page = Page::new_empty();
        let num_buckets = 5;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        
        for index in 0..num_buckets {
            let entry = BucketEntry {
                first_recent_pid: index as u32 + 100,
                first_history_pid: index as u32 + 200,
            };
            page.set_bucket_entry(index, &entry);
        }

        for index in 0..num_buckets {
            let entry = page.get_bucket_entry(index);
            assert_eq!(entry.first_recent_pid, index as u32 + 100);
            assert_eq!(entry.first_history_pid, index as u32 + 200);
        }
    }

    #[test]
    fn test_meta_page_read_and_write_all_entries() {
        let mut page = Page::new_empty();
        let num_buckets = 8;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        
        let mut entries = Vec::new();
        for index in 0..num_buckets {
            let entry = BucketEntry {
                first_recent_pid: index as u32 + 500,
                first_history_pid: index as u32 + 600,
            };
            entries.push(entry);
        }

        // Write all entries
        page.write_all_entries(&entries);

        // Read all entries
        let read_entries = page.read_all_entries();

        assert_eq!(entries, read_entries);
    }

    #[test]
    #[should_panic(expected = "Bucket index out of bounds")]
    fn test_meta_page_get_bucket_entry_out_of_bounds() {
        let mut page = Page::new_empty();
        let num_buckets = 3;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        
        // This should panic because index is equal to num_buckets
        let _entry = page.get_bucket_entry(num_buckets);
    }

    #[test]
    #[should_panic(expected = "Page size is insufficient for the number of buckets")]
    fn test_meta_page_init_too_many_buckets() {
        let mut page = Page::new_empty();
        let num_buckets = (AVAILABLE_PAGE_SIZE - BUCKET_NUM_SIZE) / BUCKET_ENTRY_SIZE + 1;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
    }
}