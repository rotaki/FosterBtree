use std::sync::Arc;
use crate::bp::{ContainerKey, MemPool};
use super::{
    Timestamp,
    mvcc_hash_join_recent_chain::MvccHashJoinRecentChain,
    mvcc_hash_join_history_chain::MvccHashJoinHistoryChain,
};

pub struct MvccHashBucket<T: MemPool> {
    c_key: ContainerKey,
    bp: Arc<T>,
    recent_chain: Arc<MvccHashJoinRecentChain<T>>,
    history_chain: Arc<MvccHashJoinHistoryChain<T>>,
}

impl<T: MemPool> MvccHashBucket<T> {
    pub fn new(c_key: ContainerKey, bp: Arc<T>) -> Self {
        let recent_chain = Arc::new(MvccHashJoinRecentChain::new(c_key, bp.clone()));
        let history_chain = Arc::new(MvccHashJoinHistoryChain::new(c_key, bp.clone()));

        Self {
            c_key,
            bp,
            recent_chain,
            history_chain,
        }
    }

    // Implement methods to insert, get, and update entries
    // which delegate to the appropriate chain
    pub fn insert(&self, key: Vec<u8>, pkey: Vec<u8>, ts: Timestamp, val: Vec<u8>) {
    }

    pub fn get(&self, key: &[u8], ts: Timestamp) -> Option<Vec<Vec<u8>>> {
        todo!()
    }

    pub fn update(&self, key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8]) {
    }

    // Additional methods as needed...
}