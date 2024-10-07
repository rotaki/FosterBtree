use std::{
    collections::BTreeMap,
    sync::{atomic::{self, AtomicU32}, Arc},
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_trace, log_warn,
    page::{Page, PageId, PAGE_SIZE},
};

use super::{
    mvcc_hash_join_history_page::MvccHashJoinHistoryPage,
    Timestamp,
};

pub struct MvccHashJoinHistoryChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,
    
    first_page_id: PageId,
    first_frame_id: AtomicU32,
}

impl<T: MemPool> MvccHashJoinHistoryChain<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        todo!()
    }

    pub fn new_from_page(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        pid: PageId,
    ) -> Self {
        todo!()
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        sts: Timestamp,
        ets: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        todo!()
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        todo!()
    }
    
    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        todo!()
    }

    pub fn first_page_id(&self) -> PageId {
        self.first_page_id
    }

    pub fn first_frame_id(&self) -> u32 {
        self.first_frame_id.load(atomic::Ordering::Acquire)
    }
}