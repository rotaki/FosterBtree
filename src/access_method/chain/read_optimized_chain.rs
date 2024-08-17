use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_trace, log_warn,
    page::{PageId, PAGE_SIZE},
};

use super::read_optimized_page::ReadOptimizedPage;

pub struct ReadOptimizedChain<T: MemPool> {
    c_key: ContainerKey,
    bp: Arc<T>,

    first_page_id: PageId,
    first_frame_id: AtomicU32,
}

impl<T: MemPool> ReadOptimizedChain<T> {
    pub fn new(c_key: ContainerKey, bp: Arc<T>) -> Self {
        let mut first_page = bp.create_new_page_for_write(c_key).unwrap();
        first_page.init();
        Self {
            c_key,
            bp: bp.clone(),
            first_page_id: first_page.get_id(),
            first_frame_id: AtomicU32::new(first_page.frame_id()),
        }
    }

    pub fn load(c_key: ContainerKey, bp: Arc<T>, first_page_id: PageId) -> Self {
        Self {
            c_key,
            bp: bp.clone(),
            first_page_id,
            first_frame_id: AtomicU32::new(u32::MAX),
        }
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.bp.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
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

    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.bp.get_page_for_write(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => {
                    panic!("Write page error: {:?}", err);
                }
            }
        }
    }

    pub fn first_key(&self) -> PageFrameKey {
        PageFrameKey::new_with_frame_id(
            self.c_key,
            self.first_page_id,
            self.first_frame_id
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn first_page(&self) -> FrameReadGuard {
        let first_frame_id = self
            .first_frame_id
            .load(std::sync::atomic::Ordering::Acquire);
        let first_page = self.read_page(PageFrameKey::new_with_frame_id(
            self.c_key,
            self.first_page_id,
            first_frame_id,
        ));
        if first_page.frame_id() != first_frame_id {
            log_debug!("Frame of the first page has been changed. Trying to fix the frame id");
            self.first_frame_id
                .store(first_page.frame_id(), std::sync::atomic::Ordering::Release);
        }
        first_page
    }

    /// Insert a new key-value pair into the index.
    /// If the key already exists, it will return an error.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        // TODO: check space then no need to get write lock
        let mut last_page = self.traverse_until_endofchain_for_insert(self.first_key(), key)?;
        log_trace!("Acquired write lock for page {}", last_page.get_id());
        match last_page.insert(key, value) {
            Ok(_) => Ok(()),
            Err(AccessMethodError::OutOfSpace) => {
                log_debug!(
                    "Not enough space in page {}. Creating a new page.",
                    last_page.get_id()
                );
                let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                new_page.init();
                last_page.set_next_page(new_page.get_id(), new_page.frame_id());
                log_trace!(
                    "Linked last page {} -> new page {}",
                    last_page.get_id(),
                    new_page.get_id()
                );
                match new_page.insert(key, value) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn traverse_until_endofchain_for_insert(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let last_page = self.try_traverse_until_endofchain_for_insert(page_key, key);
            match last_page {
                Ok(last_page) => {
                    return Ok(last_page);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_trace!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        base.pow(attempts)
                    );
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                }
                Err(AccessMethodError::KeyDuplicate) => {
                    return Err(AccessMethodError::KeyDuplicate);
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn try_traverse_until_endofchain_for_insert(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        loop {
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                if current_page.binary_search(key).0 {
                    return Err(AccessMethodError::KeyDuplicate);
                }
                // TODO: check free space may can insert here later.
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                // TODO: check key to avoid write lock in case of duplicate key
                match current_page.try_upgrade(true) {
                    Ok(upgraded_page) => {
                        return Ok(upgraded_page);
                    }
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            match current_page.get(key) {
                Ok(value) => {
                    return Ok(value.to_vec());
                }
                Err(AccessMethodError::KeyNotFound) => {
                    if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                        let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                            self.c_key,
                            next_page_id,
                            next_frame_id,
                        ));
                        if next_page.frame_id() != next_frame_id {
                            log_debug!("Frame of the next page has been changed. Trying to fix the frame id");
                            let new_frame_key = PageFrameKey::new_with_frame_id(
                                self.c_key,
                                next_page_id,
                                next_page.frame_id(),
                            );
                            let _ = fix_frame_id(current_page, &new_frame_key);
                        }
                        current_page = next_page;
                    } else {
                        return Err(AccessMethodError::KeyNotFound);
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub fn update(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.traverse_to_endofchain_for_update(self.first_key(), key, value)?;
        Ok(())
    }

    fn traverse_to_endofchain_for_update(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
        value: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let find_page = self.try_traverse_to_endofchain_for_update(page_key, key, value);
            match find_page {
                Ok(find_page) => {
                    return Ok(find_page);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_trace!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        base.pow(attempts)
                    );
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                }
                Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                    log_debug!(
                        "Should not happen in YCSB workload. key({}) old_value({})",
                        key,
                        old_val
                    );
                    return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                }
                Err(e) => {
                    log_debug!("Error while traverse for upadate: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    fn try_traverse_to_endofchain_for_update(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
        value: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        loop {
            let (found, slot_id) = current_page.binary_search(key);
            if found {
                match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        match upgraded_page.update_at_slot_id(key, value, slot_id) {
                            Ok(_) => {
                                return Ok(upgraded_page);
                            }
                            Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                                log_debug!("Not enough space in page {}. Delete the key({}) and old_value({}), then insert updated key to next page", upgraded_page.get_id(), key, old_val);
                                return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                log_debug!("Key({}) not found for update.", key);
                return Err(AccessMethodError::KeyNotFound);
            }
        }
    }

    pub fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.traverse_to_endofchain_for_upsert(self.first_key(), key, value)?;
        Ok(())
    }

    fn traverse_to_endofchain_for_upsert(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
        value: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let find_page = self.try_traverse_to_endofchain_for_upsert(page_key, key, value);
            match find_page {
                Ok(find_page) => {
                    return Ok(find_page);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_trace!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        base.pow(attempts)
                    );
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                }
                Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                    log_debug!(
                        "Should not happen in YCSB workload. key({}) old_value({})",
                        key,
                        old_val
                    );
                    return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                }
                Err(e) => {
                    log_debug!("Error while traverse for upadate: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    fn try_traverse_to_endofchain_for_upsert(
        &self,
        page_key: PageFrameKey,
        key: &[u8],
        value: &[u8],
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        loop {
            let (found, slot_id) = current_page.binary_search(key);
            if found {
                match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        match upgraded_page.update_at_slot_id(key, value, slot_id) {
                            Ok(_) => {
                                return Ok(upgraded_page);
                            }
                            Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                                log_debug!("Not enough space in page {}. Delete the key({}) and old_value({}), then insert updated key to next page", upgraded_page.get_id(), key, old_val);
                                return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        log_debug!(
                            "Key not found for update. If upsert, insert the key-value pair"
                        );
                        match upgraded_page.insert(key, value) {
                            Ok(_) => {
                                return Ok(upgraded_page);
                            }
                            Err(AccessMethodError::OutOfSpace) => {
                                log_debug!(
                                    "Not enough space in page {}. Creating a new page.",
                                    upgraded_page.get_id()
                                );
                                let mut new_page =
                                    self.bp.create_new_page_for_write(self.c_key).unwrap();
                                new_page.init();
                                upgraded_page.set_next_page(new_page.get_id(), new_page.frame_id());
                                log_trace!(
                                    "Linked last page {} -> new page {}",
                                    upgraded_page.get_id(),
                                    new_page.get_id()
                                );
                                match new_page.insert(key, value) {
                                    Ok(_) => {
                                        return Ok(new_page);
                                    }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                }
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                        return Ok(upgraded_page);
                    }
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
        }
    }

    // fn insert_traverse_to_endofchain_for_read(&self, key: &[u8]) -> Result<FrameReadGuard, AccessMethodError> {
    //     todo!();
    // }

    pub fn scan(self: &Arc<Self>, l_key: &[u8], r_key: &[u8]) -> ReadOptimizedChainRangeScanner<T> {
        ReadOptimizedChainRangeScanner::new(self, l_key, r_key)
    }

    pub fn scan_with_filter(
        self: &Arc<Self>,
        l_key: &[u8],
        r_key: &[u8],
        filter: FilterFunc,
    ) -> ReadOptimizedChainRangeScanner<T> {
        ReadOptimizedChainRangeScanner::new_with_filter(self, l_key, r_key, filter)
    }
}

/// Opportunistically try to fix the next page frame id
fn fix_frame_id<'a>(this: FrameReadGuard<'a>, new_frame_key: &PageFrameKey) -> FrameReadGuard<'a> {
    match this.try_upgrade(true) {
        Ok(mut write_guard) => {
            write_guard.set_next_page(new_frame_key.p_key().page_id, new_frame_key.frame_id());
            log_debug!("Fixed frame id of the next page");
            write_guard.downgrade()
        }
        Err(read_guard) => {
            log_debug!("Failed to fix frame id of the next page");
            read_guard
        }
    }
}

type FilterFunc = Box<dyn FnMut((&[u8], &[u8])) -> bool>;

/// Scan the Chain in the range [l_key, r_key)
/// To specify all keys, use an empty slice.
/// (l_key, r_key) = (&[], &[]) means [-inf, +inf)
pub struct ReadOptimizedChainRangeScanner<T: MemPool> {
    chain: Arc<ReadOptimizedChain<T>>, // Holds the reference to the chain

    // Scan parameters
    l_key: Vec<u8>,
    r_key: Vec<u8>,
    filter: Option<FilterFunc>,

    // States
    initialized: bool,
    finished: bool,
    current_page: Option<FrameReadGuard<'static>>, // As long as chain is alive, bp is alive so the frame is alive
    current_slot_id: u32,
}

impl<T: MemPool> ReadOptimizedChainRangeScanner<T> {
    pub fn new(chain: &Arc<ReadOptimizedChain<T>>, l_key: &[u8], r_key: &[u8]) -> Self {
        let (l_key, r_key) = Self::normalize_keys(l_key, r_key);
        Self {
            chain: chain.clone(),

            l_key,
            r_key,
            filter: None,

            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
        }
    }

    pub fn new_with_filter(
        chain: &Arc<ReadOptimizedChain<T>>,
        l_key: &[u8],
        r_key: &[u8],
        filter: FilterFunc,
    ) -> Self {
        let (l_key, r_key) = Self::normalize_keys(l_key, r_key);
        Self {
            chain: chain.clone(),

            l_key,
            r_key,
            filter: Some(filter),

            initialized: false,
            finished: false,
            current_page: None,
            current_slot_id: 0,
        }
    }

    fn normalize_keys(l_key: &[u8], r_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let l_key = if l_key.is_empty() {
            vec![0] // Minimum possible value
        } else {
            l_key.to_vec()
        };

        let r_key = if r_key.is_empty() {
            r_key.to_vec() // do not check when empty
                           // vec![255; 255] // Maximum possible value
        } else {
            r_key.to_vec()
        };

        (l_key, r_key)
    }

    fn initialize(&mut self) {
        // Start scanning from the first page
        let first_page = self.chain.first_page();
        let first_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(first_page) };
        let slot_id = first_page.binary_search(&self.l_key).1;

        self.current_page = Some(first_page);
        self.current_slot_id = slot_id;
        self.initialized = true;
    }

    fn finish(&mut self) {
        self.finished = true;
        self.current_page = None;
    }
}

impl<T: MemPool> Iterator for ReadOptimizedChainRangeScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.initialized {
            self.initialize();
        }

        loop {
            if self.current_page.is_none() {
                self.finish();
                return None;
            }

            let current_page = self.current_page.as_ref().unwrap();
            if self.current_slot_id < current_page.slot_count() {
                let (key, value) = current_page.get_with_slot_id(self.current_slot_id);
                self.current_slot_id += 1;

                if !self.r_key.is_empty() && key >= self.r_key {
                    if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                        // Move to the next page in the chain
                        let next_page = self.chain.read_page(PageFrameKey::new_with_frame_id(
                            self.chain.c_key,
                            next_page_id,
                            next_frame_id,
                        ));
                        let next_page = unsafe {
                            std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(
                                next_page,
                            )
                        };
                        self.current_slot_id = next_page.binary_search(&self.l_key).1;
                        self.current_page = Some(next_page);
                        continue;
                    } else {
                        self.finish();
                        return None;
                    }
                }

                if self.filter.is_none() || (self.filter.as_mut().unwrap())((key.as_ref(), value)) {
                    return Some((key, value.to_vec()));
                }

                continue;
            } else if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                // Move to the next page in the chain
                let next_page = self.chain.read_page(PageFrameKey::new_with_frame_id(
                    self.chain.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(next_page)
                };
                self.current_slot_id = next_page.binary_search(&self.l_key).1;
                self.current_page = Some(next_page);
                continue;
            } else {
                self.finish();
                return None;
            }
        }
    }
}
