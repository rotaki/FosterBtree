mod buffer_pool_clock;
mod buffer_pool_lru;
mod resident_set;
mod vmcache;

use std::sync::Arc;
pub use buffer_pool_clock::BufferPoolClock;
pub use buffer_pool_lru::BufferPoolLRU;
pub use vmcache::VMCachePool;

use crate::{container::ContainerManager, random::gen_random_pathname};

use std::sync::atomic::{AtomicUsize, Ordering};

const IN_MEMORY_BP_NUM_FRAMES: usize = 8 * 1024;

#[cfg(feature = "bp_clock")]
pub type InMemPool = BufferPoolClock;
#[cfg(not(feature = "bp_clock"))]
pub type InMemPool = BufferPoolLRU;

// ============================================================================
// BUFFER POOL STATISTICS
// ============================================================================

/// Statistics kept by the buffer pool.
/// These statistics are used for decision making.
pub struct BPStats {
    new_page_request: AtomicUsize,
    read_request: AtomicUsize,
    read_request_waiting_for_write: AtomicUsize,
    write_request: AtomicUsize,
}

impl std::fmt::Display for BPStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "New Page: {}\nRead Count: {}\nWrite Count: {}",
            self.new_page_request.load(Ordering::Relaxed),
            self.read_request.load(Ordering::Relaxed),
            self.write_request.load(Ordering::Relaxed)
        )
    }
}

impl BPStats {
    pub fn new() -> Self {
        BPStats {
            new_page_request: AtomicUsize::new(0),
            read_request: AtomicUsize::new(0),
            read_request_waiting_for_write: AtomicUsize::new(0),
            write_request: AtomicUsize::new(0),
        }
    }

    pub fn clear(&self) {
        self.new_page_request.store(0, Ordering::Relaxed);
        self.read_request.store(0, Ordering::Relaxed);
        self.read_request_waiting_for_write
            .store(0, Ordering::Relaxed);
        self.write_request.store(0, Ordering::Relaxed);
    }

    pub fn new_page(&self) -> usize {
        self.new_page_request.load(Ordering::Relaxed)
    }

    pub fn inc_new_page(&self) {
        #[cfg(feature = "stat")]
        self.new_page_request.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_new_pages(&self, _num_pages: usize) {
        #[cfg(feature = "stat")]
        self.new_page_request
            .fetch_add(_num_pages, Ordering::Relaxed);
    }

    pub fn read_count(&self) -> usize {
        self.read_request.load(Ordering::Relaxed)
    }

    pub fn inc_read_count(&self) {
        #[cfg(feature = "stat")]
        self.read_request.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_request_waiting_for_write_count(&self) -> usize {
        self.read_request_waiting_for_write.load(Ordering::Relaxed)
    }

    pub fn inc_read_request_waiting_for_write_count(&self) {
        #[cfg(feature = "stat")]
        self.read_request_waiting_for_write
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_count(&self) -> usize {
        self.write_request.load(Ordering::Relaxed)
    }

    pub fn inc_write_count(&self) {
        #[cfg(feature = "stat")]
        self.write_request.fetch_add(1, Ordering::Relaxed);
    }
}

use std::{collections::BTreeMap, fmt};

use crate::page::PageId;

// ============================================================================
// CORE TYPE DEFINITIONS
// ============================================================================

pub type DatabaseId = u16;
pub type LocalContainerId = u16;
pub type FrameId = u32;

pub const INVALID_FRAME_ID: FrameId = FrameId::MAX;

#[inline]
pub const fn frame_hint_from_raw(raw: FrameId) -> Option<FrameId> {
    if raw == INVALID_FRAME_ID {
        None
    } else {
        Some(raw)
    }
}

#[inline]
pub const fn frame_hint_to_raw(hint: Option<FrameId>) -> FrameId {
    match hint {
        Some(frame_id) => frame_id,
        None => INVALID_FRAME_ID,
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContainerId {
    packed: u32,
}

impl ContainerId {
    #[inline]
    pub const fn new(db_id: DatabaseId, local_container_id: LocalContainerId) -> Self {
        Self {
            packed: ((db_id as u32) << 16) | (local_container_id as u32),
        }
    }

    #[inline]
    pub const fn from_u32(raw: u32) -> Self {
        Self { packed: raw }
    }

    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.packed
    }

    #[inline]
    pub const fn db_id(self) -> DatabaseId {
        (self.packed >> 16) as DatabaseId
    }

    #[inline]
    pub const fn local_container_id(self) -> LocalContainerId {
        self.packed as LocalContainerId
    }
}

impl fmt::Debug for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ContainerId")
            .field(&self.db_id())
            .field(&self.local_container_id())
            .finish()
    }
}

impl fmt::Display for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(db:{}, c:{})", self.db_id(), self.local_container_id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageAddr {
    pub container_id: ContainerId,
    pub page_id: PageId,
}

impl PageAddr {
    #[inline]
    pub const fn new(container_id: ContainerId, page_id: PageId) -> Self {
        Self {
            container_id,
            page_id,
        }
    }

    #[inline]
    pub const fn to_u64(self) -> u64 {
        ((self.container_id.as_u32() as u64) << 32) | self.page_id as u64
    }

    #[inline]
    pub const fn from_u64(raw: u64) -> Self {
        Self {
            container_id: ContainerId::from_u32((raw >> 32) as u32),
            page_id: raw as PageId,
        }
    }
}

impl std::fmt::Display for PageAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, p:{})", self.container_id, self.page_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageRef {
    page_addr: PageAddr,
    frame_id: Option<FrameId>,
}

impl PageRef {
    #[inline]
    pub const fn new(container_id: ContainerId, page_id: PageId) -> Self {
        Self {
            page_addr: PageAddr::new(container_id, page_id),
            frame_id: None,
        }
    }

    #[inline]
    pub const fn new_with_frame_id(
        container_id: ContainerId,
        page_id: PageId,
        frame_id: FrameId,
    ) -> Self {
        Self {
            page_addr: PageAddr::new(container_id, page_id),
            frame_id: Some(frame_id),
        }
    }

    #[inline]
    pub const fn new_with_hint(
        container_id: ContainerId,
        page_id: PageId,
        frame_id: Option<FrameId>,
    ) -> Self {
        Self {
            page_addr: PageAddr::new(container_id, page_id),
            frame_id,
        }
    }

    #[inline]
    pub const fn from_raw(container_id: ContainerId, page_id: PageId, frame_id: FrameId) -> Self {
        Self {
            page_addr: PageAddr::new(container_id, page_id),
            frame_id: frame_hint_from_raw(frame_id),
        }
    }

    #[inline]
    pub const fn page_addr(&self) -> PageAddr {
        self.page_addr
    }

    #[inline]
    pub const fn container_id(&self) -> ContainerId {
        self.page_addr.container_id
    }

    #[inline]
    pub const fn page_id(&self) -> PageId {
        self.page_addr.page_id
    }

    #[inline]
    pub const fn frame_hint(&self) -> Option<FrameId> {
        self.frame_id
    }

    #[inline]
    pub const fn frame_id(&self) -> FrameId {
        frame_hint_to_raw(self.frame_id)
    }

    #[inline]
    pub fn set_frame_id(&mut self, frame_id: FrameId) {
        self.frame_id = frame_hint_from_raw(frame_id);
    }

    #[inline]
    pub fn set_frame_hint(&mut self, frame_id: Option<FrameId>) {
        self.frame_id = frame_id;
    }

    #[inline]
    pub const fn frame_id_or_invalid(&self) -> FrameId {
        frame_hint_to_raw(self.frame_id)
    }

    #[inline]
    pub const fn with_frame_id(mut self, frame_id: FrameId) -> Self {
        self.frame_id = frame_hint_from_raw(frame_id);
        self
    }
}

impl std::fmt::Display for PageRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.frame_id {
            Some(frame_id) => write!(f, "({}, f:{})", self.page_addr, frame_id),
            None => write!(f, "({}, f:none)", self.page_addr),
        }
    }
}


#[cfg(test)]
mod id_tests {
    use super::{
        frame_hint_from_raw, frame_hint_to_raw, ContainerId, PageAddr, PageRef, INVALID_FRAME_ID,
    };

    #[test]
    fn container_id_round_trips_through_u32() {
        let container_id = ContainerId::new(0x1234, 0x5678);
        let decoded = ContainerId::from_u32(container_id.as_u32());

        assert_eq!(decoded.db_id(), 0x1234);
        assert_eq!(decoded.local_container_id(), 0x5678);
        assert_eq!(decoded, container_id);
    }

    #[test]
    fn page_addr_round_trips_through_u64() {
        let page_addr = PageAddr::new(ContainerId::new(7, 11), 13);
        assert_eq!(PageAddr::from_u64(page_addr.to_u64()), page_addr);
    }

    #[test]
    fn page_ref_round_trips_through_raw_frame_id() {
        let page_ref = PageRef::new_with_frame_id(ContainerId::new(7, 11), 13, 17);
        let decoded = PageRef::from_raw(
            page_ref.container_id(),
            page_ref.page_id(),
            page_ref.frame_id_or_invalid(),
        );

        assert_eq!(decoded, page_ref);
        assert_eq!(
            PageRef::from_raw(ContainerId::new(7, 11), 13, INVALID_FRAME_ID),
            PageRef::new(ContainerId::new(7, 11), 13)
        );
    }

    #[test]
    fn frame_hint_sentinel_round_trips() {
        assert_eq!(frame_hint_to_raw(None), INVALID_FRAME_ID);
        assert_eq!(frame_hint_from_raw(INVALID_FRAME_ID), None);
        assert_eq!(frame_hint_from_raw(17), Some(17));
    }
}

// ============================================================================
// STATUS AND STATISTICS
// ============================================================================

#[derive(Debug, PartialEq)]
pub enum MemPoolStatus {
    FileManagerNotFound,
    FileManagerError(String),
    PageNotFound,
    FrameReadLatchGrantFailed,
    FrameWriteLatchGrantFailed,
    CannotEvictPage,
    MemoryAllocationError(&'static str),
}

impl From<std::io::Error> for MemPoolStatus {
    fn from(s: std::io::Error) -> Self {
        MemPoolStatus::FileManagerError(s.to_string())
    }
}

impl std::fmt::Display for MemPoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemPoolStatus::FileManagerNotFound => write!(f, "[MP] File manager not found"),
            MemPoolStatus::FileManagerError(s) => std::fmt::Display::fmt(s, f),
            MemPoolStatus::PageNotFound => write!(f, "[MP] Page not found"),
            MemPoolStatus::FrameReadLatchGrantFailed => {
                write!(f, "[MP] Frame read latch grant failed")
            }
            MemPoolStatus::FrameWriteLatchGrantFailed => {
                write!(f, "[MP] Frame write latch grant failed")
            }
            MemPoolStatus::CannotEvictPage => {
                write!(f, "[MP] All frames are latched and cannot evict page")
            }
            MemPoolStatus::MemoryAllocationError(s) => {
                write!(f, "[MP] Memory allocation error: {}", s)
            }
        }
    }
}

pub struct MemoryStats {
    // Buffer pool stats
    pub bp_num_frames_in_mem: usize,
    pub bp_new_page: usize,        // Total number of new pages created (BP)
    pub bp_read_frame: usize,      // Total number of frames requested for read (BP)
    pub bp_read_frame_wait: usize, // Total number of frames requested for read but had to wait (BP)
    pub bp_write_frame: usize,     // Total number of frames requested for write (BP)
    pub bp_num_frames_per_container: BTreeMap<ContainerId, i64>, // Number of pages of each container in BP

    // Disk stats
    pub disk_created: usize, // Total number of pages created (DISK)
    pub disk_read: usize,    // Total number of pages read (DISK)
    pub disk_write: usize,   // Total number of pages written (DISK)
    pub disk_io_per_container: BTreeMap<ContainerId, (i64, i64, i64)>, // Number of pages created, read, and written for each container
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStats {
    pub fn new() -> Self {
        MemoryStats {
            bp_num_frames_in_mem: 0,
            bp_new_page: 0,
            bp_read_frame: 0,
            bp_read_frame_wait: 0,
            bp_write_frame: 0,
            bp_num_frames_per_container: BTreeMap::new(),
            disk_created: 0,
            disk_read: 0,
            disk_write: 0,
            disk_io_per_container: BTreeMap::new(),
        }
    }

    pub fn diff(&self, previous: &MemoryStats) -> MemoryStats {
        assert_eq!(self.bp_num_frames_in_mem, previous.bp_num_frames_in_mem);
        MemoryStats {
            bp_num_frames_in_mem: self.bp_num_frames_in_mem,
            bp_new_page: self.bp_new_page - previous.bp_new_page,
            bp_read_frame: self.bp_read_frame - previous.bp_read_frame,
            bp_read_frame_wait: self.bp_read_frame_wait - previous.bp_read_frame_wait,
            bp_write_frame: self.bp_write_frame - previous.bp_write_frame,
            bp_num_frames_per_container: self
                .bp_num_frames_per_container
                .iter()
                .map(|(k, v)| {
                    let prev = previous.bp_num_frames_per_container.get(k).unwrap_or(&0);
                    (*k, v - prev)
                })
                .collect(),
            disk_created: self.disk_created - previous.disk_created,
            disk_read: self.disk_read - previous.disk_read,
            disk_write: self.disk_write - previous.disk_write,
            disk_io_per_container: self
                .disk_io_per_container
                .iter()
                .map(|(k, v)| {
                    let prev = previous.disk_io_per_container.get(k).unwrap_or(&(0, 0, 0));
                    (*k, (v.0 - prev.0, v.1 - prev.1, v.2 - prev.2))
                })
                .collect(),
        }
    }
}

impl std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Buffer pool stats:")?;
        writeln!(
            f,
            "  Number of frames in memory: {}",
            self.bp_num_frames_in_mem
        )?;
        writeln!(f, "  Number of new pages created: {}", self.bp_new_page)?;
        writeln!(
            f,
            "  Number of frames requested for read: {}",
            self.bp_read_frame
        )?;
        writeln!(
            f,
            "  Number of frames requested for read but had to wait: {}",
            self.bp_read_frame_wait
        )?;
        writeln!(
            f,
            "  Number of frames requested for write: {}",
            self.bp_write_frame
        )?;
        writeln!(f, "  Number of frames for each container:")?;
        for (container_id, num_pages) in &self.bp_num_frames_per_container {
            writeln!(f, "    {}: {}", container_id, num_pages)?;
        }
        writeln!(f, "Disk stats:")?;
        writeln!(f, "  Number of pages created: {}", self.disk_created)?;
        writeln!(f, "  Number of pages read: {}", self.disk_read)?;
        writeln!(f, "  Number of pages written: {}", self.disk_write)?;
        writeln!(f, "  Number of pages read and written for each container:")?;
        for (container_id, (num_created, num_read, num_write)) in &self.disk_io_per_container {
            writeln!(
                f,
                "    {}: created={}, read={}, written={}",
                container_id, num_created, num_read, num_write
            )?;
        }
        Ok(())
    }
}

#[allow(unused_imports)]
use crate::log;
use crate::page::Page;
use crate::rwlatch::RwLatch;
use std::sync::atomic::AtomicU64;
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::AtomicBool,
};

// ============================================================================
// ATOMIC HELPERS FOR PAGE ADDRESS ENCODING
// ============================================================================

const EMPTY: u64 = u64::MAX; // 0xFFFF_FFFF_FFFF_FFFF  ⇔  None

#[inline(always)]
fn pack(key: PageAddr) -> u64 {
    //  ⟨container_id : u32⟩  ⟨page_id : u32⟩
    let raw = ((key.container_id.as_u32() as u64) << 32) | key.page_id as u64;
    debug_assert!(raw != EMPTY, "reserved for sentinel");
    raw
}

#[inline(always)]
fn unpack(raw: u64) -> Option<PageAddr> {
    if raw == EMPTY {
        None
    } else {
        Some(PageAddr {
            container_id: ContainerId::from_u32((raw >> 32) as u32),
            page_id: (raw & 0xFFFF_FFFF) as PageId,
        })
    }
}

/// Lock-free `Option<PageAddr>` slot.
#[repr(transparent)]
struct AtomicOptionKey(AtomicU64);

impl AtomicOptionKey {
    pub const fn new_none() -> Self {
        Self(AtomicU64::new(EMPTY))
    }
    #[allow(dead_code)]
    pub fn new_some(k: PageAddr) -> Self {
        Self(AtomicU64::new(pack(k)))
    }

    #[inline]
    pub fn get(&self) -> Option<PageAddr> {
        unpack(self.0.load(Ordering::Acquire))
    }
    #[allow(dead_code)]
    #[inline]
    pub fn is_none(&self) -> bool {
        self.get().is_none()
    }
    #[allow(dead_code)]
    #[inline]
    pub fn is_some(&self) -> bool {
        self.get().is_some()
    }

    /// `take()` – fetch-and-clear
    #[allow(dead_code)]
    #[inline]
    pub fn take(&self) -> Option<PageAddr> {
        unpack(self.0.swap(EMPTY, Ordering::AcqRel))
    }

    /// `replace(new)` – swap, returning the old value
    #[inline]
    pub fn replace(&self, new: Option<PageAddr>) -> Option<PageAddr> {
        let raw = new.map_or(EMPTY, pack);
        unpack(self.0.swap(raw, Ordering::AcqRel))
    }

    /// CAS: claim the slot only if it is currently empty.
    #[allow(dead_code)]
    pub fn try_claim_empty(&self, key: PageAddr) -> Result<(), Option<PageAddr>> {
        let wanted = pack(key);
        match self
            .0
            .compare_exchange(EMPTY, wanted, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(r) => Err(unpack(r)),
        }
    }
}

// ============================================================================
// FRAME METADATA AND GUARDS
// ============================================================================

#[repr(C, align(64))]
pub struct FrameMeta {
    pub(crate) frame_id: u32, // An index of the frame in the buffer pool. This is a constant value.
    pub(crate) latch: RwLatch,
    pub(crate) is_dirty: AtomicBool, // Can be updated even when ReadGuard is held (see flush_all() in buffer_pool.rs)
    pub(crate) eviction_state: AtomicU64,
    key: AtomicOptionKey,
} // This is around 4 bytes

impl FrameMeta {
    pub fn new(frame_id: u32) -> Self {
        FrameMeta {
            frame_id,
            latch: RwLatch::default(),
            is_dirty: AtomicBool::new(false),
            eviction_state: AtomicU64::new(0),
            key: AtomicOptionKey::new_none(),
        }
    }

    /// Public façade matching the old UnsafeCell<Option<…>> API
    pub fn key(&self) -> Option<PageAddr> {
        self.key.get()
    }
    pub fn set_key(&self, k: Option<PageAddr>) {
        self.key.replace(k);
    }

    pub fn eviction_score(&self) -> u64 {
        self.eviction_state.load(Ordering::Acquire)
    }

    pub fn update_eviction_score(&self, new_score: u64) {
        self.eviction_state.store(new_score, Ordering::Release);
    }
}

unsafe impl Send for FrameMeta {}
unsafe impl Sync for FrameMeta {}

pub struct FrameReadGuard {
    upgraded: AtomicBool,
    meta: NonNull<FrameMeta>,
    page: NonNull<Page>,
    _marker: std::marker::PhantomData<*mut ()>,
}

unsafe impl Send for FrameReadGuard {}
// I don't think we need sync for FrameReadGuard, because it is not shared between threads.

impl FrameReadGuard {
    #[inline]
    fn meta_ref(&self) -> &FrameMeta {
        unsafe { self.meta.as_ref() }
    }

    #[inline]
    fn page_ptr_ref(&self) -> &Page {
        unsafe { self.page.as_ref() }
    }

    pub fn new(meta: *mut FrameMeta, page: *mut Page) -> Self {
        let upgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        unsafe { meta.as_ref() }.latch.shared();
        FrameReadGuard {
            upgraded,
            meta,
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn try_new(meta: *mut FrameMeta, page: *mut Page) -> Option<Self> {
        let upgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        if unsafe { meta.as_ref() }.latch.try_shared() {
            Some(FrameReadGuard {
                upgraded,
                meta,
                page,
                _marker: std::marker::PhantomData,
            })
        } else {
            None
        }
    }

    pub fn frame_id(&self) -> u32 {
        self.meta_ref().frame_id
    }

    pub fn latch(&self) -> &RwLatch {
        &self.meta_ref().latch
    }

    pub fn dirty(&self) -> &AtomicBool {
        &self.meta_ref().is_dirty
    }

    pub fn page_addr(&self) -> Option<PageAddr> {
        self.meta_ref().key()
    }

    pub fn page_ref(&self) -> Option<PageRef> {
        self.page_addr().map(|page_addr| {
            PageRef::new_with_frame_id(page_addr.container_id, page_addr.page_id, self.frame_id())
        })
    }

    pub fn page(&self) -> &Page {
        self.page_ptr_ref()
    }

    pub fn eviction_score(&self) -> u64 {
        self.meta_ref().eviction_score()
    }

    pub fn update_eviction_score(&self, new_score: u64) {
        self.meta_ref().update_eviction_score(new_score);
    }

    pub fn try_upgrade(self, make_dirty: bool) -> Result<FrameWriteGuard, FrameReadGuard> {
        if self.latch().try_upgrade() {
            self.upgraded.store(true, Ordering::Relaxed);
            if make_dirty {
                self.dirty().store(true, Ordering::Release);
            }
            Ok(FrameWriteGuard {
                downgraded: AtomicBool::new(false),
                meta: self.meta,
                page: self.page,
                _marker: std::marker::PhantomData,
            })
        } else {
            Err(self)
        }
    }
}

impl Drop for FrameReadGuard {
    fn drop(&mut self) {
        if !self.upgraded.load(Ordering::Relaxed) {
            self.latch().release_shared();
        }
    }
}

impl Deref for FrameReadGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.page()
    }
}

impl Debug for FrameReadGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReadGuard")
            .field("page_addr", &self.page_addr())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

pub struct FrameWriteGuard {
    downgraded: AtomicBool,
    meta: NonNull<FrameMeta>,
    page: NonNull<Page>,
    _marker: std::marker::PhantomData<*mut ()>,
}

impl FrameWriteGuard {
    #[inline]
    fn meta_ref(&self) -> &FrameMeta {
        unsafe { self.meta.as_ref() }
    }

    #[inline]
    fn page_ptr_ref(&self) -> &Page {
        unsafe { self.page.as_ref() }
    }

    #[inline]
    fn page_mut_ref(&mut self) -> &mut Page {
        unsafe { self.page.as_mut() }
    }

    pub fn new(meta: *mut FrameMeta, page: *mut Page, make_dirty: bool) -> Self {
        let downgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        unsafe { meta.as_ref() }.latch.exclusive();
        if make_dirty {
            unsafe { meta.as_ref() }
                .is_dirty
                .store(true, Ordering::Release);
        }
        FrameWriteGuard {
            downgraded,
            meta,
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn try_new(meta: *mut FrameMeta, page: *mut Page, make_dirty: bool) -> Option<Self> {
        let downgraded = AtomicBool::new(false);
        let meta = NonNull::new(meta).expect("Meta pointer is null");
        let page = NonNull::new(page).expect("Page pointer is null");
        if unsafe { meta.as_ref() }.latch.try_exclusive() {
            if make_dirty {
                unsafe { meta.as_ref() }
                    .is_dirty
                    .store(true, Ordering::Release);
            }
            Some(FrameWriteGuard {
                downgraded,
                meta,
                page,
                _marker: std::marker::PhantomData,
            })
        } else {
            None
        }
    }

    pub fn frame_id(&self) -> u32 {
        self.meta_ref().frame_id
    }

    pub fn latch(&self) -> &RwLatch {
        &self.meta_ref().latch
    }

    pub fn dirty(&self) -> &AtomicBool {
        &self.meta_ref().is_dirty
    }

    pub fn eviction_score(&self) -> u64 {
        self.meta_ref().eviction_state.load(Ordering::Acquire)
    }

    pub fn update_eviction_score(&self, new_score: u64) {
        self.meta_ref()
            .eviction_state
            .store(new_score, Ordering::Release);
    }

    pub fn page_addr(&self) -> Option<PageAddr> {
        self.meta_ref().key()
    }

    pub fn set_page_addr(&self, page_addr: Option<PageAddr>) {
        self.meta_ref().set_key(page_addr)
    }

    pub fn page_ref(&self) -> Option<PageRef> {
        self.page_addr().map(|page_addr| {
            PageRef::new_with_frame_id(page_addr.container_id, page_addr.page_id, self.frame_id())
        })
    }

    pub fn page(&self) -> &Page {
        self.page_ptr_ref()
    }

    pub fn page_mut(&mut self) -> &mut Page {
        self.page_mut_ref()
    }

    pub fn downgrade(self) -> FrameReadGuard {
        self.latch().downgrade();
        self.downgraded.store(true, Ordering::Relaxed);
        FrameReadGuard {
            upgraded: AtomicBool::new(false),
            meta: self.meta,
            page: self.page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn clear(&mut self) {
        self.dirty().store(false, Ordering::Release);
        self.update_eviction_score(0);
        self.set_page_addr(None);
    }
}

impl Drop for FrameWriteGuard {
    fn drop(&mut self) {
        if !self.downgraded.load(Ordering::Relaxed) {
            self.latch().release_exclusive();
        }
    }
}

impl Deref for FrameWriteGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        self.page()
    }
}

impl DerefMut for FrameWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        self.page_mut()
    }
}

impl Debug for FrameWriteGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriteGuard")
            .field("page_addr", &self.page_addr())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

pub fn box_as_mut_ptr<T>(b: &mut Box<T>) -> *mut T {
    // This is a primitive deref, not going through `DerefMut`, and therefore not materializing
    // any references.
    // See Box::as_mut_ptr in the standard library. We do not use it here because it is
    // not in stable Rust yet.
    &raw mut **b
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::cell::UnsafeCell;
    use std::thread;

    fn make_meta_and_page(frame_id: usize) -> (Box<FrameMeta>, Box<Page>) {
        let page = Box::new(Page::new_empty());
        let meta = Box::new(FrameMeta::new(frame_id as u32));
        (meta, page)
    }

    #[test]
    fn test_default_buffer_frame() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert!(!guard.dirty().load(Ordering::Relaxed));
        assert!(guard.page_addr().is_none());
    }

    #[test]
    fn test_read_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert_eq!(guard.page_addr(), None);
        assert!(!guard.dirty().load(Ordering::Relaxed));
        guard.iter().all(|&x| x == 0);
        assert!(!guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_write_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let mut guard =
            FrameWriteGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page), true);
        assert_eq!(guard.page_addr(), None);
        assert!(guard.dirty().load(Ordering::Relaxed));
        guard.iter().all(|&x| x == 0);
        guard[0] = 1;
        assert_eq!(guard[0], 1);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_read_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard1 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        let guard2 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert_eq!(guard1.page_addr(), None);
        assert_eq!(guard2.page_addr(), None);
        assert!(!guard1.dirty().load(Ordering::Relaxed));
        assert!(!guard2.dirty().load(Ordering::Relaxed));
        guard1.iter().all(|&x| x == 0);
        guard2.iter().all(|&x| x == 0);
        assert!(!guard1.dirty().load(Ordering::Relaxed));
        assert!(!guard2.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_write_access() {
        let (meta_box, page_box) = make_meta_and_page(0);

        struct MetaWrap {
            meta: UnsafeCell<Box<FrameMeta>>,
        }

        unsafe impl Send for MetaWrap {}
        unsafe impl Sync for MetaWrap {}

        impl MetaWrap {
            #[allow(clippy::mut_from_ref)]
            fn meta_mut(&self) -> &mut Box<FrameMeta> {
                unsafe { &mut *self.meta.get() }
            }
        }

        struct PageWrap {
            page: UnsafeCell<Box<Page>>,
        }

        unsafe impl Send for PageWrap {}
        unsafe impl Sync for PageWrap {}

        impl PageWrap {
            #[allow(clippy::mut_from_ref)]
            fn page_mut(&self) -> &mut Box<Page> {
                unsafe { &mut *self.page.get() }
            }
        }

        let meta_wrap = MetaWrap {
            meta: UnsafeCell::new(meta_box),
        };
        let page_wrap = PageWrap {
            page: UnsafeCell::new(page_box),
        };

        thread::scope(|scope| {
            for _ in 0..3 {
                let meta_wrap = &meta_wrap;
                let page_wrap = &page_wrap;
                scope.spawn(move || {
                    let meta_ptr = box_as_mut_ptr(meta_wrap.meta_mut());
                    let page_ptr = box_as_mut_ptr(page_wrap.page_mut());
                    for _ in 0..80 {
                        let mut guard = FrameWriteGuard::new(meta_ptr, page_ptr, true);
                        guard[0] += 1;
                    }
                });
            }
        }); // threads joined here

        let meta_ptr = box_as_mut_ptr(meta_wrap.meta_mut());
        let page_ptr = box_as_mut_ptr(page_wrap.page_mut());
        let guard = FrameReadGuard::new(meta_ptr, page_ptr);
        assert_eq!(guard[0], 240);
    }

    #[test]
    fn test_upgrade_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        {
            // Upgrade read guard to write guard and modify the first element
            let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
            let mut guard = guard.try_upgrade(true).unwrap();
            assert_eq!(guard.page_addr(), None);
            assert!(guard.dirty().load(Ordering::Relaxed));
            guard.iter().all(|&x| x == 0);
            guard[0] = 1;
            assert_eq!(guard[0], 1);
            assert!(guard.dirty().load(Ordering::Relaxed));
        }
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert_eq!(guard[0], 1);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_downgrade_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let mut guard =
            FrameWriteGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page), true);
        guard[0] = 1;
        let guard = guard.downgrade();
        assert_eq!(guard[0], 1);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_upgrade_and_downgrade_access() {
        let (mut meta, mut page) = make_meta_and_page(0);
        // read -> write(dirty=false) -> read -> write(dirty=true) -> read
        let guard = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        assert!(!guard.dirty().load(Ordering::Relaxed));
        let mut guard = guard.try_upgrade(false).unwrap();
        guard[0] = 1;
        assert!(!guard.dirty().load(Ordering::Relaxed));
        let guard = guard.downgrade();
        assert!(!guard.dirty().load(Ordering::Relaxed));
        let mut guard = guard.try_upgrade(true).unwrap();
        guard[0] += 1;
        assert!(guard.dirty().load(Ordering::Relaxed));
        let guard = guard.downgrade();
        assert_eq!(guard[0], 2);
        assert!(guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_upgrade_failure() {
        let (mut meta, mut page) = make_meta_and_page(0);
        let guard1 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        let guard2 = FrameReadGuard::new(box_as_mut_ptr(&mut meta), box_as_mut_ptr(&mut page));
        match guard1.try_upgrade(true) {
            Ok(_) => panic!("Expected upgrade to fail"),
            Err(guard1) => {
                // Still holding the read guard of guard1
                assert!(guard2.try_upgrade(true).is_err());
                drop(guard1);
            }
        }
    }
}

// ============================================================================
// MEMORY POOL TRAIT AND UTILITIES
// ============================================================================

pub trait MemPool: Sync + Send {
    /// Create a new page for write.
    /// This function will allocate a new page in memory and return a FrameWriteGuard.
    /// In general, this function does not need to write the page to disk.
    /// Disk write will be handled when the page is evicted from the buffer pool.
    /// This function will not guarantee that the returned page is zeroed out but the
    /// page header will be initialized with a correct page id.
    /// The caller must initialize the page content before writing any data to disk.
    fn create_new_page_for_write(
        &self,
        container_id: ContainerId,
    ) -> Result<FrameWriteGuard, MemPoolStatus>;

    /// Create new pages for write.
    /// This function will allocate multiple new pages in memory and return a list of FrameWriteGuard.
    /// In general, this function does not need to write the page to disk.
    /// Disk write will be handled when the page is evicted from the buffer pool.
    /// This function will return available pages in the memory pool.
    /// It does not guarantee that the returned vector will have the requested number of pages.
    /// This function will not guarantee that the returned pages are zeroed out but the
    /// page headers will be initialized with correct page ids.
    /// The caller must initialize the pages content before writing any data to disk.
    fn create_new_pages_for_write(
        &self,
        container_id: ContainerId,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus>;

    /// Check if a page is cached in the memory pool.
    /// This function will return true if the page is in memory, false otherwise.
    /// There are no side effects of calling this function.
    /// That is, the page will not be loaded into memory.
    fn is_in_mem(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        frame_hint: Option<FrameId>,
    ) -> bool;

    /// Get a page for write.
    /// This function will return a FrameWriteGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_write(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        frame_hint: Option<FrameId>,
    ) -> Result<FrameWriteGuard, MemPoolStatus>;

    /// Get a page for read.
    /// This function will return a FrameReadGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_read(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        frame_hint: Option<FrameId>,
    ) -> Result<FrameReadGuard, MemPoolStatus>;

    /// Prefetch page.
    /// Load the page into memory so that read access will be faster.
    fn prefetch_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        frame_hint: Option<FrameId>,
    ) -> Result<(), MemPoolStatus>;

    /// Persist all the dirty pages to disk.
    /// This function will not deallocate the memory pool.
    /// This does not clear out the frames in the memory pool.
    fn flush_all(&self) -> Result<(), MemPoolStatus>;

    /// This function clears out all the frames in the memory pool.
    /// This will NOT deallocate the memory pool.
    /// After calling this function, pages will be read from disk when requested.
    /// Dirty pages will not be written to disk and the changes will be lost.
    /// Call flush_all before calling this function if you want to persist the dirty pages to disk.
    fn clear_all(&self) -> Result<(), MemPoolStatus>;

    /// Return the runtime statistics of the memory pool.
    fn stats(&self) -> MemoryStats;

    /// Reset the runtime statistics of the memory pool.
    fn reset_stats(&self);
}

// ============================================================================
// PUBLIC UTILITY FUNCTIONS
// ============================================================================

/// Buffer pool with persistent storage.
pub fn get_bp(num_frames: usize, cm: Arc<ContainerManager>) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        Arc::new(VMCachePool::<false>::new(num_frames, cm).unwrap())
    }
    #[cfg(feature = "bp_clock")]
    {
        Arc::new(BufferPoolClock::new(num_frames, cm).unwrap())
    }
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        Arc::new(BufferPoolLRU::new(num_frames, cm).unwrap())
    }
}

/// Buffer pool without persistent storage, used for testing.
pub fn get_test_bp(num_frames: usize) -> Arc<impl MemPool> {
    #[cfg(feature = "vmcache")]
    {
        get_test_vmcache::<false>(num_frames)
    }
    #[cfg(feature = "bp_clock")]
    {
        get_test_bp_clock(num_frames)
    }
    #[cfg(not(any(feature = "vmcache", feature = "bp_clock")))]
    {
        get_test_bp_lru(num_frames)
    }
}

pub fn get_test_vmcache<const IS_SMALL: bool>(num_frames: usize) -> Arc<VMCachePool<IS_SMALL>> {
    let base_dir = gen_random_pathname(Some("test_bp_vmcache_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(VMCachePool::<IS_SMALL>::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_lru(num_frames: usize) -> Arc<BufferPoolLRU> {
    let base_dir = gen_random_pathname(Some("test_bp_lru_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPoolLRU::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_lru_with_kpc(num_frames: usize) -> Arc<BufferPoolLRU> {
    let base_dir = gen_random_pathname(Some("test_bp_lru_kpc"));
    let cm = Arc::new(ContainerManager::new(base_dir, false, true).unwrap());
    Arc::new(BufferPoolLRU::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_clock(num_frames: usize) -> Arc<BufferPoolClock> {
    let base_dir = gen_random_pathname(Some("test_bp_clock_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPoolClock::new(num_frames, cm).unwrap())
}

pub fn get_in_mem_pool() -> Arc<InMemPool> {
    let cm = Arc::new(ContainerManager::new_in_memory());
    #[cfg(feature = "bp_clock")]
    {
        Arc::new(BufferPoolClock::new(IN_MEMORY_BP_NUM_FRAMES, cm).unwrap())
    }
    #[cfg(not(feature = "bp_clock"))]
    {
        Arc::new(BufferPoolLRU::new(IN_MEMORY_BP_NUM_FRAMES, cm).unwrap())
    }
}

// ============================================================================
// PRELUDE FOR COMMON EXPORTS
// ============================================================================

pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp_lru, BufferPoolLRU, ContainerId, DatabaseId, FrameId,
        FrameReadGuard, FrameWriteGuard, InMemPool, LocalContainerId, MemPool, MemPoolStatus,
        PageAddr, PageRef, INVALID_FRAME_ID,
    };
}
