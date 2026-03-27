use std::{collections::BTreeMap, fmt};

use super::frame_guards::{FrameReadGuard, FrameWriteGuard};

use crate::page::PageId;

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
            MemPoolStatus::FileManagerError(s) => s.fmt(f),
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

#[cfg(test)]
mod tests {
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
