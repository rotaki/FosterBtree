use std::collections::BTreeMap;

use super::buffer_frame::{FrameReadGuard, FrameWriteGuard};

use crate::page::PageId;

pub type DatabaseId = u16;
pub type ContainerId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContainerKey {
    pub db_id: DatabaseId,
    pub c_id: ContainerId,
}

impl ContainerKey {
    pub fn new(db_id: DatabaseId, c_id: ContainerId) -> Self {
        ContainerKey { db_id, c_id }
    }
}

impl std::fmt::Display for ContainerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(db:{}, c:{})", self.db_id, self.c_id)
    }
}

/// Page key is used to determine a specific page in a container in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageKey {
    pub c_key: ContainerKey,
    pub page_id: PageId,
}

impl PageKey {
    pub fn new(c_key: ContainerKey, page_id: PageId) -> Self {
        PageKey { c_key, page_id }
    }
}

impl std::fmt::Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, p:{})", self.c_key, self.page_id)
    }
}

/// Page frame key is used to access a page in the buffer pool.
/// It contains not only the page key but also the frame id in the buffer pool.
/// The frame id is used as a hint to access the page in O(1) time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageFrameKey {
    p_key: PageKey,
    frame_id: u32, // Frame id in the buffer pool
}

impl PageFrameKey {
    pub fn new(c_key: ContainerKey, page_id: PageId) -> Self {
        PageFrameKey {
            p_key: PageKey::new(c_key, page_id),
            frame_id: u32::MAX,
        }
    }

    pub fn new_with_frame_id(c_key: ContainerKey, page_id: PageId, frame_id: u32) -> Self {
        PageFrameKey {
            p_key: PageKey::new(c_key, page_id),
            frame_id,
        }
    }

    pub fn p_key(&self) -> PageKey {
        self.p_key
    }

    pub fn frame_id(&self) -> u32 {
        self.frame_id
    }

    pub fn set_frame_id(&mut self, frame_id: u32) {
        self.frame_id = frame_id;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.p_key.c_key.db_id.to_be_bytes()); // 2 bytes
        bytes.extend_from_slice(&self.p_key.c_key.c_id.to_be_bytes()); // 2 bytes
        bytes.extend_from_slice(&self.p_key.page_id.to_be_bytes()); // 4 bytes
        bytes.extend_from_slice(&self.frame_id.to_be_bytes()); // 4 bytes
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let db_id = u16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let c_id = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let page_id = PageId::from_be_bytes(bytes[4..8].try_into().unwrap());
        let frame_id = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        PageFrameKey {
            p_key: PageKey::new(ContainerKey::new(db_id, c_id), page_id),
            frame_id,
        }
    }
}

impl std::fmt::Display for PageFrameKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, f:{})", self.p_key, self.frame_id)
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
    pub bp_num_frames_per_container: BTreeMap<ContainerKey, i64>, // Number of pages of each container in BP

    // Disk stats
    pub disk_read: usize,  // Total number of pages read (DISK)
    pub disk_write: usize, // Total number of pages written (DISK)
    pub disk_io_per_container: BTreeMap<ContainerKey, (i64, i64)>, // Number of pages read and written for each container
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
            disk_read: self.disk_read - previous.disk_read,
            disk_write: self.disk_write - previous.disk_write,
            disk_io_per_container: self
                .disk_io_per_container
                .iter()
                .map(|(k, v)| {
                    let prev = previous.disk_io_per_container.get(k).unwrap_or(&(0, 0));
                    (*k, (v.0 - prev.0, v.1 - prev.1))
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
        for (c_key, num_pages) in &self.bp_num_frames_per_container {
            writeln!(f, "    {}: {}", c_key, num_pages)?;
        }
        writeln!(f, "Disk stats:")?;
        writeln!(f, "  Number of pages read: {}", self.disk_read)?;
        writeln!(f, "  Number of pages written: {}", self.disk_write)?;
        writeln!(f, "  Number of pages read and written for each container:")?;
        for (c_key, (num_read, num_write)) in &self.disk_io_per_container {
            writeln!(f, "    {}: read={}, write={}", c_key, num_read, num_write)?;
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
        c_key: ContainerKey,
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
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus>;

    /// Get a page for write.
    /// This function will return a FrameWriteGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus>;

    /// Get a page for read.
    /// This function will return a FrameReadGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus>;

    /// Prefetch page
    /// Load the page into memory so that read access will be faster.
    fn prefetch_page(&self, key: PageFrameKey) -> Result<(), MemPoolStatus>;

    /// Persist all the dirty pages to disk.
    /// This function will not deallocate the memory pool.
    /// This does not clear out the frames in the memory pool.
    fn flush_all(&self) -> Result<(), MemPoolStatus>;

    /// Persist all the dirty pages to disk and reset the memory pool.
    /// This function will not deallocate the memory pool but
    /// clears out all the frames in the memory pool.
    /// After calling this function, pages will be read from disk when requested.
    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus>;

    /// Clear the dirty flags of all the pages in the memory pool.
    /// This function will not deallocate the memory pool.
    /// This does not clear out the frames in the memory pool.
    /// Dirty pages will not be written to disk.
    /// This function is used for experiments to avoid writing pages to disk.
    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus>;

    /// Tell the memory pool that a page in the frame should be evicted as soon as possible.
    /// This function will not evict the page immediately.
    /// This function is used as a hint to the memory pool to evict the page when possible.
    fn fast_evict(&self, frame_id: u32) -> Result<(), MemPoolStatus>;

    /// Return the runtime statistics of the memory pool.
    fn stats(&self) -> MemoryStats;

    /// Reset the runtime statistics of the memory pool.
    fn reset_stats(&self);
}
