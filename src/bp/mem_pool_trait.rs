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
    pub num_frames_in_mem: usize,
    pub new_page_created: usize,    // Number of new pages created
    pub read_page_from_disk: usize, // Number of pages read
    pub write_page_to_disk: usize,  // Number of pages written
    pub containers: BTreeMap<ContainerKey, i32>, // Number of pages of each container in memory
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStats {
    pub fn new() -> Self {
        MemoryStats {
            num_frames_in_mem: 0,
            new_page_created: 0,
            read_page_from_disk: 0,
            write_page_to_disk: 0,
            containers: BTreeMap::new(),
        }
    }

    pub fn diff(&self, previous: &MemoryStats) -> MemoryStats {
        assert_eq!(self.num_frames_in_mem, previous.num_frames_in_mem);
        MemoryStats {
            num_frames_in_mem: self.num_frames_in_mem,
            new_page_created: self.new_page_created - previous.new_page_created,
            read_page_from_disk: self.read_page_from_disk - previous.read_page_from_disk,
            write_page_to_disk: self.write_page_to_disk - previous.write_page_to_disk,
            containers: self.containers.clone(),
        }
    }
}

impl std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Set to "N/A" if usize::MAX, otherwise format as a right-aligned string
        let format_count = |count| {
            if count == usize::MAX {
                "N/A".to_string()
            } else {
                format!("{}", count)
            }
        };

        let num_frames_in_mem = format_count(self.num_frames_in_mem);
        let new_count = format_count(self.new_page_created);
        let read_count = format_count(self.read_page_from_disk);
        let write_count = format_count(self.write_page_to_disk);

        write!(
            f,
            "Frames in mem: {}, New: {}, Read From Disk: {}, Write To Disk: {}, Containers: {:?}",
            num_frames_in_mem, new_count, read_count, write_count, self.containers
        )
    }
}

pub trait MemPool: Sync + Send {
    /// Create a new page for write.
    /// This function will allocate a new page in memory and return a FrameWriteGuard.
    /// In general, this function does not need to write the page to disk.
    /// Disk write will be handled when the page is evicted from the buffer pool.
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
