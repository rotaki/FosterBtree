use std::{
    collections::BTreeMap,
    fmt,
    hash::{Hash, Hasher},
};

use super::{
    eviction_policy::EvictionPolicy,
    frame_guards::{FrameReadGuard, FrameWriteGuard},
};

use crate::page::PageId;

pub type DatabaseId = u16;
pub type ContainerId = u16;

/*------------------ low-level representation (unchanged) ------------------*/

#[repr(C)]
#[derive(Copy, Clone)]
struct Parts {
    db_id: DatabaseId,
    c_id: ContainerId,
}

#[repr(C)]
#[derive(Copy, Clone)]
union Repr {
    parts: Parts,
    packed: u32,
}

/*---------------------------- public newtype ------------------------------*/

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct ContainerKey {
    repr: Repr,
}

impl ContainerKey {
    #[inline]
    pub const fn new(db_id: DatabaseId, c_id: ContainerId) -> Self {
        Self {
            repr: Repr {
                parts: Parts { db_id, c_id },
            },
        }
    }

    #[inline]
    pub const fn from_u32(raw: u32) -> Self {
        Self {
            repr: Repr { packed: raw },
        }
    }

    /// Safe projection to the packed form.
    #[inline]
    pub const fn as_u32(self) -> u32 {
        unsafe { self.repr.packed }
    }

    #[inline]
    pub const fn db_id(self) -> DatabaseId {
        unsafe { self.repr.parts.db_id }
    }
    #[inline]
    pub const fn c_id(self) -> ContainerId {
        unsafe { self.repr.parts.c_id }
    }
}

/*------------------ manual equality & ordering impls ----------------------*/

impl PartialEq for ContainerKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // Either of the following is fine:
        //   self.as_u32() == other.as_u32()
        // or (endianness-independent):
        (self.db_id(), self.c_id()) == (other.db_id(), other.c_id())
    }
}
impl Eq for ContainerKey {}

impl PartialOrd for ContainerKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ContainerKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Lexicographic on (db_id, c_id); endian-safe.
        (self.db_id(), self.c_id()).cmp(&(other.db_id(), other.c_id()))
    }
}

/*---------------------- the rest of the boilerplate ------------------------*/

impl Hash for ContainerKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.as_u32())
    }
}

impl fmt::Debug for ContainerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ContainerKey")
            .field(&self.db_id())
            .field(&self.c_id())
            .finish()
    }
}
impl fmt::Display for ContainerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(db:{}, c:{})", self.db_id(), self.c_id())
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
        bytes.extend_from_slice(&self.p_key.c_key.as_u32().to_be_bytes()); // 4 bytes
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
    pub bp_num_frames_per_container: BTreeMap<ContainerKey, i64>, // Number of pages of each container in BP

    // Disk stats
    pub disk_created: usize, // Total number of pages created (DISK)
    pub disk_read: usize,    // Total number of pages read (DISK)
    pub disk_write: usize,   // Total number of pages written (DISK)
    pub disk_io_per_container: BTreeMap<ContainerKey, (i64, i64, i64)>, // Number of pages created, read, and written for each container
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
        for (c_key, num_pages) in &self.bp_num_frames_per_container {
            writeln!(f, "    {}: {}", c_key, num_pages)?;
        }
        writeln!(f, "Disk stats:")?;
        writeln!(f, "  Number of pages created: {}", self.disk_created)?;
        writeln!(f, "  Number of pages read: {}", self.disk_read)?;
        writeln!(f, "  Number of pages written: {}", self.disk_write)?;
        writeln!(f, "  Number of pages read and written for each container:")?;
        for (c_key, (num_created, num_read, num_write)) in &self.disk_io_per_container {
            writeln!(
                f,
                "    {}: created={}, read={}, written={}",
                c_key, num_created, num_read, num_write
            )?;
        }
        Ok(())
    }
}

pub trait MemPool: Sync + Send {
    type EP: EvictionPolicy;

    /// Create a container.
    /// A container is basically a file in the file system if a disk-based storage is used.
    /// If an in-memory storage is used, a container is a logical separation of pages.
    /// This function will register a container in the memory pool.
    /// If a page write is requested to a unregistered container, a container will be lazily created
    /// and the page will be written to the container file.
    /// Therefore, calling this function is not mandatory unless you want to ensure that
    /// the container is created or you want to create a temporary container.
    /// Creation of a already created container will be ignored.
    fn create_container(&self, c_key: ContainerKey, is_temp: bool) -> Result<(), MemPoolStatus>;

    /// Drop a container.
    /// This only makes the container temporary, that is, it ensures that future write
    /// requests to the container will be ignored. This does not guarantee that the pages
    /// of the container are deleted from the disk or memory.
    /// However, the page eviction policy could use this information to guide
    /// the eviction of the pages in the memory pool as evicting pages of a temporary container
    /// is virtually free.
    fn drop_container(&self, c_key: ContainerKey) -> Result<(), MemPoolStatus>;

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
    ) -> Result<FrameWriteGuard<Self::EP>, MemPoolStatus>;

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
    ) -> Result<Vec<FrameWriteGuard<Self::EP>>, MemPoolStatus>;

    /// Check if a page is cached in the memory pool.
    /// This function will return true if the page is in memory, false otherwise.
    /// There are no side effects of calling this function.
    /// That is, the page will not be loaded into memory.
    fn is_in_mem(&self, key: PageFrameKey) -> bool;

    /// Get the list of page frame keys for a container.
    /// This function will return a list of page frame keys for the container
    /// that are currently in the memory pool.
    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey>;

    /// Get a page for write.
    /// This function will return a FrameWriteGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_write(
        &self,
        key: PageFrameKey,
    ) -> Result<FrameWriteGuard<Self::EP>, MemPoolStatus>;

    /// Get a page for read.
    /// This function will return a FrameReadGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_read(
        &self,
        key: PageFrameKey,
    ) -> Result<FrameReadGuard<Self::EP>, MemPoolStatus>;

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
    ///
    /// # Safety
    ///
    /// The caller must ensure that the memory pool is not in use when calling this function.
    unsafe fn stats(&self) -> MemoryStats;

    /// Reset the runtime statistics of the memory pool.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the memory pool is not in use when calling this function.
    unsafe fn reset_stats(&self);
}
