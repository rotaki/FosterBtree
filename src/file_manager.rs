use std::sync::atomic::{AtomicU32, Ordering};

use crate::page::{Page, PageId};

pub type ContainerId = u16;

#[cfg(not(any(
    feature = "preadpwrite_sync",
    feature = "iouring_sync",
    feature = "iouring_async",
)))]
pub type FileManager = preadpwrite_sync::FileManager;

#[cfg(feature = "preadpwrite_sync")]
pub type FileManager = preadpwrite_sync::FileManager;
#[cfg(feature = "iouring_sync")]
pub type FileManager = iouring_sync::FileManager;
#[cfg(feature = "iouring_async")]
pub type FileManager = iouring_async::FileManager;

pub struct FileStats {
    pub buffered_read_count: AtomicU32,
    pub buffered_write_count: AtomicU32,
    pub direct_read_count: AtomicU32,
    pub direct_write_count: AtomicU32,
}

impl Clone for FileStats {
    fn clone(&self) -> Self {
        FileStats {
            buffered_read_count: AtomicU32::new(self.buffered_read_count.load(Ordering::Acquire)),
            buffered_write_count: AtomicU32::new(self.buffered_write_count.load(Ordering::Acquire)),
            direct_read_count: AtomicU32::new(self.direct_read_count.load(Ordering::Acquire)),
            direct_write_count: AtomicU32::new(self.direct_write_count.load(Ordering::Acquire)),
        }
    }
}

impl std::fmt::Display for FileStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Buffered read count: {}, Buffered write count: {}, Direct read count: {}, Direct write count: {}",
            self.buffered_read_count.load(Ordering::Acquire),
            self.buffered_write_count.load(Ordering::Acquire),
            self.direct_read_count.load(Ordering::Acquire),
            self.direct_write_count.load(Ordering::Acquire),
        )
    }
}

impl Default for FileStats {
    fn default() -> Self {
        Self::new()
    }
}

impl FileStats {
    pub fn new() -> Self {
        FileStats {
            buffered_read_count: AtomicU32::new(0),
            buffered_write_count: AtomicU32::new(0),
            direct_read_count: AtomicU32::new(0),
            direct_write_count: AtomicU32::new(0),
        }
    }

    pub fn read_count(&self) -> u32 {
        self.buffered_read_count.load(Ordering::Acquire)
            + self.direct_read_count.load(Ordering::Acquire)
    }

    pub fn inc_read_count(&self, _direct: bool) {
        #[cfg(feature = "stat")]
        {
            if _direct {
                self.direct_read_count.fetch_add(1, Ordering::AcqRel);
            } else {
                self.buffered_read_count.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub fn write_count(&self) -> u32 {
        self.buffered_write_count.load(Ordering::Acquire)
            + self.direct_write_count.load(Ordering::Acquire)
    }

    pub fn inc_write_count(&self, _direct: bool) {
        #[cfg(feature = "stat")]
        {
            if _direct {
                self.direct_write_count.fetch_add(1, Ordering::AcqRel);
            } else {
                self.buffered_write_count.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    #[allow(dead_code)]
    pub fn reset(&self) {
        self.buffered_read_count.store(0, Ordering::Release);
        self.buffered_write_count.store(0, Ordering::Release);
        self.direct_read_count.store(0, Ordering::Release);
        self.direct_write_count.store(0, Ordering::Release);
    }
}

pub trait FileManagerTrait: Send + Sync {
    fn num_pages(&self) -> usize;
    fn get_stats(&self) -> FileStats;
    #[allow(dead_code)]
    fn prefetch_page(&self, page_id: PageId) -> Result<(), std::io::Error>;
    fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), std::io::Error>;
    fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error>;
    fn flush(&self) -> Result<(), std::io::Error>;
}

#[allow(dead_code)]
pub mod preadpwrite_sync {
    use super::{ContainerId, FileManagerTrait, FileStats};
    #[allow(unused_imports)]
    use crate::log;
    use crate::log_trace;
    use crate::page::{Page, PageId, PAGE_SIZE};
    use libc::{c_void, fsync, pread, pwrite, O_DIRECT};
    use std::fs::{File, OpenOptions};
    use std::mem::MaybeUninit;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use std::path::PathBuf;

    pub struct FileManager {
        _path: PathBuf,
        _file: File, // When this file is dropped, the file descriptor (file_no) will be invalid.
        stats: FileStats,
        file_no: i32,
        direct: bool,
    }

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, std::io::Error> {
            std::fs::create_dir_all(&db_dir)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .custom_flags(O_DIRECT)
                .open(&path)?;
            let file_no = file.as_raw_fd();
            Ok(FileManager {
                _path: path,
                _file: file,
                stats: FileStats::new(),
                file_no,
                direct: true,
            })
        }

        // With kernel page cache
        pub fn with_kpc<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, std::io::Error> {
            std::fs::create_dir_all(&db_dir)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            let file_no = file.as_raw_fd();
            Ok(FileManager {
                _path: path,
                _file: file,
                stats: FileStats::new(),
                file_no,
                direct: false,
            })
        }
    }

    impl FileManagerTrait for FileManager {
        fn num_pages(&self) -> usize {
            // Allocate uninitialized memory for libc::stat
            let mut stat = MaybeUninit::<libc::stat>::uninit();

            // Call fstat with a pointer to our uninitialized stat buffer
            let ret = unsafe { libc::fstat(self.file_no, stat.as_mut_ptr()) };

            // Check for errors (fstat returns -1 on failure)
            if ret == -1 {
                return 0;
            }

            // Now that fstat has successfully written to the buffer,
            // we can assume it is initialized.
            let stat = unsafe { stat.assume_init() };

            // Use the file size (st_size) from stat, then compute pages.
            (stat.st_size as usize) / PAGE_SIZE
        }

        fn get_stats(&self) -> FileStats {
            self.stats.clone()
        }

        fn prefetch_page(&self, _page_id: PageId) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), std::io::Error> {
            self.stats.inc_read_count(self.direct);
            log_trace!("Reading page: {} from file: {:?}", page_id, self.path);
            unsafe {
                let ret = pread(
                    self.file_no,
                    page.get_raw_bytes_mut().as_mut_ptr() as *mut c_void,
                    PAGE_SIZE,
                    page_id as i64 * PAGE_SIZE as i64,
                );
                if ret != PAGE_SIZE as isize {
                    return Err(std::io::Error::last_os_error());
                }
            }
            debug_assert!(page.get_id() == page_id, "Page id mismatch");
            Ok(())
        }

        fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error> {
            self.stats.inc_write_count(self.direct);
            log_trace!("Writing page: {} to file: {:?}", page_id, self.path);
            debug_assert!(page.get_id() == page_id, "Page id mismatch");
            unsafe {
                let ret = pwrite(
                    self.file_no,
                    page.get_raw_bytes().as_ptr() as *const c_void,
                    PAGE_SIZE,
                    page_id as i64 * PAGE_SIZE as i64,
                );
                if ret != PAGE_SIZE as isize {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        }

        // With psync_direct, we don't need to flush.
        fn flush(&self) -> Result<(), std::io::Error> {
            if self.direct {
                Ok(())
            } else {
                unsafe {
                    let ret = fsync(self.file_no);
                    if ret != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            }
        }
    }
}

#[allow(dead_code)]
pub mod iouring_sync {
    use super::{ContainerId, FileStats};
    #[allow(unused_imports)]
    use crate::log;
    use crate::page::{Page, PageId, PAGE_SIZE};
    use io_uring::{opcode, types, IoUring};
    use libc::O_DIRECT;
    use std::cell::RefCell;
    use std::fs::{File, OpenOptions};
    use std::mem::MaybeUninit;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use std::path::PathBuf;

    enum UserData {
        Read(PageId),
        Write(PageId),
        Flush,
    }

    impl UserData {
        fn as_u64(&self) -> u64 {
            // Higher 32 bits are the operation type.
            // Lower 32 bits are the page id.
            match self {
                UserData::Read(page_id) => {
                    let upper_32 = 0;
                    let lower_32 = *page_id as u64;
                    (upper_32 << 32) | lower_32
                }
                UserData::Write(page_id) => {
                    let upper_32 = 1;
                    let lower_32 = *page_id as u64;
                    (upper_32 << 32) | lower_32
                }
                UserData::Flush => {
                    let upper_32 = 2;
                    let lower_32 = 0;
                    (upper_32 << 32) | lower_32
                }
            }
        }

        fn new_from_u64(data: u64) -> Self {
            let upper_32 = (data >> 32) as u32;
            let lower_32 = data as u32;
            match upper_32 {
                0 => UserData::Read(lower_32),
                1 => UserData::Write(lower_32),
                2 => UserData::Flush,
                _ => panic!("Invalid user data"),
            }
        }

        fn new_read(page_id: PageId) -> Self {
            UserData::Read(page_id)
        }

        fn new_write(page_id: PageId) -> Self {
            UserData::Write(page_id)
        }

        fn new_flush() -> Self {
            UserData::Flush
        }
    }

    thread_local! {
        static PER_THREAD_RING: RefCell<IoUring> = RefCell::new(IoUring::new(128).unwrap());
    }

    pub struct PerThreadRing {}

    impl PerThreadRing {
        pub fn new() -> Self {
            PerThreadRing {}
        }

        pub fn read_page(
            &self,
            fileno: i32,
            page_id: PageId,
            page: &mut Page,
        ) -> Result<(), std::io::Error> {
            let buf = page.get_raw_bytes_mut();
            let entry = opcode::Read::new(types::Fd(fileno), buf.as_mut_ptr(), buf.len() as _)
                .offset(page_id as u64 * PAGE_SIZE as u64)
                .build()
                .user_data(UserData::new_read(page_id).as_u64());
            PER_THREAD_RING.with(|ring| {
                let mut ring = ring.borrow_mut();
                unsafe {
                    ring.submission().push(&entry).expect("queue is full");
                }
                let res = ring.submit_and_wait(1)?; // Submit and wait for completion of 1 operation.
                assert_eq!(res, 1); // This is true if SQPOLL is disabled.
                if let Some(entry) = ring.completion().next() {
                    let completed = entry.user_data();
                    let user_data = UserData::new_from_u64(completed);
                    if let UserData::Read(completed_page_id) = user_data {
                        assert_eq!(completed_page_id, page_id);
                    } else {
                        panic!("Invalid user data");
                    }
                } else {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            })
        }

        pub fn write_page(
            &self,
            fileno: i32,
            page_id: PageId,
            page: &Page,
        ) -> Result<(), std::io::Error> {
            let buf = page.get_raw_bytes();
            let entry = opcode::Write::new(types::Fd(fileno), buf.as_ptr(), buf.len() as _)
                .offset(page_id as u64 * PAGE_SIZE as u64)
                .build()
                .user_data(UserData::new_write(page_id).as_u64());
            PER_THREAD_RING.with(|ring| {
                let mut ring = ring.borrow_mut();
                unsafe {
                    ring.submission().push(&entry).expect("queue is full");
                }
                let res = ring.submit_and_wait(1)?; // Submit and wait for completion of 1 operation.
                assert_eq!(res, 1); // This is true if SQPOLL is disabled.
                if let Some(entry) = ring.completion().next() {
                    let completed = entry.user_data();
                    let user_data = UserData::new_from_u64(completed);
                    if let UserData::Write(completed_page_id) = user_data {
                        assert_eq!(completed_page_id, page_id);
                    } else {
                        panic!("Invalid user data");
                    }
                } else {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            })
        }

        pub fn flush(&self, fileno: i32) -> Result<(), std::io::Error> {
            let entry = opcode::Fsync::new(types::Fd(fileno))
                .build()
                .user_data(UserData::new_flush().as_u64());
            PER_THREAD_RING.with(|ring| {
                let mut ring = ring.borrow_mut();
                unsafe {
                    ring.submission().push(&entry).expect("queue is full");
                }
                let res = ring.submit_and_wait(1)?; // Submit and wait for completion of 1 operation.
                assert_eq!(res, 1); // This is true if SQPOLL is disabled.
                if let Some(entry) = ring.completion().next() {
                    let completed = entry.user_data();
                    let user_data = UserData::new_from_u64(completed);
                    if let UserData::Flush = user_data {
                        // Do nothing
                    } else {
                        panic!("Invalid user data");
                    }
                } else {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            })
        }
    }

    pub struct FileManager {
        _path: PathBuf,
        _file: File,
        stats: FileStats,
        fileno: i32,
        direct: bool,
    }

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, std::io::Error> {
            std::fs::create_dir_all(&db_dir)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .custom_flags(O_DIRECT)
                .open(&path)?;
            let fileno = file.as_raw_fd();
            Ok(FileManager {
                _path: path,
                _file: file,
                stats: FileStats::new(),
                fileno,
                direct: true,
            })
        }

        // With kernel page cache. O_DIRECT is not set.
        pub fn with_kpc<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, std::io::Error> {
            std::fs::create_dir_all(&db_dir)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            let fileno = file.as_raw_fd();
            Ok(FileManager {
                _path: path,
                _file: file,
                stats: FileStats::new(),
                fileno,
                direct: false,
            })
        }

        pub fn get_stats(&self) -> FileStats {
            self.stats.clone()
        }

        pub fn num_pages(&self) -> usize {
            // Allocate uninitialized memory for libc::stat
            let mut stat = MaybeUninit::<libc::stat>::uninit();

            // Call fstat with a pointer to our uninitialized stat buffer
            let ret = unsafe { libc::fstat(self.fileno, stat.as_mut_ptr()) };

            // Check for errors (fstat returns -1 on failure)
            if ret == -1 {
                return 0;
            }

            // Now that fstat has successfully written to the buffer,
            // we can assume it is initialized.
            let stat = unsafe { stat.assume_init() };

            // Use the file size (st_size) from stat, then compute pages.
            (stat.st_size as usize) / PAGE_SIZE
        }

        pub fn prefetch_page(&self, _page_id: PageId) -> Result<(), std::io::Error> {
            Ok(())
        }

        pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), std::io::Error> {
            self.stats.inc_read_count(self.direct);
            PerThreadRing::new().read_page(self.fileno, page_id, page)
        }

        pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error> {
            self.stats.inc_write_count(self.direct);
            PerThreadRing::new().write_page(self.fileno, page_id, page)
        }

        pub fn flush(&self) -> Result<(), std::io::Error> {
            if !self.direct {
                Ok(())
            } else {
                PerThreadRing::new().flush(self.fileno)
            }
        }
    }
}

#[allow(dead_code)]
pub mod inmemory_async_simulator {
    use super::ContainerId;
    use crate::page::{Page, PageId};
    use std::sync::Mutex;
    const NUM_PAGE_BUFFER: usize = 128;

    pub struct FileManager {
        temp_buffers: Vec<Mutex<Page>>,
    }

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            _db_dir: P,
            _c_id: ContainerId,
        ) -> Result<Self, std::io::Error> {
            Ok(FileManager {
                temp_buffers: (0..NUM_PAGE_BUFFER)
                    .map(|_| Mutex::new(Page::new_empty()))
                    .collect(),
            })
        }

        pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error> {
            let idx = page_id as usize % NUM_PAGE_BUFFER;
            let mut temp_buffer = self.temp_buffers[idx].lock().unwrap();
            temp_buffer.copy(page);
            Ok(())
        }
    }
}

#[allow(dead_code)]
pub mod iouring_async {
    use super::{ContainerId, FileManagerTrait, FileStats};
    #[allow(unused_imports)]
    use crate::log;

    use crate::page::{Page, PageId, PAGE_SIZE};
    use io_uring::{opcode, types, IoUring};
    use libc::{iovec, O_DIRECT};
    use std::cell::UnsafeCell;
    use std::fs::{File, OpenOptions};
    use std::hash::{Hash, Hasher};
    use std::mem::MaybeUninit;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use std::path::PathBuf;

    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum UserData {
        Read(ContainerId, PageId),
        Write(ContainerId, PageId),
        Flush(ContainerId),
    }

    impl UserData {
        fn as_u64(&self) -> u64 {
            // Higher 16 bits are operation type.
            // Next 16 bits are container id.
            // Lower 32 bits are page id.
            match self {
                UserData::Read(c_id, page_id) => {
                    let upper_16 = 0;
                    let middle_16 = *c_id as u64;
                    let lower_32 = *page_id as u64;
                    (upper_16 << 48) | (middle_16 << 32) | lower_32
                }
                UserData::Write(fileno, page_id) => {
                    let upper_16 = 1;
                    let middle_16 = *fileno as u64;
                    let lower_32 = *page_id as u64;
                    (upper_16 << 48) | (middle_16 << 32) | lower_32
                }
                UserData::Flush(fileno) => {
                    let upper_16 = 2;
                    let middle_16 = *fileno as u64;
                    let lower_32 = 0;
                    (upper_16 << 48) | (middle_16 << 32) | lower_32
                }
            }
        }

        fn new_from_u64(data: u64) -> Self {
            let upper_16 = (data >> 48) as u16;
            let middle_16 = (data >> 32) as u16;
            let lower_32 = data as u32;
            match upper_16 {
                0 => UserData::Read(middle_16, lower_32),
                1 => UserData::Write(middle_16, lower_32),
                2 => UserData::Flush(middle_16),
                _ => panic!("Invalid user data"),
            }
        }

        fn new_read(c_id: ContainerId, page_id: PageId) -> Self {
            UserData::Read(c_id, page_id)
        }

        fn new_write(c_id: ContainerId, page_id: PageId) -> Self {
            UserData::Write(c_id, page_id)
        }

        fn new_flush(c_id: ContainerId) -> Self {
            UserData::Flush(c_id)
        }
    }

    struct PerPageHashRing {
        lock: Mutex<()>,
        ring: UnsafeCell<IoUring>,
        has_pending_write: UnsafeCell<bool>,
        temp_buffer_id: UnsafeCell<Option<(ContainerId, PageId)>>, // (container_id, page_id) of the page in the temp buffer. This is updated when a write is issued.
        temp_buffer: UnsafeCell<Page>,
        _io_vec: UnsafeCell<[iovec; 1]>,
    }

    unsafe impl Send for PerPageHashRing {}
    unsafe impl Sync for PerPageHashRing {}

    impl PerPageHashRing {
        pub fn new() -> Self {
            let ring = IoUring::builder().build(128).unwrap();
            let temp_buffer = UnsafeCell::new(Page::new_empty());
            let io_vec = UnsafeCell::new(unsafe {
                let io_vec: [iovec; 1] = [iovec {
                    iov_base: (*temp_buffer.get()).get_raw_bytes_mut().as_mut_ptr() as _,
                    iov_len: PAGE_SIZE as _,
                }];
                io_vec
            });
            // Register the file and the page buffer with the io_uring.
            let submitter = &ring.submitter();
            unsafe {
                submitter.register_buffers(&*io_vec.get()).unwrap();
            }
            PerPageHashRing {
                lock: Mutex::new(()),
                ring: UnsafeCell::new(ring),
                has_pending_write: UnsafeCell::new(false),
                temp_buffer_id: UnsafeCell::new(None),
                temp_buffer,
                _io_vec: io_vec,
            }
        }

        // Read a page from the file and copy it to the destination page.
        pub fn read(
            &self,
            fileno: i32,
            c_id: ContainerId,
            page_id: PageId,
            page: &mut Page,
        ) -> Result<(), std::io::Error> {
            let buf = page.get_raw_bytes_mut();
            let entry = opcode::Read::new(types::Fd(fileno), buf.as_mut_ptr(), buf.len() as _)
                .offset(page_id as u64 * PAGE_SIZE as u64)
                .build()
                .user_data(UserData::new_read(c_id, page_id).as_u64());

            let lock = self.lock.lock().unwrap();
            let ring = unsafe { &mut *self.ring.get() };
            let has_pending_writes = unsafe { &mut *self.has_pending_write.get() };
            let temp_buffer_id = unsafe { &mut *self.temp_buffer_id.get() };
            let temp_buffer = unsafe { &mut *self.temp_buffer.get() };

            // If the page_buffer contains the same page, we don't need to read it from disk.
            if temp_buffer_id.is_some() && temp_buffer_id.unwrap() == (c_id, page_id) {
                // Copy to the destination page.
                page.copy(temp_buffer);
                return Ok(()); // Return early.
            }

            unsafe {
                ring.submission().push(&entry).expect("queue is full");
            }
            // Submit and wait for completion of at least 1 operation.
            let ret = ring.submit_and_wait(1)?;
            assert_eq!(ret, 1); // This is true if SQPOLL is disabled.

            // Keep polling until the read is completed.
            loop {
                if let Some(entry) = ring.completion().next() {
                    let completed = entry.user_data();
                    let user_data = UserData::new_from_u64(completed);
                    match user_data {
                        UserData::Read(comp_c_id, comp_page_id) => {
                            assert_eq!(comp_c_id, c_id);
                            assert_eq!(comp_page_id, page_id);
                            assert_eq!(comp_page_id, page.get_id());
                            break;
                        }
                        UserData::Write(_fileno, _completion_page_id) => {
                            *has_pending_writes = false;
                        }
                        UserData::Flush(_) => {
                            // Do nothing.
                        }
                    }
                } else {
                    std::hint::spin_loop();
                }
            }

            drop(lock);

            Ok(())
        }

        pub fn write(
            &self,
            fileno: i32,
            c_id: ContainerId,
            page_id: PageId,
            page: &Page,
        ) -> Result<(), std::io::Error> {
            // 1. Create a write operation.
            let buf = unsafe { &*self.temp_buffer.get() }.get_raw_bytes();
            let entry = opcode::Write::new(types::Fd(fileno), buf.as_ptr(), buf.len() as _)
                .offset(page_id as u64 * PAGE_SIZE as u64)
                .build()
                .user_data(UserData::new_write(c_id, page_id).as_u64());

            let lock = self.lock.lock().unwrap();
            let ring = unsafe { &mut *self.ring.get() };
            let has_pending_write = unsafe { &mut *self.has_pending_write.get() };
            let temp_buffer_id = unsafe { &mut *self.temp_buffer_id.get() };
            let temp_buffer = unsafe { &mut *self.temp_buffer.get() };

            // If there are pending writes, poll the ring first.
            if *has_pending_write {
                // println!("waiting for pending write");
                loop {
                    if let Some(entry) = ring.completion().next() {
                        let completed = entry.user_data();
                        let user_data = UserData::new_from_u64(completed);
                        match user_data {
                            UserData::Read(..) => {
                                // Do nothing.
                                panic!("Read should be synchronous");
                            }
                            UserData::Write(comp_c_id, comp_page_id) => {
                                assert_eq!(comp_c_id, temp_buffer_id.unwrap().0);
                                assert_eq!(comp_page_id, temp_buffer_id.unwrap().1);
                                *has_pending_write = false;
                                break;
                            }
                            UserData::Flush(_) => {
                                // Do nothing.
                            }
                        }
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }

            // Now we can write the new page to the temp buffer.
            // 1. Copy the page to the temp buffer.
            temp_buffer.copy(page);
            *temp_buffer_id = Some((c_id, page_id)); // Update the temp buffer id.
                                                     // 2. Push the write operation to the ring.
            unsafe {
                ring.submission().push(&entry).expect("queue is full");
            }
            // 3. Submit.
            let _res = ring.submit()?; // Submit and wait for completion of 1 operation.
            assert_eq!(_res, 1); // This is true if SQPOLL is disabled.
            *has_pending_write = true;
            drop(lock);

            Ok(())
        }

        // Wait for the completion of the pending write and clear the temp buffer id.
        pub fn wait_and_clear(&self) -> Result<(), std::io::Error> {
            let lock = self.lock.lock().unwrap();
            let ring = unsafe { &mut *self.ring.get() };
            let has_pending_write = unsafe { &mut *self.has_pending_write.get() };
            let temp_buffer_id = unsafe { &mut *self.temp_buffer_id.get() };

            // If there are pending writes, poll the ring first.
            if *has_pending_write {
                loop {
                    if let Some(entry) = ring.completion().next() {
                        let completed = entry.user_data();
                        let user_data = UserData::new_from_u64(completed);
                        match user_data {
                            UserData::Read(..) => {
                                // Do nothing.
                                panic!("Read should be synchronous");
                            }
                            UserData::Write(comp_c_id, comp_page_id) => {
                                assert_eq!(comp_c_id, temp_buffer_id.unwrap().0);
                                assert_eq!(comp_page_id, temp_buffer_id.unwrap().1);
                                *has_pending_write = false;
                                break;
                            }
                            UserData::Flush(..) => {
                                // Do nothing.
                            }
                        }
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }

            temp_buffer_id.take(); // Clear the temp buffer id.

            drop(lock);

            Ok(())
        }

        // Flush is synchronous.
        // This function must be called when the kernel page cache is enabled.
        pub fn flush(&self, fileno: i32, c_id: ContainerId) -> Result<(), std::io::Error> {
            let entry = opcode::Fsync::new(types::Fd(fileno))
                .build()
                .user_data(UserData::new_flush(c_id).as_u64());

            let lock = self.lock.lock().unwrap();
            let ring = unsafe { &mut *self.ring.get() };
            let has_pending_write = unsafe { &mut *self.has_pending_write.get() };
            let temp_buffer_id = unsafe { &mut *self.temp_buffer_id.get() };

            // If there are pending writes, poll the ring first.
            if *has_pending_write {
                loop {
                    if let Some(entry) = ring.completion().next() {
                        let completed = entry.user_data();
                        let user_data = UserData::new_from_u64(completed);
                        match user_data {
                            UserData::Read(..) => {
                                // Do nothing.
                                panic!("Read should be synchronous");
                            }
                            UserData::Write(comp_c_id, comp_page_id) => {
                                assert_eq!(comp_c_id, temp_buffer_id.unwrap().0);
                                assert_eq!(comp_page_id, temp_buffer_id.unwrap().1);
                                *has_pending_write = false;
                                break;
                            }
                            UserData::Flush(..) => {
                                // Do nothing.
                            }
                        }
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }

            temp_buffer_id.take(); // Clear the temp buffer id.

            // 1. Push the flush operation to the ring.
            unsafe {
                ring.submission().push(&entry).expect("queue is full");
            }
            // 2. Submit.
            let _res = ring.submit_and_wait(1)?; // Submit and wait for completion of 1 operation.
            assert_eq!(_res, 1); // This is true if SQPOLL is disabled.

            // 3. Poll the ring for the completion of the flush operation.
            let entry = ring.completion().next().unwrap();
            let completed = entry.user_data();
            let user_data = UserData::new_from_u64(completed);
            assert_eq!(user_data, UserData::Flush(c_id));

            drop(lock);

            Ok(())
        }
    }

    pub struct GlobalRings {
        rings: Vec<PerPageHashRing>,
    }

    impl GlobalRings {
        pub fn new(num_rings: usize) -> Self {
            Self {
                rings: (0..num_rings).map(|_| PerPageHashRing::new()).collect(),
            }
        }

        fn get(&self, c_id: ContainerId, page_id: PageId) -> &PerPageHashRing {
            let index = self.hash(c_id, page_id);
            &self.rings[index]
        }

        fn hash(&self, c_id: ContainerId, page_id: PageId) -> usize {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            (c_id, page_id).hash(&mut hasher);
            hasher.finish() as usize % self.rings.len()
        }

        fn read_page(
            &self,
            fileno: i32,
            c_id: ContainerId,
            page_id: PageId,
            page: &mut Page,
        ) -> Result<(), std::io::Error> {
            // Compute a hash based on fileno and page id
            let ring = self.get(c_id, page_id);
            ring.read(fileno, c_id, page_id, page)
        }

        fn write_page(
            &self,
            fileno: i32,
            c_id: ContainerId,
            page_id: PageId,
            page: &Page,
        ) -> Result<(), std::io::Error> {
            let ring = self.get(c_id, page_id);
            ring.write(fileno, c_id, page_id, page)
        }

        pub fn flush(
            &self,
            direct: bool,
            fileno: i32,
            c_id: ContainerId,
        ) -> Result<(), std::io::Error> {
            if direct {
                for ring in &self.rings {
                    ring.wait_and_clear()?;
                }
            } else {
                let ring = self.get(c_id, 0);
                ring.flush(fileno, c_id)?;
            }

            Ok(())
        }
    }

    pub struct FileManager {
        _path: PathBuf,
        _file: File,
        stats: FileStats,
        fileno: i32,
        direct: bool,
        c_id: ContainerId,
        rings: Arc<GlobalRings>,
    }

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
            rings: Arc<GlobalRings>,
        ) -> Result<Self, std::io::Error> {
            std::fs::create_dir_all(&db_dir)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .custom_flags(O_DIRECT)
                .open(&path)?;
            let fileno = file.as_raw_fd();
            Ok(FileManager {
                _path: path,
                _file: file,
                stats: FileStats::new(),
                fileno,
                direct: true,
                c_id,
                rings,
            })
        }

        // With kernel page cache. O_DIRECT is not set.
        pub fn with_kpc<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
            rings: Arc<GlobalRings>,
        ) -> Result<Self, std::io::Error> {
            std::fs::create_dir_all(&db_dir)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            let fileno = file.as_raw_fd();
            Ok(FileManager {
                _path: path,
                _file: file,
                stats: FileStats::new(),
                fileno,
                direct: false,
                c_id,
                rings,
            })
        }
    }

    impl FileManagerTrait for FileManager {
        fn num_pages(&self) -> usize {
            // Allocate uninitialized memory for libc::stat
            let mut stat = MaybeUninit::<libc::stat>::uninit();

            // Call fstat with a pointer to our uninitialized stat buffer
            let ret = unsafe { libc::fstat(self.fileno, stat.as_mut_ptr()) };

            // Check for errors (fstat returns -1 on failure)
            if ret == -1 {
                return 0;
            }

            // Now that fstat has successfully written to the buffer,
            // we can assume it is initialized.
            let stat = unsafe { stat.assume_init() };

            // Use the file size (st_size) from stat, then compute pages.
            (stat.st_size as usize) / PAGE_SIZE
        }

        fn get_stats(&self) -> FileStats {
            self.stats.clone()
        }

        fn prefetch_page(&self, _page_id: PageId) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), std::io::Error> {
            self.stats.inc_read_count(self.direct);
            self.rings.read_page(self.fileno, self.c_id, page_id, page)
        }

        // Writes are asynchronous. It is not guaranteed that the write is completed when this function returns.
        // We guarantee that the write is completed before a new I/O operation is started on the same page.
        fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error> {
            self.stats.inc_write_count(self.direct);
            self.rings.write_page(self.fileno, self.c_id, page_id, page)
        }

        fn flush(&self) -> Result<(), std::io::Error> {
            self.rings.flush(self.direct, self.fileno, self.c_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use rstest::rstest;

    use crate::page::{Page, PageId};
    use crate::random::gen_random_permutation;

    use super::FileManagerTrait;

    fn get_preadpwrite_sync_fm(db_dir: &Path) -> impl FileManagerTrait {
        super::preadpwrite_sync::FileManager::new(db_dir, 0).unwrap()
    }

    fn get_iouring_async_fm(db_dir: &Path) -> impl FileManagerTrait {
        let rings = Arc::new(super::iouring_async::GlobalRings::new(128));
        super::iouring_async::FileManager::new(db_dir, 0, rings).unwrap()
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_page_write_read<T: FileManagerTrait>(#[case] file_manager_gen: fn(&Path) -> T) {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = file_manager_gen(temp_path.path());
        let mut page = Page::new_empty();

        let page_id = 0;
        page.set_id(page_id);

        let data = b"Hello, World!";
        page[0..data.len()].copy_from_slice(data);

        file_manager.write_page(page_id, &page).unwrap();

        let mut read_page = Page::new_empty();
        file_manager.read_page(page_id, &mut read_page).unwrap();

        assert_eq!(&read_page[0..data.len()], data);
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_prefetch_page<T: FileManagerTrait>(#[case] file_manager_gen: fn(&Path) -> T) {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = file_manager_gen(temp_path.path());

        let num_pages = 1000;
        let page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        // Write the pages
        for i in 0..num_pages {
            let mut page = Page::new_empty();
            page.set_id(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            file_manager.write_page(i, &page).unwrap();
        }

        for i in gen_random_permutation(page_id_vec) {
            file_manager.prefetch_page(i).unwrap();
            let mut read_page = Page::new_empty();
            file_manager.read_page(i, &mut read_page).unwrap();
        }
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_page_write_read_sequential<T: FileManagerTrait>(
        #[case] file_manager_gen: fn(&Path) -> T,
    ) {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = file_manager_gen(temp_path.path());

        let num_pages = 1000;

        for i in 0..num_pages {
            let mut page = Page::new_empty();
            page.set_id(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            file_manager.write_page(i, &page).unwrap();
        }

        for i in 0..num_pages {
            let mut read_page = Page::new_empty();
            file_manager.read_page(i, &mut read_page).unwrap();

            let data = format!("Hello, World! {}", i);
            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_page_write_read_random<T: FileManagerTrait>(#[case] file_manager_gen: fn(&Path) -> T) {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = file_manager_gen(temp_path.path());

        let num_pages = 1000;
        let page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        // Write the page in random order
        for i in gen_random_permutation(page_id_vec.clone()) {
            let mut page = Page::new_empty();
            page.set_id(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            file_manager.write_page(i, &page).unwrap();
        }

        // Read the page in random order
        for i in gen_random_permutation(page_id_vec) {
            let mut read_page = Page::new_empty();
            file_manager.read_page(i, &mut read_page).unwrap();

            let data = format!("Hello, World! {}", i);
            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_page_write_read_interleave<T: FileManagerTrait>(
        #[case] file_manager_gen: fn(&Path) -> T,
    ) {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = file_manager_gen(temp_path.path());

        let num_pages = 1000;
        let page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        // Write the page in random order
        for i in gen_random_permutation(page_id_vec.clone()) {
            let mut page = Page::new_empty();
            page.set_id(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            file_manager.write_page(i, &page).unwrap();

            let mut read_page = Page::new_empty();
            file_manager.read_page(i, &mut read_page).unwrap();

            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_file_flush<T: FileManagerTrait>(#[case] file_manager_gen: fn(&Path) -> T) {
        // Create two file managers with the same path.
        // Issue multiple write operations to one of the file managers.
        // Check if the other file manager can read the pages.

        let temp_path = tempfile::tempdir().unwrap();
        let file_manager1 = file_manager_gen(temp_path.path());
        let file_manager2 = file_manager_gen(temp_path.path());

        let num_pages = 2;
        let page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        // Write the page in random order
        for i in gen_random_permutation(page_id_vec.clone()) {
            let mut page = Page::new_empty();
            page.set_id(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            file_manager1.write_page(i, &page).unwrap();
        }

        file_manager1.flush().unwrap(); // If we remove this line, the test is likely to fail.

        // Read the page in random order
        for i in gen_random_permutation(page_id_vec) {
            let mut read_page = Page::new_empty();
            file_manager2.read_page(i, &mut read_page).unwrap();

            let data = format!("Hello, World! {}", i);
            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[rstest]
    #[case::preadpwrite(get_preadpwrite_sync_fm)]
    #[case::iouring_async(get_iouring_async_fm)]
    fn test_concurrent_read_write_file<T: FileManagerTrait>(
        #[case] file_manager_gen: fn(&Path) -> T,
    ) {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = file_manager_gen(temp_path.path());

        let num_pages = 1000;
        let page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        let num_threads = 2;

        // Partition the page_id_vec into num_threads partitions.
        let partitions: Vec<Vec<PageId>> = {
            let mut partitions = vec![];
            let partition_size = num_pages / num_threads;
            for i in 0..num_threads {
                let start = (i * partition_size) as usize;
                let end = if i == num_threads - 1 {
                    num_pages
                } else {
                    (i + 1) * partition_size
                } as usize;
                partitions.push(page_id_vec[start..end].to_vec());
            }
            partitions
        };

        std::thread::scope(|s| {
            for partition in partitions.clone() {
                s.spawn(|| {
                    for i in gen_random_permutation(partition) {
                        let mut page = Page::new_empty();
                        page.set_id(i);

                        let data = format!("Hello, World! {}", i);
                        page[0..data.len()].copy_from_slice(data.as_bytes());

                        file_manager.write_page(i, &page).unwrap();
                    }
                });
            }
        });

        // Issue concurrent read
        std::thread::scope(|s| {
            for partition in partitions {
                s.spawn(|| {
                    for i in gen_random_permutation(partition) {
                        let mut read_page = Page::new_empty();
                        file_manager.read_page(i, &mut read_page).unwrap();

                        let data = format!("Hello, World! {}", i);
                        assert_eq!(&read_page[0..data.len()], data.as_bytes());
                    }
                });
            }
        });
    }
}
