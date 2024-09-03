use std::io;

#[derive(Debug, PartialEq)]
pub enum FMError {
    DirCreate,
    Open,
    Seek,
    Read,
    Write,
    Flush,
    Other(String),
}

impl std::fmt::Display for FMError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FMError::DirCreate => write!(f, "[FM] Error creating directory"),
            FMError::Open => write!(f, "[FM] Error opening file"),
            FMError::Seek => write!(f, "[FM] Error seeking in file"),
            FMError::Read => write!(f, "[FM] Error reading from file"),
            FMError::Write => write!(f, "[FM] Error writing to file"),
            FMError::Flush => write!(f, "[FM] Error flushing file"),
            FMError::Other(e) => write!(f, "[FM] Other IO error: {}", e),
        }
    }
}

impl From<io::Error> for FMError {
    fn from(err: io::Error) -> Self {
        FMError::Other(err.to_string())
    }
}

#[cfg(not(any(feature = "async_write", feature = "new_async_write")))]
pub type FileManager = sync_write::FileManager;
#[cfg(feature = "async_write")]
pub type FileManager = async_write::FileManager;
#[cfg(feature = "new_async_write")]
pub type FileManager = new_async_write::FileManager;

#[cfg(not(any(feature = "async_write", feature = "new_async_write")))]
pub mod sync_write {
    use super::FMError;
    use crate::bp::ContainerId;
    #[allow(unused_imports)]
    use crate::log;
    use crate::log_info;
    use crate::page::{Page, PageId, PAGE_SIZE};
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    pub struct FileManager {
        _path: PathBuf,
        file: Mutex<File>,
        num_pages: AtomicU32,
        io_count: (AtomicU32, AtomicU32), // (Read, Write)
    }

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, FMError> {
            std::fs::create_dir_all(&db_dir).map_err(|_| FMError::DirCreate)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|_| FMError::Open)?;
            let num_pages = file.metadata().unwrap().len() as usize / PAGE_SIZE;
            Ok(FileManager {
                _path: path,
                file: Mutex::new(file),
                num_pages: AtomicU32::new(num_pages as PageId),
                io_count: (AtomicU32::new(0), AtomicU32::new(0)),
            })
        }

        pub fn fetch_add_page_id(&self) -> PageId {
            self.num_pages.fetch_add(1, Ordering::AcqRel)
        }

        pub fn fetch_sub_page_id(&self) -> PageId {
            self.num_pages.fetch_sub(1, Ordering::AcqRel)
        }

        pub fn get_stats(&self) -> String {
            let _guard = self.file.lock().unwrap();
            format!(
                "Num pages: {}, Read count: {}, Write count: {}",
                self.num_pages.load(Ordering::Acquire),
                self.io_count.0.load(Ordering::Acquire),
                self.io_count.1.load(Ordering::Acquire)
            )
        }

        pub fn reset_stats(&self) {
            let _guard = self.file.lock().unwrap();
            self.io_count.0.store(0, Ordering::Release);
            self.io_count.1.store(0, Ordering::Release);
        }

        pub fn prefetch_page(&self, _page_id: PageId) -> Result<(), FMError> {
            Ok(())
        }

        pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), FMError> {
            let mut file = self.file.lock().unwrap();
            self.io_count.0.fetch_add(1, Ordering::AcqRel);
            log_info!("Reading page: {} from file: {:?}", page_id, self.path);
            file.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
                .map_err(|_| FMError::Seek)?;
            file.read_exact(page.get_raw_bytes_mut())
                .map_err(|_| FMError::Read)?;
            debug_assert!(page.get_id() == page_id, "Page id mismatch");
            Ok(())
        }

        pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), FMError> {
            let mut file = self.file.lock().unwrap();
            log_info!("Writing page: {} to file: {:?}", page_id, self.path);
            self.io_count.1.fetch_add(1, Ordering::AcqRel);
            debug_assert!(page.get_id() == page_id, "Page id mismatch");
            file.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))
                .map_err(|_| FMError::Seek)?;
            file.write_all(page.get_raw_bytes())
                .map_err(|_| FMError::Write)?;
            Ok(())
        }

        pub fn flush(&self) -> Result<(), FMError> {
            let mut file = self.file.lock().unwrap();
            log_info!("Flushing file: {:?}", self.path);
            file.flush().map_err(|_| FMError::Flush)
        }
    }
}

#[cfg(feature = "async_write")]
pub mod async_write {
    use super::FMError;
    use crate::bp::ContainerId;
    #[allow(unused_imports)]
    use crate::log;
    use crate::page::{Page, PageId, PAGE_SIZE};
    use crate::{log_debug, log_info};
    use std::cell::UnsafeCell;
    use std::fs::{File, OpenOptions};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    use io_uring::{opcode, types, IoUring};
    use libc::iovec;
    use std::hash::{Hash, Hasher};
    use std::os::unix::io::AsRawFd;

    const PAGE_BUFFER_SIZE: usize = 128;

    pub struct FileManager {
        #[allow(dead_code)]
        path: PathBuf,
        num_pages: AtomicU32,
        file_inner: Mutex<FileManagerInner>,
    }

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, FMError> {
            std::fs::create_dir_all(&db_dir).map_err(|_| FMError::DirCreate)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(|_| FMError::Open)?;
            let num_pages = file.metadata().unwrap().len() as usize / PAGE_SIZE;
            let file_inner = FileManagerInner::new(file)?;
            Ok(FileManager {
                path,
                num_pages: AtomicU32::new(num_pages as PageId),
                file_inner: Mutex::new(file_inner),
            })
        }

        pub fn fetch_add_page_id(&self) -> PageId {
            self.num_pages.fetch_add(1, Ordering::AcqRel)
        }

        pub fn fetch_sub_page_id(&self) -> PageId {
            self.num_pages.fetch_sub(1, Ordering::AcqRel)
        }

        #[allow(dead_code)]
        pub fn get_stats(&self) -> String {
            #[cfg(feature = "stat")]
            {
                let stats = GLOBAL_FILE_STAT.lock().unwrap();
                LOCAL_STAT.with(|local_stat| {
                    stats.merge(&local_stat.stat);
                    local_stat.stat.clear();
                });
                return stats.to_string();
            }
            #[cfg(not(feature = "stat"))]
            {
                "Stat is disabled".to_string()
            }
        }

        pub fn prefetch_page(&self, page_id: PageId) -> Result<(), FMError> {
            Ok(())
        }

        pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), FMError> {
            log_trace!("Reading page: {} from file: {:?}", page_id, self.path);
            let mut file_inner = self.file_inner.lock().unwrap();
            file_inner.read_page(page_id, page)
        }

        pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), FMError> {
            log_trace!("Writing page: {} to file: {:?}", page_id, self.path);
            let mut file_inner = self.file_inner.lock().unwrap();
            file_inner.write_page(page_id, page)
        }

        pub fn flush(&self) -> Result<(), FMError> {
            log_trace!("Flushing file: {:?}", self.path);
            let mut file_inner = self.file_inner.lock().unwrap();
            file_inner.flush_page()
        }
    }

    #[cfg(feature = "stat")]
    mod stat {
        use super::*;
        use lazy_static::lazy_static;
        pub struct FileStat {
            read: UnsafeCell<[usize; 11]>, // Number of reads completed. The index is the wait count.
            write: UnsafeCell<[usize; 11]>, // Number of writes completed. The index is the wait count.
        }

        impl FileStat {
            pub fn new() -> Self {
                FileStat {
                    read: UnsafeCell::new([0; 11]),
                    write: UnsafeCell::new([0; 11]),
                }
            }

            pub fn to_string(&self) -> String {
                let read = unsafe { &*self.read.get() };
                let write = unsafe { &*self.write.get() };
                let mut result = String::new();
                result.push_str("File page async read stats: \n");
                let mut sep = "";
                let total_count = read.iter().sum::<usize>();
                let mut cumulative_count = 0;
                for i in 0..11 {
                    result.push_str(sep);
                    cumulative_count += read[i];
                    if i == 10 {
                        result.push_str(&format!(
                            "{:2}+: {:6} (p: {:6.2}%, c: {:6})",
                            i,
                            read[i],
                            read[i] as f64 / total_count as f64 * 100.0,
                            cumulative_count
                        ));
                    } else {
                        result.push_str(&format!(
                            "{:3}: {:6} (p: {:6.2}%, c: {:6})",
                            i,
                            read[i],
                            read[i] as f64 / total_count as f64 * 100.0,
                            cumulative_count
                        ));
                    }
                    sep = "\n";
                }
                result.push_str("\n\n");
                result.push_str("File page async write stats: \n");
                sep = "";
                let total_count = write.iter().sum::<usize>();
                cumulative_count = 0;
                for i in 0..11 {
                    result.push_str(sep);
                    cumulative_count += write[i];
                    if i == 10 {
                        result.push_str(&format!(
                            "{:2}+: {:6} (p: {:6.2}%, c: {:6})",
                            i,
                            write[i],
                            write[i] as f64 / total_count as f64 * 100.0,
                            cumulative_count
                        ));
                    } else {
                        result.push_str(&format!(
                            "{:3}: {:6} (p: {:6.2}%, c: {:6})",
                            i,
                            write[i],
                            write[i] as f64 / total_count as f64 * 100.0,
                            cumulative_count
                        ));
                    }
                    sep = "\n";
                }
                result
            }

            pub fn merge(&self, other: &FileStat) {
                let read = unsafe { &mut *self.read.get() };
                let other_read = unsafe { &*other.read.get() };
                let write = unsafe { &mut *self.write.get() };
                let other_write = unsafe { &*other.write.get() };
                for i in 0..11 {
                    read[i] += other_read[i];
                    write[i] += other_write[i];
                }
            }

            pub fn clear(&self) {
                let read = unsafe { &mut *self.read.get() };
                let write = unsafe { &mut *self.write.get() };
                for i in 0..11 {
                    read[i] = 0;
                    write[i] = 0;
                }
            }
        }

        pub struct LocalStat {
            pub stat: FileStat,
        }

        impl Drop for LocalStat {
            fn drop(&mut self) {
                let global_stat = GLOBAL_FILE_STAT.lock().unwrap();
                global_stat.merge(&self.stat);
            }
        }

        lazy_static! {
            pub static ref GLOBAL_FILE_STAT: Mutex<FileStat> = Mutex::new(FileStat::new());
        }

        thread_local! {
            pub static LOCAL_STAT: LocalStat = LocalStat {
                stat: FileStat::new()
            };
        }

        pub fn inc_local_read_stat(wait_count: usize) {
            LOCAL_STAT.with(|local_stat| {
                let stat = &local_stat.stat;
                let read = unsafe { &mut *stat.read.get() };
                if wait_count >= 10 {
                    read[10] += 1;
                } else {
                    read[wait_count] += 1;
                }
            });
        }

        pub fn inc_local_write_stat(wait_count: usize) {
            LOCAL_STAT.with(|local_stat| {
                let stat = &local_stat.stat;
                let write = unsafe { &mut *stat.write.get() };
                if wait_count >= 10 {
                    write[10] += 1;
                } else {
                    write[wait_count] += 1;
                }
            });
        }
    }

    #[cfg(feature = "stat")]
    use stat::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum IOOp {
        Read,
        Write,
        Flush,
    }

    impl IOOp {
        fn as_u32(&self) -> u32 {
            match self {
                IOOp::Read => 0,
                IOOp::Write => 1,
                IOOp::Flush => 2,
            }
        }
    }

    impl From<u32> for IOOp {
        fn from(op: u32) -> Self {
            match op {
                0 => IOOp::Read,
                1 => IOOp::Write,
                2 => IOOp::Flush,
                _ => panic!("Invalid IOOp"),
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct IOOpTag {
        op: IOOp,
        page_id: PageId,
    }

    impl IOOpTag {
        fn get_op(&self) -> IOOp {
            self.op
        }

        fn get_id(&self) -> PageId {
            self.page_id
        }

        fn new_read(page_id: PageId) -> Self {
            IOOpTag {
                op: IOOp::Read,
                page_id,
            }
        }

        fn new_write(page_id: PageId) -> Self {
            IOOpTag {
                op: IOOp::Write,
                page_id,
            }
        }

        fn new_flush() -> Self {
            IOOpTag {
                op: IOOp::Flush,
                page_id: PageId::MAX,
            }
        }

        fn as_u64(&self) -> u64 {
            let upper_32 = self.op.as_u32() as u64;
            let lower_32 = self.page_id as u64;
            (upper_32 << 32) | lower_32
        }
    }

    impl From<u64> for IOOpTag {
        fn from(tag: u64) -> Self {
            let upper_32 = (tag >> 32) as u32;
            let lower_32 = tag as u32;
            IOOpTag {
                op: IOOp::from(upper_32),
                page_id: lower_32,
            }
        }
    }

    unsafe impl Send for FileManagerInner {} // Send is needed for io_vec

    struct FileManagerInner {
        _file: File,
        ring: IoUring,
        page_buffer_status: Vec<bool>, // Written = true, Not written = false
        page_buffer: Vec<Page>,
        _io_vec: UnsafeCell<Vec<iovec>>, // We have to keep this in-memory for the lifetime of the io_uring.
    }

    impl FileManagerInner {
        fn new(file: File) -> Result<Self, FMError> {
            let ring = IoUring::builder()
                .setup_sqpoll(1)
                .build(PAGE_BUFFER_SIZE as _)?;
            let mut page_buffer: Vec<Page> = (0..PAGE_BUFFER_SIZE)
                .map(|_| Page::new(PageId::MAX))
                .collect();
            let io_vec = page_buffer
                .iter_mut()
                .map(|page| iovec {
                    iov_base: page.get_raw_bytes_mut().as_mut_ptr() as _,
                    iov_len: PAGE_SIZE as _,
                })
                .collect::<Vec<_>>();
            // Register the file and the page buffer with the io_uring.
            let submitter = &ring.submitter();
            submitter.register_files(&[file.as_raw_fd()])?;
            unsafe {
                submitter.register_buffers(&io_vec)?;
            }
            Ok(FileManagerInner {
                _file: file,
                ring,
                page_buffer_status: (0..PAGE_BUFFER_SIZE).map(|_| true).collect(),
                page_buffer,
                _io_vec: UnsafeCell::new(io_vec),
            })
        }

        fn compute_hash(page_id: PageId) -> usize {
            // For safety, we take the Rust std::hash function
            // instead of a simple page_id as usize % PAGE_BUFFER_SIZE
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            page_id.hash(&mut hasher);
            hasher.finish() as usize % PAGE_BUFFER_SIZE
        }

        fn read_page(&mut self, page_id: PageId, page: &mut Page) -> Result<(), FMError> {
            // Check the entry in the page_buffer
            let hash = FileManagerInner::compute_hash(page_id);
            let mut _count = 0;
            if page_id == self.page_buffer[hash].get_id() {
                page.copy(&self.page_buffer[hash]);
            } else {
                // If the page is not in the buffer, read from the file.
                let buf = page.get_raw_bytes_mut();
                let entry = opcode::Read::new(types::Fixed(0), buf.as_mut_ptr(), buf.len() as _)
                    .offset(page_id as u64 * PAGE_SIZE as u64)
                    .build()
                    .user_data(IOOpTag::new_read(page_id).as_u64());
                unsafe {
                    self.ring.submission().push(&entry).expect("queue is full");
                }
                let _res = self.ring.submit()?;
                // assert_eq!(res, 1); // This is true if SQPOLL is disabled.

                loop {
                    if let Some(entry) = self.ring.completion().next() {
                        let tag = IOOpTag::from(entry.user_data());
                        _count += 1;
                        match tag.get_op() {
                            IOOp::Read => {
                                // Reads are run in sequence, so this should be the page we are interested in.
                                assert_eq!(tag.get_id(), page_id);
                                break;
                            }
                            IOOp::Write => {
                                let this_page_id = tag.get_id();
                                let this_hash = FileManagerInner::compute_hash(this_page_id);
                                self.page_buffer_status[this_hash] = true; // Mark the page as written.
                            }
                            IOOp::Flush => {
                                // Do nothing
                            }
                        }
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }

            log_debug!(
                "Read completed for page: {} with wait count: {}",
                page_id,
                _count
            );
            #[cfg(feature = "stat")]
            inc_local_read_stat(_count);
            Ok(())
        }

        fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<(), FMError> {
            // Check the entry in the page_buffer
            let hash = FileManagerInner::compute_hash(page_id);
            let mut _count = 0;
            loop {
                // Check the status of the page buffer.
                if self.page_buffer_status[hash] {
                    // If the page is written, overwrite the page and issue an async write.
                    self.page_buffer_status[hash] = false; // Mark the page as not written.
                    self.page_buffer[hash].copy(page);
                    let buf = self.page_buffer[hash].get_raw_bytes();
                    let entry = opcode::WriteFixed::new(
                        types::Fixed(0),
                        buf.as_ptr(),
                        buf.len() as _,
                        hash as _,
                    )
                    .offset(page_id as u64 * PAGE_SIZE as u64)
                    .build()
                    .user_data(IOOpTag::new_write(page_id).as_u64());
                    unsafe {
                        self.ring.submission().push(&entry).expect("queue is full");
                    }
                    let _res = self.ring.submit()?;
                    // assert_eq!(res, 1); // This is true if SQPOLL is disabled.
                    log_debug!(
                        "Write completed for page: {} with wait count: {}",
                        page_id,
                        _count
                    );
                    #[cfg(feature = "stat")]
                    inc_local_write_stat(_count);
                    return Ok(()); // This is the only return point.
                } else {
                    // If the page is not written, wait for the write to complete.
                    loop {
                        if let Some(entry) = self.ring.completion().next() {
                            _count += 1;
                            let tag = IOOpTag::from(entry.user_data());
                            match tag.get_op() {
                                IOOp::Write => {
                                    let this_page_id = tag.get_id();
                                    let this_hash = FileManagerInner::compute_hash(this_page_id);
                                    self.page_buffer_status[this_hash] = true; // Mark the page as written.
                                    if this_hash == hash {
                                        break; // Write completed for the buffer we are interested in.
                                    }
                                }
                                IOOp::Flush => {
                                    // Do nothing
                                }
                                IOOp::Read => {
                                    // Read should run synchronously, so this should not happen.
                                    panic!("Read should not be completed while waiting for write")
                                }
                            }
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
            }
        }

        fn flush_page(&mut self) -> Result<(), FMError> {
            // Find the first entry in the page_buffer that is not written. Wait for it to be written.
            for i in 0..PAGE_BUFFER_SIZE {
                if !self.page_buffer_status[i] {
                    let mut _count = 0;
                    loop {
                        if let Some(entry) = self.ring.completion().next() {
                            _count += 1;
                            let tag = IOOpTag::from(entry.user_data());
                            match tag.get_op() {
                                IOOp::Write => {
                                    let this_page_id = tag.get_id();
                                    let this_hash = FileManagerInner::compute_hash(this_page_id);
                                    self.page_buffer_status[this_hash] = true; // Mark the page as written.
                                    if this_hash == i {
                                        break; // Write completed for the buffer we are interested in.
                                    }
                                }
                                IOOp::Flush => {
                                    // Do nothing
                                }
                                IOOp::Read => {
                                    // Read should run synchronously, so this should not happen.
                                    panic!("Read should not be completed while waiting for write")
                                }
                            }
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
            }
            assert!(self.page_buffer_status.iter().all(|&x| x));
            // Now issue a flush operation.
            let entry = opcode::Fsync::new(types::Fixed(0))
                .build()
                .user_data(IOOpTag::new_flush().as_u64());
            unsafe {
                self.ring.submission().push(&entry).expect("queue is full");
            }
            let res = self.ring.submit_and_wait(1)?;
            assert_eq!(res, 1);

            // Check the completion queue for the flush operation.
            let entry = self.ring.completion().next().unwrap();
            let tag = IOOpTag::from(entry.user_data());
            assert_eq!(tag.get_op(), IOOp::Flush);

            Ok(())
        }
    }
}

#[cfg(feature = "new_async_write")]
mod new_async_write {
    use super::FMError;
    use crate::bp::ContainerId;
    #[allow(unused_imports)]
    use crate::log;
    use crate::page::{Page, PageId, PAGE_SIZE};
    use crate::rwlatch::RwLatch;
    use crate::{log_debug, log_error, log_info};
    use std::cell::UnsafeCell;
    use std::fs::{File, OpenOptions};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Condvar, Mutex, RwLock};

    use io_uring::{opcode, types, CompletionQueue, IoUring, SubmissionQueue};
    use libc::iovec;
    use std::hash::{Hash, Hasher};
    use std::os::unix::io::AsRawFd;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum IOOp {
        Read,
        Write,
        Flush,
    }

    impl IOOp {
        fn as_u32(&self) -> u32 {
            match self {
                IOOp::Read => 0,
                IOOp::Write => 1,
                IOOp::Flush => 2,
            }
        }
    }

    impl From<u32> for IOOp {
        fn from(op: u32) -> Self {
            match op {
                0 => IOOp::Read,
                1 => IOOp::Write,
                2 => IOOp::Flush,
                _ => panic!("Invalid IOOp"),
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct IOOpTag {
        op: IOOp,
        page_id: PageId,
    }

    impl IOOpTag {
        fn get_op(&self) -> IOOp {
            self.op
        }

        fn get_id(&self) -> PageId {
            self.page_id
        }

        fn new_read(page_id: PageId) -> Self {
            IOOpTag {
                op: IOOp::Read,
                page_id,
            }
        }

        fn new_write(page_id: PageId) -> Self {
            IOOpTag {
                op: IOOp::Write,
                page_id,
            }
        }

        fn new_flush() -> Self {
            IOOpTag {
                op: IOOp::Flush,
                page_id: PageId::MAX,
            }
        }

        fn as_u64(&self) -> u64 {
            let upper_32 = self.op.as_u32() as u64;
            let lower_32 = self.page_id as u64;
            (upper_32 << 32) | lower_32
        }
    }

    impl From<u64> for IOOpTag {
        fn from(tag: u64) -> Self {
            let upper_32 = (tag >> 32) as u32;
            let lower_32 = tag as u32;
            IOOpTag {
                op: IOOp::from(upper_32),
                page_id: lower_32,
            }
        }
    }

    const PAGE_BUFFER_SIZE: usize = 128;

    pub struct FileManager {
        _file: File,
        num_pages: AtomicU32,
        ring: Mutex<IoUring>,
        page_buffer: Vec<(UnsafeCell<Page>, AtomicU32, RwLatch)>, // (Page, Status, Latch) // Status: 0 = no on-going work, 1 = on-going work
    }

    unsafe impl Sync for FileManager {}

    impl FileManager {
        pub fn new<P: AsRef<std::path::Path>>(
            db_dir: P,
            c_id: ContainerId,
        ) -> Result<Self, FMError> {
            std::fs::create_dir_all(&db_dir).map_err(|_| FMError::DirCreate)?;
            let path = db_dir.as_ref().join(format!("{}", c_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(|_| FMError::Open)?;
            let num_pages = file.metadata().unwrap().len() as usize / PAGE_SIZE;

            let ring = IoUring::builder()
                .setup_sqpoll(1)
                .build(PAGE_BUFFER_SIZE as _)?;
            let page_buffer: Vec<(UnsafeCell<Page>, AtomicU32, RwLatch)> = (0..PAGE_BUFFER_SIZE)
                .map(|_| {
                    let page = UnsafeCell::new(Page::new(PageId::MAX));
                    (page, AtomicU32::new(0), RwLatch::default())
                })
                .collect();
            // Register the file and the page buffer with the io_uring.
            let submitter = &ring.submitter();
            submitter.register_files(&[file.as_raw_fd()])?;

            Ok(FileManager {
                _file: file,
                num_pages: AtomicU32::new(num_pages as PageId),
                ring: Mutex::new(ring),
                page_buffer,
            })
        }

        pub fn fetch_add_page_id(&self) -> PageId {
            self.num_pages.fetch_add(1, Ordering::AcqRel)
        }

        pub fn fetch_sub_page_id(&self) -> PageId {
            self.num_pages.fetch_sub(1, Ordering::AcqRel)
        }

        #[allow(dead_code)]
        pub fn get_stats(&self) -> String {
            format!("Num pages: {}", self.num_pages.load(Ordering::Acquire),)
        }

        fn compute_hash(page_id: PageId) -> usize {
            // For safety, we take the Rust std::hash function
            // instead of a simple page_id as usize % PAGE_BUFFER_SIZE
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            page_id.hash(&mut hasher);
            hasher.finish() as usize % PAGE_BUFFER_SIZE
        }

        fn poll_once(&self) {
            if let Some(entry) = self.ring.lock().unwrap().completion().next() {
                let tag = IOOpTag::from(entry.user_data());
                match tag.get_op() {
                    IOOp::Read => {
                        let page_id = tag.get_id();
                        let hash = FileManager::compute_hash(page_id);
                        self.page_buffer[hash].1.store(0, Ordering::Release); // Mark the page as no on-going work.
                    }
                    IOOp::Write => {
                        let page_id = tag.get_id();
                        let hash = FileManager::compute_hash(page_id);
                        self.page_buffer[hash].1.store(0, Ordering::Release); // Mark the page as no on-going work.
                    }
                    IOOp::Flush => {
                        // Do nothing
                    }
                }
            }
        }

        pub fn prefetch_page(&self, page_id: PageId) -> Result<(), FMError> {
            let hash = FileManager::compute_hash(page_id);
            let (buffer, status, latch) = &self.page_buffer[hash];
            let buffer = unsafe { &mut *buffer.get() };

            // Latch the page for loading the page from disk to buffer.
            while !latch.try_exclusive() {
                self.poll_once();
            }

            // While there is on-going work on the page, wait.
            while status.swap(1, Ordering::AcqRel) == 1 {
                self.poll_once();
            }

            // Now, the page has no on-going work.
            if buffer.get_id() == page_id {
                status.store(0, Ordering::Release); // Mark the page as no on-going work.
                latch.release_exclusive();
                Ok(())
            } else {
                // Issue a read operation to the file.
                let buf = buffer.get_raw_bytes_mut();
                let entry = opcode::Read::new(types::Fixed(0), buf.as_mut_ptr(), buf.len() as _)
                    .offset(page_id as u64 * PAGE_SIZE as u64)
                    .build()
                    .user_data(IOOpTag::new_read(page_id).as_u64());
                let ring = &mut *self.ring.lock().unwrap();
                unsafe { ring.submission().push(&entry).expect("queue is full") };
                let _res = ring.submit()?;
                // assert_eq!(res, 1); // This is true if SQPOLL is disabled.

                latch.release_exclusive();
                // The status is kept as 1, to tell that the buffer is currently being used for prefetching.
                Ok(())
            }
        }

        pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), FMError> {
            // Check the entry in the page_buffer
            let hash = FileManager::compute_hash(page_id);
            let (buffer, status, latch) = &self.page_buffer[hash];
            let buffer = unsafe { &*buffer.get() };

            // Latch the page for loading.
            while !latch.try_exclusive() {
                self.poll_once();
            }

            // While there is on-going work on the page, wait.
            while status.swap(1, Ordering::AcqRel) == 1 {
                self.poll_once();
            }

            // Now, the page has no on-going work.
            if buffer.get_id() == page_id {
                // println!("Direct read");
                page.copy(buffer);
                status.store(0, Ordering::Release); // Mark the page as no on-going work.
                latch.release_exclusive();
                Ok(())
            } else {
                // println!("Indirect read");
                // The page is not in the buffer.
                // Since no one else is working on page, we can issue the read operation to the file.
                let buf = page.get_raw_bytes_mut();
                let entry = opcode::Read::new(types::Fixed(0), buf.as_mut_ptr(), buf.len() as _)
                    .offset(page_id as u64 * PAGE_SIZE as u64)
                    .build()
                    .user_data(IOOpTag::new_read(page_id).as_u64());
                {
                    let ring = &mut self.ring.lock().unwrap();
                    unsafe { ring.submission().push(&entry).expect("queue is full") };
                    let _res = ring.submit()?;
                }

                // Poll the completion queue until the read operation is completed.
                while status.load(Ordering::Acquire) == 1 {
                    self.poll_once();
                }

                // The read operation is completed. The page contains the buffer now.
                // Note that buffer is not updated.
                // assert_eq!(page.get_id(), page_id); This is usually true but in some tests, page_id is not set.
                latch.release_exclusive();
                Ok(())
            }
        }

        pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), FMError> {
            // Check the entry in the page_buffer
            let hash = FileManager::compute_hash(page_id);
            let (buffer, status, latch) = &self.page_buffer[hash];
            let buffer = unsafe { &mut *buffer.get() };

            // Latch the page for writing.
            while !latch.try_exclusive() {
                self.poll_once();
            }

            // While there is on-going work on the page, wait.
            while status.swap(1, Ordering::AcqRel) == 1 {
                self.poll_once();
            }

            buffer.copy(page);
            let buf = buffer.get_raw_bytes();
            let entry = opcode::Write::new(types::Fixed(0), buf.as_ptr(), buf.len() as _)
                .offset(page_id as u64 * PAGE_SIZE as u64)
                .build()
                .user_data(IOOpTag::new_write(page_id).as_u64());

            let ring = &mut *self.ring.lock().unwrap();
            unsafe { ring.submission().push(&entry).expect("queue is full") };
            let _res = ring.submit()?;
            // assert_eq!(res, 1); // This is true if SQPOLL is disabled.

            // Release the latch
            latch.release_exclusive();

            Ok(()) // This is the only return point.
        }

        pub fn flush(&self) -> Result<(), FMError> {
            // Find the first entry in the page_buffer that is not written. Wait for it to be written.
            for i in 0..PAGE_BUFFER_SIZE {
                let (_, status, latch) = &self.page_buffer[i];
                while !latch.try_exclusive() {
                    self.poll_once();
                }

                while status.load(Ordering::Acquire) == 1 {
                    self.poll_once();
                }

                latch.release_exclusive();
            }

            // Now issue a flush operation.
            // let entry = opcode::Fsync::new(types::Fixed(0))
            //     .build()
            //     .user_data(IOOpTag::new_flush().as_u64());
            // let ring = &mut *self.ring.lock().unwrap();
            // unsafe { ring.submission().push(&entry).expect("queue is full") };
            // let _res = ring.submit()?;

            // Issue a sync flush
            self._file.sync_all().map_err(|_| FMError::Flush)?;

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FileManager;
    use crate::page::{Page, PageId};
    use crate::random::gen_random_permutation;

    #[test]
    fn test_page_write_read() {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(&temp_path, 0).unwrap();
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

    #[test]
    fn test_prefetch() {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(&temp_path, 0).unwrap();

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

    #[test]
    fn test_page_write_read_sequential() {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(&temp_path, 0).unwrap();

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

    #[test]
    fn test_page_write_read_random() {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(&temp_path, 0).unwrap();

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

    #[test]
    fn test_page_write_read_interleave() {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(&temp_path, 0).unwrap();

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

    #[test]
    fn test_file_flush() {
        // Create two file managers with the same path.
        // Issue multiple write operations to one of the file managers.
        // Check if the other file manager can read the pages.

        let temp_path = tempfile::tempdir().unwrap();
        let file_manager1 = FileManager::new(&temp_path, 0).unwrap();
        let file_manager2 = FileManager::new(&temp_path, 0).unwrap();

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

    #[test]
    fn test_concurrent_read_write_file() {
        let temp_path = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(&temp_path, 0).unwrap();

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
