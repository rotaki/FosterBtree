#[cfg(feature = "iouring_async")]
use super::file_manager::iouring_async::GlobalRings;
use super::file_manager::memory::FileManager as MemoryFileManager;
use super::{FileManager, FileManagerTrait, FileStats};
use crate::bp::prelude::{ContainerId, MemPoolStatus};
use crate::page::{Page, PageId};
use dashmap::DashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

enum StorageBackend {
    Disk,
    Memory,
}

pub struct Container {
    page_count: AtomicUsize,
    is_temp: AtomicBool,
    file_manager: Box<dyn FileManagerTrait>,
}

impl Container {
    pub fn new(file_manager: Box<dyn FileManagerTrait>) -> Self {
        Container {
            page_count: AtomicUsize::new(file_manager.num_pages()),
            is_temp: AtomicBool::new(false),
            file_manager,
        }
    }

    pub fn new_temp(file_manager: Box<dyn FileManagerTrait>) -> Self {
        Container {
            page_count: AtomicUsize::new(file_manager.num_pages()),
            is_temp: AtomicBool::new(true),
            file_manager,
        }
    }

    pub fn set_temp(&self, is_temp: bool) {
        self.is_temp.store(is_temp, Ordering::Relaxed);
    }

    pub fn is_temp(&self) -> bool {
        self.is_temp.load(Ordering::Relaxed)
    }

    pub fn num_pages(&self) -> usize {
        self.page_count.load(Ordering::Relaxed)
    }

    pub fn inc_page_count(&self, count: usize) -> usize {
        self.page_count.fetch_add(count, Ordering::Relaxed)
    }

    pub fn inc_page_count_if<F, R>(&self, count: usize, func: F) -> Option<(R, usize)>
    where
        F: Fn(usize) -> Option<R>,
    {
        // Will hold the closure’s return value if we decide to increment.
        let mut result: Option<R> = None;

        // Atomically try to bump the counter.
        let prev = self
            .page_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                func(cur).map(|r| {
                    result = Some(r); // remember what the caller wants back
                    cur + count // new counter value
                })
            });

        // If the fetch-update succeeded, `prev` is Ok(old_value) and `result` is Some(_).
        // unwrap will be safe because we know the closure returned Some(_).
        prev.ok().map(|old| (result.unwrap(), old))
    }

    pub fn get_stats(&self) -> FileStats {
        self.file_manager.get_stats()
    }

    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), std::io::Error> {
        self.file_manager.read_page(page_id, page)
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), std::io::Error> {
        if !self.is_temp() {
            // Does not write to the file if the container is temporary.
            self.file_manager.write_page(page_id, page)
        } else {
            Ok(())
        }
    }

    pub fn flush(&self) -> Result<(), std::io::Error> {
        self.file_manager.flush()
    }
}

pub struct ContainerManager {
    remove_dir_on_drop: bool,
    base_dir: PathBuf,
    containers: DashMap<ContainerId, Arc<Container>>, // (db_id, c_id) -> Container
    storage_backend: StorageBackend,
    #[cfg(feature = "iouring_async")]
    ring: Option<Arc<GlobalRings>>,
    direct: bool, // Direct IO
}

impl ContainerManager {
    #[cfg(feature = "iouring_async")]
    fn open_disk_file_manager(
        direct: bool,
        db_path: &Path,
        c_id: super::file_manager::ContainerId,
        ring: &Arc<GlobalRings>,
    ) -> Box<dyn FileManagerTrait> {
        if direct {
            Box::new(FileManager::new(db_path, c_id, ring.clone()).unwrap())
        } else {
            Box::new(FileManager::with_kpc(db_path, c_id, ring.clone()).unwrap())
        }
    }

    #[cfg(not(feature = "iouring_async"))]
    fn open_disk_file_manager(
        direct: bool,
        db_path: &Path,
        c_id: super::file_manager::ContainerId,
    ) -> Box<dyn FileManagerTrait> {
        if direct {
            Box::new(FileManager::new(db_path, c_id).unwrap())
        } else {
            Box::new(FileManager::with_kpc(db_path, c_id).unwrap())
        }
    }

    fn open_memory_file_manager(
        db_path: &Path,
        c_id: super::file_manager::ContainerId,
    ) -> Box<dyn FileManagerTrait> {
        Box::new(MemoryFileManager::new(db_path, c_id).unwrap())
    }

    fn open_file_manager(
        &self,
        db_path: &Path,
        c_id: super::file_manager::ContainerId,
    ) -> Box<dyn FileManagerTrait> {
        match self.storage_backend {
            StorageBackend::Disk => {
                #[cfg(feature = "iouring_async")]
                {
                    Self::open_disk_file_manager(
                        self.direct,
                        db_path,
                        c_id,
                        self.ring.as_ref().unwrap(),
                    )
                }
                #[cfg(not(feature = "iouring_async"))]
                {
                    Self::open_disk_file_manager(self.direct, db_path, c_id)
                }
            }
            StorageBackend::Memory => Self::open_memory_file_manager(db_path, c_id),
        }
    }

    /// Directory structure
    /// * base_dir
    ///    * db_dir
    ///      * container_file
    ///
    /// A call to new will create the base_dir if it does not exist.
    /// The db_dir and container_file are lazily created when a FileManager is requested.
    /// If remove_dir_on_drop is true, then the base_dir is removed when the ContainerManager is dropped.
    pub fn new<P: AsRef<Path>>(
        base_dir: P,
        direct: bool,
        remove_dir_on_drop: bool,
    ) -> Result<Self, std::io::Error> {
        // Identify all the directories. A directory corresponds to a database.
        // A file in the directory corresponds to a container.
        // Create a FileManager for each file and store it in the container.
        // If base_dir does not exist, then create it.
        create_dir_all(&base_dir)?;

        #[cfg(feature = "iouring_async")]
        let ring = Arc::new(GlobalRings::new(128));

        let containers = DashMap::new();
        for entry in std::fs::read_dir(&base_dir).unwrap() {
            let entry = entry.unwrap();
            let db_path = entry.path();
            if db_path.is_dir() {
                let db_id = db_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                for entry in std::fs::read_dir(&db_path).unwrap() {
                    let entry = entry.unwrap();
                    let file_path = entry.path();
                    if file_path.is_file() {
                        let c_id = file_path
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .parse()
                            .unwrap();
                        #[cfg(feature = "iouring_async")]
                        let fm = Self::open_disk_file_manager(direct, &db_path, c_id, &ring);
                        #[cfg(not(feature = "iouring_async"))]
                        let fm = Self::open_disk_file_manager(direct, &db_path, c_id);
                        containers
                            .insert(ContainerId::new(db_id, c_id), Arc::new(Container::new(fm)));
                    }
                }
            }
        }

        Ok(ContainerManager {
            remove_dir_on_drop,
            base_dir: base_dir.as_ref().to_path_buf(),
            containers,
            storage_backend: StorageBackend::Disk,
            #[cfg(feature = "iouring_async")]
            ring: Some(ring),
            direct,
        })
    }

    pub fn new_in_memory() -> Self {
        static NEXT_MEMORY_NAMESPACE: AtomicUsize = AtomicUsize::new(0);
        let namespace = NEXT_MEMORY_NAMESPACE.fetch_add(1, Ordering::Relaxed);

        ContainerManager {
            remove_dir_on_drop: false,
            base_dir: PathBuf::from(format!("__in_memory__/{}", namespace)),
            containers: DashMap::new(),
            storage_backend: StorageBackend::Memory,
            #[cfg(feature = "iouring_async")]
            ring: None,
            direct: false,
        }
    }

    pub fn remove_dir_on_drop(&self) -> bool {
        self.remove_dir_on_drop
    }

    // Return the file manager for the given container id with a counter for the number of pages.
    pub fn get_container(&self, container_id: ContainerId) -> Arc<Container> {
        let container = self.containers.entry(container_id).or_insert_with(|| {
            let db_path = self.base_dir.join(container_id.db_id().to_string());
            let fm = self.open_file_manager(&db_path, container_id.local_container_id());
            Arc::new(Container::new(fm))
        });
        container.value().clone()
    }

    pub fn create_container(&self, container_id: ContainerId, is_temp: bool) {
        self.containers.entry(container_id).or_insert_with(|| {
            let db_path = self.base_dir.join(container_id.db_id().to_string());
            let fm = self.open_file_manager(&db_path, container_id.local_container_id());
            if is_temp {
                Arc::new(Container::new_temp(fm))
            } else {
                Arc::new(Container::new(fm))
            }
        });
    }

    pub fn get_stats(&self) -> Vec<(ContainerId, (usize, FileStats))> {
        let mut vec = Vec::new();
        for container in self.containers.iter() {
            let count = container.num_pages();
            let stats = container.get_stats();
            vec.push((*container.key(), (count, stats)));
        }
        vec
    }

    pub fn flush_all(&self) -> Result<(), MemPoolStatus> {
        for container in self.containers.iter() {
            container.flush()?;
        }
        Ok(())
    }
}

impl Drop for ContainerManager {
    fn drop(&mut self) {
        if self.remove_dir_on_drop && matches!(self.storage_backend, StorageBackend::Disk) {
            std::fs::remove_dir_all(&self.base_dir).unwrap();
        }
    }
}
