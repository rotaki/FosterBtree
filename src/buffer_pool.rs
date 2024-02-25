use crate::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    file_manager::FileManager,
};
use std::{
    cell::UnsafeCell,
    collections::hash_map::Entry,
    collections::HashMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

pub const NUM_PAGES: usize = 1 << 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContainerPageKey {
    container_id: usize,
    page_id: usize,
}

impl ContainerPageKey {
    pub fn new(container_id: usize, page_id: usize) -> Self {
        ContainerPageKey {
            container_id,
            page_id,
        }
    }
}

pub struct BufferPool {
    path: PathBuf,
    latch: AtomicBool,
    pages: UnsafeCell<Vec<BufferFrame>>,
    id_to_index: UnsafeCell<HashMap<ContainerPageKey, usize>>, // (container_id, page_id) -> index
    container_to_file: UnsafeCell<HashMap<usize, FileManager>>,
}

impl BufferPool {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
        // Identify all the files in the directory. Parse the file name to a number.
        // Create a FileManager for each file and store it in the container.
        let mut container = HashMap::new();
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let file_id = file_name.parse::<usize>().unwrap();
            let file_manager = FileManager::new(path);
            container.insert(file_id, file_manager);
        }

        let pages = (0..NUM_PAGES).map(|_| BufferFrame::default()).collect();
        BufferPool {
            path: path.as_ref().to_path_buf(),
            latch: AtomicBool::new(false),
            id_to_index: UnsafeCell::new(HashMap::new()),
            pages: UnsafeCell::new(pages),
            container_to_file: UnsafeCell::new(container),
        }
    }

    fn latch(&self) {
        while self.latch.swap(true, Ordering::Acquire) {
            // spin
            std::hint::spin_loop();
        }
    }

    fn unlatch(&self) {
        self.latch.store(false, Ordering::Release);
    }

    fn choose_page_to_evict(&self) -> ContainerPageKey {
        unimplemented!()
    }

    fn get_empty_frame(&self) -> usize {
        0 // TODO: Check if there is an empty frame. If not, evict a page.
    }

    pub fn create_new_page(&self, container_id: usize) -> ContainerPageKey {
        // Modification to the following data structures must be done while holding the latch
        // on the buffer pool.
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        match container_to_file.entry(container_id) {
            Entry::Occupied(mut entry) => {
                let file_manager = entry.get_mut();
                self.unlatch();

                let page_id = file_manager.new_page();
                ContainerPageKey::new(container_id, page_id)
            }
            Entry::Vacant(entry) => {
                let file_manager = FileManager::new(self.path.join(container_id.to_string()));
                let file_manager = entry.insert(file_manager);
                self.unlatch();

                let page_id = file_manager.new_page();
                ContainerPageKey::new(container_id, page_id)
            }
        }
    }

    pub fn get_page_for_write(&self, key: ContainerPageKey) -> Option<FrameWriteGuard> {
        // Modification to the following data structures must be done while holding the latch
        // on the buffer pool.
        let pages = unsafe { &mut *self.pages.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&key).copied() {
            let res = pages[index].try_write();
            self.unlatch();
            res
        } else {
            // Choose a page to evict
            let index = self.get_empty_frame();
            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                // Modify the id_to_index map
                id_to_index.insert(key, index);

                let file = container_to_file
                    .get(&key.container_id)
                    .expect("file not found");
                self.unlatch();

                let page = file.read_page(key.page_id);
                guard.copy(&page);
                Some(guard)
            } else {
                self.unlatch();
                None
            }
        }
    }

    pub fn get_page_for_read(&self, key: ContainerPageKey) -> Option<FrameReadGuard> {
        // Modification to the following data structures must be done while holding the latch
        // on the buffer pool.

        let pages = unsafe { &mut *self.pages.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&key).copied() {
            let res = pages[index].try_read();
            self.unlatch();
            res
        } else {
            // Choose a page to evict
            let index = self.get_empty_frame();
            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                // Modify the id_to_index map
                id_to_index.insert(key, index);

                let file = container_to_file
                    .get(&key.container_id)
                    .expect("file not found");
                self.unlatch();

                let page = file.read_page(key.page_id);
                guard.copy(&page);
                Some(guard.downgrade())
            } else {
                self.unlatch();
                None
            }
        }
    }
}

unsafe impl Sync for BufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        {
            let bp = BufferPool::new(temp_dir.path());
            let key = bp.create_new_page(0);
            let num_threads = 3;
            let num_iterations = 80; // Note: u8 max value is 255
            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn(|| {
                        for _ in 0..num_iterations {
                            loop {
                                if let Some(mut guard) = bp.get_page_for_write(key) {
                                    guard[0] += 1;
                                    break;
                                } else {
                                    // spin
                                    println!("spin: {:?}", thread::current().id());
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    });
                }
            });

            let guard = bp.get_page_for_read(key).unwrap();
            assert_eq!(guard[0], num_threads * num_iterations);
        }
    }
}
