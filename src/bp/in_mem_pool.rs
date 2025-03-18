use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, BTreeMap, HashMap},
};

use crate::rwlatch::RwLatch;

use super::{
    buffer_frame::BufferFrame,
    mem_pool_trait::{MemPool, MemoryStats, PageKey},
    prelude::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPoolStatus, PageFrameKey},
};

/// A simple in-memory page pool.
/// All the pages are stored in a vector in memory.
/// A latch is used to synchronize access to the pool.
/// An exclusive latch is required to create a new page and append it to the pool.
/// Getting a page for read or write requires a shared latch.
pub struct InMemPool {
    latch: RwLatch,
    frames: UnsafeCell<Vec<Box<BufferFrame>>>, // Box is required to ensure that the frame does not move when the vector is resized
    page_to_frame: UnsafeCell<HashMap<PageKey, usize>>,
    container_page_count: UnsafeCell<HashMap<ContainerKey, u32>>,
}

impl Default for InMemPool {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemPool {
    pub fn new() -> Self {
        InMemPool {
            latch: RwLatch::default(),
            frames: UnsafeCell::new(Vec::new()),
            page_to_frame: UnsafeCell::new(HashMap::new()),
            container_page_count: UnsafeCell::new(HashMap::new()),
        }
    }

    fn shared(&self) {
        self.latch.shared();
    }

    fn exclusive(&self) {
        self.latch.exclusive();
    }

    fn release_shared(&self) {
        self.latch.release_shared();
    }

    fn release_exclusive(&self) {
        self.latch.release_exclusive();
    }
}

impl MemPool for InMemPool {
    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.exclusive();
        let frames = unsafe { &mut *self.frames.get() };
        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        let container_page_count = unsafe { &mut *self.container_page_count.get() };

        let page_id = match container_page_count.entry(c_key) {
            Entry::Occupied(mut entry) => {
                let page_id = *entry.get();
                *entry.get_mut() += 1;
                page_id
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
                0
            }
        };

        let page_key = PageKey::new(c_key, page_id);
        let frame_index = frames.len();
        let frame = Box::new(BufferFrame::new(frame_index as u32));
        frames.push(frame);
        page_to_frame.insert(page_key, frame_index);
        let mut guard = (frames.get(frame_index).unwrap()).write(true);
        self.release_exclusive();

        guard.set_id(page_id);
        *guard.page_key_mut() = Some(page_key);
        Ok(guard)
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus> {
        self.exclusive();
        let frames = unsafe { &mut *self.frames.get() };
        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        let container_page_count = unsafe { &mut *self.container_page_count.get() };

        let start_page_id = match container_page_count.entry(c_key) {
            Entry::Occupied(mut entry) => {
                let page_id = *entry.get();
                *entry.get_mut() += num_pages as u32;
                page_id
            }
            Entry::Vacant(entry) => {
                entry.insert(num_pages as u32);
                0
            }
        };

        // Insert all the new pages to the pool
        for i in 0..num_pages {
            let page_id = start_page_id + i as u32;
            let page_key = PageKey::new(c_key, page_id);
            let frame_index = frames.len();
            let frame = Box::new(BufferFrame::new(frame_index as u32));
            frames.push(frame);
            page_to_frame.insert(page_key, frame_index);
        }

        let mut guards = Vec::with_capacity(num_pages);

        // Initialize all the new pages
        for i in 0..num_pages {
            let page_id = start_page_id + i as u32;
            let page_key = PageKey::new(c_key, page_id);
            let frame_index = page_to_frame.get(&page_key).unwrap();
            let mut guard = (frames.get(*frame_index).unwrap()).write(true);
            guard.set_id(page_id);
            *guard.page_key_mut() = Some(page_key);
            guards.push(guard);
        }

        self.release_exclusive();
        Ok(guards)
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.shared();
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let is_cached = page_to_frame.contains_key(&key.p_key());
        self.release_shared();
        is_cached
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.shared();
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let keys = page_to_frame
            .iter()
            .filter(|(key, _)| key.c_key == c_key)
            .map(|(key, frame_idx)| {
                PageFrameKey::new_with_frame_id(c_key, key.page_id, *frame_idx as u32)
            })
            .collect();
        self.release_shared();
        keys
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.shared();
        let frames = unsafe { &*self.frames.get() };
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let frame_index = match page_to_frame.get(&key.p_key()) {
            Some(index) => *index,
            None => {
                self.release_shared();
                return Err(MemPoolStatus::PageNotFound);
            }
        };

        let frame = (frames.get(frame_index).unwrap()).try_write(true);
        self.release_shared();
        if let Some(frame) = frame {
            Ok(frame)
        } else {
            Err(MemPoolStatus::FrameWriteLatchGrantFailed)
        }
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus> {
        self.shared();
        let frames = unsafe { &*self.frames.get() };
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let frame_index = match page_to_frame.get(&key.p_key()) {
            Some(index) => *index,
            None => {
                self.release_shared();
                return Err(MemPoolStatus::PageNotFound);
            }
        };

        let frame = (frames.get(frame_index).unwrap()).try_read();
        self.release_shared();
        if let Some(frame) = frame {
            Ok(frame)
        } else {
            Err(MemPoolStatus::FrameReadLatchGrantFailed)
        }
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn stats(&self) -> MemoryStats {
        let num_frames = unsafe { &*self.frames.get() }.len();
        let mut containers = BTreeMap::new();
        for frame in unsafe { &*self.frames.get() }.iter() {
            let frame = frame.read();
            if let Some(key) = frame.page_key() {
                *containers.entry(key.c_key).or_insert(0) += 1;
            }
        }
        MemoryStats {
            bp_num_frames_in_mem: num_frames,
            bp_new_page: num_frames,
            bp_read_frame: num_frames,
            bp_read_frame_wait: 0,
            bp_write_frame: num_frames,
            bp_num_frames_per_container: containers,
            disk_created: 0,
            disk_read: 0,
            disk_write: 0,
            disk_io_per_container: BTreeMap::new(),
        }
    }

    fn reset_stats(&self) {
        // Do nothing
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        Ok(())
    }
}

#[cfg(test)]
impl InMemPool {
    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            frame.try_write(false).unwrap();
        }
    }

    // Invariant: page_to_frame contains all pages in frames
    pub fn check_page_to_frame(&self) {
        let frames = unsafe { &*self.frames.get() };
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        for (key, index) in page_to_frame.iter() {
            let frame = &frames[*index];
            let frame = frame.read();
            assert_eq!(*frame.page_key(), Some(*key));
        }
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = frame.read();
            let key = frame.page_key().unwrap();
            let page_id = frame.get_id();
            assert_eq!(key.page_id, page_id);
        }
    }
}

unsafe impl Sync for InMemPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_mp_and_frame_latch() {
        let mp = InMemPool::new();
        let c_key = ContainerKey { db_id: 0, c_id: 0 };

        let frame = mp.create_new_page_for_write(c_key).unwrap();
        let page_key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iterations = 80;
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        loop {
                            if let Ok(mut guard) = mp.get_page_for_write(page_key) {
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

        mp.check_all_frames_unlatched();
        mp.check_page_to_frame();
        mp.check_frame_id_and_page_id_match();
        let guard = mp.get_page_for_read(page_key).unwrap();
        assert_eq!(guard[0], num_threads * num_iterations);
    }

    #[test]
    fn test_create_new_page() {
        let mp = InMemPool::new();
        let c_key = ContainerKey { db_id: 0, c_id: 0 };

        for i in 0..20 {
            let frame = mp.create_new_page_for_write(c_key).unwrap();
            assert_eq!(frame.page_key().unwrap(), PageKey::new(c_key, i));
            drop(frame);
        }

        for i in 0..20 {
            let frame = mp.get_page_for_read(PageFrameKey::new(c_key, i)).unwrap();
            assert_eq!(frame.page_key().unwrap(), PageKey::new(c_key, i));
        }

        mp.check_all_frames_unlatched();
        mp.check_page_to_frame();
        mp.check_frame_id_and_page_id_match();
    }

    #[test]
    fn test_concurrent_create_new_page() {
        let mp = InMemPool::new();
        let c_key = ContainerKey { db_id: 0, c_id: 0 };

        let mut frame1 = mp.create_new_page_for_write(c_key).unwrap();
        frame1[0] = 1;
        let mut frame2 = mp.create_new_page_for_write(c_key).unwrap();
        frame2[0] = 2;
        assert_eq!(frame1.page_key().unwrap(), PageKey::new(c_key, 0));
        assert_eq!(frame2.page_key().unwrap(), PageKey::new(c_key, 1));
        drop(frame1);
        drop(frame2);

        let frame1 = mp.get_page_for_read(PageFrameKey::new(c_key, 0)).unwrap();
        let frame2 = mp.get_page_for_read(PageFrameKey::new(c_key, 1)).unwrap();
        assert_eq!(frame1[0], 1);
        assert_eq!(frame2[0], 2);
    }
}
