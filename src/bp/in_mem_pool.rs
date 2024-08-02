use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
};

use crate::{page::Page, rwlatch::RwLatch};

use super::{
    buffer_frame::BufferFrame,
    eviction_policy::EvictionPolicy,
    mem_pool_trait::{MemPool, PageKey},
    prelude::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPoolStatus, PageFrameKey},
};

/// A simple in-memory page pool.
/// All the pages are stored in a vector in memory.
/// A latch is used to synchronize access to the pool.
/// An exclusive latch is required to create a new page and append it to the pool.
/// Getting a page for read or write requires a shared latch.

pub struct InMemPool<T: EvictionPolicy> {
    latch: RwLatch,
    frames: UnsafeCell<Vec<Box<BufferFrame<T>>>>, // Box is required to ensure that the frame does not move when the vector is resized
    id_to_index: UnsafeCell<HashMap<PageKey, usize>>,
    container_page_count: UnsafeCell<HashMap<ContainerKey, u32>>,
}

impl<T: EvictionPolicy> Default for InMemPool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: EvictionPolicy> InMemPool<T> {
    pub fn new() -> Self {
        InMemPool {
            latch: RwLatch::default(),
            frames: UnsafeCell::new(Vec::new()),
            id_to_index: UnsafeCell::new(HashMap::new()),
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

impl<T: EvictionPolicy> MemPool<T> for InMemPool<T> {
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        self.exclusive();
        let frames = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
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
        let page = Page::new(page_id);

        let page_key = PageKey::new(c_key, page_id);
        let frame_index = frames.len();
        let frame = Box::new(BufferFrame::new(frame_index as u32));
        frames.push(frame);
        id_to_index.insert(page_key, frame_index);
        let mut guard = (frames.get(frame_index).unwrap()).write(true);
        self.release_exclusive();

        guard.copy(&page);
        *guard.page_key_mut() = Some(page_key);
        Ok(guard)
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        self.shared();
        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &*self.id_to_index.get() };
        let frame_index = match id_to_index.get(&key.p_key()) {
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

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard<T>, MemPoolStatus> {
        self.shared();
        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &*self.id_to_index.get() };
        let frame_index = match id_to_index.get(&key.p_key()) {
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

    fn stats(&self) -> String {
        "Num pages: ".to_string()
            + &unsafe { &*self.frames.get() }.len().to_string()
            + "\n"
            + "Num containers: "
            + &unsafe { &*self.container_page_count.get() }
                .len()
                .to_string()
    }

    fn reset_stats(&self) {
        // Do nothing
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn clear_frames(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }
}

#[cfg(test)]
impl<T: EvictionPolicy> InMemPool<T> {
    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            frame.try_write(false).unwrap();
        }
    }

    // Invariant: id_to_index contains all pages in frames
    pub fn check_id_to_index(&self) {
        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &*self.id_to_index.get() };
        for (key, index) in id_to_index.iter() {
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

unsafe impl<T: EvictionPolicy> Sync for InMemPool<T> {}

#[cfg(test)]
mod tests {
    use crate::bp::prelude::DummyEvictionPolicy;

    use super::*;
    use std::thread;

    pub type InMemPool = super::InMemPool<DummyEvictionPolicy>;

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
        mp.check_id_to_index();
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
        mp.check_id_to_index();
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
