#[allow(unused_imports)]
use crate::log;

use crate::{log_debug, random::gen_random_int, rwlatch::RwLatch};

use super::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    eviction_policy::EvictionPolicy,
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};

use crate::file_manager::FileManager;

use std::{
    cell::{RefCell, UnsafeCell},
    collections::{BTreeMap, HashMap},
    fs::create_dir_all,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

const EVICTION_SCAN_TRIALS: usize = 5;
const EVICTION_SCAN_DEPTH: usize = 10;

use concurrent_queue::ConcurrentQueue;
use dashmap::DashMap;

/// Statistics kept by the buffer pool.
/// These statistics are used for decision making.
struct RuntimeStats {
    new_page: AtomicUsize,
    read_count: AtomicUsize,
    write_count: AtomicUsize,
}

impl std::fmt::Display for RuntimeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "New Page: {}\nRead Count: {}\nWrite Count: {}",
            self.new_page.load(Ordering::Relaxed),
            self.read_count.load(Ordering::Relaxed),
            self.write_count.load(Ordering::Relaxed)
        )
    }
}

impl RuntimeStats {
    pub fn new() -> Self {
        RuntimeStats {
            new_page: AtomicUsize::new(0),
            read_count: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
        }
    }

    pub fn inc_new_page(&self) {
        self.new_page.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_read_count(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_write_count(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> (usize, usize, usize) {
        (
            self.new_page.load(Ordering::Relaxed),
            self.read_count.load(Ordering::Relaxed),
            self.write_count.load(Ordering::Relaxed),
        )
    }

    pub fn clear(&self) {
        self.new_page.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
    }
}

pub struct Frames {
    fast_path_victims: ConcurrentQueue<usize>,
    eviction_candidates: [usize; EVICTION_SCAN_DEPTH],
    frames: Vec<BufferFrame>, // The Vec<frames> is fixed size. If not fixed size, then Pin must be used to ensure that the frame does not move when the vector is resized.
}

struct EvictionCandidate {
    candidates: [usize; EVICTION_SCAN_DEPTH],
}

thread_local! {
    static EVICTION_CANDIDATE: RefCell<EvictionCandidate> = RefCell::new(EvictionCandidate {
        candidates: [0; EVICTION_SCAN_DEPTH],
    });
}

pub struct ThreadLocalEvictionCandidate;

impl ThreadLocalEvictionCandidate {
    pub fn choose_eviction_candidate<'a>(
        &self,
        frames: &'a Vec<BufferFrame>,
    ) -> Option<FrameWriteGuard<'a>> {
        let num_frames = frames.len();
        let mut result = None;
        EVICTION_CANDIDATE.with(|c| {
            let mut candidates = c.borrow_mut();
            let eviction_candidates = &mut candidates.candidates;
            for _ in 0..EVICTION_SCAN_TRIALS {
                // Initialize the eviction candidates with max
                for i in 0..EVICTION_SCAN_DEPTH {
                    eviction_candidates[i] = usize::MAX;
                }

                // Generate the eviction candidates.
                // If the number of frames is greater than the scan depth, then generate distinct random numbers.
                // Otherwise, use all the frames as candidates.
                if num_frames > EVICTION_SCAN_DEPTH {
                    // Generate **distinct** random numbers.
                    for _ in 0..3 * EVICTION_SCAN_DEPTH {
                        let rand_idx = gen_random_int(0, num_frames - 1);
                        eviction_candidates[rand_idx % EVICTION_SCAN_DEPTH] = rand_idx;
                        // Use mod to avoid duplicates
                    }
                } else {
                    // Use all the frames as candidates
                    for i in 0..num_frames {
                        eviction_candidates[i] = i;
                    }
                }
                log_debug!("Eviction candidates: {:?}", self.eviction_candidates);

                // Go through the eviction candidates and find the victim
                let mut frame_with_min_score: Option<FrameWriteGuard> = None;
                for i in eviction_candidates.iter() {
                    if i == &usize::MAX {
                        // Skip the invalid index
                        continue;
                    }
                    let frame = frames[*i].try_write(false);
                    if let Some(guard) = frame {
                        if let Some(current_min_score) = frame_with_min_score.as_ref() {
                            if guard.eviction_score() < current_min_score.eviction_score() {
                                frame_with_min_score = Some(guard);
                            } else {
                                // No need to update the min frame
                            }
                        } else {
                            frame_with_min_score = Some(guard);
                        }
                    } else {
                        // Could not acquire the lock. Do not consider this frame.
                    }
                }

                log_debug!("Frame with min score: {:?}", frame_with_min_score);

                #[allow(clippy::manual_map)]
                if let Some(guard) = frame_with_min_score {
                    log_debug!("Victim found @ frame({})", guard.frame_id());
                    result = Some(guard);
                    break;
                } else {
                    log_debug!("All latched");
                    continue;
                }
            }
        });

        result
    }
}

impl Frames {
    pub fn new(num_frames: usize) -> Self {
        let fast_path_victims = ConcurrentQueue::unbounded();
        for i in 0..num_frames {
            fast_path_victims.push(i).unwrap();
        }

        Frames {
            fast_path_victims,
            eviction_candidates: [0; EVICTION_SCAN_DEPTH],
            frames: (0..num_frames)
                .map(|i| BufferFrame::new(i as u32))
                .collect(),
        }
    }

    pub fn reset_free_frames(&self) {
        while self.fast_path_victims.pop().is_ok() {}
        for i in 0..self.frames.len() {
            self.fast_path_victims.push(i).unwrap();
        }
    }

    pub fn push_to_eviction_queue(&self, frame_id: usize) {
        self.fast_path_victims.push(frame_id).unwrap();
    }

    /// Choose a victim frame to be evicted.
    /// If all the frames are latched, then return None.
    pub fn choose_victim(&mut self) -> Option<FrameWriteGuard> {
        log_debug!("Choosing victim");

        // First, try the fast path victims.
        while let Ok(victim) = self.fast_path_victims.pop() {
            let frame = self.frames[victim].try_write(false);
            if let Some(guard) = frame {
                log_debug!("Fast path victim found @ frame({})", guard.frame_id());
                return Some(guard);
            } else {
                // The frame is latched. Try the next frame.
            }
        }

        ThreadLocalEvictionCandidate.choose_eviction_candidate(&self.frames)
    }

    /// Choose multiple victims to be evicted
    /// The returned vector may contain fewer frames thant he requested number of victims.
    /// It can also return an empty vector.
    pub fn choose_victims(&mut self, num_victims: usize) -> Vec<FrameWriteGuard> {
        let mut victims = Vec::with_capacity(num_victims);

        while let Ok(victim) = self.fast_path_victims.pop() {
            let frame = self.frames[victim].try_write(false);
            if let Some(guard) = frame {
                log_debug!("Fast path victim found @ frame({})", guard.frame_id());
                victims.push(guard);
                if victims.len() == num_victims {
                    return victims;
                }
            } else {
                // The frame is latched. Try the next frame.
            }
        }

        while victims.len() < num_victims {
            if let Some(victim) =
                ThreadLocalEvictionCandidate.choose_eviction_candidate(&self.frames)
            {
                victims.push(victim);
            } else {
                break;
            }
        }

        victims
    }
}

impl Deref for Frames {
    type Target = Vec<BufferFrame>;

    fn deref(&self) -> &Self::Target {
        &self.frames
    }
}

impl DerefMut for Frames {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frames
    }
}

/// Buffer pool that manages the buffer frames.
pub struct BufferPool {
    remove_dir_on_drop: bool,
    path: PathBuf,
    latch: RwLatch,
    frames: UnsafeCell<Frames>,
    container_to_file: DashMap<ContainerKey, FileManager>, // c_key -> file_manager
    page_to_frame: UnsafeCell<HashMap<PageKey, usize>>, // (c_key, page_id) -> frame_index
    file_page_counter: UnsafeCell<HashMap<ContainerKey, AtomicUsize>>,
    runtime_stats: RuntimeStats,
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        if self.remove_dir_on_drop {
            std::fs::remove_dir_all(&self.path).unwrap();
        } else {
            // Persist all the pages to disk
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl BufferPool {
    /// Create a new buffer pool with the given number of frames.
    /// Directory structure
    /// * bp_dir
    ///    * db_dir
    ///      * container_file
    ///
    /// The buffer pool will create the bp_dir if it does not exist.
    /// The db_dir and container_file are lazily created when a page is evicted and
    /// a file manager is constructed.
    /// If remove_dir_on_drop is true, then the bp_dir will be removed when the buffer pool is dropped.
    pub fn new<P: AsRef<std::path::Path>>(
        bp_dir: P,
        num_frames: usize,
        remove_dir_on_drop: bool,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("Buffer pool created: num_frames: {}", num_frames);

        // Identify all the directories. A directory corresponds to a database.
        // A file in the directory corresponds to a container.
        // Create a FileManager for each file and store it in the container.
        let container = DashMap::new();
        create_dir_all(&bp_dir).unwrap();
        for entry in std::fs::read_dir(&bp_dir).unwrap() {
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
                        let fm = FileManager::new(&db_path, c_id)?;
                        container.insert(ContainerKey::new(db_id, c_id), fm);
                    }
                }
            }
        }

        Ok(BufferPool {
            remove_dir_on_drop,
            path: bp_dir.as_ref().to_path_buf(),
            latch: RwLatch::default(),
            page_to_frame: UnsafeCell::new(HashMap::new()),
            frames: UnsafeCell::new(Frames::new(num_frames)),
            container_to_file: container,
            file_page_counter: UnsafeCell::new(todo!("file_page_counter")),
            runtime_stats: RuntimeStats::new(),
        })
    }

    pub fn eviction_stats(&self) -> String {
        "Eviction stats not supported".to_string()
    }

    pub fn file_stats(&self) -> String {
        "File stat is disabled".to_string()
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

    // The exclusive latch is NOT NEEDED when calling this function
    fn choose_victim(&self) -> Option<FrameWriteGuard> {
        let frames = unsafe { &mut *self.frames.get() };
        frames.choose_victim()
    }

    // The exclusive latch is NOT NEEDED when calling this function.
    // It is not needed because a victim selection does not modify the shared state
    // such as the page_to_frame mapping.
    fn choose_victims(&self, num_victims: usize) -> Vec<FrameWriteGuard> {
        let frames = unsafe { &mut *self.frames.get() };
        frames.choose_victims(num_victims)
    }

    // The exclusive latch is NOT NEEDED when calling this function
    // This function will write the victim page to disk if it is dirty.
    fn write_victim_to_disk_if_dirty(&self, victim: &mut FrameWriteGuard) -> Result<(), MemPoolStatus> {
        let is_dirty = victim.dirty().load(Ordering::Acquire);

        if let Some(key) = victim.page_key() {
            if is_dirty {
                victim.dirty().store(false, Ordering::Release);
                // Until this file object is dropped, a lock is held on the shard of the map.
                let file = self.container_to_file.entry(key.c_key).or_insert_with(|| {
                    FileManager::new(self.path.join(key.c_key.db_id.to_string()), key.c_key.c_id)
                        .unwrap()
                });
                file.write_page(key.page_id, victim)?;
            }
        }

        Ok(())
    }

    // The exclusive latch must be held when calling this function
    // fn handle_page_fault(
    //     &self,
    //     key: PageKey,
    //     new_page: bool,
    //     location: &mut FrameWriteGuard,
    // ) -> Result<(), MemPoolStatus> {
    //     let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
    //     let container_to_file = unsafe { &mut *self.container_to_file.get() };

    //     let index = location.frame_id();
    //     let is_dirty = location.dirty().load(Ordering::Acquire);

    //     // Evict old page if necessary
    //     if let Some(old_key) = location.page_key() {
    //         if is_dirty {
    //             location.dirty().store(false, Ordering::Release);
    //             let file = container_to_file
    //                 .get(&old_key.c_key)
    //                 .ok_or(MemPoolStatus::FileManagerNotFound)?;

    //             self.runtime_stats.inc_write_count();
    //             file.write_page(old_key.page_id, location)?;
    //         }
    //         page_to_frame.remove(old_key);
    //         log_debug!("Page evicted: {}", old_key);
    //     }

    //     // Create a new page or read from disk
    //     if new_page {
    //         location.set_id(key.page_id);
    //     } else {
    //         let file = container_to_file
    //             .get(&key.c_key)
    //             .ok_or(MemPoolStatus::FileManagerNotFound)?;

    //         self.runtime_stats.inc_read_count();
    //         file.read_page(key.page_id, location)?;
    //     };

    //     page_to_frame.insert(key, index as usize);
    //     *location.page_key_mut() = Some(key);
    //     {
    //         let evict_info = location.evict_info();
    //         evict_info.reset();
    //         evict_info.update();
    //     }

    //     log_debug!("Page loaded: key: {}", key);
    //     Ok(())
    // }
}

impl MemPool for BufferPool {
    /// Create a new page for write in memory.
    /// NOTE: This function does not write the page to disk.
    /// See more at `handle_page_fault(key, new_page=true)`
    /// The newly allocated page is not formatted except for the page id.
    /// The caller is responsible for initializing the page.
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, MemPoolStatus> {
        // 1. Choose victim
        if let Some(mut victim) = self.choose_victim() {
            // 2. Handle eviction if the victim is dirty
            let res = self.write_victim_to_disk_if_dirty(&mut victim);

            match res {
                Ok(()) => {
                    // 3. Modify the page_to_frame mapping. Critical section.
                    // Need to remove the old mapping and insert the new mapping.
                    let page_key = {
                        self.exclusive();
                        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
                        let file_page_counter = unsafe { &mut *self.file_page_counter.get() };
                        // Remove the old mapping
                        if let Some(old_key) = victim.page_key() {
                            page_to_frame.remove(&old_key).unwrap(); // Unwrap is safe because victim's write latch is held. No other thread can remove the old key from page_to_frame before this thread.
                        }
                        // Insert the new mapping
                        let page_id = file_page_counter
                            .entry(c_key)
                            .or_insert_with(|| AtomicUsize::new(0))
                            .fetch_add(1, Ordering::AcqRel) as u32;
                        let index = victim.frame_id();
                        let key = PageKey::new(c_key, page_id);
                        page_to_frame.insert(key, index as usize);
                        self.release_exclusive();
                        key
                    };

                    // 4. Initialize the page
                    victim.set_id(page_key.page_id); // Initialize the page with the page id
                    *victim.page_key_mut() = Some(page_key); // Set the frame key to the new page key
                    victim.dirty().store(true, Ordering::Release);

                    return Ok(victim);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            // Victim Selection failed
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus> {
        assert!(num_pages > 0);
        // 1. Choose victims
        let mut victims = self.choose_victims(num_pages);
        if !victims.is_empty() {

            // 2. Handle eviction if the page is dirty
            for victim in victims.iter_mut() {
                let res = self.write_victim_to_disk_if_dirty(victim);
                if let Err(e) = res {
                    self.release_exclusive();
                    return Err(e);
                }
            }

            let start_page_id = {
                // 3. Modify the page_to_frame mapping. Critical section.
                // Need to remove the old mapping and insert the new mapping.
                self.exclusive();
                let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
                let file_page_counter = unsafe { &mut *self.file_page_counter.get() };

                // Remove the old mapping
                for victim in victims.iter() {
                    if let Some(old_key) = victim.page_key() {
                        page_to_frame.remove(&old_key).unwrap(); // Unwrap is safe because victim's write latch is held. No other thread can remove the old key from page_to_frame before this thread.
                    }
                }

                // Insert the new mapping
                let start_page_id = file_page_counter
                    .entry(c_key)
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(num_pages as usize, Ordering::AcqRel) as u32;
                for i in 0..num_pages {
                    let page_id = start_page_id + i as u32;
                    let key = PageKey::new(c_key, page_id);
                    page_to_frame.insert(key, victims[i].frame_id() as usize);
                }

                self.release_exclusive();
                start_page_id
            };

            // Victim modification will be done outside the critical section
            // as the frame is already write-latched.
            for i in 0..num_pages {
                let page_id = start_page_id + i as u32;
                let key = PageKey::new(c_key, page_id);
                let victim = &mut victims[i];
                victim.set_id(page_id);
                *victim.page_key_mut() = Some(key);
                victim.dirty().store(true, Ordering::Release);
            }

            self.release_exclusive();
            Ok(victims)
        } else {
            // Victims not found
            self.release_exclusive();
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus> {
        log_debug!("Page write: {}", key);

        #[cfg(not(feature = "no_bp_hint"))]
        {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                let guard = frames[frame_id as usize].try_write(false);
                match guard {
                    Some(g) if g.page_key().map(|k| k == key.p_key()).unwrap_or(false) => {
                        // Update the eviction info
                        g.evict_info().update();
                        // Mark the page as dirty
                        g.dirty().store(true, Ordering::Release);
                        log_debug!("Page fast path write: {}", key);
                        return Ok(g);
                    }
                    _ => {}
                }
            }
            // Failed due to one of the following reasons:
            // 1. The page key does not match.
            // 2. The page key is not set (empty frame).
            // 3. The frame is latched.
            // 4. The frame id is out of bounds.
            log_debug!("Page fast path write failed{}", key);
        }

        {
            self.shared();
            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = page_to_frame.get(&key.p_key()) {
                let guard = frames[index].try_write(true);
                match guard {
                    Some(g) => {
                        log_debug!("Page slow path(shared bp latch) write: {}", key);
                        g.evict_info().update();
                        self.release_shared();
                        return Ok(g);
                    }
                    None => {
                        self.release_shared();
                        return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                    }
                }
            }
            self.release_shared();
        }

        self.exclusive();

        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        let frames = unsafe { &mut *self.frames.get() };

        // We need to recheck the page_to_frame because another thread might have inserted the page
        let result = match page_to_frame.get(&key.p_key()) {
            Some(&index) => {
                let guard = frames[index].try_write(true);
                if let Some(g) = &guard {
                    g.evict_info().update();
                }
                guard.ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)
            }
            None => {
                let mut victim = if let Some(location) = self.choose_victim() {
                    location
                } else {
                    self.release_exclusive();
                    return Err(MemPoolStatus::CannotEvictPage);
                };
                let res = self.handle_page_fault(key.p_key(), false, &mut victim);
                // If guard is ok, mark the page as dirty
                if let Ok(()) = res {
                    victim.dirty().store(true, Ordering::Release);
                }
                res.map(|_| victim)
            }
        };
        self.release_exclusive();
        result
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus> {
        log_debug!("Page read: {}", key);

        #[cfg(not(feature = "no_bp_hint"))]
        {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                let guard = frames[frame_id as usize].try_read();
                match guard {
                    Some(g) if g.page_key().map(|k| k == key.p_key()).unwrap_or(false) => {
                        // Update the eviction info
                        g.evict_info().update();
                        log_debug!("Page fast path read: {}", key);
                        return Ok(g);
                    }
                    _ => {}
                }
            }
            // Failed due to one of the following reasons:
            // 1. The page key does not match.
            // 2. The page key is not set (empty frame).
            // 3. The frame is latched.
            // 4. The frame id is out of bounds.
            log_debug!("Page fast path read failed: {}", key);
        };

        {
            self.shared();
            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = page_to_frame.get(&key.p_key()) {
                let guard = frames[index].try_read();
                if let Some(g) = &guard {
                    g.evict_info().update();
                }
                match guard {
                    Some(g) => {
                        g.evict_info().update();
                        self.release_shared();
                        log_debug!("Page slow path(shared bp latch) read: {}", key);
                        return Ok(g);
                    }
                    None => {
                        self.release_shared();
                        return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                    }
                }
            }
            self.release_shared();
        }

        self.exclusive();

        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        let frames = unsafe { &mut *self.frames.get() };

        let result = match page_to_frame.get(&key.p_key()) {
            Some(&index) => {
                let guard = frames[index].try_read();
                if let Some(g) = &guard {
                    g.evict_info().update();
                }
                guard.ok_or(MemPoolStatus::FrameReadLatchGrantFailed)
            }
            None => {
                let mut victim = if let Some(location) = self.choose_victim() {
                    location
                } else {
                    self.release_exclusive();
                    return Err(MemPoolStatus::CannotEvictPage);
                };
                let res = self.handle_page_fault(key.p_key(), false, &mut victim);
                res.map(|_| victim.downgrade())
            }
        };
        self.release_exclusive();
        result
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        #[cfg(not(feature = "new_async_write"))]
        return Ok(());

        #[cfg(feature = "new_async_write")]
        {
            log_debug!("Page prefetch: {}", key);
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                let guard = frames[frame_id as usize].try_read();
                if let Some(g) = &guard {
                    // Check if the page key matches
                    if let Some(page_key) = g.page_key() {
                        if page_key == &key.p_key() {
                            return Ok(());
                        }
                    }
                }
            }

            self.shared();
            let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
            let container_to_file = unsafe { &mut *self.container_to_file.get() };

            if page_to_frame.contains_key(&key.p_key()) {
                // Do nothing
            } else {
                let file = container_to_file
                    .get(&key.p_key().c_key)
                    .ok_or(MemPoolStatus::FileManagerNotFound)
                    .unwrap();
                file.prefetch_page(key.p_key().page_id).unwrap();
            }

            self.release_shared();
            Ok(())
        }
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.shared();

        let frames = unsafe { &*self.frames.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_read() {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            if frame.dirty().swap(false, Ordering::Acquire) {
                // swap is required to avoid concurrent flushes
                let key = frame.page_key().unwrap();
                if let Some(file) = container_to_file.get(&key.c_key) {
                    self.runtime_stats.inc_write_count();
                    file.write_page(key.page_id, &frame)?;
                } else {
                    self.release_shared();
                    return Err(MemPoolStatus::FileManagerNotFound);
                }
            }
        }

        // Call fsync on all the files
        for file in container_to_file.values() {
            file.flush()?;
        }

        self.release_shared();
        Ok(())
    }

    fn fast_evict(&self, frame_id: u32) -> Result<(), MemPoolStatus> {
        // push_to_eviction_queue can be done without buffer pool latch
        // because it is a lock-free operation
        let frames = unsafe { &*self.frames.get() };
        frames.push_to_eviction_queue(frame_id as usize);
        Ok(())
    }

    // Just return the runtime stats
    fn stats(&self) -> MemoryStats {
        let (new_page, read_count, write_count) = self.runtime_stats.get();
        let mut containers = BTreeMap::new();
        for frame in unsafe { &*self.frames.get() }.iter() {
            let frame = frame.read();
            if let Some(key) = frame.page_key() {
                *containers.entry(key.c_key).or_insert(0) += 1;
            }
        }
        MemoryStats {
            num_frames_in_mem: unsafe { &*self.frames.get() }.len(),
            new_page_created: new_page,
            read_page_from_disk: read_count,
            write_page_to_disk: write_count,
            containers,
        }
    }

    // Reset the runtime stats
    fn reset_stats(&self) {
        self.runtime_stats.clear();
    }

    /// Reset the buffer pool to its initial state.
    /// This will write all the dirty pages to disk and flush the files.
    /// After this operation, the buffer pool will have all the frames cleared.
    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };
        let page_to_frame = unsafe { &mut *self.page_to_frame.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
            let mut frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            if frame.dirty().load(Ordering::Acquire) {
                let key = frame.page_key().unwrap();
                if let Some(file) = container_to_file.get(&key.c_key) {
                    self.runtime_stats.inc_write_count();
                    file.write_page(key.page_id, &frame)?;
                } else {
                    self.release_exclusive();
                    return Err(MemPoolStatus::FileManagerNotFound);
                }
            }
            frame.clear();
        }

        // Call fsync on all the files
        for file in container_to_file.values() {
            file.flush()?;
        }

        page_to_frame.clear();

        frames.reset_free_frames();

        self.release_exclusive();
        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            frame.dirty().store(false, Ordering::Release);
        }

        // Call fsync on all the files to ensure that no dirty pages
        // are left in the file manager queue
        for file in container_to_file.values() {
            file.flush()?;
        }

        // container_to_file.clear();
        self.runtime_stats.clear();

        self.release_exclusive();
        Ok(())
    }
}

#[cfg(test)]
impl BufferPool {
    pub fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_page_to_frame();
        self.check_frame_id_and_page_id_match();
    }

    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            frame.try_write(false).unwrap();
        }
    }

    // Invariant: page_to_frame contains all the pages in the buffer pool
    pub fn check_page_to_frame(&self) {
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        let mut index_to_id = HashMap::new();
        for (k, &v) in page_to_frame.iter() {
            index_to_id.insert(v, k);
        }
        let frames = unsafe { &*self.frames.get() };
        for (i, frame) in frames.iter().enumerate() {
            let frame = frame.read();
            if index_to_id.contains_key(&i) {
                assert_eq!(frame.page_key().unwrap(), *index_to_id[&i]);
            } else {
                assert_eq!(frame.page_key(), &None);
            }
        }
        // println!("page_to_frame: {:?}", page_to_frame);
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = frame.read();
            if let Some(key) = frame.page_key() {
                let page_id = frame.get_id();
                assert_eq!(key.page_id, page_id);
            }
        }
    }

    pub fn is_in_buffer_pool(&self, key: PageFrameKey) -> bool {
        let page_to_frame = unsafe { &*self.page_to_frame.get() };
        page_to_frame.contains_key(&key.p_key())
    }
}

unsafe impl Sync for BufferPool {}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::log;
    use crate::log_info;

    use super::*;
    use std::thread::{self};
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        {
            let num_frames = 10;
            let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
            let c_key = ContainerKey::new(db_id, 0);
            let frame = bp.create_new_page_for_write(c_key).unwrap();
            let key = frame.page_frame_key().unwrap();
            drop(frame);

            let num_threads = 3;
            let num_iterations = 80; // Note: u8 max value is 255
            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn(|| {
                        for _ in 0..num_iterations {
                            loop {
                                if let Ok(mut guard) = bp.get_page_for_write(key) {
                                    guard[0] += 1;
                                    break;
                                } else {
                                    // spin
                                    log_info!("Spin");
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    });
                }
            });
            bp.run_checks();
            {
                let guard = bp.get_page_for_read(key).unwrap();
                assert_eq!(guard[0], num_threads * num_iterations);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_write_back_simple() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        {
            let num_frames = 1;
            let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            let key1 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = 1;
                guard.page_frame_key().unwrap()
            };
            let key2 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = 2;
                guard.page_frame_key().unwrap()
            };
            bp.run_checks();
            // check contents of evicted page
            {
                let guard = bp.get_page_for_read(key1).unwrap();
                assert_eq!(guard[0], 1);
            }
            // check contents of the page in the buffer pool
            {
                let guard = bp.get_page_for_read(key2).unwrap();
                assert_eq!(guard[0], 2);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_write_back_many() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        {
            let mut keys = Vec::new();
            let num_frames = 1;
            let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..100 {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = i;
                keys.push(guard.page_frame_key().unwrap());
            }
            bp.run_checks();
            for (i, key) in keys.iter().enumerate() {
                let guard = bp.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_create_new_page() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 2;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let num_traversal = 100;

        let mut count = 0;
        let mut keys = Vec::new();

        for _ in 0..num_traversal {
            let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
            guard1[0] = count;
            count += 1;
            keys.push(guard1.page_frame_key().unwrap());

            let mut guard2 = bp.create_new_page_for_write(c_key).unwrap();
            guard2[0] = count;
            count += 1;
            keys.push(guard2.page_frame_key().unwrap());
        }

        bp.run_checks();

        // Traverse by 2 pages at a time
        for i in 0..num_traversal {
            let guard1 = bp.get_page_for_read(keys[i * 2]).unwrap();
            assert_eq!(guard1[0], i as u8 * 2);
            let guard2 = bp.get_page_for_read(keys[i * 2 + 1]).unwrap();
            assert_eq!(guard2[0], i as u8 * 2 + 1);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_all_frames_latched() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 1;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
        guard1[0] = 1;

        // Try to get a new page for write. This should fail because all the frames are latched.
        let res = bp.create_new_page_for_write(c_key);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(guard1);

        // Now, we should be able to get a new page for write.
        let guard2 = bp.create_new_page_for_write(c_key).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_bp_clear_frames() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 10;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 2 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        bp.run_checks();

        // Clear the buffer pool
        bp.flush_all_and_reset().unwrap();

        bp.run_checks();

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_clear_frames_durable() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 10;
        let bp1 = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 10 {
            let mut guard = bp1.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        bp1.run_checks();

        // Clear the buffer pool
        bp1.flush_all_and_reset().unwrap();

        bp1.run_checks();

        drop(bp1); // Drop will also clear the buffer pool

        // Create a new buffer pool
        let bp2 = BufferPool::new(&temp_dir, num_frames * 2, false).unwrap();

        bp2.run_checks();

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = bp2.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        bp2.run_checks();
    }

    #[test]
    fn test_bp_stats() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 1;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let key_1 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 1;
            guard.page_frame_key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        let key_2 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 2;
            guard.page_frame_key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_1).unwrap();
            assert_eq!(guard[0], 1);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_2).unwrap();
            assert_eq!(guard[0], 2);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);
    }
}
