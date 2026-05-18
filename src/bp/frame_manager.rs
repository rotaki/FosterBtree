//! Shared frame-management core for buffer-pool variants.
//!
//! **Status: shadow implementation** — not yet wired into any BP. The existing
//! `BufferPoolClock`, `PrediCache`, and `TlbBP` continue to own
//! their own frame-management code. This module exists so we can migrate one
//! BP at a time and verify behavioral parity.
//!
//! ## What lives here
//!
//! Translator-agnostic frame management:
//! - Frame array ownership (pages, metas)
//! - Free-frame hint queue (a MPMC queue of "recently empty" frame indices)
//! - Clock hand + `evict_batch` (with a translator hook for cleaning up
//!   page→frame mappings as frames are freed)
//! - `ensure_free_frames` (95% threshold, triggers eviction)
//! - `choose_victim` (pop hint, latch, validate empty; skips stale entries)
//! - Latch primitives (`try_get_read_guard`, `try_get_write_guard`)
//! - Flush primitives (`flush_all`, `flush_all_and_reset`)
//! - BP-level stats (read/write/new-page counters)
//!
//! ## What does NOT live here
//!
//! Anything that reads or mutates the PageKey→frame translation. That lives
//! in each BP variant because:
//! - LIPAH uses `DashMap<PageKey, usize>` (partitioned RwLocks)
//! - PT uses a custom chained hash table with ArcSwap per bucket
//! - TLB-BP uses a per-thread TLB + `CongeeRawU32` ART tree
//!
//! Each translator has distinct concurrency semantics, so the caller owns
//! lookup / insert / remove. `FrameManager` exposes a hook during eviction
//! so the translator can remove entries for frames being freed.

#[allow(unused_imports)]
use crate::log;

use super::{
    eviction_policy::EvictionPolicy,
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    mem_pool_trait::{MemPoolStatus, PageKey},
    mmap_array::{co_located_chunk_elements, HugepageRequest, MmapArray, MmapOptions, NumaPolicy},
};
use crate::{container::ContainerManager, log_debug, log_warn, page::Page};

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use concurrent_queue::ConcurrentQueue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

// ---------------------------------------------------------------------------
// FrameManagerOptions
// ---------------------------------------------------------------------------

/// Shared mmap policy for the page and meta arrays.
///
/// `FrameManager` always uses striped NUMA placement: it computes one shared
/// frame-index chunk size and applies it to both arrays, so `page[i]` and
/// `meta[i]` live on the same NUMA node. Callers can tune hugepage mode and
/// prefaulting, but not accidentally split the arrays differently.
#[derive(Clone, Copy, Debug)]
pub struct FrameManagerOptions {
    pub hugepages: HugepageRequest,
    pub prefault: bool,
}

impl Default for FrameManagerOptions {
    fn default() -> Self {
        Self {
            hugepages: HugepageRequest::Transparent,
            prefault: true,
        }
    }
}


// ---------------------------------------------------------------------------
// FrameManager
// ---------------------------------------------------------------------------

/// Translator-agnostic core used by all buffer-pool variants.
pub struct FrameManager<E: EvictionPolicy> {
    num_frames: usize,
    used_frames: AtomicUsize,
    clock_hand: AtomicUsize,

    /// Free-frame hint queue. Callers may observe stale entries (e.g. a frame
    /// that was in the queue but has been reused by a direct-index code path
    /// such as promotion). `choose_victim` skips those.
    free_list: ConcurrentQueue<u32>,

    /// The actual page data for each frame. Single mmap region; zero-filled
    /// at construction (MAP_ANONYMOUS gives zero pages).
    pages: MmapArray<Page>,

    /// Per-frame metadata (latch, dirty bit, eviction info, page key). Single
    /// mmap region; constructed in place from `FrameMeta::new(i)`.
    metas: MmapArray<FrameMeta<E>>,

    container_manager: Arc<ContainerManager>,
}

// SAFETY: synchronisation is done via per-frame latches.
unsafe impl<E: EvictionPolicy> Sync for FrameManager<E> {}
unsafe impl<E: EvictionPolicy> Send for FrameManager<E> {}

impl<E: EvictionPolicy> FrameManager<E> {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        Self::new_with_opts(num_frames, container_manager, FrameManagerOptions::default())
    }

    pub fn new_with_opts(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
        opts: FrameManagerOptions,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!(
            "FrameManager created: num_frames={}, hugepages={:?}, prefault={}",
            num_frames,
            opts.hugepages,
            opts.prefault,
        );

        debug_assert!(
            num_frames <= u32::MAX as usize,
            "num_frames must fit in u32 (frame ids are u32)"
        );

        let free_list = ConcurrentQueue::bounded(num_frames);
        for i in 0..num_frames {
            free_list.push(i as u32).unwrap();
        }

        let epc = co_located_chunk_elements(
            num_frames,
            &[
                std::mem::size_of::<Page>(),
                std::mem::size_of::<FrameMeta<E>>(),
            ],
        );
        let numa = NumaPolicy::Striped {
            elements_per_chunk: epc,
        };
        let mmap_opts = MmapOptions {
            hugepages: opts.hugepages,
            numa,
            prefault: opts.prefault,
        };

        let pages = MmapArray::<Page>::zeroed(num_frames, mmap_opts)?;
        let metas = MmapArray::<FrameMeta<E>>::new_with(num_frames, mmap_opts, |i| {
            FrameMeta::new(i as u32)
        })?;

        Ok(Self {
            num_frames,
            used_frames: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            free_list,
            pages,
            metas,
            container_manager,
        })
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    #[inline]
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    #[inline]
    pub fn used_frames(&self) -> usize {
        self.used_frames.load(Ordering::Acquire)
    }

    #[inline]
    pub fn container_manager(&self) -> &Arc<ContainerManager> {
        &self.container_manager
    }

    /// Read-only access to frame metadata by index. Callers must be careful:
    /// the returned reference is not synchronised with frame mutations.
    #[inline]
    pub fn meta(&self, idx: u32) -> &FrameMeta<E> {
        unsafe { &*self.metas.get_ptr(idx as usize) }
    }

    /// Issue CPU cache prefetch hints for a frame's meta + page. Best-effort:
    /// silently no-ops if `idx` is out of range.
    #[inline]
    pub fn prefetch_frame(&self, idx: u32) {
        let idx = idx as usize;
        if idx >= self.num_frames {
            return;
        }
        let meta_ptr = self.metas.get_ptr(idx) as *const u8;
        let page_ptr = self.pages.get_ptr(idx) as *const u8;
        unsafe {
            #[cfg(target_arch = "x86_64")]
            {
                std::arch::x86_64::_mm_prefetch(
                    meta_ptr as *const i8,
                    std::arch::x86_64::_MM_HINT_T0,
                );
                std::arch::x86_64::_mm_prefetch(
                    page_ptr as *const i8,
                    std::arch::x86_64::_MM_HINT_T0,
                );
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                std::ptr::read_volatile(meta_ptr);
                std::ptr::read_volatile(page_ptr);
            }
        }
    }

    // ------------------------------------------------------------------
    // Latch primitives
    // ------------------------------------------------------------------

    #[inline]
    pub fn try_get_read_guard(&self, idx: u32) -> Option<FrameReadGuard<E>> {
        let idx = idx as usize;
        FrameReadGuard::try_new(self.metas.get_ptr(idx), self.pages.get_ptr(idx))
    }

    #[inline]
    pub fn try_get_write_guard(&self, idx: u32, make_dirty: bool) -> Option<FrameWriteGuard<E>> {
        let idx = idx as usize;
        FrameWriteGuard::try_new(
            self.metas.get_ptr(idx),
            self.pages.get_ptr(idx),
            make_dirty,
        )
    }

    /// Check if a frame is likely free (no key set, not latched). This is a
    /// hint only — by the time the caller tries to latch, the answer may have
    /// changed. Used by PT's promotion to prefer already-free frames.
    #[inline]
    pub fn frame_is_free(&self, idx: u32) -> bool {
        let meta = self.meta(idx);
        meta.key().is_none() && !meta.latch.is_locked()
    }

    // ------------------------------------------------------------------
    // Free-list management
    // ------------------------------------------------------------------

    #[inline]
    pub fn enqueue_free_frame(&self, idx: u32) {
        // .ok() — the queue is bounded; lost pushes are safe (the clock scan
        // will re-discover the frame on the next eviction).
        self.free_list.push(idx).ok();
    }

    /// Pop from the free-list hint, latch, validate the frame is actually
    /// empty. Skips latch failures and stale entries silently.
    pub fn choose_victim(&self) -> Option<FrameWriteGuard<E>> {
        while let Ok(idx) = self.free_list.pop() {
            if let Some(guard) = self.try_get_write_guard(idx, false) {
                if guard.page_key().is_none() {
                    return Some(guard);
                }
                // else: stale entry — frame was repurposed (e.g., by promotion)
                // without popping the hint. Skip.
            }
            // else: latch unavailable. Skip.
        }
        None
    }

    // ------------------------------------------------------------------
    // Used-frame accounting
    // ------------------------------------------------------------------

    #[inline]
    pub fn increment_used(&self) -> usize {
        self.used_frames.fetch_add(1, Ordering::AcqRel)
    }

    #[inline]
    pub fn decrement_used(&self) {
        self.used_frames.fetch_sub(1, Ordering::AcqRel);
    }

    #[inline]
    fn fetch_add_clock_hand(&self, increment: usize) -> usize {
        self.clock_hand
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some((cur + increment) % self.num_frames)
            })
            .expect("clock hand update should not fail")
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /// Called by callers when the BP may be near full. Triggers a clock-scan
    /// eviction batch when used frames exceed 95% of capacity. The `on_evict`
    /// hook is called for each (page_key, frame_idx) pair being freed so the
    /// translator can remove its mapping.
    pub fn ensure_free_frames<F>(&self, batch_size: usize, on_evict: F) -> Result<(), MemPoolStatus>
    where
        F: Fn(&PageKey, u32),
    {
        let used = self.used_frames.load(Ordering::Acquire);
        let ratio = used as f64 / self.num_frames as f64;
        if ratio > 0.95 {
            log_warn!(
                "[FrameMgr-EVICT] Used frames: {}/{} ({:.1}%). Evicting...",
                used,
                self.num_frames,
                ratio * 100.0,
            );
            self.evict_batch(batch_size, on_evict)
        } else {
            Ok(())
        }
    }

    /// Collect candidates via clock scan, flush dirty, latch clean, finalize.
    /// Always returns `Ok(())` when the scan found candidates (even if none
    /// could be freed due to latch contention); `Err(CannotEvictPage)` only
    /// when the scan failed to find any candidate in two full passes.
    pub fn evict_batch<F>(&self, batch_size: usize, on_evict: F) -> Result<(), MemPoolStatus>
    where
        F: Fn(&PageKey, u32),
    {
        let batch = std::cmp::min(self.num_frames, batch_size.max(1));
        let max_iter = 2 * self.num_frames / batch;

        let mut clean_pages: Vec<(u32, *mut FrameMeta<E>)> = Vec::new();
        let mut dirty_pages: Vec<(u32, FrameReadGuard<E>)> = Vec::new();
        let mut to_evict: Vec<(u32, FrameWriteGuard<E>)> = Vec::new();

        // ─── 1. Collect candidates via clock scan ──────────────────────
        let mut iters = 0;
        while clean_pages.len() + dirty_pages.len() < batch {
            if iters > max_iter {
                if clean_pages.is_empty() && dirty_pages.is_empty() {
                    return Err(MemPoolStatus::CannotEvictPage);
                }
                break;
            }
            let start = self.fetch_add_clock_hand(batch);
            for offset in 0..batch {
                let idx = ((start + offset) % self.num_frames) as u32;
                self.classify_frame(idx, &mut clean_pages, &mut dirty_pages);
            }
            iters += 1;
        }

        // ─── 2. Flush dirty pages under read latch ────────────────────
        for (_, g) in &dirty_pages {
            self.write_victim_to_disk_if_dirty_r(g).unwrap();
        }

        // ─── 3. Latch clean pages for eviction ────────────────────────
        for (idx, meta) in clean_pages.drain(..) {
            if let Some(g) = FrameWriteGuard::try_new(
                meta,
                self.pages.get_ptr(idx as usize),
                false,
            ) {
                if g.page_key().is_none() {
                    continue;
                }
                self.write_victim_to_disk_if_dirty_w(&g).unwrap();
                to_evict.push((idx, g));
            }
        }

        // ─── 4. Upgrade dirty page latches (read → write) ─────────────
        for (idx, g) in dirty_pages.drain(..) {
            if let Ok(gw) = g.try_upgrade(false) {
                to_evict.push((idx, gw));
            }
        }

        // ─── 5. Notify translator, clear frames, return to free list ──
        let mut freed = 0;
        for (idx, g) in to_evict.drain(..) {
            if let Some(pk) = g.page_key() {
                on_evict(&pk, idx);
            }
            g.set_page_key(None);
            g.evict_info().reset();
            self.enqueue_free_frame(idx);
            freed += 1;
        }

        // Always decrement (no-op for 0). Always return Ok if we got past the
        // collect phase — eviction failure surfaces via choose_victim later.
        self.used_frames.fetch_sub(freed, Ordering::AcqRel);
        Ok(())
    }

    /// Classify a single frame during clock scan. Textbook second-chance:
    ///   - access calls `update()` → mark = true ("recently accessed")
    ///   - scan: if mark==true → `reset()` (clear, give second chance), skip
    ///   - scan: if mark==false (no access since prior scan) → candidate
    #[inline]
    fn classify_frame(
        &self,
        index: u32,
        clean: &mut Vec<(u32, *mut FrameMeta<E>)>,
        dirty: &mut Vec<(u32, FrameReadGuard<E>)>,
    ) {
        let idx = index as usize;
        let meta_ptr = self.metas.get_ptr(idx);
        let meta: &mut FrameMeta<E> = unsafe { &mut *meta_ptr };

        // Skip empty or latched frames immediately.
        if meta.key().is_none() || meta.latch.is_locked() {
            return;
        }

        let marked = meta.evict_info.score() > 0;
        if marked {
            meta.evict_info.reset();
            return;
        }

        let is_dirty = meta.is_dirty.load(Ordering::Acquire);
        if is_dirty {
            if let Some(g) = FrameReadGuard::try_new(meta_ptr, self.pages.get_ptr(idx)) {
                if g.page_key().is_some() {
                    dirty.push((index, g));
                }
            }
        } else {
            clean.push((index, meta_ptr));
        }
    }

    // ------------------------------------------------------------------
    // Disk I/O (called during eviction)
    // ------------------------------------------------------------------

    fn write_victim_to_disk_if_dirty_w(
        &self,
        guard: &FrameWriteGuard<E>,
    ) -> Result<(), MemPoolStatus> {
        if let Some(key) = guard.page_key() {
            if guard
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.container_manager.get_container(key.c_key);
                container.write_page(key.page_id, guard)?;
            }
        }
        Ok(())
    }

    fn write_victim_to_disk_if_dirty_r(
        &self,
        guard: &FrameReadGuard<E>,
    ) -> Result<(), MemPoolStatus> {
        if let Some(key) = guard.page_key() {
            if guard
                .dirty()
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let container = self.container_manager.get_container(key.c_key);
                container.write_page(key.page_id, guard)?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// Flush all dirty frames to disk. Holds read latches; spins on contention.
    pub fn flush_all(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let frame = loop {
                if let Some(g) = self.try_get_read_guard(i as u32) {
                    break g;
                }
                std::hint::spin_loop();
            };
            self.write_victim_to_disk_if_dirty_r(&frame).unwrap();
        });
        self.container_manager.flush_all()?;
        Ok(())
    }

    /// Flush all, then clear every frame and re-populate the free list. The
    /// `on_reset` hook is called for each resident (page_key, frame_idx) pair
    /// so the translator can clear its mapping table. The frame index lets
    /// translators that allow multiple keys per index (e.g. PT promotion races)
    /// guard with `if mapping.get(pk) == Some(idx)` before removing.
    pub fn flush_all_and_reset<F>(&self, on_reset: F) -> Result<(), MemPoolStatus>
    where
        F: Fn(&PageKey, u32) + Sync,
    {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let mut frame = loop {
                if let Some(g) = self.try_get_write_guard(i as u32, false) {
                    break g;
                }
                std::hint::spin_loop();
            };
            self.write_victim_to_disk_if_dirty_w(&frame).unwrap();
            if let Some(pk) = frame.page_key() {
                on_reset(&pk, i as u32);
            }
            frame.clear();
        });

        self.container_manager.flush_all()?;

        while self.free_list.pop().is_ok() {}
        for i in 0..self.num_frames {
            self.free_list.push(i as u32).unwrap();
        }
        self.used_frames.store(0, Ordering::Release);

        Ok(())
    }

    /// Clear the dirty flag on all frames. Used for testing / benchmarking
    /// workload switches.
    pub fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let meta = unsafe { &*self.metas.get_ptr(i) };
            meta.is_dirty.store(false, Ordering::Release);
        });
        self.container_manager.flush_all()?;
        Ok(())
    }
}
