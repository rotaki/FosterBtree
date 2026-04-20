//! Predictive Translation Buffer Pool
//!
//! **Unified translation** (paper/C++ style): one hash table holds all page→frame
//! mappings. Lookup = `overflow.lookup_with_bucket(key, pref)` (bucket head =
//! preferred slot, chain = overflow). Fault always inserts into overflow; evict
//! removes when this frame is still the entry for that page.
//!
//! ## Paper alignment
//!
//! - [x] **Promotion / demotion** (Sec 3.2, 5.1): after the second access to a
//!       page NOT in its preferred frame, probabilistically promote it on write
//!       (move to preferred frame if free, or swap with occupant and demote).
//!       Probabilities: 1/50 (no demotion) or 1/512 (demotion needed).
//!
//! - [x] **Promotion** (Sec 3.2): any page found in an overflow slot is already
//!       resident (≥ 2nd access), so it is immediately eligible for probabilistic
//!       promotion on the write path. No per-page counter needed (paper-aligned).
//!
//! - [x] **Lock-free overflow reads** (Sec 4.2): overflow table uses version +
//!       ArcSwap per bucket; read path has no mutex so lookup can overlap with
//!       preferred-frame load.
//!
//! - [x] **Inlined chaining overflow table** (Sec 4.1): custom table with first
//!       slot inlined per bucket. Frame metadata remains in `metas` (no header
//!       in hash entry yet).
//!
//! - [x] **Single lookup**: one overflow lookup per access (bucket = preferred slot).
//!
//! - [ ] **Benchmarks**: TPC-C + YCSB (uniform & skewed) vs BufferPoolClock /
//!       VMCachePool. Track throughput, IPC, L2 misses, promotion overhead.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{box_as_mut_ptr, FrameMeta, FrameReadGuard, FrameWriteGuard},
    hash::{hash_page_key, hash_page_key_2, hash_page_key_3, hash_page_key_4, hash_page_key_u128},
    macro_profile::{report as macro_profile_report, scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};
use crate::random::small_thread_rng;
use crate::{
    container::ContainerManager,
    log_debug, log_warn,
    page::{Page, PageId},
};
use rand::RngCore;

use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use concurrent_queue::ConcurrentQueue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

// ---------------------------------------------------------------------------
// Sub-step profiling / counters (feature = "pt_profile" / "pt_counts")
// ---------------------------------------------------------------------------

#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
use std::sync::atomic::AtomicU64;

/// Cumulative counters for sub-step timing within PT page accesses.
/// All `_ns` fields are cumulative nanoseconds across all threads.
#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
pub struct PTProfileCounters {
    // -- timing (cumulative ns) --
    #[cfg(feature = "pt_profile")]
    pub ensure_free_ns: AtomicU64,
    #[cfg(feature = "pt_profile")]
    pub hash_preferred_ns: AtomicU64,
    #[cfg(feature = "pt_profile")]
    pub overflow_lookup_ns: AtomicU64,
    #[cfg(feature = "pt_profile")]
    pub latch_ns: AtomicU64,
    #[cfg(feature = "pt_profile")]
    pub promotion_check_ns: AtomicU64,
    #[cfg(feature = "pt_profile")]
    pub fault_ns: AtomicU64,
    // -- counts --
    pub preferred_frame_hits: AtomicU64,
    pub fast_return_read_hits: AtomicU64,
    pub fast_return_read_ns: AtomicU64,
    pub fast_return_meta_check_ns: AtomicU64,
    pub fast_return_latch_ns: AtomicU64,
    pub fast_return_revalidate_ns: AtomicU64,
    pub fast_return_evict_update_ns: AtomicU64,
    pub overflow_chain_hits: AtomicU64,
    pub page_faults: AtomicU64,
    pub total_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub promotions_attempted: AtomicU64,
    pub promotions_fired: AtomicU64,
}

#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
impl PTProfileCounters {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "pt_profile")]
            ensure_free_ns: AtomicU64::new(0),
            #[cfg(feature = "pt_profile")]
            hash_preferred_ns: AtomicU64::new(0),
            #[cfg(feature = "pt_profile")]
            overflow_lookup_ns: AtomicU64::new(0),
            #[cfg(feature = "pt_profile")]
            latch_ns: AtomicU64::new(0),
            #[cfg(feature = "pt_profile")]
            promotion_check_ns: AtomicU64::new(0),
            #[cfg(feature = "pt_profile")]
            fault_ns: AtomicU64::new(0),
            preferred_frame_hits: AtomicU64::new(0),
            fast_return_read_hits: AtomicU64::new(0),
            fast_return_read_ns: AtomicU64::new(0),
            fast_return_meta_check_ns: AtomicU64::new(0),
            fast_return_latch_ns: AtomicU64::new(0),
            fast_return_revalidate_ns: AtomicU64::new(0),
            fast_return_evict_update_ns: AtomicU64::new(0),
            overflow_chain_hits: AtomicU64::new(0),
            page_faults: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            promotions_attempted: AtomicU64::new(0),
            promotions_fired: AtomicU64::new(0),
        }
    }

    pub fn print(&self) {
        let r = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let total_accesses = r(&self.total_reads) + r(&self.total_writes);
        println!("\n=== PT Access Profile ===");
        println!(
            "Total accesses:       {:>12}  (reads: {}, writes: {})",
            total_accesses,
            r(&self.total_reads),
            r(&self.total_writes)
        );
        println!(
            "Preferred frame hits: {:>12}  ({:.1}%)",
            r(&self.preferred_frame_hits),
            r(&self.preferred_frame_hits) as f64 / total_accesses.max(1) as f64 * 100.0
        );
        println!(
            "Fast-return read hits:{:>12}",
            r(&self.fast_return_read_hits)
        );
        println!(
            "Overflow chain hits:  {:>12}  ({:.1}%)",
            r(&self.overflow_chain_hits),
            r(&self.overflow_chain_hits) as f64 / total_accesses.max(1) as f64 * 100.0
        );
        println!(
            "Page faults:          {:>12}  ({:.1}%)",
            r(&self.page_faults),
            r(&self.page_faults) as f64 / total_accesses.max(1) as f64 * 100.0
        );
        println!(
            "Promotions attempted: {:>12}",
            r(&self.promotions_attempted)
        );
        println!("Promotions fired:     {:>12}", r(&self.promotions_fired));

        #[cfg(feature = "pt_profile")]
        {
            let total_timed_ns = r(&self.ensure_free_ns)
                + r(&self.hash_preferred_ns)
                + r(&self.overflow_lookup_ns)
                + r(&self.latch_ns)
                + r(&self.promotion_check_ns)
                + r(&self.fault_ns);
            let fmt = |ns: u64, count: u64| -> String {
                if count == 0 {
                    return "N/A".to_string();
                }
                let avg = ns as f64 / count as f64;
                if avg >= 1000.0 {
                    format!(
                        "{:>8.2} us  ({:>5.1}%)",
                        avg / 1000.0,
                        ns as f64 / total_timed_ns as f64 * 100.0
                    )
                } else {
                    format!(
                        "{:>8.1} ns  ({:>5.1}%)",
                        avg,
                        ns as f64 / total_timed_ns as f64 * 100.0
                    )
                }
            };

            println!();
            println!("Per-access avg latency breakdown (cumulative / total_accesses):");
            println!(
                "  ensure_free_frames: {}",
                fmt(r(&self.ensure_free_ns), total_accesses)
            );
            println!(
                "  hash + preferred:   {}",
                fmt(r(&self.hash_preferred_ns), total_accesses)
            );
            println!(
                "  overflow lookup:    {}",
                fmt(r(&self.overflow_lookup_ns), total_accesses)
            );
            println!(
                "  latch acquire:      {}",
                fmt(r(&self.latch_ns), total_accesses)
            );
            println!(
                "  promotion check:    {}",
                fmt(r(&self.promotion_check_ns), total_accesses)
            );
            println!(
                "  page fault:         {}",
                fmt(r(&self.fault_ns), r(&self.page_faults))
            );
            println!("  ---");
            println!(
                "  total timed:        {}",
                fmt(total_timed_ns, total_accesses)
            );
        }
        println!("=====================\n");

        let fast_return_hits = r(&self.fast_return_read_hits);
        if fast_return_hits > 0 {
            let avg = r(&self.fast_return_read_ns) as f64 / fast_return_hits as f64;
            if avg >= 1000.0 {
                println!(
                    "Fast-return read hit avg: {:.2} us over {} hits",
                    avg / 1000.0,
                    fast_return_hits
                );
            } else {
                println!(
                    "Fast-return read hit avg: {:.1} ns over {} hits",
                    avg, fast_return_hits
                );
            }
            let fmt_stage = |ns: u64| -> String {
                let avg = ns as f64 / fast_return_hits as f64;
                if avg >= 1000.0 {
                    format!("{:.2} us", avg / 1000.0)
                } else {
                    format!("{:.1} ns", avg)
                }
            };
            println!(
                "  meta check:         {}",
                fmt_stage(r(&self.fast_return_meta_check_ns))
            );
            println!(
                "  latch acquire:      {}",
                fmt_stage(r(&self.fast_return_latch_ns))
            );
            println!(
                "  post-latch verify:  {}",
                fmt_stage(r(&self.fast_return_revalidate_ns))
            );
            println!(
                "  evict update:       {}",
                fmt_stage(r(&self.fast_return_evict_update_ns))
            );
            println!();
        }
    }
}

// ---------------------------------------------------------------------------
// Type aliases (mirrors the convention in buffer_pool_clock.rs)
// ---------------------------------------------------------------------------
type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

use super::overflow_table::OverflowTable;

// ---------------------------------------------------------------------------
// Per-page fault claim (ensures only one thread faults a given page at a time)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// PredictiveTranslationBP
// ---------------------------------------------------------------------------

/// Map a 64-bit hash uniformly to `[0, n)` without division.
///
/// Uses the "fastrange" trick (Lemire): `(hash * n) >> 64`.
/// Widening u64×u64 → u128 multiply + shift (~3-4 cycles) vs `div`.
#[inline(always)]
fn fastmod(hash: u64, n: u64) -> u32 {
    (((hash as u128).wrapping_mul(n as u128)) >> 64) as u32
}

/// Like `fastmod` but for 32-bit hashes and 32-bit ranges.
///
/// Uses a u32×u32 → u64 widening multiply — a single `mul` instruction on
/// x86_64, cheaper than the u64 version which needs a 128-bit widening mul.
#[inline(always)]
pub(crate) fn fastmod32(hash: u32, n: u32) -> u32 {
    (((hash as u64).wrapping_mul(n as u64)) >> 32) as u32
}

/// Buffer pool using predictive translation.
pub struct PredictiveTranslationBP {
    pub(crate) num_frames: usize,
    num_frames_u64: u64, // cached as u64 for 64-bit fastmod
    num_frames_u32: u32, // cached as u32 for 32-bit fastmod (single-hash variant)
    pub(crate) used_frames: AtomicUsize,
    clock_hand: AtomicUsize,
    pub(crate) container_manager: Arc<ContainerManager>,
    /// Free-frame hint queue. Metadata is the source of truth; the queue is only a hint.
    free_list: ConcurrentQueue<usize>,
    /// The actual page data for each frame.
    #[allow(clippy::vec_box)]
    pub(crate) pages: UnsafeCell<Vec<Box<Page>>>,
    /// Per-frame metadata (latch, dirty bit, eviction info, page key).
    #[allow(clippy::vec_box)]
    pub(crate) metas: UnsafeCell<Vec<Box<FMeta>>>,
    /// Overflow table: maps PageKey -> frame index for pages NOT in their
    /// preferred frame.  Pages in their preferred frame are found via tag check.
    pub(crate) overflow: OverflowTable,
    /// Runtime statistics.
    pub(crate) stats: BPStats,
    /// PT access counters; timing fields are only active with `pt_profile`.
    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    pub profile: PTProfileCounters,
}

// SAFETY: synchronisation is done via per-frame latches and the translation table.
unsafe impl Sync for PredictiveTranslationBP {}
unsafe impl Send for PredictiveTranslationBP {}

impl Drop for PredictiveTranslationBP {
    fn drop(&mut self) {
        if self.container_manager.remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl PredictiveTranslationBP {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("PredictiveTranslationBP created: num_frames={}", num_frames);

        let free_list = ConcurrentQueue::bounded(num_frames);
        for i in 0..num_frames {
            free_list.push(i).unwrap();
        }

        let pages: UnsafeCell<Vec<Box<Page>>> = UnsafeCell::new(
            (0..num_frames)
                .into_par_iter()
                .map(|_| Box::new(Page::new_empty()))
                .collect(),
        );

        let metas: UnsafeCell<Vec<Box<FMeta>>> = UnsafeCell::new(
            (0..num_frames)
                .into_par_iter()
                .map(|i| Box::new(FMeta::new(i as u32)))
                .collect(),
        );

        debug_assert!(
            num_frames <= u32::MAX as usize,
            "num_frames must fit in u32 (frame ids are u32)"
        );

        Ok(Self {
            num_frames,
            num_frames_u64: num_frames as u64,
            num_frames_u32: num_frames as u32,
            used_frames: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            container_manager,
            free_list,
            pages,
            metas,
            overflow: OverflowTable::new(num_frames),

            stats: BPStats::new(),
            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
            profile: PTProfileCounters::new(),
        })
    }

    // ------------------------------------------------------------------
    // Deterministic placement
    // ------------------------------------------------------------------

    /// Compute the preferred frame index for a page key (single hash).
    ///
    /// Returns `u32` because frame ids are `u32`; cast to `usize` at indexing
    /// sites.
    #[inline]
    pub(crate) fn preferred_frame(&self, key: &PageKey) -> u32 {
        fastmod(hash_page_key(key), self.num_frames_u64)
    }

    /// Compute two preferred frame indices for a page key (two hashes).
    #[inline]
    pub(crate) fn preferred_frames(&self, key: &PageKey) -> (u32, u32) {
        let p1 = fastmod(hash_page_key(key), self.num_frames_u64);
        let p2 = fastmod(hash_page_key_2(key), self.num_frames_u64);
        (p1, p2)
    }

    /// Compute four preferred frame indices for a page key using **four**
    /// independent 64-bit hashes.
    #[inline]
    pub(crate) fn preferred_frames_four(&self, key: &PageKey) -> [u32; 4] {
        let n = self.num_frames_u64;
        [
            fastmod(hash_page_key(key), n),
            fastmod(hash_page_key_2(key), n),
            fastmod(hash_page_key_3(key), n),
            fastmod(hash_page_key_4(key), n),
        ]
    }

    /// Compute four preferred frame indices for a page key using **one**
    /// 128-bit hash split into four u32 chunks.
    ///
    /// Expected to be faster than `preferred_frames_four`:
    ///   * 1 u64 mix + 1 widening u64×u128 mul (~4 `mul` instructions total)
    ///   * 4 cheap u32 `fastmod`s (1 `mul` each)
    ///
    /// vs `preferred_frames_four`:
    ///   * 4 u64 mixes (~8 `mul` total)
    ///   * 4 u64 `fastmod`s (each needing a 128-bit widening mul, ~2 `mul` each)
    #[inline]
    pub(crate) fn preferred_frames_four_single_hash(&self, key: &PageKey) -> [u32; 4] {
        let h128 = hash_page_key_u128(key);
        let n = self.num_frames_u32;
        [
            fastmod32(h128 as u32, n),
            fastmod32((h128 >> 32) as u32, n),
            fastmod32((h128 >> 64) as u32, n),
            fastmod32((h128 >> 96) as u32, n),
        ]
    }

    /// Returns true if frame `idx` has no page (key is None). Lock-free read of atomic key.
    #[inline]
    pub(crate) fn frame_is_free(&self, idx: usize) -> bool {
        let metas = unsafe { &*self.metas.get() };
        metas[idx].key().is_none()
    }

    #[inline]
    #[allow(dead_code)]
    fn frame_matches_page(&self, idx: usize, key: &PageKey) -> bool {
        let metas = unsafe { &*self.metas.get() };
        metas[idx].key() == Some(*key)
    }

    /// Hint the CPU toward the predicted frame before translation resolves.
    #[inline]
    #[allow(dead_code)]
    fn prefetch_predicted_frame(&self, idx: usize) {
        self.prefetch_predicted_frames(idx, idx);
    }

    /// Hint the CPU toward the predicted frames before translation resolves.
    #[inline]
    pub(crate) fn prefetch_predicted_frames(&self, first: usize, second: usize) {
        let metas = unsafe { &*self.metas.get() };
        let pages = unsafe { &*self.pages.get() };

        #[cfg(target_arch = "x86_64")]
        unsafe {
            let meta_ptr = (&*metas[first]) as *const FMeta as *const i8;
            let page_ptr = (&*pages[first]) as *const Page as *const i8;
            _mm_prefetch(meta_ptr, _MM_HINT_T0);
            _mm_prefetch(page_ptr, _MM_HINT_T0);
            if second != first {
                let meta_ptr = (&*metas[second]) as *const FMeta as *const i8;
                let page_ptr = (&*pages[second]) as *const Page as *const i8;
                _mm_prefetch(meta_ptr, _MM_HINT_T0);
                _mm_prefetch(page_ptr, _MM_HINT_T0);
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = (&metas[first], &pages[first], &metas[second], &pages[second]);
        }
    }

    pub fn eviction_stats(&self) -> String {
        "PredictiveTranslation: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "PredictiveTranslation: file stats disabled".to_string()
    }

    // ------------------------------------------------------------------
    // Frame access helpers
    // ------------------------------------------------------------------

    pub(crate) fn try_get_read_guard(&self, index: usize) -> Option<FRGuard> {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FRGuard::try_new(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
        )
    }

    pub(crate) fn try_get_write_guard(&self, index: usize, make_dirty: bool) -> Option<FWGuard> {
        let metas = unsafe { &mut *self.metas.get() };
        let pages = unsafe { &mut *self.pages.get() };
        FWGuard::try_new(
            box_as_mut_ptr(&mut metas[index]),
            box_as_mut_ptr(&mut pages[index]),
            make_dirty,
        )
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /// Return frame `idx` to the free-frame hint queue.
    #[inline]
    pub(crate) fn enqueue_free_frame(&self, idx: usize) {
        self.free_list.push(idx).ok();
    }

    /// Try to get a free frame. If `preferred` is Some(p), try to take that frame first
    /// (so pages are placed in their preferred frame when free — paper §3.1).
    pub(crate) fn choose_victim(&self, preferred: Option<usize>) -> Option<FWGuard> {
        // Prefer the preferred frame when it is free.
        if let Some(p) = preferred {
            if let Some(guard) = self.try_get_write_guard(p, false) {
                if guard.page_key().is_none() {
                    return Some(guard);
                }
            }
        }
        // Pop from free list hints until we get a usable free frame.
        while let Ok(idx) = self.free_list.pop() {
            if let Some(guard) = self.try_get_write_guard(idx, false) {
                if guard.page_key().is_none() {
                    return Some(guard);
                }
            }
        }
        None
    }

    pub(crate) fn ensure_free_frames(&self) -> Result<(), MemPoolStatus> {
        let used = self.used_frames.load(Ordering::Acquire);
        let ratio = used as f64 / self.num_frames as f64;
        if ratio > 0.95 {
            log_warn!(
                "[PT-EVICT] Used frames: {}/{} ({:.1}%). Evicting...",
                used,
                self.num_frames,
                ratio * 100.0,
            );
            self.evict_batch()
        } else {
            Ok(())
        }
    }

    fn fetch_add_clock_hand(&self, increment: usize) -> usize {
        self.clock_hand
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some((cur + increment) % self.num_frames) // cold path, plain mod ok
            })
            .expect("clock hand update should not fail")
    }

    fn evict_batch(&self) -> Result<(), MemPoolStatus> {
        let batch = std::cmp::min(self.num_frames, 64);
        let max_iter = 2 * self.num_frames / batch;

        // Scratch space for multi-stage eviction.
        let mut clean_pages: Vec<(usize, *mut FMeta)> = Vec::new();
        let mut dirty_pages: Vec<(usize, FRGuard)> = Vec::new();
        let mut to_evict: Vec<(usize, FWGuard)> = Vec::new();

        // ─── 1. Collect candidates via clock scan ────────────────────
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
                let idx = (start + offset) % self.num_frames;
                let meta = &mut unsafe { &mut *self.metas.get() }[idx];

                if meta.key().is_none() || meta.latch.is_locked() {
                    continue;
                }

                // Clock: if marked, clear mark and skip. If unmarked, candidate.
                if meta.evict_info.score() > 0 {
                    meta.evict_info.reset();
                    continue;
                }

                let is_dirty = meta.is_dirty.load(Ordering::Acquire);
                if is_dirty {
                    if let Some(g) = self.try_get_read_guard(idx) {
                        if g.page_key().is_some() {
                            dirty_pages.push((idx, g));
                        }
                    }
                } else {
                    clean_pages.push((idx, box_as_mut_ptr(meta)));
                }
            }
            iters += 1;
        }

        // ─── 2. Flush dirty pages under read latch ───────────────────
        for (_, g) in &dirty_pages {
            self.write_to_disk_if_dirty_r(g).unwrap();
        }

        // ─── 3. Latch clean pages for eviction ──────────────────────
        for (idx, meta) in clean_pages.drain(..) {
            if let Some(g) = FWGuard::try_new(
                meta,
                box_as_mut_ptr(&mut unsafe { &mut *self.pages.get() }[idx]),
                false,
            ) {
                if g.page_key().is_none() {
                    continue;
                }
                self.write_to_disk_if_dirty_w(&g).unwrap();
                to_evict.push((idx, g));
            }
        }

        // ─── 4. Upgrade dirty page latches (read → write) ───────────
        for (idx, g) in dirty_pages.drain(..) {
            if let Ok(gw) = g.try_upgrade(false) {
                to_evict.push((idx, gw));
            }
        }

        // ─── 5. Remove from overflow and finalize ───────────────────
        let mut freed = 0;
        for (idx, g) in to_evict.drain(..) {
            if let Some(pk) = g.page_key() {
                if self.overflow.lookup(&pk) == Some(idx) {
                    self.overflow.remove(&pk);
                }
            }
            g.set_page_key(None);
            g.evict_info().reset();
            self.enqueue_free_frame(idx);
            freed += 1;
        }

        if freed > 0 {
            self.used_frames.fetch_sub(freed, Ordering::AcqRel);
            Ok(())
        } else {
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    // ------------------------------------------------------------------
    // Page fault (shared by get_page_for_read and get_page_for_write)
    // ------------------------------------------------------------------

    /// Load a page from disk into a frame.  Returns a write guard on the
    /// newly-loaded frame.
    ///
    /// We obtain a frame via `choose_victim(Some(pref))`, so we use the preferred
    /// frame when it's free (paper §3.1). Otherwise we take any free frame and
    /// add to the overflow table.
    pub(crate) fn handle_page_fault_write<const N: usize>(
        &self,
        page_key: PageKey,
        prefs: [usize; N],
    ) -> Result<FWGuard, MemPoolStatus> {
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        // Pick the first free preferred frame, or None to avoid latching occupied frames.
        let chosen_pref = prefs.iter().find(|&&p| self.frame_is_free(p)).copied();

        let mut victim = match self.choose_victim(chosen_pref) {
            Some(v) => v,
            None => {
                self.used_frames.fetch_sub(1, Ordering::AcqRel);
                return Err(MemPoolStatus::CannotEvictPage);
            }
        };

        debug_assert!(victim.page_key().is_none());

        // Atomic insert-if-absent: if another thread already faulted this
        // page, keep existing and retry.
        if self
            .overflow
            .try_insert(page_key, victim.frame_id() as usize)
            .is_err()
        {
            self.enqueue_free_frame(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::RetryPageFault);
        }

        victim.set_page_key(Some(page_key));

        // Read the page from disk.
        if let Err(e) = self
            .container_manager
            .get_container(page_key.c_key)
            .read_page(page_key.page_id, &mut victim)
        {
            victim.set_page_key(None);
            self.overflow.remove(&page_key);
            self.enqueue_free_frame(victim.frame_id() as usize);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        victim.dirty().store(true, Ordering::Release);

        Ok(victim)
    }

    // ------------------------------------------------------------------
    // Disk I/O helpers
    // ------------------------------------------------------------------

    fn write_to_disk_if_dirty_w(&self, guard: &FWGuard) -> Result<(), MemPoolStatus> {
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

    fn write_to_disk_if_dirty_r(&self, guard: &FRGuard) -> Result<(), MemPoolStatus> {
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
    // Promotion / demotion (§3.2, 5.1)
    // ------------------------------------------------------------------

    /// Paper probabilities: 1/50 when preferred frame is free, 1/512 when demotion needed.
    const PROMOTE_PROB_NO_DEMOTE: u32 = 50;
    const PROMOTE_PROB_DEMOTE: u32 = 512;

    /// Try to promote the page from its current (overflow) frame to one of its
    /// preferred frames. Caller holds a write guard on the overflow frame.
    /// Returns either the same guard (no promotion / failed) or a new write guard
    /// on a preferred frame (promotion done).
    fn try_promote<const N: usize>(
        &self,
        mut current_guard: FWGuard,
        page_key: PageKey,
        prefs: [usize; N],
    ) -> Result<FWGuard, MemPoolStatus> {
        let current_idx = current_guard.frame_id() as usize;
        if prefs.contains(&current_idx) {
            return Ok(current_guard);
        }

        // First pass: look for a free preferred frame.
        let mut victim_guard: Option<FWGuard> = None;
        for &p in &prefs {
            if self.frame_is_free(p) {
                if let Some(g) = self.try_get_write_guard(p, false) {
                    victim_guard = Some(g);
                    break;
                }
            }
        }

        // Second pass: if no free frame found, latch any preferred frame.
        if victim_guard.is_none() {
            for &p in &prefs {
                if let Some(g) = self.try_get_write_guard(p, false) {
                    victim_guard = Some(g);
                    break;
                }
            }
        }

        let mut victim_guard = match victim_guard {
            Some(g) => g,
            None => return Ok(current_guard),
        };
        let victim_idx = victim_guard.frame_id() as usize;

        if victim_guard.page_key().is_none() {
            // Promotion: move page to the free preferred frame.
            victim_guard.page_mut().copy(current_guard.page());
            victim_guard.set_page_key(Some(page_key));
            victim_guard.dirty().store(
                current_guard.dirty().load(Ordering::Acquire),
                Ordering::Release,
            );
            victim_guard.evict_info().update();

            self.overflow.insert(page_key, victim_idx);
            current_guard.clear();
            self.enqueue_free_frame(current_idx);
            self.used_frames.fetch_sub(1, Ordering::AcqRel);

            return Ok(victim_guard);
        }

        // Demotion: swap contents with the occupied preferred frame.
        let other_key = victim_guard.page_key().unwrap();

        let mut temp = Page::new_empty();
        temp.copy(current_guard.page());
        current_guard.page_mut().copy(victim_guard.page());
        victim_guard.page_mut().copy(&temp);

        victim_guard.set_page_key(Some(page_key));
        current_guard.set_page_key(Some(other_key));
        let other_dirty = victim_guard.dirty().load(Ordering::Acquire);
        victim_guard.dirty().store(
            current_guard.dirty().load(Ordering::Acquire),
            Ordering::Release,
        );
        current_guard.dirty().store(other_dirty, Ordering::Release);
        victim_guard.evict_info().update();
        current_guard.evict_info().update();

        self.overflow.insert(page_key, victim_idx);
        self.overflow.insert(other_key, current_idx);

        drop(current_guard);
        Ok(victim_guard)
    }

    /// Roll for promotion: true with probability 1/denom. Uses next_u32() % denom == 0.
    #[inline]
    fn promote_roll(denom: u32) -> bool {
        let mut rng = small_thread_rng();
        rng.next_u32() % denom == 0
    }

    // ------------------------------------------------------------------
    // Slow-path helpers for the fast-path wrapper
    // ------------------------------------------------------------------

    /// Read slow path: ensure free frames, then loop (overflow lookup / page fault).
    /// Called by `PredictiveTranslationFPBP` after its inlined fast path misses.
    #[inline(always)]
    pub(crate) fn get_page_for_read_slow<const N: usize>(
        &self,
        page_key: PageKey,
        prefs: [usize; N],
    ) -> Result<FRGuard, MemPoolStatus> {
        #[cfg(feature = "pt_profile")]
        let t0 = std::time::Instant::now();

        self.ensure_free_frames()?;

        #[cfg(feature = "pt_profile")]
        self.profile
            .ensure_free_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let pref_free_hint = prefs.iter().any(|&p| self.frame_is_free(p));

        loop {
            #[cfg(feature = "pt_profile")]
            let t2 = std::time::Instant::now();

            let frame_idx = self.overflow.lookup_with_bucket(&page_key, prefs[0]);

            #[cfg(feature = "pt_profile")]
            self.profile
                .overflow_lookup_ns
                .fetch_add(t2.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if let Some(idx) = frame_idx {
                #[cfg(feature = "pt_profile")]
                let t3 = std::time::Instant::now();

                if let Some(g) = self.try_get_read_guard(idx) {
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .latch_ns
                        .fetch_add(t3.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        if prefs.contains(&idx) {
                            self.profile
                                .preferred_frame_hits
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.profile
                                .overflow_chain_hits
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        // Promotion on read path (paper §3.2, Listing 3 line 25):
                        // if page is in overflow, probabilistically promote.
                        if !prefs.contains(&idx) {
                            let denom = if pref_free_hint {
                                Self::PROMOTE_PROB_NO_DEMOTE
                            } else {
                                Self::PROMOTE_PROB_DEMOTE
                            };
                            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                            self.profile
                                .promotions_attempted
                                .fetch_add(1, Ordering::Relaxed);

                            if Self::promote_roll(denom) {
                                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                                self.profile
                                    .promotions_fired
                                    .fetch_add(1, Ordering::Relaxed);
                                // Upgrade read → write, promote, downgrade back.
                                if let Ok(wg) = g.try_upgrade(false) {
                                    let wg = self.try_promote(wg, page_key, prefs)?;
                                    return Ok(wg.downgrade());
                                } else {
                                    // Upgrade failed — another thread holds it.
                                    // Page was already returned by try_upgrade's Err.
                                    continue;
                                }
                            }
                        }
                        return Ok(g);
                    }
                } else {
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .latch_ns
                        .fetch_add(t3.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    if self.overflow.lookup_with_bucket(&page_key, prefs[0]) == Some(idx) {
                        return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                    }
                }
                continue;
            }

            #[cfg(feature = "pt_profile")]
            let t6 = std::time::Instant::now();

            match self.handle_page_fault_write(page_key, prefs) {
                Ok(victim) => {
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile.page_faults.fetch_add(1, Ordering::Relaxed);
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .fault_ns
                        .fetch_add(t6.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    return Ok(victim.downgrade());
                }
                Err(MemPoolStatus::RetryPageFault) => {
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .fault_ns
                        .fetch_add(t6.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Write slow path: ensure free frames, then loop (overflow lookup / page fault / promotion).
    /// Called by `PredictiveTranslationFPBP` after its inlined fast path misses.
    #[inline(always)]
    pub(crate) fn get_page_for_write_slow<const N: usize>(
        &self,
        page_key: PageKey,
        prefs: [usize; N],
    ) -> Result<FWGuard, MemPoolStatus> {
        #[cfg(feature = "pt_profile")]
        let t0 = std::time::Instant::now();

        self.ensure_free_frames()?;

        #[cfg(feature = "pt_profile")]
        self.profile
            .ensure_free_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let pref_free_hint = prefs.iter().any(|&p| self.frame_is_free(p));

        loop {
            #[cfg(feature = "pt_profile")]
            let t2 = std::time::Instant::now();

            let frame_idx = self.overflow.lookup_with_bucket(&page_key, prefs[0]);

            #[cfg(feature = "pt_profile")]
            self.profile
                .overflow_lookup_ns
                .fetch_add(t2.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if let Some(idx) = frame_idx {
                #[cfg(feature = "pt_profile")]
                let t3 = std::time::Instant::now();

                if let Some(g) = self.try_get_write_guard(idx, true) {
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .latch_ns
                        .fetch_add(t3.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();

                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        if prefs.contains(&idx) {
                            self.profile
                                .preferred_frame_hits
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.profile
                                .overflow_chain_hits
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        // Promotion (§3.2): page in overflow, try to move to preferred.
                        if !prefs.contains(&idx) {
                            #[cfg(feature = "pt_profile")]
                            let t5 = std::time::Instant::now();

                            let denom = if pref_free_hint {
                                Self::PROMOTE_PROB_NO_DEMOTE
                            } else {
                                Self::PROMOTE_PROB_DEMOTE
                            };
                            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                            self.profile
                                .promotions_attempted
                                .fetch_add(1, Ordering::Relaxed);

                            if Self::promote_roll(denom) {
                                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                                self.profile
                                    .promotions_fired
                                    .fetch_add(1, Ordering::Relaxed);
                                #[cfg(feature = "pt_profile")]
                                self.profile
                                    .promotion_check_ns
                                    .fetch_add(t5.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                let guard = self.try_promote(g, page_key, prefs)?;
                                return Ok(guard);
                            }

                            #[cfg(feature = "pt_profile")]
                            self.profile
                                .promotion_check_ns
                                .fetch_add(t5.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                        return Ok(g);
                    }
                } else {
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .latch_ns
                        .fetch_add(t3.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    if self.overflow.lookup_with_bucket(&page_key, prefs[0]) == Some(idx) {
                        return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                    }
                }
                continue;
            }

            #[cfg(feature = "pt_profile")]
            let t6 = std::time::Instant::now();

            match self.handle_page_fault_write(page_key, prefs) {
                Ok(g) => {
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile.page_faults.fetch_add(1, Ordering::Relaxed);
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .fault_ns
                        .fetch_add(t6.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    return Ok(g);
                }
                Err(MemPoolStatus::RetryPageFault) => {
                    #[cfg(feature = "pt_profile")]
                    self.profile
                        .fault_ns
                        .fetch_add(t6.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Like `create_new_page_for_write` but tries two preferred frames for
    /// placement. Used by two-hash FP wrapper so that newly created pages can
    /// land in either of the predicted positions.
    pub(crate) fn create_new_page_for_write_two_hash(
        &self,
        c_key: ContainerKey,
    ) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::CreateNewPage);
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);
        let (p1, p2) = self.preferred_frames(&page_key);
        let (p1, p2) = (p1 as usize, p2 as usize);

        // Pick the first free preferred frame, or None to avoid latching occupied frames.
        let chosen_pref = if self.frame_is_free(p1) {
            Some(p1)
        } else if self.frame_is_free(p2) {
            Some(p2)
        } else {
            None
        };

        let mut victim = self
            .choose_victim(chosen_pref)
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        self.overflow.insert(page_key, victim.frame_id() as usize);

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        Ok(victim)
    }

    /// Like `create_new_page_for_write` but tries four preferred frames for
    /// placement. Used by four-hash FP wrapper.
    pub(crate) fn create_new_page_for_write_four_hash(
        &self,
        c_key: ContainerKey,
    ) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::CreateNewPage);
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);
        let prefs = self.preferred_frames_four(&page_key);

        // Pick the first free preferred frame, or None to avoid latching occupied frames.
        let chosen_pref = prefs
            .iter()
            .find(|&&p| self.frame_is_free(p as usize))
            .map(|&p| p as usize);

        let mut victim = self
            .choose_victim(chosen_pref)
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        self.overflow.insert(page_key, victim.frame_id() as usize);

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        Ok(victim)
    }
}

// ===========================================================================
// MemPool trait implementation
// ===========================================================================

impl MemPool for PredictiveTranslationBP {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        // Container creation is lazy via ContainerManager.
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        // TODO: mark container as temp so writes are skipped on eviction.
        Ok(())
    }

    // ----- create new page ------------------------------------------------

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::CreateNewPage);
        self.stats.inc_new_page();
        self.ensure_free_frames()?;

        let container = self.container_manager.get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);
        let pref = self.preferred_frame(&page_key) as usize;
        let mut victim = self
            .choose_victim(Some(pref))
            .ok_or(MemPoolStatus::CannotEvictPage)?;

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        // Unified: every resident page is in the overflow table.
        self.overflow.insert(page_key, victim.frame_id() as usize);

        // Initialise the frame.
        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.used_frames.fetch_add(1, Ordering::AcqRel);

        Ok(victim)
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        let mut guards = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            match self.create_new_page_for_write(c_key) {
                Ok(g) => guards.push(g),
                Err(_) => break, // Return what we have so far.
            }
        }
        Ok(guards)
    }

    // ----- page presence --------------------------------------------------

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.overflow.contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.overflow.get_page_keys(c_key)
    }

    // ----- get page for write ---------------------------------------------

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageWrite);
        self.stats.inc_write_count();

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile.total_writes.fetch_add(1, Ordering::Relaxed);

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key) as usize;

        #[cfg(not(feature = "pt_no_prefetch"))]
        self.prefetch_predicted_frames(pref, pref);

        self.get_page_for_write_slow(page_key, [pref])
    }

    // ----- get page for read ----------------------------------------------

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        self.stats.inc_read_count();

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile.total_reads.fetch_add(1, Ordering::Relaxed);

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key) as usize;

        #[cfg(not(feature = "pt_no_prefetch"))]
        self.prefetch_predicted_frames(pref, pref);

        self.get_page_for_read_slow(page_key, [pref])
    }

    // ----- prefetch -------------------------------------------------------

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        // No-op for now.
        Ok(())
    }

    // ----- flush / reset --------------------------------------------------

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let frame = loop {
                if let Some(g) = self.try_get_read_guard(i) {
                    break g;
                }
                std::hint::spin_loop();
            };
            self.write_to_disk_if_dirty_r(&frame).unwrap();
        });
        self.container_manager.flush_all()?;
        Ok(())
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let mut frame = loop {
                if let Some(g) = self.try_get_write_guard(i, false) {
                    break g;
                }
                std::hint::spin_loop();
            };
            self.write_to_disk_if_dirty_w(&frame).unwrap();
            if let Some(pk) = frame.page_key() {
                if self.overflow.lookup(&pk) == Some(i) {
                    self.overflow.remove(&pk);
                }
            }
            frame.clear();
        });

        self.container_manager.flush_all()?;

        // Drain the queue and repopulate.
        while self.free_list.pop().is_ok() {}
        for i in 0..self.num_frames {
            self.free_list.push(i).unwrap();
        }
        self.used_frames.store(0, Ordering::Release);

        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        (0..self.num_frames).into_par_iter().for_each(|i| {
            let meta = &mut unsafe { &mut *self.metas.get() }[i];
            meta.is_dirty.store(false, Ordering::Release);
        });
        self.container_manager.flush_all()?;
        Ok(())
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        // No-op hint for now.
        Ok(())
    }

    // ----- stats ----------------------------------------------------------

    unsafe fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();

        let mut num_frames_per_container = BTreeMap::new();
        for i in 0..self.num_frames {
            if let Some(key) = unsafe { &*self.metas.get() }[i].key() {
                *num_frames_per_container.entry(key.c_key).or_insert(0) += 1;
            }
        }

        let mut disk_io_per_container = BTreeMap::new();
        for (c_key, (count, file_stats)) in &self.container_manager.get_stats() {
            disk_io_per_container.insert(
                *c_key,
                (
                    *count as i64,
                    file_stats.read_count() as i64,
                    file_stats.write_count() as i64,
                ),
            );
        }
        let (total_created, total_read, total_write) = disk_io_per_container
            .iter()
            .fold((0, 0, 0), |acc, (_, (c, r, w))| {
                (acc.0 + c, acc.1 + r, acc.2 + w)
            });

        MemoryStats {
            bp_num_frames_in_mem: self.num_frames,
            bp_new_page: new_page,
            bp_read_frame: read_count,
            bp_read_frame_wait: read_count_waiting,
            bp_write_frame: write_count,
            bp_num_frames_per_container: num_frames_per_container,
            disk_created: total_created as usize,
            disk_read: total_read as usize,
            disk_write: total_write as usize,
            disk_io_per_container,
        }
    }

    unsafe fn reset_stats(&self) {
        self.stats.clear();
    }

    fn print_profile(&self) {
        if let Some(report) = macro_profile_report() {
            println!("\n{}", report);
        }

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile.print();
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
impl PredictiveTranslationBP {
    /// # Safety
    /// Must not be called while the BP is in use by other threads.
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_translation_table();
    }

    unsafe fn check_all_frames_unlatched(&self) {
        for i in 0..self.num_frames {
            assert!(
                self.try_get_write_guard(i, false).is_some(),
                "frame {} is still latched",
                i
            );
        }
    }

    unsafe fn check_translation_table(&self) {
        use std::collections::HashMap;
        // Unified: every resident page is in overflow. Check overflow matches frames.
        let mut overflow_frame_to_page: HashMap<usize, PageKey> = HashMap::new();
        self.overflow.for_each_entry(|pk, fid| {
            overflow_frame_to_page.insert(fid, pk);
        });
        for i in 0..self.num_frames {
            let meta = &(&(*self.metas.get()))[i];
            if let Some(pk) = meta.key() {
                assert!(
                    overflow_frame_to_page.get(&i) == Some(&pk),
                    "frame {} has key {:?} but overflow table disagrees",
                    i,
                    pk
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{container::ContainerManager, random::gen_random_pathname};
    use std::sync::Arc;

    fn get_test_pt(num_frames: usize) -> Arc<PredictiveTranslationBP> {
        let base_dir = gen_random_pathname(Some("test_pt_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(PredictiveTranslationBP::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_pt_create_and_read() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        // Read them back.
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_pt_write_back() {
        let db_id = 0;
        let num_frames = 2;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        // Create more pages than frames → forces eviction & disk I/O.
        let mut keys = Vec::new();
        for i in 0..100u8 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        // All pages should be retrievable (from memory or disk).
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_pt_flush_and_reset() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..(num_frames * 2) {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        unsafe { bp.run_checks() };

        bp.flush_all_and_reset().unwrap();

        unsafe { bp.run_checks() };

        // After reset, pages should still be loadable from disk.
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_pt_concurrent_latch() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_pt(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let frame = bp.create_new_page_for_write(c_key).unwrap();
        let key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iters = 80u8;
        std::thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iters {
                        loop {
                            if let Ok(mut g) = bp.get_page_for_write(key) {
                                g[0] += 1;
                                break;
                            }
                        }
                    }
                });
            }
        });

        let guard = bp.get_page_for_read(key).unwrap();
        assert_eq!(guard[0], num_threads * num_iters);
    }
}
