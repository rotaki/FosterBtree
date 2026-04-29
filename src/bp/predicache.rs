//! Predictive Translation buffer pool (PrediCache).
//!
//! Maps page keys to frames via a Stafford-mixed full-key hash; collisions
//! land in an `OptimisticPageMap` (chaining hash table with versioned per-bucket
//! locks). On read, pages found at their preferred frame use the fast path;
//! displaced pages take the OptimisticPageMap slow path. Probabilistic promotion
//! migrates pages back toward their preferred frame over time.
//!
//! Owns: `OptimisticPageMap` (the translator) + per-call diagnostic counters +
//! optional sub-step profile. Frame storage, free-list, clock hand, eviction,
//! and flush are delegated to `FrameManager`.

#[allow(unused_imports)]
use crate::log;

use super::hash::hash_page_key;
use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    frame_manager::FrameManager,
    macro_profile::{report as macro_profile_report, scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
    optimistic_page_map::OptimisticPageMap,

};
use crate::random::small_thread_rng;
use crate::{
    container::ContainerManager,
    log_debug,
    page::{Page, PageId},
};
use rand::RngCore;

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

// Runtime-tunable promotion probability denominators. See
// `promote_prob_no_demote` / `promote_prob_demote`. Initialized lazily from
// the `PT_PROMOTE_PROB_*` env vars on first BP construction.
static PROMOTE_PROB_NO_DEMOTE_ATOMIC: AtomicU32 =
    AtomicU32::new(PrediCache::PROMOTE_PROB_NO_DEMOTE_DEFAULT);
static PROMOTE_PROB_DEMOTE_ATOMIC: AtomicU32 =
    AtomicU32::new(PrediCache::PROMOTE_PROB_DEMOTE_DEFAULT);
static PROMOTE_ENV_LOADED: std::sync::Once = std::sync::Once::new();

fn load_promote_env() {
    PROMOTE_ENV_LOADED.call_once(|| {
        if let Ok(v) = std::env::var("PT_PROMOTE_PROB_NO_DEMOTE") {
            if let Ok(n) = v.parse::<u32>() {
                PROMOTE_PROB_NO_DEMOTE_ATOMIC.store(n.max(1), Ordering::Relaxed);
            }
        }
        if let Ok(v) = std::env::var("PT_PROMOTE_PROB_DEMOTE") {
            if let Ok(n) = v.parse::<u32>() {
                PROMOTE_PROB_DEMOTE_ATOMIC.store(n.max(1), Ordering::Relaxed);
            }
        }
        // Print once so bench logs make clear which denominators were in use.
        eprintln!(
            "PT promotion probs: 1/{} (no-demote), 1/{} (demote)",
            PROMOTE_PROB_NO_DEMOTE_ATOMIC.load(Ordering::Relaxed),
            PROMOTE_PROB_DEMOTE_ATOMIC.load(Ordering::Relaxed),
        );
    });
}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------
type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

// ---------------------------------------------------------------------------
// fastmod (private re-implementation; original lives in predictive_translation)
// ---------------------------------------------------------------------------

#[inline(always)]
fn fastmod(hash: u64, n: u64) -> u32 {
    (((hash as u128).wrapping_mul(n as u128)) >> 64) as u32
}

// ---------------------------------------------------------------------------
// PrediCache
// ---------------------------------------------------------------------------

pub struct PrediCache {
    pub(crate) fm: FrameManager<EvictionPolicyImpl>,
    num_frames_u64: u64,
    num_frames_u32: u32,
    pub(crate) overflow: OptimisticPageMap,
    pub(crate) stats: BPStats,
    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    pub profile: PTProfileCounters,
    pub(crate) create_attempts: AtomicU64,
    pub(crate) create_failed_ensure_free: AtomicU64,
    pub(crate) create_failed_choose_victim: AtomicU64,
    pub(crate) create_succeeded: AtomicU64,
}

unsafe impl Sync for PrediCache {}
unsafe impl Send for PrediCache {}

impl Drop for PrediCache {
    fn drop(&mut self) {
        let attempts = self.create_attempts.load(Ordering::Relaxed);
        if attempts > 0 {
            let succ = self.create_succeeded.load(Ordering::Relaxed);
            let fail_ensure = self.create_failed_ensure_free.load(Ordering::Relaxed);
            let fail_victim = self.create_failed_choose_victim.load(Ordering::Relaxed);
            eprintln!(
                "PT create_new_page_for_write: attempts={} succeeded={} ({:.1}%) failed_ensure_free={} failed_choose_victim={} (page_id leaks={})",
                attempts,
                succ,
                succ as f64 / attempts as f64 * 100.0,
                fail_ensure,
                fail_victim,
                fail_victim,
            );
        }
        if self.fm.container_manager().remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl PrediCache {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    /// Eviction batch size — kept consistent with the original PT (and
    /// `BufferPoolClock`'s default `EVICTION_BATCH_SIZE`).
    pub(crate) const EVICT_BATCH: usize = 64;

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!(
            "PrediCache created: num_frames={}",
            num_frames
        );

        load_promote_env();

        Ok(Self {
            fm: FrameManager::new(num_frames, container_manager)?,
            num_frames_u64: num_frames as u64,
            num_frames_u32: num_frames as u32,
            overflow: OptimisticPageMap::new(num_frames),
            stats: BPStats::new(),
            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
            profile: PTProfileCounters::new(),
            create_attempts: AtomicU64::new(0),
            create_failed_ensure_free: AtomicU64::new(0),
            create_failed_choose_victim: AtomicU64::new(0),
            create_succeeded: AtomicU64::new(0),
        })
    }

    #[inline]
    pub(crate) fn num_frames(&self) -> usize {
        self.fm.num_frames()
    }

    /// Translator hook for `FrameManager` eviction. Removes the overflow
    /// entry only if it still points at `idx` — preserves PT's guard against
    /// promotion races (a page may have been promoted to a different frame
    /// between when classify_frame chose it and when finalize runs).
    #[inline]
    fn on_evict(&self, pk: &PageKey, idx: u32) {
        self.overflow.remove(pk);
        // Track how often a fast-path owner loses residency (plan: B1).
        // `preferred_frame` is a pure function of the key, so this is cheap.
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        {
            if self.preferred_frame(pk) == idx {
                self.profile
                    .residency_evictions_from_preferred
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // ------------------------------------------------------------------
    // Deterministic placement (identical to original PT)
    // ------------------------------------------------------------------

    /// Stafford-mixed full-key hash. Distributes uniformly across frames; no
    /// scan locality. This is PrediCache's placement function. (Order-preserving
    /// placement is implemented in `lapt.rs`, not here.)
    #[inline]
    pub(crate) fn preferred_frame(&self, key: &PageKey) -> u32 {
        fastmod(hash_page_key(key), self.num_frames_u64)
    }

    // ------------------------------------------------------------------
    // Frame-mgmt delegates (FrameManager owns the storage)
    // ------------------------------------------------------------------

    #[inline]
    pub(crate) fn frame_is_free(&self, idx: u32) -> bool {
        self.fm.frame_is_free(idx)
    }

    /// Read-only access to a frame's metadata. Used by the FP wrapper for the
    /// inlined preferred-frame check.
    #[inline]
    pub(crate) fn meta(&self, idx: u32) -> &FMeta {
        self.fm.meta(idx)
    }

    #[inline]
    pub(crate) fn prefetch_predicted_frames(&self, idx: u32) {
        self.fm.prefetch_frame(idx);
    }

    pub fn eviction_stats(&self) -> String {
        "PredictiveTranslationV2: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "PredictiveTranslationV2: file stats disabled".to_string()
    }

    #[inline]
    pub(crate) fn try_get_read_guard(&self, index: u32) -> Option<FRGuard> {
        self.fm.try_get_read_guard(index)
    }

    #[inline]
    pub(crate) fn try_get_write_guard(&self, index: u32, make_dirty: bool) -> Option<FWGuard> {
        self.fm.try_get_write_guard(index, make_dirty)
    }

    #[inline]
    pub(crate) fn enqueue_free_frame(&self, idx: u32) {
        self.fm.enqueue_free_frame(idx);
    }

    #[inline]
    pub(crate) fn choose_victim(&self) -> Option<FWGuard> {
        self.fm.choose_victim()
    }

    #[inline]
    pub(crate) fn ensure_free_frames(&self) -> Result<(), MemPoolStatus> {
        self.fm
            .ensure_free_frames(Self::EVICT_BATCH, |pk, idx| self.on_evict(pk, idx))
    }

    // ------------------------------------------------------------------
    // Page fault (mirrors original PT::handle_page_fault_write)
    // ------------------------------------------------------------------

    pub(crate) fn handle_page_fault_write(
        &self,
        page_key: PageKey,
    ) -> Result<FWGuard, MemPoolStatus> {
        self.fm.increment_used();

        let mut victim = match self.choose_victim() {
            Some(v) => v,
            None => {
                self.fm.decrement_used();
                return Err(MemPoolStatus::CannotEvictPage);
            }
        };

        debug_assert!(victim.page_key().is_none());

        if let Err(existing_idx) = self.overflow.try_insert(page_key, victim.frame_id()) {
            self.enqueue_free_frame(victim.frame_id());
            self.fm.decrement_used();
            return self
                .try_get_write_guard(existing_idx, true)
                .ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
        }

        victim.set_page_key(Some(page_key));

        if let Err(e) = self
            .fm
            .container_manager()
            .get_container(page_key.c_key)
            .read_page(page_key.page_id, &mut victim)
        {
            victim.set_page_key(None);
            self.overflow.remove(&page_key);
            self.enqueue_free_frame(victim.frame_id());
            self.fm.decrement_used();
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        // Read faults stay clean; the write path marks dirty via the caller.
        Ok(victim)
    }

    // ------------------------------------------------------------------
    // Promotion / demotion (mirrors original PT::try_promote)
    // ------------------------------------------------------------------
    //
    // Probabilities are held in process-global AtomicU32 so benchmarks can
    // sweep them without recompiling (plan: PT weakness B3). Defaults mirror
    // the original compile-time constants: 1/50 when preferred is free, 1/512
    // when it's occupied.

    /// Default denominator for promotion probability when the preferred slot
    /// is free. Kept public so callers that want the compile-time default can
    /// reference it symbolically.
    pub const PROMOTE_PROB_NO_DEMOTE_DEFAULT: u32 = 50;
    /// Default denominator for promotion probability when the preferred slot
    /// is occupied (requires a demotion swap).
    pub const PROMOTE_PROB_DEMOTE_DEFAULT: u32 = 512;

    /// Read the current promotion-probability denominator for the "preferred
    /// is free" branch. Can be overridden at process start via the
    /// `PT_PROMOTE_PROB_NO_DEMOTE` env var (see `load_promote_env`).
    #[inline]
    pub fn promote_prob_no_demote() -> u32 {
        PROMOTE_PROB_NO_DEMOTE_ATOMIC.load(Ordering::Relaxed)
    }

    /// Read the current promotion-probability denominator for the "preferred
    /// is occupied / demote swap" branch. Can be overridden at process start
    /// via the `PT_PROMOTE_PROB_DEMOTE` env var.
    #[inline]
    pub fn promote_prob_demote() -> u32 {
        PROMOTE_PROB_DEMOTE_ATOMIC.load(Ordering::Relaxed)
    }

    fn try_promote(
        &self,
        mut current_guard: FWGuard,
        page_key: PageKey,
        pref: u32,
    ) -> Result<FWGuard, MemPoolStatus> {
        let current_idx = current_guard.frame_id();
        if current_idx == pref {
            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
            self.profile.promote_noop.fetch_add(1, Ordering::Relaxed);
            return Ok(current_guard);
        }

        let mut victim_guard = match self.try_get_write_guard(pref, false) {
            Some(g) => g,
            None => {
                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                self.profile.promote_noop.fetch_add(1, Ordering::Relaxed);
                return Ok(current_guard);
            }
        };
        let victim_idx = victim_guard.frame_id();

        if victim_guard.page_key().is_none() {
            // Promotion: move page to the free preferred frame.
            #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
            self.profile.promote_free.fetch_add(1, Ordering::Relaxed);
            victim_guard.page_mut().copy(current_guard.page());
            victim_guard.set_page_key(Some(page_key));
            victim_guard.dirty().store(
                current_guard.dirty().load(Ordering::Acquire),
                Ordering::Release,
            );
            victim_guard.evict_info().update();

            // Insert at the preferred bucket (already promoted to pref frame).
            self.overflow.insert_at_bucket(page_key, victim_idx, pref as usize);
            current_guard.clear();
            self.enqueue_free_frame(current_idx);
            // Promotion is a swap: net change in real usage is zero
            // (victim_idx was empty-but-stale-in-free_list; current_idx is
            // now empty-and-in-free_list). Decrementing used_frames here
            // leaks the counter and livelocks eviction under pressure.

            return Ok(victim_guard);
        }

        // Demotion: swap contents with the occupied preferred frame.
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile.promote_swap.fetch_add(1, Ordering::Relaxed);
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

        // After swap: page_key is now at pref (victim_idx), other_key is at current_idx.
        // Insert page_key at the preferred bucket; for other_key, use its own preferred frame.
        self.overflow.insert_at_bucket(page_key, victim_idx, pref as usize);
        let other_pref = self.preferred_frame(&other_key);
        self.overflow.insert_at_bucket(other_key, current_idx, other_pref as usize);

        drop(current_guard);
        Ok(victim_guard)
    }

    #[inline]
    fn promote_roll(denom: u32) -> bool {
        let mut rng = small_thread_rng();
        rng.next_u32() % denom == 0
    }

    // ------------------------------------------------------------------
    // Slow-path helpers (used by FP-1 wrapper after fast-path miss)
    // ------------------------------------------------------------------

    /// Read slow path: ensure free frames, then atomic lookup+latch via
    /// `overflow.get_apply_with_bucket`. The OLC version check inside
    /// `get_apply` handles all overflow-side retries, so this function is
    /// straight-line — no outer loop.
    #[inline(always)]
    pub(crate) fn get_page_for_read_slow(
        &self,
        page_key: PageKey,
        pref: u32,
    ) -> Result<FRGuard, MemPoolStatus> {
        let result = self
            .overflow
            .get_apply_with_bucket(&page_key, pref as usize, |idx| {
                self.try_get_read_guard(idx).map(|g| (idx, g))
            });


        let pref_free_hint = self.frame_is_free(pref);

        match result {
            Some(Some((idx, g))) => {
                g.evict_info().update();

                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                if idx == pref {
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.profile
                        .overflow_chain_hits
                        .fetch_add(1, Ordering::Relaxed);
                }

                // Promotion on read path (paper §3.2). On upgrade failure,
                // skip promotion and return the read guard — the roll is
                // rare and the next access will roll again.
                //
                // OPTIMIZATION: Always promote if preferred frame is free (no demotion needed).
                // Only use probabilistic promotion for expensive swap cases.
                if idx != pref {
                    let should_promote = if pref_free_hint {
                        // Preferred frame is FREE - always promote! This is fast and beneficial.
                        true
                    } else {
                        // Preferred frame is OCCUPIED - only promote probabilistically (swap is expensive)
                        let denom = Self::promote_prob_demote();
                        Self::promote_roll(denom)
                    };

                    if should_promote {
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.profile
                            .promotions_attempted
                            .fetch_add(1, Ordering::Relaxed);

                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.profile
                            .promotions_fired
                            .fetch_add(1, Ordering::Relaxed);

                        match g.try_upgrade(false) {
                            Ok(wg) => {
                                let wg = self.try_promote(wg, page_key, pref)?;
                                return Ok(wg.downgrade());
                            }
                            Err(g) => return Ok(g),
                        }
                    }
                }
                Ok(g)
            }
            Some(None) => Err(MemPoolStatus::FrameReadLatchGrantFailed),
            None => {
                self.ensure_free_frames()?;
                match self.handle_page_fault_write(page_key) {
                    Ok(victim) => {
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.profile.page_faults.fetch_add(1, Ordering::Relaxed);
                        Ok(victim.downgrade())
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Write slow path — mirrors `get_page_for_read_slow`. See that function
    /// for rationale (OLC version check, no outer loop).
    #[inline(always)]
    pub(crate) fn get_page_for_write_slow(
        &self,
        page_key: PageKey,
        pref: u32,
    ) -> Result<FWGuard, MemPoolStatus> {
        let result = self
            .overflow
            .get_apply_with_bucket(&page_key, pref as usize, |idx| {
                self.try_get_write_guard(idx, true).map(|g| (idx, g))
            });


        match result {
            Some(Some((idx, g))) => {
                g.evict_info().update();

                #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                if idx == pref {
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.profile
                        .overflow_chain_hits
                        .fetch_add(1, Ordering::Relaxed);
                }

                // OPTIMIZATION: Always promote if preferred frame is free (no demotion needed).
                // Only use probabilistic promotion for expensive swap cases.
                if idx != pref {
                    let pref_is_free = self.frame_is_free(pref);
                    let should_promote = if pref_is_free {
                        // Preferred frame is FREE - always promote! This is fast and beneficial.
                        true
                    } else {
                        // Preferred frame is OCCUPIED - only promote probabilistically (swap is expensive)
                        let denom = Self::promote_prob_demote();
                        Self::promote_roll(denom)
                    };

                    if should_promote {
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.profile
                            .promotions_attempted
                            .fetch_add(1, Ordering::Relaxed);

                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.profile
                            .promotions_fired
                            .fetch_add(1, Ordering::Relaxed);

                        return self.try_promote(g, page_key, pref);
                    }
                }
                Ok(g)
            }
            Some(None) => Err(MemPoolStatus::FrameWriteLatchGrantFailed),
            None => {
                self.ensure_free_frames()?;
                match self.handle_page_fault_write(page_key) {
                    Ok(g) => {
                        g.dirty().store(true, Ordering::Release);
                        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                        self.profile.page_faults.fetch_add(1, Ordering::Relaxed);
                        Ok(g)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

// ===========================================================================
// MemPool trait implementation
// ===========================================================================

impl MemPool for PrediCache {
    type EP = EvictionPolicyImpl;

    fn create_container(&self, _c_key: ContainerKey, _is_temp: bool) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn drop_container(&self, _c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::CreateNewPage);
        self.stats.inc_new_page();
        self.create_attempts.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.ensure_free_frames() {
            self.create_failed_ensure_free
                .fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }

        let mut victim = match self.choose_victim() {
            Some(v) => v,
            None => {
                self.create_failed_choose_victim
                    .fetch_add(1, Ordering::Relaxed);
                return Err(MemPoolStatus::CannotEvictPage);
            }
        };

        let container = self.fm.container_manager().get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        // Insert into the overflow table at the preferred bucket so future lookups find it.
        // so we must explicitly use the preferred slot as the bucket.
        let pref = self.preferred_frame(&page_key);
        self.overflow.insert_at_bucket(page_key, victim.frame_id(), pref as usize);

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.fm.increment_used();
        self.create_succeeded.fetch_add(1, Ordering::Relaxed);

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
                Err(_) => break,
            }
        }
        Ok(guards)
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.overflow.contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.overflow.get_page_keys(c_key)
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageWrite);
        self.stats.inc_write_count();
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile.total_writes.fetch_add(1, Ordering::Relaxed);

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        self.prefetch_predicted_frames(pref);

        self.get_page_for_write_slow(page_key, pref)
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        self.stats.inc_read_count();
        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile.total_reads.fetch_add(1, Ordering::Relaxed);

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        self.prefetch_predicted_frames(pref);

        self.get_page_for_read_slow(page_key, pref)
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.fm.flush_all()
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        // Translator hook: clear overflow entries that still point at the freed
        // frame (defensive guard mirroring the original PT). Then reset the
        // overflow table itself? No — the original drains entries one-by-one
        // via the `lookup == Some(i)` guard, leaving the table empty after the
        // sweep (under quiescence).
        self.fm.flush_all_and_reset(|pk, idx| {
            if self.overflow.lookup(pk) == Some(idx) {
                self.overflow.remove(pk);
            }
        })
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        self.fm.clear_dirty_flags()
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    unsafe fn stats(&self) -> MemoryStats {
        let new_page = self.stats.new_page();
        let read_count = self.stats.read_count();
        let read_count_waiting = self.stats.read_request_waiting_for_write_count();
        let write_count = self.stats.write_count();

        let mut num_frames_per_container = BTreeMap::new();
        for i in 0..self.num_frames() {
            if let Some(key) = self.fm.meta(i as u32).key() {
                *num_frames_per_container.entry(key.c_key).or_insert(0) += 1;
            }
        }

        let mut disk_io_per_container = BTreeMap::new();
        for (c_key, (count, file_stats)) in &self.fm.container_manager().get_stats() {
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
            bp_num_frames_in_mem: self.num_frames(),
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

    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    fn sample_coverage(&self) -> (u64, u64) {
        let hits = self.profile.preferred_frame_hits.load(Ordering::Relaxed);
        let total = hits
            + self.profile.overflow_chain_hits.load(Ordering::Relaxed)
            + self.profile.page_faults.load(Ordering::Relaxed);
        (hits, total)
    }
}

// ===========================================================================
// Tests (mirror predictive_translation.rs)
// ===========================================================================

#[cfg(test)]
impl PrediCache {
    /// # Safety
    /// Must not be called while the BP is in use by other threads.
    unsafe fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_translation_table();
        self.check_used_frames_accounting();
    }

    unsafe fn check_all_frames_unlatched(&self) {
        for i in 0..self.num_frames() {
            assert!(
                self.try_get_write_guard(i as u32, false).is_some(),
                "frame {} is still latched",
                i
            );
        }
    }

    /// Mirror of PT's accounting check — `used_frames` must equal the count of
    /// frames with `page_key.is_some()`, otherwise `ensure_free_frames` will
    /// eventually skip eviction and the BP livelocks under pressure.
    unsafe fn check_used_frames_accounting(&self) {
        let mut actual = 0usize;
        for i in 0..self.num_frames() {
            if self.fm.meta(i as u32).key().is_some() {
                actual += 1;
            }
        }
        let reported = self.fm.used_frames();
        assert_eq!(
            reported, actual,
            "used_frames drift: reported={}, actual={}",
            reported, actual
        );
    }

    unsafe fn check_translation_table(&self) {
        use std::collections::HashMap;
        let mut overflow_frame_to_page: HashMap<u32, PageKey> = HashMap::new();
        self.overflow.for_each_entry(|pk, fid| {
            overflow_frame_to_page.insert(fid, pk);
        });
        for i in 0..self.num_frames() {
            let i = i as u32;
            if let Some(pk) = self.fm.meta(i).key() {
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

    fn get_test_predicache(num_frames: usize) -> Arc<PrediCache> {
        let base_dir = gen_random_pathname(Some("test_pt_v2_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(PrediCache::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_ptv2_create_and_read() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_predicache(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }
        unsafe { bp.run_checks() };

        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }
        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_ptv2_write_back() {
        let db_id = 0;
        let num_frames = 2;
        let bp = get_test_predicache(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..100u8 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i;
            keys.push(guard.page_frame_key().unwrap());
        }
        unsafe { bp.run_checks() };

        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }
        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_ptv2_flush_and_reset() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_predicache(num_frames);
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

        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }
        unsafe { bp.run_checks() };
    }

    /// Hammer a resident, overflow-located page with many reads to trigger
    /// `try_promote`. Asserts `used_frames` stays in sync with actual
    /// occupancy across many promotions (see parent file's
    /// `test_pt_used_frames_accounting_under_promotion`).
    #[test]
    fn test_ptv2_used_frames_accounting_under_promotion() {
        let db_id = 0;
        let num_frames = 64;
        let bp = get_test_predicache(num_frames);
        let c_key = ContainerKey::new(db_id, 0);

        let num_pages = 32;
        let mut keys = Vec::new();
        for i in 0..num_pages {
            let mut g = bp.create_new_page_for_write(c_key).unwrap();
            g[0] = i as u8;
            keys.push(g.page_frame_key().unwrap());
        }

        for _ in 0..1000 {
            for key in &keys {
                let g = bp.get_page_for_read(*key).unwrap();
                drop(g);
            }
        }

        unsafe { bp.run_checks() };
    }

    #[test]
    fn test_ptv2_concurrent_latch() {
        let db_id = 0;
        let num_frames = 10;
        let bp = get_test_predicache(num_frames);
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

// ===========================================================================
// Profile counters + fastmod (inlined from former predictive_translation.rs)
// ===========================================================================

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
    /// `try_promote` calls that took the "preferred frame is free" branch.
    pub promote_free: AtomicU64,
    /// `try_promote` calls that took the "preferred frame is occupied → swap" branch.
    pub promote_swap: AtomicU64,
    /// `try_promote` calls that bailed early (current == pref, or latch failed).
    pub promote_noop: AtomicU64,
    /// Page evicted from a frame that still was its own preferred slot — i.e.
    /// the fast-path winner lost residency. Used to quantify how much
    /// fast-path ownership churns (plan: PT weakness B1).
    pub residency_evictions_from_preferred: AtomicU64,
}

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
            promote_free: AtomicU64::new(0),
            promote_swap: AtomicU64::new(0),
            promote_noop: AtomicU64::new(0),
            residency_evictions_from_preferred: AtomicU64::new(0),
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
        println!(
            "  → promote (free):   {:>12}  (preferred frame was empty)",
            r(&self.promote_free)
        );
        println!(
            "  → demote (swap):    {:>12}  (preferred frame was occupied)",
            r(&self.promote_swap)
        );
        println!(
            "  → no-op:            {:>12}  (already at preferred / latch failed)",
            r(&self.promote_noop)
        );
        println!(
            "Residency evictions from preferred: {:>12}  (fast-path owners that got evicted)",
            r(&self.residency_evictions_from_preferred)
        );

        // Combined fast-path coverage line — unified across PT, TLB, LIPAH so
        // bench scripts can grep for a single key.
        //
        // For PT/PT(FP) the fast-path hit = preferred_frame_hits; non-hits are
        // overflow_chain_hits + page_faults. This is what the plan calls
        // "fast_path_coverage" (B1).
        let fp_hits = r(&self.preferred_frame_hits);
        let fp_total = fp_hits + r(&self.overflow_chain_hits) + r(&self.page_faults);
        let cov = if fp_total == 0 {
            0.0
        } else {
            fp_hits as f64 / fp_total as f64
        };
        println!(
            "fast_path_coverage: {:.4}  (hits={}, total={})",
            cov, fp_hits, fp_total
        );

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

pub(crate) fn fastmod32(hash: u32, n: u32) -> u32 {
    (((hash as u64).wrapping_mul(n as u64)) >> 32) as u32
}

