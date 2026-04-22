//! Shadow re-implementation of `PredictiveTranslationBP` on top of
//! `FrameManager`.
//!
//! **Status: shadow** — coexists with the original `PredictiveTranslationBP`.
//! Intended to be behaviorally identical; use for A/B testing and migration
//! verification. After parity is confirmed across PT and TLB-BP, the original
//! can be deleted and this renamed.
//!
//! Owns: `OverflowTable` (the translator) + multi-hash constants + per-call
//! diagnostic counters + optional sub-step profile. Delegates everything
//! frame-mgmt to `FrameManager`: page/meta storage, free-frame queue, clock
//! hand, used-frame counter, container handles, eviction, flush.
//!
//! Translator hook (`on_evict`) drops an overflow entry only when it still
//! points at the frame being freed — preserves the original PT's guard against
//! promotion races during eviction.

#[allow(unused_imports)]
use crate::log;

#[allow(unused_imports)]
use super::hash::{
    hash_page_key, hash_page_key_2, hash_page_key_3, hash_page_key_4, hash_page_key_u128,
};
#[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
use super::predictive_translation::PTProfileCounters;
use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    frame_manager::FrameManager,
    macro_profile::{report as macro_profile_report, scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
    overflow_table::OverflowTable,
    predictive_translation::fastmod32,
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
    AtomicU32::new(PredictiveTranslationBPV2::PROMOTE_PROB_NO_DEMOTE_DEFAULT);
static PROMOTE_PROB_DEMOTE_ATOMIC: AtomicU32 =
    AtomicU32::new(PredictiveTranslationBPV2::PROMOTE_PROB_DEMOTE_DEFAULT);
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
// PredictiveTranslationBPV2
// ---------------------------------------------------------------------------

pub struct PredictiveTranslationBPV2 {
    pub(crate) fm: FrameManager<EvictionPolicyImpl>,
    num_frames_u64: u64,
    num_frames_u32: u32,
    pub(crate) overflow: OverflowTable,
    pub(crate) stats: BPStats,
    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
    pub profile: PTProfileCounters,
    pub(crate) create_attempts: AtomicU64,
    pub(crate) create_failed_ensure_free: AtomicU64,
    pub(crate) create_failed_choose_victim: AtomicU64,
    pub(crate) create_succeeded: AtomicU64,
}

unsafe impl Sync for PredictiveTranslationBPV2 {}
unsafe impl Send for PredictiveTranslationBPV2 {}

impl Drop for PredictiveTranslationBPV2 {
    fn drop(&mut self) {
        let attempts = self.create_attempts.load(Ordering::Relaxed);
        if attempts > 0 {
            let succ = self.create_succeeded.load(Ordering::Relaxed);
            let fail_ensure = self.create_failed_ensure_free.load(Ordering::Relaxed);
            let fail_victim = self.create_failed_choose_victim.load(Ordering::Relaxed);
            eprintln!(
                "PT-V2 create_new_page_for_write: attempts={} succeeded={} ({:.1}%) failed_ensure_free={} failed_choose_victim={} (page_id leaks={})",
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

impl PredictiveTranslationBPV2 {
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
            "PredictiveTranslationBPV2 created: num_frames={}",
            num_frames
        );

        load_promote_env();

        Ok(Self {
            fm: FrameManager::new(num_frames, container_manager)?,
            num_frames_u64: num_frames as u64,
            num_frames_u32: num_frames as u32,
            overflow: OverflowTable::new(num_frames),
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
        if self.overflow.lookup(pk) == Some(idx) {
            self.overflow.remove(pk);
        }
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

    #[inline]
    pub(crate) fn preferred_frame(&self, key: &PageKey) -> u32 {
        // When `pt_op_hash` is enabled we use the TLB-style order-preserving
        // composition: hash the container key and add the raw page id.
        // Adjacent page ids within the same container map to adjacent slots
        // (mod `num_frames`), preserving scan locality. Without it we keep
        // the Stafford-mixed full key — better distribution, zero scan
        // locality. See plan: PT strength/weakness study, Part A.
        #[cfg(feature = "pt_op_hash")]
        {
            let c_hash = super::hash::hash_u64(key.c_key.as_u32() as u64);
            let packed = c_hash.wrapping_add(key.page_id as u64);
            fastmod(packed, self.num_frames_u64)
        }
        #[cfg(not(feature = "pt_op_hash"))]
        {
            fastmod(hash_page_key(key), self.num_frames_u64)
        }
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

            self.overflow.insert(page_key, victim_idx);
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

        self.overflow.insert(page_key, victim_idx);
        self.overflow.insert(other_key, current_idx);

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
                if idx != pref {
                    let denom = if pref_free_hint {
                        Self::promote_prob_no_demote()
                    } else {
                        Self::promote_prob_demote()
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

                if idx != pref {
                    let denom = if self.frame_is_free(pref) {
                        Self::promote_prob_no_demote()
                    } else {
                        Self::promote_prob_demote()
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

impl MemPool for PredictiveTranslationBPV2 {
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

        self.overflow.insert(page_key, victim.frame_id());

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
impl PredictiveTranslationBPV2 {
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

    fn get_test_pt(num_frames: usize) -> Arc<PredictiveTranslationBPV2> {
        let base_dir = gen_random_pathname(Some("test_pt_v2_direct"));
        let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
        Arc::new(PredictiveTranslationBPV2::new(num_frames, cm).unwrap())
    }

    #[test]
    fn test_ptv2_create_and_read() {
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
        let bp = get_test_pt(num_frames);
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
        let bp = get_test_pt(num_frames);
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
