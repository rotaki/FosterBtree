//! LAPT — Locality Aware Predictive Translation.
//!
//! Three ideas, composed:
//!
//! 1. **Prefix-hash + suffix-offset placement.** A page's preferred frame is
//!    `(hash(c_key) + page_id) mod F`. The container key is hashed (prefix)
//!    to spread tenants across the frame array; the page id is added raw
//!    (suffix offset) so sequential page ids land in sequential frames. This
//!    preserves intra-container locality for scans while disambiguating
//!    across containers — sequential sibling scans get sequential frames,
//!    not collisions.
//!
//! 2. **ART fallback translator.** When a page isn't at its preferred frame
//!    (collision, displacement after eviction), the slow-path lookup hits a
//!    `CongeeRawU32<usize>` — concurrent OLC ART — keyed by the packed
//!    `PageKey`. Single-key reads share cache behavior with `tlb_bp_v2.rs`.
//!
//! 3. **LIPAH-style fast path with cheap validation.** `get_page_for_*`
//!    inlines a preferred-frame metadata check (`meta(pref).key() ==
//!    Some(page_key)`) before any ART lookup. On a hit we take the latch and
//!    re-validate `page_key()` under the guard; on a miss we fall through to
//!    the ART slow path. The validation is two pointer-sized loads — cheaper
//!    than the ART traversal it replaces in the common case.

#[allow(unused_imports)]
use crate::log;

use super::{
    buffer_pool::BPStats,
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    frame_guards::{FrameMeta, FrameReadGuard, FrameWriteGuard},
    frame_manager::FrameManager,
    macro_profile::{report as macro_profile_report, scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
};
use crate::random::small_thread_rng;
use crate::{
    container::ContainerManager,
    log_debug,
    page::{Page, PageId},
};
use congee::CongeeRawU32;
use rand::RngCore;

use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------
type EvictionPolicyImpl = ClockEvictionPolicy;
type FMeta = FrameMeta<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;

// ---------------------------------------------------------------------------
// PageKey ↔ usize packing (matches tlb_bp_v2.rs)
// ---------------------------------------------------------------------------

#[inline(always)]
fn pack_page_key(key: &PageKey) -> usize {
    ((key.c_key.as_u32() as usize) << 32) | key.page_id as usize
}

#[inline(always)]
fn unpack_page_key(packed: usize) -> PageKey {
    let c_key_u32 = (packed >> 32) as u32;
    let page_id = packed as u32;
    PageKey::new(ContainerKey::from_u32(c_key_u32), page_id)
}

// ---------------------------------------------------------------------------
// Lapt
// ---------------------------------------------------------------------------
//
// Congee-backed PT V2 with bucket-validate-first compile-time fast path.
// `get_page_for_{read,write}` inline the preferred-frame meta check before
// falling back to the congee slow path.

pub struct Lapt {
    pub(crate) fm: FrameManager<EvictionPolicyImpl>,
    num_frames_u64: u64,
    pub(crate) overflow: CongeeRawU32<usize>,
    pub(crate) stats: BPStats,
}

unsafe impl Sync for Lapt {}
unsafe impl Send for Lapt {}

impl Drop for Lapt {
    fn drop(&mut self) {
        if self.fm.container_manager().remove_dir_on_drop() {
            // Test mode — directory will be cleaned up by ContainerManager.
        } else {
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl Lapt {
    pub(crate) const EVICT_BATCH: usize = 64;

    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!(
            "Lapt created: num_frames={}",
            num_frames
        );

        Ok(Self {
            fm: FrameManager::new(num_frames, container_manager)?,
            num_frames_u64: num_frames as u64,
            overflow: CongeeRawU32::default(),
            stats: BPStats::new(),
        })
    }

    #[inline]
    pub(crate) fn num_frames(&self) -> usize {
        self.fm.num_frames()
    }

    /// Translator hook for `FrameManager` eviction. Removes the overflow
    /// entry when the frame is freed. We don't condition on "still pointing
    /// at idx" because congee's `remove` is keyed by page_key, not by
    /// (key, idx); the FrameManager guarantees the page_key is the right one
    /// for `idx` at the time of eviction.
    #[inline]
    fn on_evict(&self, pk: &PageKey, _idx: u32) {
        self.overflow_remove(pk);
    }

    // ------------------------------------------------------------------
    // Congee helpers (thin wrappers, mirror tlb_bp_v2)
    // ------------------------------------------------------------------

    #[inline]
    fn overflow_lookup(&self, key: &PageKey) -> Option<u32> {
        let guard = crossbeam_epoch::pin();
        self.overflow.get(&pack_page_key(key), &guard)
    }

    #[inline]
    fn overflow_insert(&self, key: PageKey, frame_id: u32) {
        let guard = crossbeam_epoch::pin();
        let _ = self.overflow.insert(pack_page_key(&key), frame_id, &guard);
    }

    /// Atomic insert-if-absent. Returns `Err(existing_frame)` if a competing
    /// thread inserted the same key first; otherwise `Ok(())`.
    #[inline]
    fn overflow_try_insert(&self, key: PageKey, frame_id: u32) -> Result<(), u32> {
        let guard = crossbeam_epoch::pin();
        let packed = pack_page_key(&key);
        let mut existing_frame: Option<u32> = None;
        let _ = self.overflow.compute_or_insert(
            packed,
            |existing| match existing {
                None => frame_id,
                Some(v) => {
                    existing_frame = Some(v);
                    v
                }
            },
            &guard,
        );
        match existing_frame {
            Some(v) => Err(v),
            None => Ok(()),
        }
    }

    #[inline]
    fn overflow_remove(&self, key: &PageKey) -> Option<u32> {
        let guard = crossbeam_epoch::pin();
        self.overflow.remove(&pack_page_key(key), &guard)
    }

    fn overflow_contains_key(&self, key: &PageKey) -> bool {
        self.overflow_lookup(key).is_some()
    }

    fn overflow_get_page_keys(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        let guard = crossbeam_epoch::pin();
        let start = (c_key.as_u32() as usize) << 32;
        let end = start | 0xFFFF_FFFF;
        let mut buf = vec![(0usize, 0u32); 4096];
        let mut out = Vec::new();
        let mut scan_start = start;
        loop {
            let count = self.overflow.range(&scan_start, &end, &mut buf, &guard);
            if count == 0 {
                break;
            }
            for &(packed, frame_id) in &buf[..count] {
                let pk = unpack_page_key(packed);
                out.push(PageFrameKey::new_with_frame_id(
                    pk.c_key, pk.page_id, frame_id,
                ));
            }
            if count < buf.len() {
                break;
            }
            scan_start = buf[count - 1].0 + 1;
        }
        out
    }

    // ------------------------------------------------------------------
    // Deterministic placement — same formula as PrediCache
    // ------------------------------------------------------------------

    /// Order-preserving hash: `(hash(c_key) + page_id) mod F`. Sequential page
    /// ids within a container map to sequential preferred frames; container
    /// offsets disambiguate across tenants. This is *the* LAPT placement —
    /// always on, no feature flag.
    #[inline]
    pub(crate) fn preferred_frame(&self, key: &PageKey) -> u32 {
        let c_hash = super::hash::hash_u64(key.c_key.as_u32() as u64);
        let packed = c_hash.wrapping_add(key.page_id as u64);
        (packed % self.num_frames_u64) as u32
    }

    // ------------------------------------------------------------------
    // Frame-mgmt delegates
    // ------------------------------------------------------------------

    #[inline]
    pub(crate) fn frame_is_free(&self, idx: u32) -> bool {
        self.fm.frame_is_free(idx)
    }

    #[inline]
    pub(crate) fn meta(&self, idx: u32) -> &FMeta {
        self.fm.meta(idx)
    }

    #[inline]
    pub(crate) fn prefetch_predicted_frames(&self, idx: u32) {
        self.fm.prefetch_frame(idx);
    }

    pub fn eviction_stats(&self) -> String {
        "PredictiveTranslationV2Congee: eviction stats not yet implemented".to_string()
    }

    pub fn file_stats(&self) -> String {
        "PredictiveTranslationV2Congee: file stats disabled".to_string()
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
    // Page fault — mirrors PrediCache::handle_page_fault_write
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

        if let Err(existing_idx) = self.overflow_try_insert(page_key, victim.frame_id()) {
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
            self.overflow_remove(&page_key);
            self.enqueue_free_frame(victim.frame_id());
            self.fm.decrement_used();
            return Err(MemPoolStatus::FileManagerError(e.to_string()));
        }

        victim.evict_info().reset();
        Ok(victim)
    }

    // ------------------------------------------------------------------
    // Promotion — mirrors PT-V2::try_promote, but congee inserts are not
    // bucket-indexed, so we just call insert(packed, fid, guard).
    // ------------------------------------------------------------------

    pub const PROMOTE_PROB_NO_DEMOTE_DEFAULT: u32 = 50;
    pub const PROMOTE_PROB_DEMOTE_DEFAULT: u32 = 512;

    #[inline]
    pub fn promote_prob_no_demote() -> u32 {
        // Reuse the global tunables defined in predictive_translation so
        // both variants pick up the same env vars (PT_PROMOTE_PROB_*).
        super::predicache::PrediCache::promote_prob_no_demote()
    }

    #[inline]
    pub fn promote_prob_demote() -> u32 {
        super::predicache::PrediCache::promote_prob_demote()
    }

    fn try_promote(
        &self,
        mut current_guard: FWGuard,
        page_key: PageKey,
        pref: u32,
    ) -> Result<FWGuard, MemPoolStatus> {
        let current_idx = current_guard.frame_id();
        if current_idx == pref {
            return Ok(current_guard);
        }

        let mut victim_guard = match self.try_get_write_guard(pref, false) {
            Some(g) => g,
            None => {
                return Ok(current_guard);
            }
        };
        let victim_idx = victim_guard.frame_id();

        if victim_guard.page_key().is_none() {
            // Promotion: move page to the free preferred frame.
            victim_guard.page_mut().copy(current_guard.page());
            victim_guard.set_page_key(Some(page_key));
            victim_guard.dirty().store(
                current_guard.dirty().load(Ordering::Acquire),
                Ordering::Release,
            );
            victim_guard.evict_info().update();

            self.overflow_insert(page_key, victim_idx);
            current_guard.clear();
            self.enqueue_free_frame(current_idx);

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

        self.overflow_insert(page_key, victim_idx);
        self.overflow_insert(other_key, current_idx);

        drop(current_guard);
        Ok(victim_guard)
    }

    #[inline]
    fn promote_roll(denom: u32) -> bool {
        let mut rng = small_thread_rng();
        rng.next_u32() % denom == 0
    }

    // ------------------------------------------------------------------
    // Slow paths invoked when the inlined FP check in `MemPool::get_page_for_*`
    // misses. Same shape as PT-V2 but uses congee.get(packed) directly — no
    // bucket-index trickery.
    // ------------------------------------------------------------------

    #[inline(always)]
    pub(crate) fn get_page_for_read_slow(
        &self,
        page_key: PageKey,
        pref: u32,
    ) -> Result<FRGuard, MemPoolStatus> {
        let frame_id_opt = self.overflow_lookup(&page_key);

        match frame_id_opt {
            Some(idx) => match self.try_get_read_guard(idx) {
                Some(g) => {
                    if g.page_key() != Some(page_key) {
                        // Stale overflow entry (concurrent eviction/remap).
                        // Treat as miss → re-fault.
                        drop(g);
                        self.ensure_free_frames()?;
                        return self
                            .handle_page_fault_write(page_key)
                            .map(|v| v.downgrade());
                    }
                    g.evict_info().update();

                    let pref_free_hint = self.frame_is_free(pref);

                    if idx != pref {
                        let should_promote = if pref_free_hint {
                            true
                        } else {
                            let denom = Self::promote_prob_demote();
                            Self::promote_roll(denom)
                        };

                        if should_promote {
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
                None => Err(MemPoolStatus::FrameReadLatchGrantFailed),
            },
            None => {
                self.ensure_free_frames()?;
                self.handle_page_fault_write(page_key).map(|v| v.downgrade())
            }
        }
    }

    #[inline(always)]
    pub(crate) fn get_page_for_write_slow(
        &self,
        page_key: PageKey,
        pref: u32,
    ) -> Result<FWGuard, MemPoolStatus> {
        let frame_id_opt = self.overflow_lookup(&page_key);

        match frame_id_opt {
            Some(idx) => match self.try_get_write_guard(idx, true) {
                Some(g) => {
                    if g.page_key() != Some(page_key) {
                        drop(g);
                        self.ensure_free_frames()?;
                        return self.handle_page_fault_write(page_key).map(|mut v| {
                            v.dirty().store(true, Ordering::Release);
                            v
                        });
                    }
                    g.evict_info().update();

                    if idx != pref {
                        let pref_is_free = self.frame_is_free(pref);
                        let should_promote = if pref_is_free {
                            true
                        } else {
                            let denom = Self::promote_prob_demote();
                            Self::promote_roll(denom)
                        };

                        if should_promote {
                            return self.try_promote(g, page_key, pref);
                        }
                    }
                    Ok(g)
                }
                None => Err(MemPoolStatus::FrameWriteLatchGrantFailed),
            },
            None => {
                self.ensure_free_frames()?;
                match self.handle_page_fault_write(page_key) {
                    Ok(g) => {
                        g.dirty().store(true, Ordering::Release);
                        Ok(g)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

// ===========================================================================
// MemPool trait — bucket-validate-first fast path inlined directly into
// `get_page_for_{read,write}`, falling back to the congee slow path. Mirrors
// the OptimisticPageMap-backed FP wrapper for PrediCache.
// ===========================================================================

impl MemPool for Lapt {
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
        self.ensure_free_frames()?;

        let mut victim = match self.choose_victim() {
            Some(v) => v,
            None => return Err(MemPoolStatus::CannotEvictPage),
        };

        let container = self.fm.container_manager().get_container(c_key);
        let page_id = container.inc_page_count(1) as PageId;
        let page_key = PageKey::new(c_key, page_id);

        debug_assert!(victim.page_key().is_none());
        debug_assert!(!victim.dirty().load(Ordering::Acquire));

        // Insert page → frame mapping into congee. No bucket trickery: keyed
        // purely by packed PageKey.
        self.overflow_insert(page_key, victim.frame_id());

        victim.set_id(page_id);
        victim.set_page_key(Some(page_key));
        victim.dirty().store(true, Ordering::Release);
        victim.evict_info().reset();
        self.fm.increment_used();

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
        self.overflow_contains_key(&key.p_key())
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.overflow_get_page_keys(c_key)
    }

    #[inline(always)]
    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageWrite);
        self.stats.inc_write_count();

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        if self.meta(pref).key() == Some(page_key) {
            if let Some(g) = self.try_get_write_guard(pref, true) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    return Ok(g);
                }
            }
        }

        self.get_page_for_write_slow(page_key, pref)
    }

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        self.stats.inc_read_count();

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        if self.meta(pref).key() == Some(page_key) {
            if let Some(g) = self.try_get_read_guard(pref) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    return Ok(g);
                }
            }
        }

        self.get_page_for_read_slow(page_key, pref)
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.fm.flush_all()
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.fm.flush_all_and_reset(|pk, idx| {
            if self.overflow_lookup(pk) == Some(idx) {
                self.overflow_remove(pk);
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
    }
}
