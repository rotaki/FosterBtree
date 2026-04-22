//! Fast-path wrapper around `PredictiveTranslationBPV2` (FrameManager-based PT
//! shadow). Mirrors `PredictiveTranslationFPBP` but targets the V2 inner.
//!
//! Status: shadow — coexists with the original `PredictiveTranslationFPBP` so
//! we can A/B benchmark `bp_pt_bucket` vs `bp_pt_bucket_v2`.

use std::sync::Arc;

use super::{
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    macro_profile::{scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey},
    predictive_translation_v2::PredictiveTranslationBPV2,
    FrameReadGuard, FrameWriteGuard,
};
use crate::container::ContainerManager;

type EvictionPolicyImpl = ClockEvictionPolicy;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;

/// V2 PT with bucket-validate-first compile-time fast path.
#[repr(transparent)]
pub struct PredictiveTranslationFPBPV2 {
    inner: PredictiveTranslationBPV2,
}

unsafe impl Sync for PredictiveTranslationFPBPV2 {}
unsafe impl Send for PredictiveTranslationFPBPV2 {}

impl std::ops::Deref for PredictiveTranslationFPBPV2 {
    type Target = PredictiveTranslationBPV2;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PredictiveTranslationFPBPV2 {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        Ok(Self {
            inner: PredictiveTranslationBPV2::new(num_frames, container_manager)?,
        })
    }
}

impl Drop for PredictiveTranslationFPBPV2 {
    fn drop(&mut self) {
        // Inner handles flush-on-drop.
    }
}

impl MemPool for PredictiveTranslationFPBPV2 {
    type EP = EvictionPolicyImpl;

    // ----- inlined fast paths ------------------------------------------------

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        self.stats.inc_read_count();

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile
            .total_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        // Inlined fast path: check preferred frame metadata before slow path.
        // V2 routes through `meta(idx)` instead of poking `metas` directly —
        // FrameManager owns the storage.
        if self.meta(pref).key() == Some(page_key) {
            if let Some(g) = self.try_get_read_guard(pref) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(g);
                }
            }
        }

        // Slow path: ensure free frames, overflow lookup, page fault.
        self.get_page_for_read_slow(page_key, pref)
    }

    #[inline(always)]
    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageWrite);
        self.stats.inc_write_count();

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile
            .total_writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let page_key = key.p_key();
        let pref = self.preferred_frame(&page_key);

        if self.meta(pref).key() == Some(page_key) {
            if let Some(g) = self.try_get_write_guard(pref, true) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(g);
                }
            }
        }

        self.get_page_for_write_slow(page_key, pref)
    }

    // ----- delegated methods (inner avoids infinite recursion) --------------

    fn create_container(&self, c_key: ContainerKey, is_temp: bool) -> Result<(), MemPoolStatus> {
        self.inner.create_container(c_key, is_temp)
    }

    fn drop_container(&self, c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        self.inner.drop_container(c_key)
    }

    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.inner.create_new_page_for_write(c_key)
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        num_pages: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        self.inner.create_new_pages_for_write(c_key, num_pages)
    }

    fn is_in_mem(&self, key: PageFrameKey) -> bool {
        self.inner.is_in_mem(key)
    }

    fn get_page_keys_in_mem(&self, c_key: ContainerKey) -> Vec<PageFrameKey> {
        self.inner.get_page_keys_in_mem(c_key)
    }

    fn prefetch_page(&self, key: PageFrameKey) -> Result<(), MemPoolStatus> {
        self.inner.prefetch_page(key)
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.inner.flush_all()
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.inner.flush_all_and_reset()
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        self.inner.clear_dirty_flags()
    }

    fn fast_evict(&self, frame_id: u32) -> Result<(), MemPoolStatus> {
        self.inner.fast_evict(frame_id)
    }

    unsafe fn stats(&self) -> MemoryStats {
        self.inner.stats()
    }

    unsafe fn reset_stats(&self) {
        self.inner.reset_stats()
    }

    fn print_profile(&self) {
        self.inner.print_profile()
    }

    fn sample_coverage(&self) -> (u64, u64) {
        self.inner.sample_coverage()
    }
}
