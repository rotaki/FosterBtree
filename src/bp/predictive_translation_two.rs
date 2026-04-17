//! Fast-path wrapper: two-hash PT (no bucket-validate-first).
//!
//! Uses two hash predictions but does NOT do the metadata-verify-first
//! optimisation. The fast path still checks preferred frames inline but
//! falls through to the slow path (with prefetch) on miss.

use std::sync::Arc;

use super::{
    eviction_policy::ClockEvictionPolicy,
    macro_profile::{scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey},
    predictive_translation::PredictiveTranslationBP,
    FrameReadGuard, FrameWriteGuard,
};
use crate::container::ContainerManager;

type EvictionPolicyImpl = ClockEvictionPolicy;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;

#[repr(transparent)]
pub struct PredictiveTranslationTwoBP {
    inner: PredictiveTranslationBP,
}

unsafe impl Sync for PredictiveTranslationTwoBP {}
unsafe impl Send for PredictiveTranslationTwoBP {}

impl std::ops::Deref for PredictiveTranslationTwoBP {
    type Target = PredictiveTranslationBP;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PredictiveTranslationTwoBP {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        Ok(Self {
            inner: PredictiveTranslationBP::new(num_frames, container_manager)?,
        })
    }
}

impl Drop for PredictiveTranslationTwoBP {
    fn drop(&mut self) {}
}

impl MemPool for PredictiveTranslationTwoBP {
    type EP = EvictionPolicyImpl;

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        self.stats.inc_read_count();

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile
            .total_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let page_key = key.p_key();
        let (pref1, pref2) = self.preferred_frames(&page_key);
        let (pref1, pref2) = (pref1 as usize, pref2 as usize);

        #[cfg(not(feature = "pt_no_prefetch"))]
        self.prefetch_predicted_frames(pref1, pref2);

        self.get_page_for_read_slow(page_key, [pref1, pref2])
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
        let (pref1, pref2) = self.preferred_frames(&page_key);
        let (pref1, pref2) = (pref1 as usize, pref2 as usize);

        #[cfg(not(feature = "pt_no_prefetch"))]
        self.prefetch_predicted_frames(pref1, pref2);

        self.get_page_for_write_slow(page_key, [pref1, pref2])
    }

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
        n: usize,
    ) -> Result<Vec<FWGuard>, MemPoolStatus> {
        self.inner.create_new_pages_for_write(c_key, n)
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
}
