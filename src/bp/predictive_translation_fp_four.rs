//! Fast-path wrapper: four-hash PT + bucket-validate-first.

use std::sync::Arc;

use super::{
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
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
pub struct PredictiveTranslationFPFourBP {
    inner: PredictiveTranslationBP,
}

unsafe impl Sync for PredictiveTranslationFPFourBP {}
unsafe impl Send for PredictiveTranslationFPFourBP {}

impl std::ops::Deref for PredictiveTranslationFPFourBP {
    type Target = PredictiveTranslationBP;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PredictiveTranslationFPFourBP {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        Ok(Self {
            inner: PredictiveTranslationBP::new(num_frames, container_manager)?,
        })
    }
}

impl Drop for PredictiveTranslationFPFourBP {
    fn drop(&mut self) {}
}

impl MemPool for PredictiveTranslationFPFourBP {
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
        let [p0, p1, p2, p3] = self.preferred_frames_four(&page_key);
        let (p0, p1, p2, p3) = (p0 as usize, p1 as usize, p2 as usize, p3 as usize);

        // Manually unrolled fast path: check all four preferred frames.
        let metas = unsafe { &*self.metas.get() };
        macro_rules! try_read {
            ($p:expr) => {
                if metas[$p].key() == Some(page_key) {
                    if let Some(g) = self.try_get_read_guard($p) {
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
            };
        }
        try_read!(p0);
        try_read!(p1);
        try_read!(p2);
        try_read!(p3);

        self.get_page_for_read_slow(page_key, [p0, p1, p2, p3])
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
        let [p0, p1, p2, p3] = self.preferred_frames_four(&page_key);
        let (p0, p1, p2, p3) = (p0 as usize, p1 as usize, p2 as usize, p3 as usize);

        // Manually unrolled fast path.
        let metas = unsafe { &*self.metas.get() };
        macro_rules! try_write {
            ($p:expr) => {
                if metas[$p].key() == Some(page_key) {
                    if let Some(g) = self.try_get_write_guard($p, true) {
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
            };
        }
        try_write!(p0);
        try_write!(p1);
        try_write!(p2);
        try_write!(p3);

        self.get_page_for_write_slow(page_key, [p0, p1, p2, p3])
    }

    fn create_container(&self, c_key: ContainerKey, is_temp: bool) -> Result<(), MemPoolStatus> {
        self.inner.create_container(c_key, is_temp)
    }
    fn drop_container(&self, c_key: ContainerKey) -> Result<(), MemPoolStatus> {
        self.inner.drop_container(c_key)
    }
    fn create_new_page_for_write(&self, c_key: ContainerKey) -> Result<FWGuard, MemPoolStatus> {
        self.inner.create_new_page_for_write_four_hash(c_key)
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
