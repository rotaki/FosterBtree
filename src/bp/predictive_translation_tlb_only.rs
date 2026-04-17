//! Buffer pool variant: **TLB + overflow table only** (no preferred frame).
//!
//! Every page is tracked in the overflow table (like base PT). There is no
//! preferred frame check or promotion/demotion. Instead, a per-thread
//! L1-resident TLB (32 entries, 512 bytes) caches recent translations to
//! skip the overflow table lookup on hot pages.
//!
//! This isolates the TLB benefit from the preferred-frame mechanism.

use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    macro_profile::{scoped as macro_profile_scoped, BpMacroOp},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
    predictive_translation::PredictiveTranslationBP,
    FrameReadGuard, FrameWriteGuard,
};
use crate::container::ContainerManager;

type EvictionPolicyImpl = ClockEvictionPolicy;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;

// ---------------------------------------------------------------------------
// Per-thread TLB (same as predictive_translation_fp_tlb.rs)
// ---------------------------------------------------------------------------

const TLB_SIZE: usize = 64;

#[derive(Clone, Copy)]
#[repr(C)]
struct TlbEntry {
    tag: u32,
    frame_id: u32,
}

impl TlbEntry {
    const EMPTY: Self = Self { tag: 0, frame_id: 0 };
}

#[inline(always)]
fn tlb_slot_and_tag(key: &PageKey) -> (usize, u32) {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    let slot_hash = packed ^ (packed >> 17);
    let slot = (slot_hash as usize) % TLB_SIZE;
    let tag_hash = packed ^ (packed >> 13);
    let tag = ((tag_hash >> 32) as u32) | 1;
    (slot, tag)
}

struct ThreadTlb {
    entries: [TlbEntry; TLB_SIZE],
}

impl ThreadTlb {
    fn new() -> Self {
        Self {
            entries: [TlbEntry::EMPTY; TLB_SIZE],
        }
    }

    #[inline(always)]
    fn lookup(&self, key: &PageKey) -> Option<u32> {
        let (slot, tag) = tlb_slot_and_tag(key);
        let entry = unsafe { self.entries.get_unchecked(slot) };
        if entry.tag == tag {
            Some(entry.frame_id)
        } else {
            None
        }
    }

    #[inline(always)]
    fn insert(&mut self, key: &PageKey, frame_id: u32) {
        let (slot, tag) = tlb_slot_and_tag(key);
        let entry = unsafe { self.entries.get_unchecked_mut(slot) };
        entry.tag = tag;
        entry.frame_id = frame_id;
    }

    #[inline(always)]
    fn invalidate(&mut self, key: &PageKey) {
        let (slot, tag) = tlb_slot_and_tag(key);
        let entry = unsafe { self.entries.get_unchecked_mut(slot) };
        if entry.tag == tag {
            entry.tag = 0;
        }
    }
}

thread_local! {
    static THREAD_TLB: RefCell<ThreadTlb> = RefCell::new(ThreadTlb::new());
}

// ---------------------------------------------------------------------------
// Buffer pool wrapper
// ---------------------------------------------------------------------------

/// Buffer pool with TLB + overflow table only (no preferred frame).
#[repr(transparent)]
pub struct PredictiveTranslationTlbOnlyBP {
    inner: PredictiveTranslationBP,
}

unsafe impl Sync for PredictiveTranslationTlbOnlyBP {}
unsafe impl Send for PredictiveTranslationTlbOnlyBP {}

impl std::ops::Deref for PredictiveTranslationTlbOnlyBP {
    type Target = PredictiveTranslationBP;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PredictiveTranslationTlbOnlyBP {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        Ok(Self {
            inner: PredictiveTranslationBP::new(num_frames, container_manager)?,
        })
    }
}

impl Drop for PredictiveTranslationTlbOnlyBP {
    fn drop(&mut self) {}
}

impl MemPool for PredictiveTranslationTlbOnlyBP {
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

        // TLB fast path: L1-resident lookup.
        let tlb_hit = THREAD_TLB.with(|tlb| tlb.borrow().lookup(&page_key));
        if let Some(frame_id) = tlb_hit {
            let idx = frame_id as usize;
            if let Some(g) = self.try_get_read_guard(idx) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(g);
                }
            }
            THREAD_TLB.with(|tlb| tlb.borrow_mut().invalidate(&page_key));
        }

        // No preferred frame check — go straight to overflow table slow path.
        let pref = self.preferred_frame(&page_key) as usize;
        let result = self.get_page_for_read_slow(page_key, [pref]);

        // Populate TLB on success.
        if let Ok(ref g) = result {
            let fid = g.frame_id();
            THREAD_TLB.with(|tlb| tlb.borrow_mut().insert(&page_key, fid));
        }

        result
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

        // TLB fast path.
        let tlb_hit = THREAD_TLB.with(|tlb| tlb.borrow().lookup(&page_key));
        if let Some(frame_id) = tlb_hit {
            let idx = frame_id as usize;
            if let Some(g) = self.try_get_write_guard(idx, true) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(g);
                }
            }
            THREAD_TLB.with(|tlb| tlb.borrow_mut().invalidate(&page_key));
        }

        // No preferred frame check — straight to overflow slow path.
        let pref = self.preferred_frame(&page_key) as usize;
        let result = self.get_page_for_write_slow(page_key, [pref]);

        // Populate TLB on success.
        if let Ok(ref g) = result {
            let fid = g.frame_id();
            THREAD_TLB.with(|tlb| tlb.borrow_mut().insert(&page_key, fid as u32));
        }

        result
    }

    // ----- delegated methods -------------------------------------------------

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
}
