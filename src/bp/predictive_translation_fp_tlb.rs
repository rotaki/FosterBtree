//! Fast-path wrapper with a **per-thread L1-resident translation cache (TLB)**.
//!
//! Each thread maintains a small, fixed-size cache of recent `PageKey → frame_id`
//! translations. This cache is checked *before* both the preferred-frame metadata
//! check and the overflow table lookup. At 32 entries × 16 bytes = 512 bytes, it
//! fits entirely in L1 cache (~32–64 KB per core), avoiding the pointer chase
//! into heap-allocated `FrameMeta` (~10–30 ns, L2/L3) and the overflow hash table
//! probe (~50–100 ns, L3/DRAM).
//!
//! Single-hash variant (one preferred frame per page).

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
// Per-thread translation cache (TLB)
// ---------------------------------------------------------------------------

/// Number of TLB entries. 64 entries × 8 bytes = 512 bytes (fits in L1).
const TLB_SIZE: usize = 64;

/// A single TLB entry: 8 bytes total.
///
/// Uses two hashes of the PageKey: one selects the slot, the other is stored
/// as a 32-bit tag for verification. False positive rate: 1 in 4 billion.
/// Since the TLB is a hint (verified by latch + key check), false positives
/// only cost one extra failed latch attempt (~10 ns).
#[derive(Clone, Copy)]
#[repr(C)]
struct TlbEntry {
    /// 32-bit tag: second hash of the PageKey. 0 = empty sentinel.
    tag: u32,
    /// Cached frame index.
    frame_id: u32,
}

impl TlbEntry {
    const EMPTY: Self = Self { tag: 0, frame_id: 0 };
}

/// Hash a PageKey into a slot index + tag pair.
///
/// Uses the packed u64 (same as hash_page_key) with two different bit mixers
/// so slot and tag are independent.
#[inline(always)]
fn tlb_slot_and_tag(key: &PageKey) -> (usize, u32) {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    // Slot: xor-shift mix, take low bits.
    let slot_hash = packed ^ (packed >> 17);
    let slot = (slot_hash as usize) % TLB_SIZE;
    // Tag: different xor-shift mix, take upper 32 bits. Add 1 to avoid 0 (empty sentinel).
    let tag_hash = packed ^ (packed >> 13);
    let tag = ((tag_hash >> 32) as u32) | 1; // ensure non-zero
    (slot, tag)
}

/// Per-thread direct-mapped translation cache.
///
/// Direct-mapped: each PageKey maps to exactly one slot. 8 bytes per entry,
/// 64 entries = 512 bytes, fits entirely in L1 cache.
/// Conflicts evict silently — the TLB is a hint, not authoritative.
struct ThreadTlb {
    entries: [TlbEntry; TLB_SIZE],
}

impl ThreadTlb {
    const NEW: Self = Self {
        entries: [TlbEntry::EMPTY; TLB_SIZE],
    };

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

#[thread_local]
static mut THREAD_TLB: ThreadTlb = ThreadTlb::NEW;

// ---------------------------------------------------------------------------
// Buffer pool wrapper
// ---------------------------------------------------------------------------

/// Predictive-translation buffer pool with per-thread L1-resident TLB.
#[repr(transparent)]
pub struct PredictiveTranslationFPTlbBP {
    inner: PredictiveTranslationBP,
}

unsafe impl Sync for PredictiveTranslationFPTlbBP {}
unsafe impl Send for PredictiveTranslationFPTlbBP {}

impl std::ops::Deref for PredictiveTranslationFPTlbBP {
    type Target = PredictiveTranslationBP;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PredictiveTranslationFPTlbBP {
    pub fn new(
        num_frames: usize,
        container_manager: Arc<ContainerManager>,
    ) -> Result<Self, MemPoolStatus> {
        Ok(Self {
            inner: PredictiveTranslationBP::new(num_frames, container_manager)?,
        })
    }
}

impl Drop for PredictiveTranslationFPTlbBP {
    fn drop(&mut self) {
        // Drop is handled by the inner PredictiveTranslationBP.
    }
}

impl MemPool for PredictiveTranslationFPTlbBP {
    type EP = EvictionPolicyImpl;

    // ----- TLB-accelerated fast paths ----------------------------------------

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let _macro_timer = macro_profile_scoped(BpMacroOp::GetPageRead);
        self.stats.inc_read_count();

        #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
        self.profile
            .total_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let page_key = key.p_key();

        let tlb = unsafe { &mut THREAD_TLB };

        // TLB fast path.
        if let Some(frame_id) = tlb.lookup(&page_key) {
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
            tlb.invalidate(&page_key);
        }

        // Preferred-frame fast path.
        let pref = self.preferred_frame(&page_key) as usize;
        let metas = unsafe { &*self.metas.get() };
        if metas[pref].key() == Some(page_key) {
            if let Some(g) = self.try_get_read_guard(pref) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    tlb.insert(&page_key, pref as u32);
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(g);
                }
            }
        }

        // Slow path.
        let result = self.get_page_for_read_slow(page_key, [pref]);

        if let Ok(ref g) = result {
            tlb.insert(&page_key, g.frame_id());
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

        let tlb = unsafe { &mut THREAD_TLB };

        // TLB fast path.
        if let Some(frame_id) = tlb.lookup(&page_key) {
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
            tlb.invalidate(&page_key);
        }

        // Preferred-frame fast path.
        let pref = self.preferred_frame(&page_key) as usize;
        let metas = unsafe { &*self.metas.get() };
        if metas[pref].key() == Some(page_key) {
            if let Some(g) = self.try_get_write_guard(pref, true) {
                if g.page_key() == Some(page_key) {
                    g.evict_info().update();
                    tlb.insert(&page_key, pref as u32);
                    #[cfg(any(feature = "pt_profile", feature = "pt_counts"))]
                    self.profile
                        .preferred_frame_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(g);
                }
            }
        }

        // Slow path.
        let result = self.get_page_for_write_slow(page_key, [pref]);

        if let Ok(ref g) = result {
            tlb.insert(&page_key, g.frame_id() as u32);
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
