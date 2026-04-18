//! Buffer pool variant: **TLB + overflow table only** (no preferred frame).
//!
//! Every page is tracked in the overflow table (like base PT). There is no
//! preferred frame check or promotion/demotion. Instead, a per-thread
//! L1-resident TLB caches recent translations to skip the overflow table
//! lookup on hot pages.
//!
//! 4-way set-associative, 1024 sets, 5-bit tag pre-filter.
//! Total: 4096 entries × 4 bytes = 16 KB.

use std::sync::Arc;

use super::{
    eviction_policy::{ClockEvictionPolicy, EvictionPolicy},
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, MemoryStats, PageFrameKey, PageKey},
    predictive_translation::PredictiveTranslationBP,
    FrameReadGuard, FrameWriteGuard,
};
use crate::container::ContainerManager;

type EvictionPolicyImpl = ClockEvictionPolicy;
type FRGuard = FrameReadGuard<EvictionPolicyImpl>;
type FWGuard = FrameWriteGuard<EvictionPolicyImpl>;

// ---------------------------------------------------------------------------
// Per-thread TLB: 4-way set-associative with 5-bit tag pre-filter.
// ---------------------------------------------------------------------------

const TLB_SETS: usize = 1024;
const TLB_WAYS: usize = 4;
const TLB_SET_MASK: usize = TLB_SETS - 1;

// Bit layout per entry: [tag:5 | frame_id:27]
const FRAME_BITS: u32 = 27;
const FRAME_MASK: u32 = (1 << FRAME_BITS) - 1; // 27-bit = 128M frames = 2 TB
const TAG_SHIFT: u32 = FRAME_BITS;

#[inline(always)]
fn tlb_set_index(key: &PageKey) -> usize {
    let c_hash = key.c_key.as_u32().wrapping_mul(2654435761);
    (c_hash as usize).wrapping_add(key.page_id as usize) & TLB_SET_MASK
}

#[inline(always)]
fn tlb_tag(key: &PageKey) -> u32 {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    let h = packed.wrapping_mul(0x9e3779b97f4a7c15);
    let t = ((h >> 59) as u32) | 1; // 5 bits, non-zero
    t << TAG_SHIFT
}

#[inline(always)]
fn pack_entry(tag: u32, frame_id: u32) -> u32 {
    tag | (frame_id & FRAME_MASK)
}

#[inline(always)]
fn entry_tag(entry: u32) -> u32 {
    entry & !FRAME_MASK
}

#[inline(always)]
fn entry_frame(entry: u32) -> usize {
    (entry & FRAME_MASK) as usize
}

#[thread_local]
static mut TLB: [[u32; TLB_WAYS]; TLB_SETS] = [[0; TLB_WAYS]; TLB_SETS];

const _: [(); TLB_WAYS] = [(); 4];

#[inline(always)]
fn tlb_insert_front(ways: &mut [u32; TLB_WAYS], entry: u32) {
    ways[3] = ways[2];
    ways[2] = ways[1];
    ways[1] = ways[0];
    ways[0] = entry;
}

macro_rules! tlb_probe_4way {
    ($bp:expr, $ways:expr, $page_key:expr, $tag:expr, $guard_fn:ident $(, $guard_arg:expr)*) => {{
        let ways = $ways;
        let wanted_key = Some($page_key);
        let metas = unsafe { &*($bp).metas.get() };

        macro_rules! probe_way {
            ($w:expr) => {{
                let entry = ways[$w];
                if entry_tag(entry) == $tag {
                    let frame_id = entry_frame(entry);
                    if metas[frame_id].key() == wanted_key {
                        if let Some(g) = ($bp).$guard_fn(frame_id $(, $guard_arg)*) {
                            if g.page_key() == wanted_key {
                                g.evict_info().update();
                                if $w != 0 {
                                    ways.swap(0, $w);
                                }
                                return Some(g);
                            }
                        }
                    }
                }
            }};
        }

        probe_way!(0);
        probe_way!(1);
        probe_way!(2);
        probe_way!(3);
        None
    }};
}

// ---------------------------------------------------------------------------
// Buffer pool wrapper
// ---------------------------------------------------------------------------

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

    #[inline(always)]
    fn tlb_read_hit(
        &self,
        ways: &mut [u32; TLB_WAYS],
        tag: u32,
        page_key: PageKey,
    ) -> Option<FRGuard> {
        tlb_probe_4way!(self, ways, page_key, tag, try_get_read_guard)
    }

    #[inline(always)]
    fn tlb_write_hit(
        &self,
        ways: &mut [u32; TLB_WAYS],
        tag: u32,
        page_key: PageKey,
    ) -> Option<FWGuard> {
        tlb_probe_4way!(self, ways, page_key, tag, try_get_write_guard, true)
    }
}

impl Drop for PredictiveTranslationTlbOnlyBP {
    fn drop(&mut self) {}
}

impl MemPool for PredictiveTranslationTlbOnlyBP {
    type EP = EvictionPolicyImpl;

    #[inline(always)]
    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FRGuard, MemPoolStatus> {
        let page_key = key.p_key();
        let set = tlb_set_index(&page_key);
        let tag = tlb_tag(&page_key);

        let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
        if let Some(g) = self.tlb_read_hit(ways, tag, page_key) {
            return Ok(g);
        }

        // Miss — overflow lookup (no preferred frame).
        self.ensure_free_frames()?;
        loop {
            if let Some(idx) = self.overflow.lookup(&page_key) {
                if let Some(g) = self.try_get_read_guard(idx) {
                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
                        tlb_insert_front(ways, pack_entry(tag, idx as u32));
                        return Ok(g);
                    }
                } else if self.overflow.lookup(&page_key) == Some(idx) {
                    return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                }
                continue;
            }
            match self.handle_page_fault_write(page_key, []) {
                Ok(victim) => {
                    let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
                    tlb_insert_front(ways, pack_entry(tag, victim.frame_id()));
                    return Ok(victim.downgrade());
                }
                Err(MemPoolStatus::RetryPageFault) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    #[inline(always)]
    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FWGuard, MemPoolStatus> {
        let page_key = key.p_key();
        let set = tlb_set_index(&page_key);
        let tag = tlb_tag(&page_key);

        let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
        if let Some(g) = self.tlb_write_hit(ways, tag, page_key) {
            return Ok(g);
        }

        // Miss — overflow lookup (no preferred frame).
        self.ensure_free_frames()?;
        loop {
            if let Some(idx) = self.overflow.lookup(&page_key) {
                if let Some(g) = self.try_get_write_guard(idx, true) {
                    if g.page_key() == Some(page_key) {
                        g.evict_info().update();
                        let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
                        tlb_insert_front(ways, pack_entry(tag, idx as u32));
                        return Ok(g);
                    }
                } else if self.overflow.lookup(&page_key) == Some(idx) {
                    return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                }
                continue;
            }
            match self.handle_page_fault_write(page_key, []) {
                Ok(g) => {
                    let ways = unsafe { &mut *TLB.get_unchecked_mut(set) };
                    tlb_insert_front(ways, pack_entry(tag, g.frame_id()));
                    return Ok(g);
                }
                Err(MemPoolStatus::RetryPageFault) => continue,
                Err(e) => return Err(e),
            }
        }
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
