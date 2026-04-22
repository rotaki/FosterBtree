mod buffer_pool;
mod buffer_pool_clock;
mod buffer_pool_clock_v2;
mod dashmap_bp;
mod eviction_policy;
mod frame_guards;
pub(crate) mod frame_manager;
pub(crate) mod hash;
mod hashmap_bp;
mod in_mem_pool;
mod macro_profile;
mod mem_pool_trait;
mod overflow_table;
pub mod predictive_translation;
pub mod predictive_translation_fp;
pub mod predictive_translation_fp_v2;
pub mod predictive_translation_v2;
mod resident_set;
pub mod tlb_bp;
pub mod tlb_bp_v2;
mod vmcache;

use std::sync::Arc;

pub use buffer_pool::BufferPool;
pub use buffer_pool_clock::BufferPoolClock;
pub use buffer_pool_clock_v2::BufferPoolClockV2;
pub use dashmap_bp::DashmapBP;
pub use eviction_policy::EvictionPolicy;
pub use frame_guards::{FrameReadGuard, FrameWriteGuard};
pub use hashmap_bp::HashmapBP;
pub use in_mem_pool::InMemPool;
pub use macro_profile::reset as reset_macro_profile;
pub use mem_pool_trait::{
    ContainerId, ContainerKey, DatabaseId, MemPool, MemPoolStatus, PageFrameKey,
};
pub use predictive_translation::PredictiveTranslationBP;
pub use predictive_translation_fp::PredictiveTranslationFPBP;
pub use predictive_translation_fp_v2::PredictiveTranslationFPBPV2;
pub use predictive_translation_v2::PredictiveTranslationBPV2;
pub use tlb_bp::TlbBP;
pub use tlb_bp_v2::TlbBPV2;
pub use vmcache::VMCachePool;

use crate::{container::ContainerManager, random::gen_random_pathname};

pub fn get_test_bp(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_with_kpc(num_frames: usize) -> Arc<BufferPool> {
    let base_dir = gen_random_pathname(Some("test_bp_with_kpc"));
    let cm = Arc::new(ContainerManager::new(base_dir, false, true).unwrap());
    Arc::new(BufferPool::new(num_frames, cm).unwrap())
}

pub fn get_test_vmcache<const IS_SMALL: bool, const EVICTION_BATCH_SIZE: usize>(
    num_frames: usize,
) -> Arc<VMCachePool<IS_SMALL, EVICTION_BATCH_SIZE>> {
    let base_dir = gen_random_pathname(Some("test_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(VMCachePool::<IS_SMALL, EVICTION_BATCH_SIZE>::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_clock<const EVICTION_BATCH_SIZE: usize>(
    num_frames: usize,
) -> Arc<BufferPoolClock<EVICTION_BATCH_SIZE>> {
    let base_dir = gen_random_pathname(Some("test_bp_clock_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPoolClock::<EVICTION_BATCH_SIZE>::new(num_frames, cm).unwrap())
}

pub fn get_test_bp_clock_v2<const EVICTION_BATCH_SIZE: usize>(
    num_frames: usize,
) -> Arc<BufferPoolClockV2<EVICTION_BATCH_SIZE>> {
    let base_dir = gen_random_pathname(Some("test_bp_clock_v2_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(BufferPoolClockV2::<EVICTION_BATCH_SIZE>::new(num_frames, cm).unwrap())
}

pub fn get_test_pt(num_frames: usize) -> Arc<PredictiveTranslationBP> {
    let base_dir = gen_random_pathname(Some("test_pt_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_fp(num_frames: usize) -> Arc<PredictiveTranslationFPBP> {
    let base_dir = gen_random_pathname(Some("test_pt_fp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPBP::new(num_frames, cm).unwrap())
}

pub fn get_test_tlb_bp(num_frames: usize) -> Arc<TlbBP> {
    let base_dir = gen_random_pathname(Some("test_tlb_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(TlbBP::new(num_frames, cm).unwrap())
}

pub fn get_test_tlb_bp_v2(num_frames: usize) -> Arc<TlbBPV2> {
    let base_dir = gen_random_pathname(Some("test_tlb_bp_v2_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(TlbBPV2::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_bucket_validate(num_frames: usize) -> Arc<PredictiveTranslationFPBP> {
    let base_dir = gen_random_pathname(Some("test_pt_bucket_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_bucket_validate_v2(num_frames: usize) -> Arc<PredictiveTranslationFPBPV2> {
    let base_dir = gen_random_pathname(Some("test_pt_bucket_v2_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPBPV2::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_v2(num_frames: usize) -> Arc<PredictiveTranslationBPV2> {
    let base_dir = gen_random_pathname(Some("test_pt_v2_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationBPV2::new(num_frames, cm).unwrap())
}

/// Order-preserving-hash PT V2 (no FP wrapper). Same type as `get_test_pt_v2`
/// but the `pt_op_hash` feature changes `preferred_frame` to use the TLB-style
/// hash composition `hash(c_key) + page_id`. Enabled by `bp_pt_v2_ophash`.
pub fn get_test_pt_v2_ophash(num_frames: usize) -> Arc<PredictiveTranslationBPV2> {
    let base_dir = gen_random_pathname(Some("test_pt_v2_ophash_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationBPV2::new(num_frames, cm).unwrap())
}

/// Order-preserving-hash PT(FP) V2. Same type as `get_test_pt_bucket_validate_v2`
/// but the `pt_op_hash` feature changes `preferred_frame`. Enabled by
/// `bp_pt_bucket_v2_ophash`.
pub fn get_test_pt_bucket_validate_v2_ophash(
    num_frames: usize,
) -> Arc<PredictiveTranslationFPBPV2> {
    let base_dir = gen_random_pathname(Some("test_pt_bucket_v2_ophash_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPBPV2::new(num_frames, cm).unwrap())
}

pub fn get_test_dashmap_bp(num_frames: usize) -> Arc<DashmapBP> {
    let base_dir = gen_random_pathname(Some("test_dashmap_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(DashmapBP::new(num_frames, cm).unwrap())
}

#[cfg(feature = "bp_hashmap")]
pub fn get_test_hashmap_bp(num_frames: usize) -> Arc<HashmapBP> {
    let base_dir = gen_random_pathname(Some("test_hashmap_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(HashmapBP::new(num_frames, cm).unwrap())
}

pub fn get_in_mem_pool() -> Arc<InMemPool> {
    Arc::new(InMemPool::new())
}

/// Replica of `PredictiveTranslationBPV2::preferred_frame`, exposed for
/// benchmarks that need to pick page ids by their target preferred slot
/// (e.g. deterministic-collision microbenchmarks). The formula is kept in
/// sync with the `pt_op_hash` feature: when it's enabled we use the
/// TLB-style order-preserving composition, otherwise the Stafford-mixed
/// full key. See plan: PT strength/weakness study, Part B2.
#[inline]
pub fn pt_preferred_slot(c_key: u32, page_id: u32, num_frames: u64) -> u32 {
    #[inline(always)]
    fn fastmod(hash: u64, n: u64) -> u32 {
        (((hash as u128).wrapping_mul(n as u128)) >> 64) as u32
    }
    #[cfg(feature = "pt_op_hash")]
    {
        let c_hash = hash::hash_u64(c_key as u64);
        let packed = c_hash.wrapping_add(page_id as u64);
        fastmod(packed, num_frames)
    }
    #[cfg(not(feature = "pt_op_hash"))]
    {
        let packed = (c_key as u64) << 32 | page_id as u64;
        fastmod(hash::hash_u64(packed), num_frames)
    }
}
pub mod prelude {
    pub use super::get_test_dashmap_bp;
    #[cfg(feature = "bp_hashmap")]
    pub use super::get_test_hashmap_bp;
    pub use super::{
        get_in_mem_pool, get_test_bp, get_test_pt, BufferPool, ContainerId, ContainerKey,
        DashmapBP, DatabaseId, FrameReadGuard, FrameWriteGuard, HashmapBP, InMemPool, MemPool,
        MemPoolStatus, PageFrameKey, PredictiveTranslationBP, PredictiveTranslationFPBP,
    };
}
