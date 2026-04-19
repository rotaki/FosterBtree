mod buffer_pool;
mod buffer_pool_clock;
mod dashmap_bp;
mod eviction_policy;
mod frame_guards;
pub(crate) mod hash;
mod hashmap_bp;
mod in_mem_pool;
mod macro_profile;
mod mem_pool_trait;
mod open_addressing_bp;
mod overflow_bp;
mod overflow_table;
pub mod predictive_translation;
pub mod predictive_translation_fp;
mod predictive_translation_fp_four;
pub mod predictive_translation_fp_tlb;
mod predictive_translation_fp_two;
pub mod predictive_translation_tlb_only;
mod predictive_translation_two;
mod resident_set;
pub mod tlb_bp;
mod vmcache;

use std::sync::Arc;

pub use buffer_pool::BufferPool;
pub use buffer_pool_clock::BufferPoolClock;
pub use dashmap_bp::DashmapBP;
pub use eviction_policy::EvictionPolicy;
pub use frame_guards::{FrameReadGuard, FrameWriteGuard};
pub use hashmap_bp::HashmapBP;
pub use in_mem_pool::InMemPool;
pub use macro_profile::reset as reset_macro_profile;
pub use mem_pool_trait::{
    ContainerId, ContainerKey, DatabaseId, MemPool, MemPoolStatus, PageFrameKey,
};
pub use open_addressing_bp::OpenAddressingBP;
pub use overflow_bp::OverflowBP;
pub use predictive_translation::PredictiveTranslationBP;
pub use predictive_translation_fp::PredictiveTranslationFPBP;
pub use predictive_translation_fp_four::PredictiveTranslationFPFourBP;
pub use predictive_translation_fp_tlb::PredictiveTranslationFPTlbBP;
pub use predictive_translation_fp_two::PredictiveTranslationFPTwoBP;
pub use predictive_translation_tlb_only::PredictiveTranslationTlbOnlyBP;
pub use tlb_bp::TlbBP;
pub use predictive_translation_two::PredictiveTranslationTwoBP;
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

pub fn get_test_pt(num_frames: usize) -> Arc<PredictiveTranslationBP> {
    let base_dir = gen_random_pathname(Some("test_pt_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_two_hash(num_frames: usize) -> Arc<PredictiveTranslationTwoBP> {
    let base_dir = gen_random_pathname(Some("test_pt2_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationTwoBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_fp(num_frames: usize) -> Arc<PredictiveTranslationFPBP> {
    let base_dir = gen_random_pathname(Some("test_pt_fp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_fp_two_hash(num_frames: usize) -> Arc<PredictiveTranslationFPTwoBP> {
    let base_dir = gen_random_pathname(Some("test_pt_fp2_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPTwoBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_fp_tlb(num_frames: usize) -> Arc<PredictiveTranslationFPTlbBP> {
    let base_dir = gen_random_pathname(Some("test_pt_fp_tlb_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPTlbBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_tlb_only(num_frames: usize) -> Arc<PredictiveTranslationTlbOnlyBP> {
    let base_dir = gen_random_pathname(Some("test_pt_tlb_only_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationTlbOnlyBP::new(num_frames, cm).unwrap())
}

pub fn get_test_tlb_bp(num_frames: usize) -> Arc<TlbBP> {
    let base_dir = gen_random_pathname(Some("test_tlb_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(TlbBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_fp_four_hash(num_frames: usize) -> Arc<PredictiveTranslationFPFourBP> {
    let base_dir = gen_random_pathname(Some("test_pt_fp4_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPFourBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_bucket_validate(num_frames: usize) -> Arc<PredictiveTranslationFPBP> {
    let base_dir = gen_random_pathname(Some("test_pt_bucket_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_two_hash_bucket_validate(
    num_frames: usize,
) -> Arc<PredictiveTranslationFPTwoBP> {
    let base_dir = gen_random_pathname(Some("test_pt2_bucket_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PredictiveTranslationFPTwoBP::new(num_frames, cm).unwrap())
}

pub fn get_test_pt_two_hash_with_cm(
    num_frames: usize,
    cm: Arc<ContainerManager>,
) -> Arc<PredictiveTranslationTwoBP> {
    Arc::new(PredictiveTranslationTwoBP::new(num_frames, cm).unwrap())
}

pub fn get_test_overflow_bp(num_frames: usize) -> Arc<OverflowBP> {
    let base_dir = gen_random_pathname(Some("test_overflow_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(OverflowBP::new(num_frames, cm).unwrap())
}

pub fn get_test_open_addressing_bp(num_frames: usize) -> Arc<OpenAddressingBP> {
    let base_dir = gen_random_pathname(Some("test_oa_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(OpenAddressingBP::new(num_frames, cm).unwrap())
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
pub mod prelude {
    pub use super::get_test_dashmap_bp;
    #[cfg(feature = "bp_hashmap")]
    pub use super::get_test_hashmap_bp;
    pub use super::{
        get_in_mem_pool, get_test_bp, get_test_open_addressing_bp, get_test_overflow_bp,
        get_test_pt, get_test_pt_two_hash, get_test_pt_two_hash_bucket_validate,
        get_test_pt_two_hash_with_cm, BufferPool, ContainerId, ContainerKey, DashmapBP, DatabaseId,
        FrameReadGuard, FrameWriteGuard, HashmapBP, InMemPool, MemPool, MemPoolStatus,
        OpenAddressingBP, OverflowBP, PageFrameKey, PredictiveTranslationBP,
        PredictiveTranslationFPBP, PredictiveTranslationFPTwoBP, PredictiveTranslationTwoBP,
    };
}
