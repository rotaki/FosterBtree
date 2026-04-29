mod buffer_pool;
mod buffer_pool_clock;
mod eviction_policy;
mod frame_guards;
pub(crate) mod frame_manager;
pub(crate) mod hash;
mod in_mem_pool;
mod macro_profile;
pub mod mem_pool_trait;
pub mod optimistic_page_map;
pub mod predicache;
pub mod lapt;
mod resident_set;
pub mod tlb_bp;
mod vmcache;

use std::sync::Arc;

pub use buffer_pool::BufferPool;
pub use buffer_pool_clock::BufferPoolClock;
pub use eviction_policy::EvictionPolicy;
pub use frame_guards::{FrameReadGuard, FrameWriteGuard};
pub use in_mem_pool::InMemPool;
pub use macro_profile::reset as reset_macro_profile;
pub use mem_pool_trait::{
    ContainerId, ContainerKey, DatabaseId, MemPool, MemPoolStatus, PageFrameKey,
};
pub use predicache::PrediCache;
pub use lapt::Lapt;
pub use tlb_bp::TlbBP;
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

pub fn get_test_predicache(num_frames: usize) -> Arc<PrediCache> {
    let base_dir = gen_random_pathname(Some("test_pt_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(PrediCache::new(num_frames, cm).unwrap())
}

pub fn get_test_tlb_bp(num_frames: usize) -> Arc<TlbBP> {
    let base_dir = gen_random_pathname(Some("test_tlb_bp_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(TlbBP::new(num_frames, cm).unwrap())
}


/// Congee-backed PT V2 with FP wrapper (the "enhanced PrediCache"). Same
/// fast-path semantics as `get_test_pt_bucket_validate_v2` but the overflow
/// translator is a concurrent ART (`CongeeRawU32`) instead of the chaining
/// hashmap. Enabled by `bp_lapt`.
pub fn get_test_lapt(
    num_frames: usize,
) -> Arc<Lapt> {
    let base_dir = gen_random_pathname(Some("test_pt_bucket_v2_congee_direct"));
    let cm = Arc::new(ContainerManager::new(base_dir, true, true).unwrap());
    Arc::new(Lapt::new(num_frames, cm).unwrap())
}

pub fn get_in_mem_pool() -> Arc<InMemPool> {
    Arc::new(InMemPool::new())
}

/// Replica of `PrediCache::preferred_frame`, exposed for
/// benchmarks that need to pick page ids by their target preferred slot
/// (e.g. deterministic-collision microbenchmarks). The formula is kept in
/// TLB-style order-preserving composition, otherwise the Stafford-mixed
/// full key. See plan: PT strength/weakness study, Part B2.
#[inline]
pub fn pt_preferred_slot(c_key: u32, page_id: u32, num_frames: u64) -> u32 {
    #[inline(always)]
    fn fastmod(hash: u64, n: u64) -> u32 {
        (((hash as u128).wrapping_mul(n as u128)) >> 64) as u32
    }
    {
        let packed = (c_key as u64) << 32 | page_id as u64;
        fastmod(hash::hash_u64(packed), num_frames)
    }
}
pub mod prelude {
    pub use super::{
        get_in_mem_pool, get_test_bp, get_test_predicache, BufferPool, ContainerId, ContainerKey,
        DatabaseId, FrameReadGuard, FrameWriteGuard, InMemPool, MemPool,
        MemPoolStatus, PageFrameKey, PrediCache,
    };
}
