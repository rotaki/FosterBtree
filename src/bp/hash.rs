//! Fast hash for page-to-frame translation.
//!
//! Uses **Stafford Mix13** (the finalizer from splitmix64): 2 multiplies +
//! 3 xor-shifts.  ~0.8 ns per call vs ~1.4 ns for the previous Murmur64
//! finalizer, with equivalent avalanche quality.
//!
//! The hash is deterministic and has excellent avalanche properties; it is *not*
//! cryptographic, but we don't need that for internal page keys.

use super::mem_pool_trait::PageKey;

/// Stafford variant 13 (splitmix64 finalizer).
/// 2 multiplies, 3 xor-shifts — ~0.8 ns on modern x86.
#[inline(always)]
pub(crate) fn hash_u64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    x
}

/// Hash a `PageKey` (ContainerKey: u32 + PageId: u32) by packing into a single
/// u64 and running the Stafford Mix13 finalizer.
#[inline(always)]
pub(crate) fn hash_page_key(key: &PageKey) -> u64 {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    hash_u64(packed)
}

/// Alternate page hash with a different seed. Used by multi-hash PT variants.
#[inline(always)]
pub(crate) fn hash_page_key_2(key: &PageKey) -> u64 {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    hash_u64(packed ^ 0x9e37_79b9_7f4a_7c15u64)
}

#[inline(always)]
pub(crate) fn hash_page_key_3(key: &PageKey) -> u64 {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    hash_u64(packed ^ 0x517c_c1b7_2722_0a95u64)
}

#[inline(always)]
pub(crate) fn hash_page_key_4(key: &PageKey) -> u64 {
    let packed = (key.c_key.as_u32() as u64) << 32 | key.page_id as u64;
    hash_u64(packed ^ 0x6c62_272e_07bb_0142u64)
}
