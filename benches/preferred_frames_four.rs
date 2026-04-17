//! Microbenchmark: four-hash vs one-128-bit-hash preferred-frame computation.
//!
//! Compares two strategies that both map a `PageKey` to four candidate frame
//! indices of type `u32`:
//!
//!   1. `four_hash` — four independent 64-bit `hash_page_key_*` calls, each
//!       followed by a 64-bit `fastmod` (u128 widening mul).
//!   2. `single_hash` — one 64-bit mix + one widening u64×u128 mul producing
//!       128 bits; split into four u32 chunks, each fed to a u32 `fastmod`
//!       (u64 widening mul).
//!
//! The hash/fastmod logic here mirrors the `bp::hash` / `bp::predictive_translation`
//! internals so the benchmark does not depend on crate-private APIs.
//!
//! Run with:
//!     cargo bench --bench preferred_frames_four

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ---------------------------------------------------------------------------
// Hash helpers (duplicated from src/bp/hash.rs to keep bench standalone)
// ---------------------------------------------------------------------------

#[inline(always)]
fn hash_u64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

#[inline(always)]
fn hash_packed(packed: u64) -> u64 {
    hash_u64(packed)
}

#[inline(always)]
fn hash_packed_2(packed: u64) -> u64 {
    hash_u64(packed ^ 0x9e37_79b9_7f4a_7c15u64)
}

#[inline(always)]
fn hash_packed_3(packed: u64) -> u64 {
    hash_u64(packed ^ 0x517c_c1b7_2722_0a95u64)
}

#[inline(always)]
fn hash_packed_4(packed: u64) -> u64 {
    hash_u64(packed ^ 0x6c62_272e_07bb_0142u64)
}

/// One u64 mix + one widening u64×u128 multiply.
#[inline(always)]
fn hash_packed_u128(packed: u64) -> u128 {
    let h = hash_u64(packed);
    const MIX: u128 = 0xbf58_476d_1ce4_e5b9_94d0_49bb_1331_11eb_u128;
    (h as u128).wrapping_mul(MIX)
}

// ---------------------------------------------------------------------------
// fastmod helpers
// ---------------------------------------------------------------------------

#[inline(always)]
fn fastmod64(hash: u64, n: u64) -> u32 {
    (((hash as u128).wrapping_mul(n as u128)) >> 64) as u32
}

#[inline(always)]
fn fastmod32(hash: u32, n: u32) -> u32 {
    (((hash as u64).wrapping_mul(n as u64)) >> 32) as u32
}

// ---------------------------------------------------------------------------
// Candidate strategies
// ---------------------------------------------------------------------------

/// Four independent 64-bit hashes + 64-bit fastmod.
#[inline(always)]
fn preferred_frames_four_hash(packed: u64, n_u64: u64) -> [u32; 4] {
    [
        fastmod64(hash_packed(packed), n_u64),
        fastmod64(hash_packed_2(packed), n_u64),
        fastmod64(hash_packed_3(packed), n_u64),
        fastmod64(hash_packed_4(packed), n_u64),
    ]
}

/// One 128-bit hash split into four u32 slices + u32 fastmod.
#[inline(always)]
fn preferred_frames_single_hash(packed: u64, n_u32: u32) -> [u32; 4] {
    let h128 = hash_packed_u128(packed);
    [
        fastmod32(h128 as u32, n_u32),
        fastmod32((h128 >> 32) as u32, n_u32),
        fastmod32((h128 >> 64) as u32, n_u32),
        fastmod32((h128 >> 96) as u32, n_u32),
    ]
}

// ---------------------------------------------------------------------------
// Benchmark driver
// ---------------------------------------------------------------------------

/// Build a batch of packed PageKey-shaped u64 values. Using many different
/// keys stresses the hash pipeline rather than the branch predictor.
fn make_keys(n: usize) -> Vec<u64> {
    // Container id in high 32, page id in low 32 — same layout as hash_page_key.
    (0..n as u64)
        .map(|i| ((i.wrapping_mul(0x9e37_79b9) & 0xffff_ffff) << 32) | (i & 0xffff_ffff))
        .collect()
}

fn bench_preferred_frames(c: &mut Criterion) {
    // Realistic buffer-pool size for these benches: 1 M frames.
    let num_frames: u32 = 1 << 20;
    let n_u64 = num_frames as u64;
    let n_u32 = num_frames;

    const BATCH: usize = 1024;
    let keys = make_keys(BATCH);

    // ---- Single-call latency ----
    let mut latency = c.benchmark_group("preferred_frames_four/latency");
    let seed_key: u64 = 0x0123_4567_89ab_cdef;

    latency.bench_function("four_hash", |b| {
        b.iter(|| black_box(preferred_frames_four_hash(black_box(seed_key), n_u64)));
    });
    latency.bench_function("single_hash", |b| {
        b.iter(|| black_box(preferred_frames_single_hash(black_box(seed_key), n_u32)));
    });
    latency.finish();

    // ---- Throughput over a batch of distinct keys ----
    let mut tput = c.benchmark_group("preferred_frames_four/throughput");
    tput.throughput(Throughput::Elements(BATCH as u64));

    tput.bench_function("four_hash", |b| {
        b.iter(|| {
            let mut acc: u32 = 0;
            for &k in &keys {
                let arr = preferred_frames_four_hash(k, n_u64);
                acc ^= arr[0] ^ arr[1] ^ arr[2] ^ arr[3];
            }
            black_box(acc)
        });
    });

    tput.bench_function("single_hash", |b| {
        b.iter(|| {
            let mut acc: u32 = 0;
            for &k in &keys {
                let arr = preferred_frames_single_hash(k, n_u32);
                acc ^= arr[0] ^ arr[1] ^ arr[2] ^ arr[3];
            }
            black_box(acc)
        });
    });
    tput.finish();
}

criterion_group!(benches, bench_preferred_frames);
criterion_main!(benches);
