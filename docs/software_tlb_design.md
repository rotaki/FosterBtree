# Software TLB for Buffer Pool Page Translation

## Overview

A per-thread, L1-cache-resident translation cache that maps `PageKey -> frame_id`, checked before the overflow hash table on every page access. Analogous to a hardware TLB sitting between the CPU and the page table.

**File:** `src/bp/predictive_translation_tlb_only.rs`
**Feature flag:** `bp_pt_tlb_only`

## Architecture

```
get_page(key):
  1. TLB lookup (L1, ~3-5 ns)     --> hit: latch frame, return
  2. Overflow table (~50-100 ns)   --> hit: latch frame, populate TLB, return
  3. Page fault (disk)             --> load page, populate TLB, return
```

No preferred frame check. No promotion/demotion. Pages are placed wherever `choose_victim` puts them. The TLB is the only fast-path optimization.

## TLB Structure

### Layout: 4-way set-associative, 1024 sets

```
Total entries: 1024 sets x 4 ways = 4096
Entry size:    4 bytes (u32)
Total size:    16 KB (fits in L1 data cache, typically 32-48 KB)
```

### Entry bit packing

```
[tag: 5 bits | frame_id: 27 bits] = 32 bits
```

- **27-bit frame_id**: supports up to 128M frames = 2 TB buffer pool at 16 KB pages
- **5-bit tag**: hash of PageKey, used to filter non-matching ways without pointer chase. 1/32 false positive rate per way. Tag is non-zero (OR'd with 1) so 0 = empty entry.

### Hash strategy: `hash_u64(c_key) + page_id`

All TLB operations derive from a single 64-bit value:

```rust
let c_hash = hash_u64(c_key as u64);  // Stafford Mix13 on container key
let val = c_hash.wrapping_add(page_id as u64);
```

- **Set index**: `val & TLB_SET_MASK` (low 10 bits)
- **Tag**: `(val >> 54) & 0x1F | 1` (high 5 bits, non-zero)
- **Overflow bucket** (on miss only): `hash_page_key(key)` via standard `fastmod` — must match the overflow table's own bucket computation, so this uses the canonical hash, not `val`.

This design preserves **spatial locality**: adjacent page_ids within the same container produce adjacent `val` values, mapping to adjacent TLB sets. This matters for B-tree scans where sibling leaf pages have sequential page_ids.

#### Hash unification trade-off

We tested computing TLB set, tag, and overflow bucket all from a single `hash_page_key()` call. This eliminated the rehash on miss (~5 ns savings on 8% miss rate = 0.4 ns average) but destroyed spatial locality in set indexing (Stafford Mix13 scrambles adjacent page_ids to distant values). Result: ~2 µs regression on TPC-C from increased TLB conflict misses.

The current design uses two hash computations:
1. `hash_u64(c_key) + page_id` — for TLB set + tag (spatial locality preserved)
2. `hash_page_key()` — for overflow bucket (only on miss, matches overflow table)

The miss-path rehash costs ~5 ns but occurs only on ~8% of accesses. The spatial locality benefit on the hit path more than compensates.

## Hit Path

```
1. Compute set index and tag from PageKey          (~2-4 ns, ALU)
2. Load ways[0..3] from TLB array                  (~1 ns, L1 hit)
3. For each way:
   a. Compare entry_tag(ways[w]) == tag             (~1 ns, bitmask + compare)
      - Mismatch: skip to next way (no pointer chase)
      - Match: continue to step b
   b. Extract frame_id from entry                   (~0 ns, bitmask)
   c. Load metas[frame_id].key() via Box deref      (~3-10 ns, L2/L3)
   d. Compare with page_key                         (~1 ns)
      - Mismatch: skip to next way
      - Match: latch frame, return guard
4. On hit at way > 0: swap with way 0 (LRU promote) (~1 ns)
```

The tag pre-filter is critical: without it, every way requires a metadata pointer chase (Box deref). With 5-bit tags, ~31/32 non-matching ways are filtered in L1 without touching metadata.

### Measured hit path cost

At 10 pages (100% hit rate): **5.9 ns** per access. Compare:
- LIPAH (frame hint in PageFrameKey): 7.0 ns
- PT-FP-1 (hash + preferred frame check): 7.2 ns

The TLB hit path is genuinely faster because it avoids the hash computation that PT-FP-1 requires.

## Miss Path

On TLB miss, computes `hash_page_key()` for the overflow bucket index and calls `overflow.lookup_with_bucket()`:

1. `hash_page_key()` + `fastmod()` — bucket index (~5 ns)
2. Read overflow bucket (version check, inlined slot, chain walk)
3. Returns `Some(frame_id)` or `None`

On overflow hit: latch frame, verify, populate TLB, return.
On overflow miss: page fault from disk, populate TLB, return.

### TLB population on miss

New entries are inserted at way 0, shifting existing ways down (LRU eviction of way 3):

```rust
fn tlb_insert_front(ways: &mut [u32; 4], entry: u32) {
    ways[3] = ways[2];
    ways[2] = ways[1];
    ways[1] = ways[0];
    ways[0] = entry;
}
```

## Thread Safety

- **TLB is per-thread** (`#[thread_local]` static). No synchronization needed.
- **TLB is a hint** (not authoritative). Stale entries are caught by the existing latch + page_key verification that every BP access already does.
- **No coherence protocol**: if another thread evicts or moves a page, this thread's TLB entry becomes stale. The next access detects the stale entry (metadata key mismatch or latch failure) and falls through to the overflow table, which is always up-to-date.

## Design Choices and Rationale

### Why 4-way set-associative (not direct-mapped)?

With TPC-C (10 warehouses, fixed per thread), each thread accesses ~5000 distinct pages. Direct-mapped with 4096 entries gives 74% hit rate. 4-way with 1024 sets (same 4096 entries) gives 92% hit rate. The working set has severe hot-spot conflicts (e.g., B-tree root nodes across multiple containers all mapping to the same direct-mapped slot).

| Config | Entries | Hit Rate | Latency (10w10t TPC-C) |
|---|---:|---:|---:|
| Direct-mapped 4096 | 4,096 | 74.1% | 76.6 us |
| 4-way 1024x4 (no tag) | 4,096 | 92.1% | 78.1 us |
| 4-way 1024x4 (5-bit tag) | 4,096 | 92.2% | 74.2 us |

### Why 5-bit tag pre-filter?

Without tags, checking 4 ways requires 4 metadata pointer chases (~20-40 ns total). With 5-bit tags, only the matching way (usually 1) requires a pointer chase. This saved ~4 us per transaction on TPC-C.

### Why spatial locality in set index?

Using `c_key_hash + page_id` instead of `hash(c_key || page_id)` means adjacent pages in a B-tree scan map to adjacent TLB sets. This helps because:
- B-tree internal nodes at the same level have sequential page_ids
- Scan operations access leaf pages sequentially
- Adjacent TLB sets share cache lines (4 entries x 4 bytes = 16 bytes per set)

### Why 16 KB total size?

L1 data cache is typically 32-48 KB. At 16 KB, the TLB uses ~33-50% of L1, leaving room for stack, metadata pointers, and other hot data. Testing showed that 32 KB (full L1) improved hit rate but degraded overall latency due to L1 pressure on other data.

### Why LRU (not clock)?

With only 4 ways, LRU via positional shift (3 u32 copies) is cheaper than clock (branch-heavy scan + bit manipulation). Measured: LRU 74.2 us vs clock 75.0 us on TPC-C.

### Why `#[thread_local]` (not `thread_local!` macro)?

`thread_local!` with `.with()` closure adds ~3-5 ns per access (TLS pointer lookup + closure overhead). With `#[thread_local]` (nightly feature), TLB access compiles to a direct `fs`-segment-relative load (~0 ns overhead). This saved ~130 ns per access in the micro-benchmark.

**Requires nightly Rust** (`#![feature(thread_local)]` in lib.rs).

## Performance Results

### Micro-benchmark (bare page access, 1 thread, uniform)

| Pages | LIPAH | PT-FP-1 | TLB-only |
|---:|---:|---:|---:|
| 10 | 7.0 ns | 7.2 ns | **5.9 ns** |
| 100 | 7.2 ns | 8.4 ns | **7.0 ns** |
| 500 | 7.6 ns | 9.3 ns | **8.0 ns** |
| 1,000 | 8.0 ns | 9.7 ns | **8.6 ns** |
| 5,000 | 9.7 ns | **13.7 ns** | 12.5 ns |
| 10,000 | **11.2 ns** | 17.6 ns | 24.3 ns |
| 100,000 | **47.2 ns** | 85.5 ns | 125.8 ns |

TLB wins when working set <= ~2000 pages. Loses at larger working sets due to miss rate.

### TPC-C (10 warehouses, 10 threads, fixed warehouse per thread)

| Variant | Commits | Avg Latency | Hit Rate |
|---|---:|---:|---:|
| LIPAH | 671,229 | 68.9 us | ~100% |
| PT-FP-1 | 641,155 | 72.8 us | ~91% |
| TLB-only | 625,076 | 74.2 us | 92.2% |

TLB is 1.4 us behind PT-FP-1 despite higher hit rate (92% vs 91%). The gap comes from the miss path: TLB calls `overflow.lookup()` which recomputes the hash, while PT-FP-1 reuses the precomputed preferred frame index as the bucket index.

## Optimization History

### TLB access mechanism

| Approach | Uniform 100K | Notes |
|---|---:|---|
| `thread_local!` + `RefCell` (16B entries) | 265 ns | `.with()` closure + borrow check on every access |
| `#[thread_local]` + `UnsafeCell` (8B tag+fid) | 137 ns | Eliminated closure overhead |
| `#[thread_local]` bare `u32[128]` (no tag) | 123 ns | No struct, no tag comparison, just metadata verify |
| + overflow bucket prefetch | 128/132 ns | Helped zipfian, hurt uniform (prefetch instruction overhead) |
| + page prefetch | 168 ns | Too many prefetches, counterproductive |

**Lesson**: `thread_local!` macro overhead (~130 ns) dominated early versions. `#[thread_local]` (nightly) is essential.

### Associativity

| Config | Entries | Size | TPC-C Hit Rate | TPC-C Latency |
|---|---:|---:|---:|---:|
| Direct-mapped 128 | 128 | 512 B | 89.5% | 48.7 us |
| Direct-mapped 1024 | 1,024 | 4 KB | — | 50.9 us |
| Direct-mapped 4096 | 4,096 | 16 KB | 74.1% | 76.6 us |
| 2-way 2048x2 | 4,096 | 16 KB | 77.9% | 77.7 us |
| 2-way 4096x2 | 8,192 | 32 KB | 80.1% | 78.7 us |
| 4-way 1024x4 (no tag) | 4,096 | 16 KB | 92.1% | 78.1 us |
| **4-way 1024x4 (5-bit tag)** | **4,096** | **16 KB** | **92.2%** | **74.2 us** |
| 4-way 2048x4 | 8,192 | 32 KB | 94.4% | 78.8 us |
| 4-way 512x4 | 2,048 | 8 KB | 88.5% | 78.5 us |

**Lessons**:

- Conflict misses dominate: 4-way dramatically improves hit rate over direct-mapped at same entry count (74% → 92%).
- Tag pre-filter is critical: without it, 4-way is slower than direct-mapped despite higher hit rate (78.1 vs 76.6 us). With tags, 4-way wins (74.2 us).
- 32 KB TLB hurts: uses entire L1, evicts other hot data. 16 KB is the sweet spot.
- Direct-mapped 128 entries at 512 B had the best latency on small workloads (48.7 us) but scales poorly to TPC-C.

### Eviction policy

| Policy | TPC-C Hit Rate | TPC-C Latency |
|---|---:|---:|
| LRU (swap on hit, shift on insert) | 92.2% | 74.2 us |
| Clock (1-bit, scan on evict) | 92.3% | 75.0 us |

**Lesson**: With only 4 ways, LRU shift (3 u32 copies) is cheaper than clock's branch-heavy scan. Clock adds ~0.8 us per transaction.

### Hash design

| Hash Strategy | TPC-C Latency | Notes |
|---|---:|---|
| Separate hashes (c_key×const + page_id for set, fibonacci for tag) | **75.2 us** | Best: spatial locality preserved |
| Single `hash_page_key()`, high bits for set | 78.1 us | Spatial locality destroyed |
| Single `hash_page_key()`, low bits for set | 76.8 us | Still worse — Mix13 scrambles low bits too |
| `hash_u64(c_key) + page_id` for set+tag, `hash_page_key` for overflow | 77.2 us | Spatial locality restored but two hashes |

**Lesson**: Spatial locality in set indexing matters more than saving a hash computation on the miss path. Adjacent pages within a container should map to adjacent TLB sets for B-tree scan performance. Pure hash functions (Stafford Mix13) destroy this property. The best set index is `hash(c_key) + page_id` where page_id is added directly without hashing.

### Code layout

| Approach | Latency | Notes |
|---|---:|---|
| Miss path inline in `get_page_for_read` | 120 ns | Best — compiler optimizes across hit/miss boundary |
| Miss path `#[cold] #[inline(never)]` | 137 ns | Function call overhead (~17 ns) |
| Miss path `#[cold]` (no inline hint) | 140 ns | Compiler still didn't inline well |
| Delegate to PT's `get_page_for_read_slow` | 134 ns | Extra indirection through PT's slow path |

**Lesson**: Inlining the miss path in the same function body is faster despite bloating i-cache. Extracting to a cold function costs ~15 ns in function call + register save/restore overhead. At ~10% miss rate, this is ~1.5 ns average — small but measurable.

## Potential Improvements

### 1. Eliminate metadata pointer chase on TLB hit

Currently: `metas[frame_id]` requires indexing a Vec of `Box<FrameMeta>` -- two indirections (Vec data pointer + Box deref). If `FrameMeta` were stored contiguously (arena-allocated instead of Box), the hit path would save ~3-5 ns per access. This would benefit all BP variants equally.

### 2. Combine TLB with preferred frame check (PT-TLB hybrid)

Check TLB first (L1), preferred frame on TLB miss (L2/L3), overflow on double miss. The TLB catches intra-transaction re-accesses; the preferred frame catches first accesses deterministically. This was implemented in `predictive_translation_fp_tlb.rs` but not yet tested with the optimized 4-way TLB.

### 3. Larger TLB on servers with bigger L1

Some server CPUs (e.g., AMD EPYC) have 48 KB L1 data cache. A 24 KB TLB (6144 entries) would cover more of the working set without the L1 pressure we saw at 32 KB on 32 KB L1 machines.

### 4. Prefetch overflow bucket during TLB check

Issue `_mm_prefetch` on the overflow bucket address before checking TLB ways. On TLB miss, the bucket data is already in cache. Tested locally: helped with zipfian but hurt with uniform (prefetch instruction overhead > savings when overflow is already in LLC).

### 5. Adaptive TLB sizing

Dynamically adjust the number of active sets based on observed hit rate. If hit rate is >95%, shrink the TLB to free L1 for other data. If hit rate drops below 80%, grow it. This avoids the fixed-size L1 pressure tradeoff.

### 6. Multi-thread scaling

The TLB is per-thread with zero synchronization. At 40 threads, the overflow table (~100+ MB) spills out of LLC, making each TLB hit save ~100 ns instead of ~30 ns (local LLC hit). The TLB benefit should be larger at high thread counts. This needs to be validated on the server.

### 7. Workload-specific tuning

The TLB works best with high temporal locality and small per-thread working sets (<5000 pages). Workloads like YCSB with zipfian distribution, or simple key-value point lookups, should show a larger TLB advantage than TPC-C's full transaction mix.
