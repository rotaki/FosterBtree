# Multi-Container Support for ophash Collision Testing

## Summary

Modified the `pt_fastpath_coverage` benchmark to support multiple containers, enabling collision testing for ophash (order-preserving hash) variants. Previously, ophash with a single container created no collisions (100% sequential mapping), causing the collision benchmark to crash.

## Changes Made

### 1. **src/bin/pt_fastpath_coverage.rs**

Added `--num-containers` CLI argument and multi-container support:

- **New CLI argument** (line 91-92):
  ```rust
  #[arg(long, default_value_t = 1)]
  num_containers: usize,
  ```

- **Multi-container page creation** (lines 162-177):
  - Distribute pages across N containers evenly
  - Track both `ContainerKey` and `PageFrameKey` for each page
  - Required to compute correct PT preferred slots per container

- **Updated slot bucketing** (lines 182-185):
  - Use correct container key when computing `pt_preferred_slot(c_key, page_id, num_frames)`
  - Pages from different containers now correctly map to potentially overlapping slots

### 2. **src/bp/mod.rs**

Fixed `pt_preferred_slot` to use regular modulo for ophash (lines 174-182):

```rust
#[cfg(feature = "pt_op_hash")]
{
    let c_hash = hash::hash_u64(c_key as u64);
    let packed = c_hash.wrapping_add(page_id as u64);
    // Use regular modulo instead of fastmod to preserve order-preserving property.
    (packed % num_frames) as u32
}
```

**Why**: The benchmark's `pt_preferred_slot` was still using `fastmod`, which has the same bug we fixed earlier in the BP implementation - it returns 0 for small values, breaking sequential mapping. This created a mismatch between what the benchmark thinks are collision slots vs. actual BP behavior.

### 3. **bench_ophash_only.sh**

Updated collision test sections to use `--num-containers 10`:

- **Part B2** (Collision coverage): Added `--num-containers 10 --num-pages 800000`
- **Part B3** (Promotion probability sweep): Added `--num-containers 10 --num-pages 800000`

## How It Works

### Single Container (Before)
```
Container 0: pages 0,1,2,3,...,999
Preferred slots: 0,1,2,3,...,999 (sequential, no collisions)
```

### Multiple Containers (After)
```
Container 0: pages 0-79999    → slots hash(0)+0, hash(0)+1, ... % 200000
Container 1: pages 0-79999    → slots hash(1)+0, hash(1)+1, ... % 200000
...
Container 9: pages 0-79999    → slots hash(9)+0, hash(9)+1, ... % 200000

Result: Container ranges overlap → collisions created!
```

## Test Results

Example run with 10 containers:
```
num_frames=5000 num_pages=10000 num_containers=10
Created 10000 pages across 10 containers
Hot set built: 200 pages (100 distinct preferred slots, with collisions)

Total accesses:           28966583
Preferred frame hits:     28816231  (99.5%)
Overflow chain hits:        149968  (0.5%)
Page faults:                     1  (0.0%)
Promotions fired:              383

fast_path_coverage: 0.9948
```

Successfully found 100 collision slots with k=2, whereas single-container would crash with "only found 0 slots with ≥ 2 collisions".

## Why This Matters

**Before**: ophash variants couldn't be tested for collision behavior because single-container ophash creates perfect sequential alignment (no collisions).

**After**: Multi-container ophash creates realistic collision scenarios where container page ranges overlap in the frame space, allowing:
- Part B2: Collision coverage testing (k=1,2,4,8)
- Part B3: Promotion probability sweep with collisions
- Fair comparison between default hash and ophash under collision pressure

## Files Modified

1. `src/bin/pt_fastpath_coverage.rs` - Multi-container support
2. `src/bp/mod.rs` - Fixed `pt_preferred_slot` fastmod bug
3. `bench_ophash_only.sh` - Updated collision tests to use 10 containers
