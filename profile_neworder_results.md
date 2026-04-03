# TPC-C NewOrder Per-Operation Profile

**Configuration:** 2 warehouses, 4 threads, 10s execution, 3s warmup, 16KB pages, in-memory (no disk I/O)

**Workload:** Full TPC-C mix (45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel). Only NewOrder transactions are instrumented; other transaction types run normally to keep contention realistic.

## Implementation Notes

- **BP Clock** uses direct frame hints from the B-tree layer and a fast path that checks the hinted frame first.
- **OverflowTable** is PT's custom translation layer: fixed bucket array, versioned per-bucket lock, lock-free reads, inlined first slot, and chained overflow nodes for collisions.
- **DashMap** is the sharded concurrent-map baseline.
- **HashMap** is the single-`RwLock` baseline.
- **Predictive Translation** uses deterministic preferred-frame hashing plus the overflow table; this profile reflects the current speculative-read variant, which prefetches and latches the predicted frame before translation resolves.

## Paper Alignment

- **Implemented:** deterministic placement, unified translation, in-place overflow table with lock-free reads, promotion/demotion, per-page fault serialization, conditional overflow cleanup, free-frame hinting via `ConcurrentQueue`.
- **Partially implemented:** speculative predicted-frame touch on the read path. PT now prefetches and speculatively latches the predicted frame before translation resolves, but it still falls back to the translated frame with a regular read latch when speculation misses.
- **Not implemented:** the paper's full optimistic read protocol without a read-latch CAS, and frame-header metadata stored inside the hash-table entry.

## Current Results

| BP Type | NO Commits | NO Aborts | Avg Txn Latency | NO Throughput (txn/s) | Max Latency |
|---|---:|---:|---:|---:|---:|
| **BP Clock** | 303,884 | 39,122 | 45.11 us | 30,388 | 553.31 us |
| **OverflowTable (custom HT)** | 274,815 | 36,653 | 50.49 us | 27,482 | 600.95 us |
| **DashMap** | 272,999 | 36,370 | 51.15 us | 27,300 | 598.88 us |
| **HashMap** | 267,229 | 36,020 | 52.27 us | 26,723 | 1.39 ms |
| **Predictive Translation** | 241,675 | 33,192 | 57.17 us | 24,167 | 2.98 ms |

### Predictive Translation

```
Total Transaction            avg:  57.17 us   min:  21.67 us   max:   2.98 ms   [100.0%]
|
+-- BeginTxn                 avg:     21 ns   min:     10 ns   max:   7.88 us   [  0.0%]
+-- GetWarehouse             avg:    353 ns   min:    120 ns   max:  61.69 us   [  0.6%]
+-- UpdateDistrict           avg:    480 ns   min:    130 ns   max: 109.22 us   [  0.8%]
+-- GetCustomer              avg:   1.93 us   min:    371 ns   max:  30.93 us   [  3.4%]
+-- InsertNewOrder           avg:   1.30 us   min:    470 ns   max: 126.41 us   [  2.3%]
+-- InsertOrder              avg:   1.61 us   min:    481 ns   max:  59.31 us   [  2.8%]
+-- InsertOrderSecondary     avg:   2.79 us   min:    662 ns   max: 270.93 us   [  4.9%]
|
+-- OrderLineLoop (total)    avg:  40.78 us   min:  13.24 us   max: 415.76 us   [ 71.3%]
|   +-- GetItem              avg:   1.35 us   min:    210 ns   max:  43.66 us   [  2.4%]
|   +-- UpdateStock          avg:   1.65 us   min:    181 ns   max:  55.97 us   [  2.9%]
|   +-- InsertOrderLine      avg:   1.00 us   min:    491 ns   max:  65.16 us   [  1.8%]
|
+-- Commit                   avg:   7.64 us   min:   3.13 us   max:   2.89 ms   [ 13.4%]
```

## Comparison

### Average Latency

| Operation | BP Clock | OverflowTable | DashMap | HashMap | Predictive Translation |
|---|---:|---:|---:|---:|---:|
| BeginTxn | 20 ns | 20 ns | 20 ns | 20 ns | 21 ns |
| GetWarehouse | 265 ns | 330 ns | 343 ns | 346 ns | 353 ns |
| UpdateDistrict | 343 ns | 447 ns | 455 ns | 479 ns | 480 ns |
| GetCustomer | 1.52 us | 1.74 us | 1.75 us | 1.76 us | 1.93 us |
| InsertNewOrder | 939 ns | 1.07 us | 1.10 us | 1.10 us | 1.30 us |
| InsertOrder | 1.12 us | 1.25 us | 1.28 us | 1.28 us | 1.61 us |
| InsertOrderSecondary | 2.31 us | 2.51 us | 2.52 us | 2.53 us | 2.79 us |
| OrderLineLoop | 31.98 us | 35.79 us | 36.18 us | 36.79 us | 40.78 us |
| Commit | 6.38 us | 7.08 us | 7.28 us | 7.73 us | 7.64 us |
| **Total** | **45.11 us** | **50.49 us** | **51.15 us** | **52.27 us** | **57.17 us** |

### Tail Latency

| Operation | BP Clock | OverflowTable | DashMap | HashMap | Predictive Translation |
|---|---:|---:|---:|---:|---:|
| GetWarehouse | 53.93 us | 56.95 us | 56.03 us | 55.35 us | 61.69 us |
| UpdateDistrict | 54.27 us | 106.53 us | 107.56 us | 107.26 us | 109.22 us |
| GetCustomer | 35.28 us | 41.33 us | 55.15 us | 32.98 us | 30.93 us |
| InsertNewOrder | 59.42 us | 56.49 us | 83.42 us | 56.48 us | 126.41 us |
| InsertOrder | 58.22 us | 67.98 us | 79.41 us | 55.65 us | 59.31 us |
| InsertOrderSecondary | 319.19 us | 161.18 us | 679.89 us | 375.21 us | 270.93 us |
| OrderLineLoop | 409.88 us | 400.33 us | 463.21 us | 1.36 ms | 415.76 us |
| Commit | 195.84 us | 324.81 us | 226.73 us | 167.22 us | 2.89 ms |
| **Total** | **553.31 us** | **600.95 us** | **598.88 us** | **1.39 ms** | **2.98 ms** |

## Key Observations

1. **BP Clock is still fastest** in this 2w/4t in-memory profile.
2. **PT improved materially** with the speculative-read variant, but it remains behind Clock and the translation baselines.
3. **OrderLineLoop dominates** across all BPs.
4. **PT's remaining gap** is still driven by the read path: lookup, latch acquisition, and page access structure.
5. **The speculation helped average latency** more than tail latency; the tail is still the main risk if this path is kept as-is.

## Fresh PT Profile

Run on `2026-04-02` with `cargo run --release --bin tpcc_profile_neworder --features pt_profile -- -w 2 -t 4 -d 3 -D 10`.

### Transaction Shape

- **Commits:** 202,100
- **Aborts:** 30,959
- **Average transaction latency:** 72.90 us
- **Main transaction bottleneck:** `OrderLineLoop` at 51.26 us average, or 70.3% of total transaction time

### PT Sub-Step Profile

- **Preferred frame hits:** 68,122,898, which is 90.9% of PT accesses
- **Overflow chain hits:** 6,825,406, which is 9.1% of PT accesses
- **Page faults:** 0

The hash hint "does not work" when PT falls through from the preferred-frame check to the overflow-chain lookup. In this run, that happened 6,825,406 times, and the page was still found in the hash table each time. There were no cases where PT had to fault the page in.

- **Biggest PT hot spot:** `hash + preferred` at 72.6 ns per access
- **Second PT hot spot:** `overflow lookup` at 38.7 ns per access
- **Smaller costs:** `ensure_free_frames` at 21.1 ns per access, `latch acquire` at 4.9 ns per access

This makes the current PT bottleneck the translation/read path itself, not latching or page faults.

## Current Gaps vs Paper

- The paper's **optimistic read path** is still not implemented. PT still uses a read-latch CAS on the read hot path.
- The paper's **frame-header-in-table-entry** layout is still not implemented. PT keeps metadata in `metas: Vec<FrameMeta>`.
- The paper's **superscalar overlap** is only partially approximated. PT now speculatively touches the predicted frame, but it does not yet use the paper's full optimistic versioned read protocol.

## One-Hash vs Two-Hash PT Without BP Timing Detail

Run on `2026-04-02` with lightweight PT counters only:

- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt pt_counts" -- -w 2 -t 4 -d 3 -D 10`
- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt2 pt_counts" -- -w 2 -t 4 -d 3 -D 10`

This mode keeps PT access counters but removes the per-access BP timing instrumentation, so the fallback rates are less distorted by `Instant::now()` and timing-related atomics.

### One Hash (`bp_pt`)

- **Commits:** 272,884
- **Aborts:** 35,876
- **Average transaction latency:** 50.56 us
- **Total PT accesses:** 105,079,032
- **Preferred frame hits:** 92,717,315, which is 88.2% of PT accesses
- **Overflow chain hits:** 12,333,747, which is 11.7% of PT accesses
- **Page faults:** 0

### Two Hashes (`bp_pt2`)

- **Commits:** 268,918
- **Aborts:** 35,073
- **Average transaction latency:** 51.58 us
- **Total PT accesses:** 103,431,777
- **Preferred frame hits:** 100,289,163, which is 97.0% of PT accesses
- **Overflow chain hits:** 3,119,370, which is 3.0% of PT accesses
- **Page faults:** 0

### Takeaway

Two-hash PT sharply reduces fall-through to the overflow/hash-resolved path, from **11.7%** down to **3.0%** in this sample. In this particular run, that improvement did **not** translate into better end-to-end NewOrder latency: the one-hash variant was still slightly faster overall.

## Sequential NewOrder Comparison (Corrected Clock Baseline)

Run on `2026-04-02` with `-w 2 -t 4 -d 3 -D 10`.

The runs below were executed sequentially, not in parallel, to avoid cross-run interference.

For `bp_clock`, the corrected baseline was rerun with BP hints enabled because the clock buffer pool relies on them:

- `cargo run --release --bin tpcc_profile_neworder --features "bp_clock" -- -w 2 -t 4 -d 3 -D 10`

For the PT variants, lightweight PT counters were enabled:

- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt pt_counts" -- -w 2 -t 4 -d 3 -D 10`
- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt_bucket pt_counts" -- -w 2 -t 4 -d 3 -D 10`
- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt2 pt_counts" -- -w 2 -t 4 -d 3 -D 10`
- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt2_bucket pt_counts" -- -w 2 -t 4 -d 3 -D 10`

### Summary Table

| Variant | Features | Commits | Aborts | Avg txn | Preferred hits | Overflow hits |
|---|---|---:|---:|---:|---:|---:|
| Clock | `bp_clock` | 257,001 | 34,874 | 53.83 us | N/A | N/A |
| PT one-hash | `bp_pt pt_counts` | 239,535 | 33,060 | 58.12 us | 89.9% | 10.1% |
| PT one-hash + verify-fast-return | `bp_pt_bucket pt_counts` | 241,265 | 32,866 | 57.54 us | 87.7% | 12.3% |
| PT two-hash | `bp_pt2 pt_counts` | 237,189 | 32,710 | 58.54 us | 97.4% | 2.6% |
| PT two-hash + verify-fast-return | `bp_pt2_bucket pt_counts` | 237,942 | 33,123 | 58.43 us | 97.5% | 2.5% |

### PT Page Hit Rate Table

| Variant | Total PT accesses | Preferred frame hits | Preferred hit rate | Overflow chain hits | Overflow rate | Page faults |
|---|---:|---:|---:|---:|---:|---:|
| PT one-hash (`bp_pt`) | 90,513,796 | 81,356,618 | 89.9% | 9,133,179 | 10.1% | 0 |
| PT one-hash + verify-fast-return (`bp_pt_bucket`) | 91,819,151 | 80,484,831 | 87.7% | 11,321,077 | 12.3% | 0 |
| PT two-hash (`bp_pt2`) | 90,876,581 | 88,487,597 | 97.4% | 2,362,722 | 2.6% | 0 |
| PT two-hash + verify-fast-return (`bp_pt2_bucket`) | 90,508,604 | 88,253,160 | 97.5% | 2,240,684 | 2.5% | 0 |

### Takeaways

1. **Clock with BP hints enabled is the fastest configuration in this batch** at 53.83 us average transaction latency.
2. **The best PT variant in this batch is `bp_pt_bucket`**, but it is still slower than clock by about 3.71 us per transaction.
3. **Two hashes sharply improve prediction accuracy**, reducing overflow fall-through from roughly 10 to 12% down to about 2.5 to 2.6%.
4. **That higher hit rate does not automatically translate into better end-to-end latency** in the current implementation.

## BP Macro Profile: Create vs Read Path

Run on `2026-04-02` with the coarse BP macro-profiler enabled:

- `cargo run --release --bin tpcc_profile_neworder --features "bp_clock bp_macro_profile" -- -w 2 -t 4 -d 3 -D 10`
- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt pt_counts bp_macro_profile" -- -w 2 -t 4 -d 3 -D 10`
- `cargo run --release --bin tpcc_profile_neworder --features "bp_pt2 pt_counts bp_macro_profile" -- -w 2 -t 4 -d 3 -D 10`

The macro-profiler records full-method timings for:

- `create_new_page_for_write`
- `get_page_for_read`
- `get_page_for_write`

The counters are reset after warmup, so these numbers only cover the measured 10-second profiling window.

### Summary Table

| Variant | Commits | Avg txn | CreateNewPage avg | Create cnt | GetPageForRead avg | Read cnt | Preferred hits | Overflow hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Clock | 243,763 | 56.63 us | 817 ns | 30,780 | 52 ns | 70,081,292 | N/A | N/A |
| PT one-hash | 223,779 | 63.04 us | 765 ns | 28,278 | 107 ns | 63,206,992 | 89.5% | 10.4% |
| PT two-hash | 224,679 | 62.90 us | 807 ns | 28,351 | 110 ns | 63,332,206 | 97.5% | 2.4% |

### Clock (`bp_clock`)

- **Commits:** 243,763
- **Aborts:** 33,665
- **Average transaction latency:** 56.63 us
- **CreateNewPage:** 817 ns average, 30,780 calls
- **GetPageForRead:** 52 ns average, 70,081,292 calls
- **GetPageForWrite:** not materially exercised in this run

### PT One-Hash (`bp_pt`)

- **Commits:** 223,779
- **Aborts:** 31,610
- **Average transaction latency:** 63.04 us
- **CreateNewPage:** 765 ns average, 28,278 calls
- **GetPageForRead:** 107 ns average, 63,206,992 calls
- **GetPageForWrite:** not materially exercised in this run
- **Preferred frame hits:** 75,279,966 (89.5%)
- **Overflow chain hits:** 8,767,117 (10.4%)

### PT Two-Hash (`bp_pt2`)

- **Commits:** 224,679
- **Aborts:** 32,233
- **Average transaction latency:** 62.90 us
- **CreateNewPage:** 807 ns average, 28,351 calls
- **GetPageForRead:** 110 ns average, 63,332,206 calls
- **GetPageForWrite:** not materially exercised in this run
- **Preferred frame hits:** 82,227,193 (97.5%)
- **Overflow chain hits:** 2,041,765 (2.4%)

### Takeaway

1. **The current PT slowdown is not coming from page creation.** `CreateNewPage` is roughly the same cost as clock in these runs.
2. **The main gap is the read path.** Clock's `get_page_for_read` averaged 52 ns, while PT one-hash and PT two-hash were 107 ns and 110 ns respectively.
3. **Two hashes improved hit rate substantially** but did **not** reduce the coarse read-path cost enough to beat clock.
4. In this workload, the dominant BP entry point is `get_page_for_read`, which is why the read-path overhead matters more than allocation overhead.
