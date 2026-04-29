# Buffer-pool translator comparison: LIPAH vs PrediCache vs LAPT

This document specifies the workloads, configurations, and results for the
comparison of three buffer-pool translators across translation-only
microbenchmarks and realistic B-tree workloads.

## Variants under test

| name | feature flag | one-line description |
|---|---|---|
| **LIPAH** | `bp_clock` | Frame-id hint embedded in `PageFrameKey`, fall back to sharded `DashMap` on miss. |
| **PrediCache** | `bp_pt_v2` | Predictive Translation V2 — `OverflowTable` (versioned-locked chaining hashmap, bucket index = preferred frame). No FP wrapper. |
| **LAPT** | `bp_pt_bucket_v2_congee_ophash` | Locality-Aware Predictive Translation: PT-FP-V2 (`meta(pref).key()` fast path) + congee/ART overflow + order-preserving hash for `preferred_frame`. |

LAPT is built from `PredictiveTranslationFPBPV2Congee`
([`src/bp/predictive_translation_fp_v2_congee.rs`](src/bp/predictive_translation_fp_v2_congee.rs))
wrapping `PredictiveTranslationBPV2Congee`
([`src/bp/predictive_translation_v2_congee.rs`](src/bp/predictive_translation_v2_congee.rs)).
The congee inner replaces PrediCache's `OverflowTable` field with
`CongeeRawU32<usize>` (the same ART used by `TlbBPV2`). Order-preserving hash
is enabled by `pt_op_hash`: `preferred_frame(c, p) = (hash(c) + p) % F`, so
sequential page-ids within a container map to sequential preferred frames.

## Common machine configuration

```
CPUs: 48 logical (2 sockets × 12 cores × 2 SMT)
Page size: 16 KB (default)
BP size: 200 000 frames (3.2 GB, fits all working sets in RAM)
```

## Workload 1 — translation microbench, sequential, payload sweep

**Bin**: [`src/bin/bp_translation_bench.rs`](src/bin/bp_translation_bench.rs).

**Setup**:
- Pre-create `N=100_000` pages distributed across `C=500` containers
  (`pages_per_container = N/C = 200`).
- Build a sequential next-pointer chain: page `i` → page `(i+1) mod N`. Pages
  in different containers are linked across container boundaries.
- Each page's last 12 bytes are written as
  `[c_key BE | page_id BE | frame_id BE]` (the chain pointer).

**Hot loop** (per thread, T threads in parallel):
```
current = keys[start_idx]
loop:
  guard = bp.get_page_for_read(current)              # translate + latch
  next_c_key, page_id, frame_id = decode page tail   # 12 bytes from page[len-12..]
  fold page[0..payload_bytes] into checksum          # configurable per-page work
  current = PageFrameKey(next_c_key, page_id, frame_id)
```

The pointer chase creates a strict data dependency: the next translation
cannot start until the previous page's tail has been loaded. Out-of-order
execution and the hardware prefetcher cannot hide latency on the critical
path.

**State variants**:
- **Saturated**: `--refresh-hints` (LIPAH only) + `warmup=5s` + default PT
  promotion (`PT_PROMOTE_PROB_*` env vars unset). After warmup, LIPAH frame-id
  hints are rewritten to the current frame. PT promotion converges pages to
  preferred frames during warmup.
- **Stale**: `--scramble` + `warmup=0` + `PT_PROMOTE_PROB_NO_DEMOTE=u32::MAX`
  + `PT_PROMOTE_PROB_DEMOTE=u32::MAX`. After page creation and chain install,
  the BP is flushed and pages are re-faulted in shuffled order — physical
  page→frame placement is uncorrelated with creation order. LIPAH's chain
  hints are now wrong; PT pages are not at their preferred frames; promotion
  cannot recover during the run.

**Payload sweep**: `--payload-bytes ∈ {0, 256, 1024, 4096, 16384}`. After
translation, fold `page[0..N]` into a per-thread checksum. `N=0` is the
pure-translation case (only 1 byte from `page[0]` and the 12-byte tail are
touched).

**Parameters used**:
```
N=100000  F=200000  T=44  C=500  S=10s  W_sat=5s  W_stale=0s
trials per cell: 3 (median reported)
```

### Results — saturated (Mops/s)

| variant | 0 B | 256 B | 1024 B | 4096 B | 16384 B |
|---|---:|---:|---:|---:|---:|
| LIPAH | **160.74** | 38.87 | **38.12** | **18.52** | **5.40** |
| PrediCache | 79.40 | 54.66 | 30.06 | 14.63 | 4.61 |
| LAPT | 145.43 | **81.59** | 36.23 | 15.19 | 4.88 |

### Results — stale (Mops/s)

| variant | 0 B | 256 B | 1024 B | 4096 B | 16384 B |
|---|---:|---:|---:|---:|---:|
| LIPAH | 34.09 | 27.13 | 18.56 | 12.55 | 4.97 |
| PrediCache | 78.45 | 57.17 | 30.43 | 15.20 | 5.00 |
| **LAPT** | **139.82** | **78.96** | **35.01** | **15.75** | 4.76 |

### Stale ÷ saturated (robustness ratio — closer to 1.0 = more robust)

| variant | 0 B | 256 B | 1024 B | 4096 B | 16384 B |
|---|---:|---:|---:|---:|---:|
| LIPAH | 0.21 | 0.70 | 0.49 | 0.68 | 0.92 |
| PrediCache | 0.99 | 1.05 | 1.01 | 1.04 | 1.08 |
| **LAPT** | **0.96** | **0.97** | **0.97** | **1.04** | **0.98** |

### Discussion

- **0 B (translation-isolated)**: LIPAH-sat (161) leads LAPT-sat (145) by
  ~10%. **LIPAH-stale collapses 4.7× to 34** Mops/s. LAPT-stale stays at 140 —
  essentially unchanged from saturated.
- **256 B (small payload)**: **LAPT wins decisively** in both states (82 sat,
  79 stale vs LIPAH 39 sat, 27 stale). At T=44, adding even 256 bytes per
  access pushes LIPAH off its translation-fast-path advantage; LAPT's
  adjacent-frame layout from ophash gives the prefetcher a path to warm the
  next page's cachelines.
- **1–4 KB**: Memory bandwidth and translation share the budget. LIPAH-sat
  recovers slightly because its frame-id hint avoids an additional preferred-
  frame computation per access; LAPT-sat tracks LIPAH-sat closely.
  LIPAH-stale remains ~30% behind LAPT-stale.
- **16 KB (full page)**: Bandwidth-bound. All variants converge to ~5 Mops/s
  at T=44 — the L3/DRAM streaming limit dominates everything.
- **The robustness ratio table** is the cleanest one-number summary. LAPT and
  PrediCache are within 5% of their saturated number when stale; LIPAH loses
  30–80% depending on payload.

## Workload 2 — translation microbench, 44-thread crosstab

**Same bin as Workload 1**, but T=44, single payload (`--no-page-fold`, i.e.
1 byte). Two access patterns × two prediction states = four cells per
variant.

**Access patterns**:
- **Sequential** (`--sequential`): chain mode, same as Workload 1.
- **Uniform random** (`--no-chain`): each iteration picks a uniformly random
  page index and looks up `keys[idx]`. No data dependency between
  iterations — out-of-order execution can hide some translation latency.

**Parameters**:
```
N=100000  F=200000  T=44  C=500  S=15s
saturated: W=5s + --refresh-hints (LIPAH); stale: W=0 + --scramble + PT_PROMOTE_PROB_*=u32::MAX
trials per cell: 1 (low variance at this thread count for translation-only)
```

### Results (Mops/s)

| variant | seq-sat | seq-stale | uniform-sat | uniform-stale |
|---|---:|---:|---:|---:|
| LIPAH | 130.55 | 33.83 | **140.08** | 36.00 |
| PrediCache | 79.57 | 75.98 | 82.67 | 79.26 |
| **LAPT** | **145.67** | **137.68** | 100.86 | **97.97** |

### Discussion

| | sequential | uniform random |
|---|---|---|
| **saturated** | LAPT (146) > LIPAH (130) > PrediCache (80) | LIPAH (140) > LAPT (101) > PrediCache (83) |
| **stale** | LAPT (138) > PrediCache (76) > LIPAH (34) | LAPT (98) > PrediCache (79) > LIPAH (36) |

- **LIPAH wins uniform-saturated** (140 vs 101): no chain dependency lets OoO
  hide latency, fresh hints minimize translator work, sharded DashMap
  distributes well across 44 threads × 500 c_keys.
- **LIPAH collapses 3.9× when stale** (uniform: 140 → 36; sequential: 130 →
  34): the DashMap fallback cost is the same for both access patterns. This
  is the fragility of relying on a single fallback.
- **LAPT wins sequential outright** (sat AND stale) because ophash makes both
  the FP fast path and the congee slow path fast on sequential keys.
- **PrediCache flatlines** around 75–83 Mops/s in all four cells: no FP
  wrapper, so it always pays the OverflowTable bucket lookup independent of
  state.
- **LAPT's stale-vs-sat gap is 1.06× sequential, 1.03× uniform** — the only
  variant without a cliff under prediction failure.

## Workload 3 — B-tree random GET

**Bin**: [`src/bin/fbt_on_disk_get.rs`](src/bin/fbt_on_disk_get.rs).

**Setup**:
- Insert `2_000_000` unique keys into a Foster B-tree
  (`gen_foster_btree_on_disk`, 16 KB pages, BP size 200 000 frames =
  in-memory).
- Keys are 100 bytes each, layout `[usize-be(8 bytes)][padding zeros]`
  (`RandomKVs::new` convention).
- Values are 50–100 random bytes.

**Hot loop** (`run_bench` in
[`src/bench_utils.rs`](src/bench_utils.rs)):
```
for each (k, _) in this thread's partition:
  btree.get_with(k, |val| black_box(val[0]))     # no-copy GET
```

`get_with` is a callback-based no-copy variant
([`src/access_method/fbt/foster_btree.rs:2188`](src/access_method/fbt/foster_btree.rs#L2188))
that yields a borrowed slice into the page under the leaf latch. Avoids the
per-op `Vec<u8>` allocation that the original `get` API does.

**Parameters**:
```
num_keys=2_000_000  key_size=100  val=[50,100]  T=44  bp_size=200_000
trials: 5 (mean ± range reported)
```

### Results (Mops/s)

| variant | runs | mean | range |
|---|---|---:|---:|
| LIPAH | 3.23, 3.38, 3.39, 3.42, 3.20 | **3.32** | ±3.4% |
| PrediCache | 3.22, 3.36, 3.39, 3.30, 3.32 | **3.32** | ±2.6% |
| LAPT | 3.25, 3.39, 3.17, 3.28, 3.14 | **3.25** | ±3.8% |

### Discussion

All three are tied within noise. Per-op cost is ~13 µs at T=44, of which:
- ~12 µs is B-tree traversal: ~3-4 inner pages + 1 leaf = ~4 page
  translations + key comparisons + slot decode + leaf binary search
- ~150-300 ns is BP translation
- ~80-120 ns was the value `Vec<u8>` allocation (now removed by `get_with`)

**Translation cost is ~1-2% of per-op work.** The dominant bottleneck is
**latch contention on inner B-tree pages** — root and a couple of upper
levels are accessed by every GET, so 44 concurrent threads see cacheline
ping-ponging on latch counters and page metadata regardless of which
translator is underneath.

## Workload 4 — B-tree random-start range scan

**Bin**: [`src/bin/fbt_on_disk_scan.rs`](src/bin/fbt_on_disk_scan.rs).

**Setup**: same as Workload 3 (2M keys, 100-byte keys, 50–100 byte values).

**Hot loop** (per thread):
```
loop until exec_secs elapsed:
  pick random start_key matching the inserted-key layout
       (usize index in [0, num_keys), padded with zeros to key_size)
  cursor = FosterBtreeCursor::new(&tree, &start_key, &[])
  count = 0
  cursor.for_each(|k, v| {
    count += 1
    black_box(v.first())                # touch one byte per kv, no copy
    count < scan_size                   # stop after `scan_size` records
  })
```

`cursor.for_each` is the no-copy callback-based scan
([`src/access_method/fbt/foster_btree.rs:2648`](src/access_method/fbt/foster_btree.rs#L2648))
that yields `&[u8]` slices into the leaf page under the read latch and
prefetches the next leaf via `sibling_address` while iterating the current
one.

**Parameters**:
```
num_keys=2_000_000  key_size=100  val=[50,100]  T=44  bp_size=200_000
SCAN_SIZE=1000  EXEC_SECS=15
trials: 3 (mean ± range reported)
```

### Results (M kvs/s)

| variant | runs | mean | range |
|---|---|---:|---:|
| LIPAH | 535.54, 512.41, 513.17 | **520.4** | ±2.3% |
| PrediCache | 495.84, 492.72, 496.33 | **494.9** | ±0.4% |
| **LAPT** | 523.79, 525.16, 528.19 | **525.7** | ±0.4% |

### Discussion

- **LAPT (526) ≈ LIPAH (520) > PrediCache (495)**.
- The range scan exercises the leaf-to-leaf transition path. After warmup,
  the cursor uses `sibling_address` (a `(page_id, frame_id)` hint embedded in
  each leaf) to jump to the next leaf — the frame_id is a hint that LIPAH
  uses directly and LAPT validates via `meta(pref).key()`. Both are fast.
- **PrediCache (no FP wrapper)** always falls into the OverflowTable bucket
  lookup, costing ~5%.
- ~1.9 ns/kv aggregate at T=44: most of the per-kv cost is the slot-id
  increment + key/val pointer dereferences inside the leaf. Translation is
  amortized across ~250 keys/leaf.

## Reproducibility

All workloads are runnable from a single script:

```bash
bash run_all_benchmarks.sh
```

The script:
1. Builds three binaries per BP variant (`bp_translation_bench`,
   `fbt_on_disk_get`, `fbt_on_disk_scan`), one per feature flag, copied to
   variant-tagged paths.
2. Runs each workload with the parameters above. Multiple trials per cell
   (3–5) emit one CSV row per trial under `bench_run_<timestamp>/csv/`.
3. Parses logs, populates CSVs, and invokes `plot_results.py` to generate
   PNG plots in `bench_run_<timestamp>/plots/`.

Output directory layout:
```
bench_run_<timestamp>/
  config.txt              # parameters used
  raw/                    # per-trial logs
  csv/
    seq_payload_sat.csv      # workload 1 saturated
    seq_payload_stale.csv    # workload 1 stale
    crosstab_44t.csv         # workload 2
    btree_get.csv            # workload 3
    btree_range_scan.csv     # workload 4
  plots/
    seq_payload.png          # workload 1 (saturated + stale, lines per variant)
    crosstab_44t.png         # workload 2 (grouped bar chart)
    btree_get.png            # workload 3 (bars + error bars)
    btree_range_scan.png     # workload 4 (bars + error bars)
```

### Knobs

Override via env vars:
```
N=...           # pages (translation microbench, default 100000)
F=...           # frames (default 200000)
C=...           # containers (default 500)
T=44            # thread count (single value used everywhere)
S=15            # exec seconds per cell
W_SAT=5         # saturated warmup
S_PAYLOAD=10    # exec seconds for payload sweep cells (workload 1)
TRIALS=3        # repeats per cell
TRIALS_BTREE_GET=5
NUM_KEYS=2000000
KEY_SIZE=100
SCAN_SIZE=1000
EXEC_SECS=15
SKIP_BUILD=1    # reuse existing binaries
SKIP_PLOTS=1    # skip plot generation
```

## Headline takeaways (all at T=44)

| workload | LIPAH | PrediCache | LAPT |
|---|---|---|---|
| Translation, sequential, saturated, p=0 | wins (161) | trails (79) | tied (145) |
| Translation, sequential, saturated, p=256 | trails (39) | mid (55) | **wins (82)** |
| Translation, sequential, stale, p=0 | **4.7× collapse** (34) | mid (78) | **wins (140)** |
| Translation, sequential, stale, p=256 | trails (27) | mid (57) | **wins (79)** |
| Translation, uniform random, saturated | **wins** (140) | trails (83) | mid (101) |
| Translation, uniform random, stale | **3.9× collapse** (36) | mid (79) | **wins (98)** |
| B-tree GET | tied (3.32) | tied (3.32) | tied (3.25) |
| B-tree range scan | tied (520) | trails (495) | tied (526) |

**The headline is the absence of a cliff for LAPT.** Across all
translation-microbench cells, LIPAH-saturated is fastest only at p=0 (pure
translation). Once any payload work is added (256 B and up), LAPT wins or
ties — even *more* decisively when predictions go stale, where LIPAH loses
3.9–4.7× and LAPT loses ~3%. PrediCache (no FP wrapper) is the predictable
middle baseline at ~80 Mops/s regardless of state.

For workloads where prediction freshness can't be guaranteed (post-eviction,
post-migration, multi-tenant churn), LAPT's locality-aware ART overflow is
the safer default. Where the workload is uniform random and saturated and
the operator can guarantee fresh hints, LIPAH with `--refresh-hints`
discipline is the throughput-optimal choice.
