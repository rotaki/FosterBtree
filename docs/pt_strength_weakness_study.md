# PT Strength / Weakness Study

This document describes the instrumentation, microbenchmarks, and analysis
scripts added to the repository to characterize the strengths and weaknesses
of **Predictive Translation (PT)** and **Predictive Translation with Fast
Path (PT(FP))**, relative to the `bp_tlb_v2` (TLB) and `bp_clock_v2` (LIPAH)
baselines.

Everything listed here was added under plan
[`pt_strength_weakness_study`](../.cursor/plans/pt_strength_weakness_study_ad557d27.plan.md)
(the plan file is the authoritative source for intent; this document describes
the implementation).

---

## 1. Motivation and goals

The PrediCache paper positions PT as fast only when hot pages stay in their
preferred positions and claims two weaknesses:

1. **Scans lose locality** because hashing scrambles adjacent page ids.
2. **Hot-set contention** causes fast-path churn — colliding hot pages cannot
   all sit in their preferred slots.

We wanted to empirically support or refute each claim, and make the resulting
figures reproducible.

| Claim | Implementation response |
|-------|-------------------------|
| Hash destroys scan locality | Added an order-preserving hash variant of PT (Part A) so we can show that the weakness is a hash-layout artifact, not a PT property. |
| Fast path is scarce under collisions | Added **coverage counters** across all three BPs plus two new microbenchmarks that isolate collision width and adaptation lag (Part B). |

Coverage — not throughput — is the headline metric. p = 1/512 promotion
probability makes throughput too noisy to carry the argument; coverage is
direct and stable.

---

## 2. What was implemented

### 2.1 Order-preserving PT hash (Part A)

New cargo features in [`Cargo.toml`](../Cargo.toml):

```toml
pt_op_hash = []                                                    # low-level switch
bp_pt_v2_ophash        = ["bp_pt_v2",        "pt_op_hash"]         # PT V2 (no FP) + op-hash
bp_pt_bucket_v2_ophash = ["bp_pt_bucket_v2", "pt_op_hash"]         # PT(FP) V2    + op-hash
```

Hash selector lives in
[`src/bp/predictive_translation_v2.rs::preferred_frame`](../src/bp/predictive_translation_v2.rs):

```rust
#[cfg(feature = "pt_op_hash")]
{
    let c_hash = super::hash::hash_u64(key.c_key.as_u32() as u64);
    let packed = c_hash.wrapping_add(key.page_id as u64);
    fastmod(packed, self.num_frames_u64)
}
#[cfg(not(feature = "pt_op_hash"))]
{
    fastmod(hash_page_key(key), self.num_frames_u64)
}
```

The op-hash branch mirrors the TLB-BP composition
[`hash(c_key) + page_id`](../src/bp/tlb_bp_v2.rs) so adjacent page ids inside
one container land on adjacent slots modulo `num_frames`.

Two new helpers in [`src/bp/mod.rs`](../src/bp/mod.rs):
`get_test_pt_v2_ophash` and `get_test_pt_bucket_validate_v2_ophash`. Both
feed the same inner types as the non-ophash variants — only the hash differs.

`bp_translation_bench::get_bp()` dispatches the ophash variants *before* the
base variants because `bp_pt_v2_ophash` transitively enables `bp_pt_v2`.

**Caveat (documented for reproducibility)**: the op-hash keeps locality inside
one container; scans that cross containers, or workloads with many small
containers, will re-scramble.

### 2.2 Coverage counters across PT, LIPAH, and TLB (Part B1)

**`MemPool` trait** gained a default-no-op probe
[`fn sample_coverage(&self) -> (u64, u64)`](../src/bp/mem_pool_trait.rs)
returning `(fast_hits, total)`.

**PT / PT(FP)** ([`src/bp/predictive_translation.rs`](../src/bp/predictive_translation.rs),
[`src/bp/predictive_translation_v2.rs`](../src/bp/predictive_translation_v2.rs)):
- Existing counters: `preferred_frame_hits`, `overflow_chain_hits`,
  `page_faults`, `promotions_attempted`, `promotions_fired`, `promote_free`,
  `promote_swap`, `promote_noop`.
- New counter: `residency_evictions_from_preferred` — bumped in `on_evict`
  when `preferred_frame(pk) == idx`. Quantifies fast-path-owner churn.
- Unified coverage line printed at the end of `PTProfileCounters::print()`:
  ```
  fast_path_coverage = preferred_frame_hits /
      (preferred_frame_hits + overflow_chain_hits + page_faults)
  ```

**LIPAH (`BufferPoolClockV2`)** ([`src/bp/buffer_pool_clock_v2.rs`](../src/bp/buffer_pool_clock_v2.rs)):
- New `LipahCoverage { hint_hits, hint_misses, evictions }` (only under
  `pt_profile` / `pt_counts`).
- Emits the same `fast_path_coverage: X.YYYY` line from `print_profile`.

**TLB (`TlbBPV2`)** ([`src/bp/tlb_bp_v2.rs`](../src/bp/tlb_bp_v2.rs)):
- The existing `TLB_HITS` / `TLB_MISSES` / `TLB_FALSE_HITS` counters are
  `#[thread_local]`, so `print_profile` cannot aggregate them from the main
  thread. We added process-global `AtomicU64` shadows (`TLB_HITS_GLOBAL`, …)
  updated alongside each thread-local bump.
- `false_hits` are **not** counted as fast-path hits because they fall
  through to the overflow path.

All three report the *same* `fast_path_coverage: X.YYYY` line, enabling
apples-to-apples comparisons and stable regex extraction in bench scripts.

### 2.3 Deterministic-collision microbenchmark (Part B2)

New binary [`src/bin/pt_fastpath_coverage.rs`](../src/bin/pt_fastpath_coverage.rs).

Workload:

1. Create `--num-pages` pages in a single container.
2. Bucket page ids by PT preferred slot using the public helper
   [`fbtree::bp::pt_preferred_slot`](../src/bp/mod.rs). The helper is kept in
   sync with the `pt_op_hash` feature so collision groups target whatever
   hash is currently compiled in.
3. Build the hot set two ways:
   - **Conflict** (default): pick `--hot-sets` slots with at least
     `k = --collision-width` pages; take the first `k` pages from each slot.
     Hot-set size = `k × hot_sets`.
   - **No-conflict** (`--no-conflict`): pick `k × hot_sets` pages from
     `k × hot_sets` *distinct* slots. Same size, no PT collisions — this is
     the control the plan asked for.
4. N worker threads round-robin the hot set (each starts at a different
   offset into it).
5. Reports `fast_path_coverage`, throughput, and optionally a per-op HDR
   latency histogram (`--latency`).

Verified results at k = 4, 32 hot sets, 4 threads, 3 s:

| Variant | conflict (k=4) | no-conflict control |
|---|---|---|
| PT(FP)-V2   | **0.2492** (= 1/k) | 0.9994 |
| TLB-V2      | **1.0000**         | 1.0000 |
| LIPAH-V2    | **1.0000**         | 1.0000 |

### 2.4 Runtime-tunable promotion probability (Part B3)

In [`src/bp/predictive_translation_v2.rs`](../src/bp/predictive_translation_v2.rs)
the old compile-time constants `PROMOTE_PROB_NO_DEMOTE = 50` and
`PROMOTE_PROB_DEMOTE = 512` were replaced with process-global `AtomicU32`:

```rust
static PROMOTE_PROB_NO_DEMOTE_ATOMIC: AtomicU32 = ...;
static PROMOTE_PROB_DEMOTE_ATOMIC:    AtomicU32 = ...;

// Loaded once by PredictiveTranslationBPV2::new:
fn load_promote_env() {
    // PT_PROMOTE_PROB_NO_DEMOTE / PT_PROMOTE_PROB_DEMOTE env vars override.
}
```

Env overrides are applied at BP construction and the final values are echoed
to stderr for benchmark reproducibility:

```
PT promotion probs: 1/50 (no-demote), 1/512 (demote)
```

Observed sweep at k = 4 (interpretation matrix from the plan):

| p (= 1 / denom) | PT(FP) coverage | notes |
|---|---|---|
| 1     | 0.0003 | pure thrashing — every touch swaps the preferred slot |
| 1/8   | 0.2038 | near the 1/k limit with some thrashing |
| 1/64  | 0.2440 | effectively at the scarcity limit |
| 1/512 | 0.2492 | scarcity limit 1/k; no adaptation happening fast enough to recover losers |

Both weaknesses ("scarcity" and "adaptation lag") are visible — at p = 1 we
pay the scarcity cost *plus* thrashing, while at p = 1/512 we just pay
scarcity.

### 2.5 Phase-shift coverage trace (Part B4)

New binary [`src/bin/pt_phase_shift.rs`](../src/bin/pt_phase_shift.rs). Rather
than extending `bp_translation_bench --phase-shift` in-place, we added a
dedicated binary so the trace format stays clean TSV for plotting.

Workload:

1. Create `--num-pages` pages; partition into `--num-phases` disjoint hot
   sets of size `--phase-hot-size`.
2. Every `--phase-interval` seconds the driver rotates an
   `AtomicUsize` that workers read to pick the current hot set.
3. Each worker keeps a thread-local copy of every phase's hot set so it can
   refresh its own `PageFrameKey` hint after each read. Without this,
   LIPAH's hint staleness would artificially depress its coverage after the
   first phase shift and mask PT's specific weakness.
4. A dedicated sampler thread wakes every `--sample-interval-ms`, calls
   `MemPool::sample_coverage()`, and emits a TSV line:

   ```
   # t_ms  phase  delta_hits  delta_total  coverage_window
   500    0      16 748 873  16 748 873   1.0000
   ...
   ```

Observed at hot-size = 2000 on 5000 frames (so ≈ 40 % of frames are "in
play" per phase):

- TLB-V2 / LIPAH-V2: flat at 1.0 across all phases.
- PT-FP-V2: ~0.82 baseline, dips to ~0.77 at each phase boundary, recovers
  within ~1 s — the adaptation-lag signature.

### 2.6 Bench scripts and plotter (Part B5)

[`bench_pt_strength_weakness.sh`](../bench_pt_strength_weakness.sh) is the
canonical end-to-end script. It builds each of the six variants once and
reuses the cached binaries across sub-experiments:

| Variant | features |
|---|---|
| LIPAH-V2           | `bp_clock_v2,pt_counts` |
| TLB-V2             | `bp_tlb_v2,pt_counts` |
| PT-V2              | `bp_pt_v2,pt_counts` |
| PT-V2-ophash       | `bp_pt_v2_ophash,pt_counts` |
| PT-FP-V2           | `bp_pt_bucket_v2,pt_counts` |
| PT-FP-V2-ophash    | `bp_pt_bucket_v2_ophash,pt_counts` |

Sub-commands: `part_a` (Part A scans), `part_b` (Part B coverage +
phase-shift), `all`.

[`scripts/test_pt_strength_weakness.sh`](../scripts/test_pt_strength_weakness.sh)
is a 3-variant ~2-minute sanity run to validate the full pipeline without
burning the full build matrix.

[`scripts/plot_pt_strength_weakness.py`](../scripts/plot_pt_strength_weakness.py)
parses the bench outputs and emits three PNGs into the same directory:

1. `figure_collision.png` — coverage vs collision width `k`, overlaying the
   analytic `1/k` reference line. **Headline panel 1.**
2. `figure_phase_shift.png` — windowed coverage vs time across phase
   boundaries for each variant. **Headline panel 2.**
3. `figure_scan_throughput.png` — scan / random / hotspot throughput (a
   secondary panel because throughput is too noisy at p = 1/512 to carry
   the argument on its own).

---

## 3. Reproducing the figures

```bash
# Full matrix (several minutes).
./bench_pt_strength_weakness.sh
python3 scripts/plot_pt_strength_weakness.py \
    bench_pt_strength_weakness_<timestamp>/

# Quick sanity (≈ 2 minutes).
./scripts/test_pt_strength_weakness.sh
python3 scripts/plot_pt_strength_weakness.py \
    bench_pt_strength_weakness_quick_<timestamp>/
```

### Promotion-probability sweep example

```bash
cargo build --release --bin pt_fastpath_coverage \
    --features "bp_pt_bucket_v2,pt_counts"
for p in 1 8 64 512; do
    PT_PROMOTE_PROB_NO_DEMOTE=$p PT_PROMOTE_PROB_DEMOTE=$p \
    ./target/release/pt_fastpath_coverage \
        --num-frames 10000 --num-pages 40000 \
        --collision-width 4 --hot-sets 32 \
        --threads 4 --seconds 3 --warmup 1
done
```

### Phase-shift example

```bash
cargo build --release --bin pt_phase_shift \
    --features "bp_pt_bucket_v2,pt_counts"
./target/release/pt_phase_shift \
    --num-pages 20000 --num-frames 5000 \
    --num-phases 4 --phase-hot-size 2000 --phase-interval 5 \
    --sample-interval-ms 500 --threads 12 --warmup 3
```

---

## 4. File map

| File | Role |
|------|------|
| [`Cargo.toml`](../Cargo.toml) | Feature flags `pt_op_hash`, `bp_pt_v2_ophash`, `bp_pt_bucket_v2_ophash`. |
| [`src/bp/predictive_translation_v2.rs`](../src/bp/predictive_translation_v2.rs) | Hash selector, runtime-tunable promotion probabilities, `residency_evictions_from_preferred` bump, `sample_coverage` impl. |
| [`src/bp/predictive_translation.rs`](../src/bp/predictive_translation.rs) | `PTProfileCounters` adds `residency_evictions_from_preferred` + unified `fast_path_coverage` print. |
| [`src/bp/predictive_translation_fp_v2.rs`](../src/bp/predictive_translation_fp_v2.rs) | Forwards `sample_coverage` to inner. |
| [`src/bp/buffer_pool_clock_v2.rs`](../src/bp/buffer_pool_clock_v2.rs) | LIPAH coverage counters, `print_profile`, `sample_coverage`. |
| [`src/bp/tlb_bp_v2.rs`](../src/bp/tlb_bp_v2.rs) | TLB global-atomic shadows, `print_profile`, `sample_coverage`. |
| [`src/bp/mem_pool_trait.rs`](../src/bp/mem_pool_trait.rs) | `sample_coverage` default method. |
| [`src/bp/mod.rs`](../src/bp/mod.rs) | `get_test_pt_v2_ophash`, `get_test_pt_bucket_validate_v2_ophash`, public `pt_preferred_slot` helper. |
| [`src/bin/bp_translation_bench.rs`](../src/bin/bp_translation_bench.rs) | Ophash variants wired into `get_bp()`. |
| [`src/bin/pt_fastpath_coverage.rs`](../src/bin/pt_fastpath_coverage.rs) | Deterministic-collision microbenchmark. |
| [`src/bin/pt_phase_shift.rs`](../src/bin/pt_phase_shift.rs) | Per-window coverage trace across phase shifts. |
| [`bench_pt_strength_weakness.sh`](../bench_pt_strength_weakness.sh) | Canonical end-to-end run. |
| [`scripts/test_pt_strength_weakness.sh`](../scripts/test_pt_strength_weakness.sh) | Fast 3-variant sanity. |
| [`scripts/plot_pt_strength_weakness.py`](../scripts/plot_pt_strength_weakness.py) | Headline + secondary figures. |

---

## 5. Key takeaways

- **Part A (strength)**: PT's sequential-scan disadvantage is a hash-layout
  artifact. Enabling `pt_op_hash` recovers order preservation inside a
  container; PT-V2-ophash should track TLB/LIPAH on scans while the default
  Stafford-hash PT trails. Random-access behaviour is unchanged.
- **Part B (weakness)**:
  - *Scarcity*: PT(FP)'s fast-path coverage degrades to **1/k** under
    deterministic k-way collisions, matching the analytic bound. TLB/LIPAH
    hold at 1.0 on the same workload, because their fast path is not a
    single-winner slot.
  - *Adaptation lag*: at p = 1/512, PT(FP) shows a visible coverage dip at
    phase boundaries that persists for ~1 s. Lowering p (e.g. 1/64 or 1/8)
    speeds up recovery but introduces thrashing at p = 1.
- **Throughput plots should be treated as secondary evidence.** The
  per-window coverage trace is more reliable under p = 1/512.
