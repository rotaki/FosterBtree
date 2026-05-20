# LAPT — paper deliverable bundle

This folder contains everything needed to **reproduce, regenerate, and cite**
the results in the LAPT (Locality-Aware Predictive Translation) paper. It is
intentionally self-contained: documents, bench scripts, plotting tools,
canonical CSVs, and the figures used in the paper all live here.

## Contents

```
lapt_paper/
├── README.md                       this file
├── BENCHMARK_RESULTS.md            full workload spec + results
├── COLLISION_LOCALITY_ANALYSIS.md  mechanism analysis (collision structure
│                                   + branch predictor + cache hot-path)
│
├── custom_plt_style.mplstyle       paper style sheet (used by plot scripts)
│
├── run_all_benchmarks.sh           master orchestrator: builds binaries,
│                                   runs all four workloads with N trials,
│                                   emits CSVs + plots
├── bench_payload_sweep.sh          Workload 1 — saturated payload sweep
├── bench_payload_sweep_stale.sh    Workload 1 — stale payload sweep
├── bench_all_44t.sh                Workload 2 — 44T crosstab
├── bench_uniform_lookup.sh         Workload 2 — uniform random (alone)
├── bench_btree_get.sh              Workload 3 — B-tree GET (real workload)
├── bench_btree_scan.sh             Workload 4 — B-tree range scan
│
├── plot_results.py                 figures from CSVs (workloads 1–4)
├── plot_collisions.py              figure from collision simulation
├── simulate_collisions.py          closed-form + empirical collision analysis
│
├── background_notes/               supporting analyses (not paper text):
│   ├── OPHASH_MULTI_CONTAINER_COLLISIONS.md   founding empirical observation
│   ├── PT_HONEST_ASSESSMENT.md                critical review of PT motivating LAPT
│   ├── CONGEE_VS_OVERFLOW_ANALYSIS.md         microbench: ART vs hashmap as data structures
│   ├── TLB_BP_CRITICAL_ANALYSIS.md            related-work positioning vs TLB-BP
│   └── HYBRID_TLB_PT_CHALLENGES.md            why LAPT chose congee over hybrid TLB+PT
│
└── results/
    └── canonical/                  the CSVs + PNGs/PDFs used in the paper
        ├── csv/
        │   ├── seq_payload_sat.csv
        │   ├── seq_payload_stale.csv
        │   ├── crosstab_44t.csv
        │   ├── btree_get.csv
        │   └── btree_range_scan.csv
        └── plots/
            ├── seq_payload.{png,pdf}
            ├── crosstab_44t.{png,pdf}
            ├── collision_clusters.{png,pdf}
            ├── btree_get.png        (kept as supporting evidence)
            └── btree_range_scan.png
```

## Required source-code changes (live in the parent crate)

LAPT is implemented as new modules in `../src/bp/`:

| file | role |
|---|---|
| `../src/bp/predictive_translation_v2_congee.rs` | LAPT inner: PT-V2 with `CongeeRawU32` (ART) overflow translator |
| `../src/bp/predictive_translation_fp_v2_congee.rs` | LAPT outer: FP wrapper around the congee inner |
| `../src/bp/mod.rs` | exports + `get_test_pt_bucket_validate_v2_congee` helper |
| `../src/bin/bp_translation_bench.rs` | `--scramble`, `--no-chain`, `--payload-bytes`, `--num-containers` |
| `../src/bin/fbt_on_disk_scan.rs` | random-start range-scan workload |
| `../src/bench_utils.rs` | `run_bench` timing + `get_with` no-copy GET |
| `../src/access_method/fbt/foster_btree.rs` | `get_with` callback API + cursor `for_each` |
| `../Cargo.toml` | features `bp_pt_v2_congee`, `bp_pt_bucket_v2_congee`, `bp_pt_v2_congee_ophash`, `bp_pt_bucket_v2_congee_ophash` |

## Reproducing the paper figures

### Quick: regenerate plots from canonical CSVs (no rebuild, no bench)

```bash
cd lapt_paper
python3 plot_results.py --indir results/canonical/csv --outdir results/canonical/plots
python3 plot_collisions.py
```

Takes a few seconds. Useful for iterating on figure aesthetics.

### Full: build, bench, and plot from scratch (~25 min)

From any working directory:

```bash
bash lapt_paper/run_all_benchmarks.sh
```

The scripts self-locate (`SCRIPT_DIR`/`PROJECT_ROOT`), `cd` to the project
root for `cargo build`, and write outputs under `lapt_paper/results/run_<timestamp>/`.

Default parameters match `BENCHMARK_RESULTS.md`. Override via env vars:

```bash
T=44 N=100000 F=200000 C=500 TRIALS=5 \
    bash lapt_paper/run_all_benchmarks.sh
```

### Individual workloads

Each script is independent and supports the same env-var overrides:

```bash
bash lapt_paper/bench_payload_sweep.sh                 # Workload 1 saturated
bash lapt_paper/bench_payload_sweep_stale.sh           # Workload 1 stale
bash lapt_paper/bench_all_44t.sh                       # Workload 2 crosstab
bash lapt_paper/bench_uniform_lookup.sh                # Workload 2 uniform alone
bash lapt_paper/bench_btree_get.sh                     # Workload 3
bash lapt_paper/bench_btree_scan.sh                    # Workload 4
```

### Collision analysis

```bash
python3 lapt_paper/simulate_collisions.py                    # default config
python3 lapt_paper/simulate_collisions.py --sweep --trials 5 # range of (C,ρ)
python3 lapt_paper/plot_collisions.py                        # cluster-size figure
python3 lapt_paper/plot_collisions.py --C 2000 --P 50        # try other configs
```

## Variants under test

Three variants compared throughout:

| name | feature flag | one-line description |
|---|---|---|
| **LIPAH** | `bp_clock` | Frame-id hint embedded in `PageFrameKey`, fall back to sharded `DashMap` on miss. |
| **PrediCache\*** | `bp_pt_v2` | Predictive Translation V2 — `OverflowTable` chaining hashmap + Stafford-mixed full-key hash. No FP wrapper. (Asterisk: re-implementation, not original codebase.) |
| **LAPT** | `bp_pt_bucket_v2_congee_ophash` | Locality-Aware Predictive Translation: PT-FP-V2 with `meta(pref).key()` fast path + `CongeeRawU32` (ART) overflow + order-preserving hash `(hash(c) + p) mod F`. |

## Dependencies

- Rust toolchain (the parent crate's `Cargo.toml`)
- Python 3.7+ with `matplotlib` and `pandas`
  ```bash
  pip install matplotlib pandas
  ```
- 48 logical CPUs recommended for the T=44 configuration; reduce `T=` for
  smaller machines.

## Citing

The paper text references this folder as the artifact bundle. Section
references:

| paper section | data source |
|---|---|
| §4.1 Translation cost vs payload | `results/canonical/csv/seq_payload_*.csv` |
| §4.2 Robustness to staleness | `results/canonical/csv/crosstab_44t.csv` |
| §4.3 Collision-locality analysis | `simulate_collisions.py` + `plot_collisions.py` |
| §4.4 Mechanism (perf counters) | inline in `BENCHMARK_RESULTS.md` and `COLLISION_LOCALITY_ANALYSIS.md` |
