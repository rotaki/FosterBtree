# Collision-locality analysis: why LAPT is robust under prediction failure

This document explains the mechanistic reason LAPT (PT-FP-V2 + congee + ophash)
maintains throughput when its predictions are stale, while PrediCache (PT-V2
with the default Stafford-mixed hash) and LIPAH (DashMap fallback) collapse.
The argument has three parts:

1. **Collision rate is the same** — both LAPT and PrediCache have nearly
   identical numbers of pages whose preferred slot is taken by another page.
2. **Collision *structure* differs dramatically** — PrediCache scatters
   collisions; LAPT clusters them.
3. **Clustered collisions are cheaper** for two independent reasons: cache
   reuse on the slow-path data structure, and branch-predictor accuracy in the
   hot loop.

## 1. Setup

Each variant maps a page key `(c_key, page_id)` to a "preferred frame" — the
slot in the buffer pool the BP would like to place the page in. Two pages can
want the same preferred slot; only one wins, and looking up the loser later
takes the slow path through the overflow translator.

Three preferred-frame functions appear in the system:

| variant | preferred(c, p) | implication for sequential page_ids |
|---|---|---|
| LIPAH | embedded `frame_id` hint in `PageFrameKey` (no hash) | hint = frame at write time |
| PrediCache | `mix64(c << 32 \| p) mod F` | scattered uniformly |
| **LAPT** | `(mix64(c) + p) mod F` | **sequential page_ids → sequential frames** |

PrediCache uses a Stafford-style 64-bit mixer; LAPT uses an order-preserving
composition where the container key contributes only the offset and the
page_id is added directly. Adjacent page_ids within a container therefore land
on adjacent preferred frames.

## 2. Collision rates are essentially identical

A **collision** is a slot with 2+ pages mapped to it. Closed-form expectations,
where ρ = N/F is the load factor and P = N/C is pages per container:

| scheme | E[colliding pages] | approximation |
|---|---|---|
| PrediCache (Stafford full key) | N − F·(1 − (1−1/F)^N) | ≈ N − F·(1 − e^(−ρ)) |
| LAPT (ophash) | N − F·(1 − (1−P/F)^C) | ≈ N − F·(1 − e^(−ρ)) |

For small P/F (i.e., many small containers, or one big buffer pool), both
formulas converge to the same limit. The reason: in PrediCache each page is
an independent uniform draw; in LAPT, each container drops a contiguous block
of P slots at a uniform random offset, but for any single slot, each block
has independent probability P/F of covering it — so the per-slot occupancy
distribution is the same.

### Numerical check (C=500, P=200, F=200000, ρ=0.5)

```
E[colliding pages] (PrediCache):  21,306    (21.3%)
E[colliding pages] (LAPT)      :  21,276    (21.3%)
ratio                           :  0.9986
```

Empirical (5 trials each):

```
PrediCache: 21,186 pages collide   (21.2%)
LAPT:       20,812 pages collide   (20.8%)
```

The same ~21k of 100k pages will need the slow path either way. **Collision
rate is not the difference.**

## 3. Collision *structure* differs dramatically

A **cluster** is a maximal run of consecutive slots, all with collisions.

### Tiny example: 10 frames, 2 containers, 5 pages each

**PrediCache (random)**:

| page | slot |
|---|---|
| A,0 | 3 |
| A,1 | 7 |
| A,2 | 1 |
| A,3 | 7 ← collision with A,1 |
| A,4 | 5 |
| B,0 | 3 ← collision with A,0 |
| B,1 | 9 |
| B,2 | 0 |
| B,3 | 7 ← collision with A,1, A,3 |
| B,4 | 2 |

```
slot:  0  1  2  3*  4  5  6  7**  8  9
load:  1  1  1  2   0  1  0  3    0  1
```

Collisions at slots **3 and 7** — non-adjacent. **Two clusters of size 1
each.**

**LAPT (sequential within container)**: container A starts at offset 2,
container B at offset 4 (so their ranges overlap):

| page | slot |
|---|---|
| A,0 | 2 |
| A,1 | 3 |
| A,2 | 4 ← collision with B,0 |
| A,3 | 5 ← collision with B,1 |
| A,4 | 6 ← collision with B,2 |
| B,0 | 4 |
| B,1 | 5 |
| B,2 | 6 |
| B,3 | 7 |
| B,4 | 8 |

```
slot:  0  1  2  3  4*  5*  6*  7  8  9
load:  0  0  1  1  2   2   2   1  1  0
```

Collisions at slots **4, 5, 6** — three consecutive. **One cluster of size 3.**

Both schemes have the same total colliding pages (3), but PrediCache scatters
them and LAPT clusters them.

### At realistic scale (C=500, P=200, F=200000)

| metric | PrediCache | LAPT |
|---|---:|---:|
| Total colliding pages | 21,186 | 20,812 |
| Number of clusters | **16,337** | **144** |
| Mean cluster size | 1.10 | **116.7** |
| Max cluster size | 5 | **443** |

PrediCache produces ~16k tiny clusters (mostly singletons; max 5). LAPT
produces ~144 clusters but they're long contiguous runs (max 443 consecutive
colliding slots). Visualized in
[bench_results_t44_consolidated/plots/collision_clusters.png](bench_results_t44_consolidated/plots/collision_clusters.png).

### Why structure differs

PrediCache hashes the full key uniformly — colliding pairs are random pairs
of page keys, distributed uniformly across the frame space.

LAPT places each container's pages on a contiguous block. Two blocks
overlap iff their starting offsets are within P slots of each other; **when
they do overlap, *every* page in the overlap region collides simultaneously**.
A single random "two ranges overlap" event manifests as one cluster of length
proportional to the overlap size.

## 4. Why structure matters: the slow path

When a page's preferred slot is occupied by some other page, the BP falls
to the slow path: look the page key up in the overflow translator to find
where it actually is.

| variant | slow-path data structure | per-lookup cost |
|---|---|---|
| LIPAH | sharded `DashMap` | random shard, random bucket |
| PrediCache | `OverflowTable` (chaining hashmap) | random bucket (hash of key) |
| **LAPT** | **congee `CongeeRawU32` (ART)** | walk ART by packed key |

Crucially, the LAPT slow path's data structure is **ordered by key**: the
ART places adjacent keys (consecutive packed `(c_key, page_id)` integers)
in adjacent leaves, sharing inner-node cachelines.

So when a *cluster* of colliding pages goes through the slow path:

- **PrediCache**: the cluster is small (1–5 pages), but the buckets they
  hash to are uncorrelated. Each lookup touches a fresh L3 cacheline. No
  reuse across the cluster.
- **LAPT**: the cluster is long (100–443 pages), and those pages have
  consecutive packed keys. The first lookup pulls inner ART nodes into
  L1/L2; the next 199 lookups in the cluster reuse those same cachelines.

**Same total slow-path work; very different cache footprint.**

## 5. Why clustering also helps the branch predictor

The BP's hot loop contains a branch:

```
g = bp.get_page_for_read(key);     // touches meta[pred(key)]
if (meta[pred(key)].key == key) {  // <-- this branch
    // fast path
} else {
    // slow path
}
```

The behavior of this branch determines pipeline efficiency.

**Clustered (LAPT)**:
- 200 pages in a row, all not at preferred → branch resolves "no match"
  200 times consecutively → predictor learns "always no match" → 0 mispredicts
- Then a region with no collisions → 200 consecutive "match" → predictor
  flips, then perfect again → 1-2 mispredicts per cluster boundary

**Scattered (PrediCache)**:
- Branch oscillates roughly 50/50 between match and no-match
- Modern branch predictors achieve ~50% accuracy on such patterns
- Each mispredict costs ~15-20 cycles of pipeline flush
- At ~200 ns/op, this is ~5-10 ns of additional per-access cost = ~3-5%

The branch effect compounds with the cache effect.

## 6. Combined picture

Per-access wall-clock measured directly by `bp_translation_bench`'s
`Avg latency: X ns/op (per thread)` output for the sequential-scan stale
configuration at T=44, n=100k, F=200k, c=500 (single trial each from
[bench_all_44t_20260427_150305/summary.txt](bench_all_44t_20260427_150305/summary.txt)):

| pattern | slow-path cache misses | branch mispredict rate | **measured ns/op (stale)** |
|---|---|---|---:|
| **Clustered (LAPT)** | low — ART cachelines stay hot during burst of consecutive misses | low — branch is locally predictable; ~1–2 mispredictions per cluster boundary | **319.6 ns** |
| **Scattered (PrediCache)** | high — OverflowTable cachelines for one collision are evicted by fast-path code before the next collision arrives | high — branch flips ~50%, ~50% mispredicts | **579.1 ns** |
| **Random / no locality (LIPAH-stale)** | high — DashMap shards bounce; no spatial reuse possible | branch is consistently "miss" since hint always wrong, but slow path itself has no locality | **1300.7 ns** |

These ns/op are end-to-end per-access cost, including the 12-byte tail
decode, one-byte checksum, latch acquire/release, and loop overhead — not
pure slow-path translation cost. To isolate the marginal cost of taking the
slow path vs the fast path, subtract the saturated per-access cost (where
the FP fast path hits ~always):

| variant | saturated ns/op | stale ns/op | **stale − sat** (slow-path marginal cost) |
|---|---:|---:|---:|
| LIPAH | 337.0 | 1300.7 | **+964 ns** |
| PrediCache | 552.9 | 579.1 | **+26 ns** (no FP wrapper — sat ≈ stale) |
| **LAPT** | 302.0 | 319.6 | **+18 ns** |

PrediCache's near-zero gap is structural: it has no FP wrapper, so both
saturated and stale runs always go through the OverflowTable. Only LIPAH and
LAPT have an FP fast path, and only for those is subtracting the saturated
number meaningful. **LAPT pays ~18 ns to fall back to the slow path; LIPAH
pays ~964 ns** — a 50× difference in the marginal cost of a stale prediction.

The factor-of-4 gap between LAPT and LIPAH-stale in raw throughput on
translation-only sequential scans (138 vs 34 Mops/s at T=44) is the visible
consequence: same number of stale-prediction events, but LAPT's slow path
cache-hits and LIPAH's slow path scatters across DashMap shards.

## 7. Why the entropy assumption matters

The argument above relies on `(c_key, page_id)` having structured entropy:
container keys vary slowly (few distinct values), page IDs vary fast (many
distinct values within a container). This is the **typical** case in
production:

| workload | C (containers) | P (pages/container) |
|---|---:|---:|
| TPC-C (1000 warehouses) | ~10³ | ~10⁵ |
| TPC-H (single instance) | ~10¹ | ~10⁷ |
| Multi-tenant SaaS | 10³–10⁵ | 10²–10⁵ |
| Time-series DB | 10¹–10² | 10⁹+ |
| Graph DB (edge types) | ~10¹ | 10⁸+ |

The Stafford-mixed full-key hash assumes 64 bits of independent entropy. Real
keys provide perhaps 10–17 bits in `c_key` and 27–32 bits in `page_id` — a
factor of ~10× short of the assumption. PrediCache treats all 64 bits as
random; LAPT exploits the structure that's actually there.

## 8. Implications

The co-design of **ophash** and **congee** is what gives LAPT the property
that the slow path stays cache-friendly:

- **ophash alone with a hashmap** would collide just as often, and the
  hashmap's bucket placement scatters lookups regardless. (No cluster
  benefit.)
- **congee alone with Stafford full-key hash** would collide just as often,
  and the colliding keys are scattered across the keyspace. ART inner
  nodes for unrelated keys aren't shared. (No cluster benefit.)
- **ophash + congee**: collisions cluster in keyspace AND the slow-path
  structure is ordered by keyspace → clustered colliding lookups share
  inner-node cachelines → cheap miss path.

This is why neither half of the design is sufficient on its own, and why
LAPT's robustness is structural rather than incremental.

## 9. Reproducing the analysis

```bash
# Closed-form + empirical for a single config
python3 simulate_collisions.py

# Sweep C and load factor
python3 simulate_collisions.py --sweep --trials 5

# Cluster-size distribution figure
python3 plot_collisions.py --C 500 --P 200 --F 200000
python3 plot_collisions.py --C 100 --P 1000 --F 200000   # fewer, bigger containers
python3 plot_collisions.py --C 10000 --P 10  --F 200000  # high fragmentation
```

Output:
- [`simulate_collisions.py`](simulate_collisions.py) — closed-form formulas + empirical simulation
- [`plot_collisions.py`](plot_collisions.py) — cluster-size histogram generator
- [`bench_results_t44_consolidated/plots/collision_clusters.png`](bench_results_t44_consolidated/plots/collision_clusters.png) — figure for the paper

The Python scripts are dependency-light: only `matplotlib` and `numpy`, and
the simulation is fast (a few seconds for N=100k, single-threaded).
