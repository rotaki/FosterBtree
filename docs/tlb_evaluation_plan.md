# TLB-Victim vs PT: Evaluation Plan

## Goal

Show that TLB-victim (direct-mapped + 16-entry victim cache + congee ART overflow + adaptive range prefill) is better than or competitive with PT variants across all workload dimensions.

## Variants to Compare

| Label | Feature flags | Description |
|---|---|---|
| LIPAH | `bp_clock` | Baseline: DashMap translation, frame hint in PageFrameKey |
| PT | `bp_pt` | Predictive translation with read-path promotion |
| PT-FP-1 | `bp_pt_bucket` | PT + fast-path preferred frame check |
| TLB-congee | `bp_tlb` | 4-way TLB + congee ART overflow |
| TLB-victim | `bp_tlb,tlb_victim_cache` | Direct-mapped + victim cache + congee ART |

## Workloads

### 1. TPC-C — In-Memory (everything fits)

**What it tests:** Steady-state OLTP with mixed reads/writes, B-tree traversals, no eviction.

```bash
# 10 warehouses, 10 threads (small scale)
tpcc_profile_neworder -w 10 -t 10 --fixed-warehouse-per-thread -d 3 -D 15

# 40 warehouses, 40 threads (high contention, LLC pressure)
tpcc_profile_neworder -w 40 -t 40 --fixed-warehouse-per-thread -d 3 -D 15
```

**Expected:** TLB-victim matches or beats PT. At 40t, TLB-victim wins because:
- No frame hint repair (no `no_bp_hint` write-latch upgrades on inner B-tree nodes)
- TLB is per-thread L1-resident — no cross-core cache-line bouncing
- Prior results: TLB-congee 206 µs vs LIPAH 216 µs at 40w40t

### 2. TPC-C — Memory Pressure (eviction active)

**What it tests:** Translation cost when buffer pool < working set. Eviction and re-faulting pages.

```bash
# 1 GB BP for 10 warehouses (~3 GB working set, ~3x oversubscribed)
tpcc_profile_neworder -w 10 -t 10 --fixed-warehouse-per-thread -b 1 -d 3 -D 15
```

**Expected:** PT's read-path promotion helps (72% preferred hits vs 16% without). TLB-victim may be similar or slightly worse since it has no preferred placement. The key metric is fault rate — should be similar for both since clock eviction is identical.

### 3. Micro-benchmark — Uniform Random

**What it tests:** Pure translation cost, no B-tree, no transaction overhead. Random page access.

```bash
# Small working set (fits in TLB)
bp_translation_bench -n 1000 -f 2000
bp_translation_bench -n 5000 -f 10000

# Medium (TLB conflict misses start)
bp_translation_bench -n 10000 -f 20000

# Large (high miss rate)
bp_translation_bench -n 100000 -f 200000
bp_translation_bench -n 500000 -f 600000
```

**Expected:** TLB-victim wins at small/medium working sets (fast hit path). At large working sets, miss rate dominates and congee ART lookup is slightly slower than hash table.

### 4. Micro-benchmark — Sequential Scan

**What it tests:** Spatial locality exploitation. Adjacent pages map to adjacent TLB entries.

```bash
bp_translation_bench --sequential -n 100000 -f 200000
bp_translation_bench --sequential -n 500000 -f 600000
```

**Expected:** TLB-victim dominates. Adjacent page_ids map to adjacent TLB slots (spatial locality from `c_hash + page_id`). Near-100% hit rate. PT has no spatial benefit — each page requires a hash lookup. Prior results: TLB 30 ns vs PT 145 ns (4.5x faster).

### 5. Micro-benchmark — Zipfian (Skewed)

**What it tests:** Hot-set caching. A few pages are very hot, long tail is cold.

```bash
# Moderate skew
bp_translation_bench --theta 0.8 -n 100000 -f 200000

# High skew
bp_translation_bench --theta 0.99 -n 100000 -f 200000
```

**Expected:** TLB-victim wins — hot pages stay in TLB. PT also benefits from preferred frame hits on hot pages, but TLB check is cheaper than hash bucket check.

### 6. Micro-benchmark — Phase Shift (Workload Migration)

**What it tests:** Cost of adapting when the hot set changes. First half accesses pages 0..N/2, second half switches to N/2..N.

```bash
# Without memory pressure
bp_translation_bench --phase-shift -n 100000 -f 200000 -s 10

# With memory pressure
bp_translation_bench --phase-shift -n 100000 -f 80000 -s 10
```

**Expected:** TLB-victim wins. When hot set shifts:
- TLB: old entries naturally overwritten by new accesses. Cost = ~4 bytes per entry update. Instant adaptation.
- PT with promotion: old promoted pages occupy preferred frames. New hot pages trigger promotions = 16 KB page copies + exclusive latches + overflow table updates. High transition cost.

Prior local result (with pressure): TLB-congee 60 µs vs PT 99 µs (40% faster).

### 7. Multi-thread Scaling

**What it tests:** How translation cost scales with core count. TLB is per-thread (zero contention). PT's overflow table is shared.

```bash
for t in 1 2 4 8 16 32 40; do
  bp_translation_bench -n 100000 -f 200000 -t $t -s 5
done
```

**Expected:** TLB-victim scales linearly (per-thread TLB, no shared state on hit path). PT saturates earlier due to overflow hash table contention. LIPAH's DashMap also has sharded lock contention.

## Key Metrics

For each workload, report:
- **Throughput** (ops/s or commits)
- **Avg latency** (µs or ns per operation)
- **Tail latency** (p99, p99.9) — TLB should have lower tail because no shared-state contention
- **TLB hit rate** (for TLB variants) — from stderr output
- **Preferred frame hit rate** (for PT variants, with `pt_counts`) — shows promotion effectiveness

## Summary of Expected Results

| Workload | Winner | Why |
|---|---|---|
| TPC-C in-memory | TLB-victim | No hint repair, per-thread L1 TLB |
| TPC-C pressure | PT (maybe) | Preferred placement reduces overflow lookups |
| Uniform random | TLB-victim | Faster hit path |
| Sequential scan | TLB-victim (big win) | Spatial locality, range prefill |
| Zipfian | TLB-victim | Hot pages cached in L1 TLB |
| Phase shift | TLB-victim (big win) | Instant adaptation vs 16KB copy overhead |
| Multi-thread | TLB-victim | Zero contention on hit path |
