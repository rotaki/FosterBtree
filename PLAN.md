# Contention-Aware Vertical Partitioning for HTAP

## Thesis

Prior adaptive layout work (H2O, Hyrise, Peloton) optimizes for scan I/O in read-only workloads. For HTAP workloads, the dominant cost is **cross-workload contention** — OLTP writers blocking OLAP readers on shared pages. We formalize a contention-aware cost model, show it produces different partitions than I/O-only models, and implement it inside a Foster B-tree.

## What We Have

- Foster B-tree implementation with TPC-C (primary index stores full rows)
- CH-benCHmark workload analysis: field access patterns for all 5 OLTP txns + 22 OLAP queries
- Cost model with 4 terms: read amplification, write amplification, WW contention, RW contention
- Adaptive partitioner (simulator) that converges to partition groups under different OLTP/OLAP mixes
- Convergence results showing 2-4 groups is typical, and 2 groups (hot/cold) captures ~80% of benefit

## Goal

Show that **static vertical partitioning** (fixed hot/cold split, decided offline from workload analysis) improves throughput on the CH-benCHmark compared to a full-row layout — and that the improvement is driven by contention reduction, not just I/O savings.

This is the baseline experiment before building adaptive migration. If static partitioning doesn't help, adaptive won't either.

## Phase 1: Static 2-Group Partitioning in Foster B-tree

### 1.1 Pick the partition for each table

Use the convergence results (OLTP-dominant, since we're testing TPC-C first) to fix the hot/cold split per table:

| Table | Hot group (separate page) | Cold group (leaf inline) |
|---|---|---|
| CUSTOMER | `c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data` | everything else (keys + stable descriptors) |
| DISTRICT | `d_next_o_id, d_ytd` | everything else |
| STOCK | `s_quantity, s_ytd, s_order_cnt, s_remote_cnt` | everything else (keys + s_dist_01..10 + s_data) |
| WAREHOUSE | `w_ytd` | everything else |
| ORDER_LINE | `ol_delivery_d` | everything else |
| ORDERS | `o_carrier_id` | everything else |
| NEW_ORDER | skip — entire row is hot (insert + delete) |
| HISTORY | skip — insert-only, no updates |
| ITEM | skip — read-only |

Start with **CUSTOMER only** — it has the most fields (21), the clearest hot/cold separation, and is touched by the highest-frequency transactions (Payment 43%, Delivery 4%).

### 1.2 Modify the Foster B-tree leaf page

Current structure:
```
LeafPage: [key₁, full_row₁, key₂, full_row₂, ...]
```

New structure:
```
LeafPage: [key₁, cold_fields₁, key₂, cold_fields₂, ...]
           + hot_page_ptr → HotPage: [key₁, hot_fields₁, key₂, hot_fields₂, ...]
```

Changes needed:
- [ ] Define `ColdRow` and `HotRow` structs for CUSTOMER (compile-time, no runtime overhead)
- [ ] Add `hot_page_id` field to the leaf page header
- [ ] Allocate a hot page when a leaf page is created
- [ ] On leaf split: split the hot page at the same key boundary
- [ ] On leaf merge: merge hot pages too

### 1.3 Modify the access path

- [ ] **Point lookup (read cold fields)**: unchanged — read from leaf page
- [ ] **Point lookup (read hot fields)**: follow `hot_page_ptr`, read from hot page
- [ ] **Point lookup (read both)**: read leaf + follow hot_page_ptr
- [ ] **Update hot fields** (e.g., Payment updating c_balance): follow `hot_page_ptr`, update hot page only. **Leaf page stays clean.**
- [ ] **Update cold fields**: update leaf page (rare — cold fields are rarely updated)
- [ ] **Insert**: write cold fields to leaf, write hot fields to hot page
- [ ] **Delete**: remove from both

### 1.4 Modify TPC-C transaction code

- [ ] **Payment**: read cold fields from leaf (c_first, c_last, c_address, etc.), read+write hot fields from hot page (c_balance, c_ytd_payment, c_payment_cnt, c_data)
- [ ] **Delivery**: read c_id/c_d_id/c_w_id from leaf (cold), read+write c_balance/c_delivery_cnt from hot page
- [ ] **OrderStatus**: read cold + hot (both reads, no writes)
- [ ] **NewOrder**: read c_discount/c_last/c_credit from leaf (cold only — no hot fields needed)
- [ ] **StockLevel**: doesn't touch CUSTOMER

### 1.5 Run TPC-C only (no OLAP yet)

Measure:
- [ ] Throughput (tpmC) at varying thread counts (1, 2, 4, 8, 16, 32)
- [ ] Latency distribution (p50, p99) for Payment and NewOrder
- [ ] Page-level contention: count latch wait events on leaf pages vs hot pages
- [ ] WAL bytes written per transaction (write amplification)

Compare: full-row layout vs 2-group layout.

**Expected result**: at high thread counts, the 2-group layout should show reduced latch contention because Payment and Delivery no longer dirty the same page as NewOrder reads. WAL bytes per txn should drop because Payment only logs ~32B (hot fields) instead of ~800B (full row).

## Phase 2: Add OLAP Queries (CH-benCHmark)

### 2.1 Implement the 22 CH-bench analytical queries

These are read-only SELECT queries adapted from TPC-H to run on the TPC-C schema. They scan CUSTOMER, ORDER_LINE, STOCK, etc.

### 2.2 Run mixed workload

- [ ] N OLTP streams + M OLAP streams on the same database
- [ ] Measure: OLTP throughput (tpmC) AND OLAP throughput (QphH) under contention
- [ ] Compare full-row vs 2-group

**Expected result**: the 2-group layout should show the biggest improvement HERE — OLAP scans reading the cold page are never blocked by OLTP writes to the hot page. The RW contention elimination is the main thesis.

### 2.3 Measure contention directly

- [ ] Instrument latch/lock wait times: how long do OLAP queries wait for OLTP-held latches?
- [ ] Compare full-row (OLAP waits for Payment's row latch) vs 2-group (OLAP reads cold page, Payment writes hot page — no conflict)
- [ ] This is the key measurement that proves the thesis

## Phase 3: Vary the Partition and Measure Sensitivity

### 3.1 Test different splits

- 1 group (baseline: full row store)
- 2 groups (hot/cold — the recommended split)
- 3 groups (hot / cold / blob — separate c_data)
- 4 groups (hot-Payment / hot-Delivery / cold / blob)
- N groups (one per field — approximating column store)

Plot throughput vs number of groups to confirm the 80/20 claim: 2 groups captures most of the benefit, more groups give diminishing returns.

### 3.2 Test different hot/cold boundaries

The static split from 1.1 is based on the cost model. Test alternative splits:
- What if c_data is in the hot group vs cold group?
- What if we split by writer (Payment fields vs Delivery fields) instead of by frequency?
- What if the split is "wrong" (random assignment) — how much does it hurt?

This validates that the cost model's partition is actually good, not just any split.

### 3.3 Vary the OLTP/OLAP mix

- 100% OLTP / 0% OLAP
- 90/10, 70/30, 50/50, 30/70, 10/90
- 0% OLTP / 100% OLAP

Show how the benefit of partitioning changes with mix. Expect: small benefit at 100% OLTP (only write amp savings), large benefit at 50/50 (RW contention dominates), diminishing at 100% OLAP (no writes to contend with).

## Phase 4: Adaptive Migration (If Static Results Are Good)

Only build this if Phase 1-3 show clear benefit from static partitioning.

### 4.1 Version-gated lazy migration

- Add version number to tree header and leaf pages
- Write path: if leaf.version < tree.version, migrate inline then write
- Read path: if leaf.version < tree.version, read using old layout
- No background threads

### 4.2 Online repartition

- Change the hot field set while the system is running under load
- Measure: how quickly do hot pages migrate? What's the throughput dip during migration?
- Compare: version-gated lazy migration vs stop-the-world bulk rewrite

### 4.3 Workload shift experiment

- Run 100% OLTP for 5 minutes (system converges to OLTP-optimal split)
- Shift to 50/50 HTAP (system should re-partition to isolate OLAP scan fields from OLTP write fields)
- Measure: how quickly does throughput recover after the shift?

## Milestones

1. **M1**: CUSTOMER 2-group split compiles and passes TPC-C correctness tests
2. **M2**: TPC-C throughput comparison (full-row vs 2-group) at 1-32 threads
3. **M3**: CH-bench OLAP queries implemented
4. **M4**: Mixed workload comparison with contention measurements
5. **M5**: Sensitivity analysis (number of groups, different splits, different mixes)
6. **M6**: Decision point — is adaptive migration worth building?

## Key Metrics to Report

- **tpmC** (OLTP throughput) and **QphH** (OLAP throughput) — the CH-bench standard
- **Latch wait time** — direct measurement of contention
- **WAL bytes per transaction** — write amplification
- **p99 latency** — tail latency shows contention effects
- **Throughput vs thread count** — scalability curve, contention shows up as a plateau

## What We're NOT Doing (Scope Boundaries)

- Not building a full column store or DSM — we're partitioning within a B-tree
- Not optimizing scan execution strategies (H2O's contribution) — we use the same scan code, just on narrower pages
- Not doing compression (orthogonal)
- Not doing MVCC / snapshot isolation changes — we're testing with the existing concurrency control
- Not claiming the adaptive partitioner is optimal — we're testing whether the partition it suggests is *better than no partition*
