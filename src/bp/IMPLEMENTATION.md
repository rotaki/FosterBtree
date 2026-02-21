# Predictive Translation — Implementation Status

This document is the **master reference** for the PrediCache-style predictive translation buffer pool in this repo: what is implemented, what is not, how it fits together, and how it relates to the paper and the C++ prototype. **If you don’t know PT:** we reimplemented the design from the paper (Zinsmeister et al., SIGMOD ’26) and the C++ PrediCache prototype so this storage engine has a PrediCache-equivalent buffer pool. The non-obvious part is the **overflow table** — a custom hash table (not a stock `HashMap` or DashMap) that matches the C++ code: versioned lock per bucket, inlined first slot, lock-free reads. See [§4 Overflow table](#4-overflow-table-our-design-vs-paper-vs-c) and `OVERFLOW_ROADMAP.md` for details. The repo root `README.md` also has a short “Predictive Translation (PT) and overflow table” subsection for newcomers.

**Paper:** *Predictive Translation: High-Performance Buffer Management Without the Trade-Offs* (Zinsmeister et al., SIGMOD ’26).

**C++ reference implementation:** `~/databases-research/PrediCache`. Key files: `versioned_lock.hpp`, `ht.hpp`, `buffer_manager.hpp`. See [§4](#4-overflow-table-our-design-vs-paper-vs-c) and `OVERFLOW_ROADMAP.md` for alignment.

---

## Buffer pool implementations in this repo

| Implementation | Module / feature | Description |
|----------------|------------------|-------------|
| **LRU** | `buffer_pool.rs` (default) | Classic hash table + LRU eviction. |
| **Clock** | `buffer_pool_clock.rs`, `bp_clock` | Hash table + clock eviction. |
| **VMCache** | `vmcache.rs`, `vmcache` | Virtual-memory–assisted cache (optional). |
| **Predictive Translation (PT)** | `predictive_translation.rs`, `bp_pt` | PrediCache-equivalent: deterministic placement, custom overflow table (versioned lock, inlined first slot, lock-free reads), promotion/demotion, one-hit-wonder. |
| **dashmap** | `dashmap_bp.rs`, `bp_dashmap` | Baseline: DashMap (sharded) + clock. Concurrent translation. |
| **hashmap** | `hashmap_bp.rs`, `bp_hashmap` | Baseline: single `RwLock<HashMap<...>>` + clock. Paper-style “traditional” (one latch on the table). |

Build/run with a specific BP: e.g. `cargo run --release --features bp_pt --bin tpcc -- ...` for PT. See [§7 Baselines and benchmarking](#7-baselines-and-benchmarking) for comparing PT vs dashmap vs hashmap.

---

## 1. Implemented (from the paper)

### 1.1 Deterministic frame placement (§3.1) — unified translation

- **Preferred frame:** `preferred_frame(key) = hash(page_key) % num_frames`.
- **Unified translation (single lookup):** One hash table holds *all* page→frame mappings (paper/C++ style). Lookup is a single `overflow.lookup_with_bucket(key, pref)`: the preferred slot is the bucket head, the chain is overflow. No separate tag check; no “preferred ⇒ not in overflow.”
- **Fault / eviction / promotion:** Always insert into overflow on fault (including when placing in preferred frame). On eviction or flush, remove from overflow when this frame is still the entry for that page. On promotion to preferred, insert the preferred frame into overflow so the table stays canonical.

### 1.2 Preferred-frame-aware page fault

- Free frame from the free list (via `choose_victim()`). Place page there; **always** `overflow.insert(page_key, frame_id)` (unified: preferred and overflow both in the table). Preferred frame is not proactively claimed; placement is opportunistic.

### 1.3 Eviction

- Clock sweep over all frames. On evict, remove from overflow when `overflow.lookup(pk) == this_frame`. Same for flush and fault-undo paths.

### 1.4 Overflow table (§4.1, §4.2) — structure and lock-free reads

- **Custom chaining hash table** (`overflow_table.rs`): `num_buckets == num_frames`, one bucket per logical slot. Each bucket has an **inlined first slot** (`Option<(PageKey, usize)>`) plus a chain for collisions. API: `lookup`, `lookup_with_bucket`, `insert`, `remove`, `contains_key`, `get_page_keys`, `for_each_entry`.
- **In-place updates (versioned lock):** Per bucket: one `AtomicU64` with **MSB = lock bit** (1 = writer), low bits = version (PrediCache-style). Writers: spin until MSB is 0, CAS to set MSB, mutate inlined slot and chain, then clear MSB and bump version. Readers: load version (skip if MSB set), read inlined slot and chain (chain nodes are `crossbeam_epoch::Atomic`), re-read version; if unchanged, use result; else retry. No copy on insert/remove; chain nodes are allocated and retired via crossbeam-epoch.
- **Single hash on hot path:** `get_page_for_read` / `get_page_for_write` compute `pref = preferred_frame(key)` once and call `overflow.lookup_with_bucket(&page_key, pref)` so we do not re-hash for overflow.

### 1.5 Single lookup (paper/C++ style)

- Hot path does one `overflow.lookup_with_bucket(key, pref)` per iteration; bucket head = preferred slot, chain = overflow. Resolve: hit → latch frame and return; miss → fault.

### 1.6 Concurrency

- **Re-verification under latch (TOCTOU):** After the overflow lookup returns a frame and the latch is acquired, re-verify that the frame still holds the page; if not, drop the guard and retry/fall through to fault.
- **Re-check after latch failure:** If the latch fails, re-check overflow; if the page moved or is gone, fall through instead of returning a spurious latch error.
- **Per-page fault serialization:** `fault_in_progress` set ensures only one thread runs the fault path per page; callers loop on `RetryPageFault`.
- **Early claim and duplicate-fault handling:** Set `page_key` on the victim before `read_page`; re-check via overflow that the page is not already loaded; on undo, remove from overflow when this frame is still the entry for that page.
- **Conditional remove from overflow:** Eviction, flush, fault undo: remove only when `overflow.lookup(pk) == this_frame`.

---

## 2. Implemented (standard BP / not from the paper)

| Feature | Notes |
|--------|--------|
| **Clock eviction** | Same `ClockEvictionPolicy` as `BufferPoolClock`. Paper is agnostic to eviction policy. |
| **Free frames** | `DashSet<usize>` of free frame indices; `choose_victim(Some(preferred))` picks preferred frame when free, else any from set (snapshot iteration). |
| **Eviction threshold** | Eviction when usage exceeds 95%; evict in batches of up to 64. |
| **Per-frame metadata** | `metas: Vec<Box<FrameMeta>>` (latch, dirty bit, eviction state, page key). |

---

## 3. Implemented vs not implemented (paper)

**Implemented (paper features):** Deterministic placement (§3.1), unified translation / single lookup (§3.1, §4), promotion and demotion (§3.2, 5.1), one-hit-wonder (§3.2), overflow table with inlined first slot and versioned lock / lock-free reads (§4.1, §4.2), per-page fault serialization, conditional remove from overflow.

**Not implemented (paper-mentioned):**

| Paper feature | Section | Notes |
|---------------|---------|--------|
| **Frame header in hash table entry** | 4.1 | We store only frame index in the overflow table; metadata stays in `metas: Vec<FrameMeta>`. |
| **Benchmarks** | 6 | Scripts exist (`scripts/tpcc_bench.sh`, `tpcc_bench_pt_vs_bh.sh`); systematic PT vs baselines at scale is planned. |

---

## 4. Overflow table: our design vs paper vs C++

### Paper (Section 4)

- **Structure:** Chaining hash table; **first slot inlined** in the translation (bucket) array; **frame header in the hash table entry** (no separate metadata array).
- **Synchronization:** **Optimistic latch** per bucket (version counter). Readers: read version → walk chain → re-read version; no lock. Writers take the latch exclusively and bump the version on release. Lock-free reads allow superscalar overlap with page access.

### PrediCache C++ prototype (`~/databases-research/PrediCache`)

- **`versioned_lock.hpp`:** One `atomic<uint64_t>` per bucket; **MSB = lock bit** (1 = writer), low bits = version. Writers: spin until MSB is 0, then CAS to set MSB; when done, clear MSB and increment version. Readers never take the lock; they read version, read data, re-read version and retry if changed.
- **`ht.hpp`:** Chaining table with **first entry inlined** in each bucket; `VersionedLock` + inlined head + chain. Read path: load version (skip if MSB set), walk head and chain without locking, re-check version. Insert/remove: take versioned lock, mutate chain, unlock.
- **`buffer_manager.hpp`:** Uses this hash table for translation; `fix`-style API calls `ht.access(pid, hash)` then fixes the returned frame.

**Our Rust overflow (current):** We use **in-place updates** aligned with PrediCache: versioned lock (MSB = lock, low bits = version), inlined first slot per bucket, chain with `crossbeam_epoch::Atomic<ChainNode>` and safe retirement. Writers take the lock, mutate the chain, unlock; readers validate version and read without locking. No copy on insert/remove; this avoids the clone+allocation cost of a CoW design and is closer to the C++ and the paper. See `OVERFLOW_ROADMAP.md` for history and alternatives.

**Why the overflow table is non-straightforward:** The C++ PrediCache repo does *not* use a standard hash map. It uses a custom chaining table where (1) each bucket has a **versioned lock** (one `atomic<uint64_t>`: MSB = writer lock, low bits = version) so readers never take a mutex — they read version → read bucket → re-read version and retry if changed; (2) the **first entry of each chain is inlined** in the bucket to avoid a pointer chase on the common case; (3) writers **mutate the chain in place** under the lock. Our Rust version mirrors that: `overflow_table.rs` implements this scheme with `crossbeam_epoch` for safe reclamation of chain nodes. So the PT BP depends on this custom table, not on `std::collections::HashMap` or DashMap.

---

## 5. How lookup works (high level)

Unified translation: one table, one lookup.

```
get_page_for_read / get_page_for_write  (loop until success or hard error)
│
├─ pref = preferred_frame(key)
│  frame_idx = overflow.lookup_with_bucket(key, pref)   // single lookup; bucket head = preferred
│  • Some(idx) → latch frame, re-verify → return guard
│  • None      → handle_page_fault (claim, choose_victim, overflow.insert, read_page, …)
└─ On RetryPageFault, continue loop.
```

Fault: always `overflow.insert(page_key, frame_id)`. Evict/flush: remove from overflow when this frame is still the entry for that page.

---

## 6. Key data structures

| Name | Role |
|------|------|
| **OverflowTable** | Custom chaining table (`overflow_table.rs`). **Unified translation:** maps *all* page → frame (preferred and overflow). Inlined first slot per bucket (= preferred slot); chain = overflow. Versioned lock, in-place writes, lock-free reads. `lookup_with_bucket(key, pref)` = single lookup. |
| **fault_in_progress** | `Arc<DashMap<PageKey, ()>>`. Set of pages currently being faulted; one fault at a time per page. |
| **free_frames** | `DashSet<usize>`. Indices of frames known to be free; victim choice prefers preferred frame when free. |
| **metas** | `Vec<Box<FrameMeta>>`. Per-frame metadata: latch, dirty bit, eviction state, page key. |
| **pages** | `Vec<Box<Page>>`. Page data, one per frame. |
| **clock_hand** | `AtomicUsize`. Clock pointer for eviction sweep. |
| **used_frames** | `AtomicUsize`. Count of frames currently in use. |

---

## 7. Baselines and benchmarking

We provide two hash-table baselines for comparison:

| Baseline | Feature | Translation | Purpose |
|----------|---------|-------------|--------|
| **dashmap** | `bp_dashmap` | **DashMap** (sharded concurrent map) | Concurrent translation; PT may not beat this at low thread count. |
| **hashmap** | `bp_hashmap` | **Single `RwLock<HashMap<PageKey, usize>>`** | Paper-style “traditional” baseline (one latch on the table). |

The paper compares PrediCache against a **traditional** buffer pool with a read-write spin latch per bucket (Shore-MT style). Our **hashmap** module uses one global `RwLock` so every lookup/insert/remove serializes at the table; it is the regime where PT’s lock-free overflow is designed to win. The **dashmap** module (DashMap) is already concurrent, so PT does not get a concurrency advantage over it.

**Pool size and thread count:** The paper’s in-memory evaluation uses **256GB** buffer and **192 threads**; their gains (e.g. 7.4× from optimistic latching over traditional at 192 threads) show at high concurrency. Our default TPC-C config (1 warehouse ≈ 1GB pool, 1–8 threads) is much smaller. To see PT outperform **hashmap**, use more threads (e.g. `-t 8` or higher) and enough warehouses; to see PT competitive with or ahead of **dashmap**, similar scaling helps. Script: `scripts/tpcc_bench_pt_vs_bh.sh` (runs PT, dashmap, and hashmap with warmup and multiple runs).

---

## Summary

This predictive translation implementation uses **unified translation** (single lookup, paper/C++ style): one overflow table holds all page→frame mappings; lookup is `overflow.lookup_with_bucket(key, pref)`. Implemented: deterministic placement, in-place overflow table with lock-free reads, promotion/demotion, one-hit-wonder. **Not implemented:** frame header in hash entry, benchmarks. More concurrency testing (multi-thread PT vs dashmap/hashmap) is planned.
