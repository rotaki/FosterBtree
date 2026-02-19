# Predictive Translation in LIPAH — Implementation Status

This document is the **master reference** for the PrediCache-style predictive translation buffer pool in the LIPAH repo: what is implemented, what is not, how it fits together, and how it relates to the paper and the C++ prototype.

**Paper:** *Predictive Translation: High-Performance Buffer Management Without the Trade-Offs* (Zinsmeister et al., SIGMOD ’26).

**C++ reference implementation:** `~/databases-research/PrediCache`. See [Overflow table: our design vs paper vs C++](#overflow-table-our-design-vs-paper-vs-c) below and `OVERFLOW_ROADMAP.md` for detailed paper quotes and file references (`versioned_lock.hpp`, `ht.hpp`, `buffer_manager.hpp`).

---

## 1. Implemented (from the paper)

### 1.1 Deterministic frame placement (§3.1)

- **Preferred frame:** `preferred_frame(key) = hash(page_key) % num_frames`.
- **Lookup fast path:** Go to the preferred frame, do an unlatched **tag check** (`FrameMeta::key() == page_key`). If it matches, latch the frame, re-verify under latch, and return. No hash-table lookup.
- **Lookup slow path:** If the tag check misses, use the **overflow table** (`OverflowTable`) mapping `PageKey → frame_index` for pages not in their preferred frame.
- **Overflow is secondary:** Pages in their preferred frame have no overflow entry; only overflow pages appear in the table.

### 1.2 Preferred-frame-aware page fault

- Free frame from the free list (via `choose_victim()`).
- If that frame is the page’s preferred frame → place the page there, **no overflow entry**.
- Otherwise → place in the free frame and **insert** into the overflow table.
- Preferred frame is not proactively claimed; placement is opportunistic to keep the free list simple.

### 1.3 Preferred-frame-aware eviction

- Clock sweep over all frames. On evict, **remove from overflow only if** (a) the page was not in its preferred frame, and (b) this frame is still the one recorded for that page (`overflow.lookup(pk) == this_frame`). Prevents removing the canonical mapping when evicting a duplicate or a frame that was reassigned. Same rule for flush and fault-undo paths.

### 1.4 Overflow table (§4.1, §4.2) — structure and lock-free reads

- **Custom chaining hash table** (`overflow_table.rs`): `num_buckets == num_frames`, one bucket per logical slot. Each bucket has an **inlined first slot** (`Option<(PageKey, usize)>`) plus a chain for collisions. API: `lookup`, `lookup_with_bucket`, `insert`, `remove`, `contains_key`, `get_page_keys`, `for_each_entry`.
- **Lock-free read path:** Per bucket: version counter (even = no writer) and an `ArcSwap<BucketData>` snapshot. Readers: load version (must be even), load snapshot, read inlined slot and chain, re-read version; if unchanged, use result; else retry. No mutex on the read path, so overflow lookup can overlap with preferred-frame load.
- **Single hash on hot path:** `get_page_for_read` / `get_page_for_write` compute `pref = preferred_frame(key)` once and call `overflow.lookup_with_bucket(&page_key, pref)` so we do not re-hash for overflow.

### 1.5 Superscalar interleaving (§3.1, Listing 2)

- The hot path **issues** both the preferred-frame tag check and the overflow lookup at the start of each loop iteration, then resolves: preferred hit → use it; overflow hit → use that frame; else fault. The CPU can overlap both loads.

### 1.6 Concurrency

- **Re-verification under latch (TOCTOU):** After the tag check passes and the latch is acquired, re-verify that the frame still holds the page; if not, drop the guard and fall through to overflow/fault.
- **Re-check after latch failure:** If the latch fails, re-read the tag; if the page is gone, fall through instead of returning a spurious latch error.
- **Per-page fault serialization:** `fault_in_progress` set ensures only one thread runs the fault path per page; callers loop on `RetryPageFault`.
- **Early claim and duplicate-fault handling:** Set `page_key` on the victim before `read_page`; re-check that the page is not already in preferred or overflow before/after using the victim and after I/O; on undo, remove from overflow only when this frame is still the entry for that page.
- **Conditional remove from overflow:** Every remove (eviction, flush, fault undo) removes only if the current frame is still the one stored for that page.

---

## 2. Implemented (LIPAH / standard BP, not from the paper)

| Feature | Notes |
|--------|--------|
| **Clock eviction** | Same `ClockEvictionPolicy` as `BufferPoolClock`. Paper is agnostic to eviction policy. |
| **Free list** | `ConcurrentQueue<usize>` for frame indices; bounded, concurrent. |
| **Eviction threshold** | Eviction when usage exceeds 95%; evict in batches of up to 64. |
| **Per-frame metadata** | `metas: Vec<Box<FrameMeta>>` (latch, dirty bit, eviction state, page key). |

---

## 3. Not implemented (paper-mentioned optimizations)

| Paper feature | Section | Status / notes |
|---------------|---------|----------------|
| **Promotion / demotion** | 3.2, 5.1 | Hot pages are *not* migrated into their preferred frame. Placement is opportunistic; overflow does not shrink over time via promotion. Paper uses probabilistic promotion (e.g. 1/50 without demotion, 1/512 with demotion). |
| **One-hit-wonder detection** | 3.2 | No access-count tracking; promotion is not deferred until second access. All pages are treated the same. |
| **Frame header in hash table entry** | 4.1 | We store only frame index in the overflow table; metadata stays in `metas`. Paper inlines frame header in the hash entry to remove one indirection. Optional improvement. |
| **Benchmarks** | 6 | No TPC-C / YCSB comparison vs `BufferPoolClock` or `VMCachePool` in this repo yet. |

---

## 4. Overflow table: our design vs paper vs C++

### Paper (Section 4)

- **Structure:** Chaining hash table; **first slot inlined** in the translation (bucket) array; **frame header in the hash table entry** (no separate metadata array).
- **Synchronization:** **Optimistic latch** per bucket (version counter). Readers: read version → walk chain → re-read version; no lock. Writers take the latch exclusively and bump the version on release. Lock-free reads allow superscalar overlap with page access.

### PrediCache C++ prototype (`~/databases-research/PrediCache`)

- **`versioned_lock.hpp`:** One `atomic<uint64_t>` per bucket; **MSB = lock bit** (1 = writer), low bits = version. Writers: spin until MSB is 0, then CAS to set MSB; when done, clear MSB and increment version. Readers never take the lock; they read version, read data, re-read version and retry if changed.
- **`ht.hpp`:** Chaining table with **first entry inlined** in each bucket; `VersionedLock` + inlined head + chain. Read path: load version (skip if MSB set), walk head and chain without locking, re-check version. Insert/remove: take versioned lock, mutate chain, unlock.
- **`buffer_manager.hpp`:** Uses this hash table for translation; `fix`-style API calls `ht.access(pid, hash)` then fixes the returned frame.

**Difference from our Rust overflow:** PrediCache does **in-place updates**: writers take the versioned lock, mutate the chain, then unlock. Readers validate the version on the same memory; no copy. Our implementation uses **copy-on-write** (ArcSwap + clone on write): writers clone the bucket, modify, then store a new `Arc`; readers load the current `Arc` (no mutex). Both give lock-free reads. CoW avoids partial-update concerns but pays clone + allocation on insert/remove. An in-place design (versioned lock + atomic chain + safe reclamation, e.g. with crossbeam-epoch) would be closer to the C++ and the paper; we use the CoW design for simplicity unless profiling shows write-heavy overflow as a bottleneck. See `OVERFLOW_ROADMAP.md` for more detail and a possible in-place roadmap.

---

## 5. How lookup works (high level)

```
get_page_for_read / get_page_for_write  (loop until success or hard error)
│
├─ pref = preferred_frame(key)
│  Issue: frame_holds_page(pref, key)?   and   overflow.lookup_with_bucket(key, pref)?
│  Then resolve:
│  • Preferred hit  → latch frame, re-verify → return guard (fast path)
│  • Overflow hit   → latch that frame, re-verify → return guard (slow path)
│  • Both miss      → handle_page_fault (claim page, choose_victim, maybe overflow.insert, read_page, …)
└─ On RetryPageFault, caller continues loop.
```

Fault path: claim page in `fault_in_progress`, pop frame from free list, re-check preferred/overflow for duplicate, set page_key (early claim), insert into overflow if frame ≠ preferred, read page, re-check after I/O; on failure/duplicate, conditional remove from overflow and release victim.

---

## 6. Key data structures

| Name | Role |
|------|------|
| **OverflowTable** | Custom chaining table (`overflow_table.rs`). Maps page → frame index for pages not in their preferred frame. Inlined first slot per bucket; lock-free reads (version + ArcSwap snapshot), CoW writes. `lookup_with_bucket(key, pref)` avoids re-hash. |
| **fault_in_progress** | `Arc<DashMap<PageKey, ()>>`. Set of pages currently being faulted; one fault at a time per page. |
| **free_list** | `ConcurrentQueue<usize>`. Indices of frames known to be free. |
| **metas** | `Vec<Box<FrameMeta>>`. Per-frame metadata: latch, dirty bit, eviction state, page key. |
| **pages** | `Vec<Box<Page>>`. Page data, one per frame. |
| **clock_hand** | `AtomicUsize`. Clock pointer for eviction sweep. |
| **used_frames** | `AtomicUsize`. Count of frames currently in use. |

---

## Summary

LIPAH’s predictive translation implements the **core** of the paper: deterministic placement, tag check on preferred frame, custom overflow table with inlined first slot and lock-free reads, single hash on the hot path, superscalar-style overlap, and correct concurrent fault/eviction/flush behavior. **Not implemented** are the paper’s **policy optimizations** (promotion/demotion, one-hit-wonder), the optional **frame header in the hash entry**, and **benchmarks**. The overflow table uses a **CoW** design (ArcSwap + version) rather than the C++ **in-place** versioned lock; both provide lock-free reads and match the paper’s intent for the read path.
