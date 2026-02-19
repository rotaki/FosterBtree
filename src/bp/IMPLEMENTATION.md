# Predictive Translation — Implementation Status

What is actually built in `predictive_translation.rs` right now, what isn't,
and how the pieces fit together.

---

## Implemented (from the paper)

### Deterministic frame placement (Section 3.1)

Every page has a **preferred frame** computed as `hash(page_key) % num_frames`.

- **Lookup fast path:** hash the page key, go to the preferred frame, do an
  unlatched **tag check** (`FrameMeta::key() == page_key`). If it matches, latch
  the frame and return. No hash-table lookup needed.
- **Lookup slow path:** if the tag check misses, fall back to an **overflow
  table** (`OverflowTable`) that maps `PageKey -> frame_index` for pages not
  in their preferred frame. The overflow table is a custom chaining hash table
  with inlined first slot and lock-free reads (see `overflow_table.rs`).
- **Overflow table is secondary, not primary.** In a conventional buffer pool
  *every* resident page lives in a translation hash table. Here, pages sitting
  in their preferred frame have no hash-table entry — only overflow pages appear
  in the table.

### Preferred-frame-aware page fault

When a page fault loads a page from disk:

1. A free frame is obtained from the free list (via `choose_victim()`).
2. If the free frame *happens to be* the page's preferred frame, the page is
   placed there and **no overflow entry is created** (the tag check is enough).
3. Otherwise the page goes into whatever free frame was available, and an entry
   is added to the overflow table.

The preferred frame is **not** proactively claimed. The free list is always the
source of frames. Whether a page ends up in its preferred frame is
opportunistic. This avoids desynchronising the free list.

### Preferred-frame-aware eviction

Clock-sweep eviction is unchanged. When evicting a frame we remove its page
from the overflow table only if (a) the page was *not* in its preferred frame,
and (b) this frame is still the one recorded for that page in the overflow table
(`overflow.lookup(pk) == this_frame`). That way we never remove the canonical
mapping when evicting a duplicate or when the same page landed in another frame
after our eviction decision. Pages evicted from their preferred frame have no
overflow entry to clean up. (Same rule applies in flush/undo paths.)

### Concurrency

The design keeps the fast path (tag check on preferred frame) mostly lock-free
while guaranteeing that each resident page has exactly one canonical location
(preferred frame tag or overflow table). The following are implemented so that
behaviour holds under concurrency.

1. **Re-verification under latch (TOCTOU guard).** After an unlatched tag check
   passes and the latch is acquired, we re-verify that the frame still holds the
   expected page. If the page was evicted and replaced between the tag check and
   the latch, we drop the guard and fall through to the overflow/fault path.

2. **Re-check after latch failure.** If the tag check passes but the latch
   fails (another thread holds it), we re-read the tag. If the page is still
   there, it's genuinely held by another thread — return a latch-failed error so
   the caller retries. If the page disappeared (evicted between the two checks),
   fall through to overflow/fault instead of returning a spurious latch error.

3. **Per-page fault serialization.** Only one thread may run the fault path for a
   given page at a time. Before faulting we claim the page in a `fault_in_progress`
   set (insert; if already present, return `RetryPageFault`). A guard removes the
   claim on drop so any return path (success or failure) releases it. Callers
   (`get_page_for_write` / `get_page_for_read`) loop on `RetryPageFault` until
   the page is present or another error occurs.

4. **Early claim and duplicate-fault detection in the fault path.** After
   obtaining a victim frame we set `page_key` on it before `read_page` (early
   claim). We also avoid creating duplicate mappings:
   - Before using the victim we re-check that the page is not already in the
     preferred frame or the overflow table; if it is, we release the victim and
     return `RetryPageFault`.
   - If we put the page in the overflow table (non-preferred frame), we re-check
     that we are still the entry after insert and before/after disk I/O; if
     another thread won the mapping or loaded the page into the preferred frame,
     we release our frame (and remove our overflow entry only when we are still
     the entry) and return `RetryPageFault`.
   On undo (e.g. `read_page` failure) we remove from the overflow table only when
   this frame is still the entry for that page.

5. **Conditional remove from overflow.** Whenever we remove a page from the
   overflow table (eviction, flush, or fault undo/duplicate release), we remove
   only if the current frame is still the one stored for that page. So we never
   remove the canonical mapping when evicting a frame that no longer holds the
   canonical copy (e.g. duplicate fault that lost the race).

---

### Overflow table (Section 4.1, 4.2) — custom table, lock-free reads

- **Custom chaining hash table** (`overflow_table.rs`): Fixed capacity `num_buckets == num_frames`, one bucket per frame. Each bucket has an inlined first slot (`Option<(PageKey, usize)>`) plus a chain (`Vec`) for collisions. API: `lookup`, `lookup_with_bucket`, `insert`, `remove`, `contains_key`, `get_page_keys`, `for_each_entry`.
- **Lock-free read path:** Readers do not take a mutex. Per bucket: version counter (even = no writer), `ArcSwap<BucketData>` (copy-on-write). Read path: load version (must be even), load snapshot `Arc`, read inlined slot and chain, re-read version; if unchanged, use result; else retry. Fallback `lookup_slow` uses load only. So overflow lookup can be overlapped with preferred-frame load (superscalar).
- **Single hash on hot path:** `get_page_for_read` / `get_page_for_write` compute `pref = preferred_frame(key)` once and call `overflow.lookup_with_bucket(&page_key, pref)` so we do not re-hash for overflow (same index when `num_buckets == num_frames`).

---

## Not implemented (from the paper)

| Paper feature | Section | Why it matters |
|---|---|---|
| **Promotion / demotion** | 3.2, 5.1 | Hot pages are NOT migrated into their preferred frame. Whether a page lands in its preferred frame is purely opportunistic (depends on free-list order). The overflow map does not shrink over time via promotion. |
| **One-hit-wonder detection** | 3.2 | All pages are treated equally. No access-count tracking, no deferred promotion for first-access pages. |
| **Frame header in hash table entry** | 4.1 | We store only frame index in the overflow table; metadata lives in `metas: Vec<Box<FrameMeta>>`. Paper inlines frame header in the hash entry to remove one indirection. Optional improvement. |
| **Superscalar interleaving** | 3.1, Listing 2 | **One step left:** We still do preferred check *then* overflow lookup sequentially. To finish: restructure the hot path so we *issue* both the preferred-frame load and `lookup_with_bucket` (without branching on the first result), then resolve from the two results. Overflow is already lock-free and we hash once; only the control-flow change remains. |
| **Benchmarks** | 6 | No TPC-C / YCSB comparison against `BufferPoolClock` or `VMCachePool` yet. |

---

## What’s left to finish PrediCache in LIPAH (walkthrough)

Paper reference: *Predictive Translation: High-Performance Buffer Management Without the Trade-Offs* (Zinsmeister et al., SIGMOD ’26). C++ reference: `~/databases-research/PrediCache` (see `OVERFLOW_ROADMAP.md` for `versioned_lock.hpp`, `ht.hpp`, `buffer_manager.hpp`).

### Done (paper-aligned)

| Paper | Our status |
|-------|------------|
| **§3.1 Deterministic placement** | Preferred frame = `hash(page_key) % num_frames`; tag check on preferred; overflow for the rest. |
| **§4.1 Chaining, inlined first slot** | Custom overflow table: one inlined slot per bucket + chain, `num_buckets == num_frames`. |
| **§4.2 Lock-free translation reads** | Overflow read path is lock-free (version + ArcSwap snapshot, no mutex). |
| **Single hash on hot path** | `pref` computed once; `lookup_with_bucket(&page_key, pref)` so no double hash. |
| **Eviction + overflow cleanup** | On evict/flush we remove from overflow only when this frame is still the entry for that page. |

### One step left: superscalar interleaving (§3.1, Listing 2)

**Goal (paper):** Let the CPU overlap the *predicted-frame load* with the *hash-table lookup* so translation latency is hidden behind the page read.

**Current code:** We do (1) preferred-frame tag check, (2) if miss → overflow lookup, (3) if miss → fault. So we never have both in flight.

**Change:** Restructure `get_page_for_read` and `get_page_for_write` so that we *issue* both operations before we use either result:

1. Compute `pref = preferred_frame(&page_key)` (already once per call).
2. *Issue* the preferred-frame load (e.g. load `metas[pref]` for the tag check).
3. *Issue* `overflow.lookup_with_bucket(&page_key, pref)` (no lock, same bucket index).
4. *Then* resolve: if preferred holds the page → use it; else if overflow returned a frame → use it; else page fault.

No new data structures; overflow is already lock-free and we already hash once. Only the control flow in the hot path changes so both lookups are in flight before branching.

### Optional later (paper / roadmap)

- **§3.2 Promotion / demotion, one-hit wonder:** Probabilistically promote hot pages into their preferred frame; demote current occupant to overflow. Defer promotion until second access. Not required for correctness; improves fast-path hit rate over time.
- **§4.1 Frame header in entry:** Store minimal metadata inside the overflow entry instead of only frame index (removes one indirection to `metas`). See `OVERFLOW_ROADMAP.md` Phase 3.
- **Benchmarks (§6):** TPC-C / YCSB vs `BufferPoolClock` and `VMCachePool`; optional micro-bench for overflow vs DashMap.

### Summary

To “finish” the PrediCache-style implementation on the superscalar branch: implement the hot-path restructure above so both the preferred-frame load and the overflow lookup are issued before we branch on the result. Everything else (deterministic placement, custom overflow, lock-free reads, single hash, eviction/overflow cleanup) is in place.

---

## Implemented but NOT from the paper

These are standard buffer-pool components, not paper contributions:

| Feature | Notes |
|---|---|
| **Clock eviction** | Standard clock sweep (same `ClockEvictionPolicy` as `BufferPoolClock`). The paper is agnostic about eviction policy; clock is our baseline. |
| **Free list (`ConcurrentQueue`)** | Frames recycled through a bounded concurrent queue. |
| **`ensure_free_frames` threshold** | Eviction triggers when frame usage exceeds 95%. Evicts in batches of up to 64. |

---

## How lookup works

```
get_page_for_read / get_page_for_write  (loop until success or hard error)
│
├─ 1. preferred_frame = hash(page_key) % num_frames
│     frame_holds_page(preferred_frame, page_key)?  ← unlatched tag check
│     │
│     ├─ YES → latch frame → re-verify page_key under latch
│     │         ├─ match   → return guard                    (FAST PATH)
│     │         └─ mismatch → drop guard, fall through       (page was replaced)
│     │
│     └─ NO → fall through
│
├─ 2. overflow.lookup_with_bucket(page_key, pref)?   // same pref, no re-hash
│     │
│     ├─ Some(idx) → latch frame → re-verify
│     │         ├─ match   → return guard                    (SLOW PATH)
│     │         └─ mismatch → continue (retry) / fall through (stale entry)
│     │
│     └─ None → fall through
│
└─ 3. handle_page_fault_write(page_key)                (PAGE FAULT)
        claim page in fault_in_progress (if already claimed → RetryPageFault, loop)
        pop frame from free list (choose_victim)
        re-check: page in preferred or overflow? → release victim, RetryPageFault
        set page_key on frame (early claim)
        if frame ≠ preferred: insert overflow; re-check we're still the entry
        read page from disk (on failure: undo claim only if we're still the entry)
        if in overflow: re-check preferred/overflow; if duplicate → release, RetryPageFault
        return guard  (claim released on drop)
        ─ on RetryPageFault, caller continues loop
```

---

## Key data structures

| Name | Type | Role |
|------|------|------|
| `OverflowTable` | Custom chaining table (`overflow_table.rs`) | Maps page → frame index for pages NOT in their preferred frame. Inlined first slot per bucket, lock-free reads (ArcSwap + version), CoW writes. `lookup_with_bucket` avoids re-hash when caller has `pref`. |
| `fault_in_progress` | `Arc<DashMap<PageKey, ()>>` | Set of pages currently being faulted; one thread per page. Claim before faulting, released on drop. |
| `free_list` | `ConcurrentQueue<usize>` | Indices of frames known to be free. |
| `metas` | `Vec<Box<FrameMeta>>` | Per-frame metadata: latch, dirty bit, eviction info, page key (atomic). |
| `pages` | `Vec<Box<Page>>` | Actual page data, one per frame. |
| `clock_hand` | `AtomicUsize` | Global clock pointer for eviction sweep. |
| `used_frames` | `AtomicUsize` | Count of frames currently holding a page. |
