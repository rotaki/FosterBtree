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
  DashMap** (`OverflowTable`) that maps `PageKey -> frame_index` for pages not
  in their preferred frame.
- **Overflow table is secondary, not primary.** In a conventional buffer pool
  *every* resident page lives in a translation hash table. Here, pages sitting
  in their preferred frame have no hash-table entry — only overflow pages appear
  in the DashMap.

### Preferred-frame-aware page fault

When a page fault loads a page from disk:

1. A free frame is obtained from the free list (via `choose_victim()`).
2. If the free frame *happens to be* the page's preferred frame, the page is
   placed there and **no overflow entry is created** (the tag check is enough).
3. Otherwise the page goes into whatever free frame was available, and an entry
   is added to the overflow DashMap.

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
behaviour holds under concurrency; the same protocol will apply when the
overflow table is replaced by a custom hash table (PrediCache-style).

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

## Not implemented (from the paper)

| Paper feature | Section | Why it matters |
|---|---|---|
| **Promotion / demotion** | 3.2, 5.1 | Hot pages are NOT migrated into their preferred frame. Whether a page lands in its preferred frame is purely opportunistic (depends on free-list order). The overflow map does not shrink over time via promotion. |
| **One-hit-wonder detection** | 3.2 | All pages are treated equally. No access-count tracking, no deferred promotion for first-access pages. |
| **Optimistic latch (lock-free reads)** | 4.2 | The overflow DashMap uses standard per-shard locking. Read-path lookups are not lock-free. |
| **Inlined chaining hash table** | 4.1 | Using off-the-shelf `DashMap`, not a custom hash table with inlined first slots or embedded frame metadata. |
| **Superscalar interleaving** | 3.1, Listing 2 | The lookup does tag check *then* overflow lookup sequentially. The CPU cannot overlap predicted-frame access with hash verification because the hash lookup (DashMap) takes a lock. |
| **Benchmarks** | 6 | No TPC-C / YCSB comparison against `BufferPoolClock` or `VMCachePool` yet. |

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
├─ 2. overflow.lookup(page_key)?
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
| `OverflowTable` | `DashMap<PageKey, usize>` | Maps page → frame index for pages NOT in their preferred frame. (Will be replaced by custom hash table in PrediCache path.) |
| `fault_in_progress` | `Arc<DashMap<PageKey, ()>>` | Set of pages currently being faulted; one thread per page. Claim before faulting, released on drop. |
| `free_list` | `ConcurrentQueue<usize>` | Indices of frames known to be free. |
| `metas` | `Vec<Box<FrameMeta>>` | Per-frame metadata: latch, dirty bit, eviction info, page key (atomic). |
| `pages` | `Vec<Box<Page>>` | Actual page data, one per frame. |
| `clock_hand` | `AtomicUsize` | Global clock pointer for eviction sweep. |
| `used_frames` | `AtomicUsize` | Count of frames currently holding a page. |
