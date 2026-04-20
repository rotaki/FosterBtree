# Eviction & Page Loading: LIPAH vs PT vs TLB-BP

## Purpose

Document all differences in eviction and page loading between the three BP variants to ensure fair benchmarking. Only translation mechanism should differ; eviction/loading should be identical.

## Summary of Differences

| Aspect | LIPAH (BufferPoolClock) | PT (PredictiveTranslationBP) | TLB-BP (TlbBP) |
|---|---|---|---|
| **Eviction threshold** | 95% | 95% | 95% |
| **Batch size** | Compile-time const | max(num_frames, 64) | max(num_frames, 64) |
| **Clock mark logic** | `update()` only | `update()` + `reset()` | `update()` + `reset()` |
| **Victim selection** | Free list only, no fallback | Preferred frame → free list → full scan | Free list → full scan |
| **New page placement** | Random frame | Preferred frame | Random frame |
| **Page fault insert** | Direct insert (no atomicity) | Atomic `try_insert` + RetryPageFault | Atomic `try_insert` + RetryPageFault |
| **Eviction stages** | Multi-stage (read → write upgrade) | Single-stage (write latch only) | Single-stage (write latch only) |
| **Dirty flush in eviction** | Under read latch, then upgrade | Under write latch | Under write latch |
| **Promotion/demotion** | None | Probabilistic on read+write | None |

## Detailed Differences

### 1. Clock Algorithm: `update()` vs `update() + reset()`

**LIPAH** (`buffer_pool_clock.rs`): True two-pass clock. On the first encounter with a marked frame (score > 0), it calls `update()` to clear the mark and skips. On the second encounter (score = 0), it evicts.

**PT/TLB-BP** (`predictive_translation.rs`, `tlb_bp.rs`): Calls BOTH `update()` and `reset()` on marked frames. This is a semantic difference — calling `reset()` immediately after `update()` may give different eviction behavior depending on the `ClockEvictionPolicy` implementation.

**Impact:** Could cause PT/TLB-BP to evict pages more or less aggressively than LIPAH. Need to check what `update()` then `reset()` does vs `update()` alone.

**Action needed:** Align clock logic across all three variants.

### 2. Victim Selection: Preferred Frame Placement

**LIPAH:** `choose_victim()` — only pops from the free list. No preferred frame, no fallback scan. Returns `None` if free list is empty.

**PT:** `choose_victim(Some(pref))` — checks if preferred frame is free first, then free list, then exhaustive scan of all frames.

**TLB-BP:** `choose_victim()` — free list, then exhaustive scan. No preferred frame.

**Impact:** PT places pages in their preferred frame when possible, reducing overflow lookups. LIPAH and TLB-BP use random placement. Under memory pressure, PT's preferred placement means re-faulted pages land in their preferred frame more often.

**Note:** This is an intentional design difference (PT's core feature), not a bug.

### 3. Eviction Implementation: Multi-Stage vs Single-Stage

**LIPAH** (`evict_batch`): Multi-stage eviction:
1. Collect candidates via clock scan
2. `flush_dirty()` — flush dirty pages under read latch
3. `latch_clean()` — acquire write latches on clean pages
4. `upgrade_dirty()` — upgrade dirty page read latches to write
5. `remove_from_page_table()` — remove from translation in sorted order
6. `finalize_eviction()` — clear frames, enqueue to free list

**PT/TLB-BP** (`evict_batch`): Single-stage eviction loop:
1. Clock scan frames in batch
2. For each candidate: acquire write latch → flush if dirty → remove from overflow → clear → enqueue

**Impact:** LIPAH's multi-stage approach holds read latches during flush (less blocking for readers), then upgrades. PT/TLB-BP hold write latches for the entire eviction (more blocking but simpler). Under high concurrency, this could cause different latch contention patterns.

**Action needed:** Consider aligning eviction strategy, or at minimum documenting this as a known variable.

### 4. Page Fault Race Handling

**LIPAH:** Direct insert into `page_to_frame` DashMap. No `try_insert` semantics — if two threads fault the same page, DashMap handles it via sharded locks.

**PT/TLB-BP:** Atomic `try_insert` into overflow table. If another thread already inserted the page, returns `RetryPageFault` and the caller retries the lookup. This is more correct under high concurrency.

**Impact:** Different behavior under concurrent page faults for the same page. LIPAH may have duplicate entries (last writer wins). PT/TLB-BP guarantee exactly one entry.

### 5. Free List Fallback

**LIPAH:** `choose_victim()` only uses the free list. If empty, returns `None` → `CannotEvictPage` error.

**PT/TLB-BP:** After exhausting the free list, scan all frames looking for any free one. This is a safety fallback — free list hints may be stale.

**Impact:** Under extreme pressure, LIPAH may fail to find victims while PT/TLB-BP succeed via the exhaustive scan.

### 6. Batch Size

**LIPAH:** Configurable at compile time via `EVICTION_BATCH_SIZE` generic parameter. Typically set to 64.

**PT/TLB-BP:** Hardcoded `min(num_frames, 64)`.

**Impact:** Should be equivalent when LIPAH uses 64, but could differ with other values.

## What Is Identical

- Eviction trigger threshold: 95% used frames
- Max eviction iterations: `2 * num_frames / batch`
- Dirty page flush: CAS on dirty bit (true → false), write only if CAS succeeds
- `flush_all()`: Parallel read-latch + flush for all frames
- `flush_all_and_reset()`: Parallel write-latch + flush + clear + repopulate free list
- Page initialization on fault: `set_page_key`, `read_page`, `evict_info().reset()`, `dirty.store(true)`
- `used_frames` management: increment on fault, decrement on eviction

## Recommendations for Fair Comparison

1. **Align clock logic:** Make PT/TLB-BP use the same `update()` behavior as LIPAH (or vice versa)
2. **Align eviction staging:** Either make all single-stage or all multi-stage
3. **Align free list fallback:** Add exhaustive scan fallback to LIPAH, or remove from PT/TLB-BP
4. **Keep preferred frame as the variable:** The only intentional difference should be translation mechanism + preferred placement
