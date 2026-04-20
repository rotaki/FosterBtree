# Eviction & Page Loading: PT vs TLB-BP

## Purpose

Document that PT and TLB-BP have identical eviction/loading behavior, differing ONLY in:
1. Translation mechanism (overflow hash table vs TLB + congee ART)
2. Preferred frame placement (PT has it, TLB-BP doesn't)
3. Promotion/demotion (PT has it on reads, TLB-BP doesn't)

## PT vs TLB-BP: What Is Identical

| Aspect | PT | TLB-BP |
|---|---|---|
| Eviction trigger | 95% used frames | 95% used frames |
| Batch size | min(num_frames, 64) | min(num_frames, 64) |
| Max iterations | 2 * num_frames / batch | 2 * num_frames / batch |
| Clock logic | score > 0 → reset(), skip | score > 0 → reset(), skip |
| Eviction: latch | Write latch | Write latch |
| Eviction: dirty flush | write_to_disk_if_dirty_w | write_to_disk_if_dirty_w |
| Eviction: overflow remove | lookup then remove (race check) | lookup then remove (race check) |
| Eviction: frame clear | set_page_key(None), reset(), enqueue | set_page_key(None), reset(), enqueue |
| Page fault: insert | Atomic try_insert, RetryPageFault on race | Atomic try_insert, RetryPageFault on race |
| Page fault: disk read | container.read_page() | container.read_page() |
| Page fault: error cleanup | remove from overflow, enqueue frame | remove from overflow, enqueue frame |
| Page fault: used_frames | increment before, decrement on fail | increment before, decrement on fail |
| New page: init | set_id, set_page_key, dirty=true, reset evict | set_id, set_page_key, dirty=true, reset evict |
| Free list | ConcurrentQueue + exhaustive scan fallback | ConcurrentQueue + exhaustive scan fallback |
| flush_all | Parallel read-latch + CAS dirty flush | Parallel read-latch + CAS dirty flush |
| flush_all_and_reset | Parallel write-latch + flush + clear + repopulate | Parallel write-latch + flush + clear + repopulate |
| Dirty flush CAS | compare_exchange(true, false, AcqRel, Acquire) | compare_exchange(true, false, AcqRel, Acquire) |

## PT vs TLB-BP: Intentional Differences

| Aspect | PT | TLB-BP | Reason |
|---|---|---|---|
| Translation table | OverflowTable (hash + chaining) | Congee ART tree (ordered) | TLB-BP trades hash O(1) for ordered range scans |
| Fast path | Preferred frame check (hash → frame) | Per-thread TLB probe (L1 resident) | Different translation strategy |
| choose_victim | Tries preferred frame first | No preferred frame | PT's core feature |
| create_new_page | Places in preferred frame | Random frame | PT's core feature |
| Promotion | Probabilistic on reads (1/50 or 1/512) | None | PT moves hot pages to preferred frame |
| Range prefill | None | Sequential detection + congee range() | TLB-BP pre-warms TLB on scans |

## LIPAH (BufferPoolClock) Differences

LIPAH uses a different eviction implementation. Key differences from PT/TLB-BP:

| Aspect | LIPAH | PT/TLB-BP |
|---|---|---|
| Clock logic | Non-standard: unmarked → mark + skip, marked → evict | Standard: marked → clear + skip, unmarked → evict |
| Eviction staging | Multi-stage: collect → flush dirty (read latch) → upgrade → remove | Single-stage: write latch → flush → remove → clear |
| choose_victim | Free list only, no fallback scan | Free list + exhaustive scan fallback |
| Page fault | Direct DashMap insert (no atomic try_insert) | Atomic try_insert with RetryPageFault |
| Batch size | Compile-time generic const | Hardcoded min(num_frames, 64) |
| Translation | DashMap hierarchy | Overflow hash table / Congee ART |

These differences mean LIPAH is **not** a controlled comparison for eviction behavior. PT vs TLB-BP is the fair comparison.

## Verified Identical (method-by-method audit)

Every method below was compared line-by-line between PT and TLB-BP. The only differences are overflow table method call style (direct vs wrapper) and log message prefixes.

- `try_get_read_guard` / `try_get_write_guard` — both resolve to `try_new_with_key_slot(meta, page, null)`
- `ensure_free_frames` — identical (95% threshold, calls evict_batch)
- `evict_batch` — identical clock logic, batch size, max iterations, eviction order
- `handle_page_fault` — identical except PT passes preferred frame to choose_victim
- `write_to_disk_if_dirty_w` / `_r` — character-for-character identical CAS logic
- `flush_all` — identical (parallel read-latch + CAS flush)
- `flush_all_and_reset` — identical (parallel write-latch + flush + clear + repopulate free list)
- `create_new_pages_for_write` — identical (loop over create_new_page_for_write)
- `create_new_page_for_write` — identical except PT passes preferred frame to choose_victim
- `fast_evict` — both no-op
- `stats()` / `reset_stats()` — identical output
- `Drop` — both call flush_all_and_reset if not test mode
- `create_container` / `drop_container` — both no-op
- `is_in_mem` / `get_page_keys_in_mem` — same semantics via different overflow types

**Conclusion:** PT and TLB-BP differ ONLY in the three intentional design choices: translation mechanism, preferred frame placement, and promotion.
