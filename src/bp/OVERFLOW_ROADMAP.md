# Overflow Table: Paper vs DashMap, and Roadmap

This document quotes the **Predictive Translation** paper (Zinsmeister et al., SIGMOD ’26) on the hash table used for translation when the preferred-frame prediction fails (our “overflow” table), then contrasts it with our current DashMap and outlines a concrete roadmap in `predictive_translation.rs`.

---

## What the Paper Says (Exact Quotes)

### Section 4.1 — Lightweight Translation (Hash Table Structure)

**Chaining hash table:**

> "A key observation is that, with a sufficiently large hash table, most translations will not conflict. Consequently, if the PID translation typically resolves in the first probed position, the expected CPU cache miss cost is reduced to one, rendering closed-addressing comparable in performance to open-addressing schemes. Closed-addressing, particularly **chaining**, offers advantages in concurrent environments due to its simplicity: **only one latch per key is relevant**. In contrast, open-addressing approaches such as Cuckoo hashing or linear probing involve entry relocation. This not only increases implementation complexity but also sometimes requires a single reader to hold many latches simultaneously, thereby introducing contention. **Given these considerations, we choose a chaining hash table design** for its simplicity and its suitability for highly concurrent workloads without compromising performance."

**Inlining the first slot:**

> "Moreover, since most translations are expected to **land in the first slot of the chain**, we propose **inlining this slot within the translation array** to improve cache locality, as illustrated in Figure 3(b). As we will show later in Section 6, simply **pre-allocating a large enough chaining hash table with an inlined first slot** makes PID translation highly lightweight, laying the foundation for a high-performance buffer pool. **We also inline the buffer frame header directly into its hash table entry, eliminating an additional indirection.**"

**Frame header in hash table (not in a separate array):**

> "Our design separates frame headers and buffer frames: **headers are stored in the hash table**, while buffer frames reside in a dedicated memory pool."

So the paper’s translation hash table (our overflow structure) is:

1. **Chaining** hash table (closed addressing).
2. **First slot of each chain inlined** in the bucket/translation array (no pointer chase for the common single-element case).
3. **Frame header (metadata) stored inside the hash table entry** — not in a separate `metas` array — to remove one indirection.

---

### Section 4.2 — Lightweight Synchronization (Optimistic Latch)

**Why not a mutex / rw-latch:**

> "However, hot pages may still be accessed concurrently by many threads, leading to **contention on the bucket latch—whether implemented as a standard mutex or a reader-writer latch, since both incur a memory write for each latch operation**. Therefore, an efficient and lightweight mechanism is required to synchronize hash table operations between multiple threads."

**Short chains → one latch per bucket:**

> "As discussed in Section 4.1, by using a large chaining hash table, the fill factor becomes small, leading to a minimal collision ratio. In other words, a **majority of chains are short: most of them will comprise only one slot**. … Therefore, **it is enough to have one latch for the entire bucket** instead of each element. This is especially true if that latch allows for scalable read operations."

**Lock-free reads enable superscalar overlap:**

> "**Lock-free enables superscalar execution.** Lock-free collision chains, e.g., a lock-free linked list, may seem like the most natural solution. This **eliminates strongly serialized instructions like atomic read-modify-write for read operations**. As a result, commodity CPUs can **leverage superscalar execution to overlap the bucket and page accesses**, effectively hiding their latency."

**Optimistic latch:**

> "Given that collision chains are typically short, conflicts are rare, and modifications are infrequent, we advocate using an **optimistic latch** for efficient chain synchronization. **Optimistic latches are often implemented using a version counter that increments whenever the latch transitions out of exclusive mode**. Upon latch acquisition, the system **(1) retrieves the current latch version, then (2) iterates through the chain to find the corresponding frame, and (3) validates the latch version to detect any concurrent updates or inserts**."

So the paper’s synchronization for this hash table is:

1. **One latch per bucket** (not per element).
2. **Optimistic latch** implemented with a **version counter** (incremented when leaving exclusive mode).
3. **Read path:** read version → walk chain → re-read version; if unchanged (and even), the read is valid — **no lock acquire** for readers.
4. **Writers** take the latch exclusively and bump the version when done.
5. This makes **hash table translation non-blocking for readers**, so the CPU can **overlap** translation with page access (superscalar interleaving).

---

## How This Differs From Our Current Overflow (DashMap)

| Aspect | Paper (Section 4) | Our current overflow |
|--------|-------------------|------------------------|
| **Structure** | Chaining hash table, **first slot inlined** in bucket array | DashMap: sharded hash map, no inlined first slot |
| **Metadata location** | **Frame header inlined in hash table entry** (no extra indirection) | Frame index only in map; metadata in separate `metas: Vec<Box<FrameMeta>>` (extra indirection) |
| **Synchronization** | **Optimistic latch** (version counter): readers **lock-free** (read version → read chain → validate version) | Per-shard **mutex** in DashMap: every lookup acquires a lock |
| **Read cost** | One version read + chain walk + version validate; **no atomic write** on read path | Lock acquire (often a write) on every overflow lookup |
| **Superscalar** | Hash lookup can be **overlapped with predicted-frame load** because lookup doesn’t block | Lookup takes a lock → hard to overlap with page load |

So the paper’s design differs in **structure** (chaining + inlined first slot + metadata in entry) and **synchronization** (optimistic latch for lock-free reads). Our DashMap is a correct but heavier baseline.

---

## Reference: PrediCache C++ implementation

The paper's prototype lives at **`~/databases-research/PrediCache`**. Useful files:

| File | Role |
|------|------|
| **`versioned_lock.hpp`** | Optimistic latch: one `atomic<u64>`; **MSB = lock bit** (1 = writer), low bits = version. `lock()` spins until (v>>63)==0 then CAS to set MSB; `unlock()` clears MSB and increments version. Readers never call lock — they read version, read data, re-read version and retry if changed. |
| **`ht.hpp`** | Chaining hash table with **first entry inlined** in `ChainHead` (bucket). Each bucket has `VersionedLock` + inlined `Entry head` + chain via `Entry* next`. **`access()`** (read path): load `bucket.lock.version`; if MSB set, restart; else walk `head` and chain **without taking the lock**; re-check version; return. **`insert()` / `remove()`**: `tryLock(version)`, modify chain, `unlock()`. |
| **`buffer_manager.hpp`** | Uses `HTBufferManager ht`; `fixX`/`fixS` call `ht.access(pid, hash)` then fix the returned buffer frame. |

**Design difference vs our Rust overflow table:** PrediCache does **in-place updates** with version validation: writers take the versioned lock (set MSB), mutate the chain, then unlock (clear MSB, bump version). Readers read the same memory and validate the version; no copy. Our implementation uses **copy-on-write** (ArcSwap + clone on write): writers clone the bucket payload, modify, then store a new `Arc`; readers load the current `Arc` and read it (no mutex). Both give lock-free reads; CoW avoids any risk of reading partially updated state but pays clone + allocation on insert/remove. PrediCache's in-place approach is closer to the paper and avoids per-write allocation.

**Performance prediction (CoW vs DashMap, and why we don’t do the C++ way yet):** We expect the current overflow table **not** to be slower than DashMap overall. Read path is strictly better (no mutex; a couple of atomics + plain reads). Write path incurs a **clone** of the bucket (inlined slot + `Vec`) and an allocation per insert/remove; that cost is bounded by low fill factor (typically 0–1 entries per bucket) and overflow is read-heavy, so the hot path is lookup. We have **not** adopted the C++ PrediCache style (in-place updates + versioned lock) for now; that would sidestep the clone cost on writes but requires either unsafe or atomic pointers + reclamation in Rust. If profiling later shows write-heavy overflow and clone as a bottleneck, revisiting an in-place design is an option.

---

## Roadmap in `predictive_translation.rs`

Goal: replace `OverflowTable` (backed by `DashMap<PageKey, usize>`) with a paper-style structure and synchronization, **without** changing the rest of the BP (preferred-frame fast path, `choose_victim`, clock eviction, fault handling). Call sites stay the same: `overflow.lookup`, `overflow.insert`, `overflow.remove`, `overflow.contains_key`, `overflow.get_page_keys`, and full iteration for flush/invariants.

### Phase 1 — Custom chaining overflow table (Section 4.1)

1. **Define a chaining hash table** for overflow only:
   - Key: `PageKey`, value: frame index (and optionally minimal metadata if we ever inline it).
   - Fixed capacity, number of buckets proportional to `num_frames` (e.g. same or 1.5×), pre-allocated.
2. **Inline the first slot per bucket:**
   - Each bucket has space for **one** entry directly in the bucket struct (e.g. `Option<(PageKey, usize)>` or a small struct), plus a link to a chain of additional entries (e.g. `Vec` or linked list) for collisions.
   - Lookup: hash → bucket → check inlined slot first; if miss and chain exists, walk chain. No indirection for the common single-occupant case.
3. **Keep value as frame index only for now:**
   - Paper inlines “frame header” into the hash entry; we already have `metas` and `pages` arrays. So we can keep storing only `usize` (frame index) in the overflow table and **not** move `FrameMeta` into the table in the first step. Optionally add “inline metadata” later as a separate step to remove the extra indirection to `metas[frame_id]`.
4. **Preserve exact API:** `OverflowTable::lookup`, `insert`, `remove`, `contains_key`, `get_page_keys`, and iteration (for `flush_all_and_reset` and any invariant checks). Internal representation only changes.

### Phase 2 — Optimistic latch per bucket (Section 4.2)

1. **Add a version counter per bucket** (e.g. `AtomicU64` or `AtomicU32`): even = no writer, odd = writer active.
2. **Read path (lookup / contains_key / get_page_keys / iter for read):**
   - Read version (must be even).
   - Read inlined slot and/or chain.
   - Re-read version; if same and even, use the result; else retry (or fall through to slow path / retry a bounded number of times).
3. **Write path (insert / remove):**
   - Acquire exclusive access to the bucket (e.g. small spinlock or mutex per bucket, or CAS to odd version).
   - Do insert/remove, then set version back to even (increment).
4. **No change to frame-level latches:** Page latch (read/write) stays on `FrameMeta` as today; only the **overflow table’s** synchronization changes from “DashMap’s lock” to “optimistic latch per bucket.”

### Phase 3 (optional, later) — Metadata in entry and superscalar

- **Inline minimal metadata in overflow entry** (e.g. frame index + dirty bit or just frame index if we still want to keep one indirection to `metas` for latch): only if we want to fully match the paper’s “frame header in hash table entry” and remove the extra indirection.
- **Superscalar interleaving:** Restructure the **read hot path** in `get_page_for_read` / `get_page_for_write` so that the predicted-frame load is **issued in parallel** with the overflow lookup (Section 3.1, Listing 2). This is a separate change in the caller; it becomes viable once overflow lookup is lock-free (Phase 2).

---

## Summary

- **Paper (Section 4):** Chaining hash table; **first slot inlined** in translation array; **frame header in hash table entry**; **optimistic latch** (version counter) per bucket for **lock-free reads** and superscalar overlap.
- **Us (current):** Overflow = **custom chaining** with inlined first slot (Phase 1); metadata still in separate `metas` array.
- **Roadmap:**  
  - **Phase 1:** Custom chaining overflow with **inlined first slot**, same API, still store frame index (and optionally defer “metadata in entry”).  
  - **Phase 2:** **Optimistic latch** (version counter) per bucket so overflow reads are lock-free.  
  - **Phase 3 (optional):** Inline metadata in entry if desired; then **superscalar** hot path in the BP layer.

All of this is confined to the overflow table implementation and its call sites in `predictive_translation.rs`; the rest of the BP (preferred frame, tag check, fault, eviction) stays as on the frame-hashing branch.
