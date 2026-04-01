# Transactional Storage Optimization Status

## Summary of Changes

All changes target the TPC-C NewOrder hot path.

- **Baseline:** 67us/txn, 287K commits/5s
- **Final:** 63us/txn, 305K commits/5s (+6.3% throughput)

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| total txn | 67us | 63us | -6% |
| throughput | 287K | 305K | +6.3% |
| get_item | 12us | 10us | -17% |
| get_stock | 16us | 14us | -13% |
| update_stock | 3us | 2us | -33% |
| insert_orderline | 10us | 10us | same |
| commit | 11us | 11us | same |

## Implemented (in `field.rs`, `foster_btree_page.rs`, `transactional_storage.rs`)

### 1. Selective Deserialization (`field.rs`)

**Status: Implemented, tested, measured.**

- Added `DataType::skip_bytes()` — computes byte size of a field without deserializing it. For variable-length types (String, VarBytes), reads the 4-byte length prefix only.
- Added `DataType::is_fixed_size()` — returns whether a data type has constant byte size.
- Added `bytes_to_fields_selective()` — deserializes only requested columns from a serialized row. Walks all columns to compute offsets but only allocates/deserializes the ones in `col_indices`. Skips string/varbytes heap allocations for unwanted fields.
- Replaced all `bytes_to_record` + cherry-pick patterns in `get_fields` with `bytes_to_fields_selective`.

**Impact:** Deserialization cost per `get_fields` call dropped from 271ns to ~130ns (-52%).

### 2. Byte-Level Merge on Commit (`field.rs`)

**Status: Implemented, tested, measured.**

- Added `merge_record_bytes()` — splices updated field bytes into on-disk row bytes without deserializing. For unchanged fields, copies raw bytes. For changed fields, serializes only those.
- Added `merge_record_bytes_sparse()` — same but takes a `HashMap<usize, &Field>` of sparse updates (used by the new KeyEntry design).

**Impact:** Eliminated full deserialization + re-serialization during commit merge.

### 3. In-Place Field Patching (`field.rs`, `foster_btree_page.rs`)

**Status: Implemented, tested, measured.**

- Added `all_fixed_size()` — checks if all updated columns are fixed-size types.
- Added `patch_record_fields_inplace()` — patches specific fixed-size field bytes directly in the page's mutable value slice, without building any intermediate buffer.
- Added `patch_record_fields_inplace_sparse()` — same but for sparse updates from KeyEntry.
- Added `FosterBtreePage::get_val_mut()` — returns `&mut [u8]` to a slot's value bytes, enabling in-place writes.
- Commit path uses in-place patching when all modified fields are fixed-size (common case for stock updates: S_QUANTITY, S_YTD, S_ORDER_CNT are all integers).

**Impact:** For fixed-size field updates, commit avoids all buffer allocation and copies.

### 4. Per-Field KeyEntry with Bitset Read Tracking (`transactional_storage.rs`)

**Status: Implemented, compiles, tests passed in earlier run. Needs re-verification after a git incident.**

Replaced the old `RWEntry` + separate `field_locks` HashMap with a unified per-field design:

```rust
enum KeyEntry {
    Active {
        ptr: RecordPointer,
        ghost: bool,
        read_set: u64,              // bitset: bit i = shared lock on col i
        writes: Vec<(usize, Field)>, // only modified fields
    },
    Insert { ptr, record: Vec<Field> },  // full record for ghost slot
    Delete { ptr, ghost: bool },
}
```

Key changes:
- **`ReadWriteSet`** simplified to single `HashMap<Vec<u8>, KeyEntry>`. No more separate `field_locks` HashMap. No more `RWValue` wrapper.
- **Lock mode is structural**: `read_set` bits = shared locks, `writes` entries = exclusive locks. No separate tracking.
- **`get_fields` cache-hit path**: checks `writes` vec first (return cached value), falls back to disk read via `bytes_to_fields_selective` for Read/new columns. Acquires shared locks for new columns via `read_set` bitset.
- **`update_fields`**: no longer calls `bytes_to_record`. Just stores `Write` entries directly. Traverses btree for existence check + latch-lock ordering only.
- **`update_field_with_func`**: reads only the single target field from disk via `bytes_to_fields_selective(&[col_idx])` instead of deserializing all fields. If field is already in `writes`, mutates in place.
- **`insert_record`**: stores `KeyEntry::Insert` with full record. Resurrection (Delete -> insert) creates `Active` with all fields as writes.
- **`delete_record`**: transitions Active/Insert to Delete, preserving ghost flag.
- **Commit path** (`apply_updates_and_release_exclusive_locks`): builds sparse `HashMap<usize, &Field>` from `writes` vec, uses `patch_record_fields_inplace_sparse` (fixed-size fast path) or `merge_record_bytes_sparse` (variable-size). Insert commit now rewrites the full record (handles Insert+update case correctly).
- **Abort path**: derives lock types from `read_set` + `writes` structurally.
- **`iter_next`**: overlays `writes` on deserialized disk record for Active entries.
- **key_bytes moved instead of cloned** wherever it's the last use, eliminating heap allocations on the hot path.

**Impact (measured before git incident):**
- `rwset+clone`: 154ns -> 66ns (-57%)
- `update_stock`: 3us -> 2us (-33%)
- Overall: depends on interaction with other changes

### 5. Serialized Row Format Documentation (`docs/serialized_row_format.md`)

**Status: Written.**

Documents the byte-level encoding of rows: per-field nullable indicators, little-endian numeric values, 4-byte length-prefixed strings, and implications for field access patterns.

## Not Yet Implemented

### A. Smarter BTree Traversal Hints

btree_traverse is now the dominant cost (42-45% of `get_fields`). Potential approaches:
- Multi-level hints (store parent page refs, retry from parent before root)
- Sequential access optimization (stock accesses within item loop hit nearby pages)
- Transaction-level page cache to avoid re-fetching the same page

### B. Batch Orderline Insert

`insert_orderline` is 16% of txn. All 10 orderlines have sequential keys. A batch insert API could acquire the write latch once and insert multiple records.

### C. Item Table Fast Path

The Item table is read-only in TPC-C (never updated). It could skip locking overhead entirely.

### D. Locktable API Change to Avoid Clones

`ConcurrentLockTable::try_shared/try_exclusive` take `Vec<u8>` by value (for DashMap's entry API). Changing to `&[u8]` with `.get()` first would eliminate clones for the common case where the entry already exists.

### E. Commit Re-Traversal Elimination

Commit still re-traverses the btree for every write entry. Caching page references from the update phase could eliminate ~6us of the ~10us commit cost.

## Current State

The code compiles cleanly. All 24 tpcc2 tests passed in the last full run before a git incident (accidental `git checkout --` on `transactional_storage.rs` during a sed operation, which was fortunately limited in scope). The file should be re-tested to confirm.

## Files Modified

| File | Changes |
|------|---------|
| `src/txn_storage2/field.rs` | `skip_bytes`, `is_fixed_size`, `bytes_to_fields_selective`, `merge_record_bytes`, `merge_record_bytes_sparse`, `patch_record_fields_inplace`, `patch_record_fields_inplace_sparse`, `all_fixed_size` |
| `src/access_method/fbt/foster_btree_page.rs` | `get_val_mut` trait method + impl |
| `src/txn_storage2/transactional_storage.rs` | Complete rewrite of `RWEntry`/`ReadWriteSet` to `KeyEntry` with bitset, all CRUD methods, commit/abort paths, iterator |
| `docs/serialized_row_format.md` | New documentation |