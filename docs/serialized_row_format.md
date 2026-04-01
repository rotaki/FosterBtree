# Serialized Row Format

Records are stored as contiguous byte sequences with fields serialized in schema order.
There is no header, no field offset table, and no padding between fields.
To access field N, you must walk through fields 0..N-1 to compute the offset.

## Record Layout

```
[ Field 0 ][ Field 1 ][ Field 2 ] ... [ Field N-1 ]
```

Each field is serialized independently. The schema defines the column order,
whether each column is nullable, and its data type.

## Per-Field Encoding

### Nullable Fields

If a column is marked nullable in the schema, the field is prefixed with a 1-byte null indicator:

```
+------+------------------+
| 0x00 |                  |   NULL — 1 byte total, no value follows
+------+------------------+

+------+------------------+
| 0x01 | value bytes ...  |   NOT NULL — indicator + value
+------+------------------+
```

Non-nullable columns have no indicator byte; the value starts immediately.

### Fixed-Size Types

All numeric values are stored in **little-endian** byte order.

| DataType      | Size (bytes) | Encoding                        |
|---------------|--------------|---------------------------------|
| Int8          | 1            | `i8` LE                         |
| Int16         | 2            | `i16` LE                        |
| Int32         | 4            | `i32` LE                        |
| Int64         | 8            | `i64` LE                        |
| Uint8         | 1            | raw byte                        |
| Uint16        | 2            | `u16` LE                        |
| Uint32        | 4            | `u32` LE                        |
| Uint64        | 8            | `u64` LE                        |
| Float32       | 4            | `f32` LE (IEEE 754)             |
| Float64       | 8            | `f64` LE (IEEE 754)             |
| Bool          | 1            | `0x00` = false, `0x01` = true   |
| DateTime      | 4            | `i32` LE (days from CE)         |
| Months        | 4            | `i32` LE                        |
| Days          | 8            | `i64` LE                        |
| Pointer       | 8            | `page_id: u32 BE` + `frame_id: u32 BE` |
| FixedBytes8   | 8            | raw bytes                       |
| FixedBytes16  | 16           | raw bytes                       |
| FixedBytes24  | 24           | raw bytes                       |

> Note: `Pointer` uses **big-endian** for its sub-fields (page_id, frame_id),
> unlike all other numeric types which use little-endian.

### Variable-Size Types

```
+-------------------+--------------------+
| length (4B, u32 LE) | data bytes ...   |
+-------------------+--------------------+
```

| DataType | Encoding                                     |
|----------|----------------------------------------------|
| String   | 4-byte LE length prefix + UTF-8 encoded bytes |
| VarBytes | 4-byte LE length prefix + raw bytes           |

## Example: Stock Row

Schema (from TPC-C stock table):

| Col | Name           | DataType | Nullable |
|-----|----------------|----------|----------|
| 0   | S_W_ID         | Uint16   | No       |
| 1   | S_I_ID         | Uint32   | No       |
| 2   | S_QUANTITY     | Int16    | No       |
| 3   | S_YTD          | Uint32   | No       |
| 4   | S_ORDER_CNT    | Uint16   | No       |
| 5   | S_REMOTE_CNT   | Uint16   | No       |
| 6   | S_DIST         | String   | No       |
| 7   | S_DATA         | String   | No       |

Serialized layout (no nullable columns, so no indicator bytes):

```
Offset  Size     Field
──────  ───────  ──────────
0       2        S_W_ID (u16 LE)
2       4        S_I_ID (u32 LE)
6       2        S_QUANTITY (i16 LE)
8       4        S_YTD (u32 LE)
12      2        S_ORDER_CNT (u16 LE)
14      2        S_REMOTE_CNT (u16 LE)
16      4+N      S_DIST (4B length + N bytes UTF-8)
20+N    4+M      S_DATA (4B length + M bytes UTF-8)
```

## Implications for Field Access

- **No random access**: To read field N, you must walk fields 0..N-1 to compute the byte offset.
  Variable-length fields (String, VarBytes) require reading the 4-byte length prefix to skip.
- **Fixed-size fields can be patched in-place**: If only fixed-size fields are modified,
  the total record size stays the same, allowing direct byte patching without rebuilding the buffer.
- **Selective deserialization**: Fields can be skipped without allocation by advancing
  the offset (fixed-size: add known size; variable-size: read length prefix and skip `4 + len` bytes).

## Related Code

- Serialization: `Field::to_bytes()` in `src/txn_storage2/field.rs`
- Deserialization: `Field::from_bytes()` in `src/txn_storage2/field.rs`
- Full record ser/deser: `record_to_bytes()`, `bytes_to_record()` in `src/txn_storage2/field.rs`
- Selective deser: `bytes_to_fields_selective()` in `src/txn_storage2/field.rs`
- In-place patching: `patch_record_fields_inplace()` in `src/txn_storage2/field.rs`
- Byte-splice merge: `merge_record_bytes()` in `src/txn_storage2/field.rs`
- Skip without deser: `DataType::skip_bytes()` in `src/txn_storage2/field.rs`
