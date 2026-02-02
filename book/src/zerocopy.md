# Zero-Copy Decoding

For maximum performance, `zero-postgres` provides zero-copy row decoding through the `RefFromRow` trait. This allows you to decode rows as references directly into the read buffer, avoiding any memory allocation or copying.

## When to Use

Zero-copy decoding is useful when:
- Processing large result sets where allocation overhead matters
- All columns are fixed-size types (integers, floats)
- All columns are `NOT NULL`
- You don't need to store the decoded rows (processing in a callback)

## PostgreSQL Wire Format

PostgreSQL's binary protocol includes a 4-byte length prefix before each column value. To enable zero-copy struct casting, use the `LengthPrefixed<T>` wrapper which includes this prefix in its layout.

## Requirements

To use zero-copy decoding, your struct must:

1. Derive `RefFromRow`
2. Have `#[repr(C, packed)]` attribute
3. Use `LengthPrefixed<T>` with big-endian types (PostgreSQL uses network byte order)

## Example

```rust
use zero_postgres::conversion::ref_row::{RefFromRow, LengthPrefixed, I64BE, I32BE};
use zero_postgres_derive::RefFromRow;

#[derive(RefFromRow)]
#[repr(C, packed)]
struct UserStats {
    user_id: LengthPrefixed<I64BE>,     // 4 + 8 = 12 bytes
    login_count: LengthPrefixed<I32BE>, // 4 + 4 = 8 bytes
}

// Process rows without allocation
conn.exec_foreach_ref::<UserStats, _, _, _>(&stmt, (), |row| {
    // row is &UserStats - a reference into the buffer
    println!("user_id: {}", row.user_id.get().get());
    println!("login_count: {}", row.login_count.get().get());
    Ok(())
})?;
```

## Available Types

PostgreSQL uses big-endian (network byte order) encoding. Use these types wrapped in `LengthPrefixed`:

| Rust Type | Wire Size | Total with Prefix |
|-----------|-----------|-------------------|
| `i8` / `u8` | 1 byte | 5 bytes |
| `I16BE` / `U16BE` | 2 bytes | 6 bytes |
| `I32BE` / `U32BE` | 4 bytes | 8 bytes |
| `I64BE` / `U64BE` | 8 bytes | 12 bytes |

These are re-exported from `zero_postgres::conversion::ref_row` for convenience.

## Accessing Values

`LengthPrefixed<T>` provides these methods:

```rust
// Get the length prefix (should match type size)
let len: i32 = row.user_id.len();

// Check if NULL (length == -1)
let is_null: bool = row.user_id.is_null();

// Get the inner value (returns T, e.g., I64BE)
let inner: I64BE = row.user_id.get();

// Get the native value (chain .get() calls)
let user_id: i64 = row.user_id.get().get();
```

## Limitations

- **No NULL support**: All columns must be `NOT NULL`. Use `FromRow` for nullable columns.
- **Fixed-size types only**: Variable-length types like `TEXT`, `BYTEA`, `NUMERIC` are not supported.
- **No column name matching**: Columns must match struct field order exactly.
- **Binary format only**: Only works with extended query protocol (not simple queries).
- **Callback-based only**: Returns references into the buffer, so can only be used with `exec_foreach_ref`.

## Comparison with FromRow

| Feature | `FromRow` | `RefFromRow` |
|---------|-----------|--------------|
| Allocation | Yes (per row) | No |
| NULL support | Yes (`Option<T>`) | No |
| Variable-length types | Yes | No |
| Column name matching | Yes | No |
| Return type | Owned `T` | Reference `&T` |
| API | `exec_first`, `exec_collect`, `exec_foreach` | `exec_foreach_ref` |

## How It Works

1. The derive macro generates `zerocopy` trait implementations (`FromBytes`, `KnownLayout`, `Immutable`)
2. At compile time, it verifies all fields implement `FixedWireSize`
3. At runtime, the row buffer is cast directly to `&YourStruct` using zerocopy
4. No parsing, no allocation - just a pointer cast with size validation

## Wire Format Diagram

```text
PostgreSQL DataRow for (INT8, INT4):

┌─────────────────────────────┬─────────────────────────┐
│  LengthPrefixed<I64BE>      │  LengthPrefixed<I32BE>  │
├──────────┬──────────────────┼──────────┬──────────────┤
│ len (4B) │ value (8B)       │ len (4B) │ value (4B)   │
│ 00000008 │ 0102030405060708 │ 00000004 │ 12345678     │
└──────────┴──────────────────┴──────────┴──────────────┘
```

The struct layout matches this wire format exactly, enabling direct casting.
