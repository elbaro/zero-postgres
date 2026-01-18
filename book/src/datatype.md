# Data Type

The library intentionally rejects conversions that could silently lose data. For example, reading an `INT8` (bigint) column as `i16` will return an error rather than truncating the value. This ensures data integrity and makes bugs easier to catch.

Widening conversions (e.g., reading `INT2` as `i64`) are allowed.

## Parameter Types (Rust to PostgreSQL)

| Rust Type | PostgreSQL Type | Notes |
|-----------|-----------------|-------|
| `bool` | `BOOL` | |
| `i8` | `INT2` | PostgreSQL has no `INT1` |
| `i16` | `INT2` | |
| `i32` | `INT4` | |
| `i64` | `INT8` | |
| `u8` | `INT2` | PostgreSQL has no unsigned types |
| `u16` | `INT4` | `u16` max exceeds `INT2` max |
| `u32` | `INT8` | `u32` max exceeds `INT4` max |
| `u64` | `INT8` | Overflow checked at runtime |
| `f32` | `FLOAT4` | |
| `f64` | `FLOAT8` | |
| `&str` | `TEXT` | Also works with `VARCHAR`, `CHAR`, `JSON`, `JSONB` |
| `String` | `TEXT` | |
| `&[u8]` | `BYTEA` | |
| `Vec<u8>` | `BYTEA` | |
| `Option<T>` | Same as `T` | `None` encodes as `NULL` |

### Unsigned Integer Encoding

Since PostgreSQL doesn't have unsigned integer types, unsigned Rust types are encoded into the smallest signed type that can represent all possible values:

- `u8` → `INT2` (always fits)
- `u16` → `INT4` (since `u16` max > `i16` max)
- `u32` → `INT8` (since `u32` max > `i32` max)
- `u64` → `INT8` with overflow check (fails if value > `i64::MAX`)

### Example

```rust,ignore
let stmt = conn.prepare("INSERT INTO users (name, age, active) VALUES ($1, $2, $3)")?;
conn.exec_drop(&stmt, ("Alice", 30i32, true))?;

// Using Option for nullable columns
conn.exec_drop(&stmt, ("Bob", 25i32, None::<bool>))?;
```

## Result Types (PostgreSQL to Rust)

PostgreSQL only has signed integer types. Decoding to unsigned Rust types (`u8`, `u16`, `u32`, `u64`) is not supported.

| PostgreSQL Type | Rust Types |
|-----------------|------------|
| `BOOL` | `bool` |
| `INT2` | `i16`, `i32`, `i64` |
| `INT4` | `i32`, `i64` |
| `INT8` | `i64` |
| `FLOAT4` | `f32`, `f64` |
| `FLOAT8` | `f64` |
| `NUMERIC` | `f32`, `f64` |
| `TEXT`, `VARCHAR`, `CHAR(n)`, `NAME` | `&str`, `String` |
| `BYTEA` | `&[u8]`, `Vec<u8>` |
| `NULL` | `Option<T>` |

### Example

```rust,ignore
// Reading exact types
let (id, name): (i64, String) = conn.exec_first(&stmt, ())?;

// Widening conversion: INT2 -> i64 is allowed
let count: i64 = conn.exec_first(&stmt, ())?;

// Using Option for nullable columns
let email: Option<String> = conn.exec_first(&stmt, ())?;
```

## Conversion Errors

When a conversion is not allowed, you'll get a clear error message:

```rust,ignore
// This will fail with an error like:
// "cannot decode oid 20 as i16" (OID 20 = INT8)
let value: i16 = conn.exec_first(&stmt, ())?;
```

## Feature-Gated Types

Additional type support is available through feature flags.

### `with-chrono` (chrono crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `chrono::NaiveDate` | `DATE` |
| `chrono::NaiveTime` | `TIME` |
| `chrono::NaiveDateTime` | `TIMESTAMP`, `TIMESTAMPTZ` |
| `chrono::DateTime<Utc>` | `TIMESTAMPTZ` |

```rust,ignore
use chrono::{NaiveDate, DateTime, Utc};

// Reading dates
let date: NaiveDate = conn.exec_first(&stmt, ())?;

// Reading timestamps with timezone
let created_at: DateTime<Utc> = conn.exec_first(&stmt, ())?;
```

### `with-time` (time crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `time::Date` | `DATE` |
| `time::Time` | `TIME` |
| `time::PrimitiveDateTime` | `TIMESTAMP`, `TIMESTAMPTZ` |
| `time::OffsetDateTime` | `TIMESTAMPTZ` |

```rust,ignore
use time::{Date, OffsetDateTime};

// Reading dates
let date: Date = conn.exec_first(&stmt, ())?;

// Reading timestamps with timezone
let created_at: OffsetDateTime = conn.exec_first(&stmt, ())?;
```

### `with-uuid` (uuid crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `uuid::Uuid` | `UUID` |

```rust,ignore
use uuid::Uuid;

let id: Uuid = conn.exec_first(&stmt, ())?;
```

### `with-rust-decimal` (rust_decimal crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `rust_decimal::Decimal` | `NUMERIC` |

```rust,ignore
use rust_decimal::Decimal;

// Full precision decimal arithmetic
let price: Decimal = conn.exec_first(&stmt, ())?;
```

Note: `NUMERIC` special values (`NaN`, `Infinity`, `-Infinity`) cannot be represented by `rust_decimal::Decimal` and will return an error.

## NUMERIC Encoding

When encoding `f32`, `f64`, or `rust_decimal::Decimal` as `NUMERIC`, text format is used for parameters. This is because:

1. PostgreSQL's binary NUMERIC format is complex (base-10000 encoding)
2. Text format is equally efficient (server parses quickly)
3. Text format preserves full precision through string representation

Reading `NUMERIC` values works in both text (simple query) and binary (extended query) formats.
