# Data Type

The library intentionally rejects conversions that could silently lose data. For example, reading a `BIGINT` column as `i8` will return an error rather than truncating the value. This ensures data integrity and makes bugs easier to catch.

Widening conversions (e.g., reading `SMALLINT` as `i64`) are allowed.

## Parameter Types (Rust to PostgreSQL)

| Rust Type | PostgreSQL Type | Notes |
|-----------|-----------------|-------|
| `bool` | `BOOL` (`BOOLEAN`) | |
| `i8` | `INT2` (`SMALLINT`) | PostgreSQL has no 1-byte integer |
| `i16` | `INT2` (`SMALLINT`) | |
| `i32` | `INT4` (`INTEGER`) | |
| `i64` | `INT8` (`BIGINT`) | |
| `u8` | `INT2` (`SMALLINT`) | PostgreSQL has no unsigned types |
| `u16` | `INT4` (`INTEGER`) | `u16` max exceeds `INT2` max |
| `u32` | `INT8` (`BIGINT`) | `u32` max exceeds `INT4` max |
| `u64` | `INT8` (`BIGINT`) | Overflow checked at runtime |
| `f32` | `FLOAT4` (`REAL`) | |
| `f64` | `FLOAT8` (`DOUBLE PRECISION`) | |
| `&str` | `TEXT` | Also works with `VARCHAR`, `CHAR`, `JSON`, `JSONB` |
| `String` | `TEXT` | |
| `&[u8]` | `BYTEA` | |
| `Vec<u8>` | `BYTEA` | |
| `Option<T>` | Same as `T` | `None` encodes as `NULL` |

## Result Types (PostgreSQL to Rust)

PostgreSQL only has signed integer types. Decoding to unsigned Rust types (`u8`, `u16`, `u32`, `u64`) is not supported.

| PostgreSQL Type | Rust Types |
|-----------------|------------|
| `BOOL` (`BOOLEAN`) | `bool` |
| `INT2` (`SMALLINT`) | `i16`, `i32`, `i64` |
| `INT4` (`INTEGER`) | `i32`, `i64` |
| `INT8` (`BIGINT`) | `i64` |
| `FLOAT4` (`REAL`) | `f32`, `f64` |
| `FLOAT8` (`DOUBLE PRECISION`) | `f64` |
| `NUMERIC` | `f32`, `f64` |
| `TEXT`, `VARCHAR`, `CHAR(n)`, `NAME` | `&str`, `String` |
| `BYTEA` | `&[u8]`, `Vec<u8>` |
| `NULL` | `Option<T>` |

## Feature-Gated Types

Additional type support is available through feature flags.

### `with-chrono` (chrono crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `chrono::NaiveDate` | `DATE` |
| `chrono::NaiveTime` | `TIME` |
| `chrono::NaiveDateTime` | `TIMESTAMP`, `TIMESTAMPTZ` |
| `chrono::DateTime<Utc>` | `TIMESTAMPTZ` |

| PostgreSQL Type | Rust Types |
|-----------------|------------|
| `DATE` | `chrono::NaiveDate` |
| `TIME` | `chrono::NaiveTime` |
| `TIMESTAMP` | `chrono::NaiveDateTime` |
| `TIMESTAMPTZ` | `chrono::NaiveDateTime`, `chrono::DateTime<Utc>` |

### `with-time` (time crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `time::Date` | `DATE` |
| `time::Time` | `TIME` |
| `time::PrimitiveDateTime` | `TIMESTAMP`, `TIMESTAMPTZ` |
| `time::OffsetDateTime` | `TIMESTAMPTZ` |

| PostgreSQL Type | Rust Types |
|-----------------|------------|
| `DATE` | `time::Date` |
| `TIME` | `time::Time` |
| `TIMESTAMP` | `time::PrimitiveDateTime` |
| `TIMESTAMPTZ` | `time::PrimitiveDateTime`, `time::OffsetDateTime` |

### `with-uuid` (uuid crate)

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `uuid::Uuid` | `UUID` |

| PostgreSQL Type | Rust Types |
|-----------------|------------|
| `UUID` | `uuid::Uuid` |

### `with-rust-decimal` (rust_decimal crate)

`rust_decimal::Decimal` uses 96-bit precision, not arbitrary precision like PostgreSQL's `NUMERIC`.

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `rust_decimal::Decimal` | `NUMERIC` |

| PostgreSQL Type | Rust Types |
|-----------------|------------|
| `NUMERIC` | `rust_decimal::Decimal` |
