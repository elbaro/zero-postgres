# Introduction

zero-postgres is a high-performance PostgreSQL client library for Rust. It provides both synchronous and asynchronous APIs with a focus on speed and minimal allocations.

```toml
[dependencies]
zero-postgres = "0.4"
```

## Quick Start

```rust
use zero_postgres::sync::Conn;

let mut conn = Conn::connect("postgres://user:password@localhost/mydb")?;

// Simple query
let rows = conn.query("SELECT id, name FROM users")?;

// Parameterized query
let rows = conn.query_bind("SELECT * FROM users WHERE id = $1", &[&42i32])?;

// Transaction
conn.begin()?;
conn.exec_drop_bind("INSERT INTO users (name) VALUES ($1)", &[&"Alice"])?;
conn.exec_drop_bind("INSERT INTO users (name) VALUES ($1)", &[&"Bob"])?;
conn.commit()?;
```

## Features

- **High Performance**: Minimal allocations and copies
- **Sync and Async**: Both synchronous and async (tokio) APIs
- **Pipelining**: Batch multiple queries in a single round trip
- **Type-safe**: Rust's type system for compile-time safety

## Feature Flags

- `sync` (default) - Synchronous API
- `tokio` (default) - Asynchronous API using tokio
- `sync-tls` - TLS support for Synchronous API (experimental)
- `tokio-tls` - TLS support for Asynchronous API (experimental)

## Benchmark

Inserting 10,000 rows using prepared statements (average of 10 iterations):

| Library               | Avg Time (ms) |
| --------------------- | ------------- |
| zero-postgres (sync)  | 250.35        |
| zero-postgres (async) | 269.32        |
| tokio-postgres        | 398.90        |
| postgres (sync)       | 422.61        |
