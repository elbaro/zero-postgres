# Introduction

zero-postgres is a high-performance PostgreSQL client library for Rust. It provides both synchronous and asynchronous APIs with a focus on speed and minimal allocations.

```toml
[dependencies]
zero-postgres = "*"
```

## Quick Start

```rust,ignore
use zero_postgres::sync::Conn;

let mut conn = Conn::new("postgres://user:password@localhost/dbname")?;

// Simple query
let rows: Vec<(i32, String)> = conn.query_collect("SELECT id, name FROM users")?;

// Parameterized query
let rows: Vec<(i32, String)> = conn.exec_collect("SELECT * FROM users WHERE id = $1", (42i32,))?;

// Transaction
conn.transaction(|conn, tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))?;
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Bob",))?;
    tx.commit(conn)
})?;
```
