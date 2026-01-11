# Pipelining

Pipelining allows you to send multiple queries to the server without waiting for the response of each query. This reduces round-trip latency and improves throughput.

## Basic Pipelining

```rust,ignore
// Prepare statements outside the pipeline
let stmts = conn.prepare_batch(&[
    "SELECT id, name FROM users WHERE active = $1",
    "SELECT COUNT(*) FROM users",
])?;

let (active_users, count) = conn.pipeline(|p| {
    // Queue executions
    let t1 = p.exec(&stmts[0], (true,))?;
    let t2 = p.exec(&stmts[1], ())?;

    // Sync sends all queued operations
    p.sync()?;

    // Claim results in order
    let active_users: Vec<(i32, String)> = p.claim_collect(t1)?;
    let count: Vec<(i64,)> = p.claim_collect(t2)?;

    Ok((active_users, count))
})?;
```

## Pipeline Benefits

- **Reduced Latency**: Multiple queries are sent in a single network round trip
- **Higher Throughput**: Server can process queries while client sends more
- **Batch Operations**: Efficient for bulk inserts or updates
