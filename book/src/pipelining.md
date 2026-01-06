# Pipelining

Pipelining allows you to send multiple queries to the server without waiting for the response of each query. This reduces round-trip latency and improves throughput.

## Basic Pipelining

```rust
use zero_postgres::sync::Pipeline;

let mut pipeline = conn.pipeline();

pipeline.query_bind("SELECT * FROM users WHERE id = $1", &[&1i32]);
pipeline.query_bind("SELECT * FROM users WHERE id = $1", &[&2i32]);
pipeline.query_bind("SELECT * FROM users WHERE id = $1", &[&3i32]);

let results = pipeline.execute()?;
```

## Pipeline Benefits

- **Reduced Latency**: Multiple queries are sent in a single network round trip
- **Higher Throughput**: Server can process queries while client sends more
- **Batch Operations**: Efficient for bulk inserts or updates
