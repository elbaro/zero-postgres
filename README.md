# zero-postgres

A high-performance PostgreSQL client library for Rust.


## Feature flags

- `sync` (default) - Synchronous API
- `tokio` (default) - Asynchronous API using tokio
- `sync-tls` - TLS support for sync
- `tokio-tls` - TLS support for tokio


## Benchmark

Inserting 10,000 rows using prepared statements (average of 10 iterations):

| Library | Avg Time (ms) |
|---------|---------------|
| zero-postgres (sync) | 250.35 |
| zero-postgres (async) | 269.32 |
| tokio-postgres | 398.90 |
| postgres (sync) | 422.61 |

Run benchmarks: `cargo run --release --example bench_zero_sync`

More tests and proper benchmarks will be added via pyro-postgres.

## Status

- todo: pipelining, more type handling
