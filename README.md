# zero-postgres

A high-performance PostgreSQL client library for Rust.

[API Reference (docs.rs)](https://docs.rs/zero-postgres) | [User Guide](https://elbaro.github.io/zero-postgres/)

Python binding: [pyro-postgres](https://github.com/elbaro/pyro-postgres/).


## Feature Flags

- `sync` (default) - Synchronous API
- `tokio` (default) - Asynchronous API using tokio
- `sync-tls` - TLS support for Synchronous API (experimental)
- `tokio-tls` - TLS support for Asynchronous API (experimental)
- `experimental-compio` - Experimental feature flag reserved for future compio runtime support

## Benchmark

Inserting 10,000 rows using prepared statements (average of 10 iterations):

| Library               | Avg Time (ms) |
| --------------------- | ------------- |
| zero-postgres (sync)  | 250.35        |
| zero-postgres (async) | 269.32        |
| tokio-postgres        | 398.90        |
| postgres (sync)       | 422.61        |

Run benchmarks: `cargo run --release --example bench_zero_sync`

More tests and proper benchmarks will be added via pyro-postgres.

## Status

- todo: cleanup, more tests, more data conversion
