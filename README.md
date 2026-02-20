# zero-postgres

A high-performance PostgreSQL client library for Rust.

[API Reference (docs.rs)](https://docs.rs/zero-postgres) | [User Guide](https://elbaro.github.io/zero-postgres/)

Python binding: [pyro-postgres](https://github.com/elbaro/pyro-postgres/).


## Feature Flags

- `derive` (default): `#[derive(FromRow)]` and `#[derive(RefFromRow)]` macros
- `sync` (default) - Synchronous API
- `tokio` (default) - Asynchronous API using tokio
- `compio` - Asynchronous API using compio (experimental)
- `sync-tls` - TLS support for Synchronous API (experimental)
- `tokio-tls` - TLS support for tokio (experimental)
- `compio-tls` - TLS support for compio (experimental)
- `diesel` - Diesel support (experimental)

TLS flags currently use `native-tls`.

[External type supports](https://elbaro.github.io/zero-postgres/datatype.html#feature-gated-types):
- `with-chrono` - Support [chrono](https://crates.io/crates/chrono) date/time types
- `with-time` - Support [time](https://crates.io/crates/time) date/time types
- `with-uuid` - Support [uuid](https://crates.io/crates/uuid) types
- `with-rust-decimal` - Support [rust_decimal](https://crates.io/crates/rust_decimal) types

## Benchmark

Inserting 10,000 rows using prepared statements (average of 10 iterations):

| Library               | Avg Time (ms) |
| --------------------- | ------------- |
| zero-postgres (sync)  | 250.35        |
| zero-postgres (async) | 269.32        |
| tokio-postgres        | 398.90        |
| postgres (sync)       | 422.61        |

Run benchmarks: `cargo run --release --example bench_zero_sync`

See also [Diesel Benchmarks](https://github.com/diesel-rs/metrics).
