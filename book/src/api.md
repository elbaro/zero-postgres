# API Reference

For complete API documentation, see [docs.rs/zero-postgres](https://docs.rs/zero-postgres).

## Modules

- `zero_postgres::sync` - Synchronous API
- `zero_postgres::tokio` - Asynchronous API using tokio
- `zero_postgres::types` - PostgreSQL type conversions

## Main Types

### Sync

- `Conn` - Synchronous database connection
- `Pool` - Connection pool
- `Pipeline` - Pipelined query execution

### Async (tokio)

- `Conn` - Asynchronous database connection
- `Pipeline` - Pipelined query execution
