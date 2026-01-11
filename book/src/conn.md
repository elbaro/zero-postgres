# Connection

## Synchronous Connection

```rust,ignore
use zero_postgres::sync::Conn;

// Connect using a URL
let mut conn = Conn::new("postgres://user:password@localhost/mydb")?;

// With options
let mut conn = Conn::new("postgres://user:password@localhost/mydb?application_name=myapp")?;
```

## Asynchronous Connection

```rust,ignore
use zero_postgres::tokio::Conn;

// Connect using a URL
let mut conn = Conn::new("postgres://user:password@localhost/mydb").await?;
```

## Connection Pool

```rust,ignore
use std::sync::Arc;
use zero_postgres::sync::Pool;
use zero_postgres::Opts;

let opts: Opts = "postgres://user:password@localhost/mydb".try_into()?;
let pool = Arc::new(Pool::new(opts));
let mut conn = pool.get()?;

// Use connection...
// Connection is returned to pool when dropped
```
