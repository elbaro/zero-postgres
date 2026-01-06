# Connection

## Synchronous Connection

```rust
use zero_postgres::sync::Conn;

// Connect using a URL
let mut conn = Conn::new("postgres://user:password@localhost/mydb")?;

// With options
let mut conn = Conn::new("postgres://user:password@localhost/mydb?application_name=myapp")?;
```

## Asynchronous Connection

```rust
use zero_postgres::tokio::Conn;

// Connect using a URL
let mut conn = Conn::new("postgres://user:password@localhost/mydb").await?;
```

## Connection Pool

```rust
use zero_postgres::sync::Pool;

let pool = Pool::new("postgres://user:password@localhost/mydb", 10)?;
let mut conn = pool.get()?;

// Use connection...
// Connection is returned to pool when dropped
```
