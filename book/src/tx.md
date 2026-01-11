# Transaction

Transactions ensure a group of operations either all succeed (commit) or all fail (rollback).

## Using Transactions

```rust,ignore
use zero_postgres::sync::Conn;

conn.transaction(|conn, _tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))?;
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Bob",))?;
    Ok(())
})?;
```

If the closure returns `Ok`, the transaction is automatically committed. If the closure returns `Err`, the transaction is automatically rolled back.

## Automatic Rollback on Error

```rust,ignore
conn.transaction(|conn, _tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))?;
    // Returns error - transaction will be rolled back
    Err(Error::InvalidUsage("oops".to_string()))
})?;
// No data inserted
```

## Explicit Commit/Rollback

Use `tx.commit()` or `tx.rollback()` for explicit control:

```rust,ignore
conn.transaction(|conn, tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))?;

    if some_condition {
        tx.commit(conn)
    } else {
        tx.rollback(conn)
    }
})?;
```

## Nested Transactions

Nested transactions are not supported. Calling `transaction` while already in a transaction returns `Error::NestedTransaction`.

## Async Transactions

For async connections, use async closures:

```rust,ignore
use zero_postgres::tokio::Conn;

conn.transaction(async |conn, _tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",)).await?;
    Ok(())
}).await?;
```
