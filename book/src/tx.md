# Transaction

## Basic Transaction

```rust,ignore
conn.transaction(|conn, tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))?;
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Bob",))?;

    tx.commit(conn)
})?;
```

## Rollback

```rust,ignore
conn.transaction(|conn, tx| {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))?;

    // Something went wrong, rollback
    tx.rollback(conn)
})?;
```

If the closure returns `Err` or the transaction is not explicitly committed or rolled back, the transaction is automatically rolled back.

## Async Transaction

```rust,ignore
conn.transaction(|conn, tx| async move {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",)).await?;

    tx.commit(conn).await
}).await?;
```
