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

If no explicit commit or rollback is called, the transaction is automatically committed on `Ok` and rolled back on `Err`.

## Async Transaction

```rust,ignore
conn.transaction(|conn, tx| async move {
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",)).await?;

    tx.commit(conn).await
}).await?;
```
