# Transaction

## Basic Transaction

```rust
conn.begin()?;

conn.exec_drop_bind("INSERT INTO users (name) VALUES ($1)", &[&"Alice"])?;
conn.exec_drop_bind("INSERT INTO users (name) VALUES ($1)", &[&"Bob"])?;

conn.commit()?;
```

## Rollback

```rust
conn.begin()?;

conn.exec_drop_bind("INSERT INTO users (name) VALUES ($1)", &[&"Alice"])?;

// Something went wrong, rollback
conn.rollback()?;
```

## Async Transaction

```rust
conn.begin().await?;

conn.exec_drop_bind("INSERT INTO users (name) VALUES ($1)", &[&"Alice"]).await?;

conn.commit().await?;
```
