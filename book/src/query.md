# Query

## Simple Queries

```rust
// Query returning rows
let rows = conn.query("SELECT id, name FROM users")?;
for row in rows {
    let id: i32 = row.get(0)?;
    let name: String = row.get(1)?;
    println!("{}: {}", id, name);
}

// Execute without returning rows
conn.exec_drop("DELETE FROM old_users")?;
```

## Parameterized Queries

```rust
// With parameters (prevents SQL injection)
let rows = conn.query_bind(
    "SELECT * FROM users WHERE id = $1 AND status = $2",
    &[&42i32, &"active"]
)?;

// Get first row only
let row = conn.query_first_bind(
    "SELECT * FROM users WHERE id = $1",
    &[&42i32]
)?;
```

## Prepared Statements

```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
let rows = conn.query_prepared(&stmt, &[&42i32])?;
```
