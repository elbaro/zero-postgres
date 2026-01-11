# Query

There are two sets of query APIs: Simple Query Protocol and Extended Query Protocol.

## Simple Query Protocol

Simple Protocol supports multiple statements (separated by `;`), but does not support parameter binding.
Use Extended Protocol if you need to send parameters or read typed binary results.

```rust,ignore
impl Conn {
    fn query<H: SimpleHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()>;
    fn query_drop(&mut self, sql: &str) -> Result<Option<u64>>;
    fn query_first<Row>(&mut self, sql: &str) -> Result<Option<Row>>;
    fn query_collect<Row>(&mut self, sql: &str) -> Result<Vec<Row>>;
    fn query_foreach<Row, F>(&mut self, sql: &str, f: F) -> Result<()>;
}
```

- `query`: execute SQL and process results with a handler
- `query_drop`: execute SQL and discard results, returning rows affected
- `query_first`: execute and return `Option<Row>` for the first row
- `query_collect`: execute and collect all rows into a Vec
- `query_foreach`: execute and call a closure for each row

### Example

```rust,ignore
// Execute and discard results
conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;

// Collect all rows
let users: Vec<(i32, String)> = conn.query_collect("SELECT id, name FROM users")?;

// Get first row only
let user: Option<(i32, String)> = conn.query_first("SELECT id, name FROM users LIMIT 1")?;

// Process rows one by one
conn.query_foreach("SELECT id, name FROM users", |row: (i32, String)| {
    println!("{}: {}", row.0, row.1);
})?;
```

## Extended Query Protocol

Extended Protocol uses a prepared statement and parameter binding. Use `$1`, `$2`, ..`$N` as placeholders.
Postgres does not support multiple statements in a prepared statement.

```rust,ignore
impl Conn {
    fn prepare(&mut self, sql: &str) -> Result<PreparedStatement>;
    fn prepare_typed(&mut self, sql: &str, param_oids: &[u32]) -> Result<PreparedStatement>;
    fn prepare_batch(&mut self, queries: &[&str]) -> Result<Vec<PreparedStatement>>;
    fn exec<S, P, H>(&mut self, statement: S, params: P, handler: &mut H) -> Result<()>;
    fn exec_drop<S, P>(&mut self, statement: S, params: P) -> Result<Option<u64>>;
    fn exec_first<Row, S, P>(&mut self, statement: S, params: P) -> Result<Option<Row>>;
    fn exec_collect<Row, S, P>(&mut self, statement: S, params: P) -> Result<Vec<Row>>;
    fn exec_foreach<Row, S, P, F>(&mut self, statement: S, params: P, f: F) -> Result<()>;
    fn exec_batch<S, P>(&mut self, statement: S, params_list: &[P]) -> Result<()>;
    fn exec_portal<S, P, F, T>(&mut self, statement: S, params: P, f: F) -> Result<T>;
    fn close_statement(&mut self, stmt: &PreparedStatement) -> Result<()>;
}
```

- `prepare`: prepare a statement for execution
- `prepare_typed`: prepare with explicit parameter OIDs
- `prepare_batch`: prepare multiple statements in one round-trip
- `exec`: execute with a custom handler
- `exec_drop`: execute and discard results, returning rows affected
- `exec_first`: execute and return `Option<Row>` for the first row
- `exec_collect`: execute and collect all rows into a Vec
- `exec_foreach`: execute and call a closure for each row
- `exec_batch`: execute with multiple parameter sets efficiently
- `exec_portal`: execute to create an unnamed portal that incrementally fetch rows
- `close_statement`: close a prepared statement

The `statement` parameter can be either:
- A `&PreparedStatement` returned from `prepare()`
- A raw SQL `&str` for one-shot execution (parsed once per call)

### Example: Basic

```rust,ignore
// Using prepared statement
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
let user: Option<(i32, String)> = conn.exec_first(&stmt, (42,))?;

// Using raw SQL
let user: Option<(i32, String)> = conn.exec_first(
    "SELECT * FROM users WHERE id = $1",
    (42,)
)?;

// Process rows one by one
conn.exec_foreach(&stmt, (42,), |row: (i32, String)| {
    println!("{}: {}", row.0, row.1);
    Ok(())
})?;
```

### Example: Batch Execution

`exec_batch` sends all parameter sets without waiting for the response for the already-sent parameter sets:

```rust,ignore
let stmt = conn.prepare("INSERT INTO users (name, age) VALUES ($1, $2)")?;

conn.exec_batch(&stmt, &[
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35),
])?;
```

### Example: Portal-based Iteration

For large result sets, use `exec_portal` to fetch rows incrementally.
This is useful to save memory.

```rust,ignore
let stmt = conn.prepare("SELECT * FROM large_table")?;

conn.exec_portal(&stmt, (), |portal| { // Postgres Server holds the unnamed portal
    loop {
        let rows: Vec<(i32, String)> = portal.fetch_collect(100)?; // ask the server to send the next 100 rows
        if rows.is_empty() {
            break;
        }
        for row in rows {
            process(row);
        }
    }
    Ok(())
})?;
```

## Parameters

A parameter set (`Params`) is a tuple of primitives.

```rust,ignore
exec_drop(sql, ()) // no parameter
exec_drop("SELECT $1, $2, $3", (1, 2.0, "String")) // bind 3 parameters
```

## Struct Mapping

There are two ways to map database rows to Rust structs.

### Using `#[derive(FromRow)]`

The `FromRow` derive macro automatically maps columns to struct fields by name.

```rust,ignore
use zero_postgres::r#macro::FromRow;

#[derive(FromRow)]
struct User {
    id: i32,
    name: String,
    email: Option<String>,
}

let stmt = conn.prepare("SELECT name, id, email_address as email FROM users")?;

// Collect all rows
let users: Vec<User> = conn.exec_collect(&stmt, ())?;

// Get first row only
let user: Option<User> = conn.exec_first(&stmt, ())?;

// Process rows one by one
conn.exec_foreach(&stmt, (), |user: User| {
    println!("{}: {}", user.id, user.name);
    Ok(())
})?;
```


Use `#[from_row(strict)]` to error on unknown columns:

```rust,ignore
#[derive(FromRow)]
#[from_row(strict)]
struct User {
    id: i32,
    name: String,
}

// Returns Error if query because `email` column is unknown
let user: Option<User> = conn.exec_first("SELECT id, name, email FROM users");
```

### Manual Construction with `exec_foreach`

This has an advantage of reusing `Vec` and being slightly faster because it does not match the column names against struct field names.

```rust,ignore
struct User {
    id: i32,
    name: String,
    display_name: String, // computed field
}

let stmt = conn.prepare("SELECT id, name FROM users")?;
let mut users = Vec::new();

conn.exec_foreach(&stmt, (), |row: (i32, String)| {
    users.push(User {
        id: row.0,
        display_name: format!("User: {}", row.1),
        name: row.1,
    });
    Ok(())
})?;
```
