# Query

There are two sets of query APIs: Simple Query Protocol and Extended Query Protocol.

## Simple Query Protocol

Simple queries use text format and support multiple statements separated by `;`, but do not support parameter binding.
Use the extended query protocol if you need to send parameters or read typed binary results.

```rust,ignore
impl Conn {
    fn query<H: TextHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()>;
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

Extended queries use prepared statements with binary format and parameter binding. Use `$1`, `$2`, etc. as placeholders.

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
- `exec_portal`: execute with iterative row fetching (portal-based)
- `close_statement`: close a prepared statement

The `statement` parameter can be either:
- A `&PreparedStatement` returned from `prepare()`
- A raw SQL `&str` for one-shot execution (parsed once per call)

### Example: Basic

```rust,ignore
// Using prepared statement (reusable)
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
let user: Option<(i32, String)> = conn.exec_first(&stmt, (42,))?;

// Using raw SQL (one-shot)
let user: Option<(i32, String)> = conn.exec_first(
    "SELECT * FROM users WHERE id = $1",
    (42,)
)?;

// Process rows one by one
conn.exec_foreach(&stmt, (42,), |row: (i32, String)| {
    println!("{}: {}", row.0, row.1);
})?;
```

### Example: Batch Execution

Batch execution sends multiple parameter sets efficiently in a single transaction:

```rust,ignore
let stmt = conn.prepare("INSERT INTO users (name, age) VALUES ($1, $2)")?;

conn.exec_batch(&stmt, &[
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35),
])?;
```

### Example: Portal-based Iteration

For large result sets, use `exec_portal` to fetch rows in batches:

```rust,ignore
let stmt = conn.prepare("SELECT * FROM large_table")?;

conn.exec_portal(&stmt, (), |portal| {
    loop {
        let rows: Vec<(i32, String)> = portal.fetch_collect(100)?;
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

## Statement Caching

Prepared statements are cached per connection. After calling `prepare()`, reuse the `PreparedStatement` for subsequent executions.

```rust,ignore
// Prepare once
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;

// Reuse multiple times
let user1: Option<(i32, String)> = conn.exec_first(&stmt, (1,))?;
let user2: Option<(i32, String)> = conn.exec_first(&stmt, (2,))?;
let user3: Option<(i32, String)> = conn.exec_first(&stmt, (3,))?;
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

let stmt = conn.prepare("SELECT id, name, email FROM users")?;

// Collect all rows
let users: Vec<User> = conn.exec_collect(&stmt, ())?;

// Get first row only
let user: Option<User> = conn.exec_first(&stmt, ())?;

// Process rows one by one
conn.exec_foreach(&stmt, (), |user: User| {
    println!("{}: {}", user.id, user.name);
})?;
```

Features:
- **Column order independence**: Columns are matched by name, not position
- **Optional fields**: Use `Option<T>` for nullable columns
- **Skip unknown columns**: Extra columns in the result set are ignored by default

Use `#[from_row(strict)]` to error on unknown columns:

```rust,ignore
#[derive(FromRow)]
#[from_row(strict)]
struct User {
    id: i32,
    name: String,
}

// Errors if query returns columns other than `id` and `name`
```

### Manual Construction with `exec_foreach`

For custom logic or computed fields:

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
})?;
```

## Result Handlers

zero-postgres uses a handler pattern for processing results. Implement `TextHandler` or `BinaryHandler` to customize how rows are processed.

Built-in handlers:
- `DropHandler`: Discards all results
- `FirstRowHandler<Row>`: Stores only the first row
- `CollectHandler<Row>`: Collects rows into a Vec
- `ForEachHandler<Row, F>`: Calls a closure for each row
