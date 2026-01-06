//! Tests for FromRow derive macro.
//!
//! Run with: cargo test --features derive --test derive

#![allow(dead_code)]

use std::env;
use zero_postgres::r#macro::FromRow;
use zero_postgres::sync::Conn;

fn get_conn() -> Conn {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    Conn::new(db_url.as_str()).expect("connect")
}

// ============================================================================
// Struct definitions
// ============================================================================

#[derive(Debug, PartialEq, FromRow)]
struct User {
    id: i64,
    name: String,
    age: i32,
}

#[derive(Debug, PartialEq, FromRow)]
struct UserWithOptional {
    id: i64,
    name: String,
    email: Option<String>,
}

#[derive(Debug, PartialEq, FromRow)]
#[from_row(strict)]
struct StrictUser {
    id: i64,
    name: String,
}

#[derive(Debug, PartialEq, FromRow)]
struct IntTypes {
    tiny: i16,
    small: i16,
    medium: i32,
    big: i64,
}

#[derive(Debug, PartialEq, FromRow)]
struct FloatTypes {
    float_val: f32,
    double_val: f64,
}

#[derive(Debug, PartialEq, FromRow)]
struct PartialUser {
    name: String,
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_exec_collect_basic() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_users")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_users (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_users (id, name, age) VALUES ($1, $2, $3)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))
        .expect("insert");
    conn.exec_drop(&stmt, (2i64, "Bob", 30i32)).expect("insert");

    let stmt = conn
        .prepare("SELECT id, name, age FROM test_derive_users ORDER BY id")
        .expect("prepare");
    let users: Vec<User> = conn.exec_collect(&stmt, ()).expect("select");

    assert_eq!(users.len(), 2);
    assert_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );
    assert_eq!(
        users[1],
        User {
            id: 2,
            name: "Bob".to_string(),
            age: 30
        }
    );

    conn.query_drop("DROP TABLE test_derive_users")
        .expect("cleanup");
}

#[test]
fn test_exec_first() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_first")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_first (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_first (id, name, age) VALUES ($1, $2, $3)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))
        .expect("insert");

    let stmt = conn
        .prepare("SELECT id, name, age FROM test_derive_first WHERE id = $1")
        .expect("prepare");

    let user: Option<User> = conn.exec_first(&stmt, (1i64,)).expect("select");
    assert_eq!(
        user,
        Some(User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        })
    );

    let user: Option<User> = conn.exec_first(&stmt, (999i64,)).expect("select");
    assert_eq!(user, None);

    conn.query_drop("DROP TABLE test_derive_first")
        .expect("cleanup");
}

#[test]
fn test_exec_foreach() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_foreach")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_foreach (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_foreach (id, name, age) VALUES ($1, $2, $3)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))
        .expect("insert");
    conn.exec_drop(&stmt, (2i64, "Bob", 30i32)).expect("insert");

    let stmt = conn
        .prepare("SELECT id, name, age FROM test_derive_foreach ORDER BY id")
        .expect("prepare");

    let mut names = Vec::new();
    conn.exec_foreach(&stmt, (), |user: User| {
        names.push(user.name);
    })
    .expect("foreach");

    assert_eq!(names, vec!["Alice", "Bob"]);

    conn.query_drop("DROP TABLE test_derive_foreach")
        .expect("cleanup");
}

#[test]
fn test_optional_field() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_optional")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_optional (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255)
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_optional (id, name, email) VALUES ($1, $2, $3)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", Some("alice@example.com")))
        .expect("insert");
    conn.exec_drop(&stmt, (2i64, "Bob", None::<String>))
        .expect("insert");

    let stmt = conn
        .prepare("SELECT id, name, email FROM test_derive_optional ORDER BY id")
        .expect("prepare");
    let users: Vec<UserWithOptional> = conn.exec_collect(&stmt, ()).expect("select");

    assert_eq!(users[0].email, Some("alice@example.com".to_string()));
    assert_eq!(users[1].email, None);

    conn.query_drop("DROP TABLE test_derive_optional")
        .expect("cleanup");
}

#[test]
fn test_skip_unknown_columns() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_skip")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_skip (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL,
            extra_column VARCHAR(255)
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare(
            "INSERT INTO test_derive_skip (id, name, age, extra_column) VALUES ($1, $2, $3, $4)",
        )
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32, "ignored"))
        .expect("insert");

    // Select all columns including extra_column, but PartialUser only has 'name'
    let stmt = conn
        .prepare("SELECT id, name, age, extra_column FROM test_derive_skip")
        .expect("prepare");
    let users: Vec<PartialUser> = conn.exec_collect(&stmt, ()).expect("select");

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].name, "Alice");

    conn.query_drop("DROP TABLE test_derive_skip")
        .expect("cleanup");
}

#[test]
fn test_strict_mode_unknown_column() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_strict")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_strict (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            extra VARCHAR(255)
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_strict (id, name, extra) VALUES ($1, $2, $3)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", "extra"))
        .expect("insert");

    // StrictUser expects only id and name, but we're selecting extra too
    let stmt = conn
        .prepare("SELECT id, name, extra FROM test_derive_strict")
        .expect("prepare");
    let result: Result<Vec<StrictUser>, _> = conn.exec_collect(&stmt, ());

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("unknown column"));

    conn.query_drop("DROP TABLE test_derive_strict")
        .expect("cleanup");
}

#[test]
fn test_missing_column() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_missing")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_missing (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_missing (id, name) VALUES ($1, $2)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice")).expect("insert");

    // User expects id, name, age - but age is not in the result
    let stmt = conn
        .prepare("SELECT id, name FROM test_derive_missing")
        .expect("prepare");
    let result: Result<Vec<User>, _> = conn.exec_collect(&stmt, ());

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("missing column"));

    conn.query_drop("DROP TABLE test_derive_missing")
        .expect("cleanup");
}

#[test]
fn test_column_order_independence() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_order")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_order (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_order (id, name, age) VALUES ($1, $2, $3)")
        .expect("prepare");
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))
        .expect("insert");

    // Select columns in different order than struct definition
    let stmt = conn
        .prepare("SELECT age, id, name FROM test_derive_order")
        .expect("prepare");
    let users: Vec<User> = conn.exec_collect(&stmt, ()).expect("select");

    assert_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );

    conn.query_drop("DROP TABLE test_derive_order")
        .expect("cleanup");
}

#[test]
fn test_int_types() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_ints")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_ints (
            tiny SMALLINT,
            small SMALLINT,
            medium INT,
            big BIGINT
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_ints (tiny, small, medium, big) VALUES ($1, $2, $3, $4)")
        .expect("prepare");
    conn.exec_drop(&stmt, (-1i16, -100i16, -10000i32, -1000000i64))
        .expect("insert");

    let stmt = conn
        .prepare("SELECT tiny, small, medium, big FROM test_derive_ints")
        .expect("prepare");
    let rows: Vec<IntTypes> = conn.exec_collect(&stmt, ()).expect("select");

    assert_eq!(
        rows[0],
        IntTypes {
            tiny: -1,
            small: -100,
            medium: -10000,
            big: -1000000
        }
    );

    conn.query_drop("DROP TABLE test_derive_ints")
        .expect("cleanup");
}

#[test]
fn test_float_types() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_floats")
        .expect("drop");
    conn.query_drop(
        "CREATE TABLE test_derive_floats (
            float_val REAL,
            double_val DOUBLE PRECISION
        )",
    )
    .expect("create");

    let stmt = conn
        .prepare("INSERT INTO test_derive_floats (float_val, double_val) VALUES ($1, $2)")
        .expect("prepare");
    conn.exec_drop(&stmt, (3.14f32, 2.71828f64))
        .expect("insert");

    let stmt = conn
        .prepare("SELECT float_val, double_val FROM test_derive_floats")
        .expect("prepare");
    let rows: Vec<FloatTypes> = conn.exec_collect(&stmt, ()).expect("select");

    assert!((rows[0].float_val - 3.14).abs() < 0.001);
    assert!((rows[0].double_val - 2.71828).abs() < 0.00001);

    conn.query_drop("DROP TABLE test_derive_floats")
        .expect("cleanup");
}
