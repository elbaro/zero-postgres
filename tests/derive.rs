//! Tests for FromRow derive macro.
//!
//! Run with: cargo test --features derive --test derive

#![allow(
    dead_code,
    clippy::panic_in_result_fn,
    clippy::shadow_unrelated,
    clippy::unwrap_used
)]

use std::env;
use zero_postgres::Error;
use zero_postgres::r#macro::FromRow;
use zero_postgres::sync::Conn;

fn get_conn() -> Result<Conn, Error> {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str())
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
fn exec_collect_basic() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_users")?;
    conn.query_drop(
        "CREATE TABLE test_derive_users (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )?;

    let stmt = conn.prepare("INSERT INTO test_derive_users (id, name, age) VALUES ($1, $2, $3)")?;
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))?;
    conn.exec_drop(&stmt, (2i64, "Bob", 30i32))?;

    let stmt = conn.prepare("SELECT id, name, age FROM test_derive_users ORDER BY id")?;
    let users: Vec<User> = conn.exec_collect(&stmt, ())?;

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

    conn.query_drop("DROP TABLE test_derive_users")?;
    Ok(())
}

#[test]
fn exec_first() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_first")?;
    conn.query_drop(
        "CREATE TABLE test_derive_first (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )?;

    let stmt = conn.prepare("INSERT INTO test_derive_first (id, name, age) VALUES ($1, $2, $3)")?;
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))?;

    let stmt = conn.prepare("SELECT id, name, age FROM test_derive_first WHERE id = $1")?;

    let user: Option<User> = conn.exec_first(&stmt, (1i64,))?;
    assert_eq!(
        user,
        Some(User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        })
    );

    let user: Option<User> = conn.exec_first(&stmt, (999i64,))?;
    assert_eq!(user, None);

    conn.query_drop("DROP TABLE test_derive_first")?;
    Ok(())
}

#[test]
fn exec_foreach() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_foreach")?;
    conn.query_drop(
        "CREATE TABLE test_derive_foreach (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )?;

    let stmt =
        conn.prepare("INSERT INTO test_derive_foreach (id, name, age) VALUES ($1, $2, $3)")?;
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))?;
    conn.exec_drop(&stmt, (2i64, "Bob", 30i32))?;

    let stmt = conn.prepare("SELECT id, name, age FROM test_derive_foreach ORDER BY id")?;

    let mut names = Vec::new();
    conn.exec_foreach(&stmt, (), |user: User| {
        names.push(user.name);
        Ok(())
    })?;

    assert_eq!(names, vec!["Alice", "Bob"]);

    conn.query_drop("DROP TABLE test_derive_foreach")?;
    Ok(())
}

#[test]
fn optional_field() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_optional")?;
    conn.query_drop(
        "CREATE TABLE test_derive_optional (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255)
        )",
    )?;

    let stmt =
        conn.prepare("INSERT INTO test_derive_optional (id, name, email) VALUES ($1, $2, $3)")?;
    conn.exec_drop(&stmt, (1i64, "Alice", Some("alice@example.com")))?;
    conn.exec_drop(&stmt, (2i64, "Bob", None::<String>))?;

    let stmt = conn.prepare("SELECT id, name, email FROM test_derive_optional ORDER BY id")?;
    let users: Vec<UserWithOptional> = conn.exec_collect(&stmt, ())?;

    assert_eq!(users[0].email, Some("alice@example.com".to_string()));
    assert_eq!(users[1].email, None);

    conn.query_drop("DROP TABLE test_derive_optional")?;
    Ok(())
}

#[test]
fn skip_unknown_columns() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_skip")?;
    conn.query_drop(
        "CREATE TABLE test_derive_skip (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL,
            extra_column VARCHAR(255)
        )",
    )?;

    let stmt = conn.prepare(
        "INSERT INTO test_derive_skip (id, name, age, extra_column) VALUES ($1, $2, $3, $4)",
    )?;
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32, "ignored"))?;

    // Select all columns including extra_column, but PartialUser only has 'name'
    let stmt = conn.prepare("SELECT id, name, age, extra_column FROM test_derive_skip")?;
    let users: Vec<PartialUser> = conn.exec_collect(&stmt, ())?;

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].name, "Alice");

    conn.query_drop("DROP TABLE test_derive_skip")?;
    Ok(())
}

#[test]
fn strict_mode_unknown_column() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_strict")?;
    conn.query_drop(
        "CREATE TABLE test_derive_strict (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            extra VARCHAR(255)
        )",
    )?;

    let stmt =
        conn.prepare("INSERT INTO test_derive_strict (id, name, extra) VALUES ($1, $2, $3)")?;
    conn.exec_drop(&stmt, (1i64, "Alice", "extra"))?;

    // StrictUser expects only id and name, but we're selecting extra too
    let stmt = conn.prepare("SELECT id, name, extra FROM test_derive_strict")?;
    let result: Result<Vec<StrictUser>, _> = conn.exec_collect(&stmt, ());

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("unknown column"));

    conn.query_drop("DROP TABLE test_derive_strict")?;
    Ok(())
}

#[test]
fn missing_column() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_missing")?;
    conn.query_drop(
        "CREATE TABLE test_derive_missing (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )",
    )?;

    let stmt = conn.prepare("INSERT INTO test_derive_missing (id, name) VALUES ($1, $2)")?;
    conn.exec_drop(&stmt, (1i64, "Alice"))?;

    // User expects id, name, age - but age is not in the result
    let stmt = conn.prepare("SELECT id, name FROM test_derive_missing")?;
    let result: Result<Vec<User>, _> = conn.exec_collect(&stmt, ());

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("missing column"));

    conn.query_drop("DROP TABLE test_derive_missing")?;
    Ok(())
}

#[test]
fn column_order_independence() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_order")?;
    conn.query_drop(
        "CREATE TABLE test_derive_order (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )?;

    let stmt = conn.prepare("INSERT INTO test_derive_order (id, name, age) VALUES ($1, $2, $3)")?;
    conn.exec_drop(&stmt, (1i64, "Alice", 25i32))?;

    // Select columns in different order than struct definition
    let stmt = conn.prepare("SELECT age, id, name FROM test_derive_order")?;
    let users: Vec<User> = conn.exec_collect(&stmt, ())?;

    assert_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );

    conn.query_drop("DROP TABLE test_derive_order")?;
    Ok(())
}

#[test]
fn int_types() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_ints")?;
    conn.query_drop(
        "CREATE TABLE test_derive_ints (
            tiny SMALLINT,
            small SMALLINT,
            medium INT,
            big BIGINT
        )",
    )?;

    let stmt = conn.prepare(
        "INSERT INTO test_derive_ints (tiny, small, medium, big) VALUES ($1, $2, $3, $4)",
    )?;
    conn.exec_drop(&stmt, (-1i16, -100i16, -10000i32, -1000000i64))?;

    let stmt = conn.prepare("SELECT tiny, small, medium, big FROM test_derive_ints")?;
    let rows: Vec<IntTypes> = conn.exec_collect(&stmt, ())?;

    assert_eq!(
        rows[0],
        IntTypes {
            tiny: -1,
            small: -100,
            medium: -10000,
            big: -1000000
        }
    );

    conn.query_drop("DROP TABLE test_derive_ints")?;
    Ok(())
}

#[test]
fn float_types() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_floats")?;
    conn.query_drop(
        "CREATE TABLE test_derive_floats (
            float_val REAL,
            double_val DOUBLE PRECISION
        )",
    )?;

    let stmt =
        conn.prepare("INSERT INTO test_derive_floats (float_val, double_val) VALUES ($1, $2)")?;
    conn.exec_drop(&stmt, (1.23f32, 4.56f64))?;

    let stmt = conn.prepare("SELECT float_val, double_val FROM test_derive_floats")?;
    let rows: Vec<FloatTypes> = conn.exec_collect(&stmt, ())?;

    assert!((rows[0].float_val - 1.23).abs() < 0.001);
    assert!((rows[0].double_val - 4.56).abs() < 0.00001);

    conn.query_drop("DROP TABLE test_derive_floats")?;
    Ok(())
}
