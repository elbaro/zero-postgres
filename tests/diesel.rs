//! Tests for the diesel integration.

#![cfg(feature = "diesel")]

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};

use diesel::Connection as _;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};

use zero_postgres::diesel::Connection;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_conn() -> Connection {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Connection::establish(&db_url).expect("Failed to connect")
}

fn unique_table_name() -> String {
    let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("diesel_test_{}", id)
}

// --- Connection ---

#[test]
fn establish() {
    let _conn = get_conn();
}

#[test]
fn establish_bad_url() {
    let result = Connection::establish("postgres://invalid_user_xxx@localhost/nonexistent_db_xxx");
    assert!(result.is_err());
}

// --- SimpleConnection::batch_execute ---

#[test]
fn batch_execute() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO {} (name) VALUES ('alice');
         INSERT INTO {} (name) VALUES ('bob');",
        table, table, table
    ))
    .unwrap();

    let count: i64 = diesel::sql_query(format!("SELECT COUNT(*) as count FROM {}", table))
        .get_result::<CountRow>(&mut conn)
        .unwrap()
        .count;
    assert_eq!(count, 2);
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

// --- sql_query with bind parameters ---

#[test]
fn sql_query_with_binds() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, name TEXT NOT NULL, age INT NOT NULL)",
        table
    ))
    .unwrap();

    diesel::sql_query(format!("INSERT INTO {} (name, age) VALUES ($1, $2)", table))
        .bind::<Text, _>("alice")
        .bind::<Integer, _>(30)
        .execute(&mut conn)
        .unwrap();

    diesel::sql_query(format!("INSERT INTO {} (name, age) VALUES ($1, $2)", table))
        .bind::<Text, _>("bob")
        .bind::<Integer, _>(25)
        .execute(&mut conn)
        .unwrap();

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Person {
        #[diesel(sql_type = Text)]
        name: String,
        #[diesel(sql_type = Integer)]
        age: i32,
    }

    let people: Vec<Person> =
        diesel::sql_query(format!("SELECT name, age FROM {} ORDER BY age", table))
            .load(&mut conn)
            .unwrap();

    assert_eq!(people.len(), 2);
    assert_eq!(people[0].name, "bob");
    assert_eq!(people[0].age, 25);
    assert_eq!(people[1].name, "alice");
    assert_eq!(people[1].age, 30);
}

// --- execute_returning_count ---

#[test]
fn execute_returning_count() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, value INT NOT NULL)",
        table
    ))
    .unwrap();

    let inserted = diesel::sql_query(format!(
        "INSERT INTO {} (value) VALUES (1), (2), (3)",
        table
    ))
    .execute(&mut conn)
    .unwrap();
    assert_eq!(inserted, 3);

    let updated = diesel::sql_query(format!("UPDATE {} SET value = value + 10", table))
        .execute(&mut conn)
        .unwrap();
    assert_eq!(updated, 3);

    let deleted = diesel::sql_query(format!("DELETE FROM {} WHERE value > 11", table))
        .execute(&mut conn)
        .unwrap();
    assert_eq!(deleted, 2);
}

// --- Transactions ---

#[test]
fn transaction_commit() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, value INT NOT NULL)",
        table
    ))
    .unwrap();

    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::sql_query(format!("INSERT INTO {} (value) VALUES (42)", table)).execute(conn)?;
        Ok(())
    })
    .unwrap();

    let count: CountRow = diesel::sql_query(format!("SELECT COUNT(*) as count FROM {}", table))
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(count.count, 1);
}

#[test]
fn transaction_rollback() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, value INT NOT NULL)",
        table
    ))
    .unwrap();

    let result: QueryResult<()> = conn.transaction(|conn| {
        diesel::sql_query(format!("INSERT INTO {} (value) VALUES (42)", table)).execute(conn)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(result.is_err());

    let count: CountRow = diesel::sql_query(format!("SELECT COUNT(*) as count FROM {}", table))
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(count.count, 0);
}

// --- NULL handling ---

#[test]
fn nullable_values() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, name TEXT)",
        table
    ))
    .unwrap();

    diesel::sql_query(format!("INSERT INTO {} (name) VALUES ('hello')", table))
        .execute(&mut conn)
        .unwrap();
    diesel::sql_query(format!("INSERT INTO {} (name) VALUES (NULL)", table))
        .execute(&mut conn)
        .unwrap();

    #[derive(QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
        name: Option<String>,
    }

    let rows: Vec<Row> = diesel::sql_query(format!("SELECT name FROM {} ORDER BY id", table))
        .load(&mut conn)
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name.as_deref(), Some("hello"));
    assert!(rows[1].name.is_none());
}

// --- Error mapping ---

#[test]
fn unique_violation() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!("CREATE TEMP TABLE {} (id INT PRIMARY KEY)", table))
        .unwrap();

    diesel::sql_query(format!("INSERT INTO {} (id) VALUES (1)", table))
        .execute(&mut conn)
        .unwrap();

    let result =
        diesel::sql_query(format!("INSERT INTO {} (id) VALUES (1)", table)).execute(&mut conn);

    match result {
        Err(diesel::result::Error::DatabaseError(kind, _)) => {
            assert_eq!(kind, diesel::result::DatabaseErrorKind::UniqueViolation);
        }
        other => panic!("Expected UniqueViolation, got {:?}", other),
    }
}

#[test]
fn not_null_violation() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
        table
    ))
    .unwrap();

    let result =
        diesel::sql_query(format!("INSERT INTO {} (name) VALUES (NULL)", table)).execute(&mut conn);

    match result {
        Err(diesel::result::Error::DatabaseError(kind, _)) => {
            assert_eq!(kind, diesel::result::DatabaseErrorKind::NotNullViolation);
        }
        other => panic!("Expected NotNullViolation, got {:?}", other),
    }
}

#[test]
fn foreign_key_violation() {
    let mut conn = get_conn();
    let parent = unique_table_name();
    let child = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id INT PRIMARY KEY);
         CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, parent_id INT NOT NULL REFERENCES {}(id))",
        parent, child, parent
    ))
    .unwrap();

    let result = diesel::sql_query(format!("INSERT INTO {} (parent_id) VALUES (999)", child))
        .execute(&mut conn);

    match result {
        Err(diesel::result::Error::DatabaseError(kind, _)) => {
            assert_eq!(kind, diesel::result::DatabaseErrorKind::ForeignKeyViolation);
        }
        other => panic!("Expected ForeignKeyViolation, got {:?}", other),
    }
}

#[test]
fn check_violation() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, age INT NOT NULL CHECK (age >= 0))",
        table
    ))
    .unwrap();

    let result =
        diesel::sql_query(format!("INSERT INTO {} (age) VALUES (-1)", table)).execute(&mut conn);

    match result {
        Err(diesel::result::Error::DatabaseError(kind, _)) => {
            assert_eq!(kind, diesel::result::DatabaseErrorKind::CheckViolation);
        }
        other => panic!("Expected CheckViolation, got {:?}", other),
    }
}

// --- Error info preservation ---

#[test]
fn error_info_details() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!("CREATE TEMP TABLE {} (id INT PRIMARY KEY)", table))
        .unwrap();

    diesel::sql_query(format!("INSERT INTO {} (id) VALUES (1)", table))
        .execute(&mut conn)
        .unwrap();

    let result =
        diesel::sql_query(format!("INSERT INTO {} (id) VALUES (1)", table)).execute(&mut conn);

    match result {
        Err(diesel::result::Error::DatabaseError(_, info)) => {
            assert!(info.message().contains("duplicate key"));
            assert!(info.table_name().is_some());
            assert!(info.constraint_name().is_some());
        }
        other => panic!("Expected DatabaseError, got {:?}", other),
    }
}

// --- Multiple types ---

#[test]
fn various_types() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (
            i INT NOT NULL,
            t TEXT NOT NULL,
            b BOOLEAN NOT NULL,
            f REAL NOT NULL
        )",
        table
    ))
    .unwrap();

    diesel::sql_query(format!(
        "INSERT INTO {} (i, t, b, f) VALUES ($1, $2, $3, $4)",
        table
    ))
    .bind::<Integer, _>(42)
    .bind::<Text, _>("hello")
    .bind::<diesel::sql_types::Bool, _>(true)
    .bind::<diesel::sql_types::Float, _>(1.23_f32)
    .execute(&mut conn)
    .unwrap();

    #[derive(QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = Integer)]
        i: i32,
        #[diesel(sql_type = Text)]
        t: String,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        b: bool,
        #[diesel(sql_type = diesel::sql_types::Float)]
        f: f32,
    }

    let rows: Vec<Row> = diesel::sql_query(format!("SELECT i, t, b, f FROM {}", table))
        .load(&mut conn)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].i, 42);
    assert_eq!(rows[0].t, "hello");
    assert!(rows[0].b);
    assert!((rows[0].f - 1.23).abs() < 0.01);
}

// --- Empty result set ---

#[test]
fn empty_result() {
    let mut conn = get_conn();
    let table = unique_table_name();
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {} (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
        table
    ))
    .unwrap();

    #[derive(QueryableByName, Debug)]
    #[allow(dead_code)]
    struct Row {
        #[diesel(sql_type = Text)]
        name: String,
    }

    let rows: Vec<Row> = diesel::sql_query(format!("SELECT name FROM {}", table))
        .load(&mut conn)
        .unwrap();

    assert!(rows.is_empty());
}
