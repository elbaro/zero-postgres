//! Tests for exec_iter and UnnamedPortal

use std::env;
use zero_postgres::handler::CollectHandler;
use zero_postgres::sync::Conn;

fn get_conn() -> Conn {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str()).expect("Failed to connect")
}

#[test]
fn test_exec_iter_basic() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT generate_series(1, 5) as n").unwrap();

    let total: i32 = conn
        .exec_iter(&stmt, (), |portal| {
            let mut handler = CollectHandler::new();
            let has_more = portal.fetch(0, &mut handler)?; // 0 = fetch all
            assert!(!has_more, "Expected all rows fetched");
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .unwrap();

    assert_eq!(total, 15); // 1+2+3+4+5
}

#[test]
fn test_exec_iter_batched() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT generate_series(1, 10) as n").unwrap();

    let mut all_rows: Vec<i32> = Vec::new();
    let batch_count: i32 = conn
        .exec_iter(&stmt, (), |portal| {
            let mut batches = 0;
            loop {
                let mut handler = CollectHandler::new();
                let has_more = portal.fetch(3, &mut handler)?; // fetch 3 at a time
                let rows: Vec<(i32,)> = handler.into_rows();
                all_rows.extend(rows.iter().map(|(n,)| *n));
                batches += 1;
                if !has_more {
                    break;
                }
            }
            Ok(batches)
        })
        .unwrap();

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4); // 3+3+3+1 rows in 4 batches
}

#[test]
fn test_exec_iter_empty_result() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT 1 WHERE false").unwrap();

    let row_count: usize = conn
        .exec_iter(&stmt, (), |portal| {
            let mut handler = CollectHandler::new();
            let has_more = portal.fetch(0, &mut handler)?;
            assert!(!has_more, "Expected completion on empty result");
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.len())
        })
        .unwrap();

    assert_eq!(row_count, 0);
}

#[test]
fn test_exec_iter_with_params() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT generate_series(1, $1) as n").unwrap();

    let total: i32 = conn
        .exec_iter(&stmt, (5i32,), |portal| {
            let mut handler = CollectHandler::new();
            portal.fetch(0, &mut handler)?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .unwrap();

    assert_eq!(total, 15);
}

#[test]
fn test_exec_iter_closure_returns_value() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT 42 as answer").unwrap();

    let answer: i32 = conn
        .exec_iter(&stmt, (), |portal| {
            let mut handler = CollectHandler::new();
            portal.fetch(0, &mut handler)?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows[0].0)
        })
        .unwrap();

    assert_eq!(answer, 42);
}

#[test]
fn test_exec_iter_with_raw_sql() {
    let mut conn = get_conn();

    let total: i32 = conn
        .exec_iter("SELECT generate_series(1, 5) as n", (), |portal| {
            let mut handler = CollectHandler::new();
            portal.fetch(0, &mut handler)?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .unwrap();

    assert_eq!(total, 15);
}

#[test]
fn test_exec_iter_with_raw_sql_and_params() {
    let mut conn = get_conn();

    let total: i32 = conn
        .exec_iter("SELECT generate_series(1, $1) as n", (5i32,), |portal| {
            let mut handler = CollectHandler::new();
            portal.fetch(0, &mut handler)?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .unwrap();

    assert_eq!(total, 15);
}

#[test]
fn test_exec_iter_raw_sql_batched() {
    let mut conn = get_conn();

    let mut all_rows: Vec<i32> = Vec::new();
    let batch_count: i32 = conn
        .exec_iter("SELECT generate_series(1, 10) as n", (), |portal| {
            let mut batches = 0;
            loop {
                let mut handler = CollectHandler::new();
                let has_more = portal.fetch(3, &mut handler)?; // fetch 3 at a time
                let rows: Vec<(i32,)> = handler.into_rows();
                all_rows.extend(rows.iter().map(|(n,)| *n));
                batches += 1;
                if !has_more {
                    break;
                }
            }
            Ok(batches)
        })
        .unwrap();

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4); // 3+3+3+1 rows in 4 batches
}
