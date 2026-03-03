//! Tests for exec_portal and UnnamedPortal

#![allow(clippy::panic_in_result_fn, clippy::shadow_unrelated)]

use std::env;
use zero_postgres::Error;
use zero_postgres::handler::CollectHandler;
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

#[test]
fn exec_portal_basic() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT generate_series(1, 5) as n")?;

    let total: i32 = conn.exec_portal(&stmt, (), |portal| {
        let mut handler = CollectHandler::new();
        let has_more = portal.exec(0, &mut handler)?; // 0 = fetch all
        assert!(!has_more, "Expected all rows fetched");
        let rows: Vec<(i32,)> = handler.into_rows();
        Ok(rows.iter().map(|(n,)| n).sum())
    })?;

    assert_eq!(total, 15); // 1+2+3+4+5
    Ok(())
}

#[test]
fn exec_portal_batched() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT generate_series(1, 10) as n")?;

    let mut all_rows: Vec<i32> = Vec::new();
    let batch_count: i32 = conn.exec_portal(&stmt, (), |portal| {
        let mut batches = 0;
        loop {
            let mut handler = CollectHandler::new();
            let has_more = portal.exec(3, &mut handler)?; // fetch 3 at a time
            let rows: Vec<(i32,)> = handler.into_rows();
            all_rows.extend(rows.iter().map(|(n,)| *n));
            batches += 1;
            if !has_more {
                break;
            }
        }
        Ok(batches)
    })?;

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4); // 3+3+3+1 rows in 4 batches
    Ok(())
}

#[test]
fn exec_portal_empty_result() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT 1 WHERE false")?;

    let row_count: usize = conn.exec_portal(&stmt, (), |portal| {
        let mut handler = CollectHandler::new();
        let has_more = portal.exec(0, &mut handler)?;
        assert!(!has_more, "Expected completion on empty result");
        let rows: Vec<(i32,)> = handler.into_rows();
        Ok(rows.len())
    })?;

    assert_eq!(row_count, 0);
    Ok(())
}

#[test]
fn exec_portal_with_params() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT generate_series(1, $1) as n")?;

    let total: i32 = conn.exec_portal(&stmt, (5i32,), |portal| {
        let mut handler = CollectHandler::new();
        portal.exec(0, &mut handler)?;
        let rows: Vec<(i32,)> = handler.into_rows();
        Ok(rows.iter().map(|(n,)| n).sum())
    })?;

    assert_eq!(total, 15);
    Ok(())
}

#[test]
fn exec_portal_closure_returns_value() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT 42 as answer")?;

    let answer: i32 = conn.exec_portal(&stmt, (), |portal| {
        let mut handler = CollectHandler::new();
        portal.exec(0, &mut handler)?;
        let rows: Vec<(i32,)> = handler.into_rows();
        Ok(rows[0].0)
    })?;

    assert_eq!(answer, 42);
    Ok(())
}

#[test]
fn exec_portal_with_raw_sql() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let total: i32 = conn.exec_portal("SELECT generate_series(1, 5) as n", (), |portal| {
        let mut handler = CollectHandler::new();
        portal.exec(0, &mut handler)?;
        let rows: Vec<(i32,)> = handler.into_rows();
        Ok(rows.iter().map(|(n,)| n).sum())
    })?;

    assert_eq!(total, 15);
    Ok(())
}

#[test]
fn exec_portal_with_raw_sql_and_params() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let total: i32 = conn.exec_portal("SELECT generate_series(1, $1) as n", (5i32,), |portal| {
        let mut handler = CollectHandler::new();
        portal.exec(0, &mut handler)?;
        let rows: Vec<(i32,)> = handler.into_rows();
        Ok(rows.iter().map(|(n,)| n).sum())
    })?;

    assert_eq!(total, 15);
    Ok(())
}

#[test]
fn exec_portal_raw_sql_batched() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let mut all_rows: Vec<i32> = Vec::new();
    let batch_count: i32 =
        conn.exec_portal("SELECT generate_series(1, 10) as n", (), |portal| {
            let mut batches = 0;
            loop {
                let mut handler = CollectHandler::new();
                let has_more = portal.exec(3, &mut handler)?; // fetch 3 at a time
                let rows: Vec<(i32,)> = handler.into_rows();
                all_rows.extend(rows.iter().map(|(n,)| *n));
                batches += 1;
                if !has_more {
                    break;
                }
            }
            Ok(batches)
        })?;

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4); // 3+3+3+1 rows in 4 batches
    Ok(())
}

#[test]
fn exec_portal_foreach_basic() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT generate_series(1, 5) as n")?;

    let total: i32 = conn.exec_portal(&stmt, (), |portal| {
        let mut sum = 0i32;
        let has_more = portal.exec_foreach(0, |row: (i32,)| {
            sum += row.0;
            Ok(())
        })?;
        assert!(!has_more, "Expected all rows fetched");
        Ok(sum)
    })?;

    assert_eq!(total, 15); // 1+2+3+4+5
    Ok(())
}

#[test]
fn exec_portal_foreach_batched() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let stmt = conn.prepare("SELECT generate_series(1, 10) as n")?;

    let mut all_rows: Vec<i32> = Vec::new();
    let batch_count: i32 = conn.exec_portal(&stmt, (), |portal| {
        let mut batches = 0;
        loop {
            let has_more = portal.exec_foreach(3, |row: (i32,)| {
                all_rows.push(row.0);
                Ok(())
            })?;
            batches += 1;
            if !has_more {
                break;
            }
        }
        Ok(batches)
    })?;

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4); // 3+3+3+1 rows in 4 batches
    Ok(())
}
