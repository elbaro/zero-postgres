//! Tests for async exec_portal and UnnamedPortal

#![allow(
    clippy::panic_in_result_fn,
    clippy::shadow_unrelated,
    clippy::unwrap_used
)]

use std::env;
use zero_postgres::Error;
use zero_postgres::handler::CollectHandler;
use zero_postgres::tokio::Conn;

async fn get_conn() -> Result<Conn, Error> {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str()).await
}

#[tokio::test]
async fn exec_portal_basic() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT generate_series(1, 5) as n").await?;

    let total: i32 = conn
        .exec_portal(&stmt, (), async |portal| {
            let mut handler = CollectHandler::new();
            let has_more = portal.exec(0, &mut handler).await?;
            assert!(!has_more, "Expected all rows fetched");
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .await?;

    assert_eq!(total, 15);
    Ok(())
}

#[tokio::test]
async fn exec_portal_batched() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT generate_series(1, 10) as n").await?;

    let (all_rows, batch_count) = conn
        .exec_portal(&stmt, (), async |portal| {
            let mut all_rows: Vec<i32> = Vec::new();
            let mut batches = 0;
            loop {
                let mut handler = CollectHandler::new();
                let has_more = portal.exec(3, &mut handler).await?;
                let rows: Vec<(i32,)> = handler.into_rows();
                all_rows.extend(rows.iter().map(|(n,)| *n));
                batches += 1;
                if !has_more {
                    break;
                }
            }
            Ok((all_rows, batches))
        })
        .await?;

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4);
    Ok(())
}

#[tokio::test]
async fn exec_portal_empty_result() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT 1 WHERE false").await?;

    let row_count: usize = conn
        .exec_portal(&stmt, (), async |portal| {
            let mut handler = CollectHandler::new();
            let has_more = portal.exec(0, &mut handler).await?;
            assert!(!has_more, "Expected completion on empty result");
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.len())
        })
        .await?;

    assert_eq!(row_count, 0);
    Ok(())
}

#[tokio::test]
async fn exec_portal_with_params() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT generate_series(1, $1) as n").await?;

    let total: i32 = conn
        .exec_portal(&stmt, (5i32,), async |portal| {
            let mut handler = CollectHandler::new();
            portal.exec(0, &mut handler).await?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .await?;

    assert_eq!(total, 15);
    Ok(())
}

#[tokio::test]
async fn exec_portal_closure_returns_value() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT 42 as answer").await?;

    let answer: i32 = conn
        .exec_portal(&stmt, (), async |portal| {
            let mut handler = CollectHandler::new();
            portal.exec(0, &mut handler).await?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows[0].0)
        })
        .await?;

    assert_eq!(answer, 42);
    Ok(())
}

#[tokio::test]
async fn exec_portal_with_raw_sql() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let total: i32 = conn
        .exec_portal("SELECT generate_series(1, 5) as n", (), async |portal| {
            let mut handler = CollectHandler::new();
            portal.exec(0, &mut handler).await?;
            let rows: Vec<(i32,)> = handler.into_rows();
            Ok(rows.iter().map(|(n,)| n).sum())
        })
        .await?;

    assert_eq!(total, 15);
    Ok(())
}

#[tokio::test]
async fn exec_portal_with_raw_sql_and_params() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let total: i32 = conn
        .exec_portal(
            "SELECT generate_series(1, $1) as n",
            (5i32,),
            async |portal| {
                let mut handler = CollectHandler::new();
                portal.exec(0, &mut handler).await?;
                let rows: Vec<(i32,)> = handler.into_rows();
                Ok(rows.iter().map(|(n,)| n).sum())
            },
        )
        .await?;

    assert_eq!(total, 15);
    Ok(())
}

#[tokio::test]
async fn exec_portal_raw_sql_batched() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let (all_rows, batch_count) = conn
        .exec_portal("SELECT generate_series(1, 10) as n", (), async |portal| {
            let mut all_rows: Vec<i32> = Vec::new();
            let mut batches = 0;
            loop {
                let mut handler = CollectHandler::new();
                let has_more = portal.exec(3, &mut handler).await?;
                let rows: Vec<(i32,)> = handler.into_rows();
                all_rows.extend(rows.iter().map(|(n,)| *n));
                batches += 1;
                if !has_more {
                    break;
                }
            }
            Ok((all_rows, batches))
        })
        .await?;

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4);
    Ok(())
}

#[tokio::test]
async fn exec_portal_foreach_basic() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT generate_series(1, 5) as n").await?;

    let total: i32 = conn
        .exec_portal(&stmt, (), async |portal| {
            let mut sum = 0i32;
            let has_more = portal
                .exec_foreach(0, |row: (i32,)| {
                    sum += row.0;
                    Ok(())
                })
                .await?;
            assert!(!has_more, "Expected all rows fetched");
            Ok(sum)
        })
        .await?;

    assert_eq!(total, 15); // 1+2+3+4+5
    Ok(())
}

#[tokio::test]
async fn exec_portal_foreach_batched() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let stmt = conn.prepare("SELECT generate_series(1, 10) as n").await?;

    let (all_rows, batch_count) = conn
        .exec_portal(&stmt, (), async |portal| {
            let mut all_rows: Vec<i32> = Vec::new();
            let mut batches = 0;
            loop {
                let has_more = portal
                    .exec_foreach(3, |row: (i32,)| {
                        all_rows.push(row.0);
                        Ok(())
                    })
                    .await?;
                batches += 1;
                if !has_more {
                    break;
                }
            }
            Ok((all_rows, batches))
        })
        .await?;

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batch_count, 4); // 3+3+3+1 rows in 4 batches
    Ok(())
}
