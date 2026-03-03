//! Tests for async transaction behavior (compio)

#![cfg(feature = "compio")]
#![allow(clippy::panic_in_result_fn, clippy::shadow_unrelated)]

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_postgres::Error;
use zero_postgres::compio::Conn;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

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

struct TestTable {
    name: String,
}

impl TestTable {
    async fn new(conn: &mut Conn) -> Result<Self, Error> {
        let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("tx_test_compio_{}", id);
        conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
            .await?;
        conn.query_drop(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, value INT)",
            name
        ))
        .await?;
        Ok(Self { name })
    }

    fn insert_sql(&self) -> String {
        format!("INSERT INTO {} (value) VALUES ($1)", self.name)
    }

    async fn count(&self, conn: &mut Conn) -> Result<i64, Error> {
        let rows: Vec<(i64,)> = conn
            .query_collect(&format!("SELECT COUNT(*) FROM {}", self.name))
            .await?;
        Ok(rows[0].0)
    }

    async fn cleanup(&self, conn: &mut Conn) {
        let _ = conn
            .query_drop(&format!("DROP TABLE IF EXISTS {}", self.name))
            .await;
    }
}

#[compio::test]
async fn transaction_explicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    conn.transaction(async |conn, tx| {
        conn.exec_drop(sql.as_str(), (42,)).await?;
        tx.commit(conn).await
    })
    .await?;

    assert_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_explicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    conn.transaction(async |conn, tx| {
        conn.exec_drop(sql.as_str(), (42,)).await?;
        tx.rollback(conn).await
    })
    .await?;

    assert_eq!(table.count(&mut conn).await?, 0);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_commit_on_ok() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    // Return Ok without explicit commit - should auto-commit
    conn.transaction(async |conn, _tx| {
        conn.exec_drop(sql.as_str(), (42,)).await?;
        Ok(())
    })
    .await?;

    // Data should be committed
    assert_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_rollback_on_err() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    // Return Err without explicit rollback - should auto-rollback
    let result: Result<(), Error> = conn
        .transaction(async |conn, _tx| {
            conn.exec_drop(sql.as_str(), (42,)).await?;
            Err(Error::InvalidUsage("intentional error".into()))
        })
        .await;

    assert!(result.is_err());
    // Data should be rolled back
    assert_eq!(table.count(&mut conn).await?, 0);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_commit_with_return_value() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    // Return Ok with a value without explicit commit
    let result: i32 = conn
        .transaction(async |conn, _tx| {
            conn.exec_drop(sql.as_str(), (42,)).await?;
            Ok(123)
        })
        .await?;

    assert_eq!(result, 123);
    assert_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_commit_multiple_inserts() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    conn.transaction(async |conn, _tx| {
        for i in 1..=5 {
            conn.exec_drop(sql.as_str(), (i,)).await?;
        }
        Ok(())
    })
    .await?;

    assert_eq!(table.count(&mut conn).await?, 5);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_rollback_partial_work() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    let result: Result<(), Error> = conn
        .transaction(async |conn, _tx| {
            // Do some work
            conn.exec_drop(sql.as_str(), (1,)).await?;
            conn.exec_drop(sql.as_str(), (2,)).await?;
            // Then fail
            Err(Error::InvalidUsage("intentional error".into()))
        })
        .await;

    assert!(result.is_err());
    // All work should be rolled back
    assert_eq!(table.count(&mut conn).await?, 0);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_connection_usable_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    // First transaction with implicit commit
    conn.transaction(async |conn, _tx| {
        conn.exec_drop(sql.as_str(), (1,)).await?;
        Ok(())
    })
    .await?;

    // Connection should be usable for another transaction
    conn.transaction(async |conn, _tx| {
        conn.exec_drop(sql.as_str(), (2,)).await?;
        Ok(())
    })
    .await?;

    assert_eq!(table.count(&mut conn).await?, 2);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_connection_usable_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;
    let sql = table.insert_sql();

    // First transaction with implicit rollback
    let _: Result<(), Error> = conn
        .transaction(async |conn, _tx| {
            conn.exec_drop(sql.as_str(), (1,)).await?;
            Err(Error::InvalidUsage("intentional error".into()))
        })
        .await;

    // Connection should be usable for another transaction
    conn.transaction(async |conn, _tx| {
        conn.exec_drop(sql.as_str(), (2,)).await?;
        Ok(())
    })
    .await?;

    assert_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_not_in_transaction_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    conn.transaction(async |conn, _tx| {
        assert!(conn.in_transaction());
        Ok(())
    })
    .await?;

    assert!(!conn.in_transaction());
    Ok(())
}

#[compio::test]
async fn transaction_not_in_transaction_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let _: Result<(), Error> = conn
        .transaction(async |_conn, _tx| Err(Error::InvalidUsage("intentional error".into())))
        .await;

    assert!(!conn.in_transaction());
    Ok(())
}
