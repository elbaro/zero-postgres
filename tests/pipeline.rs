//! Integration tests for the Pipeline API.
//!
//! Tests the pipeline API that supports:
//! - Batching multiple queries without waiting for responses
//! - Claiming results in order with typed handles (Ticket)
//!
//! ## Test Matrix
//!
//! ### Basic Pipeline Tests
//! - `test_pipeline_exec` - Basic exec flow with raw SQL
//! - `test_pipeline_multiple_execs` - Multiple exec calls
//! - `test_pipeline_no_rows` - Query returning no rows
//! - `test_pipeline_multiple_rows` - Query returning multiple rows
//!
//! ### Prepared Statement Tests
//! - `test_pipeline_with_prepared` - Using prepared statements in pipeline
//!
//! ### Error Handling Tests
//! - `test_pipeline_claim_order_error` - Validates claim ordering enforcement
//! - `test_pipeline_sql_error` - SQL error propagation
//! - `test_pipeline_aborted_state` - Pipeline abort state after error
//!
//! ### INSERT/UPDATE/DELETE Tests
//! - `test_pipeline_insert` - INSERT in pipeline
//! - `test_pipeline_insert_returning` - INSERT with RETURNING clause
//!
//! ### Edge Cases
//! - `test_pipeline_empty` - Empty pipeline (just sync)
//! - `test_pipeline_pending_count` - Pending count tracking

use std::env;
use zero_postgres::sync::Conn;

fn get_conn() -> Conn {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    // Disable TLS if not specified
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str()).expect("Failed to connect")
}

// === Basic Pipeline Tests ===

/// Test basic exec flow with raw SQL
#[test]
fn test_pipeline_exec() {
    let mut conn = get_conn();

    let result = conn
        .run_pipeline(|p| {
            let t = p.exec("SELECT $1::int as num, $2::text as txt", (42, "hello"))?;
            p.sync()?;
            let rows: Vec<(i32, String)> = p.claim_collect(t)?;
            Ok(rows)
        })
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (42, "hello".to_string()));
}

/// Test multiple exec calls
#[test]
fn test_pipeline_multiple_execs() {
    let mut conn = get_conn();

    let (r1, r2, r3) = conn
        .run_pipeline(|p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;
            let t3 = p.exec("SELECT $1::int", (3,))?;

            p.sync()?;

            let r1: Vec<(i32,)> = p.claim_collect(t1)?;
            let r2: Vec<(i32,)> = p.claim_collect(t2)?;
            let r3: Vec<(i32,)> = p.claim_collect(t3)?;

            Ok((r1, r2, r3))
        })
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
}

/// Test query that returns no rows
#[test]
fn test_pipeline_no_rows() {
    let mut conn = get_conn();

    let result: Vec<(i32,)> = conn
        .run_pipeline(|p| {
            let t = p.exec("SELECT 1 WHERE false", ())?;
            p.sync()?;
            p.claim_collect(t)
        })
        .unwrap();

    assert!(result.is_empty());
}

/// Test query with multiple rows
#[test]
fn test_pipeline_multiple_rows() {
    let mut conn = get_conn();

    let result: Vec<(i32,)> = conn
        .run_pipeline(|p| {
            let t = p.exec("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)", ())?;
            p.sync()?;
            p.claim_collect(t)
        })
        .unwrap();

    assert_eq!(result, vec![(1,), (2,), (3,)]);
}

// === Prepared Statement Tests ===

/// Test using prepared statements in pipeline
#[test]
fn test_pipeline_with_prepared() {
    let mut conn = get_conn();

    // Prepare statement outside pipeline
    let stmt = conn.prepare("SELECT $1::int * 2").unwrap();

    let (r1, r2) = conn
        .run_pipeline(|p| {
            let t1 = p.exec(&stmt, (5,))?;
            let t2 = p.exec(&stmt, (10,))?;

            p.sync()?;

            let r1: Vec<(i32,)> = p.claim_collect(t1)?;
            let r2: Vec<(i32,)> = p.claim_collect(t2)?;

            Ok((r1, r2))
        })
        .unwrap();

    assert_eq!(r1, vec![(10,)]);
    assert_eq!(r2, vec![(20,)]);
}

// === Error Handling Tests ===

/// Test claim order validation
#[test]
fn test_pipeline_claim_order_error() {
    let mut conn = get_conn();

    let result = conn.run_pipeline(|p| {
        let t1 = p.exec("SELECT 1", ())?;
        let t2 = p.exec("SELECT 2", ())?;

        p.sync()?;

        // Try to claim t2 before t1 - should fail
        let result: Result<Vec<(i32,)>, _> = p.claim_collect(t2);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("out of order"),
            "Expected 'out of order' error, got: {}",
            err
        );

        // Now claim in correct order
        let _: Vec<(i32,)> = p.claim_collect(t1)?;

        Ok(())
    });

    // The run_pipeline should complete (cleanup handles remaining)
    assert!(result.is_ok());
}

/// Test SQL error propagation
#[test]
fn test_pipeline_sql_error() {
    let mut conn = get_conn();

    let result = conn.run_pipeline(|p| {
        let t = p.exec("SELECT 1/0", ())?;

        p.sync()?;

        // Claiming should fail with division by zero
        let result: Result<Vec<(i32,)>, _> = p.claim_collect(t);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("division by zero"),
            "Expected 'division by zero' error, got: {}",
            err
        );

        Ok(())
    });

    assert!(result.is_ok());
}

/// Test aborted pipeline state
#[test]
fn test_pipeline_aborted_state() {
    let mut conn = get_conn();

    let result = conn.run_pipeline(|p| {
        let t1 = p.exec("SELECT 1", ())?;
        let t2 = p.exec("SELECT 1/0", ())?; // This will fail
        let t3 = p.exec("SELECT 2", ())?;

        p.sync()?;

        // First operation succeeds
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        assert_eq!(r1, vec![(1,)]);

        // t2 fails - pipeline becomes aborted
        let result: Result<Vec<(i32,)>, _> = p.claim_collect(t2);
        assert!(result.is_err());

        // Subsequent claims should also fail due to aborted state
        let result3: Result<Vec<(i32,)>, _> = p.claim_collect(t3);
        assert!(result3.is_err());
        let err = result3.unwrap_err();
        assert!(
            err.to_string().contains("aborted"),
            "Expected 'aborted' error, got: {}",
            err
        );

        Ok(())
    });

    assert!(result.is_ok());
}

// === INSERT/UPDATE/DELETE Tests ===

/// Test INSERT in pipeline
#[test]
fn test_pipeline_insert() {
    let mut conn = get_conn();

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS _pipeline_insert_test")
        .unwrap();
    conn.query_drop("CREATE TEMP TABLE _pipeline_insert_test (id int, name text)")
        .unwrap();

    conn.run_pipeline(|p| {
        let t1 = p.exec(
            "INSERT INTO _pipeline_insert_test VALUES ($1, $2)",
            (1, "alice"),
        )?;
        let t2 = p.exec(
            "INSERT INTO _pipeline_insert_test VALUES ($1, $2)",
            (2, "bob"),
        )?;

        p.sync()?;

        p.claim_drop(t1)?;
        p.claim_drop(t2)?;

        Ok(())
    })
    .unwrap();

    // Verify inserts
    let rows: Vec<(i32, String)> = conn
        .query_collect("SELECT id, name FROM _pipeline_insert_test ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "alice".to_string()));
    assert_eq!(rows[1], (2, "bob".to_string()));
}

/// Test INSERT with RETURNING
#[test]
fn test_pipeline_insert_returning() {
    let mut conn = get_conn();

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS _pipeline_returning_test")
        .unwrap();
    conn.query_drop(
        "CREATE TEMP TABLE _pipeline_returning_test (id serial PRIMARY KEY, name text)",
    )
    .unwrap();

    let (r1, r2) = conn
        .run_pipeline(|p| {
            let t1 = p.exec(
                "INSERT INTO _pipeline_returning_test (name) VALUES ($1) RETURNING id",
                ("alice",),
            )?;
            let t2 = p.exec(
                "INSERT INTO _pipeline_returning_test (name) VALUES ($1) RETURNING id",
                ("bob",),
            )?;

            p.sync()?;

            let r1: Vec<(i32,)> = p.claim_collect(t1)?;
            let r2: Vec<(i32,)> = p.claim_collect(t2)?;

            Ok((r1, r2))
        })
        .unwrap();

    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert!(r1[0].0 < r2[0].0, "IDs should be sequential");
}

// === Edge Cases ===

/// Test empty pipeline (just sync)
#[test]
fn test_pipeline_empty() {
    let mut conn = get_conn();

    conn.run_pipeline(|p| {
        p.sync()?;
        Ok(())
    })
    .unwrap();
    // Should complete without error
}

/// Test pending_count tracking
#[test]
fn test_pipeline_pending_count() {
    let mut conn = get_conn();

    conn.run_pipeline(|p| {
        assert_eq!(p.pending_count(), 0);

        let t1 = p.exec("SELECT 1", ())?;
        assert_eq!(p.pending_count(), 1);

        let t2 = p.exec("SELECT 2", ())?;
        assert_eq!(p.pending_count(), 2);

        p.sync()?;

        let _: Vec<(i32,)> = p.claim_collect(t1)?;
        assert_eq!(p.pending_count(), 1);

        let _: Vec<(i32,)> = p.claim_collect(t2)?;
        assert_eq!(p.pending_count(), 0);

        Ok(())
    })
    .unwrap();
}

/// Test claim_one for single row result
#[test]
fn test_pipeline_claim_one() {
    let mut conn = get_conn();

    let result = conn
        .run_pipeline(|p| {
            let t = p.exec("SELECT 42::int", ())?;
            p.sync()?;
            p.claim_one::<(i32,)>(t)
        })
        .unwrap();

    assert_eq!(result, Some((42,)));
}

/// Test claim_one returns None for empty result
#[test]
fn test_pipeline_claim_one_empty() {
    let mut conn = get_conn();

    let result = conn
        .run_pipeline(|p| {
            let t = p.exec("SELECT 1 WHERE false", ())?;
            p.sync()?;
            p.claim_one::<(i32,)>(t)
        })
        .unwrap();

    assert_eq!(result, None);
}
