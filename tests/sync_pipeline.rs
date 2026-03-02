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

#![allow(
    clippy::panic_in_result_fn,
    clippy::shadow_unrelated,
    clippy::unwrap_used
)]

use std::env;
use zero_postgres::Error;
use zero_postgres::sync::Conn;

fn get_conn() -> Result<Conn, Error> {
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
    Conn::new(db_url.as_str())
}

/// Verify connection still works after pipeline operations
fn verify_connection(conn: &mut Conn) -> Result<(), Error> {
    let result: Vec<(i32,)> = conn.query_collect("SELECT 7919")?;
    assert_eq!(result[0].0, 7919);
    Ok(())
}

// === Basic Pipeline Tests ===

/// Test basic exec flow with raw SQL
#[test]
fn pipeline_exec() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
        let t = p.exec("SELECT $1::int as num, $2::text as txt", (42, "hello"))?;
        p.sync()?;
        let rows: Vec<(i32, String)> = p.claim_collect(t)?;
        Ok(rows)
    })?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (42, "hello".to_string()));
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test multiple exec calls
#[test]
fn pipeline_multiple_execs() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let (r1, r2, r3) = conn.pipeline(|p| {
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT $1::int", (2,))?;
        let t3 = p.exec("SELECT $1::int", (3,))?;

        p.sync()?;

        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;
        let r3: Vec<(i32,)> = p.claim_collect(t3)?;

        Ok((r1, r2, r3))
    })?;

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test query that returns no rows
#[test]
fn pipeline_no_rows() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result: Vec<(i32,)> = conn.pipeline(|p| {
        let t = p.exec("SELECT 1 WHERE false", ())?;
        p.sync()?;
        p.claim_collect(t)
    })?;

    assert!(result.is_empty());
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test query with multiple rows
#[test]
fn pipeline_multiple_rows() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result: Vec<(i32,)> = conn.pipeline(|p| {
        let t = p.exec("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)", ())?;
        p.sync()?;
        p.claim_collect(t)
    })?;

    assert_eq!(result, vec![(1,), (2,), (3,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

// === Prepared Statement Tests ===

/// Test using prepared statements in pipeline
#[test]
fn pipeline_with_prepared() -> Result<(), Error> {
    let mut conn = get_conn()?;

    // Prepare statement outside pipeline
    let stmt = conn.prepare("SELECT $1::int * 2")?;

    let (r1, r2) = conn.pipeline(|p| {
        let t1 = p.exec(&stmt, (5,))?;
        let t2 = p.exec(&stmt, (10,))?;

        p.sync()?;

        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;

        Ok((r1, r2))
    })?;

    assert_eq!(r1, vec![(10,)]);
    assert_eq!(r2, vec![(20,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

// === Error Handling Tests ===

/// Test claim order validation
#[test]
fn pipeline_claim_order_error() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
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

    // The pipeline should complete (cleanup handles remaining)
    result?;
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test SQL error propagation
#[test]
fn pipeline_sql_error() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
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

    result?;
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test aborted pipeline state
#[test]
fn pipeline_aborted_state() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
        let t1 = p.exec("SELECT 1", ())?;
        let t2 = p.exec("SELECT 1/0", ())?; // This will fail
        let t3 = p.exec("SELECT 2", ())?;

        p.sync()?;

        // First operation succeeds
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        assert_eq!(r1, vec![(1,)]);

        // t2 fails - pipeline becomes aborted
        let result: Result<Vec<(i32,)>, _> = p.claim_collect(t2);
        result.unwrap_err();

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

    result?;
    verify_connection(&mut conn)?;
    Ok(())
}

// === INSERT/UPDATE/DELETE Tests ===

/// Test INSERT in pipeline
#[test]
fn pipeline_insert() -> Result<(), Error> {
    let mut conn = get_conn()?;

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS _pipeline_insert_test")?;
    conn.query_drop("CREATE TEMP TABLE _pipeline_insert_test (id int, name text)")?;

    conn.pipeline(|p| {
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
    })?;

    // Verify inserts
    let rows: Vec<(i32, String)> =
        conn.query_collect("SELECT id, name FROM _pipeline_insert_test ORDER BY id")?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "alice".to_string()));
    assert_eq!(rows[1], (2, "bob".to_string()));
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test INSERT with RETURNING
#[test]
fn pipeline_insert_returning() -> Result<(), Error> {
    let mut conn = get_conn()?;

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS _pipeline_returning_test")?;
    conn.query_drop(
        "CREATE TEMP TABLE _pipeline_returning_test (id serial PRIMARY KEY, name text)",
    )?;

    let (r1, r2) = conn.pipeline(|p| {
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
    })?;

    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert!(r1[0].0 < r2[0].0, "IDs should be sequential");
    verify_connection(&mut conn)?;
    Ok(())
}

// === Edge Cases ===

/// Test empty pipeline (just sync)
#[test]
fn pipeline_empty() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.pipeline(|p| {
        p.sync()?;
        Ok(())
    })?;
    // Should complete without error
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test pending_count tracking
#[test]
fn pipeline_pending_count() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.pipeline(|p| {
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
    })?;
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test claim_one for single row result
#[test]
fn pipeline_claim_one() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
        let t = p.exec("SELECT 42::int", ())?;
        p.sync()?;
        p.claim_one::<(i32,)>(t)
    })?;

    assert_eq!(result, Some((42,)));
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test claim_one returns None for empty result
#[test]
fn pipeline_claim_one_empty() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
        let t = p.exec("SELECT 1 WHERE false", ())?;
        p.sync()?;
        p.claim_one::<(i32,)>(t)
    })?;

    assert_eq!(result, None);
    verify_connection(&mut conn)?;
    Ok(())
}

// === Auto-Sync Tests ===

/// Test basic auto-sync: claim without explicit sync()
#[test]
fn pipeline_auto_sync_basic() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let (r1, r2) = conn.pipeline(|p| {
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT $1::int", (2,))?;
        // No explicit sync() - claim should auto-sync
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;
        Ok((r1, r2))
    })?;

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test interleaved exec/claim pattern without explicit sync
/// exec()*n, claim some, exec() more, claim remaining
#[test]
fn pipeline_interleaved_exec_claim() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let (r1, r2, r3, r4) = conn.pipeline(|p| {
        // First batch of execs
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT $1::int", (2,))?;

        // Claim first two (auto-syncs)
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;

        // Second batch of execs
        let t3 = p.exec("SELECT $1::int", (3,))?;
        let t4 = p.exec("SELECT $1::int", (4,))?;

        // Claim remaining (auto-syncs again)
        let r3: Vec<(i32,)> = p.claim_collect(t3)?;
        let r4: Vec<(i32,)> = p.claim_collect(t4)?;

        Ok((r1, r2, r3, r4))
    })?;

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
    assert_eq!(r4, vec![(4,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test partial claims then more execs before claiming rest
#[test]
fn pipeline_partial_claim_then_exec() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let (r1, r2, r3, r4, r5) = conn.pipeline(|p| {
        // Queue 3 operations
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT $1::int", (2,))?;
        let t3 = p.exec("SELECT $1::int", (3,))?;

        // Claim only first one (auto-syncs all 3)
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;

        // Queue more operations before claiming t2, t3
        let t4 = p.exec("SELECT $1::int", (4,))?;
        let t5 = p.exec("SELECT $1::int", (5,))?;

        // Claim t2 (no sync needed, was synced with t1)
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;

        // Claim t3 - this should trigger sync for t4, t5
        let r3: Vec<(i32,)> = p.claim_collect(t3)?;

        // Claim remaining
        let r4: Vec<(i32,)> = p.claim_collect(t4)?;
        let r5: Vec<(i32,)> = p.claim_collect(t5)?;

        Ok((r1, r2, r3, r4, r5))
    })?;

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
    assert_eq!(r4, vec![(4,)]);
    assert_eq!(r5, vec![(5,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test multiple exec/claim batches with error in between
#[test]
fn pipeline_multiple_batches_with_error() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let result = conn.pipeline(|p| {
        // First batch - all succeed
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT $1::int", (2,))?;

        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;

        assert_eq!(r1, vec![(1,)]);
        assert_eq!(r2, vec![(2,)]);

        // Second batch - has an error
        let t3 = p.exec("SELECT $1::int", (3,))?;
        let t4 = p.exec("SELECT 1/0", ())?; // Error
        let t5 = p.exec("SELECT $1::int", (5,))?;

        let r3: Vec<(i32,)> = p.claim_collect(t3)?;
        assert_eq!(r3, vec![(3,)]);

        // t4 fails
        let result4: Result<Vec<(i32,)>, _> = p.claim_collect(t4);
        result4.unwrap_err();

        // t5 should fail due to aborted state
        let result5: Result<Vec<(i32,)>, _> = p.claim_collect(t5);
        assert!(result5.is_err());
        assert!(result5.unwrap_err().to_string().contains("aborted"));

        Ok(())
    });

    // Pipeline should recover after error via cleanup
    result?;
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test recovery after error - can start new batch
#[test]
fn pipeline_error_recovery_new_batch() -> Result<(), Error> {
    let mut conn = get_conn()?;

    // First pipeline with error
    let _ = conn.pipeline(|p| {
        let t1 = p.exec("SELECT 1/0", ())?;
        let _: Result<Vec<(i32,)>, _> = p.claim_collect(t1);
        Ok(())
    });

    // Second pipeline should work fine
    let result = conn.pipeline(|p| {
        let t1 = p.exec("SELECT $1::int", (100,))?;
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        Ok(r1)
    })?;

    assert_eq!(result, vec![(100,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test explicit flush then claim (no auto-sync)
#[test]
fn pipeline_explicit_flush() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let (r1, r2) = conn.pipeline(|p| {
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT $1::int", (2,))?;

        // Explicit flush - sends FLUSH not SYNC
        p.flush()?;

        // Claims don't auto-sync (buffer is empty)
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        let r2: Vec<(i32,)> = p.claim_collect(t2)?;

        // Cleanup will send SYNC on exit
        Ok((r1, r2))
    })?;

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test complex interleaving: exec, claim, exec, claim, exec, claim
#[test]
fn pipeline_complex_interleave() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let results = conn.pipeline(|p| {
        let mut results = Vec::new();

        for i in 1..=5 {
            let t = p.exec("SELECT $1::int", (i,))?;
            let r: Vec<(i32,)> = p.claim_collect(t)?;
            results.push(r[0].0);
        }

        Ok(results)
    })?;

    assert_eq!(results, vec![1, 2, 3, 4, 5]);
    verify_connection(&mut conn)?;
    Ok(())
}

/// Test pipeline continues after error batch with new batch
/// Batch 1: error -> claim all (some fail)
/// Batch 2: should work normally
#[test]
fn pipeline_continue_after_error_batch() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let (r1, r4, r5) = conn.pipeline(|p| {
        // First batch - has an error in the middle
        let t1 = p.exec("SELECT $1::int", (1,))?;
        let t2 = p.exec("SELECT 1/0", ())?; // Error
        let t3 = p.exec("SELECT $1::int", (3,))?;

        // Claim first batch
        let r1: Vec<(i32,)> = p.claim_collect(t1)?;
        assert_eq!(r1, vec![(1,)]);

        // t2 fails
        let result2: Result<Vec<(i32,)>, _> = p.claim_collect(t2);
        result2.unwrap_err();

        // t3 fails due to aborted
        let result3: Result<Vec<(i32,)>, _> = p.claim_collect(t3);
        result3.unwrap_err();

        // After consuming all claims from error batch (including ReadyForQuery),
        // pipeline should recover and allow new batch
        let t4 = p.exec("SELECT $1::int", (4,))?;
        let t5 = p.exec("SELECT $1::int", (5,))?;

        let r4: Vec<(i32,)> = p.claim_collect(t4)?;
        let r5: Vec<(i32,)> = p.claim_collect(t5)?;

        Ok((r1, r4, r5))
    })?;

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r4, vec![(4,)]);
    assert_eq!(r5, vec![(5,)]);
    verify_connection(&mut conn)?;
    Ok(())
}
