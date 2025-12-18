//! Integration tests for the typed Pipeline API.
//!
//! Tests the new pipeline API that supports:
//! - Preparing statements within the pipeline
//! - Creating named portals for incremental fetching
//! - Multiple executions on the same portal with row limits
//! - Typed handles and the Harvest trait
//!
//! ## Test Matrix
//!
//! ### Basic Pipeline Tests
//! - `test_pipeline_prepare_exec` - Basic prepare + exec flow
//! - `test_pipeline_multiple_execs` - Multiple exec calls on same prepared statement
//! - `test_pipeline_no_rows` - Query returning no rows
//! - `test_pipeline_multiple_rows` - Query returning multiple rows
//!
//! ### Portal Tests (Incremental Fetching)
//! - `test_pipeline_portal_incremental` - Portal with multiple execute calls using row limits
//! - `test_pipeline_portal_all_at_once` - Portal execute with unlimited rows
//!
//! ### Error Handling Tests
//! - `test_pipeline_harvest_order_error` - Validates harvest ordering enforcement
//! - `test_pipeline_sync_then_harvest` - Sync followed by harvest works correctly
//! - `test_pipeline_sql_error` - SQL error propagation
//! - `test_pipeline_aborted_state` - Pipeline abort state after error
//!
//! ### INSERT/UPDATE/DELETE Tests
//! - `test_pipeline_insert` - INSERT in pipeline
//! - `test_pipeline_insert_returning` - INSERT with RETURNING clause
//!
//! ### Reuse Tests
//! - `test_pipeline_reuse_prepared_statement` - Harvested PreparedStatement usable outside pipeline
//!
//! ### Edge Cases
//! - `test_pipeline_empty` - Empty pipeline (just sync)
//! - `test_pipeline_prepare_only` - Pipeline with only prepare (no exec)
//! - `test_pipeline_pending_count` - Pending count tracking

use std::env;
use zero_postgres::sync::{Conn, ExecResult};

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

/// Test basic prepare + exec flow
#[test]
fn test_pipeline_prepare_exec() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    // Prepare a statement
    let prep = p.prepare("SELECT $1::int as num, $2::text as txt").unwrap();

    // Execute it
    let q = p.exec::<(i32, String), _>(&prep, (42, "hello")).unwrap();

    // Sync to complete the pipeline
    p.sync().unwrap();

    // Harvest in order
    let stmt = p.harvest(prep).unwrap();
    let result: ExecResult<(i32, String)> = p.harvest(q).unwrap();

    // Verify results
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], (42, "hello".to_string()));
    assert!(!result.suspended);

    // The harvested PreparedStatement should be usable
    assert!(!stmt.wire_name().is_empty());
}

/// Test multiple exec calls on the same prepared statement
#[test]
fn test_pipeline_multiple_execs() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep = p.prepare("SELECT $1::int").unwrap();
    let q1 = p.exec::<(i32,), _>(&prep, (1,)).unwrap();
    let q2 = p.exec::<(i32,), _>(&prep, (2,)).unwrap();
    let q3 = p.exec::<(i32,), _>(&prep, (3,)).unwrap();

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    let r1: ExecResult<(i32,)> = p.harvest(q1).unwrap();
    let r2: ExecResult<(i32,)> = p.harvest(q2).unwrap();
    let r3: ExecResult<(i32,)> = p.harvest(q3).unwrap();

    assert_eq!(r1.rows, vec![(1,)]);
    assert_eq!(r2.rows, vec![(2,)]);
    assert_eq!(r3.rows, vec![(3,)]);
}

/// Test query that returns no rows
#[test]
fn test_pipeline_no_rows() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep = p.prepare("SELECT 1 WHERE false").unwrap();
    let q = p.exec::<(i32,), _>(&prep, ()).unwrap();

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    let result: ExecResult<(i32,)> = p.harvest(q).unwrap();

    assert!(result.rows.is_empty());
    assert!(!result.suspended);
}

/// Test query with multiple rows
#[test]
fn test_pipeline_multiple_rows() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep = p
        .prepare("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)")
        .unwrap();
    let q = p.exec::<(i32,), _>(&prep, ()).unwrap();

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    let result: ExecResult<(i32,)> = p.harvest(q).unwrap();

    assert_eq!(result.rows, vec![(1,), (2,), (3,)]);
}

// === Portal Tests (Incremental Fetching) ===

/// Test portal with multiple execute calls
#[test]
fn test_pipeline_portal_incremental() {
    let mut conn = get_conn();

    // Create a table with multiple rows
    conn.query_drop("DROP TABLE IF EXISTS _pipeline_portal_test")
        .unwrap();
    conn.query_drop(
        "CREATE TEMP TABLE _pipeline_portal_test AS SELECT generate_series(1, 10) as n",
    )
    .unwrap();

    let mut p = conn.pipeline();

    let prep = p
        .prepare("SELECT n FROM _pipeline_portal_test ORDER BY n")
        .unwrap();
    let portal = p.bind(&prep, ()).unwrap();

    // Execute with row limit
    let batch1 = p.execute::<(i32,)>(&portal, 3).unwrap();
    let batch2 = p.execute::<(i32,)>(&portal, 3).unwrap();
    let batch3 = p.execute::<(i32,)>(&portal, 10).unwrap(); // Request more than remaining

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    p.harvest(portal).unwrap();

    let r1: ExecResult<(i32,)> = p.harvest(batch1).unwrap();
    let r2: ExecResult<(i32,)> = p.harvest(batch2).unwrap();
    let r3: ExecResult<(i32,)> = p.harvest(batch3).unwrap();

    // First batch: 3 rows, suspended
    assert_eq!(r1.rows, vec![(1,), (2,), (3,)]);
    assert!(r1.suspended, "First batch should be suspended");

    // Second batch: 3 rows, suspended
    assert_eq!(r2.rows, vec![(4,), (5,), (6,)]);
    assert!(r2.suspended, "Second batch should be suspended");

    // Third batch: remaining 4 rows, not suspended
    assert_eq!(r3.rows, vec![(7,), (8,), (9,), (10,)]);
    assert!(!r3.suspended, "Third batch should not be suspended");
}

/// Test portal with execute that gets all rows at once
#[test]
fn test_pipeline_portal_all_at_once() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep = p
        .prepare("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)")
        .unwrap();
    let portal = p.bind(&prep, ()).unwrap();
    let batch = p.execute::<(i32,)>(&portal, 0).unwrap(); // 0 = unlimited

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    p.harvest(portal).unwrap();
    let result: ExecResult<(i32,)> = p.harvest(batch).unwrap();

    assert_eq!(result.rows, vec![(1,), (2,), (3,)]);
    assert!(!result.suspended);
}

// === Error Handling Tests ===

/// Test harvest order validation
#[test]
fn test_pipeline_harvest_order_error() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep = p.prepare("SELECT 1").unwrap();
    let q = p.exec::<(i32,), _>(&prep, ()).unwrap();

    p.sync().unwrap();

    // Try to harvest q before prep - should fail
    let result: Result<ExecResult<(i32,)>, _> = p.harvest(q);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("out of order"),
        "Expected 'out of order' error, got: {}",
        err
    );
}

/// Test that sync succeeds and we can harvest after
#[test]
fn test_pipeline_sync_then_harvest() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep = p.prepare("SELECT 1").unwrap();
    let q = p.exec::<(i32,), _>(&prep, ()).unwrap();

    // Sync succeeds (just sends the Sync message)
    p.sync().unwrap();

    // Now harvest in order
    let _stmt = p.harvest(prep).unwrap();
    let result: ExecResult<(i32,)> = p.harvest(q).unwrap();
    assert_eq!(result.rows, vec![(1,)]);
}

/// Test SQL error propagation
#[test]
fn test_pipeline_sql_error() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    // Prepare a statement that will fail at execution time
    let prep = p.prepare("SELECT 1/0").unwrap();
    let q = p.exec::<(i32,), _>(&prep, ()).unwrap();

    p.sync().unwrap();

    // Harvesting prepare should succeed
    let _stmt = p.harvest(prep).unwrap();

    // Harvesting the exec should fail with division by zero
    let result: Result<ExecResult<(i32,)>, _> = p.harvest(q);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("division by zero"),
        "Expected 'division by zero' error, got: {}",
        err
    );
}

/// Test aborted pipeline state
#[test]
fn test_pipeline_aborted_state() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();

    let prep1 = p.prepare("SELECT 1").unwrap();
    let q1 = p.exec::<(i32,), _>(&prep1, ()).unwrap();
    let prep2 = p.prepare("SELECT 1/0").unwrap(); // This will fail
    let q2 = p.exec::<(i32,), _>(&prep2, ()).unwrap();
    let prep3 = p.prepare("SELECT 2").unwrap();
    let _q3 = p.exec::<(i32,), _>(&prep3, ()).unwrap();

    p.sync().unwrap();

    // First operations succeed
    let _stmt1 = p.harvest(prep1).unwrap();
    let r1: ExecResult<(i32,)> = p.harvest(q1).unwrap();
    assert_eq!(r1.rows, vec![(1,)]);

    let _stmt2 = p.harvest(prep2).unwrap();

    // q2 fails - pipeline becomes aborted
    let result: Result<ExecResult<(i32,)>, _> = p.harvest(q2);
    assert!(result.is_err());

    // Subsequent harvests should also fail due to aborted state
    let result3 = p.harvest(prep3);
    assert!(result3.is_err());
    let err = result3.unwrap_err();
    assert!(
        err.to_string().contains("aborted"),
        "Expected 'aborted' error, got: {}",
        err
    );
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

    let mut p = conn.pipeline();

    let prep = p
        .prepare("INSERT INTO _pipeline_insert_test VALUES ($1, $2)")
        .unwrap();
    let q1 = p.exec::<(), _>(&prep, (1, "alice")).unwrap();
    let q2 = p.exec::<(), _>(&prep, (2, "bob")).unwrap();

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    p.harvest(q1).unwrap();
    p.harvest(q2).unwrap();

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

    let mut p = conn.pipeline();

    let prep = p
        .prepare("INSERT INTO _pipeline_returning_test (name) VALUES ($1) RETURNING id")
        .unwrap();
    let q1 = p.exec::<(i32,), _>(&prep, ("alice",)).unwrap();
    let q2 = p.exec::<(i32,), _>(&prep, ("bob",)).unwrap();

    p.sync().unwrap();

    let _stmt = p.harvest(prep).unwrap();
    let r1: ExecResult<(i32,)> = p.harvest(q1).unwrap();
    let r2: ExecResult<(i32,)> = p.harvest(q2).unwrap();

    assert_eq!(r1.rows.len(), 1);
    assert_eq!(r2.rows.len(), 1);
    assert!(r1.rows[0].0 < r2.rows[0].0, "IDs should be sequential");
}

// === Reusing PreparedStatement after harvest ===

/// Test that harvested PreparedStatement can be used for regular queries
#[test]
fn test_pipeline_reuse_prepared_statement() {
    let mut conn = get_conn();

    // Prepare in pipeline
    let mut p = conn.pipeline();
    let prep = p.prepare("SELECT $1::int * 2").unwrap();
    let q = p.exec::<(i32,), _>(&prep, (5,)).unwrap();
    p.sync().unwrap();
    let stmt = p.harvest(prep).unwrap();
    let r: ExecResult<(i32,)> = p.harvest(q).unwrap();
    assert_eq!(r.rows, vec![(10,)]);

    // Now use the statement outside the pipeline
    let rows: Vec<(i32,)> = conn.exec_collect(&stmt, (7,)).unwrap();
    assert_eq!(rows, vec![(14,)]);

    let rows: Vec<(i32,)> = conn.exec_collect(&stmt, (100,)).unwrap();
    assert_eq!(rows, vec![(200,)]);
}

// === Edge Cases ===

/// Test empty pipeline (just sync)
#[test]
fn test_pipeline_empty() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();
    p.sync().unwrap();
    // Should complete without error
}

/// Test pipeline with only prepare (no exec)
#[test]
fn test_pipeline_prepare_only() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();
    let prep = p.prepare("SELECT 1").unwrap();
    p.sync().unwrap();
    let stmt = p.harvest(prep).unwrap();

    // Statement should be usable
    let rows: Vec<(i32,)> = conn.exec_collect(&stmt, ()).unwrap();
    assert_eq!(rows, vec![(1,)]);
}

/// Test pending_count tracking
#[test]
fn test_pipeline_pending_count() {
    let mut conn = get_conn();

    let mut p = conn.pipeline();
    assert_eq!(p.pending_count(), 0);

    let prep = p.prepare("SELECT 1").unwrap();
    assert_eq!(p.pending_count(), 1);

    let _q = p.exec::<(i32,), _>(&prep, ()).unwrap();
    assert_eq!(p.pending_count(), 2);

    p.sync().unwrap();

    p.harvest(prep).unwrap();
    assert_eq!(p.pending_count(), 1);

    let _: ExecResult<(i32,)> = p.harvest(_q).unwrap();
    assert_eq!(p.pending_count(), 0);
}
