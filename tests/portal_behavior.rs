//! PostgreSQL Extended Protocol Portal Tests
//!
//! Tests portal (BIND/EXECUTE) behavior with transactions using zero-postgres lowlevel API.
//!
//! Test Matrix for Named Portals (EN0-EN6):
//! - EN0a/EN0b: Implicit tx ended by SYNC (closes) vs FLUSH (keeps open)
//! - EN1-EN3: Portal created BEFORE BEGIN, then COMMIT/ROLLBACK/ERROR
//! - EN4-EN6: Portal created WITHIN explicit tx, then COMMIT/ROLLBACK/ERROR
//!
//! Test Matrix for Unnamed Portals (EU1-EU4):
//! - EU1: Implicit tx ended by SYNC (closes)
//! - EU2: Implicit tx ended by ERROR (closes)
//! - EU3: Implicit tx with FLUSH (keeps open)
//! - EU4: Unnamed portal replacement on BIND
//!
//! Execution Persistence Tests (P1-P4):
//! - P1: INSERT + COMMIT -> rows persist
//! - P2: INSERT + ROLLBACK -> rows gone
//! - P3: INSERT + ERROR + ROLLBACK -> rows gone
//! - P4: Named portal WITHIN tx + ROLLBACK -> portal gone, cannot INSERT

use std::env;
use zero_postgres::handler::DropHandler;
use zero_postgres::sync::Conn;

fn get_conn() -> Conn {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    // Disable TLS if not specified (for testing without sync-tls feature)
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str()).expect("Failed to connect")
}

/// Helper to check if a portal exists by trying to execute it.
/// Returns false only if the error is "portal does not exist" (SQLSTATE 34000).
/// Panics on other errors (e.g., execution errors).
fn portal_exists(conn: &mut Conn, portal: &str) -> bool {
    let mut handler = DropHandler::new();
    match conn.lowlevel_execute(portal, 0, &mut handler) {
        Ok(_) => true,
        Err(e) => {
            // SQLSTATE 34000 = invalid_cursor_name (portal does not exist)
            if e.sqlstate() == Some("34000") {
                false
            } else {
                panic!("Unexpected error while checking portal existence: {e}");
            }
        }
    }
}

// === Named Portal Tests ===

/// EN0a: Named portal in implicit tx, then SYNC -> portal should be gone
#[test]
fn test_en0a_named_portal_implicit_tx_sync() {
    let mut conn = get_conn();

    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();

    // Portal should exist within the implicit tx
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal does not exist in tx"
    );

    // SYNC ends implicit tx
    conn.lowlevel_sync().unwrap();

    // Portal should be gone after implicit tx ended
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after SYNC"
    );

    let _ = conn.lowlevel_sync();
}

/// EN0b: Named portal in implicit tx, then FLUSH -> portal still exists
#[test]
fn test_en0b_named_portal_implicit_tx_flush() {
    let mut conn = get_conn();

    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();

    // FLUSH does NOT end implicit tx
    conn.lowlevel_flush().unwrap();

    // Portal should still exist (FLUSH doesn't end tx)
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal gone after FLUSH (should still exist)"
    );

    // Clean up with SYNC
    let _ = conn.lowlevel_sync();
}

/// EN1: Named portal created BEFORE BEGIN, then COMMIT -> portal should be gone
#[test]
fn test_en1_named_portal_before_begin_commit() {
    let mut conn = get_conn();

    // Prepare statement
    let stmt1 = conn.prepare("SELECT 1").unwrap();

    // Create named portal BEFORE BEGIN
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();

    // BEGIN explicit transaction (portal was created before this)
    conn.query_drop("BEGIN").unwrap();

    // Portal should still exist within the transaction
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal does not exist in tx"
    );

    // COMMIT the transaction
    conn.query_drop("COMMIT").unwrap();

    // Portal should be gone after COMMIT
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after COMMIT"
    );

    // Sync to clear any error state
    let _ = conn.lowlevel_sync();
}

/// EN2: Named portal created BEFORE BEGIN, then ROLLBACK -> portal should be gone
#[test]
fn test_en2_named_portal_before_begin_rollback() {
    let mut conn = get_conn();

    // Prepare statement
    let stmt1 = conn.prepare("SELECT 1").unwrap();

    // Create named portal BEFORE BEGIN
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();

    // BEGIN explicit transaction
    conn.query_drop("BEGIN").unwrap();

    // Portal should still exist within the transaction
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal does not exist in tx"
    );

    // ROLLBACK the transaction
    conn.query_drop("ROLLBACK").unwrap();

    // Portal should be gone after ROLLBACK
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after ROLLBACK"
    );

    let _ = conn.lowlevel_sync();
}

/// EN3: Named portal created BEFORE BEGIN, ERROR occurs -> portal should be gone
#[test]
fn test_en3_named_portal_before_begin_error() {
    let mut conn = get_conn();

    // Prepare statement
    let stmt1 = conn.prepare("SELECT 1").unwrap();

    // Create named portal BEFORE BEGIN
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();

    // BEGIN explicit transaction
    conn.query_drop("BEGIN").unwrap();

    // Portal should still exist within the transaction
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal does not exist in tx"
    );

    // Cause an error - tx is now aborted
    let _ = conn.query_drop("SELECT 1/0");

    // Portal should be unusable in aborted tx
    let mut handler = DropHandler::new();
    let result = conn.lowlevel_execute("portal1", 0, &mut handler);
    assert!(result.is_err(), "Portal usable in aborted tx (should fail)");

    // Sync and rollback
    let _ = conn.lowlevel_sync();
    conn.query_drop("ROLLBACK").unwrap();

    // Portal should be gone after rollback
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after ERROR + ROLLBACK"
    );

    let _ = conn.lowlevel_sync();
}

/// EN4: Named portal created WITHIN explicit tx + COMMIT -> portal should be gone
#[test]
fn test_en4_named_portal_within_tx_commit() {
    let mut conn = get_conn();

    conn.query_drop("BEGIN").unwrap();
    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();
    conn.query_drop("COMMIT").unwrap();

    // Portal should be gone after COMMIT
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after explicit COMMIT"
    );

    let _ = conn.lowlevel_sync();
}

/// EN5: Named portal created WITHIN explicit tx + ROLLBACK -> portal should be gone
#[test]
fn test_en5_named_portal_within_tx_rollback() {
    let mut conn = get_conn();

    conn.query_drop("BEGIN").unwrap();
    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();
    conn.query_drop("ROLLBACK").unwrap();

    // Portal should be gone after ROLLBACK
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after explicit ROLLBACK"
    );

    let _ = conn.lowlevel_sync();
}

/// EN6: Named portal created WITHIN tx + ERROR -> portal unusable, gone after rollback
#[test]
fn test_en6_named_portal_within_tx_error() {
    let mut conn = get_conn();

    conn.query_drop("BEGIN").unwrap();
    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("portal1", &stmt1.wire_name(), ())
        .unwrap();

    // Cause an error
    let _ = conn.query_drop("SELECT 1/0");

    // Portal should be unusable in aborted tx
    let mut handler = DropHandler::new();
    let result = conn.lowlevel_execute("portal1", 0, &mut handler);
    assert!(result.is_err(), "Portal usable in aborted tx (should fail)");

    // Sync and rollback
    let _ = conn.lowlevel_sync();
    conn.query_drop("ROLLBACK").unwrap();

    // Portal should be gone after rollback
    assert!(
        !portal_exists(&mut conn, "portal1"),
        "Portal still exists after ROLLBACK from error"
    );

    let _ = conn.lowlevel_sync();
}

// === Unnamed Portal Tests ===

/// EU1: Unnamed portal in implicit tx, then SYNC -> portal gone
#[test]
fn test_eu1_unnamed_portal_implicit_tx_sync() {
    let mut conn = get_conn();

    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("", &stmt1.wire_name(), ()).unwrap();

    // SYNC ends implicit tx
    conn.lowlevel_sync().unwrap();

    // Unnamed portal should be gone after implicit tx ended
    assert!(
        !portal_exists(&mut conn, ""),
        "Unnamed portal still exists after SYNC"
    );

    let _ = conn.lowlevel_sync();
}

/// EU3: Unnamed portal in implicit tx, then FLUSH -> portal still exists
#[test]
fn test_eu3_unnamed_portal_implicit_tx_flush() {
    let mut conn = get_conn();

    let stmt1 = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("", &stmt1.wire_name(), ()).unwrap();

    // FLUSH does NOT end implicit tx
    conn.lowlevel_flush().unwrap();

    // Unnamed portal should still exist (FLUSH doesn't end tx)
    assert!(
        portal_exists(&mut conn, ""),
        "Unnamed portal gone after FLUSH (should still exist)"
    );

    // Clean up with SYNC
    let _ = conn.lowlevel_sync();
}

/// EU2: Unnamed portal in implicit tx + ERROR -> portal gone
#[test]
fn test_eu2_unnamed_portal_implicit_tx_error() {
    let mut conn = get_conn();

    // Create a table with zero to force runtime evaluation
    conn.query_drop("DROP TABLE IF EXISTS _err_test").unwrap();
    conn.query_drop("CREATE TEMP TABLE _err_test (n int)")
        .unwrap();
    conn.query_drop("INSERT INTO _err_test VALUES (0)").unwrap();

    let stmt1 = conn.prepare("SELECT 1").unwrap();
    let err_stmt = conn.prepare("SELECT 1/n FROM _err_test").unwrap();

    // Create unnamed portal
    conn.lowlevel_bind("", &stmt1.wire_name(), ()).unwrap();

    // Cause an error via extended protocol
    conn.lowlevel_bind("err_portal", &err_stmt.wire_name(), ())
        .unwrap();
    let mut handler = DropHandler::new();
    let _ = conn.lowlevel_execute("err_portal", 0, &mut handler);

    // SYNC to complete implicit rollback
    let _ = conn.lowlevel_sync();

    // Unnamed portal should be gone after error
    assert!(
        !portal_exists(&mut conn, ""),
        "Unnamed portal still exists after ERROR"
    );

    let _ = conn.lowlevel_sync();
}

/// EU4: Unnamed portal + new BIND -> old portal replaced
#[test]
fn test_eu4_unnamed_portal_replaced_by_new_bind() {
    let mut conn = get_conn();

    let stmt1 = conn.prepare("SELECT 1 as a").unwrap();
    let stmt2 = conn.prepare("SELECT 2 as b").unwrap();

    // Bind first statement to unnamed portal
    conn.lowlevel_bind("", &stmt1.wire_name(), ()).unwrap();

    // Bind second statement to unnamed portal (should replace)
    conn.lowlevel_bind("", &stmt2.wire_name(), ()).unwrap();

    // Execute unnamed portal - should get result from stmt2
    let rows: Vec<(i32,)> = {
        let mut handler = zero_postgres::handler::CollectHandler::new();
        conn.lowlevel_execute("", 0, &mut handler).unwrap();
        handler.into_rows()
    };

    conn.lowlevel_sync().unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2, "Expected result from stmt2, got {:?}", rows);
}

// === Execution Persistence Tests ===

/// P1: INSERT via portal + COMMIT -> rows persist
#[test]
fn test_p1_insert_commit_persists() {
    let mut conn = get_conn();

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS portal_test_p1")
        .unwrap();
    conn.query_drop("CREATE TABLE portal_test_p1 (id int)")
        .unwrap();

    conn.query_drop("BEGIN").unwrap();
    let ins = conn
        .prepare("INSERT INTO portal_test_p1 VALUES (1)")
        .unwrap();
    conn.lowlevel_bind("", &ins.wire_name(), ()).unwrap();
    let mut handler = DropHandler::new();
    conn.lowlevel_execute("", 0, &mut handler).unwrap();
    conn.lowlevel_sync().unwrap();
    conn.query_drop("COMMIT").unwrap();

    // Check persistence
    let count: Vec<(i64,)> = conn
        .query_collect("SELECT count(*) FROM portal_test_p1")
        .unwrap();
    conn.query_drop("DROP TABLE portal_test_p1").ok();

    assert_eq!(count[0].0, 1, "Expected 1 row after COMMIT");
}

/// P2: INSERT via portal + ROLLBACK -> rows gone
#[test]
fn test_p2_insert_rollback_gone() {
    let mut conn = get_conn();

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS portal_test_p2")
        .unwrap();
    conn.query_drop("CREATE TABLE portal_test_p2 (id int)")
        .unwrap();

    conn.query_drop("BEGIN").unwrap();
    let ins = conn
        .prepare("INSERT INTO portal_test_p2 VALUES (1)")
        .unwrap();
    conn.lowlevel_bind("", &ins.wire_name(), ()).unwrap();
    let mut handler = DropHandler::new();
    conn.lowlevel_execute("", 0, &mut handler).unwrap();
    conn.lowlevel_sync().unwrap();
    conn.query_drop("ROLLBACK").unwrap();

    // Check rows are gone
    let count: Vec<(i64,)> = conn
        .query_collect("SELECT count(*) FROM portal_test_p2")
        .unwrap();
    conn.query_drop("DROP TABLE portal_test_p2").ok();

    assert_eq!(count[0].0, 0, "Expected 0 rows after ROLLBACK");
}

/// P3: INSERT via portal + ERROR + ROLLBACK -> rows gone
#[test]
fn test_p3_insert_error_rollback_gone() {
    let mut conn = get_conn();

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS portal_test_p3")
        .unwrap();
    conn.query_drop("CREATE TABLE portal_test_p3 (id int)")
        .unwrap();

    conn.query_drop("BEGIN").unwrap();
    let ins_p3 = conn
        .prepare("INSERT INTO portal_test_p3 VALUES (1)")
        .unwrap();
    conn.lowlevel_bind("", &ins_p3.wire_name(), ()).unwrap();
    let mut handler = DropHandler::new();
    conn.lowlevel_execute("", 0, &mut handler).unwrap();
    conn.lowlevel_sync().unwrap();

    // Cause an error - tx is now aborted
    let err = conn.query_drop("SELECT 1/0");
    assert!(err.is_err(), "Expected error from SELECT 1/0");

    // ROLLBACK the aborted transaction
    conn.query_drop("ROLLBACK").unwrap();

    // Use a fresh connection to check rows - avoids potential state issues
    let mut conn2 = get_conn();
    let count: Vec<(i64,)> = conn2
        .query_collect("SELECT count(*) FROM portal_test_p3")
        .unwrap();
    conn2.query_drop("DROP TABLE portal_test_p3").ok();

    assert_eq!(count[0].0, 0, "Expected 0 rows after ERROR + ROLLBACK");
}

/// P4: Named portal created WITHIN tx, ROLLBACK, then try to INSERT -> portal gone, insert fails
#[test]
fn test_p4_named_portal_within_tx_rollback_then_insert() {
    let mut conn = get_conn();

    // Setup
    conn.query_drop("DROP TABLE IF EXISTS portal_test_p4")
        .unwrap();
    conn.query_drop("CREATE TABLE portal_test_p4 (id int)")
        .unwrap();

    // BEGIN and create named portal for INSERT
    conn.query_drop("BEGIN").unwrap();
    let ins_p4 = conn
        .prepare("INSERT INTO portal_test_p4 VALUES (1)")
        .unwrap();
    conn.lowlevel_bind("portal_ins", &ins_p4.wire_name(), ())
        .unwrap();
    conn.lowlevel_sync().unwrap();

    // ROLLBACK the transaction
    conn.query_drop("ROLLBACK").unwrap();

    // Try to execute the (now gone) portal - should fail with "portal does not exist"
    let mut handler = DropHandler::new();
    let result = conn.lowlevel_execute("portal_ins", 0, &mut handler);
    assert!(
        result.is_err(),
        "Execute should fail on non-existent portal"
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.sqlstate(),
        Some("34000"),
        "Expected SQLSTATE 34000 (invalid_cursor_name)"
    );

    let _ = conn.lowlevel_sync();

    // Verify no rows were inserted
    let count: Vec<(i64,)> = conn
        .query_collect("SELECT count(*) FROM portal_test_p4")
        .unwrap();
    conn.query_drop("DROP TABLE portal_test_p4").ok();

    assert_eq!(
        count[0].0, 0,
        "Expected 0 rows - portal was destroyed by ROLLBACK"
    );
}

/// SYNC inside explicit transaction does NOT destroy named portals.
///
/// This is the key behavior that makes Transaction::exec_portal safe:
/// portals created within BEGIN/COMMIT survive intermediate SYNC messages.
#[test]
fn test_sync_inside_explicit_tx_preserves_portal() {
    let mut conn = get_conn();

    conn.query_drop("BEGIN").unwrap();

    let stmt = conn.prepare("SELECT 1").unwrap();
    conn.lowlevel_bind("portal1", &stmt.wire_name(), ())
        .unwrap();

    // Portal should exist
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal should exist after bind"
    );

    // SYNC while inside explicit transaction - should NOT destroy portal
    conn.lowlevel_sync().unwrap();

    // Portal should still exist
    assert!(
        portal_exists(&mut conn, "portal1"),
        "Portal was destroyed by SYNC inside explicit transaction!"
    );

    conn.query_drop("ROLLBACK").unwrap();
}
