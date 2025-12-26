//! Tests for exec_portal and NamedPortal

use std::env;
use zero_postgres::sync::Conn;

fn get_conn() -> Conn {
    let db_url = env::var("DATABASE_URL").unwrap();
    Conn::new(db_url.as_str()).expect("Failed to connect")
}

#[test]
fn test_exec_portal_basic() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT generate_series(1, 5) as n").unwrap();

    let mut portal = conn.exec_portal(&stmt, ()).unwrap();
    assert!(!portal.is_complete());

    let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 0).unwrap();
    assert!(portal.is_complete());
    assert_eq!(rows.len(), 5);

    let total: i32 = rows.iter().map(|(n,)| n).sum();
    assert_eq!(total, 15); // 1+2+3+4+5

    portal.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_batched() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT generate_series(1, 10) as n").unwrap();

    let mut portal = conn.exec_portal(&stmt, ()).unwrap();
    let mut all_rows: Vec<i32> = Vec::new();
    let mut batches = 0;

    while !portal.is_complete() {
        let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 3).unwrap();
        all_rows.extend(rows.iter().map(|(n,)| *n));
        batches += 1;
    }

    assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(batches, 4); // 3+3+3+1 rows in 4 batches

    portal.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_empty_result() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT 1 WHERE false").unwrap();

    let mut portal = conn.exec_portal(&stmt, ()).unwrap();
    let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 0).unwrap();

    assert!(portal.is_complete());
    assert_eq!(rows.len(), 0);

    portal.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_with_params() {
    let mut conn = get_conn();

    let stmt = conn.prepare("SELECT generate_series(1, $1) as n").unwrap();

    let mut portal = conn.exec_portal(&stmt, (5i32,)).unwrap();
    let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 0).unwrap();

    assert_eq!(rows.len(), 5);
    let total: i32 = rows.iter().map(|(n,)| n).sum();
    assert_eq!(total, 15);

    portal.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_with_raw_sql() {
    let mut conn = get_conn();

    let mut portal = conn
        .exec_portal("SELECT generate_series(1, 5) as n", ())
        .unwrap();
    let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 0).unwrap();

    assert_eq!(rows.len(), 5);
    let total: i32 = rows.iter().map(|(n,)| n).sum();
    assert_eq!(total, 15);

    portal.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_with_raw_sql_and_params() {
    let mut conn = get_conn();

    let mut portal = conn
        .exec_portal("SELECT generate_series(1, $1) as n", (5i32,))
        .unwrap();
    let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 0).unwrap();

    assert_eq!(rows.len(), 5);
    let total: i32 = rows.iter().map(|(n,)| n).sum();
    assert_eq!(total, 15);

    portal.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_portal_name() {
    let mut conn = get_conn();

    let mut portal1 = conn.exec_portal("SELECT 1", ()).unwrap();
    let mut portal2 = conn.exec_portal("SELECT 2", ()).unwrap();

    // Each portal should have a unique name
    assert_ne!(portal1.name(), portal2.name());
    assert!(portal1.name().starts_with("_zero_p_"));
    assert!(portal2.name().starts_with("_zero_p_"));

    // Consume the portals
    let _: Vec<(i32,)> = portal1.execute_collect(&mut conn, 0).unwrap();
    let _: Vec<(i32,)> = portal2.execute_collect(&mut conn, 0).unwrap();

    portal1.close(&mut conn).unwrap();
    portal2.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_multiple_portals() {
    let mut conn = get_conn();

    // Create two portals
    let mut portal1 = conn
        .exec_portal("SELECT generate_series(1, 3) as n", ())
        .unwrap();
    let mut portal2 = conn
        .exec_portal("SELECT generate_series(10, 12) as n", ())
        .unwrap();

    // Interleave fetches from both portals
    let rows1: Vec<(i32,)> = portal1.execute_collect(&mut conn, 2).unwrap();
    let rows2: Vec<(i32,)> = portal2.execute_collect(&mut conn, 2).unwrap();

    assert_eq!(rows1.iter().map(|(n,)| *n).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(
        rows2.iter().map(|(n,)| *n).collect::<Vec<_>>(),
        vec![10, 11]
    );

    // Fetch remaining
    let rows1: Vec<(i32,)> = portal1.execute_collect(&mut conn, 0).unwrap();
    let rows2: Vec<(i32,)> = portal2.execute_collect(&mut conn, 0).unwrap();

    assert_eq!(rows1.iter().map(|(n,)| *n).collect::<Vec<_>>(), vec![3]);
    assert_eq!(rows2.iter().map(|(n,)| *n).collect::<Vec<_>>(), vec![12]);

    portal1.close(&mut conn).unwrap();
    portal2.close(&mut conn).unwrap();
}

#[test]
fn test_exec_portal_is_complete_tracking() {
    let mut conn = get_conn();

    let mut portal = conn
        .exec_portal("SELECT generate_series(1, 5) as n", ())
        .unwrap();

    assert!(!portal.is_complete());

    // Fetch 3 rows, should not be complete
    let _: Vec<(i32,)> = portal.execute_collect(&mut conn, 3).unwrap();
    assert!(!portal.is_complete());

    // Fetch remaining 2 rows, should be complete
    let _: Vec<(i32,)> = portal.execute_collect(&mut conn, 0).unwrap();
    assert!(portal.is_complete());

    portal.close(&mut conn).unwrap();
}
