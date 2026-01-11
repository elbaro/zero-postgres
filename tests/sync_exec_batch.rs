//! Tests for exec_batch

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_postgres::sync::Conn;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

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

struct TestTable {
    name: String,
}

impl TestTable {
    fn new(conn: &mut Conn) -> Self {
        let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("exec_batch_test_{}", id);
        conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
            .unwrap();
        conn.query_drop(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, name TEXT, value INT)",
            name
        ))
        .unwrap();
        Self { name }
    }

    fn insert_sql(&self) -> String {
        format!("INSERT INTO {} (name, value) VALUES ($1, $2)", self.name)
    }

    fn count_sql(&self) -> String {
        format!("SELECT COUNT(*) FROM {}", self.name)
    }

    fn cleanup(&self, conn: &mut Conn) {
        let _ = conn.query_drop(&format!("DROP TABLE IF EXISTS {}", self.name));
    }
}

#[test]
fn test_exec_batch_with_raw_sql() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    conn.exec_batch(
        table.insert_sql().as_str(),
        &[("alice", 10), ("bob", 20), ("charlie", 30)],
    )
    .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 3);

    let rows: Vec<(String, i32)> = conn
        .query_collect(&format!(
            "SELECT name, value FROM {} ORDER BY value",
            table.name
        ))
        .unwrap();
    assert_eq!(rows[0], ("alice".to_string(), 10));
    assert_eq!(rows[1], ("bob".to_string(), 20));
    assert_eq!(rows[2], ("charlie".to_string(), 30));

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_with_prepared_statement() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    let stmt = conn.prepare(table.insert_sql().as_str()).unwrap();

    conn.exec_batch(&stmt, &[("dave", 40), ("eve", 50)])
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 2);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_empty_params() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Empty params should be a no-op
    let empty: &[(&str, i32)] = &[];
    conn.exec_batch(table.insert_sql().as_str(), empty).unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 0);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_single_item() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    conn.exec_batch(table.insert_sql().as_str(), &[("single", 100)])
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 1);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_chunked() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Create 10 items, chunk by 3 (should result in 4 syncs: 3+3+3+1)
    let params: Vec<(&str, i32)> = (0..10).map(|i| ("user", i)).collect();

    conn.exec_batch_chunked(table.insert_sql().as_str(), &params, 3)
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 10);

    // Verify values
    let rows: Vec<(i32,)> = conn
        .query_collect(&format!("SELECT value FROM {} ORDER BY value", table.name))
        .unwrap();
    let values: Vec<i32> = rows.into_iter().map(|(v,)| v).collect();
    assert_eq!(values, (0..10).collect::<Vec<i32>>());

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_chunked_exact_multiple() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // 6 items with chunk size 3 = exactly 2 chunks
    let params: Vec<(&str, i32)> = (0..6).map(|i| ("user", i)).collect();

    conn.exec_batch_chunked(table.insert_sql().as_str(), &params, 3)
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 6);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_chunked_size_one() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Chunk size 1 means each item gets its own sync
    conn.exec_batch_chunked(
        table.insert_sql().as_str(),
        &[("a", 1), ("b", 2), ("c", 3)],
        1,
    )
    .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 3);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_chunked_size_larger_than_items() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Chunk size larger than items = single chunk
    conn.exec_batch_chunked(table.insert_sql().as_str(), &[("a", 1), ("b", 2)], 1000)
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 2);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_with_transaction() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Start a transaction, do batch insert, commit
    conn.query_drop("BEGIN").unwrap();

    conn.exec_batch(table.insert_sql().as_str(), &[("tx1", 1), ("tx2", 2)])
        .unwrap();

    conn.query_drop("COMMIT").unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 2);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_with_transaction_rollback() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Start a transaction, do batch insert, rollback
    conn.query_drop("BEGIN").unwrap();

    conn.exec_batch(table.insert_sql().as_str(), &[("tx1", 1), ("tx2", 2)])
        .unwrap();

    conn.query_drop("ROLLBACK").unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 0);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_error_recovery() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Add a unique constraint
    conn.query_drop(&format!(
        "ALTER TABLE {} ADD CONSTRAINT {}_unique_value UNIQUE (value)",
        table.name, table.name
    ))
    .unwrap();

    // Insert some valid data first
    conn.exec_batch(table.insert_sql().as_str(), &[("first", 100)])
        .unwrap();

    // Try to insert duplicate - should fail
    let result = conn.exec_batch(
        table.insert_sql().as_str(),
        &[("second", 100)], // duplicate value
    );
    assert!(result.is_err());

    // Connection should still be usable after error
    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 1); // Only the first insert succeeded

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_multiple_batches_same_connection() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // First batch
    conn.exec_batch(
        table.insert_sql().as_str(),
        &[("batch1_a", 1), ("batch1_b", 2)],
    )
    .unwrap();

    // Second batch
    conn.exec_batch(
        table.insert_sql().as_str(),
        &[("batch2_a", 3), ("batch2_b", 4)],
    )
    .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 4);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_with_null_values() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    conn.exec_batch(
        table.insert_sql().as_str(),
        &[
            (Some("with_value"), Some(10)),
            (None::<&str>, Some(20)),
            (Some("null_value"), None::<i32>),
            (None::<&str>, None::<i32>),
        ],
    )
    .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 4);

    let null_names: Vec<(i64,)> = conn
        .query_collect(&format!(
            "SELECT COUNT(*) FROM {} WHERE name IS NULL",
            table.name
        ))
        .unwrap();
    assert_eq!(null_names[0].0, 2);

    let null_values: Vec<(i64,)> = conn
        .query_collect(&format!(
            "SELECT COUNT(*) FROM {} WHERE value IS NULL",
            table.name
        ))
        .unwrap();
    assert_eq!(null_values[0].0, 2);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_large_batch() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Create a large batch that will trigger multiple chunks with default chunk size
    let params: Vec<(&str, i32)> = (0..2500).map(|i| ("user", i)).collect();

    conn.exec_batch(table.insert_sql().as_str(), &params)
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 2500);

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_update() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Insert initial data
    conn.exec_batch(table.insert_sql().as_str(), &[("a", 1), ("b", 2), ("c", 3)])
        .unwrap();

    // Batch update
    let update_sql = format!(
        "UPDATE {} SET value = value + $1 WHERE name = $2",
        table.name
    );
    conn.exec_batch(update_sql.as_str(), &[(10, "a"), (20, "b"), (30, "c")])
        .unwrap();

    let rows: Vec<(String, i32)> = conn
        .query_collect(&format!(
            "SELECT name, value FROM {} ORDER BY name",
            table.name
        ))
        .unwrap();
    assert_eq!(rows[0], ("a".to_string(), 11));
    assert_eq!(rows[1], ("b".to_string(), 22));
    assert_eq!(rows[2], ("c".to_string(), 33));

    table.cleanup(&mut conn);
}

#[test]
fn test_exec_batch_delete() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Insert initial data
    conn.exec_batch(
        table.insert_sql().as_str(),
        &[("a", 1), ("b", 2), ("c", 3), ("d", 4)],
    )
    .unwrap();

    // Batch delete
    let delete_sql = format!("DELETE FROM {} WHERE name = $1", table.name);
    conn.exec_batch(delete_sql.as_str(), &[("a",), ("c",)])
        .unwrap();

    let count: Vec<(i64,)> = conn.query_collect(table.count_sql().as_str()).unwrap();
    assert_eq!(count[0].0, 2);

    let remaining: Vec<(String,)> = conn
        .query_collect(&format!("SELECT name FROM {} ORDER BY name", table.name))
        .unwrap();
    assert_eq!(remaining[0].0, "b");
    assert_eq!(remaining[1].0, "d");

    table.cleanup(&mut conn);
}
