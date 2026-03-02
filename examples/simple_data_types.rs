#![expect(clippy::non_ascii_literal)]
//! Example: Data types using simple query protocol with typed decoding.
//!
//! Tests PostgreSQL data types with boundary values using typed results.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example simple_data_types

use std::env;
use zero_postgres::sync::Conn;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("DATABASE_URL")?;

    println!("Connecting...");
    let mut conn = Conn::new(url.as_str())?;
    println!("Connected!\n");

    // Setup test table
    conn.query_drop("DROP TABLE IF EXISTS test_types")?;
    conn.query_drop(
        "CREATE TABLE test_types (
            id SERIAL PRIMARY KEY,
            col_bool BOOLEAN,
            col_smallint SMALLINT,
            col_int INTEGER,
            col_bigint BIGINT,
            col_real REAL,
            col_double DOUBLE PRECISION,
            col_text TEXT,
            col_bytea BYTEA
        )",
    )?;
    println!("Created test table.\n");

    // === Boolean ===
    println!("=== Boolean Type ===\n");

    conn.query_drop("INSERT INTO test_types (col_bool) VALUES (NULL), (TRUE), (FALSE)")?;

    let rows1: Vec<(i32, Option<bool>)> =
        conn.query_collect("SELECT id, col_bool FROM test_types ORDER BY id")?;
    println!("boolean:");
    for (id, val) in &rows1 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // === Integer types ===
    println!("=== Integer Types ===\n");

    // smallint: -32768 to 32767
    conn.query_drop("INSERT INTO test_types (col_smallint) VALUES (NULL), (0), (-32768), (32767)")?;

    let rows2: Vec<(i32, Option<i16>)> =
        conn.query_collect("SELECT id, col_smallint FROM test_types ORDER BY id")?;
    println!("smallint (range: -32768 to 32767):");
    for (id, val) in &rows2 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // integer: -2147483648 to 2147483647
    conn.query_drop(
        "INSERT INTO test_types (col_int) VALUES (NULL), (0), (-2147483648), (2147483647)",
    )?;

    let rows3: Vec<(i32, Option<i32>)> =
        conn.query_collect("SELECT id, col_int FROM test_types ORDER BY id")?;
    println!("integer (range: -2147483648 to 2147483647):");
    for (id, val) in &rows3 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // bigint: -9223372036854775808 to 9223372036854775807
    conn.query_drop(
        "INSERT INTO test_types (col_bigint) VALUES (NULL), (0), (-9223372036854775808), (9223372036854775807)",
    )?;

    let rows4: Vec<(i32, Option<i64>)> =
        conn.query_collect("SELECT id, col_bigint FROM test_types ORDER BY id")?;
    println!("bigint (range: -9223372036854775808 to 9223372036854775807):");
    for (id, val) in &rows4 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // === Floating point types ===
    println!("=== Floating Point Types ===\n");

    conn.query_drop(
        "INSERT INTO test_types (col_real) VALUES (NULL), (0), (3.14159), (-3.14159), ('Infinity'), ('-Infinity'), ('NaN')",
    )?;

    let rows5: Vec<(i32, Option<f32>)> =
        conn.query_collect("SELECT id, col_real FROM test_types ORDER BY id")?;
    println!("real (4 bytes, 6 decimal digits precision):");
    for (id, val) in &rows5 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    conn.query_drop(
        "INSERT INTO test_types (col_double) VALUES (NULL), (0), (3.141592653589793), ('Infinity'), ('NaN')",
    )?;

    let rows6: Vec<(i32, Option<f64>)> =
        conn.query_collect("SELECT id, col_double FROM test_types ORDER BY id")?;
    println!("double precision (8 bytes, 15 decimal digits precision):");
    for (id, val) in &rows6 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // === Text types ===
    println!("=== Text Types ===\n");

    conn.query_drop("INSERT INTO test_types (col_text) VALUES (NULL)")?;
    conn.query_drop("INSERT INTO test_types (col_text) VALUES ('')")?;
    conn.query_drop("INSERT INTO test_types (col_text) VALUES ('hello')")?;
    conn.query_drop("INSERT INTO test_types (col_text) VALUES ('unicode: éñü')")?;
    conn.query_drop("INSERT INTO test_types (col_text) VALUES ('emoji: 😀')")?;
    conn.query_drop("INSERT INTO test_types (col_text) VALUES ('japanese: こんにちは')")?;

    let rows7: Vec<(i32, Option<String>)> =
        conn.query_collect("SELECT id, col_text FROM test_types ORDER BY id")?;
    println!("text:");
    for (id, val) in &rows7 {
        println!("  id={}, value={:?}", id, val);
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // === Binary data ===
    println!("=== Binary Type ===\n");

    conn.query_drop("INSERT INTO test_types (col_bytea) VALUES (NULL)")?;
    conn.query_drop("INSERT INTO test_types (col_bytea) VALUES (E'\\\\x')")?;
    conn.query_drop("INSERT INTO test_types (col_bytea) VALUES (E'\\\\xDEADBEEF')")?;

    let rows8: Vec<(i32, Option<Vec<u8>>)> =
        conn.query_collect("SELECT id, col_bytea FROM test_types ORDER BY id")?;
    println!("bytea:");
    for (id, val) in &rows8 {
        match val {
            Some(bytes) => println!("  id={}, value={:02X?}", id, bytes),
            None => println!("  id={}, value=NULL", id),
        }
    }
    conn.query_drop("DELETE FROM test_types")?;
    println!();

    // === Mixed types in one query ===
    println!("=== Mixed Types ===\n");

    conn.query_drop(
        "INSERT INTO test_types (col_bool, col_int, col_double, col_text)
         VALUES (TRUE, 42, 3.14, 'hello')",
    )?;

    type MixedRow = (i32, Option<bool>, Option<i32>, Option<f64>, Option<String>);
    let rows9: Vec<MixedRow> =
        conn.query_collect("SELECT id, col_bool, col_int, col_double, col_text FROM test_types")?;
    println!("Mixed row:");
    for (id, b, i, d, t) in &rows9 {
        println!(
            "  id={}, bool={:?}, int={:?}, double={:?}, text={:?}",
            id, b, i, d, t
        );
    }
    println!();

    // === Query first row ===
    println!("=== Query First Row ===\n");

    let first: Option<(i32, Option<String>)> =
        conn.query_first("SELECT id, col_text FROM test_types ORDER BY id LIMIT 1")?;
    println!("First row: {:?}", first);
    println!();

    // Cleanup
    conn.query_drop("DROP TABLE test_types")?;
    println!("Cleaned up test table.");

    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
