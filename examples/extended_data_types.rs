#![expect(clippy::non_ascii_literal)]
//! Example: Data types using extended query protocol with typed decoding.
//!
//! Tests PostgreSQL data types with prepared statements and typed results.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example extended_data_types

use std::env;
use zero_postgres::sync::Conn;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("DATABASE_URL")?;

    println!("Connecting...");
    let mut conn = Conn::new(url.as_str())?;
    println!("Connected!\n");

    // Setup test table
    conn.query_drop("DROP TABLE IF EXISTS test_extended")?;
    conn.query_drop(
        "CREATE TABLE test_extended (
            id SERIAL PRIMARY KEY,
            col_bool BOOLEAN,
            col_int INTEGER,
            col_bigint BIGINT,
            col_double DOUBLE PRECISION,
            col_text TEXT,
            col_bytea BYTEA
        )",
    )?;
    println!("Created test table.\n");

    // === Prepare statements ===
    println!("=== Preparing Statements ===\n");

    let insert_stmt = conn.prepare(
        "INSERT INTO test_extended (col_bool, col_int, col_bigint, col_double, col_text, col_bytea)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )?;
    println!("Prepared insert statement: {}", insert_stmt.wire_name());
    println!("  Parameter OIDs: {:?}", insert_stmt.param_oids);

    let select_stmt = conn.prepare(
        "SELECT id, col_bool, col_int, col_bigint, col_double, col_text FROM test_extended ORDER BY id",
    )?;
    println!("Prepared select statement: {}", select_stmt.wire_name());
    if let Some(Ok(cols)) = select_stmt.parse_columns() {
        println!("  Result columns:");
        for col in cols.iter() {
            println!("    - {} (OID: {})", col.name, col.type_oid());
        }
    }
    println!();

    // === Insert rows using prepared statement ===
    println!("=== Inserting Rows ===\n");

    // Row 1: All values
    conn.exec_drop(
        &insert_stmt,
        (
            true,                          // bool
            42_i32,                        // int
            i64::MAX,                      // bigint (max)
            std::f64::consts::PI,          // double
            "hello world",                 // text
            &[0xDE, 0xAD, 0xBE, 0xEF][..], // bytea
        ),
    )?;
    println!("Inserted row 1 (all values)");

    // Row 2: With NULLs
    conn.exec_drop(
        &insert_stmt,
        (
            None::<bool>,   // NULL bool
            i32::MIN,       // int (min)
            None::<i64>,    // NULL bigint
            f64::INFINITY,  // double infinity
            "unicode: éñü", // text with unicode
            None::<&[u8]>,  // NULL bytea
        ),
    )?;
    println!("Inserted row 2 (with NULLs)");

    // Row 3: Edge cases
    conn.exec_drop(
        &insert_stmt,
        (
            false,    // false
            0_i32,    // zero
            i64::MIN, // bigint (min)
            f64::NAN, // NaN
            "",       // empty string
            &[][..],  // empty bytea
        ),
    )?;
    println!("Inserted row 3 (edge cases)");
    println!();

    // === Select using prepared statement with typed results ===
    println!("=== Selecting with Typed Results ===\n");

    type ExtendedRow = (
        i32,
        Option<bool>,
        Option<i32>,
        Option<i64>,
        Option<f64>,
        Option<String>,
    );
    let rows1: Vec<ExtendedRow> = conn.exec_collect(&select_stmt, ())?;

    println!("Retrieved {} rows:", rows1.len());
    for (id, b, i, bi, d, t) in &rows1 {
        println!(
            "  id={}, bool={:?}, int={:?}, bigint={:?}, double={:?}, text={:?}",
            id, b, i, bi, d, t
        );
    }
    println!();

    // === Prepare and execute with parameter ===
    println!("=== Parameterized Query ===\n");

    let select_by_id_stmt = conn.prepare("SELECT id, col_text FROM test_extended WHERE id = $1")?;

    let rows2: Vec<(i32, Option<String>)> = conn.exec_collect(&select_by_id_stmt, (1_i32,))?;
    println!("Query with id=1: {:?}", rows2);

    let rows3: Vec<(i32, Option<String>)> = conn.exec_collect(&select_by_id_stmt, (2_i32,))?;
    println!("Query with id=2: {:?}", rows3);

    let rows4: Vec<(i32, Option<String>)> = conn.exec_collect(&select_by_id_stmt, (999_i32,))?;
    println!("Query with id=999 (not found): {:?}", rows4);
    println!();

    // === Close statements ===
    println!("=== Closing Statements ===\n");

    conn.close_statement(&insert_stmt)?;
    println!("Closed insert statement");

    conn.close_statement(&select_stmt)?;
    println!("Closed select statement");

    conn.close_statement(&select_by_id_stmt)?;
    println!("Closed select_by_id statement");
    println!();

    // Cleanup
    conn.query_drop("DROP TABLE test_extended")?;
    println!("Cleaned up test table.");

    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
