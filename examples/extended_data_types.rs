//! Example: Data types using extended query protocol with typed decoding.
//!
//! Tests PostgreSQL data types with prepared statements and typed results.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example extended_data_types

use std::env;
use zero_postgres::sync::Conn;

fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

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
        "insert_row",
        "INSERT INTO test_extended (col_bool, col_int, col_bigint, col_double, col_text, col_bytea)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )?;
    println!("Prepared insert statement: {:?}", insert_stmt.name);
    println!("  Parameter OIDs: {:?}", insert_stmt.param_oids);

    let select_stmt = conn.prepare(
        "select_all",
        "SELECT id, col_bool, col_int, col_bigint, col_double, col_text FROM test_extended ORDER BY id",
    )?;
    println!("Prepared select statement: {:?}", select_stmt.name);
    if let Some(cols) = &select_stmt.columns {
        println!("  Result columns:");
        for col in cols {
            println!("    - {} (OID: {})", col.name, col.type_oid());
        }
    }
    println!();

    // === Insert rows using prepared statement ===
    println!("=== Inserting Rows ===\n");

    // Row 1: All values
    conn.exec_drop(
        "insert_row",
        (
            true,                          // bool
            42_i32,                        // int
            i64::MAX,                      // bigint (max)
            3.14159_f64,                   // double
            "hello world",                 // text
            &[0xDE, 0xAD, 0xBE, 0xEF][..], // bytea
        ),
    )?;
    println!("Inserted row 1 (all values)");

    // Row 2: With NULLs
    conn.exec_drop(
        "insert_row",
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
        "insert_row",
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

    let rows: Vec<(
        i32,
        Option<bool>,
        Option<i32>,
        Option<i64>,
        Option<f64>,
        Option<String>,
    )> = conn.exec_collect("select_all", ())?;

    println!("Retrieved {} rows:", rows.len());
    for (id, b, i, bi, d, t) in &rows {
        println!(
            "  id={}, bool={:?}, int={:?}, bigint={:?}, double={:?}, text={:?}",
            id, b, i, bi, d, t
        );
    }
    println!();

    // === Prepare and execute with parameter ===
    println!("=== Parameterized Query ===\n");

    conn.prepare(
        "select_by_id",
        "SELECT id, col_text FROM test_extended WHERE id = $1",
    )?;

    let rows: Vec<(i32, Option<String>)> = conn.exec_collect("select_by_id", (1_i32,))?;
    println!("Query with id=1: {:?}", rows);

    let rows: Vec<(i32, Option<String>)> = conn.exec_collect("select_by_id", (2_i32,))?;
    println!("Query with id=2: {:?}", rows);

    let rows: Vec<(i32, Option<String>)> = conn.exec_collect("select_by_id", (999_i32,))?;
    println!("Query with id=999 (not found): {:?}", rows);
    println!();

    // === Close statements ===
    println!("=== Closing Statements ===\n");

    conn.close_statement("insert_row")?;
    println!("Closed insert_row statement");

    conn.close_statement("select_all")?;
    println!("Closed select_all statement");

    conn.close_statement("select_by_id")?;
    println!("Closed select_by_id statement");
    println!();

    // Cleanup
    conn.query_drop("DROP TABLE test_extended")?;
    println!("Cleaned up test table.");

    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
