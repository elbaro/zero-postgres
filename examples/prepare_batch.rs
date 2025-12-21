//! Example: Batch statement preparation.
//!
//! Demonstrates using `prepare_batch()` to prepare multiple statements
//! in a single round-trip, which is more efficient than calling `prepare()`
//! multiple times when you need to prepare several statements upfront.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example prepare_batch

use std::env;
use std::time::Instant;
use zero_postgres::sync::Conn;

fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("Connecting...");
    let mut conn = Conn::new(url.as_str())?;
    println!("Connected!\n");

    // Setup test table
    conn.query_drop("DROP TABLE IF EXISTS users")?;
    conn.query_drop(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )",
    )?;
    println!("Created test table.\n");

    // === Prepare multiple statements in a single round-trip ===
    println!("=== Batch Prepare ===\n");

    let start = Instant::now();
    let stmts = conn.prepare_batch(&[
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
        "SELECT id, name, email FROM users WHERE id = $1",
        "UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name",
        "DELETE FROM users WHERE id = $1 RETURNING id",
        "SELECT COUNT(*) FROM users",
    ])?;
    let batch_time = start.elapsed();
    println!("Batch prepared {} statements in {:?}", stmts.len(), batch_time);

    // Compare with individual prepares
    let start = Instant::now();
    let _stmt1 = conn.prepare("INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id")?;
    let _stmt2 = conn.prepare("SELECT id, name, email FROM users WHERE id = $1")?;
    let _stmt3 = conn.prepare("UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name")?;
    let _stmt4 = conn.prepare("DELETE FROM users WHERE id = $1 RETURNING id")?;
    let _stmt5 = conn.prepare("SELECT COUNT(*) FROM users")?;
    let individual_time = start.elapsed();
    println!("Individually prepared 5 statements in {:?}", individual_time);
    println!(
        "Speedup: {:.1}x\n",
        individual_time.as_secs_f64() / batch_time.as_secs_f64()
    );

    // === Use the prepared statements ===
    println!("=== Using Prepared Statements ===\n");

    // Insert some users
    let insert_stmt = &stmts[0];
    let select_stmt = &stmts[1];
    let update_stmt = &stmts[2];
    let delete_stmt = &stmts[3];
    let count_stmt = &stmts[4];

    // Insert users
    let id1: Option<(i32,)> = conn.exec_first(insert_stmt, ("Alice", "alice@example.com"))?;
    let id2: Option<(i32,)> = conn.exec_first(insert_stmt, ("Bob", "bob@example.com"))?;
    let id3: Option<(i32,)> = conn.exec_first(insert_stmt, ("Charlie", "charlie@example.com"))?;

    println!("Inserted users with IDs: {:?}, {:?}, {:?}", id1, id2, id3);

    // Select a user
    let user: Option<(i32, String, String)> = conn.exec_first(select_stmt, (1,))?;
    println!("Selected user: {:?}", user);

    // Update a user
    let updated: Option<(i32, String)> = conn.exec_first(update_stmt, ("Alice Smith", 1))?;
    println!("Updated user: {:?}", updated);

    // Count users
    let count: Option<(i64,)> = conn.exec_first(count_stmt, ())?;
    println!("Total users: {:?}", count);

    // Delete a user
    let deleted: Option<(i32,)> = conn.exec_first(delete_stmt, (2,))?;
    println!("Deleted user: {:?}", deleted);

    // Count again
    let count: Option<(i64,)> = conn.exec_first(count_stmt, ())?;
    println!("Users after delete: {:?}", count);

    println!();

    // Cleanup
    conn.query_drop("DROP TABLE users")?;
    println!("Cleaned up test table.");

    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
