//! Example: Pipeline mode for batching queries.
//!
//! Pipeline mode allows sending multiple queries without waiting for responses,
//! reducing round-trip latency. This is especially useful for bulk operations.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example pipeline

use std::env;
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
            active BOOLEAN NOT NULL DEFAULT TRUE
        )",
    )?;
    println!("Created test table.\n");

    // Insert some test data
    conn.query_drop("INSERT INTO users (name, active) VALUES ('Alice', true)")?;
    conn.query_drop("INSERT INTO users (name, active) VALUES ('Bob', false)")?;
    conn.query_drop("INSERT INTO users (name, active) VALUES ('Charlie', true)")?;
    conn.query_drop("INSERT INTO users (name, active) VALUES ('Diana', false)")?;
    println!("Inserted test data.\n");

    // === Using Prepared Statements ===
    println!("=== Pipeline with Prepared Statements ===\n");

    {
        // Prepare statements outside the pipeline
        let stmts = conn.prepare_batch(&[
            "SELECT id, name FROM users WHERE active = $1",
            "SELECT COUNT(*) FROM users",
        ])?;

        let (active, inactive, count) = conn.run_pipeline(|p| {
            // Execute with different parameters - all queries are sent together
            let t1 = p.exec(&stmts[0], (true,))?;
            let t2 = p.exec(&stmts[0], (false,))?;
            let t3 = p.exec(&stmts[1], ())?;

            // Send sync to flush all commands
            p.sync()?;

            // Claim results in the order they were queued
            let active: Vec<(i32, String)> = p.claim_collect(t1)?;
            let inactive: Vec<(i32, String)> = p.claim_collect(t2)?;
            let count: Vec<(i64,)> = p.claim_collect(t3)?;

            Ok((active, inactive, count))
        })?;

        println!("Active users:");
        for (id, name) in &active {
            println!("  id={}, name={}", id, name);
        }

        println!("\nInactive users:");
        for (id, name) in &inactive {
            println!("  id={}, name={}", id, name);
        }

        println!("\nTotal count: {}", count[0].0);
    }
    println!();

    // === Using Raw SQL ===
    println!("=== Pipeline with Raw SQL ===\n");

    {
        let (names, alice, bob) = conn.run_pipeline(|p| {
            // Execute raw SQL directly (no need to prepare)
            let t1 = p.exec("SELECT name FROM users ORDER BY name", ())?;
            let t2 = p.exec("SELECT name FROM users WHERE id = $1", (1_i32,))?;
            let t3 = p.exec("SELECT name FROM users WHERE id = $1", (2_i32,))?;

            p.sync()?;

            let names: Vec<(String,)> = p.claim_collect(t1)?;
            let alice: Option<(String,)> = p.claim_one(t2)?;
            let bob: Option<(String,)> = p.claim_one(t3)?;

            Ok((names, alice, bob))
        })?;

        println!(
            "All names: {:?}",
            names.iter().map(|r| &r.0).collect::<Vec<_>>()
        );
        println!("User 1: {:?}", alice.map(|r| r.0));
        println!("User 2: {:?}", bob.map(|r| r.0));
    }
    println!();

    // === Mixed: Prepared + Raw SQL ===
    println!("=== Mixed Pipeline ===\n");

    {
        let stmt = conn.prepare("SELECT id, name FROM users WHERE id > $1")?;

        let (all, active_count) = conn.run_pipeline(|p| {
            // Mix prepared statements and raw SQL
            let t1 = p.exec(&stmt, (0_i32,))?;
            let t2 = p.exec("SELECT COUNT(*) FROM users WHERE active = $1", (true,))?;
            let t3 = p.exec(&stmt, (2_i32,))?;

            p.sync()?;

            let all: Vec<(i32, String)> = p.claim_collect(t1)?;
            let active_count: Option<(i64,)> = p.claim_one(t2)?;
            p.claim_drop(t3)?;

            Ok((all, active_count))
        })?;

        println!("All users (id > 0): {} rows", all.len());
        println!("Active user count: {}", active_count.unwrap().0);
    }
    println!();

    // Cleanup
    conn.query_drop("DROP TABLE users")?;
    println!("Cleaned up test table.");

    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
