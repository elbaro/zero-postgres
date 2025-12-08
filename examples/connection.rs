//! Example: Connection information
//!
//! Connects to PostgreSQL and prints various server and connection details.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example connection

use std::env;
use zero_postgres::sync::Conn;

fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("Connecting...");
    let mut conn = Conn::new(url.as_str())?;
    println!("Connected!\n");

    // Print backend process info
    if let Some(key) = conn.backend_key() {
        println!("=== Backend Process ===");
        println!("  Process ID: {}", key.process_id());
        println!("  Secret Key: {}", key.secret());
        println!();
    }

    // Print server parameters
    println!("=== Server Parameters ===");
    for (name, value) in conn.server_params() {
        println!("  {}: {}", name, value);
    }
    println!();

    // Print transaction status
    println!("=== Connection State ===");
    println!("  Transaction Status: {:?}", conn.transaction_status());
    println!("  In Transaction: {}", conn.in_transaction());
    println!("  Is Broken: {}", conn.is_broken());
    println!();

    // Query additional server info
    println!("=== Server Info (from queries) ===");

    let (_, rows) = conn.query_collect("SELECT version()")?;
    if let Some(row) = rows.first() {
        if let Some(Some(version)) = row.first() {
            println!("  Version: {}", String::from_utf8_lossy(version));
        }
    }

    let (_, rows) = conn.query_collect(
        "SELECT current_database(), current_user, inet_server_addr(), inet_server_port()",
    )?;
    if let Some(row) = rows.first() {
        let get = |i: usize| {
            row.get(i)
                .and_then(|v| v.as_ref())
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_else(|| "(null)".into())
        };
        println!("  Database: {}", get(0));
        println!("  User: {}", get(1));
        println!("  Server Address: {}", get(2));
        println!("  Server Port: {}", get(3));
    }

    let (_, rows) = conn.query_collect("SHOW server_encoding")?;
    if let Some(row) = rows.first() {
        if let Some(Some(enc)) = row.first() {
            println!("  Server Encoding: {}", String::from_utf8_lossy(enc));
        }
    }

    let (_, rows) = conn.query_collect("SELECT pg_postmaster_start_time()")?;
    if let Some(row) = rows.first() {
        if let Some(Some(time)) = row.first() {
            println!("  Server Start Time: {}", String::from_utf8_lossy(time));
        }
    }

    println!();
    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
