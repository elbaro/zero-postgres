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
        println!("  Secret Key: {:?}", key.secret_key());
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

    let rows: Vec<(String,)> = conn.query_collect("SELECT version()")?;
    if let Some((version,)) = rows.first() {
        println!("  Version: {}", version);
    }

    let rows: Vec<(String, String, Option<String>, Option<i32>)> = conn.query_collect(
        "SELECT current_database(), current_user, host(inet_server_addr()), inet_server_port()",
    )?;
    if let Some((db, user, addr, port)) = rows.first() {
        println!("  Database: {}", db);
        println!("  User: {}", user);
        println!("  Server Address: {}", addr.as_deref().unwrap_or("(null)"));
        println!(
            "  Server Port: {}",
            port.map(|p| p.to_string()).unwrap_or("(null)".into())
        );
    }

    let rows: Vec<(String,)> = conn.query_collect("SHOW server_encoding")?;
    if let Some((enc,)) = rows.first() {
        println!("  Server Encoding: {}", enc);
    }

    let rows: Vec<(String,)> = conn.query_collect("SELECT pg_postmaster_start_time()::text")?;
    if let Some((time,)) = rows.first() {
        println!("  Server Start Time: {}", time);
    }

    println!();
    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
