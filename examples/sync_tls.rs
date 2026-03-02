//! Example: Synchronous TLS connection
//!
//! Connects to PostgreSQL over TLS and executes a few queries.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example sync_tls --features sync-tls

use std::env;
use zero_postgres::sync::Conn;
use zero_postgres::{Opts, SslMode};

#[expect(clippy::print_stdout)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("DATABASE_URL")?;

    // Parse URL and force TLS
    let mut opts: Opts = url.as_str().try_into()?;
    opts.ssl_mode = SslMode::Require;

    println!("Connecting with TLS...");
    let mut conn = Conn::new(opts)?;
    println!("Connected!\n");

    // Query 1: Check SSL status
    println!("=== SSL Status ===");
    let rows1: Vec<(bool,)> =
        conn.query_collect("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")?;
    if let Some((ssl_enabled,)) = rows1.first() {
        println!("  SSL enabled: {}", ssl_enabled);
    }

    // Query 2: Get server version
    println!("\n=== Server Version ===");
    let rows2: Vec<(String,)> = conn.query_collect("SELECT version()")?;
    if let Some((version,)) = rows2.first() {
        println!("  {}", version);
    }

    // Query 3: Current timestamp
    println!("\n=== Current Time ===");
    let rows3: Vec<(String,)> = conn.query_collect("SELECT now()::text")?;
    if let Some((now,)) = rows3.first() {
        println!("  Server time: {}", now);
    }

    // Query 4: Simple calculation
    println!("\n=== Calculation ===");
    let rows4: Vec<(i32,)> = conn.query_collect("SELECT 1 + 2 + 3")?;
    if let Some((sum,)) = rows4.first() {
        println!("  1 + 2 + 3 = {}", sum);
    }

    // Query 5: Generate series
    println!("\n=== Generate Series ===");
    let rows5: Vec<(i32,)> = conn.query_collect("SELECT generate_series(1, 5)")?;
    print!("  Series: ");
    for (i, (n,)) in rows5.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", n);
    }
    println!();

    println!();
    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
