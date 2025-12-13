//! Example: Asynchronous TLS connection
//!
//! Connects to PostgreSQL over TLS asynchronously and executes a few queries.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example async_tls --features tokio-tls

use std::env;
use zero_postgres::tokio::Conn;
use zero_postgres::{Opts, SslMode};

#[allow(clippy::print_stdout)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Parse URL and force TLS
    let mut opts: Opts = url.as_str().try_into()?;
    opts.ssl_mode = SslMode::Require;

    println!("Connecting with TLS...");
    let mut conn = Conn::new(opts).await?;
    println!("Connected!\n");

    // Query 1: Check SSL status
    println!("=== SSL Status ===");
    let rows: Vec<(bool,)> = conn
        .query_collect("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .await?;
    if let Some((ssl_enabled,)) = rows.first() {
        println!("  SSL enabled: {}", ssl_enabled);
    }

    // Query 2: Get server version
    println!("\n=== Server Version ===");
    let rows: Vec<(String,)> = conn.query_collect("SELECT version()").await?;
    if let Some((version,)) = rows.first() {
        println!("  {}", version);
    }

    // Query 3: Current timestamp
    println!("\n=== Current Time ===");
    let rows: Vec<(String,)> = conn.query_collect("SELECT now()::text").await?;
    if let Some((now,)) = rows.first() {
        println!("  Server time: {}", now);
    }

    // Query 4: Simple calculation
    println!("\n=== Calculation ===");
    let rows: Vec<(i32,)> = conn.query_collect("SELECT 1 + 2 + 3").await?;
    if let Some((sum,)) = rows.first() {
        println!("  1 + 2 + 3 = {}", sum);
    }

    // Query 5: Generate series
    println!("\n=== Generate Series ===");
    let rows: Vec<(i32,)> = conn.query_collect("SELECT generate_series(1, 5)").await?;
    print!("  Series: ");
    for (i, (n,)) in rows.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", n);
    }
    println!();

    println!();
    conn.close().await?;
    println!("Connection closed.");

    Ok(())
}
