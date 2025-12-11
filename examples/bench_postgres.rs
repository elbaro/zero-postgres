//! Benchmark: postgres (sync)
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/test cargo run --example bench_postgres

use postgres::NoTls;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    // postgres crate expects postgres:// not pg://
    let url = url
        .strip_prefix("pg://")
        .map(|s| format!("postgres://{}", s))
        .unwrap_or(url);
    let mut client = postgres::Client::connect(&url, NoTls)?;

    // Setup - use temp table (session-scoped, often memory-resident)
    client.execute(
        "CREATE TEMP TABLE test_bench (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            email VARCHAR(100),
            score REAL,
            description VARCHAR(100)
        )",
        &[],
    )?;

    let stmt = client.prepare(
        "INSERT INTO test_bench (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
    )?;

    const N: usize = 10000;
    let mut rows = Vec::with_capacity(N);
    for i in 0..N {
        rows.push((
            format!("user_{}", i),
            (20 + (i % 50)) as i32,
            format!("user{}@example.com", i),
            (i % 100) as f32 / 10.0,
            format!("Description for user {}", i),
        ));
    }

    for iteration in 0..10 {
        let iteration_start = std::time::Instant::now();

        for (username, age, email, score, description) in rows.iter() {
            client.execute(&stmt, &[username, age, email, score, description])?;
        }

        let elapsed = iteration_start.elapsed();
        let row = client.query_one("SELECT COUNT(*) FROM test_bench", &[])?;
        let count: i64 = row.get(0);
        println!(
            "Iteration {}: Inserted {} rows (took {:.2}ms)",
            iteration,
            count,
            elapsed.as_secs_f64() * 1000.0
        );
        client.execute("TRUNCATE TABLE test_bench", &[])?;
    }

    Ok(())
}
