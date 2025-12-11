//! Benchmark: zero-postgres sync
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/test cargo run --example bench_zero_async

use std::env;
use zero_postgres::sync::Conn;

fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut conn = Conn::new(url.as_str())?;

    // Setup - use temp table (session-scoped, often memory-resident)
    conn.query_drop(
        "CREATE TEMP TABLE test_bench (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            email VARCHAR(100),
            score REAL,
            description VARCHAR(100)
        )",
    )?;

    conn.prepare(
        "insert_bench",
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
            conn.exec_drop(
                "insert_bench",
                (
                    username.as_str(),
                    *age,
                    email.as_str(),
                    *score,
                    description.as_str(),
                ),
            )?;
        }

        let elapsed = iteration_start.elapsed();
        let count: Vec<(i64,)> = conn.query_collect("SELECT COUNT(*) FROM test_bench")?;
        println!(
            "Iteration {}: Inserted {} rows (took {:.2}ms)",
            iteration,
            count[0].0,
            elapsed.as_secs_f64() * 1000.0
        );
        conn.query_drop("TRUNCATE TABLE test_bench")?;
    }

    conn.close_statement("insert_bench")?;
    conn.close()?;

    Ok(())
}
