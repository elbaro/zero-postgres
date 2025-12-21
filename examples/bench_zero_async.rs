//! Benchmark: zero-postgres async (tokio)
//!
//! Note: This benchmark does not use pipeline mode as async pipeline is not yet implemented.
//! For pipeline benchmarks, use bench_zero_sync.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/test cargo run --example bench_zero_async

use std::env;
use zero_postgres::tokio::Conn;

#[tokio::main(flavor = "current_thread")]
async fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut conn = Conn::new(url.as_str()).await?;

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
    )
    .await?;

    let insert_stmt = conn
        .prepare(
            "INSERT INTO test_bench (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
        )
        .await?;

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
                &insert_stmt,
                (
                    username.as_str(),
                    *age,
                    email.as_str(),
                    *score,
                    description.as_str(),
                ),
            )
            .await?;
        }

        let elapsed = iteration_start.elapsed();
        let count: Vec<(i64,)> = conn
            .query_collect("SELECT COUNT(*) FROM test_bench")
            .await?;
        println!(
            "Iteration {}: Inserted {} rows (took {:.2}ms)",
            iteration,
            count[0].0,
            elapsed.as_secs_f64() * 1000.0
        );
        conn.query_drop("TRUNCATE TABLE test_bench").await?;
    }

    conn.close_statement(&insert_stmt).await?;
    conn.close().await?;

    Ok(())
}
