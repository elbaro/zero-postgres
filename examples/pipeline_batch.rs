//! Example: Pipeline mode for batch operations.
//!
//! Demonstrates using pipelines for efficient bulk inserts and updates,
//! significantly reducing round-trip latency compared to individual queries.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example pipeline_batch

use std::env;
use std::time::Instant;
use zero_postgres::sync::Conn;

fn main() -> zero_postgres::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("Connecting...");
    let mut conn = Conn::new(url.as_str())?;
    println!("Connected!\n");

    // Setup test table
    conn.query_drop("DROP TABLE IF EXISTS products")?;
    conn.query_drop(
        "CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0
        )",
    )?;
    println!("Created test table.\n");

    // === Batch Insert ===
    println!("=== Batch Insert (100 products) ===\n");

    let products: Vec<(&str, f64, i32)> = (1..=100)
        .map(|i| {
            let name: &'static str = Box::leak(format!("Product {}", i).into_boxed_str());
            (name, i as f64 * 9.99, i * 10)
        })
        .collect();

    let start = Instant::now();
    {
        // Prepare the insert statement once (outside the pipeline)
        let insert_stmt =
            conn.prepare("INSERT INTO products (name, price, quantity) VALUES ($1, $2, $3) RETURNING id")?;

        let ids = conn.run_pipeline(|p| {
            // Queue all inserts
            let mut tickets = Vec::new();
            for (name, price, qty) in &products {
                let t = p.exec(&insert_stmt, (*name, *price, *qty))?;
                tickets.push(t);
            }

            p.sync()?;

            // Claim all insert results
            let mut ids = Vec::new();
            for t in tickets {
                let id: Option<(i32,)> = p.claim_one(t)?;
                if let Some((id,)) = id {
                    ids.push(id);
                }
            }
            Ok(ids)
        })?;

        println!("Inserted {} products with IDs: {:?}...", ids.len(), &ids[..5]);
    }
    let elapsed = start.elapsed();
    println!("Pipeline insert took: {:?}\n", elapsed);

    // === Batch Update ===
    println!("=== Batch Update (increase all prices by 10%) ===\n");

    let start = Instant::now();
    {
        // Prepare update statement outside the pipeline
        let update_stmt =
            conn.prepare("UPDATE products SET price = price * $1 WHERE id = $2 RETURNING id, price")?;

        let updated = conn.run_pipeline(|p| {
            // Queue updates for first 50 products
            let mut tickets = Vec::new();
            for id in 1..=50 {
                let t = p.exec(&update_stmt, (1.10, id))?;
                tickets.push(t);
            }

            p.sync()?;

            let mut updated = 0;
            for t in tickets {
                // Note: price is NUMERIC which uses binary format, so we decode as f64
                let result: Vec<(i32, f64)> = p.claim_collect(t)?;
                if !result.is_empty() {
                    updated += 1;
                }
            }
            Ok(updated)
        })?;

        println!("Updated {} products", updated);
    }
    let elapsed = start.elapsed();
    println!("Pipeline update took: {:?}\n", elapsed);

    // === Mixed Operations ===
    println!("=== Mixed Operations (insert, select, update) ===\n");

    {
        // Prepare all statements in a batch
        let stmts = conn.prepare_batch(&[
            "INSERT INTO products (name, price, quantity) VALUES ($1, $2, $3) RETURNING id",
            "SELECT id, name, price FROM products WHERE price > $1 ORDER BY price DESC LIMIT 5",
            "UPDATE products SET quantity = quantity + $1 WHERE id = $2",
            "SELECT COUNT(*) FROM products",
        ])?;

        let (id1, id2, expensive, count) = conn.run_pipeline(|p| {
            // Queue mixed operations
            let t_insert1 = p.exec(&stmts[0], ("Special Product A", 999.99, 5))?;
            let t_insert2 = p.exec(&stmts[0], ("Special Product B", 1499.99, 3))?;
            let t_expensive = p.exec(&stmts[1], (500.0,))?;
            let t_update1 = p.exec(&stmts[2], (100, 1))?;
            let t_update2 = p.exec(&stmts[2], (100, 2))?;
            let t_count = p.exec(&stmts[3], ())?;

            p.sync()?;

            // Claim results
            let id1: Option<(i32,)> = p.claim_one(t_insert1)?;
            let id2: Option<(i32,)> = p.claim_one(t_insert2)?;
            let expensive: Vec<(i32, String, f64)> = p.claim_collect(t_expensive)?;
            p.claim_drop(t_update1)?;
            p.claim_drop(t_update2)?;
            let count: Option<(i64,)> = p.claim_one(t_count)?;

            Ok((id1, id2, expensive, count))
        })?;

        println!(
            "Inserted products with IDs: {:?}, {:?}",
            id1.map(|r| r.0),
            id2.map(|r| r.0)
        );
        println!("\nTop 5 expensive products:");
        for (id, name, price) in &expensive {
            println!("  id={}, name={}, price={:.2}", id, name, price);
        }
        println!("\nTotal products: {}", count.unwrap().0);
    }
    println!();

    // === Comparison: Pipeline vs Individual ===
    println!("=== Performance Comparison ===\n");

    // Reset table
    conn.query_drop("DELETE FROM products")?;

    // Individual inserts
    let start = Instant::now();
    for i in 1..=50 {
        conn.query_drop(&format!(
            "INSERT INTO products (name, price, quantity) VALUES ('Item {}', {}, {})",
            i,
            i as f64 * 1.99,
            i
        ))?;
    }
    let individual_time = start.elapsed();

    conn.query_drop("DELETE FROM products")?;

    // Pipeline inserts
    let start = Instant::now();
    {
        let insert_stmt =
            conn.prepare("INSERT INTO products (name, price, quantity) VALUES ($1, $2, $3)")?;

        conn.run_pipeline(|p| {
            let mut tickets = Vec::new();
            for i in 1..=50 {
                let name: &'static str = Box::leak(format!("Item {}", i).into_boxed_str());
                let t = p.exec(&insert_stmt, (name, i as f64 * 1.99, i))?;
                tickets.push(t);
            }

            p.sync()?;
            for t in tickets {
                p.claim_drop(t)?;
            }
            Ok(())
        })?;
    }
    let pipeline_time = start.elapsed();

    println!("50 inserts - Individual: {:?}", individual_time);
    println!("50 inserts - Pipeline:   {:?}", pipeline_time);
    println!(
        "Speedup: {:.1}x",
        individual_time.as_secs_f64() / pipeline_time.as_secs_f64()
    );
    println!();

    // Cleanup
    conn.query_drop("DROP TABLE products")?;
    println!("Cleaned up test table.");

    conn.close()?;
    println!("Connection closed.");

    Ok(())
}
