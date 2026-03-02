//! Test that a connection remains usable after an extended-query server error.
//!
//! Verifies the state machine drains to ReadyForQuery internally,
//! so the next query sees a clean protocol state.

#![cfg(feature = "compio")]
#![allow(
    clippy::expect_used,
    clippy::panic_in_result_fn,
    clippy::shadow_unrelated,
    clippy::unwrap_used
)]

use zero_postgres::Error;
use zero_postgres::compio::Conn;

async fn get_conn() -> Result<Conn, Error> {
    let mut db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str()).await
}

#[compio::test]
async fn exec_drop_reusable_after_server_error() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let first_err = conn
        .exec_drop("SELECT 1 / $1", (0_i32,))
        .await
        .expect_err("division by zero should fail");
    assert!(
        first_err.to_string().contains("division by zero"),
        "unexpected first error: {first_err}"
    );

    let second_err = conn
        .exec_drop("SELECT 1 / $1", (0_i32,))
        .await
        .expect_err("division by zero should fail again");
    assert!(
        second_err.to_string().contains("division by zero"),
        "unexpected second error (protocol desync?): {second_err}"
    );

    let rows: Vec<(i32,)> = conn.exec_collect("SELECT $1::int", (42_i32,)).await?;
    assert_eq!(rows[0].0, 42);
    Ok(())
}
