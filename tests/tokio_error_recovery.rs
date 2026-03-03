#![allow(clippy::panic_in_result_fn, clippy::shadow_unrelated)]

use zero_postgres::Error;
use zero_postgres::tokio::Conn;

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

#[tokio::test]
async fn tokio_exec_drop_recovers_after_server_error() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let first_err = conn
        .exec_drop("SELECT 1 / $1", (0_i32,))
        .await
        .expect_err("first statement should fail");
    assert!(
        first_err.to_string().contains("division by zero"),
        "unexpected first error: {first_err}"
    );

    let second_err = conn
        .exec_drop("SELECT 1 / $1", (0_i32,))
        .await
        .expect_err("second statement should fail");
    assert!(
        second_err.to_string().contains("division by zero"),
        "unexpected second error: {second_err}"
    );
    Ok(())
}
