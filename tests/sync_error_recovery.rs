use std::env;

use zero_postgres::sync::Conn;

fn get_conn() -> Conn {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    return Conn::new(db_url.as_str()).expect("failed to connect");
}

#[test]
fn test_sync_exec_drop_recovers_after_server_error() {
    let mut conn = get_conn();

    let first_err = conn
        .exec_drop("SELECT 1 / $1", (0_i32,))
        .expect_err("first statement should fail");
    assert!(
        first_err.to_string().contains("division by zero"),
        "unexpected first error: {first_err}"
    );

    let second_err = conn
        .exec_drop("SELECT 1 / $1", (0_i32,))
        .expect_err("second statement should fail");
    assert!(
        second_err.to_string().contains("division by zero"),
        "unexpected second error: {second_err}"
    );
}
