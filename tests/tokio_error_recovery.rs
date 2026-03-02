use zero_postgres::tokio::Conn;

async fn get_conn() -> Conn {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    Conn::new(db_url.as_str()).await.expect("failed to connect")
}

#[tokio::test]
async fn tokio_exec_drop_recovers_after_server_error() {
    let mut conn = get_conn().await;

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
}
