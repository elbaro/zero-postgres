//! Tests for async exec_portal and NamedPortal (compio)

#![cfg(feature = "experimental-compio")]

use std::env;
use zero_postgres::compio::Conn;

async fn get_conn() -> Conn {
    let db_url = env::var("DATABASE_URL").unwrap();
    Conn::new(db_url.as_str()).await.expect("Failed to connect")
}

#[compio::test]
async fn test_exec_portal_basic() {
    let mut conn = get_conn().await;

    let stmt = conn
        .prepare("SELECT generate_series(1, 5) as n")
        .await
        .unwrap();

    conn.transaction(async |conn, tx| {
        let mut portal = tx.exec_portal_named(conn, &stmt, ()).await?;
        assert!(!portal.is_complete());

        let rows: Vec<(i32,)> = portal.exec_collect(conn, 0).await?;
        assert!(portal.is_complete());
        assert_eq!(rows.len(), 5);

        let total: i32 = rows.iter().map(|(n,)| n).sum();
        assert_eq!(total, 15);

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_batched() {
    let mut conn = get_conn().await;

    let stmt = conn
        .prepare("SELECT generate_series(1, 10) as n")
        .await
        .unwrap();

    conn.transaction(async |conn, tx| {
        let mut portal = tx.exec_portal_named(conn, &stmt, ()).await?;
        let mut all_rows: Vec<i32> = Vec::new();
        let mut batches = 0;

        while !portal.is_complete() {
            let rows: Vec<(i32,)> = portal.exec_collect(conn, 3).await?;
            all_rows.extend(rows.iter().map(|(n,)| *n));
            batches += 1;
        }

        assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(batches, 4);

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_empty_result() {
    let mut conn = get_conn().await;

    let stmt = conn.prepare("SELECT 1 WHERE false").await.unwrap();

    conn.transaction(async |conn, tx| {
        let mut portal = tx.exec_portal_named(conn, &stmt, ()).await?;
        let rows: Vec<(i32,)> = portal.exec_collect(conn, 0).await?;

        assert!(portal.is_complete());
        assert_eq!(rows.len(), 0);

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_with_params() {
    let mut conn = get_conn().await;

    let stmt = conn
        .prepare("SELECT generate_series(1, $1) as n")
        .await
        .unwrap();

    conn.transaction(async |conn, tx| {
        let mut portal = tx.exec_portal_named(conn, &stmt, (5i32,)).await?;
        let rows: Vec<(i32,)> = portal.exec_collect(conn, 0).await?;

        assert_eq!(rows.len(), 5);
        let total: i32 = rows.iter().map(|(n,)| n).sum();
        assert_eq!(total, 15);

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_with_raw_sql() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal = tx
            .exec_portal_named(conn, "SELECT generate_series(1, 5) as n", ())
            .await?;
        let rows: Vec<(i32,)> = portal.exec_collect(conn, 0).await?;

        assert_eq!(rows.len(), 5);
        let total: i32 = rows.iter().map(|(n,)| n).sum();
        assert_eq!(total, 15);

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_with_raw_sql_and_params() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal = tx
            .exec_portal_named(conn, "SELECT generate_series(1, $1) as n", (5i32,))
            .await?;
        let rows: Vec<(i32,)> = portal.exec_collect(conn, 0).await?;

        assert_eq!(rows.len(), 5);
        let total: i32 = rows.iter().map(|(n,)| n).sum();
        assert_eq!(total, 15);

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_portal_name() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal1 = tx.exec_portal_named(conn, "SELECT 1", ()).await?;
        let mut portal2 = tx.exec_portal_named(conn, "SELECT 2", ()).await?;

        assert_ne!(portal1.name(), portal2.name());
        assert!(portal1.name().starts_with("_zero_p_"));
        assert!(portal2.name().starts_with("_zero_p_"));

        let _: Vec<(i32,)> = portal1.exec_collect(conn, 0).await?;
        let _: Vec<(i32,)> = portal2.exec_collect(conn, 0).await?;

        portal1.close(conn).await?;
        portal2.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_multiple_portals() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal1 = tx
            .exec_portal_named(conn, "SELECT generate_series(1, 3) as n", ())
            .await?;
        let mut portal2 = tx
            .exec_portal_named(conn, "SELECT generate_series(10, 12) as n", ())
            .await?;

        let rows1: Vec<(i32,)> = portal1.exec_collect(conn, 2).await?;
        let rows2: Vec<(i32,)> = portal2.exec_collect(conn, 2).await?;

        assert_eq!(rows1.iter().map(|(n,)| *n).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(
            rows2.iter().map(|(n,)| *n).collect::<Vec<_>>(),
            vec![10, 11]
        );

        let rows1: Vec<(i32,)> = portal1.exec_collect(conn, 0).await?;
        let rows2: Vec<(i32,)> = portal2.exec_collect(conn, 0).await?;

        assert_eq!(rows1.iter().map(|(n,)| *n).collect::<Vec<_>>(), vec![3]);
        assert_eq!(rows2.iter().map(|(n,)| *n).collect::<Vec<_>>(), vec![12]);

        portal1.close(conn).await?;
        portal2.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_is_complete_tracking() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal = tx
            .exec_portal_named(conn, "SELECT generate_series(1, 5) as n", ())
            .await?;

        assert!(!portal.is_complete());

        let _: Vec<(i32,)> = portal.exec_collect(conn, 3).await?;
        assert!(!portal.is_complete());

        let _: Vec<(i32,)> = portal.exec_collect(conn, 0).await?;
        assert!(portal.is_complete());

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_foreach_basic() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal = tx
            .exec_portal_named(conn, "SELECT generate_series(1, 5) as n", ())
            .await?;
        let mut total = 0i32;

        portal
            .exec_foreach(conn, 0, |row: (i32,)| {
                total += row.0;
                Ok(())
            })
            .await?;

        assert!(portal.is_complete());
        assert_eq!(total, 15); // 1+2+3+4+5

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}

#[compio::test]
async fn test_exec_portal_foreach_batched() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, tx| {
        let mut portal = tx
            .exec_portal_named(conn, "SELECT generate_series(1, 10) as n", ())
            .await?;
        let mut all_rows: Vec<i32> = Vec::new();
        let mut batches = 0;

        while !portal.is_complete() {
            portal
                .exec_foreach(conn, 3, |row: (i32,)| {
                    all_rows.push(row.0);
                    Ok(())
                })
                .await?;
            batches += 1;
        }

        assert_eq!(all_rows, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(batches, 4); // 3+3+3+1 rows in 4 batches

        portal.close(conn).await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();
}
