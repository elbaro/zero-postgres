//! Integration tests for the async Pipeline API (compio).

#![cfg(feature = "compio")]

use std::env;
use zero_postgres::compio::Conn;

async fn get_conn() -> Conn {
    let mut db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string());
    if !db_url.contains("sslmode=") {
        if db_url.contains('?') {
            db_url.push_str("&sslmode=disable");
        } else {
            db_url.push_str("?sslmode=disable");
        }
    }
    Conn::new(db_url.as_str()).await.expect("Failed to connect")
}

async fn verify_connection(conn: &mut Conn) {
    let result: (i32,) = conn.query_first("SELECT 7919").await.unwrap().unwrap();
    assert_eq!(result.0, 7919);
}

#[compio::test]
async fn test_pipeline_exec() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t = p.exec("SELECT $1::int as num, $2::text as txt", (42, "hello"))?;
            p.sync().await?;
            let rows: Vec<(i32, String)> = p.claim_collect(t).await?;
            Ok(rows)
        })
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (42, "hello".to_string()));
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_multiple_execs() {
    let mut conn = get_conn().await;

    let (r1, r2, r3) = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;
            let t3 = p.exec("SELECT $1::int", (3,))?;

            p.sync().await?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;
            let r3: Vec<(i32,)> = p.claim_collect(t3).await?;

            Ok((r1, r2, r3))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_no_rows() {
    let mut conn = get_conn().await;

    let result: Vec<(i32,)> = conn
        .pipeline(async |p| {
            let t = p.exec("SELECT 1 WHERE false", ())?;
            p.sync().await?;
            p.claim_collect(t).await
        })
        .await
        .unwrap();

    assert!(result.is_empty());
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_multiple_rows() {
    let mut conn = get_conn().await;

    let result: Vec<(i32,)> = conn
        .pipeline(async |p| {
            let t = p.exec("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)", ())?;
            p.sync().await?;
            p.claim_collect(t).await
        })
        .await
        .unwrap();

    assert_eq!(result, vec![(1,), (2,), (3,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_with_prepared() {
    let mut conn = get_conn().await;

    let stmt = conn.prepare("SELECT $1::int * 2").await.unwrap();

    let (r1, r2) = conn
        .pipeline(async |p| {
            let t1 = p.exec(&stmt, (5,))?;
            let t2 = p.exec(&stmt, (10,))?;

            p.sync().await?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;

            Ok((r1, r2))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(10,)]);
    assert_eq!(r2, vec![(20,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_claim_order_error() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT 1", ())?;
            let t2 = p.exec("SELECT 2", ())?;

            p.sync().await?;

            // Try to claim t2 before t1 - should fail
            let result: Result<Vec<(i32,)>, _> = p.claim_collect(t2).await;
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("out of order"),
                "Expected 'out of order' error, got: {}",
                err
            );

            // Now claim in correct order
            let _: Vec<(i32,)> = p.claim_collect(t1).await?;

            Ok(())
        })
        .await;

    assert!(result.is_ok());
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_sql_error() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t = p.exec("SELECT 1/0", ())?;

            p.sync().await?;

            let result: Result<Vec<(i32,)>, _> = p.claim_collect(t).await;
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("division by zero"),
                "Expected 'division by zero' error, got: {}",
                err
            );

            Ok(())
        })
        .await;

    assert!(result.is_ok());
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_aborted_state() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT 1", ())?;
            let t2 = p.exec("SELECT 1/0", ())?;
            let t3 = p.exec("SELECT 2", ())?;

            p.sync().await?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            assert_eq!(r1, vec![(1,)]);

            let result: Result<Vec<(i32,)>, _> = p.claim_collect(t2).await;
            assert!(result.is_err());

            let result3: Result<Vec<(i32,)>, _> = p.claim_collect(t3).await;
            assert!(result3.is_err());
            let err = result3.unwrap_err();
            assert!(
                err.to_string().contains("aborted"),
                "Expected 'aborted' error, got: {}",
                err
            );

            Ok(())
        })
        .await;

    assert!(result.is_ok());
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_insert() {
    let mut conn = get_conn().await;

    conn.query_drop("DROP TABLE IF EXISTS _pipeline_insert_test_compio")
        .await
        .unwrap();
    conn.query_drop("CREATE TEMP TABLE _pipeline_insert_test_compio (id int, name text)")
        .await
        .unwrap();

    conn.pipeline(async |p| {
        let t1 = p.exec(
            "INSERT INTO _pipeline_insert_test_compio VALUES ($1, $2)",
            (1, "alice"),
        )?;
        let t2 = p.exec(
            "INSERT INTO _pipeline_insert_test_compio VALUES ($1, $2)",
            (2, "bob"),
        )?;

        p.sync().await?;

        p.claim_drop(t1).await?;
        p.claim_drop(t2).await?;

        Ok(())
    })
    .await
    .unwrap();

    let rows: Vec<(i32, String)> = conn
        .query_collect("SELECT id, name FROM _pipeline_insert_test_compio ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "alice".to_string()));
    assert_eq!(rows[1], (2, "bob".to_string()));
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_insert_returning() {
    let mut conn = get_conn().await;

    conn.query_drop("DROP TABLE IF EXISTS _pipeline_returning_test_compio")
        .await
        .unwrap();
    conn.query_drop(
        "CREATE TEMP TABLE _pipeline_returning_test_compio (id serial PRIMARY KEY, name text)",
    )
    .await
    .unwrap();

    let (r1, r2) = conn
        .pipeline(async |p| {
            let t1 = p.exec(
                "INSERT INTO _pipeline_returning_test_compio (name) VALUES ($1) RETURNING id",
                ("alice",),
            )?;
            let t2 = p.exec(
                "INSERT INTO _pipeline_returning_test_compio (name) VALUES ($1) RETURNING id",
                ("bob",),
            )?;

            p.sync().await?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;

            Ok((r1, r2))
        })
        .await
        .unwrap();

    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert!(r1[0].0 < r2[0].0);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_empty() {
    let mut conn = get_conn().await;

    conn.pipeline(async |p| {
        p.sync().await?;
        Ok(())
    })
    .await
    .unwrap();
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_pending_count() {
    let mut conn = get_conn().await;

    conn.pipeline(async |p| {
        assert_eq!(p.pending_count(), 0);

        let t1 = p.exec("SELECT 1", ())?;
        assert_eq!(p.pending_count(), 1);

        let t2 = p.exec("SELECT 2", ())?;
        assert_eq!(p.pending_count(), 2);

        p.sync().await?;

        let _: Vec<(i32,)> = p.claim_collect(t1).await?;
        assert_eq!(p.pending_count(), 1);

        let _: Vec<(i32,)> = p.claim_collect(t2).await?;
        assert_eq!(p.pending_count(), 0);

        Ok(())
    })
    .await
    .unwrap();
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_claim_one() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t = p.exec("SELECT 42::int", ())?;
            p.sync().await?;
            p.claim_one::<(i32,)>(t).await
        })
        .await
        .unwrap();

    assert_eq!(result, Some((42,)));
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_claim_one_empty() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t = p.exec("SELECT 1 WHERE false", ())?;
            p.sync().await?;
            p.claim_one::<(i32,)>(t).await
        })
        .await
        .unwrap();

    assert_eq!(result, None);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_auto_sync_basic() {
    let mut conn = get_conn().await;

    let (r1, r2) = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;
            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;
            Ok((r1, r2))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_interleaved_exec_claim() {
    let mut conn = get_conn().await;

    let (r1, r2, r3, r4) = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;

            let t3 = p.exec("SELECT $1::int", (3,))?;
            let t4 = p.exec("SELECT $1::int", (4,))?;

            let r3: Vec<(i32,)> = p.claim_collect(t3).await?;
            let r4: Vec<(i32,)> = p.claim_collect(t4).await?;

            Ok((r1, r2, r3, r4))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
    assert_eq!(r4, vec![(4,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_partial_claim_then_exec() {
    let mut conn = get_conn().await;

    let (r1, r2, r3, r4, r5) = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;
            let t3 = p.exec("SELECT $1::int", (3,))?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;

            let t4 = p.exec("SELECT $1::int", (4,))?;
            let t5 = p.exec("SELECT $1::int", (5,))?;

            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;
            let r3: Vec<(i32,)> = p.claim_collect(t3).await?;
            let r4: Vec<(i32,)> = p.claim_collect(t4).await?;
            let r5: Vec<(i32,)> = p.claim_collect(t5).await?;

            Ok((r1, r2, r3, r4, r5))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    assert_eq!(r3, vec![(3,)]);
    assert_eq!(r4, vec![(4,)]);
    assert_eq!(r5, vec![(5,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_multiple_batches_with_error() {
    let mut conn = get_conn().await;

    let result = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;

            assert_eq!(r1, vec![(1,)]);
            assert_eq!(r2, vec![(2,)]);

            let t3 = p.exec("SELECT $1::int", (3,))?;
            let t4 = p.exec("SELECT 1/0", ())?;
            let t5 = p.exec("SELECT $1::int", (5,))?;

            let r3: Vec<(i32,)> = p.claim_collect(t3).await?;
            assert_eq!(r3, vec![(3,)]);

            let result4: Result<Vec<(i32,)>, _> = p.claim_collect(t4).await;
            assert!(result4.is_err());

            let result5: Result<Vec<(i32,)>, _> = p.claim_collect(t5).await;
            assert!(result5.is_err());
            assert!(result5.unwrap_err().to_string().contains("aborted"));

            Ok(())
        })
        .await;

    assert!(result.is_ok());
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_error_recovery_new_batch() {
    let mut conn = get_conn().await;

    let _ = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT 1/0", ())?;
            let _: Result<Vec<(i32,)>, _> = p.claim_collect(t1).await;
            Ok(())
        })
        .await;

    let result = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (100,))?;
            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            Ok(r1)
        })
        .await
        .unwrap();

    assert_eq!(result, vec![(100,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_explicit_flush() {
    let mut conn = get_conn().await;

    let (r1, r2) = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT $1::int", (2,))?;

            p.flush().await?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            let r2: Vec<(i32,)> = p.claim_collect(t2).await?;

            Ok((r1, r2))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r2, vec![(2,)]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_complex_interleave() {
    let mut conn = get_conn().await;

    let results = conn
        .pipeline(async |p| {
            let mut results = Vec::new();

            for i in 1..=5 {
                let t = p.exec("SELECT $1::int", (i,))?;
                let r: Vec<(i32,)> = p.claim_collect(t).await?;
                results.push(r[0].0);
            }

            Ok(results)
        })
        .await
        .unwrap();

    assert_eq!(results, vec![1, 2, 3, 4, 5]);
    verify_connection(&mut conn).await;
}

#[compio::test]
async fn test_pipeline_continue_after_error_batch() {
    let mut conn = get_conn().await;

    let (r1, r4, r5) = conn
        .pipeline(async |p| {
            let t1 = p.exec("SELECT $1::int", (1,))?;
            let t2 = p.exec("SELECT 1/0", ())?;
            let t3 = p.exec("SELECT $1::int", (3,))?;

            let r1: Vec<(i32,)> = p.claim_collect(t1).await?;
            assert_eq!(r1, vec![(1,)]);

            let result2: Result<Vec<(i32,)>, _> = p.claim_collect(t2).await;
            assert!(result2.is_err());

            let result3: Result<Vec<(i32,)>, _> = p.claim_collect(t3).await;
            assert!(result3.is_err());

            let t4 = p.exec("SELECT $1::int", (4,))?;
            let t5 = p.exec("SELECT $1::int", (5,))?;

            let r4: Vec<(i32,)> = p.claim_collect(t4).await?;
            let r5: Vec<(i32,)> = p.claim_collect(t5).await?;

            Ok((r1, r4, r5))
        })
        .await
        .unwrap();

    assert_eq!(r1, vec![(1,)]);
    assert_eq!(r4, vec![(4,)]);
    assert_eq!(r5, vec![(5,)]);
    verify_connection(&mut conn).await;
}
