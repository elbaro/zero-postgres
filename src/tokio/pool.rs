//! Asynchronous connection pool.

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use tokio::sync::Semaphore;

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;

pub struct Pool {
    opts: Opts,
    conns: ArrayQueue<Conn>,
    semaphore: Option<Arc<Semaphore>>,
}

impl Pool {
    pub fn new(opts: Opts) -> Self {
        let semaphore = opts
            .pool_max_concurrency
            .map(|n| Arc::new(Semaphore::new(n)));
        Self {
            conns: ArrayQueue::new(opts.pool_max_idle_conn),
            opts,
            semaphore,
        }
    }

    pub async fn get(self: &Arc<Self>) -> Result<PooledConn> {
        let permit = if let Some(sem) = &self.semaphore {
            Some(sem.clone().acquire_owned().await.unwrap())
        } else {
            None
        };
        let conn = loop {
            match self.conns.pop() {
                Some(mut c) => {
                    if c.ping().await.is_ok() {
                        break c;
                    }
                    // Connection dead, try next one
                }
                None => break Conn::new(self.opts.clone()).await?,
            }
        };
        Ok(PooledConn {
            conn: ManuallyDrop::new(conn),
            pool: Arc::clone(self),
            _permit: permit,
        })
    }

    async fn check_in(&self, mut conn: Conn) {
        if conn.is_broken() {
            return;
        }
        if conn.query_drop("DISCARD ALL").await.is_err() {
            return;
        }
        let _ = self.conns.push(conn);
    }
}

pub struct PooledConn {
    pool: Arc<Pool>,
    conn: ManuallyDrop<Conn>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Deref for PooledConn {
    type Target = Conn;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for PooledConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        // SAFETY: conn is never accessed after this
        let conn = unsafe { ManuallyDrop::take(&mut self.conn) };
        let pool = Arc::clone(&self.pool);
        tokio::spawn(async move {
            pool.check_in(conn).await;
        });
    }
}
