use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use std_semaphore::Semaphore;

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;

pub struct Pool {
    opts: Opts,
    conns: ArrayQueue<Conn>,
    semaphore: Option<Semaphore>,
}

impl Pool {
    pub fn new(opts: Opts) -> Self {
        let semaphore = opts
            .pool_max_concurrency
            .map(|n| Semaphore::new(n as isize));
        Self {
            conns: ArrayQueue::new(opts.pool_max_idle_conn),
            opts,
            semaphore,
        }
    }

    pub fn get(self: &Arc<Self>) -> Result<PooledConn> {
        if let Some(sem) = &self.semaphore {
            sem.acquire();
        }
        let conn = loop {
            match self.conns.pop() {
                Some(mut c) => {
                    if c.ping().is_ok() {
                        break c;
                    }
                    // Connection dead, try next one
                }
                None => break Conn::new(self.opts.clone())?,
            }
        };
        Ok(PooledConn {
            conn: ManuallyDrop::new(conn),
            pool: Arc::clone(self),
        })
    }

    fn check_in(&self, mut conn: Conn) {
        if conn.is_broken() {
            return;
        }
        if conn.query_drop("DISCARD ALL").is_err() {
            return;
        }
        let _ = self.conns.push(conn);
    }
}

pub struct PooledConn {
    pool: Arc<Pool>,
    conn: ManuallyDrop<Conn>,
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
        self.pool.check_in(conn);
        if let Some(sem) = &self.pool.semaphore {
            sem.release();
        }
    }
}
