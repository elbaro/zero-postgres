//! Asynchronous connection pool for compio (single-threaded, Rc-based).

use std::cell::RefCell;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;

pub struct Pool {
    opts: Opts,
    conns: RefCell<Vec<Conn>>,
    max_idle: usize,
}

impl Pool {
    pub fn new(opts: Opts) -> Rc<Self> {
        let max_idle = opts.pool_max_idle_conn;
        Rc::new(Self {
            opts,
            conns: RefCell::new(Vec::new()),
            max_idle,
        })
    }

    pub async fn get(self: &Rc<Self>) -> Result<PooledConn> {
        let conn = loop {
            let candidate = self.conns.borrow_mut().pop();
            match candidate {
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
            pool: Rc::clone(self),
        })
    }

    async fn check_in(&self, mut conn: Conn) {
        if conn.is_broken() {
            return;
        }
        if conn.query_drop("DISCARD ALL").await.is_err() {
            return;
        }
        let mut conns = self.conns.borrow_mut();
        if conns.len() < self.max_idle {
            conns.push(conn);
        }
    }
}

pub struct PooledConn {
    pool: Rc<Pool>,
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
        let pool = Rc::clone(&self.pool);
        compio::runtime::spawn(async move {
            pool.check_in(conn).await;
        })
        .detach();
    }
}
