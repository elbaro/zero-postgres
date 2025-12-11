//! Synchronous PostgreSQL client.

pub mod conn;
mod pool;
mod stream;

pub use conn::Conn;
pub use pool::{Pool, PooledConn};
