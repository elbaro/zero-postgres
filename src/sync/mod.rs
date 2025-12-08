//! Synchronous PostgreSQL client.

pub mod conn;
mod stream;

pub use conn::Conn;
