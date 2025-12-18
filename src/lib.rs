//! A high-performance PostgreSQL client library.
//!
//! # Features
//!
//! - **Zero-copy parsing**: Message payloads are parsed directly from the read buffer
//! - **Sans-I/O state machines**: Protocol logic is separated from I/O
//! - **Sync and async APIs**: Choose between synchronous and tokio-based async
//! - **Full protocol support**: Simple query, extended query, COPY, pipelining
//!
//! # Example
//!
//! ```no_run
//! use zero_postgres::sync::Conn;
//! use zero_postgres::Opts;
//!
//! fn main() -> zero_postgres::Result<()> {
//!     let opts = Opts {
//!         host: "localhost".into(),
//!         user: "postgres".into(),
//!         database: Some("mydb".into()),
//!         password: Some("secret".into()),
//!         ..Default::default()
//!     };
//!
//!     let mut conn = Conn::new(opts)?;
//!
//!     let rows: Vec<(i32,)> = conn.query_collect("SELECT 1 AS num")?;
//!     println!("Rows: {:?}", rows);
//!
//!     conn.close()?;
//!     Ok(())
//! }
//! ```

// private
mod buffer_pool;
mod buffer_set;
mod error;
mod opts;
mod statement;

// pub
pub mod conversion;
pub mod handler;
pub mod protocol;
pub mod state;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "tokio")]
pub mod tokio;

pub use buffer_pool::BufferPool;
pub use buffer_set::BufferSet;
pub use error::{Error, Result, ServerError};
pub use handler::AsyncMessageHandler;
pub use opts::{Opts, SslMode};
pub use state::action::AsyncMessage;
pub use state::extended::PreparedStatement;
pub use statement::IntoStatement;
