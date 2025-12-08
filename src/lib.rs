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
//! use zero_postgres::sync::Connection;
//! use zero_postgres::state::connection::ConnectionOptions;
//!
//! fn main() -> zero_postgres::error::Result<()> {
//!     let options = ConnectionOptions::new("postgres")
//!         .database("mydb")
//!         .password("secret");
//!
//!     let mut conn = Connection::connect("localhost", 5432, options)?;
//!
//!     let (columns, rows) = conn.query_collect("SELECT 1 AS num")?;
//!     println!("Columns: {:?}", columns);
//!     println!("Rows: {:?}", rows);
//!
//!     conn.close()?;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod protocol;
pub mod state;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "tokio")]
pub mod tokio;

pub use error::{Error, ErrorFields, Result};
pub use protocol::types::{FormatCode, Oid, TransactionStatus};
pub use state::connection::{Opts, SslMode};
pub use state::simple_query::{ControlFlow, QueryHandler};
