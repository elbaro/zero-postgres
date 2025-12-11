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
//! fn main() -> zero_postgres::error::Result<()> {
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

pub mod error;
pub mod handler;
pub mod opts;
pub mod protocol;
pub mod row;
pub mod state;
pub mod types;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "tokio")]
pub mod tokio;

pub use error::{Error, Result, ServerError};
pub use handler::{BinaryHandler, CollectHandler, DropHandler, FirstRowHandler, TextHandler};
pub use opts::{Opts, SslMode};
pub use protocol::types::{FormatCode, Oid, TransactionStatus};
pub use row::FromRow;
pub use state::extended::{ColumnInfo, PreparedStatement};
pub use state::simple_query::BufferSet;
pub use types::{FromWireValue, ToParams, ToWireValue};
