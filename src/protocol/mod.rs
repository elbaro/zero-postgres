//! PostgreSQL wire protocol implementation.
//!
//! This module contains the low-level protocol encoding and decoding.
//!
//! # Structure
//!
//! - `backend`: Server → Client messages (parsing)
//! - `frontend`: Client → Server messages (encoding)
//! - `codec`: Low-level encoding/decoding primitives
//! - `types`: Common protocol types (FormatCode, Oid, TransactionStatus)

pub mod backend;
pub mod codec;
pub mod frontend;
pub mod types;

// Re-export commonly used types
pub use backend::RawMessage;
pub use types::{FormatCode, Oid, TransactionStatus};
