//! Error types for zero-postgres.

use std::collections::HashMap;
use thiserror::Error;

/// Result type for zero-postgres operations.
pub type Result<T> = core::result::Result<T, Error>;

/// PostgreSQL error field type codes.
pub mod field_type {
    pub const SEVERITY: u8 = b'S';
    pub const SEVERITY_V: u8 = b'V';
    pub const CODE: u8 = b'C';
    pub const MESSAGE: u8 = b'M';
    pub const DETAIL: u8 = b'D';
    pub const HINT: u8 = b'H';
    pub const POSITION: u8 = b'P';
    pub const INTERNAL_POSITION: u8 = b'p';
    pub const INTERNAL_QUERY: u8 = b'q';
    pub const WHERE: u8 = b'W';
    pub const SCHEMA: u8 = b's';
    pub const TABLE: u8 = b't';
    pub const COLUMN: u8 = b'c';
    pub const DATA_TYPE: u8 = b'd';
    pub const CONSTRAINT: u8 = b'n';
    pub const FILE: u8 = b'F';
    pub const LINE: u8 = b'L';
    pub const ROUTINE: u8 = b'R';
}

/// PostgreSQL server error/notice message.
#[derive(Debug, Clone)]
pub struct ServerError(pub(crate) HashMap<u8, String>);

impl ServerError {
    /// Create from a HashMap of field codes to values.
    pub fn new(fields: HashMap<u8, String>) -> Self {
        Self(fields)
    }

    // Always present (PostgreSQL 9.6+)

    /// Severity (localized): ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG
    pub fn severity(&self) -> &str {
        self.0
            .get(&field_type::SEVERITY)
            .map(|s| s.as_str())
            .unwrap_or_default()
    }

    /// Severity (non-localized, never translated)
    pub fn severity_v(&self) -> &str {
        self.0
            .get(&field_type::SEVERITY_V)
            .map(|s| s.as_str())
            .unwrap_or_default()
    }

    /// SQLSTATE error code (5 characters)
    pub fn code(&self) -> &str {
        self.0
            .get(&field_type::CODE)
            .map(|s| s.as_str())
            .unwrap_or_default()
    }

    /// Primary error message
    pub fn message(&self) -> &str {
        self.0
            .get(&field_type::MESSAGE)
            .map(|s| s.as_str())
            .unwrap_or_default()
    }

    // Optional fields

    /// Detailed error explanation
    pub fn detail(&self) -> Option<&str> {
        self.0.get(&field_type::DETAIL).map(|s| s.as_str())
    }

    /// Suggestion for fixing the error
    pub fn hint(&self) -> Option<&str> {
        self.0.get(&field_type::HINT).map(|s| s.as_str())
    }

    /// Cursor position in query string (1-based)
    pub fn position(&self) -> Option<u32> {
        self.0
            .get(&field_type::POSITION)
            .and_then(|s| s.parse().ok())
    }

    /// Position in internal query
    pub fn internal_position(&self) -> Option<u32> {
        self.0
            .get(&field_type::INTERNAL_POSITION)
            .and_then(|s| s.parse().ok())
    }

    /// Failed internal command text
    pub fn internal_query(&self) -> Option<&str> {
        self.0.get(&field_type::INTERNAL_QUERY).map(|s| s.as_str())
    }

    /// Context/stack trace
    pub fn where_(&self) -> Option<&str> {
        self.0.get(&field_type::WHERE).map(|s| s.as_str())
    }

    /// Schema name
    pub fn schema(&self) -> Option<&str> {
        self.0.get(&field_type::SCHEMA).map(|s| s.as_str())
    }

    /// Table name
    pub fn table(&self) -> Option<&str> {
        self.0.get(&field_type::TABLE).map(|s| s.as_str())
    }

    /// Column name
    pub fn column(&self) -> Option<&str> {
        self.0.get(&field_type::COLUMN).map(|s| s.as_str())
    }

    /// Data type name
    pub fn data_type(&self) -> Option<&str> {
        self.0.get(&field_type::DATA_TYPE).map(|s| s.as_str())
    }

    /// Constraint name
    pub fn constraint(&self) -> Option<&str> {
        self.0.get(&field_type::CONSTRAINT).map(|s| s.as_str())
    }

    /// Source file name
    pub fn file(&self) -> Option<&str> {
        self.0.get(&field_type::FILE).map(|s| s.as_str())
    }

    /// Source line number
    pub fn line(&self) -> Option<u32> {
        self.0.get(&field_type::LINE).and_then(|s| s.parse().ok())
    }

    /// Source routine name
    pub fn routine(&self) -> Option<&str> {
        self.0.get(&field_type::ROUTINE).map(|s| s.as_str())
    }

    /// Get a field by its type code.
    pub fn get(&self, field_type: u8) -> Option<&str> {
        self.0.get(&field_type).map(|s| s.as_str())
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (SQLSTATE {})",
            self.severity(),
            self.message(),
            self.code()
        )?;
        if let Some(detail) = self.detail() {
            write!(f, "\nDETAIL: {}", detail)?;
        }
        if let Some(hint) = self.hint() {
            write!(f, "\nHINT: {}", hint)?;
        }
        Ok(())
    }
}

/// Error type for zero-postgres.
#[derive(Debug, Error)]
pub enum Error {
    /// Server error response
    #[error("PostgreSQL error: {0}")]
    Server(ServerError),

    /// Protocol error (malformed message, unexpected response, etc.)
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Authentication failed
    #[error("Authentication failed: {0}")]
    Auth(String),

    /// TLS error
    #[cfg(any(feature = "sync-tls", feature = "tokio-tls"))]
    #[error("TLS error: {0}")]
    Tls(#[from] native_tls::Error),

    /// Connection is broken and cannot be reused
    #[error("Connection is broken")]
    ConnectionBroken,

    /// Invalid usage (e.g., nested transactions)
    #[error("Invalid usage: {0}")]
    InvalidUsage(String),

    /// Unsupported feature
    #[error("Unsupported: {0}")]
    Unsupported(String),

    /// Value decode error
    #[error("Decode error: {0}")]
    Decode(String),
}

impl Error {
    /// Returns true if the error indicates the connection is broken and cannot be reused.
    pub fn is_connection_broken(&self) -> bool {
        match self {
            Error::Io(_) | Error::ConnectionBroken => true,
            Error::Server(err) => {
                // FATAL and PANIC errors indicate connection is broken
                matches!(err.severity_v(), "FATAL" | "PANIC")
            }
            _ => false,
        }
    }

    /// Get the SQLSTATE code if this is a server error.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Error::Server(err) => Some(err.code()),
            _ => None,
        }
    }
}

impl<Src: std::fmt::Debug, Dst: std::fmt::Debug + ?Sized> From<zerocopy::error::CastError<Src, Dst>>
    for Error
{
    fn from(err: zerocopy::error::CastError<Src, Dst>) -> Self {
        Error::Protocol(format!("zerocopy cast error: {err:?}"))
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(err: std::convert::Infallible) -> Self {
        match err {}
    }
}
