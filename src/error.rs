//! Error types for zero-postgres.

use thiserror::Error;

/// Result type for zero-postgres operations.
pub type Result<T> = core::result::Result<T, Error>;

/// PostgreSQL error/notice field types.
#[derive(Debug, Clone, Default)]
pub struct ErrorFields {
    /// Severity: ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG
    pub severity: Option<String>,
    /// Non-localized severity (same as severity but never translated)
    pub severity_non_localized: Option<String>,
    /// SQLSTATE error code (5 characters)
    pub code: Option<String>,
    /// Primary error message
    pub message: Option<String>,
    /// Detailed error explanation
    pub detail: Option<String>,
    /// Suggestion for fixing the error
    pub hint: Option<String>,
    /// Cursor position in query string (1-based)
    pub position: Option<u32>,
    /// Position in internal query
    pub internal_position: Option<u32>,
    /// Failed internal command text
    pub internal_query: Option<String>,
    /// Context/stack trace
    pub where_: Option<String>,
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: Option<String>,
    /// Column name
    pub column: Option<String>,
    /// Data type name
    pub data_type: Option<String>,
    /// Constraint name
    pub constraint: Option<String>,
    /// Source file name
    pub file: Option<String>,
    /// Source line number
    pub line: Option<u32>,
    /// Source routine name
    pub routine: Option<String>,
}

impl std::fmt::Display for ErrorFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(severity) = &self.severity {
            write!(f, "{}: ", severity)?;
        }
        if let Some(message) = &self.message {
            write!(f, "{}", message)?;
        }
        if let Some(code) = &self.code {
            write!(f, " (SQLSTATE {})", code)?;
        }
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {}", detail)?;
        }
        if let Some(hint) = &self.hint {
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
    Server(ErrorFields),

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
}

impl Error {
    /// Returns true if the error indicates the connection is broken and cannot be reused.
    pub fn is_connection_broken(&self) -> bool {
        match self {
            Error::Io(_) | Error::ConnectionBroken => true,
            Error::Server(fields) => {
                // FATAL and PANIC errors indicate connection is broken
                matches!(
                    fields.severity.as_deref(),
                    Some("FATAL") | Some("PANIC")
                )
            }
            _ => false,
        }
    }

    /// Get the SQLSTATE code if this is a server error.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Error::Server(fields) => fields.code.as_deref(),
            _ => None,
        }
    }
}

impl<Src: std::fmt::Debug, Dst: std::fmt::Debug + ?Sized>
    From<zerocopy::error::CastError<Src, Dst>> for Error
{
    fn from(err: zerocopy::error::CastError<Src, Dst>) -> Self {
        Error::Protocol(format!("zerocopy cast error: {err:?}"))
    }
}
