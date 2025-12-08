//! PostgreSQL frontend (client → server) messages.

pub mod auth;
pub mod copy;
pub mod extended;
pub mod simple;
pub mod startup;

pub use auth::{write_password, write_sasl_initial_response, write_sasl_response};
pub use copy::{write_copy_data, write_copy_done, write_copy_fail};
pub use extended::{
    write_bind, write_close_portal, write_close_statement, write_describe_portal,
    write_describe_statement, write_execute, write_flush, write_parse, write_sync,
};
pub use simple::write_query;
pub use startup::{write_cancel_request, write_ssl_request, write_startup, write_terminate};

/// Frontend message type bytes.
pub mod msg_type {
    /// Password/SASL response (all auth response types use 'p')
    pub const PASSWORD: u8 = b'p';
    /// Query (simple query protocol)
    pub const QUERY: u8 = b'Q';
    /// Parse (extended query protocol)
    pub const PARSE: u8 = b'P';
    /// Bind (extended query protocol)
    pub const BIND: u8 = b'B';
    /// Execute (extended query protocol)
    pub const EXECUTE: u8 = b'E';
    /// Describe (extended query protocol)
    pub const DESCRIBE: u8 = b'D';
    /// Close (extended query protocol)
    pub const CLOSE: u8 = b'C';
    /// Sync (extended query protocol)
    pub const SYNC: u8 = b'S';
    /// Flush (extended query protocol)
    pub const FLUSH: u8 = b'H';
    /// Function call
    pub const FUNCTION_CALL: u8 = b'F';
    /// CopyData
    pub const COPY_DATA: u8 = b'd';
    /// CopyDone
    pub const COPY_DONE: u8 = b'c';
    /// CopyFail
    pub const COPY_FAIL: u8 = b'f';
    /// Terminate
    pub const TERMINATE: u8 = b'X';
}
