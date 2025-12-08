//! PostgreSQL backend (server → client) messages.

pub mod auth;
pub mod copy;
pub mod error;
pub mod extended;
pub mod query;

pub use auth::{AuthenticationMessage, BackendKeyData, ParameterStatus, ReadyForQuery};
pub use copy::{CopyData, CopyDone, CopyInResponse, CopyOutResponse};
pub use error::{ErrorResponse, NoticeResponse};
pub use extended::{
    BindComplete, CloseComplete, NoData, ParameterDescription, ParseComplete, PortalSuspended,
};
pub use query::{CommandComplete, DataRow, EmptyQueryResponse, RowDescription};

/// Backend message type bytes.
pub mod msg_type {
    /// Authentication message
    pub const AUTHENTICATION: u8 = b'R';
    /// BackendKeyData
    pub const BACKEND_KEY_DATA: u8 = b'K';
    /// ParameterStatus
    pub const PARAMETER_STATUS: u8 = b'S';
    /// ReadyForQuery
    pub const READY_FOR_QUERY: u8 = b'Z';
    /// RowDescription
    pub const ROW_DESCRIPTION: u8 = b'T';
    /// DataRow
    pub const DATA_ROW: u8 = b'D';
    /// CommandComplete
    pub const COMMAND_COMPLETE: u8 = b'C';
    /// EmptyQueryResponse
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    /// ErrorResponse
    pub const ERROR_RESPONSE: u8 = b'E';
    /// NoticeResponse
    pub const NOTICE_RESPONSE: u8 = b'N';
    /// NotificationResponse
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    /// ParseComplete
    pub const PARSE_COMPLETE: u8 = b'1';
    /// BindComplete
    pub const BIND_COMPLETE: u8 = b'2';
    /// CloseComplete
    pub const CLOSE_COMPLETE: u8 = b'3';
    /// ParameterDescription
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    /// NoData
    pub const NO_DATA: u8 = b'n';
    /// PortalSuspended
    pub const PORTAL_SUSPENDED: u8 = b's';
    /// CopyInResponse
    pub const COPY_IN_RESPONSE: u8 = b'G';
    /// CopyOutResponse
    pub const COPY_OUT_RESPONSE: u8 = b'H';
    /// CopyBothResponse
    pub const COPY_BOTH_RESPONSE: u8 = b'W';
    /// CopyData
    pub const COPY_DATA: u8 = b'd';
    /// CopyDone
    pub const COPY_DONE: u8 = b'c';
    /// FunctionCallResponse
    pub const FUNCTION_CALL_RESPONSE: u8 = b'V';
    /// NegotiateProtocolVersion
    pub const NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';
}

/// Raw message from the PostgreSQL server.
///
/// This is a thin wrapper around the message type byte and payload.
/// Individual message types are parsed on demand by state machines.
#[derive(Debug, Clone, Copy)]
pub struct RawMessage<'a> {
    /// Message type byte
    pub type_byte: u8,
    /// Message payload (after length field)
    pub payload: &'a [u8],
}

impl<'a> RawMessage<'a> {
    /// Create a new RawMessage.
    pub fn new(type_byte: u8, payload: &'a [u8]) -> Self {
        Self { type_byte, payload }
    }

    /// Check if this is an error response.
    pub fn is_error(&self) -> bool {
        self.type_byte == msg_type::ERROR_RESPONSE
    }

    /// Check if this is a notice response.
    pub fn is_notice(&self) -> bool {
        self.type_byte == msg_type::NOTICE_RESPONSE
    }

    /// Check if this is a notification response.
    pub fn is_notification(&self) -> bool {
        self.type_byte == msg_type::NOTIFICATION_RESPONSE
    }

    /// Check if this is a parameter status message.
    pub fn is_parameter_status(&self) -> bool {
        self.type_byte == msg_type::PARAMETER_STATUS
    }

    /// Check if this is an async message (can arrive at any time).
    pub fn is_async(&self) -> bool {
        Self::is_async_type(self.type_byte)
    }

    /// Check if a type byte represents an async message (can arrive at any time).
    pub fn is_async_type(type_byte: u8) -> bool {
        matches!(
            type_byte,
            msg_type::NOTICE_RESPONSE
                | msg_type::NOTIFICATION_RESPONSE
                | msg_type::PARAMETER_STATUS
        )
    }
}
