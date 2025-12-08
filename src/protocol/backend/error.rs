//! Error and notice response messages.

use crate::error::{Error, ErrorFields, Result};
use crate::protocol::codec::read_cstr;

/// Error field type codes from PostgreSQL protocol.
pub mod field_type {
    /// Severity (localized)
    pub const SEVERITY: u8 = b'S';
    /// Severity (non-localized, PostgreSQL 9.6+)
    pub const SEVERITY_NON_LOCALIZED: u8 = b'V';
    /// SQLSTATE code
    pub const CODE: u8 = b'C';
    /// Message
    pub const MESSAGE: u8 = b'M';
    /// Detail
    pub const DETAIL: u8 = b'D';
    /// Hint
    pub const HINT: u8 = b'H';
    /// Position in query
    pub const POSITION: u8 = b'P';
    /// Internal position
    pub const INTERNAL_POSITION: u8 = b'p';
    /// Internal query
    pub const INTERNAL_QUERY: u8 = b'q';
    /// Where (context)
    pub const WHERE: u8 = b'W';
    /// Schema name
    pub const SCHEMA: u8 = b's';
    /// Table name
    pub const TABLE: u8 = b't';
    /// Column name
    pub const COLUMN: u8 = b'c';
    /// Data type name
    pub const DATA_TYPE: u8 = b'd';
    /// Constraint name
    pub const CONSTRAINT: u8 = b'n';
    /// File name
    pub const FILE: u8 = b'F';
    /// Line number
    pub const LINE: u8 = b'L';
    /// Routine name
    pub const ROUTINE: u8 = b'R';
}

/// Parse error/notice fields from payload.
fn parse_fields(payload: &[u8]) -> Result<ErrorFields> {
    let mut fields = ErrorFields::default();
    let mut data = payload;

    while !data.is_empty() && data[0] != 0 {
        let field_type = data[0];
        data = &data[1..];

        let (value, rest) = read_cstr(data)?;
        data = rest;

        match field_type {
            field_type::SEVERITY => fields.severity = Some(value.to_string()),
            field_type::SEVERITY_NON_LOCALIZED => {
                fields.severity_non_localized = Some(value.to_string())
            }
            field_type::CODE => fields.code = Some(value.to_string()),
            field_type::MESSAGE => fields.message = Some(value.to_string()),
            field_type::DETAIL => fields.detail = Some(value.to_string()),
            field_type::HINT => fields.hint = Some(value.to_string()),
            field_type::POSITION => fields.position = value.parse().ok(),
            field_type::INTERNAL_POSITION => fields.internal_position = value.parse().ok(),
            field_type::INTERNAL_QUERY => fields.internal_query = Some(value.to_string()),
            field_type::WHERE => fields.where_ = Some(value.to_string()),
            field_type::SCHEMA => fields.schema = Some(value.to_string()),
            field_type::TABLE => fields.table = Some(value.to_string()),
            field_type::COLUMN => fields.column = Some(value.to_string()),
            field_type::DATA_TYPE => fields.data_type = Some(value.to_string()),
            field_type::CONSTRAINT => fields.constraint = Some(value.to_string()),
            field_type::FILE => fields.file = Some(value.to_string()),
            field_type::LINE => fields.line = value.parse().ok(),
            field_type::ROUTINE => fields.routine = Some(value.to_string()),
            _ => {
                // Unknown field type - ignore
                tracing::debug!("Unknown error field type: {}", field_type as char);
            }
        }
    }

    Ok(fields)
}

/// ErrorResponse message - fatal error from server.
#[derive(Debug, Clone)]
pub struct ErrorResponse {
    /// Parsed error fields
    pub fields: ErrorFields,
}

impl ErrorResponse {
    /// Parse an ErrorResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        Ok(Self {
            fields: parse_fields(payload)?,
        })
    }

    /// Convert to an Error.
    pub fn into_error(self) -> Error {
        Error::Server(self.fields)
    }

    /// Get the SQLSTATE code.
    pub fn code(&self) -> Option<&str> {
        self.fields.code.as_deref()
    }

    /// Get the primary message.
    pub fn message(&self) -> Option<&str> {
        self.fields.message.as_deref()
    }

    /// Get the severity.
    pub fn severity(&self) -> Option<&str> {
        self.fields
            .severity_non_localized
            .as_deref()
            .or(self.fields.severity.as_deref())
    }
}

/// NoticeResponse message - non-fatal warning/info from server.
#[derive(Debug, Clone)]
pub struct NoticeResponse {
    /// Parsed notice fields
    pub fields: ErrorFields,
}

impl NoticeResponse {
    /// Parse a NoticeResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        Ok(Self {
            fields: parse_fields(payload)?,
        })
    }

    /// Get the SQLSTATE code.
    pub fn code(&self) -> Option<&str> {
        self.fields.code.as_deref()
    }

    /// Get the primary message.
    pub fn message(&self) -> Option<&str> {
        self.fields.message.as_deref()
    }

    /// Get the severity.
    pub fn severity(&self) -> Option<&str> {
        self.fields
            .severity_non_localized
            .as_deref()
            .or(self.fields.severity.as_deref())
    }
}
