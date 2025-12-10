//! Error and notice response messages.

use std::collections::HashMap;

use crate::error::{Error, Result, ServerError};
use crate::protocol::codec::read_cstr;

/// Parse error/notice fields from payload into a ServerError.
fn parse_fields(payload: &[u8]) -> Result<ServerError> {
    let mut fields = HashMap::new();
    let mut data = payload;

    while !data.is_empty() && data[0] != 0 {
        let field_type = data[0];
        data = &data[1..];

        let (value, rest) = read_cstr(data)?;
        data = rest;

        fields.insert(field_type, value.to_string());
    }

    Ok(ServerError::new(fields))
}

/// ErrorResponse message - fatal error from server.
#[derive(Debug, Clone)]
pub struct ErrorResponse(pub ServerError);

impl ErrorResponse {
    /// Parse an ErrorResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        Ok(Self(parse_fields(payload)?))
    }

    /// Convert to an Error.
    pub fn into_error(self) -> Error {
        Error::Server(self.0)
    }

    /// Get the underlying ServerError.
    pub fn error(&self) -> &ServerError {
        &self.0
    }
}

/// NoticeResponse message - non-fatal warning/info from server.
#[derive(Debug, Clone)]
pub struct NoticeResponse(pub ServerError);

impl NoticeResponse {
    /// Parse a NoticeResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        Ok(Self(parse_fields(payload)?))
    }

    /// Get the underlying ServerError.
    pub fn error(&self) -> &ServerError {
        &self.0
    }
}
