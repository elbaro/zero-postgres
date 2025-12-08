//! COPY protocol backend messages.

use crate::error::Result;
use crate::protocol::codec::{read_u8, read_u16};
use crate::protocol::types::FormatCode;

/// CopyInResponse message - server is ready to receive COPY data.
#[derive(Debug, Clone)]
pub struct CopyInResponse {
    /// Overall format (0=text, 1=binary)
    pub format: FormatCode,
    /// Per-column format codes
    pub column_formats: Vec<FormatCode>,
}

impl CopyInResponse {
    /// Parse a CopyInResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let (format_byte, rest) = read_u8(payload)?;
        let format = FormatCode::from_u16(format_byte as u16);

        let (num_columns, mut rest) = read_u16(rest)?;
        let mut column_formats = Vec::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            let (fmt, remaining) = read_u16(rest)?;
            column_formats.push(FormatCode::from_u16(fmt));
            rest = remaining;
        }

        Ok(Self {
            format,
            column_formats,
        })
    }

    /// Check if binary format is used.
    pub fn is_binary(&self) -> bool {
        matches!(self.format, FormatCode::Binary)
    }
}

/// CopyOutResponse message - server is about to send COPY data.
#[derive(Debug, Clone)]
pub struct CopyOutResponse {
    /// Overall format (0=text, 1=binary)
    pub format: FormatCode,
    /// Per-column format codes
    pub column_formats: Vec<FormatCode>,
}

impl CopyOutResponse {
    /// Parse a CopyOutResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let (format_byte, rest) = read_u8(payload)?;
        let format = FormatCode::from_u16(format_byte as u16);

        let (num_columns, mut rest) = read_u16(rest)?;
        let mut column_formats = Vec::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            let (fmt, remaining) = read_u16(rest)?;
            column_formats.push(FormatCode::from_u16(fmt));
            rest = remaining;
        }

        Ok(Self {
            format,
            column_formats,
        })
    }

    /// Check if binary format is used.
    pub fn is_binary(&self) -> bool {
        matches!(self.format, FormatCode::Binary)
    }
}

/// CopyBothResponse message - server is ready for bidirectional COPY (replication).
#[derive(Debug, Clone)]
pub struct CopyBothResponse {
    /// Overall format (0=text, 1=binary)
    pub format: FormatCode,
    /// Per-column format codes
    pub column_formats: Vec<FormatCode>,
}

impl CopyBothResponse {
    /// Parse a CopyBothResponse message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let (format_byte, rest) = read_u8(payload)?;
        let format = FormatCode::from_u16(format_byte as u16);

        let (num_columns, mut rest) = read_u16(rest)?;
        let mut column_formats = Vec::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            let (fmt, remaining) = read_u16(rest)?;
            column_formats.push(FormatCode::from_u16(fmt));
            rest = remaining;
        }

        Ok(Self {
            format,
            column_formats,
        })
    }
}

/// CopyData message - COPY data (used in both directions).
#[derive(Debug, Clone, Copy)]
pub struct CopyData<'a> {
    /// Raw data bytes
    pub data: &'a [u8],
}

impl<'a> CopyData<'a> {
    /// Parse a CopyData message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        Ok(Self { data: payload })
    }
}

/// CopyDone message - COPY operation completed.
#[derive(Debug, Clone, Copy)]
pub struct CopyDone;

impl CopyDone {
    /// Parse a CopyDone message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}
