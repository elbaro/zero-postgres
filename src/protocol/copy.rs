//! COPY protocol messages shared between frontend and backend.

use crate::error::Result;

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
