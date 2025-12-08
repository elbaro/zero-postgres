//! Extended query protocol backend messages.

use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::error::{Error, Result};
use crate::protocol::codec::read_u32;
use crate::protocol::types::{Oid, U16BE};

/// ParseComplete message - statement parsing completed.
#[derive(Debug, Clone, Copy)]
pub struct ParseComplete;

impl ParseComplete {
    /// Parse a ParseComplete message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}

/// BindComplete message - parameter binding completed.
#[derive(Debug, Clone, Copy)]
pub struct BindComplete;

impl BindComplete {
    /// Parse a BindComplete message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}

/// CloseComplete message - statement/portal close completed.
#[derive(Debug, Clone, Copy)]
pub struct CloseComplete;

impl CloseComplete {
    /// Parse a CloseComplete message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}

/// NoData message - query returns no data.
#[derive(Debug, Clone, Copy)]
pub struct NoData;

impl NoData {
    /// Parse a NoData message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}

/// PortalSuspended message - row limit reached in Execute.
#[derive(Debug, Clone, Copy)]
pub struct PortalSuspended;

impl PortalSuspended {
    /// Parse a PortalSuspended message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}

/// ParameterDescription message header.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct ParameterDescriptionHead {
    /// Number of parameters
    pub num_params: U16BE,
}

/// ParameterDescription message - describes parameters for a prepared statement.
#[derive(Debug, Clone)]
pub struct ParameterDescription {
    /// Parameter type OIDs
    param_oids: Vec<Oid>,
}

impl ParameterDescription {
    /// Parse a ParameterDescription message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let head = ParameterDescriptionHead::ref_from_bytes(&payload[..2])
            .map_err(|e| Error::Protocol(format!("ParameterDescription header: {e:?}")))?;

        let num_params = head.num_params.get() as usize;
        let mut param_oids = Vec::with_capacity(num_params);
        let mut data = &payload[2..];

        for _ in 0..num_params {
            let (oid, rest) = read_u32(data)?;
            param_oids.push(oid);
            data = rest;
        }

        Ok(Self { param_oids })
    }

    /// Get the number of parameters.
    pub fn len(&self) -> usize {
        self.param_oids.len()
    }

    /// Check if there are no parameters.
    pub fn is_empty(&self) -> bool {
        self.param_oids.is_empty()
    }

    /// Get parameter type OIDs.
    pub fn oids(&self) -> &[Oid] {
        &self.param_oids
    }

    /// Iterate over parameter type OIDs.
    pub fn iter(&self) -> impl Iterator<Item = &Oid> {
        self.param_oids.iter()
    }
}
