//! Authentication-related backend messages.

use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::error::{Error, Result};
use crate::protocol::codec::{read_cstr, read_i32, read_u32};
use crate::protocol::types::TransactionStatus;
use zerocopy::byteorder::big_endian::U32 as U32BE;

/// Authentication method constants.
pub mod auth_type {
    pub const OK: i32 = 0;
    pub const KERBEROS_V5: i32 = 2;
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    pub const MD5_PASSWORD: i32 = 5;
    pub const GSS: i32 = 7;
    pub const GSS_CONTINUE: i32 = 8;
    pub const SSPI: i32 = 9;
    pub const SASL: i32 = 10;
    pub const SASL_CONTINUE: i32 = 11;
    pub const SASL_FINAL: i32 = 12;
}

/// Authentication message from the server.
#[derive(Debug)]
pub enum AuthenticationMessage<'a> {
    /// Authentication successful
    Ok,
    /// Kerberos V5 authentication required
    KerberosV5,
    /// Cleartext password required
    CleartextPassword,
    /// MD5 password required (with 4-byte salt)
    Md5Password { salt: [u8; 4] },
    /// GSS authentication
    Gss,
    /// GSS continue (with additional data)
    GssContinue { data: &'a [u8] },
    /// SSPI authentication
    Sspi,
    /// SASL authentication required (with list of mechanisms)
    Sasl { mechanisms: Vec<&'a str> },
    /// SASL continue (with server-first-message)
    SaslContinue { data: &'a [u8] },
    /// SASL final (with server-final-message)
    SaslFinal { data: &'a [u8] },
}

impl<'a> AuthenticationMessage<'a> {
    /// Parse an Authentication message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let (auth_type, rest) = read_i32(payload)?;

        match auth_type {
            auth_type::OK => Ok(AuthenticationMessage::Ok),
            auth_type::KERBEROS_V5 => Ok(AuthenticationMessage::KerberosV5),
            auth_type::CLEARTEXT_PASSWORD => Ok(AuthenticationMessage::CleartextPassword),
            auth_type::MD5_PASSWORD => {
                if rest.len() < 4 {
                    return Err(Error::Protocol("MD5Password: missing salt".into()));
                }
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&rest[..4]);
                Ok(AuthenticationMessage::Md5Password { salt })
            }
            auth_type::GSS => Ok(AuthenticationMessage::Gss),
            auth_type::GSS_CONTINUE => Ok(AuthenticationMessage::GssContinue { data: rest }),
            auth_type::SSPI => Ok(AuthenticationMessage::Sspi),
            auth_type::SASL => {
                let mut mechanisms = Vec::new();
                let mut data = rest;
                while !data.is_empty() && data[0] != 0 {
                    let (mechanism, remaining) = read_cstr(data)?;
                    mechanisms.push(mechanism);
                    data = remaining;
                }
                Ok(AuthenticationMessage::Sasl { mechanisms })
            }
            auth_type::SASL_CONTINUE => Ok(AuthenticationMessage::SaslContinue { data: rest }),
            auth_type::SASL_FINAL => Ok(AuthenticationMessage::SaslFinal { data: rest }),
            _ => Err(Error::Protocol(format!(
                "Unknown authentication type: {}",
                auth_type
            ))),
        }
    }
}

/// BackendKeyData message - contains process ID and secret key for cancellation.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct BackendKeyData {
    /// Process ID of the backend
    pub pid: U32BE,
    /// Secret key for cancellation
    pub secret_key: U32BE,
}

impl BackendKeyData {
    /// Parse a BackendKeyData message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<&Self> {
        Self::ref_from_bytes(payload).map_err(|e| Error::Protocol(format!("BackendKeyData: {e:?}")))
    }

    /// Get the process ID.
    pub fn process_id(&self) -> u32 {
        self.pid.get()
    }

    /// Get the secret key.
    pub fn secret(&self) -> u32 {
        self.secret_key.get()
    }
}

/// ParameterStatus message - server parameter name and value.
#[derive(Debug, Clone)]
pub struct ParameterStatus<'a> {
    /// Parameter name
    pub name: &'a str,
    /// Parameter value
    pub value: &'a str,
}

impl<'a> ParameterStatus<'a> {
    /// Parse a ParameterStatus message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let (name, rest) = read_cstr(payload)?;
        let (value, _) = read_cstr(rest)?;
        Ok(Self { name, value })
    }
}

/// ReadyForQuery message - indicates server is ready for a new query.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct ReadyForQuery {
    /// Transaction status byte
    pub status: u8,
}

impl ReadyForQuery {
    /// Parse a ReadyForQuery message from payload bytes.
    pub fn parse(payload: &[u8]) -> Result<&Self> {
        Self::ref_from_bytes(payload).map_err(|e| Error::Protocol(format!("ReadyForQuery: {e:?}")))
    }

    /// Get the transaction status.
    pub fn transaction_status(&self) -> Option<TransactionStatus> {
        TransactionStatus::from_byte(self.status)
    }
}

/// NotificationResponse message - asynchronous notification from LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct NotificationResponse<'a> {
    /// PID of the notifying backend
    pub pid: u32,
    /// Channel name
    pub channel: &'a str,
    /// Notification payload
    pub payload: &'a str,
}

impl<'a> NotificationResponse<'a> {
    /// Parse a NotificationResponse message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let (pid, rest) = read_u32(payload)?;
        let (channel, rest) = read_cstr(rest)?;
        let (payload_str, _) = read_cstr(rest)?;
        Ok(Self {
            pid,
            channel,
            payload: payload_str,
        })
    }
}

/// NegotiateProtocolVersion message - server doesn't support requested protocol features.
#[derive(Debug, Clone)]
pub struct NegotiateProtocolVersion<'a> {
    /// Newest minor protocol version supported
    pub newest_minor_version: u32,
    /// Unrecognized protocol options
    pub unrecognized_options: Vec<&'a str>,
}

impl<'a> NegotiateProtocolVersion<'a> {
    /// Parse a NegotiateProtocolVersion message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let (newest_minor_version, rest) = read_u32(payload)?;
        let (num_options, mut rest) = read_u32(rest)?;

        let mut unrecognized_options = Vec::with_capacity(num_options as usize);
        for _ in 0..num_options {
            let (option, remaining) = read_cstr(rest)?;
            unrecognized_options.push(option);
            rest = remaining;
        }

        Ok(Self {
            newest_minor_version,
            unrecognized_options,
        })
    }
}
