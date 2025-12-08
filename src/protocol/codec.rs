//! PostgreSQL wire protocol encoding and decoding primitives.
//!
//! PostgreSQL uses big-endian (network byte order) for all integers.

use crate::error::{Error, Result};
use zerocopy::FromBytes;

use super::types::{I16BE, I32BE, U16BE, U32BE};

/// Read 1-byte unsigned integer.
#[inline]
pub fn read_u8(data: &[u8]) -> Result<(u8, &[u8])> {
    if data.is_empty() {
        return Err(Error::Protocol("read_u8: empty buffer".into()));
    }
    Ok((data[0], &data[1..]))
}

/// Read 2-byte big-endian signed integer.
#[inline]
pub fn read_i16(data: &[u8]) -> Result<(i16, &[u8])> {
    if data.len() < 2 {
        return Err(Error::Protocol(format!(
            "read_i16: buffer too short: {} < 2",
            data.len()
        )));
    }
    let value = I16BE::ref_from_bytes(&data[..2])
        .map_err(|e| Error::Protocol(format!("read_i16: {e:?}")))?
        .get();
    Ok((value, &data[2..]))
}

/// Read 2-byte big-endian unsigned integer.
#[inline]
pub fn read_u16(data: &[u8]) -> Result<(u16, &[u8])> {
    if data.len() < 2 {
        return Err(Error::Protocol(format!(
            "read_u16: buffer too short: {} < 2",
            data.len()
        )));
    }
    let value = U16BE::ref_from_bytes(&data[..2])
        .map_err(|e| Error::Protocol(format!("read_u16: {e:?}")))?
        .get();
    Ok((value, &data[2..]))
}

/// Read 4-byte big-endian signed integer.
#[inline]
pub fn read_i32(data: &[u8]) -> Result<(i32, &[u8])> {
    if data.len() < 4 {
        return Err(Error::Protocol(format!(
            "read_i32: buffer too short: {} < 4",
            data.len()
        )));
    }
    let value = I32BE::ref_from_bytes(&data[..4])
        .map_err(|e| Error::Protocol(format!("read_i32: {e:?}")))?
        .get();
    Ok((value, &data[4..]))
}

/// Read 4-byte big-endian unsigned integer.
#[inline]
pub fn read_u32(data: &[u8]) -> Result<(u32, &[u8])> {
    if data.len() < 4 {
        return Err(Error::Protocol(format!(
            "read_u32: buffer too short: {} < 4",
            data.len()
        )));
    }
    let value = U32BE::ref_from_bytes(&data[..4])
        .map_err(|e| Error::Protocol(format!("read_u32: {e:?}")))?
        .get();
    Ok((value, &data[4..]))
}

/// Read fixed-length bytes.
#[inline]
pub fn read_bytes(data: &[u8], len: usize) -> Result<(&[u8], &[u8])> {
    if data.len() < len {
        return Err(Error::Protocol(format!(
            "read_bytes: buffer too short: {} < {}",
            data.len(),
            len
        )));
    }
    Ok((&data[..len], &data[len..]))
}

/// Read null-terminated string (PostgreSQL String type).
/// Returns the string bytes (without the null terminator) and remaining data.
#[inline]
pub fn read_cstring(data: &[u8]) -> Result<(&[u8], &[u8])> {
    match memchr::memchr(0, data) {
        Some(pos) => Ok((&data[..pos], &data[pos + 1..])),
        None => Err(Error::Protocol(
            "read_cstring: no null terminator found".into(),
        )),
    }
}

/// Read null-terminated string as &str.
#[inline]
pub fn read_cstr(data: &[u8]) -> Result<(&str, &[u8])> {
    let (bytes, rest) = read_cstring(data)?;
    let s = std::str::from_utf8(bytes)
        .map_err(|e| Error::Protocol(format!("read_cstr: invalid UTF-8: {e}")))?;
    Ok((s, rest))
}

/// Write 1-byte unsigned integer.
#[inline]
pub fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

/// Write 2-byte big-endian signed integer.
#[inline]
pub fn write_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Write 2-byte big-endian unsigned integer.
#[inline]
pub fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Write 4-byte big-endian signed integer.
#[inline]
pub fn write_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Write 4-byte big-endian unsigned integer.
#[inline]
pub fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Write raw bytes.
#[inline]
pub fn write_bytes(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(data);
}

/// Write null-terminated string (PostgreSQL String type).
#[inline]
pub fn write_cstring(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(s);
    out.push(0);
}

/// Write null-terminated string from &str.
#[inline]
pub fn write_cstr(out: &mut Vec<u8>, s: &str) {
    write_cstring(out, s.as_bytes());
}

/// Message builder helper that handles the length field.
///
/// PostgreSQL message format:
/// - Type byte (1 byte) - NOT included in length
/// - Length (4 bytes) - includes itself
/// - Payload (Length - 4 bytes)
pub struct MessageBuilder<'a> {
    buf: &'a mut Vec<u8>,
    start: usize,
}

impl<'a> MessageBuilder<'a> {
    /// Start building a message with a type byte.
    pub fn new(buf: &'a mut Vec<u8>, type_byte: u8) -> Self {
        buf.push(type_byte);
        let start = buf.len();
        buf.extend_from_slice(&[0, 0, 0, 0]); // Placeholder for length
        Self { buf, start }
    }

    /// Start building a startup message (no type byte).
    pub fn new_startup(buf: &'a mut Vec<u8>) -> Self {
        let start = buf.len();
        buf.extend_from_slice(&[0, 0, 0, 0]); // Placeholder for length
        Self { buf, start }
    }

    /// Get mutable access to the underlying buffer.
    pub fn buf(&mut self) -> &mut Vec<u8> {
        self.buf
    }

    /// Write a u8.
    pub fn write_u8(&mut self, value: u8) {
        write_u8(self.buf, value);
    }

    /// Write an i16.
    pub fn write_i16(&mut self, value: i16) {
        write_i16(self.buf, value);
    }

    /// Write a u16.
    pub fn write_u16(&mut self, value: u16) {
        write_u16(self.buf, value);
    }

    /// Write an i32.
    pub fn write_i32(&mut self, value: i32) {
        write_i32(self.buf, value);
    }

    /// Write a u32.
    pub fn write_u32(&mut self, value: u32) {
        write_u32(self.buf, value);
    }

    /// Write raw bytes.
    pub fn write_bytes(&mut self, data: &[u8]) {
        write_bytes(self.buf, data);
    }

    /// Write null-terminated string.
    pub fn write_cstr(&mut self, s: &str) {
        write_cstr(self.buf, s);
    }

    /// Finish building the message and fill in the length field.
    pub fn finish(self) {
        let len = (self.buf.len() - self.start) as i32;
        self.buf[self.start..self.start + 4].copy_from_slice(&len.to_be_bytes());
    }
}
