//! Byte type implementations (`&[u8]`, `Vec<u8>`).

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

impl<'a> FromWireValue<'a> for &'a [u8] {
    fn from_text(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        if oid != oid::BYTEA {
            return Err(Error::Decode(format!("cannot decode oid {} as bytes", oid)));
        }
        // Text format for bytea is hex-encoded: \x followed by hex digits
        // For simplicity, we just return the raw bytes (caller can decode if needed)
        Ok(bytes)
    }

    fn from_binary(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        if oid != oid::BYTEA {
            return Err(Error::Decode(format!("cannot decode oid {} as bytes", oid)));
        }
        Ok(bytes)
    }
}

impl FromWireValue<'_> for Vec<u8> {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::BYTEA {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as Vec<u8>",
                oid
            )));
        }
        // Text format for bytea is hex-encoded: \xDEADBEEF
        if bytes.starts_with(b"\\x") {
            decode_hex(&bytes[2..])
        } else {
            // Fallback: return raw bytes
            Ok(bytes.to_vec())
        }
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::BYTEA {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as Vec<u8>",
                oid
            )));
        }
        Ok(bytes.to_vec())
    }
}

impl ToWireValue for &[u8] {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.len() as i32).to_be_bytes());
        buf.extend_from_slice(self);
    }
}

impl ToWireValue for Vec<u8> {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().to_binary(buf);
    }
}

/// Decode hex string to bytes
fn decode_hex(hex: &[u8]) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(Error::Decode("invalid hex length".into()));
    }

    let mut result = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.chunks(2) {
        let high = hex_digit(chunk[0])?;
        let low = hex_digit(chunk[1])?;
        result.push((high << 4) | low);
    }
    Ok(result)
}

fn hex_digit(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::Decode(format!("invalid hex digit: {}", b as char))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytea_hex() {
        assert_eq!(
            Vec::<u8>::from_text(oid::BYTEA, b"\\xDEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }
}
