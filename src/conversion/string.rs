//! String type implementations (&str, String).

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

impl<'a> FromWireValue<'a> for &'a str {
    fn from_text(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        if !matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME) {
            return Err(Error::Decode(format!("cannot decode oid {} as str", oid)));
        }
        simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        if !matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME) {
            return Err(Error::Decode(format!("cannot decode oid {} as str", oid)));
        }
        simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }
}

impl FromWireValue<'_> for String {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME) {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as String",
                oid
            )));
        }
        simdutf8::compat::from_utf8(bytes)
            .map(|s| s.to_owned())
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME) {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as String",
                oid
            )));
        }
        simdutf8::compat::from_utf8(bytes)
            .map(|s| s.to_owned())
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }
}

impl ToWireValue for &str {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.len() as i32).to_be_bytes());
        buf.extend_from_slice(self.as_bytes());
    }
}

impl ToWireValue for String {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        self.as_str().to_binary(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_text() {
        assert_eq!(String::from_text(oid::TEXT, b"hello").unwrap(), "hello");
    }

    #[test]
    fn test_type_mismatch() {
        // Trying to decode INT4 as String should fail
        assert!(String::from_binary(oid::INT4, &[0, 0, 0, 1]).is_err());
    }
}
