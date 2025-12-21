//! String type implementations (&str, String).

use crate::error::{Error, Result};
use crate::protocol::types::{oid, Oid};

use super::{FromWireValue, ToWireValue};

impl<'a> FromWireValue<'a> for &'a str {
    fn from_text(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        // NUMERIC uses text format, so it can be decoded as str in text mode
        if !matches!(
            oid,
            oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::NUMERIC
        ) {
            return Err(Error::Decode(format!("cannot decode oid {} as str", oid)));
        }
        simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        // Note: NUMERIC binary format is NOT UTF-8, so we don't accept it here
        if !matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME) {
            return Err(Error::Decode(format!("cannot decode oid {} as str", oid)));
        }
        simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }
}

impl FromWireValue<'_> for String {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        // NUMERIC uses text format, so it can be decoded as String in text mode
        if !matches!(
            oid,
            oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::NUMERIC
        ) {
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
        // Note: NUMERIC binary format is NOT UTF-8, so we don't accept it here
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

impl ToWireValue for str {
    fn natural_oid(&self) -> Oid {
        oid::TEXT
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::JSON | oid::JSONB => {
                let bytes = self.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl ToWireValue for String {
    fn natural_oid(&self) -> Oid {
        oid::TEXT
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        self.as_str().encode(target_oid, buf)
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
