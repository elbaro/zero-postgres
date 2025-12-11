//! Primitive type implementations (bool, integers, floats).

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

// === Boolean ===

impl FromWireValue<'_> for bool {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::BOOL {
            return Err(Error::Decode(format!("cannot decode oid {} as bool", oid)));
        }
        match bytes {
            b"t" | b"true" | b"TRUE" | b"T" | b"1" => Ok(true),
            b"f" | b"false" | b"FALSE" | b"F" | b"0" => Ok(false),
            _ => Err(Error::Decode(format!(
                "invalid boolean: {:?}",
                String::from_utf8_lossy(bytes)
            ))),
        }
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::BOOL {
            return Err(Error::Decode(format!("cannot decode oid {} as bool", oid)));
        }
        if bytes.len() != 1 {
            return Err(Error::Decode(format!(
                "invalid boolean length: {}",
                bytes.len()
            )));
        }
        Ok(bytes[0] != 0)
    }
}

impl ToWireValue for bool {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&1_i32.to_be_bytes());
        buf.push(if *self { 1 } else { 0 });
    }
}

// === Integer types ===

impl FromWireValue<'_> for i16 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::INT2 {
            return Err(Error::Decode(format!("cannot decode oid {} as i16", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid i16: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::INT2 {
            return Err(Error::Decode(format!("cannot decode oid {} as i16", oid)));
        }
        let arr: [u8; 2] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid i16 length: {}", bytes.len())))?;
        Ok(i16::from_be_bytes(arr))
    }
}

impl ToWireValue for i16 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&2_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl FromWireValue<'_> for i32 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::INT2 | oid::INT4) {
            return Err(Error::Decode(format!("cannot decode oid {} as i32", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid i32: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        match oid {
            oid::INT2 => {
                let arr: [u8; 2] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid i16 length: {}", bytes.len())))?;
                Ok(i16::from_be_bytes(arr) as i32)
            }
            oid::INT4 => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid i32 length: {}", bytes.len())))?;
                Ok(i32::from_be_bytes(arr))
            }
            _ => Err(Error::Decode(format!("cannot decode oid {} as i32", oid))),
        }
    }
}

impl ToWireValue for i32 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&4_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl FromWireValue<'_> for i64 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::INT2 | oid::INT4 | oid::INT8) {
            return Err(Error::Decode(format!("cannot decode oid {} as i64", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid i64: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        match oid {
            oid::INT2 => {
                let arr: [u8; 2] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid i16 length: {}", bytes.len())))?;
                Ok(i16::from_be_bytes(arr) as i64)
            }
            oid::INT4 => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid i32 length: {}", bytes.len())))?;
                Ok(i32::from_be_bytes(arr) as i64)
            }
            oid::INT8 => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid i64 length: {}", bytes.len())))?;
                Ok(i64::from_be_bytes(arr))
            }
            _ => Err(Error::Decode(format!("cannot decode oid {} as i64", oid))),
        }
    }
}

impl ToWireValue for i64 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&8_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

// === Floating point types ===

impl FromWireValue<'_> for f32 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::FLOAT4 {
            return Err(Error::Decode(format!("cannot decode oid {} as f32", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid f32: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::FLOAT4 {
            return Err(Error::Decode(format!("cannot decode oid {} as f32", oid)));
        }
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid f32 length: {}", bytes.len())))?;
        Ok(f32::from_be_bytes(arr))
    }
}

impl ToWireValue for f32 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&4_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl FromWireValue<'_> for f64 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::FLOAT4 | oid::FLOAT8) {
            return Err(Error::Decode(format!("cannot decode oid {} as f64", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid f64: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        match oid {
            oid::FLOAT4 => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid f32 length: {}", bytes.len())))?;
                Ok(f32::from_be_bytes(arr) as f64)
            }
            oid::FLOAT8 => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid f64 length: {}", bytes.len())))?;
                Ok(f64::from_be_bytes(arr))
            }
            _ => Err(Error::Decode(format!("cannot decode oid {} as f64", oid))),
        }
    }
}

impl ToWireValue for f64 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&8_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_text() {
        assert!(bool::from_text(oid::BOOL, b"t").unwrap());
        assert!(bool::from_text(oid::BOOL, b"true").unwrap());
        assert!(!bool::from_text(oid::BOOL, b"f").unwrap());
        assert!(!bool::from_text(oid::BOOL, b"false").unwrap());
    }

    #[test]
    fn test_bool_binary() {
        assert!(bool::from_binary(oid::BOOL, &[1]).unwrap());
        assert!(!bool::from_binary(oid::BOOL, &[0]).unwrap());
    }

    #[test]
    fn test_i32_text() {
        assert_eq!(i32::from_text(oid::INT4, b"12345").unwrap(), 12345);
        assert_eq!(i32::from_text(oid::INT4, b"-12345").unwrap(), -12345);
    }

    #[test]
    fn test_i32_binary() {
        assert_eq!(
            i32::from_binary(oid::INT4, &[0, 0, 0x30, 0x39]).unwrap(),
            12345
        );
    }

    #[test]
    fn test_f64_text() {
        assert_eq!(f64::from_text(oid::FLOAT8, b"3.14").unwrap(), 3.14);
    }

    #[test]
    fn test_widening() {
        // i32 can decode INT2
        assert_eq!(i32::from_binary(oid::INT2, &[0, 42]).unwrap(), 42);
        // i64 can decode INT4
        assert_eq!(i64::from_binary(oid::INT4, &[0, 0, 0, 42]).unwrap(), 42);
        // f64 can decode FLOAT4
        let f32_bytes = 3.14_f32.to_be_bytes();
        assert!((f64::from_binary(oid::FLOAT4, &f32_bytes).unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_type_mismatch() {
        // Trying to decode TEXT as i32 should fail
        assert!(i32::from_text(oid::TEXT, b"123").is_err());
    }
}
