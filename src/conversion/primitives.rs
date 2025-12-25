//! Primitive type implementations (bool, integers, floats).

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::numeric_util::{numeric_to_f32, numeric_to_f64};
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
    fn natural_oid(&self) -> Oid {
        oid::BOOL
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::BOOL => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(if *self { 1 } else { 0 });
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
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
    fn natural_oid(&self) -> Oid {
        oid::INT2
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&self.to_be_bytes());
            }
            oid::INT4 => {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i32).to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i64).to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
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
    fn natural_oid(&self) -> Oid {
        oid::INT4
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                let v = i16::try_from(*self).map_err(|_| Error::overflow("i32", "INT2"))?;
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT4 => {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&self.to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i64).to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
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
    fn natural_oid(&self) -> Oid {
        oid::INT8
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                let v = i16::try_from(*self).map_err(|_| Error::overflow("i64", "INT2"))?;
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT4 => {
                let v = i32::try_from(*self).map_err(|_| Error::overflow("i64", "INT4"))?;
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&self.to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

// === i8 (encodes as INT2) ===

impl ToWireValue for i8 {
    fn natural_oid(&self) -> Oid {
        oid::INT2 // PostgreSQL doesn't have INT1, use INT2
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i16).to_be_bytes());
            }
            oid::INT4 => {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i32).to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i64).to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

// === u8 (encodes as INT2) ===

impl ToWireValue for u8 {
    fn natural_oid(&self) -> Oid {
        oid::INT2 // PostgreSQL doesn't have unsigned types, use INT2
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i16).to_be_bytes());
            }
            oid::INT4 => {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i32).to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i64).to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

// === u16 (encodes as INT4 due to sign) ===

impl ToWireValue for u16 {
    fn natural_oid(&self) -> Oid {
        // u16 max (65535) exceeds i16 max (32767), so use INT4
        oid::INT4
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                let v = i16::try_from(*self).map_err(|_| Error::overflow("u16", "INT2"))?;
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT4 => {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i32).to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i64).to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

// === u32 (encodes as INT8 due to sign) ===

impl ToWireValue for u32 {
    fn natural_oid(&self) -> Oid {
        // u32 max (4294967295) exceeds i32 max (2147483647), so use INT8
        oid::INT8
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                let v = i16::try_from(*self).map_err(|_| Error::overflow("u32", "INT2"))?;
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT4 => {
                let v = i32::try_from(*self).map_err(|_| Error::overflow("u32", "INT4"))?;
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as i64).to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

// === u64 (encodes as INT8 with overflow check) ===

impl ToWireValue for u64 {
    fn natural_oid(&self) -> Oid {
        oid::INT8
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::INT2 => {
                let v = i16::try_from(*self).map_err(|_| Error::overflow("u64", "INT2"))?;
                buf.extend_from_slice(&2_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT4 => {
                let v = i32::try_from(*self).map_err(|_| Error::overflow("u64", "INT4"))?;
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            oid::INT8 => {
                let v = i64::try_from(*self).map_err(|_| Error::overflow("u64", "INT8"))?;
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&v.to_be_bytes());
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

// === Floating point types ===

impl FromWireValue<'_> for f32 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::FLOAT4 | oid::NUMERIC) {
            return Err(Error::Decode(format!("cannot decode oid {} as f32", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;

        // Handle special text values
        match s {
            "NaN" => return Ok(f32::NAN),
            "Infinity" => return Ok(f32::INFINITY),
            "-Infinity" => return Ok(f32::NEG_INFINITY),
            _ => {}
        }

        let value: f64 = s
            .parse()
            .map_err(|e| Error::Decode(format!("invalid f32: {}", e)))?;

        // Check for overflow
        if value > f32::MAX as f64 || value < f32::MIN as f64 {
            return Err(Error::Decode("NUMERIC value overflows f32".to_string()));
        }

        Ok(value as f32)
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        match oid {
            oid::FLOAT4 => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| Error::Decode(format!("invalid f32 length: {}", bytes.len())))?;
                Ok(f32::from_be_bytes(arr))
            }
            oid::NUMERIC => numeric_to_f32(bytes),
            _ => Err(Error::Decode(format!("cannot decode oid {} as f32", oid))),
        }
    }
}

impl ToWireValue for f32 {
    fn natural_oid(&self) -> Oid {
        oid::FLOAT4
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::FLOAT4 => {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&self.to_bits().to_be_bytes());
            }
            oid::FLOAT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as f64).to_bits().to_be_bytes());
            }
            oid::NUMERIC => {
                // NUMERIC uses text format
                let s = self.to_string();
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
    }
}

impl FromWireValue<'_> for f64 {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::FLOAT4 | oid::FLOAT8 | oid::NUMERIC) {
            return Err(Error::Decode(format!("cannot decode oid {} as f64", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;

        // Handle special text values
        match s {
            "NaN" => return Ok(f64::NAN),
            "Infinity" => return Ok(f64::INFINITY),
            "-Infinity" => return Ok(f64::NEG_INFINITY),
            _ => {}
        }

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
            oid::NUMERIC => numeric_to_f64(bytes),
            _ => Err(Error::Decode(format!("cannot decode oid {} as f64", oid))),
        }
    }
}

impl ToWireValue for f64 {
    fn natural_oid(&self) -> Oid {
        oid::FLOAT8
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::FLOAT4 => {
                // Note: potential precision loss
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&(*self as f32).to_bits().to_be_bytes());
            }
            oid::FLOAT8 => {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&self.to_bits().to_be_bytes());
            }
            oid::NUMERIC => {
                // NUMERIC uses text format
                let s = self.to_string();
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
            _ => return Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
        Ok(())
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

    #[test]
    fn test_i8_encoding() {
        let mut buf = Vec::new();
        42i8.encode(oid::INT2, &mut buf).unwrap();
        // Length prefix (4 bytes) + i16 value (2 bytes)
        assert_eq!(buf.len(), 6);
        assert_eq!(&buf[0..4], &2_i32.to_be_bytes()); // length = 2
        assert_eq!(&buf[4..6], &42_i16.to_be_bytes());
    }

    #[test]
    fn test_u8_encoding() {
        let mut buf = Vec::new();
        200u8.encode(oid::INT2, &mut buf).unwrap();
        assert_eq!(buf.len(), 6);
        assert_eq!(&buf[0..4], &2_i32.to_be_bytes());
        assert_eq!(&buf[4..6], &200_i16.to_be_bytes());
    }

    #[test]
    fn test_u16_encoding() {
        let mut buf = Vec::new();
        50000u16.encode(oid::INT4, &mut buf).unwrap();
        // u16 encodes as INT4
        assert_eq!(buf.len(), 8);
        assert_eq!(&buf[0..4], &4_i32.to_be_bytes());
        assert_eq!(&buf[4..8], &50000_i32.to_be_bytes());
    }

    #[test]
    fn test_u16_overflow_to_int2() {
        // 50000 > i16::MAX, should fail when encoding to INT2
        let result = 50000u16.encode(oid::INT2, &mut Vec::new());
        assert!(result.is_err());

        // But 1000 should work
        let mut buf = Vec::new();
        1000u16.encode(oid::INT2, &mut buf).unwrap();
        assert_eq!(&buf[4..6], &1000_i16.to_be_bytes());
    }

    #[test]
    fn test_u32_encoding() {
        let mut buf = Vec::new();
        3_000_000_000u32.encode(oid::INT8, &mut buf).unwrap();
        // u32 encodes as INT8
        assert_eq!(buf.len(), 12);
        assert_eq!(&buf[0..4], &8_i32.to_be_bytes());
        assert_eq!(&buf[4..12], &3_000_000_000_i64.to_be_bytes());
    }

    #[test]
    fn test_u32_overflow_to_int4() {
        // 3 billion > i32::MAX, should fail when encoding to INT4
        let result = 3_000_000_000u32.encode(oid::INT4, &mut Vec::new());
        assert!(result.is_err());

        // But 1 million should work
        let mut buf = Vec::new();
        1_000_000u32.encode(oid::INT4, &mut buf).unwrap();
        assert_eq!(&buf[4..8], &1_000_000_i32.to_be_bytes());
    }

    #[test]
    fn test_u64_encoding() {
        let mut buf = Vec::new();
        1000u64.encode(oid::INT8, &mut buf).unwrap();
        assert_eq!(buf.len(), 12);
        assert_eq!(&buf[0..4], &8_i32.to_be_bytes());
        assert_eq!(&buf[4..12], &1000_i64.to_be_bytes());
    }

    #[test]
    fn test_u64_overflow() {
        // u64::MAX > i64::MAX, should fail
        let result = u64::MAX.encode(oid::INT8, &mut Vec::new());
        assert!(result.is_err());

        // But i64::MAX as u64 should work
        let mut buf = Vec::new();
        (i64::MAX as u64).encode(oid::INT8, &mut buf).unwrap();
        assert_eq!(&buf[4..12], &i64::MAX.to_be_bytes());
    }

    // Helper to build NUMERIC binary representation
    fn make_numeric(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for &d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    #[test]
    fn test_numeric_to_f64() {
        // 123.45
        let bytes = make_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        let result = f64::from_binary(oid::NUMERIC, &bytes).unwrap();
        assert!((result - 123.45).abs() < 0.001);
    }

    #[test]
    fn test_numeric_to_f64_negative() {
        // -123.45
        let bytes = make_numeric(2, 0, 0x4000, 2, &[123, 4500]);
        let result = f64::from_binary(oid::NUMERIC, &bytes).unwrap();
        assert!((result + 123.45).abs() < 0.001);
    }

    #[test]
    fn test_numeric_to_f64_special() {
        // NaN
        let bytes = make_numeric(0, 0, 0xC000, 0, &[]);
        assert!(f64::from_binary(oid::NUMERIC, &bytes).unwrap().is_nan());

        // +Infinity
        let bytes = make_numeric(0, 0, 0xD000, 0, &[]);
        assert_eq!(
            f64::from_binary(oid::NUMERIC, &bytes).unwrap(),
            f64::INFINITY
        );

        // -Infinity
        let bytes = make_numeric(0, 0, 0xF000, 0, &[]);
        assert_eq!(
            f64::from_binary(oid::NUMERIC, &bytes).unwrap(),
            f64::NEG_INFINITY
        );
    }

    #[test]
    fn test_numeric_to_f32() {
        // 123.45
        let bytes = make_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        let result = f32::from_binary(oid::NUMERIC, &bytes).unwrap();
        assert!((result - 123.45).abs() < 0.01);
    }

    #[test]
    fn test_numeric_to_f32_special() {
        // NaN
        let bytes = make_numeric(0, 0, 0xC000, 0, &[]);
        assert!(f32::from_binary(oid::NUMERIC, &bytes).unwrap().is_nan());

        // +Infinity
        let bytes = make_numeric(0, 0, 0xD000, 0, &[]);
        assert_eq!(
            f32::from_binary(oid::NUMERIC, &bytes).unwrap(),
            f32::INFINITY
        );

        // -Infinity
        let bytes = make_numeric(0, 0, 0xF000, 0, &[]);
        assert_eq!(
            f32::from_binary(oid::NUMERIC, &bytes).unwrap(),
            f32::NEG_INFINITY
        );
    }

    #[test]
    fn test_f64_from_text_special() {
        assert!(f64::from_text(oid::NUMERIC, b"NaN").unwrap().is_nan());
        assert_eq!(
            f64::from_text(oid::NUMERIC, b"Infinity").unwrap(),
            f64::INFINITY
        );
        assert_eq!(
            f64::from_text(oid::NUMERIC, b"-Infinity").unwrap(),
            f64::NEG_INFINITY
        );
    }

    #[test]
    fn test_f32_from_text_special() {
        assert!(f32::from_text(oid::NUMERIC, b"NaN").unwrap().is_nan());
        assert_eq!(
            f32::from_text(oid::NUMERIC, b"Infinity").unwrap(),
            f32::INFINITY
        );
        assert_eq!(
            f32::from_text(oid::NUMERIC, b"-Infinity").unwrap(),
            f32::NEG_INFINITY
        );
    }
}
