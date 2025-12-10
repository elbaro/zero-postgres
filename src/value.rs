//! Value encoding and decoding traits.

use crate::error::{Error, Result};
use crate::protocol::types::{FormatCode, Oid};

/// Trait for decoding PostgreSQL values into Rust types.
///
/// This trait provides methods for decoding values from different formats:
/// - `from_null()` - Handle NULL values
/// - `from_text()` - Decode from text format (default for simple queries)
/// - `from_binary()` - Decode from binary format
/// - `from_unknown()` - Handle custom/extension types with OID
pub trait FromWire<'a>: Sized {
    /// Decode from NULL value.
    ///
    /// Default implementation returns an error. Override for types that can
    /// represent NULL (like `Option<T>`).
    fn from_null() -> Result<Self> {
        Err(Error::Decode("unexpected NULL value".into()))
    }

    /// Decode from text format bytes.
    ///
    /// Text format is the default for simple queries. Values are UTF-8 encoded
    /// string representations.
    fn from_text(_bytes: &'a [u8]) -> Result<Self> {
        Err(Error::Decode(
            "text format not supported for this type".into(),
        ))
    }

    /// Decode from binary format bytes.
    ///
    /// Binary format uses PostgreSQL's internal representation. Integers are
    /// big-endian, floats are IEEE 754, etc.
    fn from_binary(_bytes: &'a [u8]) -> Result<Self> {
        Err(Error::Decode(
            "binary format not supported for this type".into(),
        ))
    }

    /// Decode unknown/custom type with OID.
    ///
    /// Called when the OID doesn't match known PostgreSQL types. This allows
    /// users to handle custom types from PostgreSQL extensions.
    fn from_unknown(_bytes: &'a [u8], oid: Oid, _format: FormatCode) -> Result<Self> {
        Err(Error::Decode(format!("unknown type OID: {}", oid)))
    }
}

// === Option<T> - NULL handling ===

impl<'a, T: FromWire<'a>> FromWire<'a> for Option<T> {
    fn from_null() -> Result<Self> {
        Ok(None)
    }

    fn from_text(bytes: &'a [u8]) -> Result<Self> {
        T::from_text(bytes).map(Some)
    }

    fn from_binary(bytes: &'a [u8]) -> Result<Self> {
        T::from_binary(bytes).map(Some)
    }

    fn from_unknown(bytes: &'a [u8], oid: Oid, format: FormatCode) -> Result<Self> {
        T::from_unknown(bytes, oid, format).map(Some)
    }
}

// === Boolean ===

impl FromWire<'_> for bool {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        match bytes {
            b"t" | b"true" | b"TRUE" | b"T" | b"1" => Ok(true),
            b"f" | b"false" | b"FALSE" | b"F" | b"0" => Ok(false),
            _ => Err(Error::Decode(format!(
                "invalid boolean: {:?}",
                String::from_utf8_lossy(bytes)
            ))),
        }
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 1 {
            return Err(Error::Decode(format!(
                "invalid boolean length: {}",
                bytes.len()
            )));
        }
        Ok(bytes[0] != 0)
    }
}

// === Integer types ===

impl FromWire<'_> for i16 {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid i16: {}", e)))
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 2] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid i16 length: {}", bytes.len())))?;
        Ok(i16::from_be_bytes(arr))
    }
}

impl FromWire<'_> for i32 {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid i32: {}", e)))
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid i32 length: {}", bytes.len())))?;
        Ok(i32::from_be_bytes(arr))
    }
}

impl FromWire<'_> for i64 {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid i64: {}", e)))
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid i64 length: {}", bytes.len())))?;
        Ok(i64::from_be_bytes(arr))
    }
}

// === Floating point types ===

impl FromWire<'_> for f32 {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid f32: {}", e)))
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid f32 length: {}", bytes.len())))?;
        Ok(f32::from_be_bytes(arr))
    }
}

impl FromWire<'_> for f64 {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        s.parse()
            .map_err(|e| Error::Decode(format!("invalid f64: {}", e)))
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid f64 length: {}", bytes.len())))?;
        Ok(f64::from_be_bytes(arr))
    }
}

// === String types ===

impl<'a> FromWire<'a> for &'a str {
    fn from_text(bytes: &'a [u8]) -> Result<Self> {
        std::str::from_utf8(bytes).map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }

    fn from_binary(bytes: &'a [u8]) -> Result<Self> {
        std::str::from_utf8(bytes).map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }
}

impl FromWire<'_> for String {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))
    }
}

// === Byte types ===

impl<'a> FromWire<'a> for &'a [u8] {
    fn from_text(bytes: &'a [u8]) -> Result<Self> {
        // Text format for bytea is hex-encoded: \x followed by hex digits
        // For simplicity, we just return the raw bytes (caller can decode if needed)
        Ok(bytes)
    }

    fn from_binary(bytes: &'a [u8]) -> Result<Self> {
        Ok(bytes)
    }
}

impl FromWire<'_> for Vec<u8> {
    fn from_text(bytes: &[u8]) -> Result<Self> {
        // Text format for bytea is hex-encoded: \xDEADBEEF
        if bytes.starts_with(b"\\x") {
            decode_hex(&bytes[2..])
        } else {
            // Fallback: return raw bytes
            Ok(bytes.to_vec())
        }
    }

    fn from_binary(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
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

// ============================================================================
// ENCODING (ToValue / ToParams)
// ============================================================================

/// Trait for encoding Rust values as PostgreSQL binary parameters.
///
/// Implementations write length-prefixed binary data directly to the buffer:
/// - Int32 length followed by the value bytes, OR
/// - Int32 -1 for NULL
pub trait ToWire {
    /// Encode as a length-prefixed binary parameter.
    fn to_binary(&self, buf: &mut Vec<u8>);
}

// === Option<T> - NULL handling ===

impl<T: ToWire> ToWire for Option<T> {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        match self {
            Some(v) => v.to_binary(buf),
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
}

// === Boolean ===

impl ToWire for bool {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&1_i32.to_be_bytes());
        buf.push(if *self { 1 } else { 0 });
    }
}

// === Integer types ===

impl ToWire for i16 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&2_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl ToWire for i32 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&4_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl ToWire for i64 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&8_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

// === Floating point types ===

impl ToWire for f32 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&4_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

impl ToWire for f64 {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&8_i32.to_be_bytes());
        buf.extend_from_slice(&self.to_be_bytes());
    }
}

// === String types ===

impl ToWire for &str {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.len() as i32).to_be_bytes());
        buf.extend_from_slice(self.as_bytes());
    }
}

impl ToWire for String {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        self.as_str().to_binary(buf);
    }
}

// === Byte types ===

impl ToWire for &[u8] {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.len() as i32).to_be_bytes());
        buf.extend_from_slice(self);
    }
}

impl ToWire for Vec<u8> {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().to_binary(buf);
    }
}

// === Reference support ===

impl<T: ToWire + ?Sized> ToWire for &T {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        (*self).to_binary(buf);
    }
}

// ============================================================================
// ToParams - Tuple encoding
// ============================================================================

/// Trait for encoding multiple parameters.
pub trait ToParams {
    /// Number of parameters.
    fn param_count(&self) -> usize;

    /// Encode all parameters to the buffer.
    fn to_binary(&self, buf: &mut Vec<u8>);
}

// Empty params
impl ToParams for () {
    fn param_count(&self) -> usize {
        0
    }

    fn to_binary(&self, _buf: &mut Vec<u8>) {}
}

// Reference support
impl<T: ToParams + ?Sized> ToParams for &T {
    fn param_count(&self) -> usize {
        (*self).param_count()
    }

    fn to_binary(&self, buf: &mut Vec<u8>) {
        (*self).to_binary(buf);
    }
}

// Tuple implementations via macro
macro_rules! impl_to_params {
    ($count:expr, $($idx:tt: $T:ident),+) => {
        impl<$($T: ToWire),+> ToParams for ($($T,)+) {
            fn param_count(&self) -> usize {
                $count
            }

            fn to_binary(&self, buf: &mut Vec<u8>) {
                $(self.$idx.to_binary(buf);)+
            }
        }
    };
}

impl_to_params!(1, 0: T0);
impl_to_params!(2, 0: T0, 1: T1);
impl_to_params!(3, 0: T0, 1: T1, 2: T2);
impl_to_params!(4, 0: T0, 1: T1, 2: T2, 3: T3);
impl_to_params!(5, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4);
impl_to_params!(6, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5);
impl_to_params!(7, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6);
impl_to_params!(8, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7);
impl_to_params!(9, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8);
impl_to_params!(10, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9);
impl_to_params!(11, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10);
impl_to_params!(12, 0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10, 11: T11);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_text() {
        assert!(bool::from_text(b"t").unwrap());
        assert!(bool::from_text(b"true").unwrap());
        assert!(!bool::from_text(b"f").unwrap());
        assert!(!bool::from_text(b"false").unwrap());
    }

    #[test]
    fn test_bool_binary() {
        assert!(bool::from_binary(&[1]).unwrap());
        assert!(!bool::from_binary(&[0]).unwrap());
    }

    #[test]
    fn test_i32_text() {
        assert_eq!(i32::from_text(b"12345").unwrap(), 12345);
        assert_eq!(i32::from_text(b"-12345").unwrap(), -12345);
    }

    #[test]
    fn test_i32_binary() {
        assert_eq!(i32::from_binary(&[0, 0, 0x30, 0x39]).unwrap(), 12345);
    }

    #[test]
    fn test_f64_text() {
        assert_eq!(f64::from_text(b"3.14").unwrap(), 3.14);
    }

    #[test]
    fn test_string_text() {
        assert_eq!(String::from_text(b"hello").unwrap(), "hello");
    }

    #[test]
    fn test_option_null() {
        assert_eq!(Option::<i32>::from_null().unwrap(), None);
    }

    #[test]
    fn test_option_some() {
        assert_eq!(Option::<i32>::from_text(b"42").unwrap(), Some(42));
    }

    #[test]
    fn test_bytea_hex() {
        assert_eq!(
            Vec::<u8>::from_text(b"\\xDEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }
}
