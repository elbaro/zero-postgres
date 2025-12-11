//! Type encoding and decoding for PostgreSQL wire protocol.
//!
//! This module provides traits and implementations for converting between
//! Rust types and PostgreSQL wire format values.

mod bytes;
mod primitives;
mod string;

#[cfg(feature = "with-chrono")]
mod chrono;
#[cfg(feature = "with-rust-decimal")]
mod decimal;
#[cfg(feature = "with-time")]
mod time;
#[cfg(feature = "with-uuid")]
mod uuid;

use crate::error::{Error, Result};
use crate::protocol::types::Oid;

/// Trait for decoding PostgreSQL values into Rust types.
///
/// This trait provides methods for decoding values from different formats:
/// - `from_null()` - Handle NULL values
/// - `from_text()` - Decode from text format (simple queries)
/// - `from_binary()` - Decode from binary format (extended queries)
///
/// The OID parameter allows implementations to check the PostgreSQL type
/// and reject incompatible types with clear error messages.
pub trait FromWireValue<'a>: Sized {
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
    fn from_text(oid: Oid, bytes: &'a [u8]) -> Result<Self>;

    /// Decode from binary format bytes.
    ///
    /// Binary format uses PostgreSQL's internal representation. Integers are
    /// big-endian, floats are IEEE 754, etc.
    fn from_binary(oid: Oid, bytes: &'a [u8]) -> Result<Self>;
}

/// Trait for encoding Rust values as PostgreSQL binary parameters.
///
/// Implementations write length-prefixed binary data directly to the buffer:
/// - Int32 length followed by the value bytes, OR
/// - Int32 -1 for NULL
pub trait ToWireValue {
    /// Encode as a length-prefixed binary parameter.
    fn to_binary(&self, buf: &mut Vec<u8>);
}

/// Trait for encoding multiple parameters.
pub trait ToParams {
    /// Number of parameters.
    fn param_count(&self) -> usize;

    /// Encode all parameters to the buffer.
    fn to_binary(&self, buf: &mut Vec<u8>);
}

// === Option<T> - NULL handling ===

impl<'a, T: FromWireValue<'a>> FromWireValue<'a> for Option<T> {
    fn from_null() -> Result<Self> {
        Ok(None)
    }

    fn from_text(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        T::from_text(oid, bytes).map(Some)
    }

    fn from_binary(oid: Oid, bytes: &'a [u8]) -> Result<Self> {
        T::from_binary(oid, bytes).map(Some)
    }
}

impl<T: ToWireValue> ToWireValue for Option<T> {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        match self {
            Some(v) => v.to_binary(buf),
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
}

// === Reference support ===

impl<T: ToWireValue + ?Sized> ToWireValue for &T {
    fn to_binary(&self, buf: &mut Vec<u8>) {
        (*self).to_binary(buf);
    }
}

// === ToParams implementations ===

impl ToParams for () {
    fn param_count(&self) -> usize {
        0
    }

    fn to_binary(&self, _buf: &mut Vec<u8>) {}
}

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
        impl<$($T: ToWireValue),+> ToParams for ($($T,)+) {
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
    fn test_option_null() {
        assert_eq!(Option::<i32>::from_null().unwrap(), None);
    }
}
