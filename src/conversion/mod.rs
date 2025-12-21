//! Type encoding and decoding for PostgreSQL wire protocol.
//!
//! This module provides traits and implementations for converting between
//! Rust types and PostgreSQL wire format values.

mod bytes;
mod primitives;
mod row;
mod string;

pub use primitives::numeric_to_string;

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
pub use row::FromRow;

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

/// Trait for encoding Rust values as PostgreSQL parameters.
///
/// Implementations write length-prefixed data directly to the buffer:
/// - Int32 length followed by the value bytes, OR
/// - Int32 -1 for NULL
///
/// The trait provides OID-aware encoding:
/// - `natural_oid()` returns the OID this value naturally encodes to
/// - `encode()` encodes the value for a specific target OID, using the
///   preferred format (text for NUMERIC, binary for everything else)
pub trait ToWireValue {
    /// The OID this value naturally encodes to.
    ///
    /// For example, i64 naturally encodes to INT8 (OID 20).
    fn natural_oid(&self) -> Oid;

    /// Encode this value for the given target OID.
    ///
    /// This allows flexible encoding: an i64 can encode as INT2, INT4, or INT8
    /// depending on what the server expects (with overflow checking).
    ///
    /// The format (text vs binary) is determined by `preferred_format(target_oid)`:
    /// - NUMERIC uses text format (decimal string representation)
    /// - All other types use binary format
    ///
    /// The implementation should write:
    /// - 4-byte length (i32, big-endian)
    /// - followed by the encoded data
    ///
    /// For NULL values, write -1 as the length (no data follows).
    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()>;
}

/// Trait for encoding multiple parameters.
pub trait ToParams {
    /// Number of parameters.
    fn param_count(&self) -> usize;

    /// Get natural OIDs for all parameters (for exec_* with SQL string).
    fn natural_oids(&self) -> Vec<Oid>;

    /// Encode all parameters using specified target OIDs.
    ///
    /// The target_oids slice must have the same length as param_count().
    /// Each parameter is encoded using its preferred format based on the OID.
    fn encode(&self, target_oids: &[Oid], buf: &mut Vec<u8>) -> Result<()>;
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
    fn natural_oid(&self) -> Oid {
        match self {
            Some(v) => v.natural_oid(),
            None => 0, // Unknown/NULL
        }
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match self {
            Some(v) => v.encode(target_oid, buf),
            None => {
                // NULL is represented as -1 length
                buf.extend_from_slice(&(-1_i32).to_be_bytes());
                Ok(())
            }
        }
    }
}

// === Reference support ===

impl<T: ToWireValue + ?Sized> ToWireValue for &T {
    fn natural_oid(&self) -> Oid {
        (*self).natural_oid()
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        (*self).encode(target_oid, buf)
    }
}

// === ToParams implementations ===

impl ToParams for () {
    fn param_count(&self) -> usize {
        0
    }

    fn natural_oids(&self) -> Vec<Oid> {
        vec![]
    }

    fn encode(&self, _target_oids: &[Oid], _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

impl<T: ToParams + ?Sized> ToParams for &T {
    fn param_count(&self) -> usize {
        (*self).param_count()
    }

    fn natural_oids(&self) -> Vec<Oid> {
        (*self).natural_oids()
    }

    fn encode(&self, target_oids: &[Oid], buf: &mut Vec<u8>) -> Result<()> {
        (*self).encode(target_oids, buf)
    }
}

// Tuple implementations via macro
macro_rules! impl_to_params {
    ($count:expr, $($idx:tt: $T:ident),+) => {
        impl<$($T: ToWireValue),+> ToParams for ($($T,)+) {
            fn param_count(&self) -> usize {
                $count
            }

            fn natural_oids(&self) -> Vec<Oid> {
                vec![$(self.$idx.natural_oid()),+]
            }

            fn encode(&self, target_oids: &[Oid], buf: &mut Vec<u8>) -> Result<()> {
                let mut _idx = 0;
                $(
                    self.$idx.encode(target_oids[_idx], buf)?;
                    _idx += 1;
                )+
                Ok(())
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
