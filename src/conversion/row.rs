//! Row decoding traits and implementations.

use crate::conversion::FromWireValue;
use crate::error::{Error, Result};
use crate::protocol::backend::query::{DataRow, FieldDescription};

/// Trait for decoding a PostgreSQL row into a Rust type.
pub trait FromRow<'a>: Sized {
    /// Decode a row from text format (simple protocol).
    fn from_row_text(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self>;

    /// Decode a row from binary format (extended protocol).
    fn from_row_binary(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self>;
}

/// Decode a single column value as text.
fn decode_column_text<'a, T: FromWireValue<'a>>(
    field: &FieldDescription,
    value: Option<&'a [u8]>,
) -> Result<T> {
    match value {
        None => T::from_null(),
        Some(bytes) => T::from_text(field.type_oid(), bytes),
    }
}

/// Decode a single column value as binary.
fn decode_column_binary<'a, T: FromWireValue<'a>>(
    field: &FieldDescription,
    value: Option<&'a [u8]>,
) -> Result<T> {
    match value {
        None => T::from_null(),
        Some(bytes) => T::from_binary(field.type_oid(), bytes),
    }
}

// === Tuple implementations ===

/// Implementation for empty tuple - used for statements that don't return rows
impl FromRow<'_> for () {
    fn from_row_text(_cols: &[FieldDescription], _row: DataRow<'_>) -> Result<Self> {
        Ok(())
    }

    fn from_row_binary(_cols: &[FieldDescription], _row: DataRow<'_>) -> Result<Self> {
        Ok(())
    }
}

macro_rules! impl_from_row_tuple {
    ($count:literal: $($idx:tt => $T:ident),+) => {
        impl<'a, $($T: FromWireValue<'a>),+> FromRow<'a> for ($($T,)+) {
            fn from_row_text(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
                if cols.len() < $count {
                    return Err(Error::Decode("not enough columns for tuple".into()));
                }
                let mut iter = row.iter();
                Ok(($(
                    decode_column_text(&cols[$idx], iter.next().flatten())?,
                )+))
            }

            fn from_row_binary(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
                if cols.len() < $count {
                    return Err(Error::Decode("not enough columns for tuple".into()));
                }
                let mut iter = row.iter();
                Ok(($(
                    decode_column_binary(&cols[$idx], iter.next().flatten())?,
                )+))
            }
        }
    };
}

impl_from_row_tuple!(1: 0 => T1);
impl_from_row_tuple!(2: 0 => T1, 1 => T2);
impl_from_row_tuple!(3: 0 => T1, 1 => T2, 2 => T3);
impl_from_row_tuple!(4: 0 => T1, 1 => T2, 2 => T3, 3 => T4);
impl_from_row_tuple!(5: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5);
impl_from_row_tuple!(6: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6);
impl_from_row_tuple!(7: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6, 6 => T7);
impl_from_row_tuple!(8: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6, 6 => T7, 7 => T8);
impl_from_row_tuple!(9: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6, 6 => T7, 7 => T8, 8 => T9);
impl_from_row_tuple!(10: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6, 6 => T7, 7 => T8, 8 => T9, 9 => T10);
impl_from_row_tuple!(11: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6, 6 => T7, 7 => T8, 8 => T9, 9 => T10, 10 => T11);
impl_from_row_tuple!(12: 0 => T1, 1 => T2, 2 => T3, 3 => T4, 4 => T5, 5 => T6, 6 => T7, 7 => T8, 8 => T9, 9 => T10, 10 => T11, 11 => T12);
