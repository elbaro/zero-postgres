//! Row decoding traits and implementations.

use crate::error::{Error, Result};
use crate::protocol::backend::query::{DataRow, FieldDescription};
use crate::protocol::types::FormatCode;
use crate::value::FromValue;

/// Trait for decoding a PostgreSQL row into a Rust type.
pub trait FromRow<'a>: Sized {
    /// Decode a row using column metadata.
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self>;
}

/// Decode a single column value.
fn decode_column<'a, T: FromValue<'a>>(
    field: &FieldDescription,
    value: Option<&'a [u8]>,
) -> Result<T> {
    match value {
        None => T::from_null(),
        Some(bytes) => match field.format() {
            FormatCode::Text => T::from_text(bytes),
            FormatCode::Binary => T::from_binary(bytes),
        },
    }
}

// === Tuple implementations ===

impl<'a, T1: FromValue<'a>> FromRow<'a> for (T1,) {
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 1 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        Ok((v1,))
    }
}

impl<'a, T1: FromValue<'a>, T2: FromValue<'a>> FromRow<'a> for (T1, T2) {
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 2 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        Ok((v1, v2))
    }
}

impl<'a, T1: FromValue<'a>, T2: FromValue<'a>, T3: FromValue<'a>> FromRow<'a> for (T1, T2, T3) {
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 3 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        Ok((v1, v2, v3))
    }
}

impl<'a, T1: FromValue<'a>, T2: FromValue<'a>, T3: FromValue<'a>, T4: FromValue<'a>> FromRow<'a>
    for (T1, T2, T3, T4)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 4 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        Ok((v1, v2, v3, v4))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 5 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 6 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
        T7: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6, T7)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 7 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        let v7 = decode_column(&cols[6], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6, v7))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
        T7: FromValue<'a>,
        T8: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6, T7, T8)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 8 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        let v7 = decode_column(&cols[6], iter.next().flatten())?;
        let v8 = decode_column(&cols[7], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6, v7, v8))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
        T7: FromValue<'a>,
        T8: FromValue<'a>,
        T9: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 9 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        let v7 = decode_column(&cols[6], iter.next().flatten())?;
        let v8 = decode_column(&cols[7], iter.next().flatten())?;
        let v9 = decode_column(&cols[8], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6, v7, v8, v9))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
        T7: FromValue<'a>,
        T8: FromValue<'a>,
        T9: FromValue<'a>,
        T10: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 10 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        let v7 = decode_column(&cols[6], iter.next().flatten())?;
        let v8 = decode_column(&cols[7], iter.next().flatten())?;
        let v9 = decode_column(&cols[8], iter.next().flatten())?;
        let v10 = decode_column(&cols[9], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
        T7: FromValue<'a>,
        T8: FromValue<'a>,
        T9: FromValue<'a>,
        T10: FromValue<'a>,
        T11: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 11 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        let v7 = decode_column(&cols[6], iter.next().flatten())?;
        let v8 = decode_column(&cols[7], iter.next().flatten())?;
        let v9 = decode_column(&cols[8], iter.next().flatten())?;
        let v10 = decode_column(&cols[9], iter.next().flatten())?;
        let v11 = decode_column(&cols[10], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
    }
}

impl<
        'a,
        T1: FromValue<'a>,
        T2: FromValue<'a>,
        T3: FromValue<'a>,
        T4: FromValue<'a>,
        T5: FromValue<'a>,
        T6: FromValue<'a>,
        T7: FromValue<'a>,
        T8: FromValue<'a>,
        T9: FromValue<'a>,
        T10: FromValue<'a>,
        T11: FromValue<'a>,
        T12: FromValue<'a>,
    > FromRow<'a> for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
{
    fn from_row(cols: &[FieldDescription], row: DataRow<'a>) -> Result<Self> {
        if cols.len() < 12 {
            return Err(Error::Decode("not enough columns for tuple".into()));
        }
        let mut iter = row.iter();
        let v1 = decode_column(&cols[0], iter.next().flatten())?;
        let v2 = decode_column(&cols[1], iter.next().flatten())?;
        let v3 = decode_column(&cols[2], iter.next().flatten())?;
        let v4 = decode_column(&cols[3], iter.next().flatten())?;
        let v5 = decode_column(&cols[4], iter.next().flatten())?;
        let v6 = decode_column(&cols[5], iter.next().flatten())?;
        let v7 = decode_column(&cols[6], iter.next().flatten())?;
        let v8 = decode_column(&cols[7], iter.next().flatten())?;
        let v9 = decode_column(&cols[8], iter.next().flatten())?;
        let v10 = decode_column(&cols[9], iter.next().flatten())?;
        let v11 = decode_column(&cols[10], iter.next().flatten())?;
        let v12 = decode_column(&cols[11], iter.next().flatten())?;
        Ok((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12))
    }
}
