//! Decimal type implementation (rust_decimal crate).
//!
//! PostgreSQL NUMERIC uses text format for encoding parameters because:
//! 1. Binary format is complex (base-10000 encoding)
//! 2. Text format is equally efficient (server parses quickly)
//! 3. Preserves full precision through string representation
//!
//! Binary decoding is still supported for receiving results.

use rust_decimal::Decimal;

use crate::error::{Error, Result};
use crate::protocol::types::{oid, Oid};

use super::{FromWireValue, ToWireValue};

const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NBASE: i128 = 10000;

impl FromWireValue<'_> for Decimal {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::NUMERIC {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as Decimal",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;

        // Handle special values
        if s == "NaN" {
            return Err(Error::Decode("NaN cannot be represented as Decimal".into()));
        }

        Decimal::from_str_exact(s).map_err(|e| Error::Decode(format!("invalid decimal: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::NUMERIC {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as Decimal",
                oid
            )));
        }

        if bytes.len() < 8 {
            return Err(Error::Decode(format!(
                "invalid NUMERIC length: {}",
                bytes.len()
            )));
        }

        let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
        let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
        let dscale = u16::from_be_bytes([bytes[6], bytes[7]]);

        if sign == NUMERIC_NAN {
            return Err(Error::Decode("NaN cannot be represented as Decimal".into()));
        }

        if ndigits == 0 {
            return Ok(Decimal::ZERO);
        }

        let expected_len = 8 + ndigits * 2;
        if bytes.len() < expected_len {
            return Err(Error::Decode(format!(
                "invalid NUMERIC length: {} (expected {})",
                bytes.len(),
                expected_len
            )));
        }

        // Read base-10000 digits
        let mut digits = Vec::with_capacity(ndigits);
        for i in 0..ndigits {
            let offset = 8 + i * 2;
            let digit = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
            digits.push(digit);
        }

        // Convert from base-10000 to decimal
        // value = sum(digits[i] * 10000^(weight - i)) for i in 0..ndigits
        let mut value: i128 = 0;
        for &digit in &digits {
            value = value * NBASE + (digit as i128);
        }

        // Calculate the actual scale
        // weight is the power of 10000 for the first digit
        // Each digit represents 4 decimal places
        // The total value without scaling is: value * 10000^(weight - ndigits + 1)
        let exponent = (weight as i32 - ndigits as i32 + 1) * 4;

        // Apply sign
        if sign == NUMERIC_NEG {
            value = -value;
        }

        // Create decimal with proper scale
        // If exponent is negative, we need to divide (increase scale)
        // If exponent is positive, we need to multiply (decrease scale)
        let mut decimal = Decimal::from_i128_with_scale(value, 0);

        if exponent > 0 {
            // Multiply by 10^exponent
            for _ in 0..exponent {
                decimal = decimal
                    .checked_mul(Decimal::TEN)
                    .ok_or_else(|| Error::Decode("decimal overflow".into()))?;
            }
        } else if exponent < 0 {
            // Divide by 10^(-exponent), which is same as setting scale
            decimal
                .set_scale((-exponent) as u32)
                .map_err(|e| Error::Decode(format!("decimal scale error: {}", e)))?;
        }

        // Normalize to the display scale from PostgreSQL
        if dscale > 0 {
            decimal = decimal.round_dp(dscale as u32);
        }

        Ok(decimal)
    }
}

impl ToWireValue for Decimal {
    fn natural_oid(&self) -> Oid {
        oid::NUMERIC
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::NUMERIC => {
                // Use text format for NUMERIC - simple and preserves full precision
                use std::fmt::Write;
                let mut text = String::new();
                write!(&mut text, "{}", self).expect("Decimal formatting cannot fail");
                let bytes = text.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_decimal_text_decode() {
        let dec = Decimal::from_text(oid::NUMERIC, b"123.45").unwrap();
        assert_eq!(dec, Decimal::from_str("123.45").unwrap());
    }

    #[test]
    fn test_decimal_text_negative() {
        let dec = Decimal::from_text(oid::NUMERIC, b"-999.999").unwrap();
        assert_eq!(dec, Decimal::from_str("-999.999").unwrap());
    }

    #[test]
    fn test_decimal_zero() {
        let dec = Decimal::from_text(oid::NUMERIC, b"0").unwrap();
        assert_eq!(dec, Decimal::ZERO);
    }

    #[test]
    fn test_decimal_encode_text_format() {
        // encode() now produces text format for NUMERIC
        let original = Decimal::from_str("12345.6789").unwrap();
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        // Skip 4-byte length prefix
        let text = std::str::from_utf8(&buf[4..]).unwrap();
        assert_eq!(text, "12345.6789");
    }

    #[test]
    fn test_decimal_encode_zero() {
        let original = Decimal::ZERO;
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        let text = std::str::from_utf8(&buf[4..]).unwrap();
        assert_eq!(text, "0");
    }

    #[test]
    fn test_decimal_encode_negative() {
        let original = Decimal::from_str("-123.456").unwrap();
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        let text = std::str::from_utf8(&buf[4..]).unwrap();
        assert_eq!(text, "-123.456");
    }

    #[test]
    fn test_decimal_nan_text() {
        let result = Decimal::from_text(oid::NUMERIC, b"NaN");
        assert!(result.is_err());
    }
}
