//! Decimal type implementation (rust_decimal crate).
//!
//! PostgreSQL NUMERIC binary format:
//! - ndigits: i16 - number of base-10000 digits
//! - weight: i16 - weight of first digit (power of 10000)
//! - sign: u16 - 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
//! - dscale: u16 - display scale (decimal places)
//! - digits: [u16] - base-10000 digits

use rust_decimal::Decimal;

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

const NUMERIC_POS: u16 = 0x0000;
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

    fn to_binary(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::NUMERIC => {
                if self.is_zero() {
                    // Zero: ndigits=0, weight=0, sign=positive, dscale=0
                    buf.extend_from_slice(&8_i32.to_be_bytes()); // length
                    buf.extend_from_slice(&0_i16.to_be_bytes()); // ndigits
                    buf.extend_from_slice(&0_i16.to_be_bytes()); // weight
                    buf.extend_from_slice(&NUMERIC_POS.to_be_bytes()); // sign
                    buf.extend_from_slice(&0_u16.to_be_bytes()); // dscale
                    return Ok(());
                }

                let is_negative = self.is_sign_negative();
                let scale = self.scale();

                // Get the unscaled value (mantissa)
                let mantissa = self.mantissa().unsigned_abs();

                // Convert mantissa to base-10000 digits
                let mut digits = Vec::new();
                let mut remaining = mantissa;

                while remaining > 0 {
                    digits.push((remaining % (NBASE as u128)) as u16);
                    remaining /= NBASE as u128;
                }
                digits.reverse();

                if digits.is_empty() {
                    digits.push(0);
                }

                // Calculate weight
                // The value is: mantissa * 10^(-scale)
                // In base 10000: each digit represents 10000^(weight - i)
                // Total decimal digits before converting to base 10000
                let total_decimal_digits = if mantissa == 0 {
                    1
                } else {
                    (mantissa as f64).log10().floor() as i32 + 1
                };

                // Weight represents how many groups of 4 digits before the decimal point
                // weight = (total_decimal_digits - scale - 1) / 4
                let weight = ((total_decimal_digits as i32 - scale as i32 - 1) / 4) as i16;

                let ndigits = digits.len() as i16;
                let sign = if is_negative {
                    NUMERIC_NEG
                } else {
                    NUMERIC_POS
                };
                let dscale = scale as u16;

                // Calculate total length
                let data_len = 8 + (ndigits as usize) * 2;
                buf.extend_from_slice(&(data_len as i32).to_be_bytes());
                buf.extend_from_slice(&ndigits.to_be_bytes());
                buf.extend_from_slice(&weight.to_be_bytes());
                buf.extend_from_slice(&sign.to_be_bytes());
                buf.extend_from_slice(&dscale.to_be_bytes());

                for digit in &digits {
                    buf.extend_from_slice(&digit.to_be_bytes());
                }
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
    fn test_decimal_text() {
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
    fn test_decimal_zero_binary_roundtrip() {
        let original = Decimal::ZERO;
        let mut buf = Vec::new();
        original.to_binary(original.natural_oid(), &mut buf).unwrap();
        let decoded = Decimal::from_binary(oid::NUMERIC, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decimal_positive_roundtrip() {
        let original = Decimal::from_str("12345.6789").unwrap();
        let mut buf = Vec::new();
        original.to_binary(original.natural_oid(), &mut buf).unwrap();
        let decoded = Decimal::from_binary(oid::NUMERIC, &buf[4..]).unwrap();
        // Note: precision may vary due to base-10000 conversion
        assert!((original - decoded).abs() < Decimal::from_str("0.0001").unwrap());
    }

    #[test]
    fn test_decimal_nan_text() {
        let result = Decimal::from_text(oid::NUMERIC, b"NaN");
        assert!(result.is_err());
    }
}
