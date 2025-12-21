//! Utility functions for decoding PostgreSQL NUMERIC binary format.

use crate::error::{Error, Result};

// NUMERIC sign constants
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

/// Converts PostgreSQL NUMERIC binary encoding to String.
///
/// Based on PostgreSQL's `get_str_from_var()` from `numeric.c`:
/// <https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c>
///
/// Binary format:
/// - 2 bytes: ndigits (number of base-10000 digits)
/// - 2 bytes: weight (position of first digit relative to decimal point)
/// - 2 bytes: sign (0x0000=positive, 0x4000=negative, 0xC000=NaN, 0xD000=+Inf, 0xF000=-Inf)
/// - 2 bytes: dscale (display scale)
/// - ndigits * 2 bytes: digits (each 0-9999 in base 10000)
pub fn numeric_to_string(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 8 {
        return Err(Error::Decode(format!(
            "invalid NUMERIC length: {}",
            bytes.len()
        )));
    }

    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]) as i32;
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]) as i32;

    // Handle special values
    match sign {
        NUMERIC_NAN => return Ok("NaN".to_string()),
        NUMERIC_PINF => return Ok("Infinity".to_string()),
        NUMERIC_NINF => return Ok("-Infinity".to_string()),
        _ => {}
    }

    // Zero case
    if ndigits == 0 {
        return if dscale > 0 {
            let mut s = "0.".to_string();
            for _ in 0..dscale {
                s.push('0');
            }
            Ok(s)
        } else {
            Ok("0".to_string())
        };
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
        let digit = i16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        digits.push(digit);
    }

    // Build the string representation
    let mut result = String::new();

    // Add sign
    if sign == NUMERIC_NEG {
        result.push('-');
    }

    // Number of decimal digits before the decimal point
    // Each base-10000 digit represents 4 decimal digits
    let int_digits = (weight + 1) * 4;

    if int_digits <= 0 {
        // All digits are after decimal point
        result.push_str("0.");
        // Add leading zeros after decimal point
        for _ in 0..(-int_digits) {
            result.push('0');
        }
        // Add all digit groups (each group is 4 decimal digits)
        let mut frac_digits_written = (-int_digits) as i32;
        for (i, &d) in digits.iter().enumerate() {
            let s = format!("{:04}", d);
            if i == ndigits - 1 && dscale > 0 {
                // Last group: only output up to dscale
                for c in s.chars() {
                    if frac_digits_written < dscale {
                        result.push(c);
                        frac_digits_written += 1;
                    }
                }
            } else {
                result.push_str(&s);
                frac_digits_written += 4;
            }
        }
        // Pad with trailing zeros if needed
        while frac_digits_written < dscale {
            result.push('0');
            frac_digits_written += 1;
        }
    } else {
        // Some digits before decimal point
        let mut d_idx = 0;

        // First digit group (may have fewer than 4 digits displayed)
        if d_idx < ndigits {
            let d = digits[d_idx];
            result.push_str(&d.to_string());
            d_idx += 1;
        }

        // Remaining integer part digits
        let full_int_groups = weight as usize;
        while d_idx <= full_int_groups && d_idx < ndigits {
            result.push_str(&format!("{:04}", digits[d_idx]));
            d_idx += 1;
        }

        // Pad with zeros if we have fewer digits than weight suggests
        while d_idx <= full_int_groups {
            result.push_str("0000");
            d_idx += 1;
        }

        // Decimal point and fractional part
        if dscale > 0 {
            result.push('.');

            let mut frac_digits_written = 0;
            while d_idx < ndigits && frac_digits_written < dscale {
                let s = format!("{:04}", digits[d_idx]);
                for c in s.chars() {
                    if frac_digits_written < dscale {
                        result.push(c);
                        frac_digits_written += 1;
                    }
                }
                d_idx += 1;
            }

            // Pad with trailing zeros if needed
            while frac_digits_written < dscale {
                result.push('0');
                frac_digits_written += 1;
            }
        }
    }

    Ok(result)
}

/// Decode PostgreSQL NUMERIC binary format to f64.
pub fn numeric_to_f64(bytes: &[u8]) -> Result<f64> {
    if bytes.len() < 8 {
        return Err(Error::Decode(format!(
            "NUMERIC too short: {} bytes",
            bytes.len()
        )));
    }

    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);

    // Handle special values
    match sign {
        NUMERIC_NAN => return Ok(f64::NAN),
        NUMERIC_PINF => return Ok(f64::INFINITY),
        NUMERIC_NINF => return Ok(f64::NEG_INFINITY),
        _ => {}
    }

    // Check expected length
    let expected_len = 8 + ndigits * 2;
    if bytes.len() < expected_len {
        return Err(Error::Decode(format!(
            "NUMERIC length mismatch: expected {}, got {}",
            expected_len,
            bytes.len()
        )));
    }

    // Zero case
    if ndigits == 0 {
        return Ok(0.0);
    }

    // Accumulate the value
    // Each digit is in base 10000, weight indicates power of 10000
    let mut result: f64 = 0.0;
    let mut digit_idx = 8;
    for i in 0..ndigits {
        let digit = i16::from_be_bytes([bytes[digit_idx], bytes[digit_idx + 1]]) as f64;
        digit_idx += 2;
        // Position of this digit: weight - i (in powers of 10000)
        let power = (weight as i32) - (i as i32);
        result += digit * 10000_f64.powi(power);
    }

    // Apply sign
    if sign == NUMERIC_NEG {
        result = -result;
    }

    // Check for overflow (result became infinity from finite NUMERIC)
    if result.is_infinite() && sign != NUMERIC_PINF && sign != NUMERIC_NINF {
        return Err(Error::Decode("NUMERIC value overflows f64".to_string()));
    }

    Ok(result)
}

/// Decode PostgreSQL NUMERIC binary format to f32.
pub fn numeric_to_f32(bytes: &[u8]) -> Result<f32> {
    if bytes.len() < 8 {
        return Err(Error::Decode(format!(
            "NUMERIC too short: {} bytes",
            bytes.len()
        )));
    }

    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);

    // Handle special values
    match sign {
        NUMERIC_NAN => return Ok(f32::NAN),
        NUMERIC_PINF => return Ok(f32::INFINITY),
        NUMERIC_NINF => return Ok(f32::NEG_INFINITY),
        _ => {}
    }

    // Check expected length
    let expected_len = 8 + ndigits * 2;
    if bytes.len() < expected_len {
        return Err(Error::Decode(format!(
            "NUMERIC length mismatch: expected {}, got {}",
            expected_len,
            bytes.len()
        )));
    }

    // Zero case
    if ndigits == 0 {
        return Ok(0.0);
    }

    // Accumulate the value using f64 for precision, then convert
    let mut result: f64 = 0.0;
    let mut digit_idx = 8;
    for i in 0..ndigits {
        let digit = i16::from_be_bytes([bytes[digit_idx], bytes[digit_idx + 1]]) as f64;
        digit_idx += 2;
        let power = (weight as i32) - (i as i32);
        result += digit * 10000_f64.powi(power);
    }

    // Apply sign
    if sign == NUMERIC_NEG {
        result = -result;
    }

    // Check for overflow before converting to f32
    if result > f32::MAX as f64 || result < f32::MIN as f64 {
        return Err(Error::Decode("NUMERIC value overflows f32".to_string()));
    }

    let result_f32 = result as f32;

    // Additional check: finite f64 becoming infinite f32
    if result_f32.is_infinite() && result.is_finite() {
        return Err(Error::Decode("NUMERIC value overflows f32".to_string()));
    }

    Ok(result_f32)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_numeric_to_string_zero() {
        let bytes = make_numeric(0, 0, 0x0000, 0, &[]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "0");

        // Zero with scale
        let bytes = make_numeric(0, 0, 0x0000, 2, &[]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "0.00");
    }

    #[test]
    fn test_numeric_to_string_simple() {
        // 12345 = 1 * 10000 + 2345, weight=1
        let bytes = make_numeric(2, 1, 0x0000, 0, &[1, 2345]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "12345");
    }

    #[test]
    fn test_numeric_to_string_decimal() {
        // 123.45: weight=0, dscale=2, digits=[123, 4500]
        let bytes = make_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "123.45");
    }

    #[test]
    fn test_numeric_to_string_negative() {
        // -123.45
        let bytes = make_numeric(2, 0, 0x4000, 2, &[123, 4500]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "-123.45");
    }

    #[test]
    fn test_numeric_to_string_small_decimal() {
        // 0.0001: weight=-1, digits=[1]
        let bytes = make_numeric(1, -1, 0x0000, 4, &[1]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "0.0001");
    }

    #[test]
    fn test_numeric_to_string_special_values() {
        // NaN
        let bytes = make_numeric(0, 0, 0xC000, 0, &[]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "NaN");

        // +Infinity
        let bytes = make_numeric(0, 0, 0xD000, 0, &[]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "Infinity");

        // -Infinity
        let bytes = make_numeric(0, 0, 0xF000, 0, &[]);
        assert_eq!(numeric_to_string(&bytes).unwrap(), "-Infinity");
    }

    #[test]
    fn test_numeric_to_f64() {
        // 123.45
        let bytes = make_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        let result = numeric_to_f64(&bytes).unwrap();
        assert!((result - 123.45).abs() < 0.001);
    }

    #[test]
    fn test_numeric_to_f64_negative() {
        // -123.45
        let bytes = make_numeric(2, 0, 0x4000, 2, &[123, 4500]);
        let result = numeric_to_f64(&bytes).unwrap();
        assert!((result + 123.45).abs() < 0.001);
    }

    #[test]
    fn test_numeric_to_f64_special() {
        // NaN
        let bytes = make_numeric(0, 0, 0xC000, 0, &[]);
        assert!(numeric_to_f64(&bytes).unwrap().is_nan());

        // +Infinity
        let bytes = make_numeric(0, 0, 0xD000, 0, &[]);
        assert_eq!(numeric_to_f64(&bytes).unwrap(), f64::INFINITY);

        // -Infinity
        let bytes = make_numeric(0, 0, 0xF000, 0, &[]);
        assert_eq!(numeric_to_f64(&bytes).unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn test_numeric_to_f32() {
        // 123.45
        let bytes = make_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        let result = numeric_to_f32(&bytes).unwrap();
        assert!((result - 123.45).abs() < 0.01);
    }

    #[test]
    fn test_numeric_to_f32_special() {
        // NaN
        let bytes = make_numeric(0, 0, 0xC000, 0, &[]);
        assert!(numeric_to_f32(&bytes).unwrap().is_nan());

        // +Infinity
        let bytes = make_numeric(0, 0, 0xD000, 0, &[]);
        assert_eq!(numeric_to_f32(&bytes).unwrap(), f32::INFINITY);

        // -Infinity
        let bytes = make_numeric(0, 0, 0xF000, 0, &[]);
        assert_eq!(numeric_to_f32(&bytes).unwrap(), f32::NEG_INFINITY);
    }
}
