//! Time crate type implementations (Date, Time, PrimitiveDateTime, OffsetDateTime).

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

/// PostgreSQL epoch is 2000-01-01, whose Julian day is 2451545
const PG_EPOCH_JULIAN_DAY: i32 = 2_451_545;

impl FromWireValue<'_> for time::Date {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::DATE {
            return Err(Error::Decode(format!("cannot decode oid {} as Date", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // Parse format: YYYY-MM-DD
        let format = time::format_description::parse("[year]-[month]-[day]").expect("valid format");
        time::Date::parse(s, &format).map_err(|e| Error::Decode(format!("invalid date: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::DATE {
            return Err(Error::Decode(format!("cannot decode oid {} as Date", oid)));
        }
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Date length: {}", bytes.len())))?;
        let pg_days = i32::from_be_bytes(arr);
        time::Date::from_julian_day(pg_days + PG_EPOCH_JULIAN_DAY)
            .map_err(|e| Error::Decode(format!("invalid date: {}", e)))
    }
}

impl ToWireValue for time::Date {
    fn natural_oid(&self) -> Oid {
        oid::DATE
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::DATE => {
                let pg_days = self.to_julian_day() - PG_EPOCH_JULIAN_DAY;
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&pg_days.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl FromWireValue<'_> for time::Time {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIME {
            return Err(Error::Decode(format!("cannot decode oid {} as Time", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // Try parsing with microseconds first, then without
        let format_with_micro =
            time::format_description::parse("[hour]:[minute]:[second].[subsecond]")
                .expect("valid format");
        let format_without_micro =
            time::format_description::parse("[hour]:[minute]:[second]").expect("valid format");
        time::Time::parse(s, &format_with_micro)
            .or_else(|_| time::Time::parse(s, &format_without_micro))
            .map_err(|e| Error::Decode(format!("invalid time: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIME {
            return Err(Error::Decode(format!("cannot decode oid {} as Time", oid)));
        }
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Time length: {}", bytes.len())))?;
        let usecs = i64::from_be_bytes(arr);
        // Convert microseconds to hours, minutes, seconds, microseconds
        let hours = (usecs / 3_600_000_000) as u8;
        let remaining = usecs % 3_600_000_000;
        let minutes = (remaining / 60_000_000) as u8;
        let remaining = remaining % 60_000_000;
        let seconds = (remaining / 1_000_000) as u8;
        let micros = (remaining % 1_000_000) as u32;
        time::Time::from_hms_micro(hours, minutes, seconds, micros)
            .map_err(|e| Error::Decode(format!("invalid time: {}", e)))
    }
}

impl ToWireValue for time::Time {
    fn natural_oid(&self) -> Oid {
        oid::TIME
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TIME => {
                let (hour, minute, second, nano) = self.as_hms_nano();
                let usecs = (hour as i64) * 3_600_000_000
                    + (minute as i64) * 60_000_000
                    + (second as i64) * 1_000_000
                    + (nano as i64) / 1000;
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&usecs.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl FromWireValue<'_> for time::PrimitiveDateTime {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::TIMESTAMP | oid::TIMESTAMPTZ) {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as Timestamp",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // Remove timezone suffix if present for TIMESTAMPTZ
        let s = s
            .find(|c| c == '+' || c == '-')
            .filter(|&pos| pos > 10) // Make sure it's not the date separator
            .map(|pos| &s[..pos])
            .unwrap_or(s);
        // Try parsing with microseconds first, then without
        let format_with_micro = time::format_description::parse(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]",
        )
        .expect("valid format");
        let format_without_micro =
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
                .expect("valid format");
        time::PrimitiveDateTime::parse(s, &format_with_micro)
            .or_else(|_| time::PrimitiveDateTime::parse(s, &format_without_micro))
            .map_err(|e| Error::Decode(format!("invalid timestamp: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::TIMESTAMP | oid::TIMESTAMPTZ) {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as Timestamp",
                oid
            )));
        }
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Timestamp length: {}", bytes.len())))?;
        let usecs = i64::from_be_bytes(arr);
        // PostgreSQL epoch is 2000-01-01 00:00:00
        const PG_EPOCH: time::PrimitiveDateTime = time::macros::datetime!(2000-01-01 00:00:00);
        PG_EPOCH
            .checked_add(time::Duration::microseconds(usecs))
            .ok_or_else(|| Error::Decode("timestamp overflow".into()))
    }
}

impl ToWireValue for time::PrimitiveDateTime {
    fn natural_oid(&self) -> Oid {
        oid::TIMESTAMP
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TIMESTAMP | oid::TIMESTAMPTZ => {
                // Calculate microseconds since PostgreSQL epoch (2000-01-01 00:00:00)
                const PG_EPOCH: time::PrimitiveDateTime =
                    time::macros::datetime!(2000-01-01 00:00:00);
                let duration = *self - PG_EPOCH;
                let usecs = duration.whole_microseconds() as i64;
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&usecs.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl FromWireValue<'_> for time::OffsetDateTime {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIMESTAMPTZ {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as OffsetDateTime",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // PostgreSQL returns TIMESTAMPTZ with offset like "2024-01-15 10:30:00+00"
        // Try various formats
        let formats = [
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour]",
        ];
        for fmt in &formats {
            if let Ok(format) = time::format_description::parse(fmt)
                && let Ok(dt) = time::OffsetDateTime::parse(s, &format)
            {
                return Ok(dt);
            }
        }
        Err(Error::Decode(format!("invalid timestamptz: {}", s)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIMESTAMPTZ {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as OffsetDateTime",
                oid
            )));
        }
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Timestamp length: {}", bytes.len())))?;
        let usecs = i64::from_be_bytes(arr);
        // PostgreSQL stores TIMESTAMPTZ as UTC microseconds since 2000-01-01 00:00:00 UTC
        const PG_EPOCH: time::OffsetDateTime = time::macros::datetime!(2000-01-01 00:00:00 UTC);
        PG_EPOCH
            .checked_add(time::Duration::microseconds(usecs))
            .ok_or_else(|| Error::Decode("timestamp overflow".into()))
    }
}

impl ToWireValue for time::OffsetDateTime {
    fn natural_oid(&self) -> Oid {
        oid::TIMESTAMPTZ
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TIMESTAMP | oid::TIMESTAMPTZ => {
                // Convert to UTC and calculate microseconds since PostgreSQL epoch
                let utc = self.to_offset(time::UtcOffset::UTC);
                const PG_EPOCH: time::OffsetDateTime =
                    time::macros::datetime!(2000-01-01 00:00:00 UTC);
                let duration = utc - PG_EPOCH;
                let usecs = duration.whole_microseconds() as i64;
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&usecs.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_text() {
        let date = time::Date::from_text(oid::DATE, b"2024-01-15").unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month() as u8, 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_date_binary() {
        // 2024-01-15 is 8780 days since 2000-01-01
        let days: i32 = 8780;
        let bytes = days.to_be_bytes();
        let date = time::Date::from_binary(oid::DATE, &bytes).unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month() as u8, 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_date_roundtrip() {
        let original = time::Date::from_calendar_date(2024, time::Month::January, 15).unwrap();
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        let decoded = time::Date::from_binary(oid::DATE, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_time_text() {
        let time = time::Time::from_text(oid::TIME, b"10:30:45").unwrap();
        assert_eq!(time.hour(), 10);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
    }

    #[test]
    fn test_time_text_with_micros() {
        let time = time::Time::from_text(oid::TIME, b"10:30:45.123456").unwrap();
        assert_eq!(time.hour(), 10);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
        assert_eq!(time.microsecond(), 123456);
    }

    #[test]
    fn test_time_binary() {
        // 10:30:45 = (10*3600 + 30*60 + 45) * 1_000_000 microseconds
        let usecs: i64 = (10 * 3600 + 30 * 60 + 45) * 1_000_000;
        let bytes = usecs.to_be_bytes();
        let time = time::Time::from_binary(oid::TIME, &bytes).unwrap();
        assert_eq!(time.hour(), 10);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
    }

    #[test]
    fn test_time_roundtrip() {
        let original = time::Time::from_hms_micro(10, 30, 45, 123456).unwrap();
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        let decoded = time::Time::from_binary(oid::TIME, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_timestamp_text() {
        let ts =
            time::PrimitiveDateTime::from_text(oid::TIMESTAMP, b"2024-01-15 10:30:45").unwrap();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month() as u8, 1);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 10);
        assert_eq!(ts.minute(), 30);
        assert_eq!(ts.second(), 45);
    }

    #[test]
    fn test_timestamp_binary() {
        // 2024-01-15 10:30:45 UTC
        // Days since 2000-01-01: 8780
        // Time: 10:30:45 = 37845 seconds = 37845000000 microseconds
        let day_usecs: i64 = 8780 * 24 * 3600 * 1_000_000;
        let time_usecs: i64 = (10 * 3600 + 30 * 60 + 45) * 1_000_000;
        let total_usecs = day_usecs + time_usecs;
        let bytes = total_usecs.to_be_bytes();
        let ts = time::PrimitiveDateTime::from_binary(oid::TIMESTAMP, &bytes).unwrap();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month() as u8, 1);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 10);
        assert_eq!(ts.minute(), 30);
        assert_eq!(ts.second(), 45);
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let date = time::Date::from_calendar_date(2024, time::Month::January, 15).unwrap();
        let time = time::Time::from_hms_micro(10, 30, 45, 123456).unwrap();
        let original = time::PrimitiveDateTime::new(date, time);
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        let decoded = time::PrimitiveDateTime::from_binary(oid::TIMESTAMP, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_timestamptz_binary() {
        // Same as timestamp but with timezone
        let day_usecs: i64 = 8780 * 24 * 3600 * 1_000_000;
        let time_usecs: i64 = (10 * 3600 + 30 * 60 + 45) * 1_000_000;
        let total_usecs = day_usecs + time_usecs;
        let bytes = total_usecs.to_be_bytes();
        let ts = time::OffsetDateTime::from_binary(oid::TIMESTAMPTZ, &bytes).unwrap();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month() as u8, 1);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 10);
        assert_eq!(ts.minute(), 30);
        assert_eq!(ts.second(), 45);
    }

    #[test]
    fn test_timestamptz_roundtrip() {
        let original = time::OffsetDateTime::now_utc();
        // Truncate to microseconds (PostgreSQL precision)
        let original = original
            .replace_nanosecond((original.nanosecond() / 1000) * 1000)
            .unwrap();
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        let decoded = time::OffsetDateTime::from_binary(oid::TIMESTAMPTZ, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }
}
