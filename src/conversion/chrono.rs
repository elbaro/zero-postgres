//! Chrono crate type implementations (NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>).

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

/// PostgreSQL epoch: 2000-01-01
const PG_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(2000, 1, 1) {
    Some(d) => d,
    None => panic!("invalid date"),
};

/// Microseconds per second
const USECS_PER_SEC: i64 = 1_000_000;

/// Microseconds per day (used in tests)
#[cfg(test)]
const USECS_PER_DAY: i64 = 86_400_000_000;

impl FromWireValue<'_> for NaiveDate {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::DATE {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as NaiveDate",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| Error::Decode(format!("invalid date: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::DATE {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as NaiveDate",
                oid
            )));
        }
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Date length: {}", bytes.len())))?;
        let pg_days = i32::from_be_bytes(arr);
        PG_EPOCH
            .checked_add_days(chrono::Days::new(pg_days.max(0) as u64))
            .or_else(|| PG_EPOCH.checked_sub_days(chrono::Days::new((-pg_days).max(0) as u64)))
            .ok_or_else(|| Error::Decode("date overflow".into()))
    }
}

impl ToWireValue for NaiveDate {
    fn natural_oid(&self) -> Oid {
        oid::DATE
    }

    fn to_binary(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::DATE => {
                let pg_days = self.signed_duration_since(PG_EPOCH).num_days() as i32;
                buf.extend_from_slice(&4_i32.to_be_bytes());
                buf.extend_from_slice(&pg_days.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl FromWireValue<'_> for NaiveTime {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIME {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as NaiveTime",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // Try with microseconds first, then without
        NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
            .map_err(|e| Error::Decode(format!("invalid time: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIME {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as NaiveTime",
                oid
            )));
        }
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Time length: {}", bytes.len())))?;
        let usecs = i64::from_be_bytes(arr);
        let secs = (usecs / USECS_PER_SEC) as u32;
        let nano = ((usecs % USECS_PER_SEC) * 1000) as u32;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
            .ok_or_else(|| Error::Decode("invalid time".into()))
    }
}

impl ToWireValue for NaiveTime {
    fn natural_oid(&self) -> Oid {
        oid::TIME
    }

    fn to_binary(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TIME => {
                let usecs = (self.num_seconds_from_midnight() as i64) * USECS_PER_SEC
                    + (self.nanosecond() as i64) / 1000;
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&usecs.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl FromWireValue<'_> for NaiveDateTime {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::TIMESTAMP | oid::TIMESTAMPTZ) {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as NaiveDateTime",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // Remove timezone suffix if present for TIMESTAMPTZ
        let s = s
            .find(|c| c == '+' || c == '-')
            .filter(|&pos| pos > 10)
            .map(|pos| &s[..pos])
            .unwrap_or(s);
        // Try with microseconds first, then without
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
            .map_err(|e| Error::Decode(format!("invalid timestamp: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if !matches!(oid, oid::TIMESTAMP | oid::TIMESTAMPTZ) {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as NaiveDateTime",
                oid
            )));
        }
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Timestamp length: {}", bytes.len())))?;
        let usecs = i64::from_be_bytes(arr);
        let pg_epoch_dt = PG_EPOCH
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| Error::Decode("invalid epoch".into()))?;
        let secs = usecs / USECS_PER_SEC;
        let nano = ((usecs % USECS_PER_SEC).abs() * 1000) as u32;
        pg_epoch_dt
            .checked_add_signed(chrono::Duration::seconds(secs))
            .and_then(|dt| {
                if usecs >= 0 {
                    dt.with_nanosecond(nano)
                } else {
                    dt.checked_sub_signed(chrono::Duration::nanoseconds(nano as i64))
                }
            })
            .ok_or_else(|| Error::Decode("timestamp overflow".into()))
    }
}

impl ToWireValue for NaiveDateTime {
    fn natural_oid(&self) -> Oid {
        oid::TIMESTAMP
    }

    fn to_binary(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TIMESTAMP | oid::TIMESTAMPTZ => {
                let pg_epoch_dt = PG_EPOCH.and_hms_opt(0, 0, 0).expect("valid epoch");
                let duration = self.signed_duration_since(pg_epoch_dt);
                let usecs = duration.num_microseconds().unwrap_or(i64::MAX);
                buf.extend_from_slice(&8_i32.to_be_bytes());
                buf.extend_from_slice(&usecs.to_be_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

impl FromWireValue<'_> for DateTime<Utc> {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIMESTAMPTZ {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as DateTime<Utc>",
                oid
            )));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        // PostgreSQL returns TIMESTAMPTZ like "2024-01-15 10:30:00+00"
        // Parse with timezone
        DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
            .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z"))
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| Error::Decode(format!("invalid timestamptz: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::TIMESTAMPTZ {
            return Err(Error::Decode(format!(
                "cannot decode oid {} as DateTime<Utc>",
                oid
            )));
        }
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Decode(format!("invalid Timestamp length: {}", bytes.len())))?;
        let usecs = i64::from_be_bytes(arr);
        // PostgreSQL stores TIMESTAMPTZ as UTC microseconds since 2000-01-01 00:00:00 UTC
        let pg_epoch_utc = PG_EPOCH
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| Error::Decode("invalid epoch".into()))?
            .and_utc();
        let secs = usecs / USECS_PER_SEC;
        let nano = ((usecs % USECS_PER_SEC).abs() * 1000) as u32;
        pg_epoch_utc
            .checked_add_signed(chrono::Duration::seconds(secs))
            .and_then(|dt| {
                if usecs >= 0 {
                    dt.with_nanosecond(nano)
                } else {
                    dt.checked_sub_signed(chrono::Duration::nanoseconds(nano as i64))
                }
            })
            .ok_or_else(|| Error::Decode("timestamp overflow".into()))
    }
}

impl ToWireValue for DateTime<Utc> {
    fn natural_oid(&self) -> Oid {
        oid::TIMESTAMPTZ
    }

    fn to_binary(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::TIMESTAMP | oid::TIMESTAMPTZ => {
                let pg_epoch_utc = PG_EPOCH
                    .and_hms_opt(0, 0, 0)
                    .expect("valid epoch")
                    .and_utc();
                let duration = self.signed_duration_since(pg_epoch_utc);
                let usecs = duration.num_microseconds().unwrap_or(i64::MAX);
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
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_date_text() {
        let date = NaiveDate::from_text(oid::DATE, b"2024-01-15").unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_date_binary() {
        // 2024-01-15 is 8780 days since 2000-01-01
        let days: i32 = 8780;
        let bytes = days.to_be_bytes();
        let date = NaiveDate::from_binary(oid::DATE, &bytes).unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_date_roundtrip() {
        let original = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let mut buf = Vec::new();
        original.to_binary(original.natural_oid(), &mut buf).unwrap();
        let decoded = NaiveDate::from_binary(oid::DATE, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_time_text() {
        let time = NaiveTime::from_text(oid::TIME, b"10:30:45").unwrap();
        assert_eq!(time.hour(), 10);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
    }

    #[test]
    fn test_time_text_with_micros() {
        let time = NaiveTime::from_text(oid::TIME, b"10:30:45.123456").unwrap();
        assert_eq!(time.hour(), 10);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
        // chrono stores nanoseconds
        assert_eq!(time.nanosecond(), 123456000);
    }

    #[test]
    fn test_time_binary() {
        // 10:30:45 = (10*3600 + 30*60 + 45) * 1_000_000 microseconds
        let usecs: i64 = (10 * 3600 + 30 * 60 + 45) * 1_000_000;
        let bytes = usecs.to_be_bytes();
        let time = NaiveTime::from_binary(oid::TIME, &bytes).unwrap();
        assert_eq!(time.hour(), 10);
        assert_eq!(time.minute(), 30);
        assert_eq!(time.second(), 45);
    }

    #[test]
    fn test_time_roundtrip() {
        let original = NaiveTime::from_hms_micro_opt(10, 30, 45, 123456).unwrap();
        let mut buf = Vec::new();
        original.to_binary(original.natural_oid(), &mut buf).unwrap();
        let decoded = NaiveTime::from_binary(oid::TIME, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_timestamp_text() {
        let ts = NaiveDateTime::from_text(oid::TIMESTAMP, b"2024-01-15 10:30:45").unwrap();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month(), 1);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 10);
        assert_eq!(ts.minute(), 30);
        assert_eq!(ts.second(), 45);
    }

    #[test]
    fn test_timestamp_binary() {
        // 2024-01-15 10:30:45 UTC
        // Days since 2000-01-01: 8780
        let day_usecs: i64 = 8780 * USECS_PER_DAY;
        let time_usecs: i64 = (10 * 3600 + 30 * 60 + 45) * USECS_PER_SEC;
        let total_usecs = day_usecs + time_usecs;
        let bytes = total_usecs.to_be_bytes();
        let ts = NaiveDateTime::from_binary(oid::TIMESTAMP, &bytes).unwrap();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month(), 1);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 10);
        assert_eq!(ts.minute(), 30);
        assert_eq!(ts.second(), 45);
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let original = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_micro_opt(10, 30, 45, 123456)
            .unwrap();
        let mut buf = Vec::new();
        original.to_binary(original.natural_oid(), &mut buf).unwrap();
        let decoded = NaiveDateTime::from_binary(oid::TIMESTAMP, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_timestamptz_binary() {
        let day_usecs: i64 = 8780 * USECS_PER_DAY;
        let time_usecs: i64 = (10 * 3600 + 30 * 60 + 45) * USECS_PER_SEC;
        let total_usecs = day_usecs + time_usecs;
        let bytes = total_usecs.to_be_bytes();
        let ts = DateTime::<Utc>::from_binary(oid::TIMESTAMPTZ, &bytes).unwrap();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month(), 1);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 10);
        assert_eq!(ts.minute(), 30);
        assert_eq!(ts.second(), 45);
    }

    #[test]
    fn test_timestamptz_roundtrip() {
        let original = Utc::now();
        // Truncate to microseconds (PostgreSQL precision)
        let original = original
            .with_nanosecond((original.nanosecond() / 1000) * 1000)
            .unwrap();
        let mut buf = Vec::new();
        original.to_binary(original.natural_oid(), &mut buf).unwrap();
        let decoded = DateTime::<Utc>::from_binary(oid::TIMESTAMPTZ, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }
}
