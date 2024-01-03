//! Utilities to deal with date-time types. (DateTime, Instant, SystemTime, etc)

use std::fmt;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// Converts an `Instant` into a `SystemTime`.
pub fn instant_to_system_time(instant: Instant) -> SystemTime {
    SystemTime::now() - instant.elapsed()
}

/// Converts an `Instant` into a `DateTime`.
pub fn instant_to_date_time(instant: Instant) -> chrono::DateTime<chrono::Utc> {
    instant_to_system_time(instant).into()
}

/// Returns the number of milliseconds contained by this `Duration` as `f64`.
///
/// The returned value does include the fractional (nanosecond) part of the duration.
///
/// # Example
///
/// ```
/// use std::time::Duration;
///
/// let duration = Duration::from_nanos(2_125_000);
/// let millis = relay_common::time::duration_to_millis(duration);
/// assert_eq!(millis, 2.125);
/// ```
pub fn duration_to_millis(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 1_000_000_000f64).round() / 1_000_000f64
}

/// Returns the positive number of milliseconds contained by this `Duration` as `f64`.
///
/// The returned value does include the fractional (nanosecond) part of the duration. If the
/// duration is negative, this returns `0.0`;
///
/// # Example
///
/// ```
/// use chrono::Duration;
///
/// let duration = Duration::nanoseconds(2_125_000);
/// let millis = relay_common::time::chrono_to_positive_millis(duration);
/// assert_eq!(millis, 2.125);
/// ```
///
/// Negative durations are clamped to `0.0`:
///
/// ```
/// use chrono::Duration;
///
/// let duration = Duration::nanoseconds(-2_125_000);
/// let millis = relay_common::time::chrono_to_positive_millis(duration);
/// assert_eq!(millis, 0.0);
/// ```
pub fn chrono_to_positive_millis(duration: chrono::Duration) -> f64 {
    duration_to_millis(duration.to_std().unwrap_or_default())
}

/// A unix timestamp (full seconds elapsed since 1970-01-01 00:00 UTC).
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UnixTimestamp(u64);

impl UnixTimestamp {
    /// Creates a unix timestamp from the given number of seconds.
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs)
    }

    /// Creates a unix timestamp from the given system time.
    pub fn from_system(time: SystemTime) -> Self {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self(duration)
    }

    /// Creates a unix timestamp from the given chrono `DateTime`.
    ///
    /// Returns `Some` if this is a valid date time starting with 1970-01-01 00:00 UTC. If the date
    /// lies before the UNIX epoch, this function returns `None`.
    pub fn from_datetime(date_time: DateTime<impl TimeZone>) -> Option<Self> {
        let timestamp = date_time.timestamp();
        if timestamp >= 0 {
            Some(UnixTimestamp::from_secs(timestamp as u64))
        } else {
            None
        }
    }

    /// Converts the given `Instant` into a UNIX timestamp.
    ///
    /// This is done by comparing the `Instant` with the current system time. Note that the system
    /// time is subject to skew, so subsequent calls to `from_instant` may return different values.
    #[inline]
    pub fn from_instant(instant: Instant) -> Self {
        Self::from_system(instant_to_system_time(instant))
    }

    /// Returns the current timestamp.
    #[inline]
    pub fn now() -> Self {
        Self::from_system(SystemTime::now())
    }

    /// Returns the number of seconds since the UNIX epoch start.
    pub fn as_secs(self) -> u64 {
        self.0
    }

    /// Returns the timestamp as chrono datetime.
    pub fn as_datetime(self) -> Option<DateTime<Utc>> {
        NaiveDateTime::from_timestamp_opt(self.0 as i64, 0)
            .map(|n| DateTime::from_naive_utc_and_offset(n, Utc))
    }
}

impl fmt::Debug for UnixTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnixTimestamp({})", self.as_secs())
    }
}

impl fmt::Display for UnixTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_secs().fmt(f)
    }
}

/// Adds _whole_ seconds of the given duration to the timestamp.
impl std::ops::Add<Duration> for UnixTimestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.saturating_add(rhs.as_secs()))
    }
}

impl std::ops::Sub for UnixTimestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::from_secs(self.0 - rhs.0)
    }
}

#[derive(Debug)]
/// An error returned from parsing [`UnixTimestamp`].
pub struct ParseUnixTimestampError(());

impl std::str::FromStr for UnixTimestamp {
    type Err = ParseUnixTimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(datetime) = s.parse::<DateTime<Utc>>() {
            let timestamp = datetime.timestamp();
            if timestamp >= 0 {
                return Ok(UnixTimestamp(timestamp as u64));
            }
        }
        let ts = s.parse().or(Err(ParseUnixTimestampError(())))?;
        Ok(Self(ts))
    }
}

impl Serialize for UnixTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.as_secs())
    }
}

struct UnixTimestampVisitor;

impl<'de> serde::de::Visitor<'de> for UnixTimestampVisitor {
    type Value = UnixTimestamp;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a non-negative timestamp or datetime string")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(UnixTimestamp::from_secs(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v < 0.0 || v > u64::MAX as f64 {
            return Err(E::custom("timestamp out-of-range"));
        }

        Ok(UnixTimestamp::from_secs(v.trunc() as u64))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let datetime = v.parse::<DateTime<Utc>>().map_err(E::custom)?;
        let timestamp = datetime.timestamp();

        if timestamp >= 0 {
            Ok(UnixTimestamp(timestamp as u64))
        } else {
            Err(E::custom("timestamp out-of-range"))
        }
    }
}

impl<'de> Deserialize<'de> for UnixTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(UnixTimestampVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde_test::{assert_de_tokens, assert_de_tokens_error, assert_tokens, Token};

    use super::*;

    #[test]
    fn test_parse_timestamp_int() {
        assert_tokens(&UnixTimestamp::from_secs(123), &[Token::U64(123)]);
    }

    #[test]
    fn test_parse_timestamp_neg_int() {
        assert_de_tokens_error::<UnixTimestamp>(
            &[Token::I64(-1)],
            "invalid type: integer `-1`, expected a non-negative timestamp or datetime string",
        );
    }

    #[test]
    fn test_parse_timestamp_float() {
        assert_de_tokens(&UnixTimestamp::from_secs(123), &[Token::F64(123.4)]);
    }

    #[test]
    fn test_parse_timestamp_large_float() {
        assert_de_tokens_error::<UnixTimestamp>(
            &[Token::F64(2.0 * (u64::MAX as f64))],
            "timestamp out-of-range",
        );
    }

    #[test]
    fn test_parse_timestamp_neg_float() {
        assert_de_tokens_error::<UnixTimestamp>(&[Token::F64(-1.0)], "timestamp out-of-range");
    }

    #[test]
    fn test_parse_timestamp_str() {
        assert_de_tokens(
            &UnixTimestamp::from_secs(123),
            &[Token::Str("1970-01-01T00:02:03Z")],
        );
    }

    #[test]
    fn test_parse_timestamp_other() {
        assert_de_tokens_error::<UnixTimestamp>(
            &[Token::Bool(true)],
            "invalid type: boolean `true`, expected a non-negative timestamp or datetime string",
        );
    }

    #[test]
    fn test_parse_datetime_bogus() {
        assert_de_tokens_error::<UnixTimestamp>(
            &[Token::Str("adf3rt546")],
            "input contains invalid characters",
        );
    }
}
