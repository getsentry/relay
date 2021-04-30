//! Utilities to deal with date-time types. (DateTime, Instant, SystemTime, etc)

use std::fmt;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// Converts an `Instant` into a `SystemTime`.
pub fn instant_to_system_time(instant: Instant) -> SystemTime {
    SystemTime::now() - instant.elapsed()
}

/// Converts an `Instant` into a `DateTime`.
pub fn instant_to_date_time(instant: Instant) -> chrono::DateTime<chrono::Utc> {
    instant_to_system_time(instant).into()
}

/// The conversion result of [`UnixTimestamp::to_instant`].
///
/// If the time is outside of what can be represented in an [`Instant`], this is `Past` or
/// `Future`.
#[derive(Clone, Copy, Debug)]
pub enum MonotonicResult {
    /// A time before the earliest representable `Instant`.
    Past,
    /// A representable `Instant`.
    Instant(Instant),
    /// A time after the latest representable `Instant`.
    Future,
}

/// A unix timestamp (full seconds elapsed since 1970-01-01 00:00 UTC).
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UnixTimestamp(u64);

impl UnixTimestamp {
    /// Creates a unix timestamp from the given number of seconds.
    pub fn from_secs(secs: u64) -> Self {
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

    /// Converts the UNIX timestamp into an `Instant` based on the current system timestamp.
    ///
    /// Returns [`MonotonicResult::Instant`] if the timestamp can be represented. Otherwise, returns
    /// [`MonotonicResult::Past`] or [`MonotonicResult::Future`].
    ///
    /// Note that the system time is subject to skew, so subsequent calls to `to_instant` may return
    /// different values.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    /// use relay_common::{MonotonicResult, UnixTimestamp};
    ///
    /// let timestamp = UnixTimestamp::now();
    /// if let MonotonicResult::Instant(instant) = timestamp.to_instant() {
    ///    assert!((Instant::now() - instant) < Duration::from_millis(1));
    /// }
    /// ```
    pub fn to_instant(self) -> MonotonicResult {
        let now = Self::now();

        if self > now {
            match Instant::now().checked_add(self - now) {
                Some(instant) => MonotonicResult::Instant(instant),
                None => MonotonicResult::Future,
            }
        } else {
            match Instant::now().checked_sub(now - self) {
                Some(instant) => MonotonicResult::Instant(instant),
                None => MonotonicResult::Past,
            }
        }
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

impl std::ops::Sub for UnixTimestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::from_secs(self.0 - rhs.0)
    }
}

/// An error returned from parsing [`UnixTimestamp`].
pub struct ParseUnixTimestampError(());

impl std::str::FromStr for UnixTimestamp {
    type Err = ParseUnixTimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
    use super::*;
    use serde_test::{assert_de_tokens, assert_de_tokens_error, assert_tokens, Token};

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
