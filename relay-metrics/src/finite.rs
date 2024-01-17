use std::cmp::Ordering;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::num::ParseFloatError;
use std::str::FromStr;
use std::{fmt, ops};

use serde::{Deserialize, Serialize};

/// A finite 64-bit floating point type.
///
/// This is a restricted version of [`f64`] that does not allow NaN or infinity.
#[derive(Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "f64")]
#[repr(transparent)]
pub struct FiniteF64(f64);

impl FiniteF64 {
    /// Largest finite value.
    pub const MAX: Self = Self(f64::MAX);
    /// Smallest finite value.
    pub const MIN: Self = Self(f64::MIN);
    /// Smallest positive normal value.
    pub const EPSILON: Self = Self(f64::EPSILON);

    /// Creates a finite float without checking whether the value is finte. This results in
    /// undefined behavior if the value is non-finite.
    ///
    /// # Safety
    ///
    /// The value must not be NaN or infinite.
    #[must_use]
    #[inline]
    pub const unsafe fn new_unchecked(value: f64) -> Self {
        Self(value)
    }

    /// Creates a finite float if the value is finite.
    #[must_use]
    #[inline]
    pub fn new(value: f64) -> Option<Self> {
        if value.is_finite() {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Returns the plain [`f64`].
    #[inline]
    pub const fn to_f64(self) -> f64 {
        self.0
    }

    /// Computes the absolute value of self.
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Returns the maximum of two numbers.
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    /// Returns the minimum of two numbers.
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    /// Adds two numbers, saturating at the maximum and minimum representable values.
    pub fn saturating_add(self, other: Self) -> Self {
        Self((self.0 + other.0).clamp(f64::MIN, f64::MAX))
    }

    /// Adds two numbers, saturating at the maximum and minimum representable values.
    pub fn saturating_sub(self, other: Self) -> Self {
        Self((self.0 - other.0).clamp(f64::MIN, f64::MAX))
    }

    /// Multiplies two numbers, saturating at the maximum and minimum representable values.
    pub fn saturating_mul(self, other: Self) -> Self {
        Self((self.0 * other.0).clamp(f64::MIN, f64::MAX))
    }

    // NB: There is no saturating_div, since 0/0 is NaN, which is not finite.
}

impl Eq for FiniteF64 {}

impl PartialOrd for FiniteF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FiniteF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        // Safety: NaN and infinity cannot be constructed from a finite f64.
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Less)
    }
}

impl Hash for FiniteF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Safety: NaN and infinity cannot be constructed from a finite f64.
        self.0.to_bits().hash(state)
    }
}

impl fmt::Debug for FiniteF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for FiniteF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ops::Add for FiniteF64 {
    type Output = Option<Self>;

    fn add(self, other: Self) -> Option<Self> {
        Self::new(self.0 + other.0)
    }
}

impl ops::Sub for FiniteF64 {
    type Output = Option<Self>;

    fn sub(self, other: Self) -> Option<Self> {
        Self::new(self.0 - other.0)
    }
}

impl ops::Mul for FiniteF64 {
    type Output = Option<Self>;

    fn mul(self, other: Self) -> Option<Self> {
        Self::new(self.0 * other.0)
    }
}

impl ops::Div for FiniteF64 {
    type Output = Option<Self>;

    fn div(self, other: Self) -> Option<Self> {
        Self::new(self.0 / other.0)
    }
}

impl ops::Rem for FiniteF64 {
    type Output = Option<Self>;

    fn rem(self, other: Self) -> Option<Self> {
        Self::new(self.0 % other.0)
    }
}

/// Error type returned when conversion to [`FiniteF64`] fails.
#[derive(Debug, thiserror::Error)]
#[error("float is nan or infinite")]
pub struct TryFromFloatError;

impl TryFrom<f64> for FiniteF64 {
    type Error = TryFromFloatError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(TryFromFloatError)
    }
}

impl TryFrom<f32> for FiniteF64 {
    type Error = TryFromFloatError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        f64::from(value).try_into()
    }
}

impl From<i8> for FiniteF64 {
    fn from(value: i8) -> Self {
        unsafe { Self::new_unchecked(value.into()) }
    }
}

impl From<i16> for FiniteF64 {
    fn from(value: i16) -> Self {
        unsafe { Self::new_unchecked(value.into()) }
    }
}

impl From<i32> for FiniteF64 {
    fn from(value: i32) -> Self {
        unsafe { Self::new_unchecked(value.into()) }
    }
}

impl From<u8> for FiniteF64 {
    fn from(value: u8) -> Self {
        unsafe { Self::new_unchecked(value.into()) }
    }
}

impl From<u16> for FiniteF64 {
    fn from(value: u16) -> Self {
        unsafe { Self::new_unchecked(value.into()) }
    }
}

impl From<u32> for FiniteF64 {
    fn from(value: u32) -> Self {
        unsafe { Self::new_unchecked(value.into()) }
    }
}

impl From<FiniteF64> for f64 {
    fn from(value: FiniteF64) -> Self {
        value.to_f64()
    }
}

#[derive(Debug)]
enum ParseFiniteFloatErrorKind {
    Invalid(ParseFloatError),
    NonFinite(TryFromFloatError),
}

/// Error type returned when parsing [`FiniteF64`] fails.
#[derive(Debug)]
pub struct ParseFiniteFloatError(ParseFiniteFloatErrorKind);

impl fmt::Display for ParseFiniteFloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ParseFiniteFloatErrorKind::Invalid(err) => err.fmt(f),
            ParseFiniteFloatErrorKind::NonFinite(err) => err.fmt(f),
        }
    }
}

impl Error for ParseFiniteFloatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.0 {
            ParseFiniteFloatErrorKind::Invalid(err) => Some(err),
            ParseFiniteFloatErrorKind::NonFinite(err) => Some(err),
        }
    }
}

impl From<ParseFloatError> for ParseFiniteFloatError {
    fn from(err: ParseFloatError) -> Self {
        Self(ParseFiniteFloatErrorKind::Invalid(err))
    }
}

impl From<TryFromFloatError> for ParseFiniteFloatError {
    fn from(err: TryFromFloatError) -> Self {
        Self(ParseFiniteFloatErrorKind::NonFinite(err))
    }
}

impl FromStr for FiniteF64 {
    type Err = ParseFiniteFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.parse::<f64>()?.try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        assert_eq!(FiniteF64::new(0.0), Some(FiniteF64(0.0)));
        assert_eq!(FiniteF64::new(1.0), Some(FiniteF64(1.0)));
        assert_eq!(FiniteF64::new(-1.0), Some(FiniteF64(-1.0)));
        assert_eq!(FiniteF64::new(f64::MIN), Some(FiniteF64(f64::MIN)));
        assert_eq!(FiniteF64::new(f64::MAX), Some(FiniteF64(f64::MAX)));
        assert_eq!(FiniteF64::new(f64::NAN), None);
        assert_eq!(FiniteF64::new(f64::INFINITY), None);
        assert_eq!(FiniteF64::new(f64::NEG_INFINITY), None);
    }

    #[test]
    fn test_arithmetics() {
        assert_eq!(FiniteF64(1.0) + FiniteF64(1.0), Some(FiniteF64(2.0)));
        assert_eq!(FiniteF64(f64::MAX) + FiniteF64(f64::MAX), None);
        assert_eq!(FiniteF64(f64::MIN) + FiniteF64(f64::MIN), None);

        assert_eq!(FiniteF64(1.0) - FiniteF64(1.0), Some(FiniteF64(0.0)));
        assert_eq!(
            FiniteF64(f64::MAX) - FiniteF64(f64::MAX),
            Some(FiniteF64(0.0))
        );
        assert_eq!(
            FiniteF64(f64::MIN) - FiniteF64(f64::MIN),
            Some(FiniteF64(0.0))
        );

        assert_eq!(FiniteF64(2.0) * FiniteF64(2.0), Some(FiniteF64(4.0)));
        assert_eq!(FiniteF64(f64::MAX) * FiniteF64(f64::MAX), None);
        assert_eq!(FiniteF64(f64::MIN) * FiniteF64(f64::MIN), None);

        assert_eq!(FiniteF64(2.0) / FiniteF64(2.0), Some(FiniteF64(1.0)));
        assert_eq!(FiniteF64(2.0) / FiniteF64(0.0), None); // Infinity
        assert_eq!(FiniteF64(-2.0) / FiniteF64(0.0), None); // -Infinity
        assert_eq!(FiniteF64(0.0) / FiniteF64(0.0), None); // NaN
    }

    #[test]
    fn test_saturating_add() {
        assert_eq!(
            FiniteF64(1.0).saturating_add(FiniteF64(1.0)),
            FiniteF64(2.0)
        );
        assert_eq!(
            FiniteF64(f64::MAX).saturating_add(FiniteF64(1.0)),
            FiniteF64(f64::MAX)
        );
        assert_eq!(
            FiniteF64(f64::MIN).saturating_add(FiniteF64(-1.0)),
            FiniteF64(f64::MIN)
        );
    }

    #[test]
    fn test_saturating_sub() {
        assert_eq!(
            FiniteF64(1.0).saturating_sub(FiniteF64(1.0)),
            FiniteF64(0.0)
        );
        assert_eq!(
            FiniteF64(f64::MAX).saturating_sub(FiniteF64(-1.0)),
            FiniteF64(f64::MAX)
        );
        assert_eq!(
            FiniteF64(f64::MIN).saturating_sub(FiniteF64(1.0)),
            FiniteF64(f64::MIN)
        );
    }

    #[test]
    fn test_saturating_mul() {
        assert_eq!(
            FiniteF64::from(2).saturating_mul(FiniteF64::from(2)),
            FiniteF64::from(4)
        );
        assert_eq!(
            FiniteF64(f64::MAX).saturating_mul(FiniteF64::from(2)),
            FiniteF64(f64::MAX)
        );
        assert_eq!(
            FiniteF64(f64::MIN).saturating_mul(FiniteF64::from(2)),
            FiniteF64(f64::MIN)
        );
    }

    #[test]
    fn teste_parse() {
        assert_eq!("0".parse::<FiniteF64>().unwrap(), FiniteF64(0.0));
        assert_eq!("0.0".parse::<FiniteF64>().unwrap(), FiniteF64(0.0));

        assert!("bla".parse::<FiniteF64>().is_err());
        assert!("inf".parse::<FiniteF64>().is_err());
        assert!("-inf".parse::<FiniteF64>().is_err());
        assert!("NaN".parse::<FiniteF64>().is_err());
    }
}
