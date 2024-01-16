use std::cmp::Ordering;
use std::fmt;
use std::hash::Hasher as _;

use hash32::{FnvHasher, Hasher as _};

#[doc(inline)]
pub use relay_base_schema::metrics::{
    CustomUnit, DurationUnit, FractionUnit, InformationUnit, MetricNamespace,
    MetricResourceIdentifier, MetricType, MetricUnit, ParseMetricError, ParseMetricUnitError,
};
#[doc(inline)]
pub use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};

/// Type used for Counter metric
pub type CounterType = FiniteF64;

/// Type of distribution entries
pub type DistributionType = FiniteF64;

/// Type used for set elements in Set metric
pub type SetType = u32;

/// Type used for Gauge entries
pub type GaugeType = FiniteF64;

/// TODO: Doc
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
#[repr(transparent)]
pub struct FiniteF64(f64);

impl FiniteF64 {
    /// TODO: Doc
    ///
    /// # Safety
    ///
    /// TODO
    #[must_use]
    #[inline]
    pub const unsafe fn new_unchecked(value: f64) -> Self {
        Self(value)
    }

    /// TODO: Doc
    #[must_use]
    #[inline]
    pub fn new(value: f64) -> Option<Self> {
        if value.is_finite() {
            Some(Self(value))
        } else {
            None
        }
    }

    /// TODO: Doc
    #[inline]
    pub const fn to_f64(self) -> f64 {
        self.0
    }

    /// TODO: Doc
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    /// TODO: Doc
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }
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

impl std::hash::Hash for FiniteF64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Safety: NaN and infinity cannot be constructed from a finite f64.
        self.0.to_bits().hash(state)
    }
}

impl fmt::Display for FiniteF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Add for FiniteF64 {
    type Output = Option<Self>;

    fn add(self, other: Self) -> Option<Self> {
        Self::new(self.0 + other.0)
    }
}

impl std::ops::Sub for FiniteF64 {
    type Output = Option<Self>;

    fn sub(self, other: Self) -> Option<Self> {
        Self::new(self.0 - other.0)
    }
}

impl std::ops::Mul for FiniteF64 {
    type Output = Option<Self>;

    fn mul(self, other: Self) -> Option<Self> {
        Self::new(self.0 * other.0)
    }
}

impl std::ops::Div for FiniteF64 {
    type Output = Option<Self>;

    fn div(self, other: Self) -> Option<Self> {
        Self::new(self.0 / other.0)
    }
}

impl std::ops::Rem for FiniteF64 {
    type Output = Option<Self>;

    fn rem(self, other: Self) -> Option<Self> {
        Self::new(self.0 % other.0)
    }
}

/// TODO: Doc
#[derive(Debug, thiserror::Error)]
#[error("float is nan or infinite")]
pub struct InfiniteFloatError;

impl TryFrom<f64> for FiniteF64 {
    type Error = InfiniteFloatError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(InfiniteFloatError)
    }
}

impl TryFrom<f32> for FiniteF64 {
    type Error = InfiniteFloatError;

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

#[derive(Debug, thiserror::Error)]
pub enum ParseFiniteFloatError {
    #[error(transparent)]
    Invalid(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    Infinite(#[from] InfiniteFloatError),
}

impl std::str::FromStr for FiniteF64 {
    type Err = ParseFiniteFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.parse::<f64>()?.try_into()?)
    }
}

impl<'de> Deserialize<'de> for FiniteF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        f64::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

// TODO: Impl Eq, Hash

/// Validates a tag key.
///
/// Tag keys currently only need to not contain ASCII control characters. This might change.
pub(crate) fn is_valid_tag_key(tag_key: &str) -> bool {
    // iterating over bytes produces better asm, and we're only checking for ascii chars
    for &byte in tag_key.as_bytes() {
        if (byte as char).is_ascii_control() {
            return false;
        }
    }
    true
}

/// Validates a tag value.
///
/// Tag values are never entirely rejected, but invalid characters (ASCII control characters) are
/// stripped out.
pub(crate) fn validate_tag_value(tag_value: &mut String) {
    tag_value.retain(|c| !c.is_ascii_control());
}

/// Hashes the given set value.
///
/// Sets only guarantee 32-bit accuracy, but arbitrary strings are allowed on the protocol. Upon
/// parsing, they are hashed and only used as hashes subsequently.
pub(crate) fn hash_set_value(string: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(string.as_bytes());
    hasher.finish32()
}
