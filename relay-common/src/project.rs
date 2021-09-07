use std::fmt;
use std::str::FromStr;

#[doc(inline)]
pub use sentry_types::ProjectId;

/// An error parsing [`ProjectKey`].
#[derive(Clone, Copy, Debug)]
pub struct ParseProjectKeyError;

impl fmt::Display for ParseProjectKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid project key")
    }
}

impl std::error::Error for ParseProjectKeyError {}

/// The public key used in a DSN to identify and authenticate for a project at Sentry.
///
/// Project keys are always 32-character hexadecimal strings.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ProjectKey(u128);

impl_str_serde!(ProjectKey, "a project key string");

impl ProjectKey {
    /// Parses a `ProjectKey` from a string.
    pub fn parse(key: &str) -> Result<Self, ParseProjectKeyError> {
        if key.len() != 32 || !key.is_ascii() {
            return Err(ParseProjectKeyError);
        }

        let number = u128::from_str_radix(key, 16).map_err(|_| ParseProjectKeyError)?;
        Ok(ProjectKey(number))
    }

    /// Parses a `ProjectKey` from a string with flags.
    pub fn parse_with_flags(key: &str) -> Result<(Self, Vec<&str>), ParseProjectKeyError> {
        let mut iter = key.split('.');
        let key = ProjectKey::parse(iter.next().ok_or(ParseProjectKeyError)?)?;
        Ok((key, iter.collect()))
    }
}

impl fmt::Debug for ProjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProjectKey(\"{}\")", self)
    }
}

impl fmt::Display for ProjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:0width$x}", self.0, width = 32)
    }
}

impl FromStr for ProjectKey {
    type Err = ParseProjectKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
