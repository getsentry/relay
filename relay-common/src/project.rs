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
pub struct ProjectKey([u8; 32]);

impl_str_serde!(ProjectKey, "a project key string");

impl ProjectKey {
    /// Parses a `ProjectKey` from a string.
    pub fn parse(key: &str) -> Result<Self, ParseProjectKeyError> {
        if key.len() != 32 || !key.is_ascii() {
            return Err(ParseProjectKeyError);
        }

        let mut project_key = Self(Default::default());
        project_key.0.copy_from_slice(key.as_bytes());
        Ok(project_key)
    }

    /// Parses a `ProjectKey` from a string with flags.
    pub fn parse_with_flags(key: &str) -> Result<(Self, Vec<&str>), ParseProjectKeyError> {
        let mut iter = key.split('.');
        let key = ProjectKey::parse(iter.next().ok_or(ParseProjectKeyError)?)?;
        Ok((key, iter.collect()))
    }

    /// Returns the string representation of the project key.
    #[inline]
    pub fn as_str(&self) -> &str {
        // Safety: The string is already validated to be of length 32 and valid ASCII when
        // constructing `ProjectKey`.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl fmt::Debug for ProjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProjectKey(\"{}\")", self.as_str())
    }
}

impl fmt::Display for ProjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl FromStr for ProjectKey {
    type Err = ParseProjectKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
