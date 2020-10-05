use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

#[doc(inline)]
pub use sentry_types::ProjectId;

/// TODO: Doc
#[derive(Clone, Copy, Debug)]
pub struct ParseProjectKeyError;

impl fmt::Display for ParseProjectKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid project key")
    }
}

impl std::error::Error for ParseProjectKeyError {}

/// TODO: Document
#[derive(Clone, Copy, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ProjectKey([u8; 32]);

impl Serialize for ProjectKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProjectKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let cow = Cow::<str>::deserialize(deserializer)?;
        Self::parse(&cow).map_err(serde::de::Error::custom)
    }
}

impl ProjectKey {
    /// TODO: Document
    pub fn parse(key: &str) -> Result<Self, ParseProjectKeyError> {
        if key.len() != 32 || !key.is_ascii() {
            return Err(ParseProjectKeyError);
        }

        let mut project_key = Self(Default::default());
        project_key.0.copy_from_slice(key.as_bytes());
        Ok(project_key)
    }

    /// TODO: Document
    #[inline]
    pub fn as_str(&self) -> &str {
        // TODO: Describe safety
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
