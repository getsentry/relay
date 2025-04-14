use std::fmt;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// A UUID wrapper type specialized for Sentry's needs.
///
/// This type serializes and deserializes UUIDs in a compact format without hyphens.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SentryUuid(Uuid);

impl SentryUuid {
    /// Creates a new random [`SentryUuid`].
    ///
    /// This generates a random UUID v4 internally.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Consumes this [`SentryUuid`] and returns the underlying [`Uuid`].
    ///
    /// This allows access to the full API of the wrapped UUID when needed.
    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for SentryUuid {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Deref for SentryUuid {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for SentryUuid {
    type Err = <Uuid as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(SentryUuid)
    }
}

impl Display for SentryUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_simple())
    }
}

impl Serialize for SentryUuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(self.simple().encode_lower(&mut Uuid::encode_buffer()))
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for SentryUuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Uuid::deserialize(deserializer).map(SentryUuid)
    }
}
