//! Redactions for rules.
use serde::{Deserialize, Serialize};

fn default_replace_text() -> String {
    "[Filtered]".into()
}

/// Replaces a value with a specific string.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplaceRedaction {
    /// The replacement string.
    #[serde(default = "default_replace_text")]
    pub text: String,
}

impl From<String> for ReplaceRedaction {
    fn from(text: String) -> ReplaceRedaction {
        ReplaceRedaction { text }
    }
}

impl Default for ReplaceRedaction {
    fn default() -> Self {
        ReplaceRedaction {
            text: default_replace_text(),
        }
    }
}

/// Defines how replacements happen.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Redaction {
    /// The default redaction for this operation (normally equivalent to `Remove`).
    ///
    /// The main difference to `Remove` is that if the redaction is explicitly
    /// set to `Remove` it also applies in situations where a default
    /// redaction is therwise not passed down (for instance with `Multiple`).
    Default,
    /// Removes the value and puts nothing in its place.
    Remove,
    /// Replaces the matched group with a new value.
    Replace(ReplaceRedaction),
    /// Overwrites the matched value by masking.
    Mask,
    /// Replaces the value with a hash
    Hash,
}

impl Default for Redaction {
    fn default() -> Redaction {
        Redaction::Default
    }
}
