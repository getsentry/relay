//! Redactions for rules.
use smartstring::alias::String;
use serde::{Deserialize, Serialize};

/// Defines the hash algorithm to use for hashing
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[allow(clippy::enum_variant_names)]
pub enum HashAlgorithm {
    /// HMAC-SHA1
    #[serde(rename = "HMAC-SHA1")]
    HmacSha1,
    /// HMAC-SHA256
    #[serde(rename = "HMAC-SHA256")]
    HmacSha256,
    /// HMAC-SHA512
    #[serde(rename = "HMAC-SHA512")]
    HmacSha512,
}

impl Default for HashAlgorithm {
    fn default() -> HashAlgorithm {
        HashAlgorithm::HmacSha1
    }
}

fn default_mask_char() -> char {
    '*'
}

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

impl From<std::string::String> for ReplaceRedaction {
    fn from(text: std::string::String) -> ReplaceRedaction {
        ReplaceRedaction { text: text.into() }
    }
}

impl Default for ReplaceRedaction {
    fn default() -> Self {
        ReplaceRedaction {
            text: default_replace_text(),
        }
    }
}

/// Masks the value
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MaskRedaction {
    /// The character to mask with.
    #[serde(default = "default_mask_char")]
    pub mask_char: char,
    /// Characters to skip during masking to preserve structure.
    #[serde(default)]
    pub chars_to_ignore: String,
    /// Index range to mask in. Negative indices count from the string's end.
    #[serde(default)]
    pub range: (Option<i32>, Option<i32>),
}

impl Default for MaskRedaction {
    fn default() -> Self {
        MaskRedaction {
            mask_char: default_mask_char(),
            chars_to_ignore: String::new(),
            range: (None, None),
        }
    }
}

/// Replaces the value with a hash
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HashRedaction {
    /// The hash algorithm
    #[serde(default)]
    pub algorithm: HashAlgorithm,
    /// The secret key (if not to use the default)
    #[serde(default)]
    pub key: Option<String>,
}

impl Default for HashRedaction {
    fn default() -> Self {
        HashRedaction {
            algorithm: HashAlgorithm::default(),
            key: None,
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
    Mask(MaskRedaction),
    /// Replaces the value with a hash
    Hash(HashRedaction),
}

impl Default for Redaction {
    fn default() -> Redaction {
        Redaction::Default
    }
}
