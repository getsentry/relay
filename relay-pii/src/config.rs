use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use once_cell::sync::OnceCell;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{CompiledPiiConfig, Redaction, SelectorSpec};

const COMPILED_PATTERN_MAX_SIZE: usize = 262_144;

/// Helper method to check whether a flag is false.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn is_flag_default(flag: &bool) -> bool {
    !*flag
}

/// An error returned when parsing [`PiiConfig`].
#[derive(Clone, Debug, thiserror::Error)]
pub enum PiiConfigError {
    /// A match pattern in a PII rule config could not be parsed.
    #[error("could not parse pattern")]
    RegexError(#[source] regex::Error),
}

/// Wrapper for the regex and the raw pattern string.
///
/// The regex will be compiled only when it used once, and the compiled version will be reused on
/// consecutive calls.
#[derive(Debug, Clone)]
pub struct LazyPattern {
    raw: Cow<'static, str>,
    case_insensitive: bool,
    pattern: OnceCell<Result<Regex, PiiConfigError>>,
}

impl PartialEq for LazyPattern {
    fn eq(&self, other: &Self) -> bool {
        self.raw.to_lowercase() == other.raw.to_lowercase()
    }
}

impl LazyPattern {
    /// Create a new [`LazyPattern`] from a raw string.
    pub fn new<S>(raw: S) -> Self
    where
        Cow<'static, str>: From<S>,
    {
        Self {
            raw: raw.into(),
            case_insensitive: false,
            pattern: OnceCell::new(),
        }
    }

    /// Change the case sensativity settings for the underlying regex.
    ///
    /// It's possible to set the case sensativity on already compiled [`LazyPattern`], which will
    /// be recompiled (re-built) once it's used again.
    pub fn case_insensitive(mut self, value: bool) -> Self {
        self.case_insensitive = value;
        self.pattern.take();
        self
    }

    /// Compiles the regex from the internal raw string.
    pub fn compiled(&self) -> Result<&Regex, &PiiConfigError> {
        self.pattern
            .get_or_init(|| {
                let regex_result = RegexBuilder::new(&self.raw)
                    .size_limit(COMPILED_PATTERN_MAX_SIZE)
                    .case_insensitive(self.case_insensitive)
                    .build()
                    .map_err(PiiConfigError::RegexError);

                if let Err(ref error) = regex_result {
                    relay_log::error!(
                        error = error as &dyn std::error::Error,
                        "unable to compile pattern into regex"
                    );
                }
                regex_result
            })
            .as_ref()
    }
}

impl From<&'static str> for LazyPattern {
    fn from(pattern: &'static str) -> LazyPattern {
        LazyPattern::new(pattern)
    }
}

impl Serialize for LazyPattern {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for LazyPattern {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(LazyPattern::new(raw))
    }
}

#[allow(clippy::unnecessary_wraps)]
fn replace_groups_default() -> Option<BTreeSet<u8>> {
    let mut set = BTreeSet::new();
    set.insert(0);
    Some(set)
}

/// A rule that matches a regex pattern.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PatternRule {
    /// The regular expression to apply.
    pub pattern: LazyPattern,
    /// The match group indices to replace.
    #[serde(default = "replace_groups_default")]
    pub replace_groups: Option<BTreeSet<u8>>,
}

/// A rule that dispatches to multiple other rules.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MultipleRule {
    /// A reference to other rules to apply
    pub rules: Vec<String>,
    /// When set to true, the outer rule is reported.
    #[serde(default, skip_serializing_if = "is_flag_default")]
    pub hide_inner: bool,
}

/// An alias for another rule.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AliasRule {
    /// A reference to another rule to apply.
    pub rule: String,
    /// When set to true, the outer rule is reported.
    #[serde(default, skip_serializing_if = "is_flag_default")]
    pub hide_inner: bool,
}

/// A pair redaction rule.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RedactPairRule {
    /// A pattern to match for keys.
    pub key_pattern: LazyPattern,
}

/// Supported scrubbing rules.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleType {
    /// Matches any value.
    Anything,
    /// Applies a regular expression.
    Pattern(PatternRule),
    /// Matchse an IMEI or IMEISV
    Imei,
    /// Matches a mac address
    Mac,
    /// Matches a UUID
    Uuid,
    /// Matches an email
    Email,
    /// Matches any IP address
    Ip,
    /// Matches a creditcard number
    Creditcard,
    /// Matches an IBAN
    Iban,
    /// Sanitizes a path from user data
    Userpath,
    /// A PEM encoded key
    Pemkey,
    /// Auth info from URLs
    UrlAuth,
    /// US SSN.
    UsSsn,
    /// Keys that look like passwords
    Password,
    /// When a regex matches a key, a value is removed
    #[serde(alias = "redactPair")]
    RedactPair(RedactPairRule),
    /// Applies multiple rules.
    Multiple(MultipleRule),
    /// Applies another rule.  Works like a single multiple.
    Alias(AliasRule),
    /// Unknown ruletype for forward compatibility
    Unknown(String),
}

/// A single rule configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RuleSpec {
    /// The matching rule to apply on fields.
    #[serde(flatten)]
    pub ty: RuleType,

    /// The redaction to apply on matched fields.
    #[serde(default)]
    pub redaction: Redaction,
}

/// Configuration for rule parameters.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Vars {
    /// The default secret key for hashing operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_key: Option<String>,
}

impl Vars {
    fn is_empty(&self) -> bool {
        self.hash_key.is_none()
    }
}

/// A set of named rule configurations.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct PiiConfig {
    /// A map of custom PII rules.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rules: BTreeMap<String, RuleSpec>,

    /// Parameters for PII rules.
    #[serde(default, skip_serializing_if = "Vars::is_empty")]
    pub vars: Vars,

    /// Mapping of selectors to rules.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub applications: BTreeMap<SelectorSpec, Vec<String>>,

    /// PII config derived from datascrubbing settings.
    ///
    /// Cached because the conversion process is expensive.
    #[serde(skip)]
    pub(super) compiled: OnceCell<CompiledPiiConfig>,
}

impl PartialEq for PiiConfig {
    fn eq(&self, other: &PiiConfig) -> bool {
        // This is written in this way such that people will not forget to update this PartialEq
        // impl when they add more fields.
        let PiiConfig {
            rules,
            vars,
            applications,
            compiled: _compiled,
        } = &self;

        rules == &other.rules && vars == &other.vars && applications == &other.applications
    }
}

impl PiiConfig {
    /// Get a representation of this `PiiConfig` that is more (CPU-)efficient for processing.
    ///
    /// This can be computationally expensive when called for the first time. The result is cached
    /// internally and reused on the second call.
    pub fn compiled(&self) -> &CompiledPiiConfig {
        self.compiled.get_or_init(|| self.compiled_uncached())
    }

    /// Like [`compiled`](Self::compiled) but without internal caching.
    #[inline]
    pub fn compiled_uncached(&self) -> CompiledPiiConfig {
        CompiledPiiConfig::new(self)
    }
}
