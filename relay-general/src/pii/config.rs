use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Deref;

use once_cell::sync::OnceCell;
use regex::{Regex, RegexBuilder};
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

use crate::pii::{CompiledPiiConfig, Redaction};
use crate::processor::SelectorSpec;

/// A regex pattern for text replacement.
#[derive(Clone)]
pub struct Pattern(Regex);

impl Pattern {
    pub fn new(s: &str, case_insensitive: bool) -> Result<Self, regex::Error> {
        let regex = RegexBuilder::new(s)
            .size_limit(262_144)
            .case_insensitive(case_insensitive)
            .build()?;

        Ok(Self(regex))
    }

    pub fn regex(&self) -> &Regex {
        &self.0
    }
}

impl Deref for Pattern {
    type Target = Regex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&'static str> for Pattern {
    fn from(pattern: &'static str) -> Pattern {
        Pattern(Regex::new(pattern).unwrap())
    }
}

impl fmt::Debug for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl Serialize for Pattern {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Pattern {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        let pattern = Pattern::new(&raw, false).map_err(Error::custom)?;
        Ok(pattern)
    }
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Pattern) -> bool {
        // unclear if we could derive Eq as well, but better not. We don't need it.
        self.0.as_str() == other.0.as_str()
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
    pub pattern: Pattern,
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
    #[serde(default)]
    pub hide_inner: bool,
}

/// An alias for another rule.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AliasRule {
    /// A reference to another rule to apply.
    pub rule: String,
    /// When set to true, the outer rule is reported.
    #[serde(default)]
    pub hide_inner: bool,
}

/// A pair redaction rule.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RedactPairRule {
    /// A pattern to match for keys.
    pub key_pattern: Pattern,
}

/// Supported stripping rules.
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
}

/// A single rule configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RuleSpec {
    #[serde(flatten)]
    pub ty: RuleType,
    #[serde(default)]
    pub redaction: Redaction,
}

/// Configuration for rule parameters.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Vars {
    /// The default secret key for hashing operations.
    #[serde(default)]
    pub hash_key: Option<String>,
}

/// A set of named rule configurations.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct PiiConfig {
    /// A map of custom PII rules.
    #[serde(default)]
    pub rules: BTreeMap<String, RuleSpec>,

    /// Parameters for PII rules.
    #[serde(default)]
    pub vars: Vars,

    /// Mapping of selectors to rules.
    #[serde(default)]
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
    /// Loads a PII config from a JSON string.
    pub fn from_json(s: &str) -> Result<PiiConfig, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Serializes an annotated value into a JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }

    /// Serializes an annotated value into a pretty JSON string.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self)
    }

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
