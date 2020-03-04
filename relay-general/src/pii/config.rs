use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Deref;

use regex::{Regex, RegexBuilder};
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

use crate::pii::builtin::BUILTIN_RULES_MAP;
use crate::pii::Redaction;
use crate::processor::SelectorSpec;

/// A regex pattern for text replacement.
#[derive(Clone)]
pub struct Pattern(pub Regex);

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
        let pattern = RegexBuilder::new(&raw)
            .size_limit(262_144)
            .build()
            .map_err(Error::custom)?;
        Ok(Pattern(pattern))
    }
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Pattern) -> bool {
        // unclear if we could derive Eq as well, but better not. We don't need it.
        self.0.as_str() == other.0.as_str()
    }
}

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
#[derive(Serialize, Debug, Clone, PartialEq)]
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
    /// When a regex matches a key, a value is removed
    RedactPair(RedactPairRule),
    /// Applies multiple rules.
    Multiple(MultipleRule),
    /// Applies another rule.  Works like a single multiple.
    Alias(AliasRule),
}

impl<'de> Deserialize<'de> for RuleType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum RuleTypeWithLegacy {
            Anything,
            Pattern(PatternRule),
            Imei,
            Mac,
            Uuid,
            Email,
            Ip,
            Creditcard,
            Userpath,
            Pemkey,
            UrlAuth,
            UsSsn,
            RedactPair(RedactPairRule),
            #[serde(rename = "redactPair")]
            RedactPairLegacy(RedactPairRule),
            Multiple(MultipleRule),
            Alias(AliasRule),
        }

        Ok(match RuleTypeWithLegacy::deserialize(deserializer)? {
            RuleTypeWithLegacy::Anything => RuleType::Anything,
            RuleTypeWithLegacy::Pattern(r) => RuleType::Pattern(r),
            RuleTypeWithLegacy::Imei => RuleType::Imei,
            RuleTypeWithLegacy::Mac => RuleType::Mac,
            RuleTypeWithLegacy::Uuid => RuleType::Uuid,
            RuleTypeWithLegacy::Email => RuleType::Email,
            RuleTypeWithLegacy::Ip => RuleType::Ip,
            RuleTypeWithLegacy::Creditcard => RuleType::Creditcard,
            RuleTypeWithLegacy::Userpath => RuleType::Userpath,
            RuleTypeWithLegacy::Pemkey => RuleType::Pemkey,
            RuleTypeWithLegacy::UrlAuth => RuleType::UrlAuth,
            RuleTypeWithLegacy::UsSsn => RuleType::UsSsn,
            RuleTypeWithLegacy::RedactPair(r) => RuleType::RedactPair(r),
            RuleTypeWithLegacy::RedactPairLegacy(r) => RuleType::RedactPair(r),
            RuleTypeWithLegacy::Multiple(r) => RuleType::Multiple(r),
            RuleTypeWithLegacy::Alias(r) => RuleType::Alias(r),
        })
    }
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
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
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

    pub(crate) fn rule<'a>(&'a self, id: &'a str) -> Option<RuleRef<'a>> {
        if let Some(spec) = self.rules.get(id) {
            Some(RuleRef::new(self, id, spec))
        } else if let Some(spec) = BUILTIN_RULES_MAP.get(id) {
            Some(RuleRef::new(self, id, spec))
        } else {
            None
        }
    }
}

/// Reference to a PII rule.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RuleRef<'a> {
    pub config: &'a PiiConfig,
    pub id: &'a str,
    pub origin: &'a str,
    pub ty: &'a RuleType,
    pub redaction: &'a Redaction,
}

impl<'a> RuleRef<'a> {
    fn new(config: &'a PiiConfig, id: &'a str, spec: &'a RuleSpec) -> Self {
        RuleRef {
            config,
            id,
            origin: id,
            ty: &spec.ty,
            redaction: &spec.redaction,
        }
    }

    pub fn for_parent(self, parent: Self) -> Self {
        RuleRef {
            config: self.config,
            id: self.id,
            origin: parent.origin,
            ty: self.ty,
            redaction: match parent.redaction {
                Redaction::Default => self.redaction,
                _ => parent.redaction,
            },
        }
    }
}

impl PartialEq for RuleRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RuleRef<'_> {}

impl PartialOrd for RuleRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(other.id)
    }
}

impl Ord for RuleRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(other.id)
    }
}
