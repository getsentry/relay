//! Matching rules.
use std::collections::BTreeSet;
use std::fmt;

use regex::{Regex, RegexBuilder};
use serde::{Serialize, Serializer, Deserialize, Deserializer, de::Error};

use crate::pii::config::PiiConfig;
use crate::pii::redactions::Redaction;

/// A regex pattern for text replacement.
#[derive(Clone)]
pub struct Pattern(pub Regex);

impl From<&'static str> for Pattern {
    fn from(pattern: &'static str) -> Pattern {
        Pattern(Regex::new(pattern).unwrap())
    }
}

impl fmt::Debug for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

/// A rule that matches a regex pattern.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PatternRule {
    /// The regular expression to apply.
    pub pattern: Pattern,
    /// The match group indices to replace.
    pub replace_groups: Option<BTreeSet<u8>>,
}

/// A rule that dispatches to multiple other rules.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MultipleRule {
    /// A reference to other rules to apply
    pub rules: Vec<String>,
    /// When set to true, the outer rule is reported.
    #[serde(default)]
    pub hide_rule: bool,
}

/// An alias for another rule.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AliasRule {
    /// A reference to another rule to apply.
    pub rule: String,
    /// When set to true, the outer rule is reported.
    #[serde(default)]
    pub hide_rule: bool,
}

/// A pair redaction rule.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RedactPairRule {
    /// A pattern to match for keys.
    pub key_pattern: Pattern,
}

/// Supported stripping rules.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum RuleType {
    /// Matches any value.
    Anything,
    /// Applies a regular expression.
    Pattern(PatternRule),
    /// Matchse an IMEI or IMEISV
    Imei,
    /// Matches a mac address
    Mac,
    /// Matches an email
    Email,
    /// Matches any IP address
    Ip,
    /// Matches a creditcard number
    Creditcard,
    /// Sanitizes a path from user data
    Userpath,
    /// When a regex matches a key, a value is removed
    RedactPair(RedactPairRule),
    /// Applies multiple rules.
    Multiple(MultipleRule),
    /// Applies another rule.  Works like a single multiple.
    Alias(AliasRule),
}

/// A single rule configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuleSpec {
    #[serde(flatten)]
    pub ty: RuleType,
    #[serde(default)]
    pub redaction: Redaction,
}

/// A rule is a rule config plus id.
#[derive(Debug, Clone)]
pub struct Rule<'a> {
    pub id: &'a str,
    pub spec: &'a RuleSpec,
    pub cfg: &'a PiiConfig,
}

impl<'a> Rule<'a> {
    pub fn lookup_referenced_rule(
        &'a self,
        rule_id: &'a str,
        hide_rule: bool,
    ) -> Option<(Rule, Option<&'a Rule>, Option<&'a Redaction>)> {
        if let Some(rule) = self.cfg.lookup_rule(rule_id) {
            let report_rule = if hide_rule { Some(self) } else { None };
            let redaction_override = match self.spec.redaction {
                Redaction::Default => None,
                ref red => Some(red),
            };
            Some((rule, report_rule, redaction_override))
        } else {
            // XXX: handle bad references here?
            None
        }
    }
}
