use std::borrow::Cow;
use std::fmt;

use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Deserializer, Serialize};

use crate::protocol::OperationType;
use crate::utils::Glob;

/// Wrapper type around the raw string pattern and the [`crate::utils::Glob`].
///
/// This allows to compile the Glob with internal regexes only then whent they are used.
#[derive(Clone, Eq, PartialEq)]
pub struct LazyGlob {
    raw: String,
    glob: OnceCell<Glob>,
}

impl LazyGlob {
    /// Create a new [`LazyGlob`] from the raw string.
    pub fn new(raw: String) -> Self {
        Self {
            raw,
            glob: OnceCell::new(),
        }
    }

    /// Returns the compiled version of the [`crate::utils::Glob`].
    pub fn compiled(&self) -> &Glob {
        self.glob.get_or_init(|| {
            Glob::builder(&self.raw)
                .capture_star(true)
                .capture_double_star(false)
                .capture_question_mark(false)
                .build()
        })
    }
}

impl fmt::Debug for LazyGlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LazyGlob({:?})", self.raw)
    }
}

impl Serialize for LazyGlob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for LazyGlob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(LazyGlob::new)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct SpanDescriptionRuleScope {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub op: OperationType,
}

/// Default value for substitution in [`RedactionRule`].
fn default_substitution() -> String {
    "*".to_string()
}

/// Describes what to do with the matched pattern.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum RedactionRule {
    Replace {
        #[serde(default = "default_substitution")]
        substitution: String,
    },
    #[serde(other)]
    Unknown,
}

impl Default for RedactionRule {
    fn default() -> Self {
        Self::Replace {
            substitution: default_substitution(),
        }
    }
}

/// The rule describes how span descriptions should be changed.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpanDescriptionRule {
    /// The pattern which will be applied to the span description.
    pub pattern: LazyGlob,
    /// Date time when the rule expires and it should not be applied anymore.
    pub expiry: DateTime<Utc>,
    /// Object containing transaction attributes the rules must only be applied to.
    pub scope: SpanDescriptionRuleScope,
    /// Object describing what to do with the matched pattern.
    pub redaction: RedactionRule,
}

impl From<&TransactionNameRule> for SpanDescriptionRule {
    fn from(value: &TransactionNameRule) -> Self {
        SpanDescriptionRule {
            pattern: LazyGlob::new(format!("**{}", value.pattern.raw)),
            expiry: value.expiry,
            scope: SpanDescriptionRuleScope::default(),
            redaction: value.redaction.clone(),
        }
    }
}

impl SpanDescriptionRule {
    /// Applies the span description rule to the given string, if it matches the pattern.
    ///
    /// TODO(iker): we should check the rule's domain, similar to transaction name rules.
    pub fn match_and_apply(&self, mut string: Cow<String>) -> Option<String> {
        let slash_is_present = string.ends_with('/');
        if !slash_is_present {
            string.to_mut().push('/');
        }
        let is_matched = self.matches(&string);

        if is_matched {
            let mut result = self.apply(&string);
            if !slash_is_present {
                result.pop();
            }
            Some(result)
        } else {
            None
        }
    }

    /// Returns `true` if the rule isn't expired yet and its pattern matches the given string.
    fn matches(&self, string: &str) -> bool {
        let now = Utc::now();
        self.expiry > now && self.pattern.compiled().is_match(string)
    }

    /// Applies the rule to the provided value.
    fn apply(&self, value: &str) -> String {
        match &self.redaction {
            RedactionRule::Replace { substitution } => self
                .pattern
                .compiled()
                .replace_captures(value, substitution),
            _ => {
                relay_log::trace!("Replacement rule type is unsupported!");
                value.to_owned()
            }
        }
    }
}

/// The rule describes how transaction name should be changed.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TransactionNameRule {
    /// The pattern which will be applied to transaction name.
    pub pattern: LazyGlob,
    /// Date time when the rule expires and it should not be applied anymore.
    pub expiry: DateTime<Utc>,
    /// Object describing what to do with the matched pattern.
    pub redaction: RedactionRule,
}

impl TransactionNameRule {
    /// Checks is the current rule matches and tries to apply it.
    pub fn match_and_apply(&self, mut transaction: Cow<String>) -> Option<String> {
        let slash_is_present = transaction.ends_with('/');
        if !slash_is_present {
            transaction.to_mut().push('/');
        }
        let is_matched = self.matches(&transaction);

        if is_matched {
            let mut result = self.apply(&transaction);
            if !slash_is_present {
                result.pop();
            }
            Some(result)
        } else {
            None
        }
    }

    /// Applies the rule to the provided value.
    ///
    /// Note: currently only `url` source for rules supported.
    fn apply(&self, value: &str) -> String {
        match &self.redaction {
            RedactionRule::Replace { substitution } => self
                .pattern
                .compiled()
                .replace_captures(value, substitution),
            _ => {
                relay_log::trace!("Replacement rule type is unsupported!");
                value.to_owned()
            }
        }
    }

    /// Returns `true` if the current rule pattern matches transaction, expected transaction
    /// source, and not expired yet.
    fn matches(&self, transaction: &str) -> bool {
        self.expiry > Utc::now() && self.pattern.compiled().is_match(transaction)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn test_rule_format() {
        let json = r#"
        {
          "pattern": "/auth/login/*/**",
          "expiry": "2022-11-30T00:00:00.000000Z",
          "scope": {
            "source": "url"
          },
          "redaction": {
            "method": "replace",
            "substitution": ":id"
          }
        }
        "#;

        let rule: TransactionNameRule = serde_json::from_str(json).unwrap();

        let parsed_time = DateTime::parse_from_rfc3339("2022-11-30T00:00:00Z").unwrap();
        let result = TransactionNameRule {
            pattern: LazyGlob::new("/auth/login/*/**".to_string()),
            expiry: DateTime::from_utc(parsed_time.naive_utc(), Utc),
            redaction: RedactionRule::Replace {
                substitution: String::from(":id"),
            },
        };

        assert_eq!(rule, result);
    }

    #[test]
    fn test_rule_format_defaults() {
        let json = r#"
        {
          "pattern": "/auth/login/*/**",
          "expiry": "2022-11-30T00:00:00.000000Z",
          "redaction": {
            "method": "replace"
          }
        }
        "#;

        let rule: TransactionNameRule = serde_json::from_str(json).unwrap();

        let parsed_time = DateTime::parse_from_rfc3339("2022-11-30T00:00:00Z").unwrap();
        let result = TransactionNameRule {
            pattern: LazyGlob::new("/auth/login/*/**".to_string()),
            expiry: DateTime::from_utc(parsed_time.naive_utc(), Utc),
            redaction: RedactionRule::Replace {
                substitution: default_substitution(),
            },
        };

        assert_eq!(rule, result);
    }

    #[test]
    fn test_rule_format_unsupported_reduction() {
        let json = r#"
        {
          "pattern": "/auth/login/*/**",
          "expiry": "2022-11-30T00:00:00.000000Z",
          "redaction": {
            "method": "update"
          }
        }
        "#;

        let rule: TransactionNameRule = serde_json::from_str(json).unwrap();
        let result = rule.apply("/auth/login/test/");

        assert_eq!(result, "/auth/login/test/".to_string());
    }

    #[test]
    fn test_rule_format_roundtrip() {
        let json = r#"{
  "pattern": "/auth/login/*/**",
  "expiry": "2022-11-30T00:00:00Z",
  "redaction": {
    "method": "replace",
    "substitution": ":id"
  }
}"#;

        let rule: TransactionNameRule = serde_json::from_str(json).unwrap();
        let rule_json = serde_json::to_string_pretty(&rule).unwrap();
        // Make sure that we can  serialize into the same format we receive from the wire.
        assert_eq!(json, rule_json);
    }
}
