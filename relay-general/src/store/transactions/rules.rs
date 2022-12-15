use chrono::{DateTime, Utc};
use relay_common::Glob;
use serde::{Deserialize, Deserializer, Serialize};

use crate::protocol::TransactionSource;

/// Helper function to deserialize the string patter into the [`relay_common::Glob`].
fn deserialize_glob_pattern<'de, D>(deserializer: D) -> Result<Glob, D::Error>
where
    D: Deserializer<'de>,
{
    let pattern = String::deserialize(deserializer)?;
    let glob = Glob::builder(&pattern)
        .capture_star(true)
        .capture_double_star(false)
        .capture_question_mark(false)
        .build();
    Ok(glob)
}

/// Contains transaction attribute the rule must only be applied to.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct RuleScope {
    /// The source of the transaction.
    pub source: TransactionSource,
}

impl Default for RuleScope {
    fn default() -> Self {
        Self {
            source: TransactionSource::Url,
        }
    }
}

/// Describes what to do with the matched pattern.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum RedactionRule {
    Replace(Replace),
    Other,
}

impl Default for RedactionRule {
    fn default() -> Self {
        Self::Replace(Replace {
            substitution: default_substitution(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Replace {
    #[serde(default = "default_substitution")]
    substitution: String,
}

fn default_substitution() -> String {
    "*".to_string()
}

/// The rule describes how transaction name should be changed.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TransactionNameRule {
    /// The pattern which will be applied to transaction name.
    #[serde(deserialize_with = "deserialize_glob_pattern")]
    pub pattern: Glob,
    /// Date time when the rule expires and it should not be applied anymore.
    pub expiry: DateTime<Utc>,
    /// Object containing transaction attributes the rules must only be applied to.
    #[serde(default)]
    pub scope: RuleScope,
    /// Object describing what to do with the matched pattern.
    #[serde(default)]
    pub redaction: RedactionRule,
}

impl TransactionNameRule {
    /// Applies the rule to the provided value.
    ///
    /// Note: currently only `url` source for rules supported.
    pub fn apply(&self, value: &str) -> Option<String> {
        match &self.redaction {
            RedactionRule::Replace(Replace { substitution }) => {
                let result = self.pattern.apply(value, substitution);
                Some(result)
            }
            other => {
                relay_log::error!("Replacement rule {:?} unsupported!", other);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn test_rule_format() {
        let json = r###"
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
        "###;

        let rule: TransactionNameRule = serde_json::from_str(json).unwrap();

        let parsed_time = DateTime::parse_from_rfc3339("2022-11-30T00:00:00Z").unwrap();
        let result = TransactionNameRule {
            pattern: Glob::new("/auth/login/*/**"),
            expiry: DateTime::from_utc(parsed_time.naive_utc(), Utc),
            scope: Default::default(),
            redaction: RedactionRule::Replace(Replace {
                substitution: String::from(":id"),
            }),
        };

        assert_eq!(rule, result);
    }

    #[test]
    fn test_rule_check_defaults() {
        let json = r###"
        {
          "pattern": "/auth/login/*/**",
          "expiry": "2022-11-30T00:00:00.000000Z",
          "redaction": {
            "method": "replace"
          }
        }
        "###;

        let rule: TransactionNameRule = serde_json::from_str(json).unwrap();

        let parsed_time = DateTime::parse_from_rfc3339("2022-11-30T00:00:00Z").unwrap();
        let result = TransactionNameRule {
            pattern: Glob::new("/auth/login/*/**"),
            expiry: DateTime::from_utc(parsed_time.naive_utc(), Utc),
            scope: Default::default(),
            redaction: Default::default(),
        };

        assert_eq!(rule, result);
    }
}
