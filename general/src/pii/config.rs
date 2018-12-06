use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::pii::builtin::BUILTIN_RULES_MAP;
use crate::pii::rules::{Rule, RuleSpec};
use crate::processor::PiiKind;

/// Common config vars.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Vars {
    /// The default secret key for hashing operations.
    pub hash_key: Option<String>,
}

/// A set of named rule configurations.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct PiiConfig {
    /// A map of rules to apply.
    #[serde(default)]
    pub rules: BTreeMap<String, RuleSpec>,
    /// Variables that should become available to the pii ruls.
    #[serde(default)]
    pub vars: Vars,
    /// kind to rule applications.
    #[serde(default)]
    pub applications: BTreeMap<PiiKind, Vec<String>>,
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

    /// Looks up a rule in the PII config.
    pub fn lookup_rule<'a>(&'a self, rule_id: &'a str) -> Option<Rule<'a>> {
        if let Some(rule_spec) = self.rules.get(rule_id) {
            Some(Rule {
                id: rule_id,
                spec: rule_spec,
                cfg: self,
            })
        } else if let Some(rule_spec) = BUILTIN_RULES_MAP.get(rule_id) {
            Some(Rule {
                id: rule_id,
                spec: rule_spec,
                cfg: self,
            })
        } else {
            None
        }
    }
}
