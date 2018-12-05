use std::collections::BTreeMap;

use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::pii::builtin::BUILTIN_RULES_MAP;
use crate::pii::rules::{Rule, RuleSpec};
use crate::pii::text::apply_rule_to_chunks;
use crate::processor::{
    process_chunked_value, process_value, PiiKind, ProcessValue, ProcessingState, Processor,
};
use crate::types::{Annotated, Meta, ValueAction};

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

/// A processor that performs PII stripping.
pub struct PiiProcessor<'a> {
    config: &'a PiiConfig,
    applications: BTreeMap<PiiKind, Vec<Rule<'a>>>,
}

impl<'a> PiiProcessor<'a> {
    /// Creates a new processor based on a config.
    pub fn new(config: &'a PiiConfig) -> PiiProcessor {
        let mut applications = BTreeMap::new();

        for (&pii_kind, cfg_applications) in &config.applications {
            let mut rules = vec![];
            for application in cfg_applications {
                // XXX: log bad rule reference here
                if let Some(rule) = config.lookup_rule(application.as_str()) {
                    rules.push(rule);
                }
            }
            applications.insert(pii_kind, rules);
        }

        PiiProcessor {
            config,
            applications,
        }
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &PiiConfig {
        self.config
    }

    /// Shortcut to process a root value.
    pub fn process_root_value<T: ProcessValue>(&mut self, mut value: Annotated<T>) -> Annotated<T> {
        process_value(&mut value, self, ProcessingState::default());
        value
    }
}

impl<'a> Processor for PiiProcessor<'a> {
    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: ProcessingState,
    ) -> ValueAction {
        if let Some(pii_kind) = state.attrs().pii_kind {
            if let Some(rules) = self.applications.get(&pii_kind) {
                process_chunked_value(value, meta, |mut chunks| {
                    for rule in rules {
                        chunks = apply_rule_to_chunks(rule, chunks);
                    }
                    chunks
                });
            }
        }
        ValueAction::Keep
    }
}
