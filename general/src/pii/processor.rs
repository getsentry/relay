use std::collections::BTreeMap;

use crate::pii::config::PiiConfig;
use crate::pii::rules::Rule;
use crate::pii::text::apply_rule_to_chunks;
use crate::processor::{process_chunked_value, PiiKind, ProcessingState, Processor};
use crate::types::{Meta, ValueAction};

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
