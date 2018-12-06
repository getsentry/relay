use std::collections::BTreeMap;

use crate::pii::config::PiiConfig;
use crate::pii::databag::apply_rule_to_databag_value;
use crate::pii::rules::Rule;
use crate::pii::text::apply_rule_to_chunks;
use crate::processor::{process_chunked_value, PiiKind, ProcessValue, ProcessingState, Processor};
use crate::protocol::PairList;
use crate::types::{Annotated, Meta, ValueAction};

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

    fn process_pairlist<T: ProcessValue>(
        &mut self,
        value: &mut PairList<T>,
        _meta: &mut Meta,
        state: ProcessingState,
    ) -> ValueAction {
        if let Some(pii_kind) = state.attrs().pii_kind {
            if let Some(rules) = self.applications.get(&pii_kind) {
                for pair in value.iter_mut() {
                    for rule in rules {
                        if let Some((Annotated(ref k, _), ref mut v)) = pair.0 {
                            v.apply(|v, m| {
                                apply_rule_to_databag_value(
                                    rule,
                                    k.as_ref().map(|x| x.as_str()),
                                    v,
                                    m,
                                )
                            });
                        }
                    }
                }
            }
        }

        value.process_child_values(self, state);
        ValueAction::Keep
    }
}

#[cfg(test)]
use {
    crate::processor::process_value,
    crate::protocol::{Event, Headers, Request},
};

#[test]
fn test_basic_stripping() {
    let config = PiiConfig::from_json(
        r#"
        {
            "rules": {
                "remove_bad_headers": {
                    "type": "redactPair",
                    "keyPattern": "(?i)cookie"
                }
            },
            "applications": {
                "freeform": ["@ip"],
                "databag": ["remove_bad_headers"]
            }
        }
    "#,
    ).unwrap();

    let mut event = Annotated::new(Event {
        request: Annotated::new(Request {
            headers: {
                let mut rv = Vec::new();
                rv.push(Annotated::new((
                    Annotated::new("Cookie".to_string()),
                    Annotated::new("super secret".into()),
                )));
                rv.push(Annotated::new((
                    Annotated::new("X-Forwarded-For".to_string()),
                    Annotated::new("127.0.0.1".into()),
                )));
                Annotated::new(Headers(PairList(rv)))
            },
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut processor = PiiProcessor::new(&config);
    process_value(&mut event, &mut processor, Default::default());

    let req = event.0.unwrap().request;
    assert_eq!(req.to_json_pretty().unwrap(), r#"{
  "headers": [
    [
      "Cookie",
      null
    ],
    [
      "X-Forwarded-For",
      "[ip]"
    ]
  ],
  "_meta": {
    "headers": {
      "0": {
        "1": {
          "": {
            "rem": [
              [
                "remove_bad_headers",
                "x"
              ]
            ]
          }
        }
      },
      "1": {
        "1": {
          "": {
            "rem": [
              [
                "@ip:replace",
                "s",
                0,
                4
              ]
            ],
            "len": 9
          }
        }
      }
    }
  }
}"#);
}
