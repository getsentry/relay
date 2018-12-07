use std::collections::BTreeMap;

use crate::pii::config::PiiConfig;
use crate::pii::databag::apply_rule_to_databag_value;
use crate::pii::rules::Rule;
use crate::pii::text::apply_rule_to_chunks;
use crate::processor::{process_chunked_value, PiiKind, ProcessValue, ProcessingState, Processor};
use crate::protocol::{AsPair, PairList};
use crate::types::{Meta, Object, ValueAction};

/// A processor that performs PII stripping.
pub struct PiiProcessor<'a> {
    config: &'a PiiConfig,
    applications: BTreeMap<PiiKind, Vec<Rule<'a>>>,
}

impl<'a> PiiProcessor<'a> {
    /// Creates a new processor based on a config.
    pub fn new(config: &'a PiiConfig) -> PiiProcessor<'_> {
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

    /// Get the rules to apply for a list.
    pub fn get_rules(&self, state: &ProcessingState, kind: PiiKind) -> &[Rule<'_>] {
        if state.attrs().pii {
            self.applications.get(&kind).map(|x| x.as_slice())
        } else {
            None
        }
        .unwrap_or_default()
    }
}

impl<'a> Processor for PiiProcessor<'a> {
    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: ProcessingState<'_>,
    ) -> ValueAction {
        let rules = self.get_rules(&state, PiiKind::Text);
        if !rules.is_empty() {
            process_chunked_value(value, meta, |mut chunks| {
                for rule in rules {
                    chunks = apply_rule_to_chunks(rule, chunks);
                }
                chunks
            });
        }
        ValueAction::Keep
    }

    fn process_object<T: ProcessValue>(
        &mut self,
        value: &mut Object<T>,
        _meta: &mut Meta,
        state: ProcessingState,
    ) -> ValueAction {
        let rules = self.get_rules(&state, PiiKind::Container);
        if !rules.is_empty() {
            for (k, v) in value.iter_mut() {
                for rule in rules {
                    v.apply(|v, m| apply_rule_to_databag_value(rule, Some(k.as_str()), v, m));
                }
            }
        }

        value.process_child_values(self, state);
        ValueAction::Keep
    }

    fn process_pairlist<T: ProcessValue + AsPair>(
        &mut self,
        value: &mut PairList<T>,
        _meta: &mut Meta,
        state: ProcessingState,
    ) -> ValueAction {
        let rules = self.get_rules(&state, PiiKind::Container);
        if !rules.is_empty() {
            for pair in value.iter_mut() {
                for rule in rules {
                    if let Some(ref mut pair) = pair.value_mut() {
                        let (ref mut k, ref mut v) = pair.as_pair();
                        v.apply(|v, m| {
                            apply_rule_to_databag_value(
                                rule,
                                k.value().as_ref().map(|x| x.as_str()),
                                v,
                                m,
                            )
                        });
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
    crate::protocol::{Event, Headers, LogEntry, Request},
    crate::types::{Annotated, Value},
};

#[test]
fn test_basic_stripping() {
    use crate::protocol::{TagEntry, Tags};
    let config = PiiConfig::from_json(
        r#"
        {
            "rules": {
                "remove_bad_headers": {
                    "type": "redactPair",
                    "keyPattern": "(?i)cookie|secret[-_]?key"
                }
            },
            "applications": {
                "text": ["@ip"],
                "container": ["remove_bad_headers"]
            }
        }
    "#,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        logentry: Annotated::new(LogEntry {
            formatted: Annotated::new("Hello from 127.0.0.1!".to_string()),
            ..Default::default()
        }),
        request: Annotated::new(Request {
            env: {
                let mut rv = Object::new();
                rv.insert(
                    "SECRET_KEY".to_string(),
                    Annotated::new(Value::String("134141231231231231231312".into())),
                );
                Annotated::new(rv)
            },
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
        tags: Annotated::new(Tags(
            vec![Annotated::new(TagEntry(
                Annotated::new("forwarded_for".to_string()),
                Annotated::new("127.0.0.1".to_string()),
            ))]
            .into(),
        )),
        ..Default::default()
    });

    let mut processor = PiiProcessor::new(&config);
    process_value(&mut event, &mut processor, Default::default());

    assert_eq_str!(
        event.to_json_pretty().unwrap(),
        r#"{
  "logentry": {
    "formatted": "Hello from [ip]!"
  },
  "request": {
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
    "env": {
      "SECRET_KEY": null
    }
  },
  "tags": [
    [
      "forwarded_for",
      "[ip]"
    ]
  ],
  "_meta": {
    "logentry": {
      "formatted": {
        "": {
          "rem": [
            [
              "@ip:replace",
              "s",
              11,
              15
            ]
          ],
          "len": 21
        }
      }
    },
    "request": {
      "env": {
        "SECRET_KEY": {
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
    },
    "tags": {
      "0": {
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
}"#
    );
}
