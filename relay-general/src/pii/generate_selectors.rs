use std::collections::BTreeSet;

use serde::Serialize;

use crate::pii::utils::process_pairlist;
use crate::processor::{
    process_value, Pii, ProcessValue, ProcessingState, Processor, SelectorPathItem, SelectorSpec,
    ValueType,
};
use crate::protocol::{AsPair, PairList};
use crate::types::{Annotated, Meta, ProcessingResult, Value};

/// Metadata about a selector found in the event
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct SelectorSuggestion {
    /// The selector that users should be able to use to address the underlying value
    pub path: SelectorSpec,
    /// The JSON-serialized value for previewing what the selector means.
    ///
    /// Right now this only contains string values.
    pub value: Option<String>,
}

struct GenerateSelectorsProcessor {
    selectors: BTreeSet<SelectorSuggestion>,
}

impl Processor for GenerateSelectorsProcessor {
    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // The following skip-conditions are in sync with what the PiiProcessor does.
        if state.value_type().contains(ValueType::Boolean)
            || value.is_none()
            || state.attrs().pii == Pii::False
        {
            return Ok(());
        }

        let mut insert_path = |path: SelectorSpec| {
            if state.attrs().pii != Pii::Maybe || path.is_specific() {
                let mut string_value = None;
                if let Some(value) = value {
                    if let Value::String(s) = value.clone().to_value() {
                        string_value = Some(s);
                    }
                }
                self.selectors.insert(SelectorSuggestion {
                    path,
                    value: string_value,
                });
                true
            } else {
                false
            }
        };

        let mut path = Vec::new();

        // Walk through processing state in reverse order and build selector path off of that.
        for substate in state.iter() {
            if !substate.entered_anything() {
                continue;
            }

            for value_type in substate.value_type() {
                match value_type {
                    // $array.0.foo and $object.bar are not particularly good suggestions.
                    ValueType::Object | ValueType::Array => {}

                    // a.b.c.$string is not a good suggestion, so special case those.
                    ty @ ValueType::String
                    | ty @ ValueType::Number
                    | ty @ ValueType::Boolean
                    | ty @ ValueType::DateTime => {
                        insert_path(SelectorSpec::Path(vec![SelectorPathItem::Type(ty)]));
                    }

                    ty => {
                        let mut path = path.clone();
                        path.push(SelectorPathItem::Type(ty));
                        path.reverse();
                        if insert_path(SelectorSpec::Path(path)) {
                            // If we managed to generate $http.header.Authorization, we do not want to
                            // generate request.headers.Authorization as well.
                            return Ok(());
                        }
                    }
                }
            }

            if let Some(key) = substate.path().key() {
                path.push(SelectorPathItem::Key(key.to_owned()));
            } else if substate.path().index().is_some() {
                path.push(SelectorPathItem::Wildcard);
            } else {
                debug_assert!(substate.depth() == 0);
                break;
            }
        }

        if !path.is_empty() {
            path.reverse();
            insert_path(SelectorSpec::Path(path));
        }

        Ok(())
    }

    fn process_pairlist<T: ProcessValue + AsPair>(
        &mut self,
        value: &mut PairList<T>,
        _meta: &mut Meta,
        state: &ProcessingState,
    ) -> ProcessingResult {
        process_pairlist(self, value, state)
    }
}

/// Walk through a value and collect selectors that can be applied to it in a PII config. This
/// function is used in the UI to provide auto-completion of selectors.
///
/// This generates a couple of duplicate suggestions such as `request.headers` and `$http.headers`
/// in order to make it more likely that the user input starting with either prefix can be
/// completed.
///
/// The main value in autocompletion is that we can complete `$http.headers.Authorization` as soon
/// as the user types `Auth`.
///
/// XXX: This function should not have to take a mutable ref, we only do that due to restrictions
/// on the Processor trait that we internally use to traverse the event.
pub fn selector_suggestions_from_value<T: ProcessValue>(
    value: &mut Annotated<T>,
) -> BTreeSet<SelectorSuggestion> {
    let mut processor = GenerateSelectorsProcessor {
        selectors: BTreeSet::new(),
    };

    process_value(value, &mut processor, ProcessingState::root())
        .expect("This processor is supposed to be infallible");

    processor.selectors
}

#[cfg(test)]
mod tests {
    use crate::protocol::Event;

    use super::*;

    #[test]
    fn test_empty() {
        // Test that an event without PII will generate empty list.
        let mut event =
            Annotated::<Event>::from_json(r#"{"logentry": {"message": "hi"}}"#).unwrap();

        let selectors = selector_suggestions_from_value(&mut event);
        insta::assert_yaml_snapshot!(selectors, @r###"
        ---
        []
        "###);
    }

    #[test]
    fn test_full() {
        let mut event = Annotated::<Event>::from_json(
            r##"
            {
              "message": "hi",
              "exception": {
                "values": [
                  {
                    "type": "ZeroDivisionError",
                    "value": "Divided by zero",
                    "stacktrace": {
                      "frames": [
                        {
                          "abs_path": "foo/bar/baz",
                          "filename": "baz",
                          "vars": {
                            "foo": "bar"
                          }
                        }
                      ]
                    }
                  },
                  {
                    "type": "BrokenException",
                    "value": "Something failed",
                    "stacktrace": {
                      "frames": [
                        {
                          "vars": {
                            "bam": "bar"
                          }
                        }
                      ]
                    }
                  }
                ]
              },
              "extra": {
                "My Custom Value": "123"
              },
              "request": {
                "headers": {
                  "Authorization": "not really"
                }
              }
            }
            "##,
        )
        .unwrap();

        let selectors = selector_suggestions_from_value(&mut event);
        insta::assert_yaml_snapshot!(selectors, @r###"
        ---
        - path: $string
          value: "123"
        - path: $string
          value: Divided by zero
        - path: $string
          value: Something failed
        - path: $string
          value: bar
        - path: $string
          value: hi
        - path: $string
          value: not really
        - path: $error.value
          value: Divided by zero
        - path: $error.value
          value: Something failed
        - path: $frame.abs_path
          value: foo/bar/baz
        - path: $frame.filename
          value: baz
        - path: $frame.vars
          value: ~
        - path: $frame.vars.bam
          value: bar
        - path: $frame.vars.foo
          value: bar
        - path: $http.headers
          value: ~
        - path: $http.headers.Authorization
          value: not really
        - path: $message
          value: hi
        - path: extra
          value: ~
        - path: "extra.'My Custom Value'"
          value: "123"
        "###);
    }
}
