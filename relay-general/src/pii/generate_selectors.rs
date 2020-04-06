use std::collections::BTreeSet;

use crate::pii::utils::process_pairlist;
use crate::processor::{
    process_value, Pii, ProcessValue, ProcessingState, Processor, SelectorPathItem, SelectorSpec,
    ValueType,
};
use crate::protocol::{AsPair, PairList};
use crate::types::{Annotated, Meta, ProcessingResult};

struct GenerateSelectorsProcessor {
    paths: BTreeSet<SelectorSpec>,
}

impl Processor for GenerateSelectorsProcessor {
    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // The following skip-conditions are in sync with what the PiiProcessor does.
        if state.value_type() == Some(ValueType::Boolean)
            || value.is_none()
            || state.attrs().pii == Pii::False
        {
            return Ok(());
        }

        let mut insert_path = |path: SelectorSpec| {
            if state.attrs().pii != Pii::Maybe || path.is_specific() {
                self.paths.insert(path);
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

            match substate.value_type() {
                // $array.0.foo and $object.bar are not particularly good suggestions.
                Some(ValueType::Object) | Some(ValueType::Array) => {}

                // a.b.c.$string is not a good suggestion, so special case those.
                ty @ Some(ValueType::String)
                | ty @ Some(ValueType::Number)
                | ty @ Some(ValueType::Boolean)
                | ty @ Some(ValueType::DateTime) => {
                    insert_path(SelectorSpec::Path(vec![SelectorPathItem::Type(
                        ty.unwrap(),
                    )]));
                }

                Some(ty) => {
                    let mut path = path.clone();
                    path.push(SelectorPathItem::Type(ty));
                    path.reverse();
                    if insert_path(SelectorSpec::Path(path)) {
                        // If we managed to generate $http.header.Authorization, we do not want to
                        // generate request.headers.Authorization as well.
                        return Ok(());
                    }
                }

                None => {}
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
pub fn selectors_from_value<T: ProcessValue>(value: &mut Annotated<T>) -> BTreeSet<SelectorSpec> {
    let mut processor = GenerateSelectorsProcessor {
        paths: BTreeSet::new(),
    };

    process_value(value, &mut processor, ProcessingState::root())
        .expect("This processor is supposed to be infallible");

    processor.paths
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

        let selectors = selectors_from_value(&mut event);
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

        let selectors = selectors_from_value(&mut event);
        insta::assert_yaml_snapshot!(selectors, @r###"
        ---
        - $string
        - $frame.abs_path
        - $frame.filename
        - $frame.vars
        - $frame.vars.bam
        - $frame.vars.foo
        - $http.headers
        - $http.headers.Authorization
        - $message
        - extra
        - "extra.'My Custom Value'"
        "###);
    }
}
