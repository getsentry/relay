//! Span tree normalization.

use std::collections::BTreeSet;
use std::mem;

use relay_event_schema::protocol::{Event, TraceContext};
use relay_protocol::Error;

/// Enforce that every span has a valid parent. If any `parent_span_id` is pointing nowhere, the
/// span is re-parented onto the root span.
///
/// This is to avoid any nasty surprises in the span buffer specifically. Other readers of spans
/// (such as the frontend's tree component) were already able to deal with detached spans.
pub fn fix_trees(event: &mut Event) {
    let Some(ref mut spans) = event.spans.value_mut() else {
        return;
    };

    let Some(contexts) = event.contexts.value_mut() else {
        return;
    };

    let Some(trace_context) = contexts.get_mut::<TraceContext>() else {
        return;
    };

    let Some(root_span_id) = trace_context.span_id.value() else {
        return;
    };

    let valid_span_ids = spans
        .iter()
        .filter_map(|span| span.value())
        .filter_map(|span| span.span_id.value())
        .chain(Some(root_span_id))
        .cloned()
        .collect::<BTreeSet<_>>();

    for span in spans {
        let Some(span) = span.value_mut() else {
            continue;
        };

        let Some(parent_span_id) = span.parent_span_id.value_mut() else {
            continue;
        };

        if valid_span_ids.contains(&*parent_span_id) {
            continue;
        };

        let invalid_parent = mem::replace(parent_span_id, root_span_id.clone());
        span.parent_span_id
            .meta_mut()
            .add_error(Error::invalid(format!(
                "Span ID does not exist: {}",
                invalid_parent
            )));
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::Span;
    use relay_protocol::FromValue;
    use relay_protocol::SerializableAnnotated;

    use super::*;

    #[test]
    fn basic() {
        let mut data = Event::from_value(
            serde_json::json!({
                "type": "transaction",
                "start_timestamp": 1609455600,
                "end_timestamp": 1609455605,
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "aaaaaaaaaaaaaaaa",
                        "parent_span_id": "ffffffffffffffff",
                    }
                },
                "spans": [
                    {
                        "span_id": "bbbbbbbbbbbbbbbb",
                        "parent_span_id": "aaaaaaaaaaaaaaaa"
                    },
                    {
                        "span_id": "bbbbbbbbbbbbbbbb",
                        "parent_span_id": "dddddddddddddddd",
                    },
                    {
                        "span_id": "bbbbbbbbbbbbbbbb",
                        "parent_span_id": "eeeeeeeeeeeeeeee",
                    },
                ],
            })
            .into(),
        );

        fix_trees(data.value_mut().as_mut().unwrap());

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "type": "transaction",
          "start_timestamp": 1609455600.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "aaaaaaaaaaaaaaaa",
              "parent_span_id": "ffffffffffffffff",
              "type": "trace"
            }
          },
          "spans": [
            {
              "span_id": "bbbbbbbbbbbbbbbb",
              "parent_span_id": "aaaaaaaaaaaaaaaa"
            },
            {
              "span_id": "bbbbbbbbbbbbbbbb",
              "parent_span_id": "aaaaaaaaaaaaaaaa"
            },
            {
              "span_id": "bbbbbbbbbbbbbbbb",
              "parent_span_id": "aaaaaaaaaaaaaaaa"
            }
          ],
          "end_timestamp": 1609455605,
          "_meta": {
            "spans": {
              "1": {
                "parent_span_id": {
                  "": {
                    "err": [
                      [
                        "invalid_data",
                        {
                          "reason": "Span ID does not exist: dddddddddddddddd"
                        }
                      ]
                    ]
                  }
                }
              },
              "2": {
                "parent_span_id": {
                  "": {
                    "err": [
                      [
                        "invalid_data",
                        {
                          "reason": "Span ID does not exist: eeeeeeeeeeeeeeee"
                        }
                      ]
                    ]
                  }
                }
              }
            }
          }
        }
        "###);
    }
}
