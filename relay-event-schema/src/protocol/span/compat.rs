use relay_conventions::{DESCRIPTION, PROFILE_ID, SEGMENT_ID};
use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, Object, Value};

use crate::protocol::{Attributes, EventId, SpanV2, Timestamp};

/// Temporary type that amends a SpansV2 span with fields needed by the sentry span consumer.
/// This can be removed once the consumer has been updated to use the new schema.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue)]
pub struct CompatSpan {
    #[metastructure(flatten)]
    pub span_v2: SpanV2,

    pub data: Annotated<Object<Value>>,
    pub description: Annotated<String>,
    pub duration_ms: Annotated<u64>,
    pub end_timestamp_precise: Annotated<Timestamp>,
    pub profile_id: Annotated<EventId>,
    pub segment_id: Annotated<String>,
    pub start_timestamp_ms: Annotated<u64>, // TODO: remove from kafka schema, no longer used in consumer
    pub start_timestamp_precise: Annotated<Timestamp>,

    #[metastructure(field = "_performance_issues_spans")]
    pub performance_issues_spans: Annotated<bool>, // TODO: add to Kafka schema?
}

impl TryFrom<SpanV2> for CompatSpan {
    type Error = uuid::Error;

    fn try_from(span_v2: SpanV2) -> Result<Self, uuid::Error> {
        let mut compat_span = CompatSpan {
            start_timestamp_precise: span_v2.start_timestamp.clone(),
            start_timestamp_ms: span_v2
                .start_timestamp
                .clone()
                .map_value(|ts| ts.0.timestamp_millis() as u64),
            end_timestamp_precise: span_v2.end_timestamp.clone(),
            ..Default::default()
        };

        if let (Some(start_timestamp), Some(end_timestamp)) = (
            span_v2.start_timestamp.value(),
            span_v2.end_timestamp.value(),
        ) {
            let delta = (*end_timestamp - *start_timestamp).num_milliseconds();
            compat_span.duration_ms = u64::try_from(delta).unwrap_or(0).into();
        }

        if let Some(attributes) = span_v2.attributes.value() {
            // Write all attributes to data:
            for (key, value) in attributes.iter() {
                compat_span
                    .data
                    .get_or_insert_with(Default::default)
                    .insert(key.clone(), value.clone().and_then(|attr| attr.value.value));
            }

            // Double-write some attributes to top-level fields:
            if let Some(description) = get_string_or_error(attributes, DESCRIPTION)
            // TODO: EAP expects sentry.raw_description, double write this somewhere.
            {
                compat_span.description = description;
            }
            if let Some(profile_id) = get_string_or_error(attributes, PROFILE_ID) {
                compat_span.profile_id = profile_id.and_then(|s| match s.parse::<EventId>() {
                    Ok(id) => Annotated::from(id),
                    Err(_) => Annotated::from_error(Error::invalid("profile_id"), None),
                });
            }
            if let Some(segment_id) = get_string_or_error(attributes, SEGMENT_ID) {
                compat_span.segment_id = segment_id;
            }
            if let Some(Value::Bool(b)) =
                attributes.get_value("sentry._internal.performance_issues_spans")
            {
                // ignoring meta here is OK, internal attribute set by Relay.
                compat_span.performance_issues_spans = Annotated::new(*b);
            }
        }

        compat_span.span_v2 = span_v2;
        Ok(compat_span)
    }
}

fn get_string_or_error(attributes: &Attributes, key: &str) -> Option<Annotated<String>> {
    let annotated = attributes.0.get(key)?;
    let value: Annotated<Value> = annotated.clone().and_then(|attr| attr.value.value);
    match value {
        Annotated(Some(Value::String(description)), meta) => {
            Some(Annotated(Some(description), meta))
        }
        Annotated(None, meta) => Some(Annotated(None, meta)),
        // Deliberate silent fail. We assume this is a naming collision, not invalid data.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use insta::assert_debug_snapshot;
    use relay_protocol::{Error, SerializableAnnotated};

    use crate::protocol::Attributes;

    use super::*;

    #[test]
    fn basic_conversion() {
        let json = r#"{
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "fa90fdead5f74052",
            "parent_span_id": "fa90fdead5f74051",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "name": "myname",
            "status": "ok",
            "links": [],
            "attributes": {
                "browser.name": {
                    "value": "Chrome",
                    "type": "string"
                },
                "sentry.description": {
                    "value": "mydescription",
                    "type": "string"
                },
                "sentry.environment": {
                    "value": "prod",
                    "type": "string"
                },
                "sentry.op": {
                    "value": "myop",
                    "type": "string"
                },
                "sentry.platform": {
                    "value": "php",
                    "type": "string"
                },
                "sentry.profile_id": {
                    "value": "a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab",
                    "type": "string"
                },
                "sentry.release": {
                    "value": "myapp@1.0.0",
                    "type": "string"
                },
                "sentry.sdk.name": {
                    "value": "sentry.php",
                    "type": "string"
                },
                "sentry.segment.id": {
                    "value": "FA90FDEAD5F74052",
                    "type": "string"
                },
                "sentry.segment.name": {
                    "value": "my 1st transaction",
                    "type": "string"
                },
                "sentry._internal.performance_issues_spans": {
                    "value": true,
                    "type": "bool"
                }
            }
        }"#;

        let span_v2: SpanV2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let compat_span = CompatSpan::try_from(span_v2).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::from(compat_span)), @r###"
        {
          "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
          "parent_span_id": "fa90fdead5f74051",
          "span_id": "fa90fdead5f74052",
          "name": "myname",
          "status": "ok",
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {
            "browser.name": {
              "type": "string",
              "value": "Chrome"
            },
            "sentry._internal.performance_issues_spans": {
              "type": "bool",
              "value": true
            },
            "sentry.description": {
              "type": "string",
              "value": "mydescription"
            },
            "sentry.environment": {
              "type": "string",
              "value": "prod"
            },
            "sentry.op": {
              "type": "string",
              "value": "myop"
            },
            "sentry.platform": {
              "type": "string",
              "value": "php"
            },
            "sentry.profile_id": {
              "type": "string",
              "value": "a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab"
            },
            "sentry.release": {
              "type": "string",
              "value": "myapp@1.0.0"
            },
            "sentry.sdk.name": {
              "type": "string",
              "value": "sentry.php"
            },
            "sentry.segment.id": {
              "type": "string",
              "value": "FA90FDEAD5F74052"
            },
            "sentry.segment.name": {
              "type": "string",
              "value": "my 1st transaction"
            }
          },
          "data": {
            "browser.name": "Chrome",
            "sentry._internal.performance_issues_spans": true,
            "sentry.description": "mydescription",
            "sentry.environment": "prod",
            "sentry.op": "myop",
            "sentry.platform": "php",
            "sentry.profile_id": "a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab",
            "sentry.release": "myapp@1.0.0",
            "sentry.sdk.name": "sentry.php",
            "sentry.segment.id": "FA90FDEAD5F74052",
            "sentry.segment.name": "my 1st transaction"
          },
          "description": "mydescription",
          "duration_ms": 500,
          "end_timestamp_precise": 123.5,
          "profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
          "segment_id": "FA90FDEAD5F74052",
          "start_timestamp_ms": 123000,
          "start_timestamp_precise": 123.0,
          "_performance_issues_spans": true
        }
        "###);
    }

    #[test]
    fn negative_duration() {
        let span_v2 = SpanV2 {
            start_timestamp: Timestamp(DateTime::from_timestamp_nanos(100)).into(),
            end_timestamp: Timestamp(DateTime::from_timestamp_nanos(50)).into(),
            ..Default::default()
        };

        let compat_span = CompatSpan::try_from(span_v2).unwrap();
        assert_eq!(compat_span.duration_ms.value(), Some(&0));
    }

    #[test]
    fn meta_conversion() {
        let span_v2 = SpanV2 {
            trace_id: Annotated::from_error(Error::invalid("trace_id"), None),
            parent_span_id: Annotated::from_error(Error::invalid("parent_span_id"), None),
            span_id: Annotated::from_error(Error::invalid("span_id"), None),
            name: Annotated::from_error(Error::invalid("name"), None),
            status: Annotated::from_error(Error::invalid("status"), None),
            is_remote: Annotated::from_error(Error::invalid("is_remote"), None),
            kind: Annotated::from_error(Error::invalid("kind"), None),
            start_timestamp: Annotated::from_error(Error::invalid("start_timestamp"), None),
            end_timestamp: Annotated::from_error(Error::invalid("end_timestamp"), None),
            links: Annotated::from_error(Error::invalid("links"), None),
            attributes: Annotated::new(Attributes::from_iter([
                (
                    "sentry.description".to_owned(),
                    Annotated::from_error(Error::invalid("description"), None),
                ),
                (
                    "sentry.profile_id".to_owned(),
                    Annotated::from_error(Error::invalid("profile ID"), None),
                ),
                (
                    "sentry.segment.id".to_owned(),
                    Annotated::from_error(Error::invalid("segment ID"), None),
                ),
                (
                    "performance_issues_spans".to_owned(),
                    Annotated::from_error(Error::invalid("flag"), None),
                ),
                (
                    "other_attribute".to_owned(),
                    Annotated::from_error(Error::invalid("other_attribute"), None),
                ),
            ])),
            other: BTreeMap::from([(
                "foo".to_owned(),
                Annotated::from_error(Error::invalid("other"), None),
            )]),
        };

        let compat_span = CompatSpan::try_from(span_v2).unwrap();
        assert_debug_snapshot!(compat_span);
    }
}
