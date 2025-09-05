use std::str::FromStr;

use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::protocol::{EventId, SpanV2, Timestamp};

/// Temporary type that amends a SpansV2 span with fields needed by the sentry span consumer.
/// This can be removed once the consumer has been updated to use the new schema.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue)]
#[allow(dead_code)]
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
}

impl TryFrom<SpanV2> for CompatSpan {
    type Error = uuid::Error;

    fn try_from(span_v2: SpanV2) -> Result<Self, uuid::Error> {
        let mut compat_span = Self::default();

        if let Some(start_timestamp) = span_v2.start_timestamp.value() {
            let dt = start_timestamp.0;
            compat_span.start_timestamp_precise = (*start_timestamp).into();
            compat_span.start_timestamp_ms = (dt.timestamp_millis() as u64).into();
        }

        if let Some(end_timestamp) = span_v2.end_timestamp.value() {
            compat_span.end_timestamp_precise = (*end_timestamp).into();
        }

        if let (Some(start_timestamp), Some(end_timestamp)) = (
            span_v2.start_timestamp.value(),
            span_v2.end_timestamp.value(),
        ) {
            let delta = (*end_timestamp - *start_timestamp).num_milliseconds();
            compat_span.duration_ms = u64::try_from(delta).unwrap_or(0).into();
        }

        if let Some(attributes) = span_v2.attributes.value() {
            for (key, value) in attributes
                .iter()
                .filter_map(|(k, a)| Some((k, a.value()?.value.value.value()?)))
            {
                // NOTE: This discards `_meta`
                compat_span
                    .data
                    .get_or_insert_with(Default::default)
                    .insert(key.clone(), value.clone().into());
            }

            if let Some(description) = attributes
                .get_value("sentry.description") // TODO: EAP expects sentry.raw_description, double write this somewhere.
                .and_then(Value::as_str)
            {
                compat_span.description = Annotated::from(description.to_owned());
            }

            if let Some(profile_id) = attributes
                .get_value("sentry.profile_id")
                .and_then(Value::as_str)
            {
                compat_span.profile_id = Annotated::from(EventId::from_str(profile_id)?);
            }

            if let Some(segment_id) = attributes
                .get_value("sentry.segment.id") // TODO: EAP expects `sentry.segment_id`, double write this somewhere.
                .and_then(Value::as_str)
            {
                compat_span.segment_id = Annotated::from(segment_id.to_owned());
            }
        }

        compat_span.span_v2 = span_v2;
        Ok(compat_span)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use relay_protocol::SerializableAnnotated;

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
          "start_timestamp_precise": 123.0
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
}
