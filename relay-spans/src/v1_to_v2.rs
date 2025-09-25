use std::borrow::Cow;

use relay_event_schema::protocol::{
    Attribute, AttributeType, AttributeValue, Attributes, JsonLenientString, Span as SpanV1,
    SpanData, SpanLink, SpanStatus as SpanV1Status, SpanV2, SpanV2Link, SpanV2Status,
};
use relay_protocol::{Annotated, Empty, Error, IntoValue, Meta, Value};

/// Converts a legacy span to the new Span V2 schema.
///
/// - `tags`, `sentry_tags`, `measurements` and `data` are transferred to `attributes`.
/// - Nested `data` items are encoded as JSON.
pub fn span_v1_to_span_v2(span_v1: SpanV1) -> SpanV2 {
    let SpanV1 {
        timestamp,
        start_timestamp,
        exclusive_time,
        op,
        span_id,
        parent_span_id,
        trace_id,
        segment_id,
        is_segment,
        is_remote,
        status,
        description,
        tags,
        origin,
        profile_id,
        data,
        links,
        sentry_tags,
        received: _, // needs to go into the Kafka span eventually, but makes no sense in Span V2 schema.
        measurements,
        platform,
        was_transaction,
        kind,
        performance_issues_spans,
        other,
    } = span_v1;

    let mut annotated_attributes = attributes_from_data(data);
    let attributes = annotated_attributes.get_or_insert_with(Default::default);

    // Top-level fields have higher precedence than `data`:
    attributes.insert("sentry.exclusive_time", exclusive_time);
    attributes.insert("sentry.op", op);

    attributes.insert("sentry.segment.id", segment_id.map_value(|v| v.to_string()));
    attributes.insert("sentry.is_segment", is_segment);
    attributes.insert("sentry.description", description);
    attributes.insert("sentry.origin", origin);
    attributes.insert("sentry.profile_id", profile_id.map_value(|v| v.to_string()));
    attributes.insert("sentry.platform", platform);
    attributes.insert("sentry.was_transaction", was_transaction);
    attributes.insert(
        "sentry._internal.performance_issues_spans",
        performance_issues_spans,
    );

    // Use same precedence as `backfill_data` for data bags:
    if let Some(measurements) = measurements.into_value() {
        for (key, measurement) in measurements.0 {
            let key = match key.as_str() {
                "client_sample_rate" => "sentry.client_sample_rate",
                "server_sample_rate" => "sentry.server_sample_rate",
                other => other,
            };

            attributes.insert_if_missing(key, || match measurement {
                Annotated(Some(measurement), _) => measurement.value.map_value(|f| f.to_f64()),
                Annotated(None, meta) => Annotated(None, meta),
            });
        }
    }
    if let Some(tags) = tags.into_value() {
        for (key, value) in tags {
            if !attributes.contains_key(&key) {
                attributes.insert_raw(
                    key,
                    value
                        .map_value(|JsonLenientString(s)| AttributeValue::from(s))
                        .and_then(Attribute::from),
                )
            }
        }
    }
    if let Some(tags) = sentry_tags.into_value()
        && let Value::Object(tags) = tags.into_value()
    {
        for (key, value) in tags {
            let key = match key.as_str() {
                "description" => "sentry.normalized_description".into(),
                other => Cow::Owned(format!("sentry.{}", other)),
            };
            if !value.is_empty() && !attributes.contains_key(key.as_ref()) {
                attributes.insert_raw(key.into_owned(), attribute_from_value(value));
            }
        }
    }

    SpanV2 {
        trace_id,
        parent_span_id,
        span_id,
        name: attributes
            .get_value("sentry.name")
            .and_then(|v| Some(v.as_str()?.to_owned()))
            .into(),
        status: Annotated::map_value(status, span_v1_status_to_span_v2_status),
        is_remote,
        kind,
        start_timestamp,
        end_timestamp: timestamp,
        links: links.map_value(span_v1_links_to_span_v2_links),
        attributes: annotated_attributes,
        other,
    }
}

fn span_v1_status_to_span_v2_status(status: SpanV1Status) -> SpanV2Status {
    match status {
        SpanV1Status::Ok => SpanV2Status::Ok,
        _ => SpanV2Status::Error,
    }
}

fn span_v1_links_to_span_v2_links(links: Vec<Annotated<SpanLink>>) -> Vec<Annotated<SpanV2Link>> {
    links
        .into_iter()
        .map(|link| {
            link.map_value(
                |SpanLink {
                     trace_id,
                     span_id,
                     sampled,
                     attributes,
                     other,
                 }| {
                    SpanV2Link {
                        trace_id,
                        span_id,
                        sampled,
                        attributes: attributes.map_value(|attrs| {
                            Attributes::from_iter(
                                attrs
                                    .into_iter()
                                    .map(|(key, value)| (key, attribute_from_value(value))),
                            )
                        }),
                        other,
                    }
                },
            )
        })
        .collect()
}

fn attributes_from_data(data: Annotated<SpanData>) -> Annotated<Attributes> {
    let Annotated(data, meta) = data;
    let Some(data) = data else {
        return Annotated(None, meta);
    };
    let Value::Object(data) = data.into_value() else {
        debug_assert!(false, "`SpanData` must convert to Object");
        return Annotated(None, meta);
    };

    Annotated::new(Attributes::from_iter(data.into_iter().filter_map(
        |(key, value)| (!value.is_empty()).then_some((key, attribute_from_value(value))),
    )))
}

fn attribute_from_value(value: Annotated<Value>) -> Annotated<Attribute> {
    let value: Annotated<AttributeValue> = value.and_then(attribute_value_from_value);
    value.map_value(Attribute::from)
}

/// Converts a generic [`Value`] into an annotated attribute value with the proper type.
///
/// - Any conversion errors are documented in [`Meta`].
/// - Nested values are serialized into strings.
fn attribute_value_from_value(value: Value) -> Annotated<AttributeValue> {
    match value {
        Value::Bool(v) => AttributeValue::from(v),
        Value::I64(v) => AttributeValue::from(v),
        Value::U64(v) => match i64::try_from(v) {
            Ok(i) => AttributeValue::from(i),
            Err(_) => return Annotated::from_error(Error::invalid("integer too large"), None),
        },
        Value::F64(v) => AttributeValue::from(v),
        Value::String(v) => AttributeValue::from(v),
        Value::Array(_) | Value::Object(_) => {
            return match Annotated::new(value).to_json() {
                Ok(s) => Annotated(
                    Some(AttributeValue {
                        ty: AttributeType::String.into(),
                        value: Value::String(s).into(),
                    }),
                    Meta::from_error(Error::expected("scalar attribute")),
                ),
                Err(_) => Annotated::from_error(
                    Error::invalid("failed to serialize nested attribute"),
                    None,
                ),
            };
        }
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_event_schema::protocol::{CompatSpan, Event};
    use relay_protocol::{FromValue, SerializableAnnotated};

    #[test]
    fn parse() {
        let json = serde_json::json!({
          "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
          "parent_span_id": "fa90fdead5f74051",
          "span_id": "fa90fdead5f74052",
          "status": "ok",
          "is_remote": true,
          "kind": "server",
          "start_timestamp": -63158400.0,
          "timestamp": 0.0,
          "links": [
            {
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "fa90fdead5f74052",
            "sampled": true,
              "attributes": {
                "boolAttr": true,
                "numAttr": 123,
                "stringAttr": "foo"
              }
            }
          ],
          "tags": {
            "foo": "bar"
          },
          "measurements": {
            "memory": {
              "value": 9001.0,
              "unit": "byte"
            },
            "client_sample_rate": {
              "value": 0.11
            },
            "server_sample_rate": {
              "value": 0.22
            }
          },
          "data": {
            "my.data.field": "my.data.value",
            "my.nested": {
              "numbers": [
                1,
                2,
                3
              ]
            }
          },
          "_performance_issues_spans": true,
          "description": "raw description",
          "exclusive_time": 1.23,
          "is_segment": true,
          "sentry_tags": {
            "description": "normalized description",
            "user": "id:user123",
          },
          "op": "operation",
          "origin": "auto.http",
          "platform": "javascript",
          "profile_id": "4c79f60c11214eb38604f4ae0781bfb0",
          "segment_id": "fa90fdead5f74050",
          "was_transaction": true,

          "received": 0.2,
          "additional_field": "additional field value"
        });

        let span_v1 = SpanV1::from_value(json.into()).into_value().unwrap();
        let span_v2 = span_v1_to_span_v2(span_v1);

        let annotated_span_v2: Annotated<SpanV2> = Annotated::new(span_v2);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span_v2), @r###"
        {
          "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
          "parent_span_id": "fa90fdead5f74051",
          "span_id": "fa90fdead5f74052",
          "status": "ok",
          "is_remote": true,
          "kind": "server",
          "start_timestamp": -63158400.0,
          "end_timestamp": 0.0,
          "links": [
            {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74052",
              "sampled": true,
              "attributes": {
                "boolAttr": {
                  "type": "boolean",
                  "value": true
                },
                "numAttr": {
                  "type": "integer",
                  "value": 123
                },
                "stringAttr": {
                  "type": "string",
                  "value": "foo"
                }
              }
            }
          ],
          "attributes": {
            "foo": {
              "type": "string",
              "value": "bar"
            },
            "memory": {
              "type": "double",
              "value": 9001.0
            },
            "my.data.field": {
              "type": "string",
              "value": "my.data.value"
            },
            "my.nested": {
              "type": "string",
              "value": "{\"numbers\":[1,2,3]}"
            },
            "sentry._internal.performance_issues_spans": {
              "type": "boolean",
              "value": true
            },
            "sentry.client_sample_rate": {
              "type": "double",
              "value": 0.11
            },
            "sentry.description": {
              "type": "string",
              "value": "raw description"
            },
            "sentry.exclusive_time": {
              "type": "integer",
              "value": 1.23
            },
            "sentry.is_segment": {
              "type": "boolean",
              "value": true
            },
            "sentry.normalized_description": {
              "type": "string",
              "value": "normalized description"
            },
            "sentry.op": {
              "type": "string",
              "value": "operation"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.http"
            },
            "sentry.platform": {
              "type": "string",
              "value": "javascript"
            },
            "sentry.profile_id": {
              "type": "string",
              "value": "4c79f60c11214eb38604f4ae0781bfb0"
            },
            "sentry.segment.id": {
              "type": "string",
              "value": "fa90fdead5f74050"
            },
            "sentry.server_sample_rate": {
              "type": "double",
              "value": 0.22
            },
            "sentry.user": {
              "type": "string",
              "value": "id:user123"
            },
            "sentry.was_transaction": {
              "type": "boolean",
              "value": true
            }
          },
          "additional_field": "additional field value",
          "_meta": {
            "attributes": {
              "my.nested": {
                "": {
                  "err": [
                    [
                      "invalid_data",
                      {
                        "reason": "expected scalar attribute"
                      }
                    ]
                  ]
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn transaction_conversion() {
        let txn = Annotated::<Event>::from_json(r#"{"transaction": "hi"}"#)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(txn.transaction.as_str(), Some("hi"));
        let span_v1 = SpanV1::from(&txn);
        assert_eq!(
            span_v1.data.value().unwrap().segment_name.as_str(),
            Some("hi")
        );
        let span_v2 = span_v1_to_span_v2(span_v1);
        assert_eq!(
            span_v2
                .attributes
                .value()
                .unwrap()
                .get_value("sentry.segment.name")
                .and_then(Value::as_str),
            Some("hi")
        );
        let compat_span = CompatSpan::try_from(span_v2).unwrap();
        assert_eq!(
            compat_span
                .data
                .value()
                .unwrap()
                .get("sentry.segment.name")
                .and_then(|v| v.as_str()),
            Some("hi")
        );
    }
}
