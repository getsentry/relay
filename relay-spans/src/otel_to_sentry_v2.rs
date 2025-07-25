use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;
use opentelemetry_proto::tonic::trace::v1::span::Link as OtelLink;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind as OtelSpanKind;
use relay_event_schema::protocol::{Attribute, AttributeType, Attributes, SpanV2Kind};
use relay_protocol::ErrorKind;

use crate::otel_trace::{
    Span as OtelSpan, SpanFlags as OtelSpanFlags, status::StatusCode as OtelStatusCode,
};
use relay_event_schema::protocol::{
    SpanId, SpanV2 as SentrySpanV2, SpanV2Link, SpanV2Status, Timestamp, TraceId,
};
use relay_protocol::{Annotated, Error, Value};

/// Transform an OTEL span to a Sentry span V2.
///
/// This uses attributes in the OTEL span to populate various fields in the Sentry span.
/// * The Sentry span's `name` field may be set based on `db` or `http` attributes
///   if the OTEL span's `name` is empty.
/// * The Sentry span's `sentry.description` attribute may be set based on `db` or `http` attributes
///   if the OTEL span's `sentry.description` attribute is empty.
///
/// All other attributes are carried over from the OTEL span to the Sentry span.
pub fn otel_to_sentry_span(otel_span: OtelSpan) -> Result<SentrySpanV2, Error> {
    let OtelSpan {
        trace_id,
        span_id,
        parent_span_id,
        flags,
        name,
        kind,
        attributes,
        status,
        links,
        start_time_unix_nano,
        end_time_unix_nano,
        trace_state: _,
        dropped_attributes_count: _,
        events: _,
        dropped_events_count: _,
        dropped_links_count: _,
    } = otel_span;

    let start_timestamp = Utc.timestamp_nanos(start_time_unix_nano as i64);
    let end_timestamp = Utc.timestamp_nanos(end_time_unix_nano as i64);

    let span_id = SpanId::try_from(span_id.as_slice())?;
    let trace_id = TraceId::try_from(trace_id.as_slice())?;
    let parent_span_id = match parent_span_id.as_slice() {
        &[] => None,
        bytes => Some(SpanId::try_from(bytes)?),
    };

    let mut sentry_attributes = Attributes::new();
    let mut name = if name.is_empty() { None } else { Some(name) };

    for (key, value) in attributes.into_iter().flat_map(|attribute| {
        let value = attribute.value?.value?;
        Some((attribute.key, value))
    }) {
        match key.as_str() {
            key if key.starts_with("db") => {
                name = name.or(Some("db".to_owned()));
            }
            "http.method" | "http.request.method" => {
                let http_op = match kind {
                    2 => "http.server",
                    3 => "http.client",
                    _ => "http",
                };
                name = name.or(Some(http_op.to_owned()));
            }
            _ => (),
        }

        if let Some(v) = otel_value_to_attr(value) {
            sentry_attributes.insert_raw(key, Annotated::new(v));
        }
    }

    let sentry_links: Vec<Annotated<SpanV2Link>> = links
        .into_iter()
        .map(|link| otel_to_sentry_link(link).map(Into::into))
        .collect::<Result<_, _>>()?;

    if let Some(status_message) = status.clone().map(|status| status.message) {
        sentry_attributes.insert("sentry.status.message".to_owned(), status_message);
    }

    let event_span = SentrySpanV2 {
        name: name.into(),
        parent_span_id: parent_span_id.into(),
        span_id: span_id.into(),
        is_remote: Annotated::from(otel_flags_is_remote(flags)),
        start_timestamp: Timestamp(start_timestamp).into(),
        end_timestamp: Timestamp(end_timestamp).into(),
        status: status
            .map(|status| otel_to_sentry_status(status.code))
            .into(),
        trace_id: Annotated::new(trace_id),
        kind: otel_to_sentry_kind(kind),
        links: sentry_links.into(),
        attributes: Annotated::new(sentry_attributes),
        ..Default::default()
    };

    Ok(event_span)
}

fn otel_flags_is_remote(value: u32) -> Option<bool> {
    if value & OtelSpanFlags::ContextHasIsRemoteMask as u32 == 0 {
        None
    } else {
        Some(value & OtelSpanFlags::ContextIsRemoteMask as u32 != 0)
    }
}

fn otel_to_sentry_kind(kind: i32) -> Annotated<SpanV2Kind> {
    match kind {
        kind if kind == OtelSpanKind::Unspecified as i32 => Annotated::empty(),
        kind if kind == OtelSpanKind::Internal as i32 => Annotated::new(SpanV2Kind::Internal),
        kind if kind == OtelSpanKind::Server as i32 => Annotated::new(SpanV2Kind::Server),
        kind if kind == OtelSpanKind::Client as i32 => Annotated::new(SpanV2Kind::Client),
        kind if kind == OtelSpanKind::Producer as i32 => Annotated::new(SpanV2Kind::Producer),
        kind if kind == OtelSpanKind::Consumer as i32 => Annotated::new(SpanV2Kind::Consumer),
        _ => Annotated::from_error(ErrorKind::InvalidData, Some(Value::I64(kind as i64))),
    }
}

fn otel_to_sentry_status(status_code: i32) -> SpanV2Status {
    if status_code == OtelStatusCode::Unset as i32 || status_code == OtelStatusCode::Ok as i32 {
        SpanV2Status::Ok
    } else {
        SpanV2Status::Error
    }
}

fn otel_value_to_attr(otel_value: OtelValue) -> Option<Attribute> {
    let (ty, value) = match otel_value {
        OtelValue::StringValue(s) => (AttributeType::String, Value::String(s)),
        OtelValue::BoolValue(b) => (AttributeType::Boolean, Value::Bool(b)),
        OtelValue::IntValue(i) => (AttributeType::Integer, Value::I64(i)),
        OtelValue::DoubleValue(d) => (AttributeType::Double, Value::F64(d)),
        OtelValue::BytesValue(bytes) => {
            let s = String::from_utf8(bytes).ok()?;
            (AttributeType::String, Value::String(s))
        }
        OtelValue::ArrayValue(array) => {
            // Technically, `ArrayValue` can contain other arrays, or nested key-value lists. This
            // is not usually allowed by the OTLP protocol, but for safety we filter those values
            // out before serializing.
            let safe_values: Vec<serde_json::Value> = array
                .values
                .into_iter()
                .filter_map(|v| match v.value? {
                    OtelValue::StringValue(s) => Some(serde_json::Value::String(s)),
                    OtelValue::BoolValue(b) => Some(serde_json::Value::Bool(b)),
                    OtelValue::IntValue(i) => {
                        Some(serde_json::Value::Number(serde_json::Number::from(i)))
                    }
                    OtelValue::DoubleValue(d) => {
                        serde_json::Number::from_f64(d).map(serde_json::Value::Number)
                    }
                    OtelValue::BytesValue(bytes) => {
                        String::from_utf8(bytes).ok().map(serde_json::Value::String)
                    }
                    OtelValue::ArrayValue(_) | OtelValue::KvlistValue(_) => None,
                })
                .collect();

            // Serialize the arrays values as a JSON string. Even though there is some nominal
            // support for array values in Sentry, it's not robust and not ready to be used.
            // Instead, serialize arrays to a JSON string, and have the UI decode the JSON if
            // possible.
            let json = serde_json::to_string(&safe_values).unwrap_or_default();
            (AttributeType::String, Value::String(json))
        }
        OtelValue::KvlistValue(_) => {
            // Key-value pairs are supported by the type definition, but the OTLP protocol does
            // _not_ allow setting this kind of value on a span, so we don't need to handle this
            // case
            return None;
        }
    };

    Some(Attribute::new(ty, value))
}

fn otel_to_sentry_link(otel_link: OtelLink) -> Result<SpanV2Link, Error> {
    // See the W3C trace context specification:
    // <https://www.w3.org/TR/trace-context-2/#sampled-flag>
    const W3C_TRACE_CONTEXT_SAMPLED: u32 = 1 << 0;

    let attributes = Attributes::from_iter(otel_link.attributes.into_iter().filter_map(|kv| {
        let value = kv.value?.value?;
        let attr_value = otel_value_to_attr(value)?;
        Some((kv.key, Annotated::new(attr_value)))
    }));

    let span_link = SpanV2Link {
        trace_id: Annotated::new(hex::encode(otel_link.trace_id).parse()?),
        span_id: SpanId::try_from(otel_link.span_id.as_slice())?.into(),
        sampled: (otel_link.flags & W3C_TRACE_CONTEXT_SAMPLED != 0).into(),
        attributes: Annotated::new(attributes),
        other: Default::default(),
    };

    Ok(span_link)
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    #[test]
    fn parse_span() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800",
            "attributes": [
                {
                    "key": "sentry.environment",
                    "value": {
                        "stringValue": "test"
                    }
                },
                {
                "key": "fastify.type",
                    "value": {
                        "stringValue": "middleware"
                    }
                },
                {
                    "key": "plugin.name",
                    "value": {
                        "stringValue": "fastify -> @fastify/multipart"
                    }
                },
                {
                    "key": "hook.name",
                    "value": {
                        "stringValue": "onResponse"
                    }
                },
                {
                    "key": "sentry.sample_rate",
                    "value": {
                        "intValue": "1"
                    }
                },
                {
                    "key": "sentry.parentSampled",
                    "value": {
                        "boolValue": true
                    }
                },
                {
                    "key": "sentry.exclusive_time_nano",
                    "value": {
                        "intValue": "1000000000"
                    }
                }
            ],
            "droppedAttributesCount": 0,
            "events": [],
            "droppedEventsCount": 0,
            "status": {
                "code": 0,
                "message": "test"
            },
            "links": [],
            "droppedLinksCount": 0
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "middleware - fastify -> @fastify/multipart",
          "status": "ok",
          "kind": "internal",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "fastify.type": {
              "type": "string",
              "value": "middleware"
            },
            "hook.name": {
              "type": "string",
              "value": "onResponse"
            },
            "plugin.name": {
              "type": "string",
              "value": "fastify -> @fastify/multipart"
            },
            "sentry.environment": {
              "type": "string",
              "value": "test"
            },
            "sentry.exclusive_time_nano": {
              "type": "integer",
              "value": 1000000000
            },
            "sentry.parentSampled": {
              "type": "boolean",
              "value": true
            },
            "sentry.sample_rate": {
              "type": "integer",
              "value": 1
            },
            "sentry.status.message": {
              "type": "string",
              "value": "test"
            }
          }
        }
        "###);
    }

    #[test]
    fn parse_span_with_exclusive_time_nano_attribute() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "name": "middleware - fastify -> @fastify/multipart",
          "kind": 1,
          "startTimeUnixNano": "1697620454980000000",
          "endTimeUnixNano": "1697620454980078800",
          "attributes": [
            {
              "key": "sentry.exclusive_time_nano",
              "value": {
                "intValue": "3200000000"
              }
            }
          ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "middleware - fastify -> @fastify/multipart",
          "kind": "internal",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "sentry.exclusive_time_nano": {
              "type": "integer",
              "value": 3200000000
            }
          }
        }
        "###);
    }

    #[test]
    fn parse_span_with_db_attributes() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "name": "database query",
          "kind": 3,
          "startTimeUnixNano": "1697620454980000000",
          "endTimeUnixNano": "1697620454980078800",
          "attributes": [
            {
              "key": "db.name",
              "value": {
                "stringValue": "database"
              }
            },
            {
              "key": "db.type",
              "value": {
                "stringValue": "sql"
              }
            },
            {
              "key": "db.statement",
              "value": {
                "stringValue": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
              }
            }
          ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "database query",
          "kind": "client",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "db.name": {
              "type": "string",
              "value": "database"
            },
            "db.statement": {
              "type": "string",
              "value": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
            },
            "db.type": {
              "type": "string",
              "value": "sql"
            }
          }
        }
        "###);
    }

    #[test]
    fn parse_span_with_db_attributes_and_description() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "name": "database query",
          "kind": 3,
          "startTimeUnixNano": "1697620454980000000",
          "endTimeUnixNano": "1697620454980078800",
          "attributes": [
            {
              "key": "db.name",
              "value": {
                "stringValue": "database"
              }
            },
            {
              "key": "db.type",
              "value": {
                "stringValue": "sql"
              }
            },
            {
              "key": "db.statement",
              "value": {
                "stringValue": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
              }
            },
            {
              "key": "sentry.description",
              "value": {
                "stringValue": "index view query"
              }
            }
          ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "database query",
          "kind": "client",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "db.name": {
              "type": "string",
              "value": "database"
            },
            "db.statement": {
              "type": "string",
              "value": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
            },
            "db.type": {
              "type": "string",
              "value": "sql"
            },
            "sentry.description": {
              "type": "string",
              "value": "index view query"
            }
          }
        }
        "###);
    }

    #[test]
    fn parse_span_with_http_attributes() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "name": "http client request",
          "kind": 3,
          "startTimeUnixNano": "1697620454980000000",
          "endTimeUnixNano": "1697620454980078800",
          "attributes": [
            {
              "key": "http.request.method",
              "value": {
                "stringValue": "GET"
              }
            },
            {
              "key": "url.path",
              "value": {
                "stringValue": "/api/search?q=foobar"
              }
            }
          ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "http client request",
          "kind": "client",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "http.request.method": {
              "type": "string",
              "value": "GET"
            },
            "url.path": {
              "type": "string",
              "value": "/api/search?q=foobar"
            }
          }
        }
        "###);
    }

    /// Intended to be synced with `relay-event-schema::protocol::span::convert::tests::roundtrip`.
    #[test]
    fn parse_sentry_attributes() {
        let json = r#"{
          "traceId": "4c79f60c11214eb38604f4ae0781bfb2",
          "spanId": "fa90fdead5f74052",
          "parentSpanId": "fa90fdead5f74051",
          "startTimeUnixNano": "123000000000",
          "endTimeUnixNano": "123500000000",
          "name": "myname",
          "status": {
            "code": 0,
            "message": "foo"
          },
          "attributes": [
            {
              "key": "browser.name",
              "value": {
                "stringValue": "Chrome"
              }
            },
            {
              "key": "sentry.description",
              "value": {
                "stringValue": "mydescription"
              }
            },
            {
              "key": "sentry.environment",
              "value": {
                "stringValue": "prod"
              }
            },
            {
              "key": "sentry.op",
              "value": {
                "stringValue": "myop"
              }
            },
            {
              "key": "sentry.platform",
              "value": {
                "stringValue": "php"
              }
            },
            {
              "key": "sentry.profile.id",
              "value": {
                "stringValue": "a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab"
              }
            },
            {
              "key": "sentry.release",
              "value": {
                "stringValue": "myapp@1.0.0"
              }
            },
            {
              "key": "sentry.sdk.name",
              "value": {
                "stringValue": "sentry.php"
              }
            },
            {
              "key": "sentry.segment.id",
              "value": {
                "stringValue": "FA90FDEAD5F74052"
              }
            },
            {
              "key": "sentry.segment.name",
              "value": {
                "stringValue": "my 1st transaction"
              }
            },
            {
              "key": "sentry.metrics_summary.some_metric",
              "value": {
                "arrayValue": {
                  "values": [
                    {
                      "kvlistValue": {
                        "values": [
                          {
                            "key": "min",
                            "value": {
                              "doubleValue": 1
                            }
                          },
                          {
                            "key": "max",
                            "value": {
                              "doubleValue": 2
                            }
                          },
                          {
                            "key": "sum",
                            "value": {
                              "doubleValue": 3
                            }
                          },
                          {
                            "key": "count",
                            "value": {
                              "intValue": "2"
                            }
                          },
                          {
                            "key": "tags",
                            "value": {
                              "kvlistValue": {
                                "values": [
                                  {
                                    "key": "environment",
                                    "value": {
                                      "stringValue": "test"
                                    }
                                  }
                                ]
                              }
                            }
                          }
                        ]
                      }
                    }
                  ]
                }
              }
            }
          ]
        }"#;

        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();

        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
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
            "sentry.metrics_summary.some_metric": {
              "type": "string",
              "value": "[]"
            },
            "sentry.op": {
              "type": "string",
              "value": "myop"
            },
            "sentry.platform": {
              "type": "string",
              "value": "php"
            },
            "sentry.profile.id": {
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
            },
            "sentry.status.message": {
              "type": "string",
              "value": "foo"
            }
          }
        }
        "###);
    }

    #[test]
    fn parse_span_is_remote() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "startTimeUnixNano": "123000000000",
          "endTimeUnixNano": "123500000000",
          "flags": 768
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "is_remote": true,
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {}
        }
        "###);
    }

    #[test]
    fn parse_span_is_not_remote() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "startTimeUnixNano": "123000000000",
          "endTimeUnixNano": "123500000000",
          "flags": 256
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "is_remote": false,
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {}
        }
        "###);
    }

    #[test]
    fn extract_span_kind() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "parentSpanId": "0c7a7dea069bf5a6",
          "startTimeUnixNano": "123000000000",
          "endTimeUnixNano": "123500000000",
          "kind": 3
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "kind": "client",
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {}
        }
        "###);
    }

    #[test]
    fn parse_link() {
        let json = r#"{
          "traceId": "3c79f60c11214eb38604f4ae0781bfb2",
          "spanId": "e342abb1214ca181",
          "links": [
            {
              "traceId": "4c79f60c11214eb38604f4ae0781bfb2",
              "spanId": "fa90fdead5f74052",
              "attributes": [
                {
                  "key": "str_key",
                  "value": {
                    "stringValue": "str_value"
                  }
                },
                {
                  "key": "bool_key",
                  "value": {
                    "boolValue": true
                  }
                },
                {
                  "key": "int_key",
                  "value": {
                    "intValue": "123"
                  }
                },
                {
                  "key": "double_key",
                  "value": {
                    "doubleValue": 1.23
                  }
                }
              ],
              "flags": 1
            }
          ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
          "span_id": "e342abb1214ca181",
          "start_timestamp": 0.0,
          "end_timestamp": 0.0,
          "links": [
            {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74052",
              "sampled": true,
              "attributes": {
                "bool_key": {
                  "type": "boolean",
                  "value": true
                },
                "double_key": {
                  "type": "double",
                  "value": 1.23
                },
                "int_key": {
                  "type": "integer",
                  "value": 123
                },
                "str_key": {
                  "type": "string",
                  "value": "str_value"
                }
              }
            }
          ],
          "attributes": {}
        }
        "###);
    }
}
