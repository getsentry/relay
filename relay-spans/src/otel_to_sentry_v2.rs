use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::Link as OtelLink;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind as OtelSpanKind;
use relay_conventions::IS_REMOTE;
use relay_conventions::ORIGIN;
use relay_conventions::PLATFORM;
use relay_conventions::SPAN_KIND;
use relay_conventions::STATUS_MESSAGE;
use relay_event_schema::protocol::{Attributes, SpanKind};
use relay_otel::otel_resource_to_platform;
use relay_otel::otel_value_to_attribute;
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
pub fn otel_to_sentry_span(
    otel_span: OtelSpan,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> SentrySpanV2 {
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

    let span_id = SpanId::try_from(span_id.as_slice()).into();
    let trace_id = TraceId::try_from_slice_or_random(trace_id.as_slice());

    let parent_span_id = match parent_span_id.as_slice() {
        &[] => Annotated::empty(),
        bytes => SpanId::try_from(bytes).into(),
    };

    let mut sentry_attributes = Attributes::new();

    relay_otel::otel_scope_into_attributes(&mut sentry_attributes, resource, scope);

    sentry_attributes.insert(ORIGIN, "auto.otlp.spans".to_owned());
    if let Some(resource) = resource
        && let Some(platform) = otel_resource_to_platform(resource)
    {
        sentry_attributes.insert(PLATFORM, platform.to_owned());
    }

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

        if let Some(v) = otel_value_to_attribute(value) {
            sentry_attributes.0.insert(key, Annotated::new(v));
        }
    }

    let sentry_links: Vec<Annotated<SpanV2Link>> = links
        .into_iter()
        .map(|link| otel_to_sentry_link(link).into())
        .collect();

    if let Some(status_message) = status.clone().map(|status| status.message) {
        sentry_attributes.insert(STATUS_MESSAGE.to_owned(), status_message);
    }

    let is_remote = otel_flags_is_remote(flags);
    if let Some(is_remote) = is_remote {
        sentry_attributes.insert(IS_REMOTE, is_remote);
    }

    sentry_attributes.insert(
        SPAN_KIND,
        otel_to_sentry_kind(kind).map_value(|v| v.to_string()),
    );

    // A remote span is a segment span, but not every segment span is remote:
    let is_segment = match is_remote {
        Some(true) => Some(true),
        _ => None,
    }
    .into();

    SentrySpanV2 {
        name: name.into(),
        trace_id,
        span_id,
        parent_span_id,
        is_segment,
        start_timestamp: Timestamp(start_timestamp).into(),
        end_timestamp: Timestamp(end_timestamp).into(),
        status: status
            .map(|status| otel_to_sentry_status(status.code))
            .unwrap_or(SpanV2Status::Ok)
            .into(),
        links: sentry_links.into(),
        attributes: Annotated::new(sentry_attributes),
        ..Default::default()
    }
}

fn otel_flags_is_remote(value: u32) -> Option<bool> {
    if value & OtelSpanFlags::ContextHasIsRemoteMask as u32 == 0 {
        None
    } else {
        Some(value & OtelSpanFlags::ContextIsRemoteMask as u32 != 0)
    }
}

fn otel_to_sentry_kind(kind: i32) -> Annotated<SpanKind> {
    match kind {
        kind if kind == OtelSpanKind::Unspecified as i32 => Annotated::empty(),
        kind if kind == OtelSpanKind::Internal as i32 => Annotated::new(SpanKind::Internal),
        kind if kind == OtelSpanKind::Server as i32 => Annotated::new(SpanKind::Server),
        kind if kind == OtelSpanKind::Client as i32 => Annotated::new(SpanKind::Client),
        kind if kind == OtelSpanKind::Producer as i32 => Annotated::new(SpanKind::Producer),
        kind if kind == OtelSpanKind::Consumer as i32 => Annotated::new(SpanKind::Consumer),
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

// This function has been moved to relay-otel crate as otel_value_to_attribute

fn otel_to_sentry_link(otel_link: OtelLink) -> Result<SpanV2Link, Error> {
    // See the W3C trace context specification:
    // <https://www.w3.org/TR/trace-context-2/#sampled-flag>
    const W3C_TRACE_CONTEXT_SAMPLED: u32 = 1 << 0;

    let attributes = Attributes::from_iter(otel_link.attributes.into_iter().filter_map(|kv| {
        let value = kv.value?.value?;
        let attr_value = otel_value_to_attribute(value)?;
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
                    "key": "sentry.exclusive_time",
                    "value": {
                        "doubleValue": 1000.0
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

        let resource = serde_json::from_value(serde_json::json!({
            "attributes": [{
                "key": "service.name",
                "value": {"stringValue": "test-service"},
            }, {
              "key": "telemetry.sdk.language",
              "value": {"stringValue": "nodejs"},
            }]
        }))
        .unwrap();

        let scope = InstrumentationScope {
            name: "Eins Name".to_owned(),
            version: "123.42".to_owned(),
            attributes: Vec::new(),
            dropped_attributes_count: 12,
        };

        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span, Some(&resource), Some(&scope));
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "middleware - fastify -> @fastify/multipart",
          "status": "ok",
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
            "instrumentation.name": {
              "type": "string",
              "value": "Eins Name"
            },
            "instrumentation.version": {
              "type": "string",
              "value": "123.42"
            },
            "plugin.name": {
              "type": "string",
              "value": "fastify -> @fastify/multipart"
            },
            "resource.service.name": {
              "type": "string",
              "value": "test-service"
            },
            "resource.telemetry.sdk.language": {
              "type": "string",
              "value": "nodejs"
            },
            "sentry.environment": {
              "type": "string",
              "value": "test"
            },
            "sentry.exclusive_time": {
              "type": "double",
              "value": 1000.0
            },
            "sentry.kind": {
              "type": "string",
              "value": "internal"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            },
            "sentry.parentSampled": {
              "type": "boolean",
              "value": true
            },
            "sentry.platform": {
              "type": "string",
              "value": "node"
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
        "#);
    }

    #[test]
    fn parse_span_with_exclusive_time_attribute() {
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
              "key": "sentry.exclusive_time",
              "value": {
                "doubleValue": 3200.000000
              }
            }
          ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "middleware - fastify -> @fastify/multipart",
          "status": "ok",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "sentry.exclusive_time": {
              "type": "double",
              "value": 3200.0
            },
            "sentry.kind": {
              "type": "string",
              "value": "internal"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "database query",
          "status": "ok",
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
            "sentry.kind": {
              "type": "string",
              "value": "client"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "database query",
          "status": "ok",
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
            },
            "sentry.kind": {
              "type": "string",
              "value": "client"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "name": "http client request",
          "status": "ok",
          "start_timestamp": 1697620454.98,
          "end_timestamp": 1697620454.980079,
          "links": [],
          "attributes": {
            "http.request.method": {
              "type": "string",
              "value": "GET"
            },
            "sentry.kind": {
              "type": "string",
              "value": "client"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            },
            "url.path": {
              "type": "string",
              "value": "/api/search?q=foobar"
            }
          }
        }
        "#);
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
              "key": "sentry.profile_id",
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
        let event_span = otel_to_sentry_span(otel_span, None, None);

        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
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
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
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
            },
            "sentry.status.message": {
              "type": "string",
              "value": "foo"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "status": "ok",
          "is_segment": true,
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {
            "sentry.is_remote": {
              "type": "boolean",
              "value": true
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "status": "ok",
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {
            "sentry.is_remote": {
              "type": "boolean",
              "value": false
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "parent_span_id": "0c7a7dea069bf5a6",
          "span_id": "e342abb1214ca181",
          "status": "ok",
          "start_timestamp": 123.0,
          "end_timestamp": 123.5,
          "links": [],
          "attributes": {
            "sentry.kind": {
              "type": "string",
              "value": "client"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
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
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
          "span_id": "e342abb1214ca181",
          "status": "ok",
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
          "attributes": {
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            }
          }
        }
        "#);
    }

    #[test]
    fn parse_span_error_status() {
        let json = r#"{
          "traceId": "89143b0763095bd9c9955e8175d1fb23",
          "spanId": "e342abb1214ca181",
          "status": {
            "code": 2,
            "message": "2 is the error status code"
          }
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span, None, None);
        let annotated_span: Annotated<SentrySpanV2> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r#"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "span_id": "e342abb1214ca181",
          "status": "error",
          "start_timestamp": 0.0,
          "end_timestamp": 0.0,
          "links": [],
          "attributes": {
            "sentry.origin": {
              "type": "string",
              "value": "auto.otlp.spans"
            },
            "sentry.status.message": {
              "type": "string",
              "value": "2 is the error status code"
            }
          }
        }
        "#);
    }
}
