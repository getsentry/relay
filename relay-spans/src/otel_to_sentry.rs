use crate::otel_to_sentry_v2;
use crate::otel_trace::Span as OtelSpan;
use crate::v2_to_v1;
use relay_event_schema::protocol::Span as EventSpan;
use relay_protocol::Error;

/// Transforms an OTEL span to a Sentry span.
///
/// This uses attributes in the OTEL span to populate various fields in the Sentry span.
/// * The Sentry span's `name` field may be set based on `db` or `http` attributes
///   if the OTEL span's `name` is empty.
/// * The Sentry span's `op` field will be inferred based on the OTEL span's `sentry.op` attribute,
///   or other available attributes if `sentry.op` is not provided.
/// * The Sentry span's `description` field may be set based on `db` or `http` attributes
///   if the OTEL span's `sentry.description` attribute is empty.
/// * The Sentry span's `status` field is set based on the OTEL span's `status` field and
///   `http.status_code` and `rpc.grpc.status_code` attributes.
/// * The Sentry span's `exclusive_time` field is set based on the OTEL span's `exclusive_time_nano`
///   attribute, or the difference between the start and end timestamp if that attribute is not set.
/// * The Sentry span's `platform` field is set based on the OTEL span's `sentry.platform` attribute.
/// * The Sentry span's `profile_id` field is set based on the OTEL span's `sentry.profile.id` attribute.
/// * The Sentry span's `segment_id` field is set based on the OTEL span's `sentry.segment.id` attribute.
///
/// All other attributes are carried over from the OTEL span to the Sentry span's `data`.
pub fn otel_to_sentry_span(otel_span: OtelSpan) -> Result<EventSpan, Error> {
    let span_v2 = otel_to_sentry_v2::otel_to_sentry_span(otel_span)?;
    Ok(v2_to_v1::span_v2_to_span_v1(span_v2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{Annotated, SerializableAnnotated};

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
                    "key": "http.route", "value": {
                        "stringValue": "/home"
                    }
                },
                {
                    "key": "http.request.method",
                    "value": {
                        "stringValue": "GET"
                        }
                    },
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 1000.0,
          "op": "http",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "ok",
          "description": "GET /home",
          "data": {
            "sentry.environment": "test",
            "fastify.type": "middleware",
            "hook.name": "onResponse",
            "http.request.method": "GET",
            "http.route": "/home",
            "plugin.name": "fastify -> @fastify/multipart",
            "sentry.name": "middleware - fastify -> @fastify/multipart",
            "sentry.parentSampled": true,
            "sentry.sample_rate": 1
          },
          "links": [],
          "kind": "internal"
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 3200.0,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "middleware - fastify -> @fastify/multipart",
          "data": {
            "sentry.name": "middleware - fastify -> @fastify/multipart"
          },
          "links": [],
          "kind": "internal"
        }
        "###);
    }

    #[test]
    fn parse_span_no_exclusive_time_nano_attribute() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800"
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "middleware - fastify -> @fastify/multipart",
          "data": {
            "sentry.name": "middleware - fastify -> @fastify/multipart"
          },
          "links": [],
          "kind": "internal"
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
                    "key" : "db.name",
                    "value": {
                        "stringValue": "database"
                    }
                },
                {
                    "key" : "db.type",
                    "value": {
                        "stringValue": "sql"
                    }
                },
                {
                    "key" : "db.statement",
                    "value": {
                        "stringValue": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
          "data": {
            "db.name": "database",
            "db.statement": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
            "db.type": "sql",
            "sentry.name": "database query"
          },
          "links": [],
          "kind": "client"
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
                    "key" : "db.name",
                    "value": {
                        "stringValue": "database"
                    }
                },
                {
                    "key" : "db.type",
                    "value": {
                        "stringValue": "sql"
                    }
                },
                {
                    "key" : "db.statement",
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "index view query",
          "data": {
            "db.name": "database",
            "db.statement": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
            "db.type": "sql",
            "sentry.name": "database query"
          },
          "links": [],
          "kind": "client"
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
                    "key" : "http.request.method",
                    "value": {
                        "stringValue": "GET"
                    }
                },
                {
                    "key" : "url.path",
                    "value": {
                        "stringValue": "/api/search?q=foobar"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "http.client",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET /api/search?q=foobar",
          "data": {
            "http.request.method": "GET",
            "sentry.name": "http client request",
            "url.path": "/api/search?q=foobar"
          },
          "links": [],
          "kind": "client"
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
            "status": {"code": 0, "message": "foo"},
            "attributes": [
                {
                    "key" : "browser.name",
                    "value": {
                        "stringValue": "Chrome"
                    }
                },
                {
                    "key" : "sentry.description",
                    "value": {
                        "stringValue": "mydescription"
                    }
                },
                {
                    "key" : "sentry.environment",
                    "value": {
                        "stringValue": "prod"
                    }
                },
                {
                    "key" : "sentry.op",
                    "value": {
                        "stringValue": "myop"
                    }
                },
                {
                    "key" : "sentry.platform",
                    "value": {
                        "stringValue": "php"
                    }
                },
                {
                    "key" : "sentry.profile.id",
                    "value": {
                        "stringValue": "a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab"
                    }
                },
                {
                    "key" : "sentry.release",
                    "value": {
                        "stringValue": "myapp@1.0.0"
                    }
                },
                {
                    "key" : "sentry.sdk.name",
                    "value": {
                        "stringValue": "sentry.php"
                    }
                },
                {
                    "key" : "sentry.segment.id",
                    "value": {
                        "stringValue": "FA90FDEAD5F74052"
                    }
                },
                {
                    "key" : "sentry.segment.name",
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
                                                    "doubleValue": 1.0
                                                }
                                            },
                                            {
                                                "key": "max",
                                                "value": {
                                                    "doubleValue": 2.0
                                                }
                                            },
                                            {
                                                "key": "sum",
                                                "value": {
                                                    "doubleValue": 3.0
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

        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "op": "myop",
          "span_id": "fa90fdead5f74052",
          "parent_span_id": "fa90fdead5f74051",
          "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
          "segment_id": "fa90fdead5f74052",
          "status": "ok",
          "description": "mydescription",
          "profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
          "data": {
            "browser.name": "Chrome",
            "sentry.environment": "prod",
            "sentry.release": "myapp@1.0.0",
            "sentry.segment.name": "my 1st transaction",
            "sentry.sdk.name": "sentry.php",
            "sentry.name": "myname"
          },
          "links": [],
          "platform": "php"
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "is_remote": true,
          "status": "unknown",
          "data": {},
          "links": []
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "is_remote": false,
          "status": "unknown",
          "data": {},
          "links": []
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "client"
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 0.0,
          "start_timestamp": 0.0,
          "exclusive_time": 0.0,
          "op": "default",
          "span_id": "e342abb1214ca181",
          "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
          "status": "unknown",
          "data": {},
          "links": [
            {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74052",
              "sampled": true,
              "attributes": {
                "bool_key": true,
                "double_key": 1.23,
                "int_key": 123,
                "str_key": "str_value"
              }
            }
          ]
        }
        "###);
    }
}
