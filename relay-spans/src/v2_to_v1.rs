use std::str::FromStr;

use relay_event_schema::protocol::{SpanKind, SpanV2Link};

use crate::status_codes;
use relay_event_schema::protocol::{
    EventId, Span as SpanV1, SpanData, SpanId, SpanLink, SpanStatus, SpanV2, SpanV2Kind,
    SpanV2Status,
};
use relay_protocol::{Annotated, FromValue, Object, Value};

/// Transforms a Sentry Span V2 to a Sentry Span.
pub fn span_v2_to_span_v1(span_v2: SpanV2) -> SpanV1 {
    let mut exclusive_time_ms = 0f64;
    let mut data = Object::new();
    let SpanV2 {
        start_timestamp,
        end_timestamp,
        trace_id,
        span_id,
        parent_span_id,
        name,
        kind,
        links,
        attributes,
        status,
        is_remote,
        other: _other,
    } = span_v2;

    let mut op = name;
    let mut description = Annotated::empty();
    let mut http_method = Annotated::empty();
    let mut http_route = Annotated::empty();
    let mut http_status_code = Annotated::empty();
    let mut grpc_status_code = Annotated::empty();
    let mut platform = Annotated::empty();
    let mut segment_id = Annotated::empty();
    let mut profile_id = Annotated::empty();

    for (key, value) in attributes.into_value().into_iter().flat_map(|attributes| {
        attributes.into_iter().flat_map(|(key, attribute)| {
            let attribute = attribute.into_value()?;
            Some((key, attribute.value.value))
        })
    }) {
        match key.as_str() {
            "sentry.description" => {
                description = String::from_value(value);
            }
            key if key.starts_with("db") => {
                op = op.or_else(|| Annotated::new(String::from("db")));
                if key == "db.statement" {
                    description = description.or_else(|| String::from_value(value));
                }
            }
            "http.method" | "http.request.method" => {
                let http_op = match kind.value() {
                    Some(SpanV2Kind::Server) => "http.server",
                    Some(SpanV2Kind::Client) => "http.client",
                    _ => "http",
                };
                op = op.or_else(|| Annotated::new(http_op.to_owned()));
                http_method = String::from_value(value);
            }
            "http.route" | "url.path" => {
                http_route = String::from_value(value);
            }
            key if key.contains("exclusive_time_nano") => {
                let value = match value.value() {
                    Some(Value::I64(v)) => *v as f64,
                    Some(Value::U64(v)) => *v as f64,
                    Some(Value::F64(v)) => *v,
                    Some(Value::String(v)) => v.parse::<f64>().unwrap_or_default(),
                    _ => 0f64,
                };
                exclusive_time_ms = value / 1e6f64;
            }
            "http.status_code" => {
                http_status_code = i64::from_value(value);
            }
            "rpc.grpc.status_code" => {
                grpc_status_code = i64::from_value(value);
            }
            "sentry.platform" => {
                platform = String::from_value(value);
            }
            "sentry.segment.id" => {
                segment_id = SpanId::from_value(value);
            }
            "sentry.profile.id" => {
                profile_id = EventId::from_value(value);
            }
            _ => {
                data.insert(key.to_owned(), value);
            }
        }
    }

    if exclusive_time_ms == 0f64 {
        if let (Some(start), Some(end)) = (start_timestamp.value(), end_timestamp.value()) {
            if let Some(nanos) = (end.0 - start.0).num_nanoseconds() {
                exclusive_time_ms = nanos as f64 / 1e6f64;
            }
        }
    }

    if let (Some(http_method), Some(http_route)) = (http_method.value(), http_route.value()) {
        description = description.or_else(|| Annotated::new(format!("{http_method} {http_route}")));
    }

    let links = links.map_value(|links| {
        links
            .into_iter()
            .map(|link| link.map_value(span_v2_link_to_span_v1_link))
            .collect()
    });

    let status = span_v2_status_to_span_v1_status(status, http_status_code, grpc_status_code);

    SpanV1 {
        op,
        description,
        data: SpanData::from_value(Annotated::new(data.into())),
        exclusive_time: exclusive_time_ms.into(),
        parent_span_id,
        segment_id,
        span_id,
        is_remote,
        profile_id,
        start_timestamp,
        status,
        timestamp: end_timestamp,
        trace_id,
        platform,
        kind: kind.map_value(span_v2_kind_to_span_v1_kind),
        links,
        ..Default::default()
    }
}

fn span_v2_status_to_span_v1_status(
    status: Annotated<SpanV2Status>,
    http_status_code: Annotated<i64>,
    grpc_status_code: Annotated<i64>,
) -> Annotated<SpanStatus> {
    status
        .and_then(|status| (status == SpanV2Status::Ok).then_some(SpanStatus::Ok))
        .or_else(|| {
            http_status_code.and_then(|http_status_code| {
                status_codes::HTTP
                    .get(&http_status_code)
                    .and_then(|sentry_status| SpanStatus::from_str(sentry_status).ok())
            })
        })
        .or_else(|| {
            grpc_status_code.and_then(|grpc_status_code| {
                status_codes::GRPC
                    .get(&grpc_status_code)
                    .and_then(|sentry_status| SpanStatus::from_str(sentry_status).ok())
            })
        })
        .or_else(|| Annotated::new(SpanStatus::Unknown))
}

fn span_v2_kind_to_span_v1_kind(kind: SpanV2Kind) -> SpanKind {
    match kind {
        SpanV2Kind::Internal => SpanKind::Internal,
        SpanV2Kind::Server => SpanKind::Server,
        SpanV2Kind::Client => SpanKind::Client,
        SpanV2Kind::Producer => SpanKind::Producer,
        SpanV2Kind::Consumer => SpanKind::Consumer,
        SpanV2Kind::Other(_) => SpanKind::Unspecified,
    }
}

fn span_v2_link_to_span_v1_link(link: SpanV2Link) -> SpanLink {
    let SpanV2Link {
        trace_id,
        span_id,
        sampled,
        attributes,
        other,
    } = link;

    let attributes = attributes.map_value(|attributes| {
        attributes
            .into_iter()
            .map(|(key, attribute)| {
                (
                    key,
                    attribute.and_then(|attribute| attribute.value.value.into_value()),
                )
            })
            .collect()
    });
    SpanLink {
        trace_id,
        span_id,
        sampled,
        attributes,
        other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    #[test]
    fn parse_span() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": "internal",
            "start_timestamp": "2023-10-18T09:14:14.980Z",
            "end_timestamp": "2023-10-18T09:14:14.980078800Z",
            "links": [],
            "attributes": {
                "sentry.environment": {
                    "value": "test",
                    "type": "string"
                },
                "fastify.type": {
                    "value": "middleware",
                    "type": "string"
                },
                "plugin.name": {
                    "value": "fastify -> @fastify/multipart",
                    "type": "string"
                },
                "hook.name": { 
                    "value": "onResponse",
                    "type": "string"
                },
                "sentry.sample_rate": {
                    "value": 1,
                    "type": "u64"
                },
                "sentry.parentSampled": {
                    "value": true,
                    "type": "boolean"
                },
                "sentry.exclusive_time_nano": {
                    "value": "1000000000",
                    "type": "u64"
                }
            },
            "status": "ok",
            "links": []
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 1000.0,
          "op": "middleware - fastify -> @fastify/multipart",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "ok",
          "data": {
            "sentry.environment": "test",
            "fastify.type": "middleware",
            "hook.name": "onResponse",
            "plugin.name": "fastify -> @fastify/multipart",
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
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": "internal",
            "start_timestamp": "2023-10-18T09:14:14.980Z",
            "end_timestamp": "2023-10-18T09:14:14.980078800Z",
            "links": [],
            "attributes": {
                "sentry.exclusive_time_nano": {
                    "value": 3200000000,
                    "type": "u64"
                }
            }
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 3200.0,
          "op": "middleware - fastify -> @fastify/multipart",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "internal"
        }
        "###);
    }

    #[test]
    fn parse_span_no_exclusive_time_nano_attribute() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": "internal",
            "start_timestamp": "2023-10-18T09:14:14.980Z",
            "end_timestamp": "2023-10-18T09:14:14.980078800Z",
            "links": []
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "middleware - fastify -> @fastify/multipart",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "internal"
        }
        "###);
    }

    #[test]
    fn parse_span_with_db_attributes() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "database query",
            "kind": "client",
            "start_timestamp": "2023-10-18T09:14:14.980Z",
            "end_timestamp": "2023-10-18T09:14:14.980078800Z",
            "links": [],
            "attributes": {
                "db.name": {
                    "value": "database",
                    "type": "string"
                },
                "db.type": {
                    "value": "sql",
                    "type": "string"
                },
                "db.statement": {
                    "value": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
                    "type": "string"
                }
            }
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "database query",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_span_with_db_attributes_and_description() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "database query",
            "kind": "client",
            "start_timestamp": "2023-10-18T09:14:14.980Z",
            "end_timestamp": "2023-10-18T09:14:14.980078800Z",
            "links": [],
            "attributes": {
                "db.name": {
                    "value": "database",
                    "type": "string"
                },
                "db.type": {
                    "value": "sql",
                    "type": "string"
                },
                "db.statement": {
                    "value": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
                    "type": "string"
                },
                "sentry.description": {
                    "value": "index view query",
                    "type": "string"
                }
            }
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "database query",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "index view query",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_span_with_http_attributes() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "http client request",
            "kind": "client",
            "start_timestamp": "2023-10-18T09:14:14.980Z",
            "end_timestamp": "2023-10-18T09:14:14.980078800Z",
            "links": [],
            "attributes": {
                "http.request.method": {
                    "value": "GET",
                    "type": "string"
                },
                "url.path": {
                    "value": "/api/search?q=foobar",
                    "type": "string"
                }
            }
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "http client request",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET /api/search?q=foobar",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_sentry_attributes() {
        let json = r#"{
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "fa90fdead5f74052",
            "parent_span_id": "fa90fdead5f74051",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "name": "myname",
            "kind": "unspecified",
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
                "sentry.profile.id": {
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

        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);

        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "op": "myname",
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
            "sentry.op": "myop"
          },
          "links": [],
          "platform": "php",
          "kind": "unspecified"
        }
        "###);
    }

    #[test]
    fn parse_span_is_remote() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "unspecified",
            "is_remote": true,
            "links": []
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "is_remote": true,
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "unspecified"
        }
        "###);
    }

    #[test]
    fn parse_span_is_not_remote() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "unspecified",
            "is_remote": false,
            "links": []
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "is_remote": false,
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "unspecified"
        }
        "###);
    }

    #[test]
    fn extract_span_kind() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "links": []
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
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
            "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
            "links": [
                {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74052",
                    "sampled": true,
                    "attributes": {
                        "str_key": {
                            "value": "str_value",
                            "type": "string"
                        },
                        "bool_key": {
                            "value": true,
                            "type": "boolean"
                        },
                        "int_key": {
                            "value": 123,
                            "type": "i64"
                        },
                        "double_key": {
                            "value": 1.23,
                            "type": "f64"
                        }
                    }
                }
            ]
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "exclusive_time": 0.0,
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
