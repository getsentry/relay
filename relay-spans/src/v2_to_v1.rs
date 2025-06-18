use std::str::FromStr;

use relay_event_schema::protocol::{SpanKind, SpanV2Link};

use crate::status_codes;
use relay_event_schema::protocol::{
    Attribute, EventId, Span as SpanV1, SpanData, SpanId, SpanLink, SpanStatus, SpanV2, SpanV2Kind,
    SpanV2Status,
};
use relay_protocol::{Annotated, FromValue, Object, Value};
use url::Url;

/// Transforms a Sentry span V2 to a Sentry span V1.
///
/// This uses attributes in the V2 span to populate various fields in the V1 span.
/// * The V1 span's `op` field will be set based on the V2 span's `sentry.op` attribute, or
///   inferred from other attributes if the `sentry.op` attribute is not set.
/// * The V1 span's `description` field will be set based on the V2 span's `sentry.description`
/// attribute, or inferred from other attributes if the `sentry.description` attribute is not set.
/// * The V1 span's `description` field is set based on the V2 span's `sentry.description` attribute.
/// * The V1 span's `status` field is set based on the V2 span's `status` field and
///   `http.status_code` and `rpc.grpc.status_code` attributes.
/// * The V1 span's `exclusive_time` field is set based on the V2 span's `exclusive_time_nano`
///   attribute, or the difference between the start and end timestamp if that attribute is not set.
/// * The V1 span's `platform` field is set based on the V2 span's `sentry.platform` attribute.
/// * The V1 span's `profile_id` field is set based on the V2 span's `sentry.profile.id` attribute.
/// * The V1 span's `segment_id` field is set based on the V2 span's `sentry.segment.id` attribute.
///
/// All other attributes are carried over from the V2 span to the V1 span's `data`.
pub fn span_v2_to_span_v1(span_v2: SpanV2) -> SpanV1 {
    let mut exclusive_time_ms = 0f64;
    let mut data = Object::new();

    let inferred_op = derive_op_for_v2_span(&span_v2);
    let inferred_description = derive_description_for_v2_span(&span_v2);

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

    let mut description = Annotated::empty();
    let mut op = Annotated::empty();
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
            "sentry.op" => {
                op = String::from_value(value);
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
                http_status_code = i64::from_value(value.clone());
                data.insert(key.to_owned(), value);
            }
            "rpc.grpc.status_code" => {
                grpc_status_code = i64::from_value(value.clone());
                data.insert(key.to_owned(), value);
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

    // Note: This writes the incoming `name` field to a the `sentry.name` attribute. This creates a
    // bit of duplication, since the attribute `sentry.name` will have the same value as the `op`
    // field. This duplication is temporary, since we will soon generate a proper `op` field that will
    // be different from the name.
    if let Some(name) = name.value() {
        data.insert(
            "sentry.name".to_owned(),
            Annotated::new(Value::String(name.to_owned())),
        );
    }

    if exclusive_time_ms == 0f64 {
        if let (Some(start), Some(end)) = (start_timestamp.value(), end_timestamp.value()) {
            if let Some(nanos) = (end.0 - start.0).num_nanoseconds() {
                exclusive_time_ms = nanos as f64 / 1e6f64;
            }
        }
    }

    let links = links.map_value(|links| {
        links
            .into_iter()
            .map(|link| link.map_value(span_v2_link_to_span_v1_link))
            .collect()
    });

    let status = span_v2_status_to_span_v1_status(status, http_status_code, grpc_status_code);

    // If the SDK sent in a `sentry.op` attribute, use it. If not, derive it from the span attributes.
    let op = op.or_else(|| Annotated::from(inferred_op));

    // If the SDK sent in a `sentry.description` attribute, use it. If not, derive it from the span attributes.
    let description = description.or_else(|| Annotated::from(inferred_description));

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

/// Generates a `sentry.op` attribute for V2 span, if possible.
///
/// This uses attributes of the span to figure out an appropriate operation name, inferring what the
/// SDK might have sent. Reliably infers an op for well-known OTel span kinds like database
/// operations. Does not infer an op for frontend and mobile spans sent by Sentry SDKs that don't
/// have an OTel equivalent (e.g., resource loads).
fn derive_op_for_v2_span(span: &SpanV2) -> String {
    // NOTE: `op` is not a required field in the SDK, so the fallback is an empty string.
    let op = String::from("default");

    let Some(attributes) = span.attributes.value() else {
        return op;
    };

    if attributes.contains_key("http.request.method") || attributes.contains_key("http.method") {
        return match span.kind.value() {
            Some(SpanV2Kind::Client) => String::from("http.client"),
            Some(SpanV2Kind::Server) => String::from("http.server"),
            _ => {
                if attributes.contains_key("sentry.http.prefetch") {
                    String::from("http.prefetch")
                } else {
                    String::from("http")
                }
            }
        };
    }

    if attributes.contains_key("db.system") || attributes.contains_key("db.system.name") {
        return String::from("db");
    }

    if attributes.contains_key("gen_ai.system") {
        return String::from("gen_ai");
    }

    if attributes.contains_key("rpc.service") {
        return String::from("rpc");
    }

    if attributes.contains_key("messaging.system") {
        return String::from("message");
    }

    if let Some(faas_trigger) = attributes
        .get("faas.trigger")
        .and_then(|faas_trigger| faas_trigger.value())
        .and_then(|trigger_value| trigger_value.value.value.value())
        .and_then(|v| v.as_str())
    {
        return faas_trigger.to_owned();
    }

    op
}

/// Generates a `sentry.description` attribute for V2 span, if possible.
///
/// This uses attributes of the span to figure out an appropriate description, trying to match what
/// the SDK might have sent. This works well for HTTP and database spans, but doesn't have a
/// thorough implementation for other types of spans for now.
fn derive_description_for_v2_span(span: &SpanV2) -> Option<String> {
    // `name` is a low-cardinality description of the span, so it makes for a good fallback.
    let description = span.name.value().map(|v| v.to_owned());

    let Some(attributes) = span.attributes.value() else {
        return description;
    };

    if attributes.contains_key("http.request.method") || attributes.contains_key("http.method") {
        return derive_http_description(attributes, &span.kind.value());
    }

    if attributes.contains_key("db.system") || attributes.contains_key("db.system.name") {
        let is_cache_op = attributes
            .get("sentry.op")
            .and_then(|attr| attr.value())
            .and_then(|attr_val| attr_val.value.value.value())
            .and_then(|v| v.as_str())
            .map(|op| op.starts_with("cache."))
            .unwrap_or(false);

        if !is_cache_op {
            return derive_db_description(attributes);
        }
    }

    description
}

fn derive_http_description(
    attributes: &Object<Attribute>,
    kind: &Option<&SpanV2Kind>,
) -> Option<String> {
    // Get HTTP method
    let http_method = attributes
        .get("http.request.method")
        .or_else(|| attributes.get("http.method"))
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())?;

    let description = http_method.to_owned();

    // Get URL path information
    let url_path = match kind {
        Some(SpanV2Kind::Server) => get_server_url_path(attributes),
        Some(SpanV2Kind::Client) => get_client_url_path(attributes),
        _ => None,
    };

    if url_path.is_none() {
        return Some(description);
    }

    let url_path = url_path.unwrap();
    let base_description = format!("{} {}", http_method, url_path);

    // Check for GraphQL operations
    if let Some(graphql_ops) = attributes
        .get("sentry.graphql.operation")
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())
    {
        return Some(format!("{} ({})", base_description, graphql_ops));
    }

    Some(base_description)
}

fn derive_db_description(attributes: &Object<Attribute>) -> Option<String> {
    // Check for db.statement first
    if let Some(statement) = attributes
        .get("db.statement")
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())
    {
        return Some(statement.to_string());
    }

    None
}

fn get_server_url_path(attributes: &Object<Attribute>) -> Option<String> {
    // `http.route` takes precedence. If available, this is the matched route of the server
    // framework for server spans. Not always available, even for server spans.
    if let Some(route) = attributes
        .get("http.route")
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())
    {
        return Some(route.to_string());
    }

    // `url.path` is the path of the HTTP request for server spans. This is required for server spans.
    if let Some(path) = attributes
        .get("url.path")
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())
    {
        return Some(path.to_string());
    }

    // `http.target` is deprecated, but might be present in older data. Here as a fallback
    if let Some(target) = attributes
        .get("http.target")
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())
    {
        return Some(strip_url_query_and_fragment(target));
    }

    None
}

fn strip_url_query_and_fragment(url: &str) -> String {
    url.split('?')
        .next()
        .unwrap_or(url)
        .split('#')
        .next()
        .unwrap_or(url)
        .to_string()
}

fn get_client_url_path(attributes: &Object<Attribute>) -> Option<String> {
    // `url.full` is a new required attribute for client spans. This is the full URL and should be
    // safe to parse. `http.url` is its old name
    if let Some(url) = attributes
        .get("url.full")
        .or_else(|| attributes.get("http.url"))
        .and_then(|attr| attr.value())
        .and_then(|attr_val| attr_val.value.value.value())
        .and_then(|v| v.as_str())
    {
        if let Ok(parsed_url) = Url::parse(url) {
            return Some(format!(
                "{}://{}{}",
                parsed_url.scheme(),
                parsed_url.domain().unwrap_or(""),
                parsed_url.path()
            ));
        }
    }

    None
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
          "op": "default",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "ok",
          "description": "middleware - fastify -> @fastify/multipart",
          "data": {
            "sentry.environment": "test",
            "fastify.type": "middleware",
            "hook.name": "onResponse",
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
    fn parse_sentry_attributes() {
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
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
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
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
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
    fn parse_http_client_span_only_method() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "attributes": {
                "http.method": {
                    "value": "GET",
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
          "op": "http.client",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET",
          "data": {
            "http.request_method": "GET"
          },
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_semantic_http_client_span() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "attributes": {
                "server.address": {
                    "value": "github.com",
                    "type": "string"
                },
                "server.port": {
                    "value": 443,
                    "type": "integer"
                },
                "http.request.method": {
                    "value": "GET",
                    "type": "string"
                },
                "url.full": {
                    "value": "https://github.com/rust-lang/rust/issues?labels=E-easy&state=open",
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
          "op": "http.client",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET https://github.com/rust-lang/rust/issues",
          "data": {
            "server.address": "github.com",
            "url.full": "https://github.com/rust-lang/rust/issues?labels=E-easy&state=open",
            "http.request.method": "GET",
            "server.port": 443
          },
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_http_server_span_only_method() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "server",
            "attributes": {
                "http.method": {
                    "value": "GET",
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
          "op": "http.server",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET",
          "data": {
            "http.request_method": "GET"
          },
          "kind": "server"
        }
        "###);
    }

    #[test]
    fn parse_semantic_http_server_span() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "server",
            "attributes": {
                "http.request.method": {
                    "value": "GET",
                    "type": "string"
                },
                "url.path": {
                    "value": "/users",
                    "type": "string"
                },
                "url.scheme": {
                    "value": "GET",
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
          "op": "http.server",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET /users",
          "data": {
            "url.scheme": "GET",
            "http.request.method": "GET",
            "url.path": "/users"
          },
          "kind": "server"
        }
        "###);
    }

    #[test]
    fn parse_database_span_only_system() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "attributes": {
                "db.system": {
                    "value": "postgres",
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
          "op": "db",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {
            "db.system": "postgres"
          },
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_semantic_database_span() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "attributes": {
                "db.system": {
                    "value": "postgres",
                    "type": "string"
                },
                "db.statement": {
                    "value": "SELECT * FROM users",
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
          "op": "db",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "SELECT * FROM users",
          "data": {
            "db.system": "postgres",
            "db.statement": "SELECT * FROM users"
          },
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_gen_ai_span() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "attributes": {
                "gen_ai.system": {
                    "value": "openai",
                    "type": "string"
                },
                "gen_ai.agent.name": {
                    "value": "Seer",
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
          "op": "gen_ai",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {
            "gen_ai.agent.name": "Seer",
            "gen_ai.system": "openai"
          },
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_span_with_sentry_op() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "kind": "client",
            "attributes": {
                "db.system": {
                    "value": "postgres",
                    "type": "string"
                },
                "sentry.op": {
                    "value": "function",
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
          "op": "function",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {
            "db.system": "postgres"
          },
          "kind": "client"
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
          "op": "default",
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

    #[test]
    fn parse_faas_trigger_span() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "name": "FAAS",
            "attributes": {
                "faas.trigger": {
                    "value": "http",
                    "type": "string"
                }
            }
        }"#;
        let span_v2 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v1: SpanV1 = span_v2_to_span_v1(span_v2);
        let annotated_span: Annotated<SpanV1> = Annotated::new(span_v1);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "exclusive_time": 0.0,
          "op": "http",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "FAAS",
          "data": {
            "faas.trigger": "http",
            "sentry.name": "FAAS"
          }
        }
        "###);
    }

    #[test]
    fn parse_http_span_with_route() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "name": "GET /api/users",
            "kind": "server",
            "attributes": {
                "http.method": {
                    "value": "GET",
                    "type": "string"
                },
                "http.route": {
                    "value": "/api/users",
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
          "op": "http.server",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET /api/users",
          "data": {
            "http.request_method": "GET",
            "http.route": "/api/users",
            "sentry.name": "GET /api/users"
          },
          "kind": "server"
        }
        "###);
    }

    #[test]
    fn parse_db_span_with_statement() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "name": "SELECT users",
            "kind": "client",
            "attributes": {
                "db.system": {
                    "value": "postgres",
                    "type": "string"
                },
                "db.statement": {
                    "value": "SELECT * FROM users WHERE id = $1",
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
          "op": "db",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "SELECT * FROM users WHERE id = $1",
          "data": {
            "db.system": "postgres",
            "db.statement": "SELECT * FROM users WHERE id = $1",
            "sentry.name": "SELECT users"
          },
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_http_span_with_graphql() {
        let json = r#"{
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            "span_id": "e342abb1214ca181",
            "parent_span_id": "0c7a7dea069bf5a6",
            "start_timestamp": 123,
            "end_timestamp": 123.5,
            "name": "POST /graphql",
            "kind": "server",
            "attributes": {
                "http.method": {
                    "value": "POST",
                    "type": "string"
                },
                "http.route": {
                    "value": "/graphql",
                    "type": "string"
                },
                "sentry.graphql.operation": {
                    "value": "getUserById",
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
          "op": "http.server",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "POST /graphql (getUserById)",
          "data": {
            "http.request_method": "POST",
            "http.route": "/graphql",
            "sentry.graphql.operation": "getUserById",
            "sentry.name": "POST /graphql"
          },
          "kind": "server"
        }
        "###);
    }
}
