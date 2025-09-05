use std::collections::BTreeMap;

use relay_event_schema::protocol::{
    Attribute, AttributeValue, Attributes, Span as SpanV1, SpanData, SpanKind as SpanV1Kind,
    SpanLink, SpanStatus as SpanV1Status, SpanV2, SpanV2Kind, SpanV2Link, SpanV2Status,
};
use relay_protocol::{Annotated, FromValue, IntoValue, Value};

pub fn span_v1_to_span_v2(span_v1: SpanV1) -> SpanV2 {
    let SpanV1 {
        timestamp,
        start_timestamp,
        exclusive_time: _exclusive_time, // set by span consumer
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
        _performance_issues_spans,
        other,
    } = span_v1;

    let mut attributes = attributes_from_data(data);
    if let Some(attributes) = attributes.value_mut() {
        // Top-level fields have higher precedence than `data`:
        attributes.insert("sentry.op", op);
        if let Some(segment_id) = segment_id.into_value() {
            attributes.insert("sentry.segment.id", segment_id.to_string()); // TODO: test
        }
        attributes.insert("sentry.is_segment", is_segment);
        attributes.insert("sentry.description", description);
        attributes.insert("sentry.origin", origin);
        if let Some(profile_id) = profile_id.into_value() {
            attributes.insert("sentry.profile_id", profile_id.0.to_string()); // TODO: test
        }
        attributes.insert("sentry.platform", platform);
        attributes.insert("sentry.was_transaction", was_transaction);

        // Use same precedence as `backfill_data` for data bags:
        if let Some(measurements) = measurements.into_value() {
            for (key, measurement) in measurements.0 {
                if let Some(measurement) = measurement.into_value() {
                    attributes
                        .insert_if_missing(&key, || measurement.value.map_value(|a| a.to_f64()));
                }
            }
        }
        if let Some(tags) = tags.into_value() {
            for (key, value) in tags {
                // TODO: exceptions (see backfill_data)
                if let Some(value) = value.into_value() {
                    attributes.insert_if_missing(&key, || value.0);
                }
            }
        }
        if let Some(tags) = sentry_tags.into_value() {
            if let Value::Object(tags) = tags.into_value() {
                for (key, value) in tags {
                    if value.value().is_some() {
                        if let Some(value) = AttributeValue::from_value(value).into_value() {
                            attributes.insert_if_missing(&key, || value);
                        }
                    }
                }
            }
        }
    }

    SpanV2 {
        trace_id,
        parent_span_id,
        span_id,
        name: attributes
            .value()
            .and_then(|attrs| attrs.get_value("sentry.name"))
            .and_then(|v| Some(v.as_str()?.to_owned()))
            .into(),
        status: Annotated::map_value(status, span_v1_status_to_span_v2_status),
        is_remote,
        kind: Annotated::map_value(kind, span_v1_kind_to_span_v2_kind),
        start_timestamp,
        end_timestamp: timestamp,
        links: links.map_value(span_v1_links_to_span_v2_links),
        attributes,
        other,
    }
}

fn span_v1_status_to_span_v2_status(status: SpanV1Status) -> SpanV2Status {
    match status {
        SpanV1Status::Ok => SpanV2Status::Ok,
        _ => SpanV2Status::Error,
    }
}

fn span_v1_kind_to_span_v2_kind(kind: SpanV1Kind) -> SpanV2Kind {
    match kind {
        SpanV1Kind::Internal => SpanV2Kind::Internal,
        SpanV1Kind::Server => SpanV2Kind::Server,
        SpanV1Kind::Client => SpanV2Kind::Client,
        SpanV1Kind::Producer => SpanV2Kind::Producer,
        SpanV1Kind::Consumer => SpanV2Kind::Consumer,
        // TODO: implement catchall type so outdated customer relays can still forward the field.
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
                 }| SpanV2Link {
                    trace_id,
                    span_id,
                    sampled,
                    attributes: attributes.map_value(|attrs| {
                        Attributes::from_iter(attrs.into_iter().filter_map(|(key, value)| {
                            Some((
                                key,
                                Attribute {
                                    value: AttributeValue::from_value(value.into_value()?.into())
                                        .into_value()?,
                                    other: BTreeMap::new(),
                                }
                                .into(),
                            ))
                        }))
                    }),
                    other,
                },
            )
        })
        .collect()
}

fn attributes_from_data(data: Annotated<SpanData>) -> Annotated<Attributes> {
    let Some(data) = data.into_value() else {
        return Annotated::empty();
    };
    let Value::Object(data) = data.into_value() else {
        return Annotated::empty();
    };

    Annotated::new(Attributes::from_iter(data.into_iter().filter_map(
        |(key, value)| {
            Some((
                key,
                Attribute {
                    value: AttributeValue::from_value(value.into_value()?.into()).into_value()?,
                    other: BTreeMap::new(),
                }
                .into(),
            ))
        },
    )))
}

#[cfg(test)]
mod tests {
    use relay_protocol::SerializableAnnotated;

    use crate::span_v2_to_span_v1;

    use super::*;

    #[test]
    fn roundtrip() {
        let json = r#"{
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "sentry.category": "db",
                "sentry.name": "my 1st OTel span",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
            },
            "description": "my 1st OTel span",
            "downsampled_retention_days": 90,
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": true,
            "is_remote": false,
            "links": [
                {
                    "trace_id": "89143b0763095bd9c9955e8175d1fb24",
                    "span_id": "e342abb1214ca183",
                    "sampled": false,
                    "attributes": {"link_double_key": 1.23}
                }
            ],
            "measurements": {"score.total": {"value": 0.12121616}},
            "organization_id": 1,
            "project_id": 42,
            "key_id": 123,
            "retention_days": 90,
            "segment_id": "a342abb1214ca181",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "db",
                "op": "default",
                "status": "unknown"
            },
            "tags": {
                "foo": "bar"
            },
            "span_id": "a342abb1214ca181",
            "start_timestamp_ms": 1234,
            "start_timestamp_precise": 1.234,
            "end_timestamp_precise": 1.235,
            "trace_id": "89143b0763095bd9c9955e8175d1fb23"
        }"#;

        let span_v1 = Annotated::from_json(json).unwrap().into_value().unwrap();
        let span_v2 = span_v1_to_span_v2(span_v1);

        let annotated_span: Annotated<SpanV2> = Annotated::new(span_v2);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "span_id": "a342abb1214ca181",
          "is_remote": false,
          "links": [
            {
              "trace_id": "89143b0763095bd9c9955e8175d1fb24",
              "span_id": "e342abb1214ca183",
              "sampled": false,
              "attributes": {}
            }
          ],
          "attributes": {
            "foo": {
              "type": "string",
              "value": "bar"
            },
            "score.total": {
              "type": "double",
              "value": 0.12121616
            },
            "sentry.description": {
              "type": "string",
              "value": "my 1st OTel span"
            },
            "sentry.is_segment": {
              "type": "boolean",
              "value": true
            },
            "sentry.segment.id": {
              "type": "string",
              "value": "a342abb1214ca181"
            }
          },
          "downsampled_retention_days": 90,
          "duration_ms": 500,
          "end_timestamp_precise": 1.235,
          "exclusive_time_ms": 500.0,
          "key_id": 123,
          "organization_id": 1,
          "project_id": 42,
          "retention_days": 90,
          "start_timestamp_ms": 1234,
          "start_timestamp_precise": 1.234
        }
        "###);

        let span_v1 = span_v2_to_span_v1(annotated_span.into_value().unwrap());
        assert_eq!(Annotated::new(span_v1).to_json_pretty().unwrap(), json);
    }
}
