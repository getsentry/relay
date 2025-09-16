use std::borrow::Cow;

use relay_event_schema::protocol::{
    Attribute, Attributes, Span as SpanV1, SpanData, SpanKind as SpanV1Kind, SpanLink,
    SpanStatus as SpanV1Status, SpanV2, SpanV2Kind, SpanV2Link, SpanV2Status,
};
use relay_protocol::{Annotated, Empty, IntoValue, Value};

const MILLIS_TO_NANOS: f64 = 1000. * 1000.;

#[allow(dead_code)]
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
        performance_issues_spans: _performance_issues_spans,
        other,
    } = span_v1;

    let mut annotated_attributes = attributes_from_data(data);
    let attributes = annotated_attributes.get_or_insert_with(Default::default);

    // Top-level fields have higher precedence than `data`:
    attributes.insert(
        "sentry.exclusive_time_nano", // TODO: update conventions, they list `sentry.exclusive_time`
        exclusive_time.map_value(|v| (v * MILLIS_TO_NANOS) as i64),
    );
    attributes.insert("sentry.op", op);

    attributes.insert("sentry.segment.id", segment_id.map_value(|v| v.to_string())); // TODO: test
    attributes.insert("sentry.is_segment", is_segment);
    attributes.insert("sentry.description", description);
    attributes.insert("sentry.origin", origin);
    attributes.insert("sentry.profile_id", profile_id.map_value(|v| v.to_string())); // TODO: test
    attributes.insert("sentry.platform", platform);
    attributes.insert("sentry.was_transaction", was_transaction);
    attributes.insert(
        "sentry._internal.performance_issues_spans",
        _performance_issues_spans,
    );

    // Use same precedence as `backfill_data` for data bags:
    if let Some(measurements) = measurements.into_value() {
        for (key, measurement) in measurements.0 {
            if let Some(measurement) = measurement.into_value() {
                let key = match key.as_str() {
                    "client_sample_rate" => "sentry.client_sample_rate",
                    "server_sample_rate" => "sentry.server_sample_rate",
                    other => other,
                };
                attributes.insert_if_missing(key, || measurement.value.map_value(|a| a.to_f64()));
            }
        }
    }
    if let Some(tags) = tags.into_value() {
        for (key, value) in tags {
            // TODO: special cases (see backfill_data)
            if !attributes.contains_key(&key) {
                attributes.insert_raw(
                    key,
                    Attribute::annotated_from_value(value.map_value(IntoValue::into_value)),
                );
            }
        }
    }
    if let Some(tags) = sentry_tags.into_value() {
        if let Value::Object(tags) = tags.into_value() {
            for (key, value) in tags {
                let key = match key.as_str() {
                    "description" => "sentry.normalized_description".into(),
                    other => Cow::Owned(format!("sentry.{}", other)),
                };
                if !value.is_empty() && !attributes.contains_key(key.as_ref()) {
                    attributes.insert_raw(key.into_owned(), Attribute::annotated_from_value(value));
                }
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
        kind: Annotated::map_value(kind, span_v1_kind_to_span_v2_kind),
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
                 }| {
                    SpanV2Link {
                        trace_id,
                        span_id,
                        sampled,
                        attributes: attributes.map_value(|attrs| {
                            Attributes::from_iter(
                                attrs.into_iter().map(|(key, value)| {
                                    (key, Attribute::annotated_from_value(value))
                                }),
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
        return Annotated(None, meta);
    };

    Annotated::new(Attributes::from_iter(data.into_iter().filter_map(
        |(key, value)| (!value.is_empty()).then_some((key, Attribute::annotated_from_value(value))),
    )))
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::Measurement;
    use relay_protocol::{FiniteF64, SerializableAnnotated};

    use crate::span_v2_to_span_v1;

    use super::*;

    #[test]
    fn roundtrip() {
        let json = r#"{
  "timestamp": 0.0,
  "start_timestamp": -63158400.0,
  "exclusive_time": 1.23,
  "op": "operation",
  "span_id": "fa90fdead5f74052",
  "parent_span_id": "fa90fdead5f74051",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "segment_id": "fa90fdead5f74050",
  "is_segment": true,
  "is_remote": true,
  "status": "ok",
  "description": "raw description",
  "tags": {
    "foo": "bar"
  },
  "origin": "auto.http",
  "profile_id": "4c79f60c11214eb38604f4ae0781bfb0",
  "data": {
    "my.data.field": "my.data.value"
  },
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
  "sentry_tags": {
    "user": "id:user123",
    "description": "normalized description"
  },
  "received": 0.2,
  "measurements": {
    "client_sample_rate": {
      "value": 0.11
    },
    "server_sample_rate": {
      "value": 0.22
    },
    "memory": {
      "value": 9001.0,
      "unit": "byte"
    }
  },
  "platform": "javascript",
  "was_transaction": true,
  "kind": "server",
  "_performance_issues_spans": true,
  "additional_field": "additional field value"
}"#;

        let span_v1 = Annotated::from_json(json).unwrap().into_value().unwrap();
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
            "sentry.exclusive_time_nano": {
              "type": "integer",
              "value": 1230000
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
          "additional_field": "additional field value"
        }
        "###);

        let mut reconstructed_span_v1 = span_v2_to_span_v1(annotated_span_v2.into_value().unwrap());

        // User tags cannot be converted losslessly:
        let data = reconstructed_span_v1.data.value_mut().as_mut().unwrap();
        let tags = reconstructed_span_v1
            .tags
            .get_or_insert_with(Default::default);
        let value = data.other.remove("foo").unwrap().into_value().unwrap();
        tags.insert(
            dbg!("foo").to_owned(),
            Annotated::new(dbg!(value).as_str().unwrap().to_owned().into()),
        );

        // Measurements cannot be converted losslessly:
        let measurements = reconstructed_span_v1
            .measurements
            .get_or_insert_with(Default::default);
        for key in ["client_sample_rate", "memory", "server_sample_rate"] {
            let value = data
                .other
                .remove(match key {
                    "memory" => "memory",
                    "client_sample_rate" => "sentry.client_sample_rate",
                    "server_sample_rate" => "sentry.server_sample_rate",
                    _ => panic!(),
                })
                .unwrap()
                .into_value()
                .unwrap();
            measurements.insert(
                key.to_owned(),
                Annotated::new(Measurement {
                    value: FiniteF64::new(value.as_f64().unwrap()).unwrap().into(),
                    unit: Annotated::empty(),
                }),
            );
        }

        assert_eq!(
            json,
            Annotated::new(reconstructed_span_v1)
                .to_json_pretty()
                .unwrap(),
        );
    }
}
