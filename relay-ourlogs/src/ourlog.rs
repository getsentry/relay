use chrono::{DateTime, TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;

use crate::OtelLog;
use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::{
    Attribute, AttributeType, OurLog, OurLogLevel, SpanId, Timestamp, TraceId,
};
use relay_event_schema::protocol::{Attributes, datetime_to_timestamp};
use relay_protocol::{Annotated, Error, Object, Value};

fn otel_value_to_log_attribute(value: OtelValue) -> Option<Attribute> {
    match value {
        OtelValue::BoolValue(v) => Some(Attribute::new(AttributeType::Boolean, Value::Bool(v))),
        OtelValue::DoubleValue(v) => Some(Attribute::new(AttributeType::Double, Value::F64(v))),
        OtelValue::IntValue(v) => Some(Attribute::new(AttributeType::Integer, Value::I64(v))),
        OtelValue::StringValue(v) => Some(Attribute::new(AttributeType::String, Value::String(v))),
        OtelValue::BytesValue(v) => String::from_utf8(v).map_or(None, |str| {
            Some(Attribute::new(AttributeType::String, Value::String(str)))
        }),
        OtelValue::ArrayValue(_) => None,
        OtelValue::KvlistValue(_) => None,
    }
}

/// Transform an OtelLog to a Sentry log.
pub fn otel_to_sentry_log(otel_log: OtelLog, received_at: DateTime<Utc>) -> Result<OurLog, Error> {
    let OtelLog {
        severity_number,
        severity_text,
        body,
        attributes,
        trace_id,
        span_id,
        time_unix_nano,
        ..
    } = otel_log;
    let span_id = SpanId::try_from(span_id.as_slice())?;
    let trace_id = TraceId::try_from(trace_id.as_slice())?;
    let timestamp = Utc.timestamp_nanos(time_unix_nano as i64);
    let body = body
        .and_then(|v| v.value)
        .and_then(|v| match v {
            OtelValue::StringValue(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(String::new);

    let received_at_nanos = received_at
        .timestamp_nanos_opt()
        .unwrap_or_else(|| UnixTimestamp::now().as_nanos() as i64);

    let mut attribute_data = Attributes::default();

    attribute_data.insert("sentry.severity_text".to_owned(), severity_text.clone());
    attribute_data.insert("sentry.severity_number".to_owned(), severity_number as i64);
    attribute_data.insert(
        "sentry.timestamp_nanos".to_owned(),
        time_unix_nano.to_string(),
    );
    attribute_data.insert("sentry.timestamp_precise".to_owned(), time_unix_nano as i64);
    attribute_data.insert(
        "sentry.observed_timestamp_nanos".to_owned(),
        received_at_nanos.to_string(),
    );
    attribute_data.insert("sentry.trace_flags".to_owned(), 0);
    attribute_data.insert("sentry.body".to_owned(), body.clone());
    attribute_data.insert("sentry.span_id".to_owned(), span_id.to_string());

    for attribute in attributes.into_iter() {
        if let Some(value) = attribute.value.and_then(|v| v.value) {
            let key = attribute.key;
            if let Some(v) = otel_value_to_log_attribute(value) {
                attribute_data.insert_raw(key, Annotated::new(v));
            }
        }
    }

    // Map severity_number to OurLogLevel, falling back to severity_text if it's not a number.
    // Finally default to Info if severity_number is not in range and severity_text is not a valid
    // log level.
    let level = match severity_number {
        1..=4 => OurLogLevel::Trace,
        5..=8 => OurLogLevel::Debug,
        9..=12 => OurLogLevel::Info,
        13..=16 => OurLogLevel::Warn,
        17..=20 => OurLogLevel::Error,
        21..=24 => OurLogLevel::Fatal,
        _ => match severity_text.as_str() {
            "trace" => OurLogLevel::Trace,
            "debug" => OurLogLevel::Debug,
            "info" => OurLogLevel::Info,
            "warn" => OurLogLevel::Warn,
            "error" => OurLogLevel::Error,
            "fatal" => OurLogLevel::Fatal,
            _ => OurLogLevel::Info,
        },
    };
    let ourlog = OurLog {
        timestamp: Annotated::new(Timestamp(timestamp)),
        trace_id: Annotated::new(trace_id),
        span_id: Annotated::new(span_id),
        level: Annotated::new(level),
        attributes: Annotated::new(attribute_data),
        body: Annotated::new(body),
        other: Object::default(),
    };

    Ok(ourlog)
}

/// This fills attributes with OTel specific fields to be compatible with the OTel schema.
pub fn ourlog_merge_otel(ourlog: &mut Annotated<OurLog>, received_at: DateTime<Utc>) {
    let Some(ourlog_value) = ourlog.value_mut() else {
        return;
    };
    let attributes = ourlog_value.attributes.value_mut().get_or_insert_default();
    // We can only extract microseconds as the conversion from float to Timestamp
    // messes up with the precision and nanoseconds are never preserved.
    let timestamp_nanos = ourlog_value
        .timestamp
        .value()
        .map(|timestamp| {
            ((datetime_to_timestamp(timestamp.into_inner()) * 1e6).round() as i64) * 1000
        })
        .unwrap_or_default();

    let received_at_nanos = received_at
        .timestamp_nanos_opt()
        .unwrap_or_else(|| UnixTimestamp::now().as_nanos() as i64);

    attributes.insert(
        "sentry.severity_text".to_owned(),
        ourlog_value
            .level
            .value()
            .map(|level| level.to_string())
            .unwrap_or_else(|| "info".to_owned()),
    );
    attributes.insert(
        "sentry.timestamp_nanos".to_owned(),
        timestamp_nanos.to_string(),
    );
    attributes.insert("sentry.timestamp_precise".to_owned(), timestamp_nanos);
    attributes.insert(
        "sentry.observed_timestamp_nanos".to_owned(),
        received_at_nanos.to_string(),
    );
    attributes.insert("sentry.trace_flags".to_owned(), 0);
    attributes.insert(
        "sentry.body".to_owned(),
        ourlog_value.body.value().cloned().unwrap_or_default(),
    );

    if let Some(span_id) = ourlog_value.span_id.value() {
        attributes.insert("sentry.span_id".to_owned(), span_id.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{SerializableAnnotated, get_path};

    #[test]
    fn parse_otel_log() {
        // https://github.com/open-telemetry/opentelemetry-proto/blob/c4214b8168d0ce2a5236185efb8a1c8950cccdd6/examples/logs.json
        let json = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 10,
            "severityText": "Information",
            "traceId": "5B8EFFF798038103D269B633813FC60C",
            "spanId": "EEE19B7EC3C1B174",
            "body": {
                "stringValue": "Example log record"
            },
            "attributes": [
                {
                    "key": "string.attribute",
                    "value": {
                        "stringValue": "some string"
                    }
                },
                {
                    "key": "boolean.attribute",
                    "value": {
                        "boolValue": true
                    }
                },
                {
                    "key": "int.attribute",
                    "value": {
                        "intValue": "10"
                    }
                },
                {
                    "key": "double.attribute",
                    "value": {
                        "doubleValue": 637.704
                    }
                },
                {
                    "key": "array.attribute",
                    "value": {
                        "arrayValue": {
                            "values": [
                                {
                                    "stringValue": "many"
                                },
                                {
                                    "stringValue": "values"
                                }
                            ]
                        }
                    }
                },
                {
                    "key": "map.attribute",
                    "value": {
                        "kvlistValue": {
                            "values": [
                                {
                                    "key": "some.map.key",
                                    "value": {
                                        "stringValue": "some value"
                                    }
                                }
                            ]
                        }
                    }
                }
            ]
        }"#;

        let otel_log: OtelLog = serde_json::from_str(json).unwrap();
        let our_log: OurLog =
            otel_to_sentry_log(otel_log, DateTime::from_timestamp_nanos(946684800000000000))
                .unwrap();
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);
        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Example log record".into()))
        );
    }

    #[test]
    fn parse_otellog_with_invalid_trace_id() {
        let json = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 10,
            "severityText": "Information",
            "traceId": "",
            "spanId": "EEE19B7EC3C1B174"
        }"#;

        let otel_log: OtelLog = serde_json::from_str(json).unwrap();
        let our_log =
            otel_to_sentry_log(otel_log, DateTime::from_timestamp_nanos(946684800000000000));

        assert!(our_log.is_err());
    }

    #[test]
    fn parse_otel_log_with_db_attributes() {
        let json = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 10,
            "severityText": "Information",
            "traceId": "5B8EFFF798038103D269B633813FC60C",
            "spanId": "EEE19B7EC3C1B174",
            "body": {
                "stringValue": "Database query executed"
            },
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
        let otel_log: OtelLog = serde_json::from_str(json).unwrap();
        let our_log =
            otel_to_sentry_log(otel_log, DateTime::from_timestamp_nanos(946684800000000000))
                .unwrap();
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Database query executed".into()))
        );
        assert_eq!(
            annotated_log
                .value()
                .unwrap()
                .attributes
                .value()
                .unwrap()
                .get_value("db.statement")
                .unwrap()
                .as_str(),
            Some("SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s")
        );
    }

    #[test]
    fn parse_otel_log_without_observed_time() {
        let json_without_observed_time = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "0",
            "severityNumber": 10,
            "severityText": "Information",
            "traceId": "5B8EFFF798038103D269B633813FC60C",
            "spanId": "EEE19B7EC3C1B174",
            "body": {
                "stringValue": "Example log record"
            },
            "attributes": []
        }"#;

        let otel_log: OtelLog = serde_json::from_str(json_without_observed_time).unwrap();
        let our_log =
            otel_to_sentry_log(otel_log, DateTime::from_timestamp_nanos(946684800000000000))
                .unwrap();

        let observed_timestamp = our_log
            .attributes
            .value()
            .and_then(|attrs| {
                attrs
                    .get_value("sentry.observed_timestamp_nanos")?
                    .as_str()?
                    .parse::<u64>()
                    .ok()
            })
            .unwrap();

        assert_eq!(observed_timestamp, 946684800000000000);
    }

    #[test]
    fn parse_otel_log_ignores_observed_time() {
        let json_with_observed_time = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 10,
            "severityText": "Information",
            "traceId": "5B8EFFF798038103D269B633813FC60C",
            "spanId": "EEE19B7EC3C1B174",
            "body": {
                "stringValue": "Example log record"
            },
            "attributes": []
        }"#;

        let otel_log: OtelLog = serde_json::from_str(json_with_observed_time).unwrap();
        let our_log: OurLog =
            otel_to_sentry_log(otel_log, DateTime::from_timestamp_nanos(946684800000000000))
                .unwrap();

        let observed_timestamp = our_log
            .attributes
            .value()
            .and_then(|attrs| {
                attrs
                    .get_value("sentry.observed_timestamp_nanos")?
                    .as_str()?
                    .parse::<u64>()
                    .ok()
            })
            .unwrap();

        assert_ne!(observed_timestamp, 1544712660300000000);
    }

    #[test]
    fn ourlog_merge_otel_log() {
        let json = r#"{
            "timestamp": 946684800.0,
            "level": "info",
            "trace_id": "5B8EFFF798038103D269B633813FC60C",
            "span_id": "EEE19B7EC3C1B174",
            "body": "Example log record",
            "attributes": {
                "foo": {
                    "value": "9",
                    "type": "string"
                }
            }
        }"#;

        let mut merged_log = Annotated::<OurLog>::from_json(json).unwrap();
        ourlog_merge_otel(
            &mut merged_log,
            DateTime::from_timestamp_nanos(946684800000000000),
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&merged_log), @r###"
        {
          "timestamp": 946684800.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Example log record",
          "attributes": {
            "foo": {
              "type": "string",
              "value": "9"
            },
            "sentry.body": {
              "type": "string",
              "value": "Example log record"
            },
            "sentry.observed_timestamp_nanos": {
              "type": "string",
              "value": "946684800000000000"
            },
            "sentry.severity_text": {
              "type": "string",
              "value": "info"
            },
            "sentry.span_id": {
              "type": "string",
              "value": "eee19b7ec3c1b174"
            },
            "sentry.timestamp_nanos": {
              "type": "string",
              "value": "946684800000000000"
            },
            "sentry.timestamp_precise": {
              "type": "integer",
              "value": 946684800000000000
            },
            "sentry.trace_flags": {
              "type": "integer",
              "value": 0
            }
          }
        }
        "###);
    }

    #[test]
    #[allow(deprecated)]
    fn ourlog_merge_otel_log_with_timestamp() {
        let mut attributes = Attributes::new();
        attributes.insert("foo".to_owned(), "9".to_owned());
        let datetime = Utc.with_ymd_and_hms(2021, 11, 29, 0, 0, 0).unwrap();
        let mut ourlog = Annotated::new(OurLog {
            timestamp: Annotated::new(Timestamp(datetime)),
            attributes: Annotated::new(attributes),
            body: Annotated::new("somebody".into()),
            ..Default::default()
        });

        ourlog_merge_otel(
            &mut ourlog,
            DateTime::from_timestamp_nanos(946684800000000000),
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&ourlog), @r###"
        {
          "timestamp": 1638144000.0,
          "body": "somebody",
          "attributes": {
            "foo": {
              "type": "string",
              "value": "9"
            },
            "sentry.body": {
              "type": "string",
              "value": "somebody"
            },
            "sentry.observed_timestamp_nanos": {
              "type": "string",
              "value": "946684800000000000"
            },
            "sentry.severity_text": {
              "type": "string",
              "value": "info"
            },
            "sentry.timestamp_nanos": {
              "type": "string",
              "value": "1638144000000000000"
            },
            "sentry.timestamp_precise": {
              "type": "integer",
              "value": 1638144000000000000
            },
            "sentry.trace_flags": {
              "type": "integer",
              "value": 0
            }
          }
        }
        "###);
    }
}
