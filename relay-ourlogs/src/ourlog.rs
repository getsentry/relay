use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;

use crate::OtelLog;
use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::{
    OurLog, OurLogAttribute, OurLogAttributeType, OurLogLevel, SpanId, Timestamp, TraceId,
};
use relay_protocol::{Annotated, Error, Object, Value};

fn otel_value_to_log_attribute(value: OtelValue) -> Option<OurLogAttribute> {
    match value {
        OtelValue::BoolValue(v) => Some(OurLogAttribute::new(
            OurLogAttributeType::Boolean,
            Value::Bool(v),
        )),
        OtelValue::DoubleValue(v) => Some(OurLogAttribute::new(
            OurLogAttributeType::Double,
            Value::F64(v),
        )),
        OtelValue::IntValue(v) => Some(OurLogAttribute::new(
            OurLogAttributeType::Integer,
            Value::I64(v),
        )),
        OtelValue::StringValue(v) => Some(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(v),
        )),
        OtelValue::BytesValue(v) => String::from_utf8(v).map_or(None, |str| {
            Some(OurLogAttribute::new(
                OurLogAttributeType::String,
                Value::String(str),
            ))
        }),
        OtelValue::ArrayValue(_) => None,
        OtelValue::KvlistValue(_) => None,
    }
}

/// Transform an OtelLog to a Sentry log.
pub fn otel_to_sentry_log(otel_log: OtelLog) -> Result<OurLog, Error> {
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

    let span_id = SpanId(hex::encode(span_id));
    let trace_id: TraceId = hex::encode(trace_id).parse()?;
    let nanos = time_unix_nano;
    let timestamp = Utc.timestamp_nanos(nanos as i64);

    let body = body
        .and_then(|v| v.value)
        .and_then(|v| match v {
            OtelValue::StringValue(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(String::new);

    let mut attribute_data = Object::new();

    // We ignore the passed observed time since Relay always acts as the collector in Sentry.
    // We may change this in the future with forwarding Relays.
    let observed_time_unix_nano = UnixTimestamp::now().as_nanos();

    attribute_data.insert(
        "sentry.severity_text".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(severity_text.clone()),
        )),
    );
    attribute_data.insert(
        "sentry.severity_number".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::Integer,
            Value::I64(severity_number as i64),
        )),
    );
    attribute_data.insert(
        "sentry.timestamp_nanos".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(time_unix_nano.to_string()),
        )),
    );
    attribute_data.insert(
        "sentry.observed_timestamp_nanos".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(observed_time_unix_nano.to_string()),
        )),
    );
    attribute_data.insert(
        "sentry.trace_flags".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::Integer,
            Value::I64(0),
        )),
    );
    attribute_data.insert(
        "sentry.body".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(body.clone()),
        )),
    );
    attribute_data.insert(
        "sentry.span_id".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(span_id.to_string()),
        )),
    );

    for attribute in attributes.into_iter() {
        if let Some(value) = attribute.value.and_then(|v| v.value) {
            let key = attribute.key;
            if let Some(v) = otel_value_to_log_attribute(value) {
                attribute_data.insert(key, Annotated::new(v));
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

    let mut other = Object::default();
    other.insert(
        "severity_text".to_owned(),
        Annotated::new(Value::String(severity_text)),
    );
    other.insert(
        "severity_number".to_owned(),
        Annotated::new(Value::I64(severity_number as i64)),
    );
    other.insert("trace_flags".to_owned(), Annotated::new(Value::I64(0)));
    other.insert(
        "timestamp_nanos".to_owned(),
        Annotated::new(Value::U64(otel_log.time_unix_nano)),
    );
    other.insert(
        "observed_timestamp_nanos".to_owned(),
        Annotated::new(Value::U64(observed_time_unix_nano)),
    );

    let ourlog = OurLog {
        timestamp: Annotated::new(Timestamp(timestamp)),
        trace_id: Annotated::new(trace_id),
        span_id: Annotated::new(span_id),
        level: Annotated::new(level),
        attributes: Annotated::new(attribute_data),
        body: Annotated::new(body),
        other,
    };

    Ok(ourlog)
}

/// This fills attributes with OTel specific fields to be compatible with the otel schema.
///
/// This also currently backfills data into deprecated fields (other) on the OurLog protocol in order to continue working with the snuba consumers.
///
/// This will need to transform all fields into attributes to be ported to using the generic trace items consumers once they're done.
pub fn ourlog_merge_otel(ourlog: &mut Annotated<OurLog>) {
    let Some(ourlog_value) = ourlog.value_mut() else {
        return;
    };

    let attributes = ourlog_value.attributes.value_mut().get_or_insert_default();
    attributes.insert(
        "sentry.severity_number".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::Integer,
            Value::I64(level_to_otel_severity_number(
                ourlog_value.level.value().cloned(),
            )),
        )),
    );
    attributes.insert(
        "sentry.severity_text".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(
                ourlog_value
                    .level
                    .value()
                    .map(|level| level.to_string())
                    .unwrap_or_else(|| "info".to_owned()),
            ),
        )),
    );
    attributes.insert(
        "sentry.body".to_owned(),
        Annotated::new(OurLogAttribute::new(
            OurLogAttributeType::String,
            Value::String(ourlog_value.body.value().cloned().unwrap_or_default()),
        )),
    );

    if let Some(span_id) = ourlog_value.span_id.value() {
        attributes.insert(
            "sentry.span_id".to_owned(),
            Annotated::new(OurLogAttribute::new(
                OurLogAttributeType::String,
                Value::String(span_id.to_string()),
            )),
        );
    }

    if let Some(value) = ourlog_value
        .attribute("sentry.severity_text")
        .and_then(|v| v.as_str())
    {
        ourlog_value.other.insert(
            "severity_text".to_owned(),
            Annotated::new(Value::String(value.to_owned())),
        );
    }

    if let Some(value) = ourlog_value
        .attribute("sentry.severity_number")
        .and_then(|v| v.value())
    {
        ourlog_value
            .other
            .insert("severity_number".to_owned(), Annotated::new(value.clone()));
    }

    if let Some(value) = ourlog_value
        .attribute("sentry.trace_flags")
        .and_then(|v| v.value())
    {
        ourlog_value
            .other
            .insert("trace_flags".to_owned(), Annotated::new(value.clone()));
    }

    if let Some(value) = ourlog_value
        .attribute("sentry.observed_timestamp_nanos")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
    {
        ourlog_value.other.insert(
            "observed_timestamp_nanos".to_owned(),
            Annotated::new(Value::U64(value)),
        );
    }

    if let Some(value) = ourlog_value
        .attribute("sentry.timestamp_nanos")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
    {
        ourlog_value.other.insert(
            "timestamp_nanos".to_owned(),
            Annotated::new(Value::U64(value)),
        );
    }

    // We ignore the passed observed time since Relay always acts as the collector in Sentry.
    // We may change this in the future with forwarding Relays.
    let observed_time_unix_nano = UnixTimestamp::now().as_nanos();
    ourlog_value.other.insert(
        "observed_timestamp_nanos".to_owned(),
        Annotated::new(Value::U64(observed_time_unix_nano)),
    );

    if let Some(timestamp_nanos) = ourlog_value
        .timestamp
        .value()
        .and_then(|timestamp| timestamp.0.timestamp_nanos_opt())
    {
        ourlog_value.other.insert(
            "timestamp_nanos".to_owned(),
            Annotated::new(Value::U64(timestamp_nanos as u64)),
        );
    }
}

fn level_to_otel_severity_number(level: Option<OurLogLevel>) -> i64 {
    match level {
        Some(OurLogLevel::Trace) => 1,
        Some(OurLogLevel::Debug) => 5,
        Some(OurLogLevel::Info) => 9,
        Some(OurLogLevel::Warn) => 13,
        Some(OurLogLevel::Error) => 17,
        Some(OurLogLevel::Fatal) => 21,
        // 0 is the default value.
        // https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/68e1d6cd94bfca9bdf725327d4221f97ce0e0564/pkg/stanza/docs/types/severity.md
        _ => 0,
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
        let our_log: OurLog = otel_to_sentry_log(otel_log).unwrap();
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
        let our_log = otel_to_sentry_log(otel_log);

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
        let our_log = otel_to_sentry_log(otel_log).unwrap();
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Database query executed".into()))
        );
        assert_eq!(
            annotated_log
                .value()
                .and_then(|v| v.attribute("db.statement"))
                .unwrap()
                .value()
                .and_then(|v| v.as_str()),
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

        let before_test = UnixTimestamp::now().as_nanos();
        let otel_log: OtelLog = serde_json::from_str(json_without_observed_time).unwrap();
        let our_log: OurLog = otel_to_sentry_log(otel_log).unwrap();
        let after_test = UnixTimestamp::now().as_nanos();

        // Get the observed timestamp from attributes
        let observed_timestamp = our_log
            .attribute("sentry.observed_timestamp_nanos")
            .and_then(|value| value.as_str().and_then(|s| s.parse::<u64>().ok()))
            .unwrap_or(0);

        assert!(observed_timestamp > 0);
        assert!(observed_timestamp >= before_test);
        assert!(observed_timestamp <= after_test);
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

        let before_test = UnixTimestamp::now().as_nanos();
        let otel_log: OtelLog = serde_json::from_str(json_with_observed_time).unwrap();
        let our_log: OurLog = otel_to_sentry_log(otel_log).unwrap();
        let after_test = UnixTimestamp::now().as_nanos();

        // Get the observed timestamp from attributes
        let observed_timestamp = our_log
            .attribute("sentry.observed_timestamp_nanos")
            .and_then(|value| value.as_str().and_then(|s| s.parse::<u64>().ok()))
            .unwrap_or(0);

        assert!(observed_timestamp > 0);
        assert!(observed_timestamp >= before_test);
        assert!(observed_timestamp <= after_test);
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
        ourlog_merge_otel(&mut merged_log);

        if let Some(log) = merged_log.value_mut() {
            log.other.insert(
                "observed_timestamp_nanos".to_owned(),
                Annotated::new(Value::U64(1742481864000000000)),
            );
        }

        insta::assert_debug_snapshot!(merged_log, @r#"
        OurLog {
            timestamp: Timestamp(
                2000-01-01T00:00:00Z,
            ),
            trace_id: TraceId("5b8efff798038103d269b633813fc60c"),
            span_id: SpanId(
                "eee19b7ec3c1b174",
            ),
            level: Info,
            body: "Example log record",
            attributes: {
                "foo": OurLogAttribute {
                    value: String(
                        "9",
                    ),
                    type: String,
                    other: {},
                },
                "sentry.body": OurLogAttribute {
                    value: String(
                        "Example log record",
                    ),
                    type: String,
                    other: {},
                },
                "sentry.severity_number": OurLogAttribute {
                    value: I64(
                        9,
                    ),
                    type: Integer,
                    other: {},
                },
                "sentry.severity_text": OurLogAttribute {
                    value: String(
                        "info",
                    ),
                    type: String,
                    other: {},
                },
                "sentry.span_id": OurLogAttribute {
                    value: String(
                        "eee19b7ec3c1b174",
                    ),
                    type: String,
                    other: {},
                },
            },
            other: {
                "observed_timestamp_nanos": U64(
                    1742481864000000000,
                ),
                "severity_number": I64(
                    9,
                ),
                "severity_text": String(
                    "info",
                ),
                "timestamp_nanos": U64(
                    946684800000000000,
                ),
            },
        }
        "#);
    }

    #[test]
    fn ourlog_merge_otel_log_with_unknown_severity_number() {
        let json = r#"{
            "timestamp": 946684800.0,
            "level": "abc",
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

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();
        ourlog_merge_otel(&mut data);
        assert_eq!(
            data.value().unwrap().other.get("severity_number"),
            Some(&Annotated::new(Value::I64(0)))
        );
    }

    #[test]
    #[allow(deprecated)]
    fn ourlog_merge_otel_log_with_timestamp() {
        let mut attributes = Object::new();
        attributes.insert(
            "foo".to_owned(),
            Annotated::new(OurLogAttribute::new(
                OurLogAttributeType::String,
                Value::String("9".to_owned()),
            )),
        );
        let datetime = Utc.with_ymd_and_hms(2021, 11, 29, 0, 0, 0).unwrap();
        let mut ourlog = Annotated::new(OurLog {
            timestamp: Annotated::new(Timestamp(datetime)),
            attributes: Annotated::new(attributes),
            body: Annotated::new("somebody".into()),
            ..Default::default()
        });
        ourlog_merge_otel(&mut ourlog);

        if let Some(log) = ourlog.value_mut() {
            log.other.insert(
                "observed_timestamp_nanos".to_owned(),
                Annotated::new(Value::U64(1742481864000000000)),
            );
        }

        insta::assert_debug_snapshot!(ourlog, @r#"
        OurLog {
            timestamp: Timestamp(
                2021-11-29T00:00:00Z,
            ),
            trace_id: ~,
            span_id: ~,
            level: ~,
            body: "somebody",
            attributes: {
                "foo": OurLogAttribute {
                    value: String(
                        "9",
                    ),
                    type: String,
                    other: {},
                },
                "sentry.body": OurLogAttribute {
                    value: String(
                        "somebody",
                    ),
                    type: String,
                    other: {},
                },
                "sentry.severity_number": OurLogAttribute {
                    value: I64(
                        0,
                    ),
                    type: Integer,
                    other: {},
                },
                "sentry.severity_text": OurLogAttribute {
                    value: String(
                        "info",
                    ),
                    type: String,
                    other: {},
                },
            },
            other: {
                "observed_timestamp_nanos": U64(
                    1742481864000000000,
                ),
                "severity_number": I64(
                    0,
                ),
                "severity_text": String(
                    "info",
                ),
                "timestamp_nanos": U64(
                    1638144000000000000,
                ),
            },
        }
        "#);

        insta::assert_json_snapshot!(SerializableAnnotated(&ourlog), @r#"
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
            "sentry.severity_number": {
              "type": "integer",
              "value": 0
            },
            "sentry.severity_text": {
              "type": "string",
              "value": "info"
            }
          },
          "observed_timestamp_nanos": 1742481864000000000,
          "severity_number": 0,
          "severity_text": "info",
          "timestamp_nanos": 1638144000000000000
        }
        "#);
    }
}
