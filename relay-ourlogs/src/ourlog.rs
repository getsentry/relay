use chrono::Utc;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;

use crate::OtelLog;
use relay_event_schema::protocol::{AttributeValue, OurLog, SpanId, TraceId};
use relay_protocol::{Annotated, Object};

/// Transform an OtelLog to a Sentry log.
pub fn otel_to_sentry_log(otel_log: OtelLog) -> OurLog {
    let OtelLog {
        severity_number,
        severity_text,
        body,
        attributes,
        trace_id,
        span_id,
        ..
    } = otel_log;

    let span_id = hex::encode(span_id);
    let trace_id = hex::encode(trace_id);

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
    let observed_time_unix_nano = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

    for attribute in attributes.into_iter() {
        if let Some(value) = attribute.value.and_then(|v| v.value) {
            let key = attribute.key;
            match value {
                OtelValue::ArrayValue(_) => {}
                OtelValue::BoolValue(v) => {
                    attribute_data.insert(key, Annotated::new(AttributeValue::BoolValue(v)));
                }
                OtelValue::BytesValue(v) => {
                    if let Ok(v) = String::from_utf8(v) {
                        attribute_data.insert(key, Annotated::new(AttributeValue::StringValue(v)));
                    }
                }
                OtelValue::DoubleValue(v) => {
                    attribute_data.insert(key, Annotated::new(AttributeValue::DoubleValue(v)));
                }
                OtelValue::IntValue(v) => {
                    attribute_data.insert(key, Annotated::new(AttributeValue::IntValue(v)));
                }
                OtelValue::KvlistValue(_) => {}
                OtelValue::StringValue(v) => {
                    attribute_data.insert(key, Annotated::new(AttributeValue::StringValue(v)));
                }
            }
        }
    }

    OurLog {
        timestamp_nanos: Annotated::new(otel_log.time_unix_nano),
        observed_timestamp_nanos: Annotated::new(observed_time_unix_nano),
        trace_id: TraceId(trace_id).into(),
        span_id: Annotated::new(SpanId(span_id)),
        trace_flags: Annotated::new(0),
        severity_text: severity_text.into(),
        severity_number: Annotated::new(severity_number as i64),
        attributes: attribute_data.into(),
        body: Annotated::new(body),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{get_path, get_value};

    #[test]
    fn parse_log() {
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
        let our_log: OurLog = otel_to_sentry_log(otel_log);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);
        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Example log record".into()))
        );
    }

    #[test]
    fn parse_log_with_db_attributes() {
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
        let our_log = otel_to_sentry_log(otel_log);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Database query executed".into()))
        );
        assert_eq!(
            get_value!(annotated_log.attributes["db.statement"]!).string_value(),
            Some(&"SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s".into())
        );
    }

    #[test]
    fn parse_log_without_observed_time() {
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

        let before_test = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let otel_log: OtelLog = serde_json::from_str(json_without_observed_time).unwrap();
        let our_log: OurLog = otel_to_sentry_log(otel_log);
        let after_test = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        let observed_time = our_log.observed_timestamp_nanos.value().unwrap();
        assert!(*observed_time > 0);
        assert!(*observed_time >= before_test);
        assert!(*observed_time <= after_test);
    }

    #[test]
    fn parse_log_ignores_observed_time() {
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

        let before_test = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let otel_log: OtelLog = serde_json::from_str(json_with_observed_time).unwrap();
        let our_log: OurLog = otel_to_sentry_log(otel_log);
        let after_test = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        let observed_time = our_log.observed_timestamp_nanos.value().unwrap();
        assert!(*observed_time > 0);
        assert!(*observed_time >= before_test);
        assert!(*observed_time <= after_test);

        assert_ne!(
            our_log.observed_timestamp_nanos,
            Annotated::new(1544712660300000000)
        );
    }
}
