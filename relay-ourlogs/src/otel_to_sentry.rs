//! Transforms OpenTelemetry logs to Sentry logs.
//!
//! This module provides functionality to convert OpenTelemetry log records
//! into Sentry's internal log format (`OurLog`).

use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord as OtelLogRecord;

use opentelemetry_proto::tonic::resource::v1::Resource;
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, SpanId, Timestamp, TraceId};
use relay_otel::otel_value_to_attribute;
use relay_protocol::{Annotated, Meta, Object, Remark, RemarkType};

/// Maps OpenTelemetry severity number to Sentry log level.
///
/// This function maps OpenTelemetry severity numbers according to the OpenTelemetry specification:
/// - 1-4: Trace
/// - 5-8: Debug
/// - 9-12: Info
/// - 13-16: Warn
/// - 17-20: Error
/// - 21-24: Fatal
///
/// If the severity number is out of range, it falls back to parsing the severity text.
fn map_severity_to_level(severity_number: i32, severity_text: &str) -> OurLogLevel {
    match severity_number {
        1..=4 => OurLogLevel::Trace,
        5..=8 => OurLogLevel::Debug,
        9..=12 => OurLogLevel::Info,
        13..=16 => OurLogLevel::Warn,
        17..=20 => OurLogLevel::Error,
        21..=24 => OurLogLevel::Fatal,
        _ => match severity_text.to_lowercase().as_str() {
            "trace" => OurLogLevel::Trace,
            "debug" => OurLogLevel::Debug,
            "info" | "information" => OurLogLevel::Info,
            "warn" | "warning" => OurLogLevel::Warn,
            "error" => OurLogLevel::Error,
            "fatal" => OurLogLevel::Fatal,
            _ => OurLogLevel::Info, // Default fallback
        },
    }
}

/// Transforms an OpenTelemetry log record to a Sentry log.
pub fn otel_to_sentry_log(
    otel_log: OtelLogRecord,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> OurLog {
    let OtelLogRecord {
        time_unix_nano,
        severity_number,
        severity_text,
        body,
        attributes,
        trace_id,
        span_id,
        ..
    } = otel_log;

    let span_id = SpanId::try_from(span_id.as_slice())
        .map(Annotated::new)
        .unwrap_or_default();
    let trace_id = match TraceId::try_from(trace_id.as_slice()) {
        Ok(id) => Annotated::new(id),
        Err(_) => {
            // Generate a new trace ID when missing or invalid
            let generated_id = TraceId::random();
            let mut meta = Meta::default();
            meta.add_remark(Remark::new(RemarkType::Substituted, "trace_id.missing"));
            Annotated(Some(generated_id), meta)
        }
    };
    let timestamp = Utc.timestamp_nanos(time_unix_nano as i64);
    let level = map_severity_to_level(severity_number, &severity_text);
    let body = body.and_then(|v| v.value).and_then(|v| match v {
        OtelValue::StringValue(s) => Some(s),
        _ => None,
    });

    let mut attribute_data = Attributes::default();

    for attribute in resource.into_iter().flat_map(|s| &s.attributes) {
        if let Some(attr) = attribute
            .value
            .clone()
            .and_then(|v| v.value)
            .and_then(otel_value_to_attribute)
        {
            let key = format!("resource.{}", attribute.key);
            attribute_data.insert_raw(key, Annotated::new(attr));
        }
    }

    for attribute in scope.into_iter().flat_map(|s| &s.attributes) {
        if let Some(attr) = attribute
            .value
            .clone()
            .and_then(|v| v.value)
            .and_then(otel_value_to_attribute)
        {
            let key = format!("instrumentation.{}", attribute.key);
            attribute_data.insert_raw(key, Annotated::new(attr));
        }
    }

    if let Some(scope) = scope {
        attribute_data.insert("instrumentation.name".to_owned(), scope.name.clone());
        attribute_data.insert("instrumentation.version".to_owned(), scope.version.clone());
    }

    for attribute in attributes {
        if let Some(attr) = attribute
            .value
            .and_then(|v| v.value)
            .and_then(otel_value_to_attribute)
        {
            attribute_data.insert_raw(attribute.key, Annotated::new(attr));
        }
    }

    OurLog {
        timestamp: Annotated::new(Timestamp(timestamp)),
        trace_id,
        span_id,
        level: Annotated::new(level),
        body: Annotated::from(body),
        attributes: Annotated::new(attribute_data),
        other: Object::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{SerializableAnnotated, get_path};

    /// Helper function to assert that a trace ID was generated (has the substitution remark)
    fn assert_trace_id_generated(log: &OurLog) {
        assert!(log.trace_id.value().is_some(), "Trace ID should be present");
        assert!(
            log.trace_id.meta().iter_remarks().any(|remark| {
                remark.ty() == RemarkType::Substituted && remark.rule_id() == "trace_id.missing"
            }),
            "Trace ID should have a 'trace_id.missing' substitution remark"
        );
    }

    /// Helper function to assert that span ID is empty/default
    fn assert_span_id_empty(log: &OurLog) {
        assert!(
            log.span_id.value().is_none(),
            "Span ID should be empty/default"
        );
    }

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

        let resource = serde_json::from_value(serde_json::json!({
            "attributes": [{
                "key": "service.name",
                "value": {"stringValue": "test-service"},
            }]
        }))
        .unwrap();

        let scope = InstrumentationScope {
            name: "Eins Name".to_owned(),
            version: "123.42".to_owned(),
            attributes: Vec::new(),
            dropped_attributes_count: 12,
        };

        let otel_log: OtelLogRecord = serde_json::from_str(json).unwrap();
        let our_log: OurLog = otel_to_sentry_log(otel_log, Some(&resource), Some(&scope));
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Example log record".into()))
        );

        // Snapshot test for comprehensive validation
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_log), @r#"
        {
          "timestamp": 1544712660.3,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Example log record",
          "attributes": {
            "array.attribute": {
              "type": "string",
              "value": "[\"many\",\"values\"]"
            },
            "boolean.attribute": {
              "type": "boolean",
              "value": true
            },
            "double.attribute": {
              "type": "double",
              "value": 637.704
            },
            "instrumentation.name": {
              "type": "string",
              "value": "Eins Name"
            },
            "instrumentation.version": {
              "type": "string",
              "value": "123.42"
            },
            "int.attribute": {
              "type": "integer",
              "value": 10
            },
            "map.attribute": {
              "type": "string",
              "value": "{\"some.map.key\":\"some value\"}"
            },
            "resource.service.name": {
              "type": "string",
              "value": "test-service"
            },
            "string.attribute": {
              "type": "string",
              "value": "some string"
            }
          }
        }
        "#);
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

        let otel_log: OtelLogRecord = serde_json::from_str(json).unwrap();
        let our_log = otel_to_sentry_log(otel_log, None, None);
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

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_log), @r#"
        {
          "timestamp": 1544712660.3,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Database query executed",
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
        "#);
    }

    #[test]
    fn parse_otel_log_severity_mapping() {
        let test_cases = vec![
            (1, "trace", OurLogLevel::Trace),
            (5, "debug", OurLogLevel::Debug),
            (9, "info", OurLogLevel::Info),
            (13, "warn", OurLogLevel::Warn),
            (17, "error", OurLogLevel::Error),
            (21, "fatal", OurLogLevel::Fatal),
            (0, "warning", OurLogLevel::Warn), // fallback to text
            (0, "unknown", OurLogLevel::Info), // default fallback
        ];

        for (severity_number, severity_text, expected_level) in test_cases {
            let json = format!(
                r#"{{
                "timeUnixNano": "1544712660300000000",
                "severityNumber": {},
                "severityText": "{}",
                "traceId": "5B8EFFF798038103D269B633813FC60C",
                "spanId": "EEE19B7EC3C1B174",
                "body": {{
                    "stringValue": "Test log"
                }}
            }}"#,
                severity_number, severity_text
            );

            let otel_log: OtelLogRecord = serde_json::from_str(&json).unwrap();
            let our_log = otel_to_sentry_log(otel_log, None, None);

            assert_eq!(
                our_log.level.value().unwrap(),
                &expected_level,
                "Severity {} with text '{}' should map to {:?}",
                severity_number,
                severity_text,
                expected_level
            );
        }
    }

    #[test]
    fn parse_otel_log_without_span_and_trace_ids() {
        // Test with completely missing traceId and spanId fields
        let json = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 10,
            "severityText": "Information",
            "body": {
                "stringValue": "Log without trace context"
            },
            "attributes": [
                {
                    "key": "test.attribute",
                    "value": {
                        "stringValue": "test value"
                    }
                }
            ]
        }"#;

        let otel_log: OtelLogRecord = serde_json::from_str(json).unwrap();
        let our_log = otel_to_sentry_log(otel_log, None, None);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Log without trace context".into()))
        );
        assert_eq!(
            annotated_log.value().unwrap().level.value().unwrap(),
            &OurLogLevel::Info
        );

        let our_log = annotated_log.value().unwrap();
        assert_trace_id_generated(our_log);
        assert_span_id_empty(our_log);

        assert_eq!(
            annotated_log
                .value()
                .unwrap()
                .attributes
                .value()
                .unwrap()
                .get_value("test.attribute")
                .unwrap()
                .as_str(),
            Some("test value")
        );

        // Note: trace_id will be a random UUID, so we only check that it exists and has the right metadata
        let trace_id_value = annotated_log.value().unwrap().trace_id.value().unwrap();
        assert!(!trace_id_value.to_string().is_empty());

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_log), {
            ".trace_id" => "4bf92f3577b34da6a3ce929d0e0e4736"
        }, @r#"
        {
          "timestamp": 1544712660.3,
          "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
          "level": "info",
          "body": "Log without trace context",
          "attributes": {
            "test.attribute": {
              "type": "string",
              "value": "test value"
            }
          },
          "_meta": {
            "trace_id": {
              "": {
                "rem": [
                  [
                    "trace_id.missing",
                    "s"
                  ]
                ]
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn parse_otel_log_with_empty_trace_ids() {
        // Test with empty traceId and spanId byte arrays
        let json = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 17,
            "severityText": "Error",
            "traceId": "",
            "spanId": "",
            "body": {
                "stringValue": "Error log with empty trace IDs"
            },
            "attributes": [
                {
                    "key": "error.type",
                    "value": {
                        "stringValue": "ValidationError"
                    }
                }
            ]
        }"#;

        let otel_log: OtelLogRecord = serde_json::from_str(json).unwrap();
        let our_log = otel_to_sentry_log(otel_log, None, None);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Error log with empty trace IDs".into()))
        );
        assert_eq!(
            annotated_log.value().unwrap().level.value().unwrap(),
            &OurLogLevel::Error
        );

        let our_log = annotated_log.value().unwrap();
        assert_trace_id_generated(our_log);
        assert_span_id_empty(our_log);

        assert_eq!(
            annotated_log
                .value()
                .unwrap()
                .attributes
                .value()
                .unwrap()
                .get_value("error.type")
                .unwrap()
                .as_str(),
            Some("ValidationError")
        );

        // Note: trace_id will be a random UUID, so we only check that it exists and has the right metadata
        let trace_id_value = annotated_log.value().unwrap().trace_id.value().unwrap();
        assert!(!trace_id_value.to_string().is_empty());

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_log), {
            ".trace_id" => "4bf92f3577b34da6a3ce929d0e0e4736"
        }, @r#"
        {
          "timestamp": 1544712660.3,
          "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
          "level": "error",
          "body": "Error log with empty trace IDs",
          "attributes": {
            "error.type": {
              "type": "string",
              "value": "ValidationError"
            }
          },
          "_meta": {
            "trace_id": {
              "": {
                "rem": [
                  [
                    "trace_id.missing",
                    "s"
                  ]
                ]
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn parse_otel_log_with_invalid_trace_ids() {
        // Test with invalid traceId and spanId formats (wrong length)
        let json = r#"{
            "timeUnixNano": "1544712660300000000",
            "observedTimeUnixNano": "1544712660300000000",
            "severityNumber": 13,
            "severityText": "Warning",
            "traceId": "abc123",
            "spanId": "def456",
            "body": {
                "stringValue": "Warning log with invalid trace IDs"
            },
            "attributes": [
                {
                    "key": "warning.code",
                    "value": {
                        "intValue": "42"
                    }
                }
            ]
        }"#;

        let otel_log: OtelLogRecord = serde_json::from_str(json).unwrap();
        let our_log = otel_to_sentry_log(otel_log, None, None);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Warning log with invalid trace IDs".into()))
        );
        assert_eq!(
            annotated_log.value().unwrap().level.value().unwrap(),
            &OurLogLevel::Warn
        );

        let our_log = annotated_log.value().unwrap();
        assert_trace_id_generated(our_log);
        assert_span_id_empty(our_log);

        let warning_code_value = annotated_log
            .value()
            .unwrap()
            .attributes
            .value()
            .unwrap()
            .get_value("warning.code")
            .unwrap();

        match warning_code_value {
            relay_protocol::Value::I64(val) => assert_eq!(*val, 42),
            _ => panic!("Expected integer value for warning.code"),
        }

        // Note: trace_id will be a random UUID, so we only check that it exists and has the right metadata
        let trace_id_value = annotated_log.value().unwrap().trace_id.value().unwrap();
        assert!(!trace_id_value.to_string().is_empty());

        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_log), {
            ".trace_id" => "4bf92f3577b34da6a3ce929d0e0e4736"
        }, @r#"
        {
          "timestamp": 1544712660.3,
          "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
          "level": "warn",
          "body": "Warning log with invalid trace IDs",
          "attributes": {
            "warning.code": {
              "type": "integer",
              "value": 42
            }
          },
          "_meta": {
            "trace_id": {
              "": {
                "rem": [
                  [
                    "trace_id.missing",
                    "s"
                  ]
                ]
              }
            }
          }
        }
        "#);
    }
}
