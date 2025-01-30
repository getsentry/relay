use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;
use relay_event_schema::protocol::{
    AttributeValue, Breadcrumb, Event, OurLog, SpanId, TraceContext, TraceId,
};

use crate::OtelLog;
use relay_protocol::{Annotated, Object, Value};

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
        observed_timestamp_nanos: Annotated::new(otel_log.observed_time_unix_nano),
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

/// Transform event breadcrumbs to OurLogs.
///
/// Only converts up to `max_breadcrumbs` breadcrumbs.
pub fn breadcrumbs_to_ourlogs(event: &Event, max_breadcrumbs: usize) -> Vec<OurLog> {
    let event_trace_id = event
        .context::<TraceContext>()
        .and_then(|trace_ctx| trace_ctx.trace_id.value())
        .cloned();

    let breadcrumbs = match event.breadcrumbs.value() {
        Some(breadcrumbs) => breadcrumbs,
        None => return Vec::new(),
    };

    let values = match breadcrumbs.values.value() {
        Some(values) => values,
        None => return Vec::new(),
    };

    values
        .iter()
        .take(max_breadcrumbs)
        .filter_map(|breadcrumb| {
            let breadcrumb = breadcrumb.value()?;

            // Convert to nanoseconds
            let timestamp_nanos = breadcrumb
                .timestamp
                .value()?
                .into_inner()
                .timestamp_nanos_opt()
                .unwrap() as u64;
            let mut attribute_data = Object::new();

            if let Some(category) = breadcrumb.category.value() {
                // Add category as sentry.category attribute if present, since the protocol doesn't have an equivalent field.
                attribute_data.insert(
                    "sentry.category".to_string(),
                    Annotated::new(AttributeValue::StringValue(category.to_string())),
                );
            }

            // Get span_id from data field if it exists and we have a trace_id from context, otherwise ignore it.
            let span_id = if event_trace_id.is_some() {
                breadcrumb
                    .data
                    .value()
                    .and_then(|data| data["__span"].value())
                    .and_then(|span| match span {
                        Value::String(s) => Some(Annotated::new(SpanId(s.clone()))),
                        _ => None,
                    })
                    .unwrap_or_else(Annotated::empty)
            } else {
                Annotated::empty()
            };

            // Convert breadcrumb data fields to primitive attributes
            if let Some(data) = breadcrumb.data.value() {
                for (key, value) in data.iter() {
                    if let Some(value) = value.value() {
                        let attribute = match value {
                            Value::String(s) => Some(AttributeValue::StringValue(s.clone())),
                            Value::Bool(b) => Some(AttributeValue::BoolValue(*b)),
                            Value::I64(i) => Some(AttributeValue::IntValue(*i)),
                            Value::F64(f) => Some(AttributeValue::DoubleValue(*f)),
                            _ => None, // Complex types will be supported once consumers are updated to ingest them.
                        };

                        if let Some(attr) = attribute {
                            attribute_data.insert(key.clone(), Annotated::new(attr));
                        }
                    }
                }
            }

            let (body, level) = match breadcrumb.ty.value().map(|ty| ty.as_str()) {
                Some("http") => format_http_breadcrumb(breadcrumb)?,
                Some(_) | None => format_default_breadcrumb(breadcrumb)?,
            };

            Some(OurLog {
                timestamp_nanos: Annotated::new(timestamp_nanos),
                observed_timestamp_nanos: Annotated::new(timestamp_nanos),
                trace_id: event_trace_id
                    .clone()
                    .map(Annotated::new)
                    .unwrap_or_else(Annotated::empty),
                span_id,
                trace_flags: Annotated::new(0),
                severity_text: level,
                severity_number: Annotated::empty(),
                body,
                attributes: Annotated::new(attribute_data),
                ..Default::default()
            })
        })
        .collect()
}

fn format_http_breadcrumb(
    breadcrumb: &Breadcrumb,
) -> Option<(Annotated<String>, Annotated<String>)> {
    let data = breadcrumb.data.value().cloned().unwrap_or_default();

    match (
        data.get("method").and_then(|v| v.value()),
        data.get("status_code").and_then(|v| v.value()),
        data.get("url").and_then(|v| v.value()),
    ) {
        (Some(Value::String(method)), Some(Value::I64(status)), Some(Value::String(url))) => {
            Some((
                Annotated::new(format!("[{}] - {} {}", status, method, url)),
                Annotated::new("info".to_string()),
            ))
        }
        _ => {
            relay_log::trace!(
                "Missing body in log when converting breadcrumb missing required fields: method={}, status={}, url={}",
                data.contains_key("method"),
                data.contains_key("status_code"),
                data.contains_key("url")
            );
            None
        }
    }
}

fn format_default_breadcrumb(
    breadcrumb: &Breadcrumb,
) -> Option<(Annotated<String>, Annotated<String>)> {
    breadcrumb.message.value()?; // Log must have a message.
    Some((
        breadcrumb.message.clone(),
        breadcrumb
            .level
            .clone()
            .map_value(|level| level.to_string()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use relay_event_schema::protocol::{Breadcrumb, Level, Values};
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
    fn test_breadcrumbs_to_ourlogs() {
        let json = r#"{
  "timestamp_nanos": 1577836800000000000,
  "observed_timestamp_nanos": 1577836800000000000,
  "trace_flags": 0,
  "severity_text": "info",
  "body": "test message",
  "attributes": {
    "bool_key": {
      "bool_value": true
    },
    "float_key": {
      "double_value": 42.5
    },
    "int_key": {
      "int_value": 42
    },
    "sentry.category": {
      "string_value": "test category"
    },
    "string_key": {
      "string_value": "string value"
    }
  }
}"#;

        let timestamp = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let mut breadcrumb = Breadcrumb::default();
        breadcrumb.message = Annotated::new("test message".to_string());
        breadcrumb.category = Annotated::new("test category".to_string());
        breadcrumb.timestamp = Annotated::new(timestamp.into());
        breadcrumb.level = Annotated::new(Level::Info);

        let mut data = Object::new();
        data.insert(
            "string_key".to_string(),
            Annotated::new(Value::String("string value".to_string())),
        );
        data.insert("bool_key".to_string(), Annotated::new(Value::Bool(true)));
        data.insert("int_key".to_string(), Annotated::new(Value::I64(42)));
        data.insert("float_key".to_string(), Annotated::new(Value::F64(42.5)));
        breadcrumb.data = Annotated::new(data);

        let mut event = Event::default();
        event.breadcrumbs = Annotated::new(Values {
            values: Annotated::new(vec![Annotated::new(breadcrumb)]),
            other: Object::default(),
        });

        let ourlogs = breadcrumbs_to_ourlogs(&event, 100);
        assert_eq!(ourlogs.len(), 1);

        let annotated_log = Annotated::new(ourlogs[0].clone());
        assert_eq!(json, annotated_log.to_json_pretty().unwrap());
    }

    #[test]
    fn test_breadcrumbs_limit() {
        let mut breadcrumbs = Vec::new();
        for i in 0..5 {
            let timestamp = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, i).unwrap();
            let mut breadcrumb = Breadcrumb::default();
            breadcrumb.message = Annotated::new(format!("message {}", i));
            breadcrumb.timestamp = Annotated::new(timestamp.into());
            breadcrumbs.push(Annotated::new(breadcrumb));
        }

        let mut event = Event::default();
        event.breadcrumbs = Annotated::new(Values {
            values: Annotated::new(breadcrumbs),
            other: Object::default(),
        });

        let ourlogs = breadcrumbs_to_ourlogs(&event, 3);
        assert_eq!(ourlogs.len(), 3, "Limited to 3 breadcrumbs");
        assert_eq!(ourlogs[2].body.value().unwrap(), "message 2");

        let ourlogs = breadcrumbs_to_ourlogs(&event, 10);
        assert_eq!(ourlogs.len(), 5, "No limit");
        assert_eq!(ourlogs[4].body.value().unwrap(), "message 4");
    }

    #[test]
    fn test_http_breadcrumb_conversion() {
        let json = r#"{
  "timestamp_nanos": 1738209657000000000,
  "observed_timestamp_nanos": 1738209657000000000,
  "trace_flags": 0,
  "severity_text": "info",
  "body": "[200] - GET /api/0/organizations/sentry/issues/",
  "attributes": {
    "__span": {
      "string_value": "bd61ce905c5f1bbd"
    },
    "method": {
      "string_value": "GET"
    },
    "sentry.category": {
      "string_value": "fetch"
    },
    "status_code": {
      "int_value": 200
    },
    "url": {
      "string_value": "/api/0/organizations/sentry/issues/"
    }
  }
}"#;

        let timestamp = Utc.with_ymd_and_hms(2025, 1, 30, 4, 0, 57).unwrap();
        let mut breadcrumb = Breadcrumb::default();
        breadcrumb.ty = Annotated::new("http".to_string());
        breadcrumb.category = Annotated::new("fetch".to_string());
        breadcrumb.timestamp = Annotated::new(timestamp.into());

        let mut data = Object::new();
        data.insert(
            "__span".to_string(),
            Annotated::new(Value::String("bd61ce905c5f1bbd".to_string())),
        );
        data.insert(
            "method".to_string(),
            Annotated::new(Value::String("GET".to_string())),
        );
        data.insert("status_code".to_string(), Annotated::new(Value::I64(200)));
        data.insert(
            "url".to_string(),
            Annotated::new(Value::String(
                "/api/0/organizations/sentry/issues/".to_string(),
            )),
        );
        breadcrumb.data = Annotated::new(data);

        let mut event = Event::default();
        event.breadcrumbs = Annotated::new(Values {
            values: Annotated::new(vec![Annotated::new(breadcrumb)]),
            other: Object::default(),
        });

        let ourlogs = breadcrumbs_to_ourlogs(&event, 100);
        assert_eq!(ourlogs.len(), 1);

        let annotated_log = Annotated::new(ourlogs[0].clone());
        assert_eq!(json, annotated_log.to_json_pretty().unwrap());
    }
}
