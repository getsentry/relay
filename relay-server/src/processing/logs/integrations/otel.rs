use opentelemetry_proto::tonic::logs::v1::LogsData;
use prost::Message as _;
use relay_event_schema::protocol::OurLog;

use crate::integrations::OtelFormat;
use crate::processing::logs::{Error, Result, Settings};
use crate::services::outcome::DiscardReason;

/// Expands OTeL logs into the [`OurLog`] format.
pub fn expand<F>(
    format: OtelFormat,
    payload: &[u8],
    max_ops: usize,
    mut produce: F,
) -> Result<Settings>
where
    F: FnMut(OurLog),
{
    let logs = parse_logs_data(format, payload, max_ops)?;

    for resource_logs in logs.resource_logs {
        let resource = resource_logs.resource.as_ref();
        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            for log_record in scope_logs.log_records {
                let log = relay_ourlogs::otel_to_sentry_log(log_record, resource, scope);
                produce(log);
            }
        }
    }

    Ok(Settings::default())
}

fn parse_logs_data(format: OtelFormat, payload: &[u8], max_ops: usize) -> Result<LogsData, Error> {
    match format {
        OtelFormat::Json => {
            let mut de = serde_json::Deserializer::from_reader(payload);
            relay_serialization::serde::deserialize(&mut de, max_ops).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })
        }
        OtelFormat::Protobuf => LogsData::decode(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}
#[cfg(test)]
mod tests {

    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, InstrumentationScope, KeyValue,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use relay_ourlogs::otel_logs::{LogRecord, LogsData, ResourceLogs, ScopeLogs};

    use crate::processing::logs::integrations::otel::parse_logs_data;

    #[test]
    fn test_basic_json() {
        let log_data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_owned(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_owned())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "test-library".to_owned(),
                        version: "".to_owned(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 123,
                        observed_time_unix_nano: 123,
                        severity_number: 2,
                        severity_text: "Information".to_owned(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("a body".to_owned())),
                        }),
                        attributes: vec![
                            KeyValue {
                                key: "attribute".to_owned(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("value".to_owned())),
                                }),
                            },
                            KeyValue {
                                key: "nested attribute".to_owned(),
                                value: Some(AnyValue {
                                    value: Some(Value::ArrayValue(ArrayValue {
                                        values: vec![AnyValue {
                                            value: Some(Value::StringValue("value".to_owned())),
                                        }],
                                    })),
                                }),
                            },
                        ],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: "5B8EFFF798038103D269B633813FC60C".into(),
                        span_id: "EEE19B7EC3C1B174".into(),
                        event_name: "".to_owned(),
                    }],
                    schema_url: "".to_owned(),
                }],
                schema_url: "http://example.com".to_owned(),
            }],
        };

        let json = serde_json::to_string(&log_data).unwrap();

        let unjson: LogsData = serde_json::from_str(&json).unwrap();

        assert_eq!(unjson, log_data);
    }

    #[test]
    fn test_abusive_json() {
        let mut abusive_log = "{},".repeat(1_001);
        abusive_log.pop();

        let json = r#"{
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "test-service"}
                        }
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {"name": "test-library"},
                        "logRecords": ["#
            .to_owned();
        let json = json + &abusive_log + "]}]}]}";

        assert!(
            parse_logs_data(crate::integrations::OtelFormat::Json, json.as_bytes(), 1000).is_err()
        );
    }
}
