use relay_cogs::AppFeature;
use relay_event_schema::protocol::ourlog::container::{InferIp, InferUserAgent, IngestSettings};
use relay_otel::descriptors::Decodable;
use relay_ourlogs::otel_logs::LogsData;
use relay_quotas::DataCategory;

use crate::envelope::WithHeader;
use crate::integrations::{Integration, LogsIntegration, OtelFormat};
use crate::managed::{Counted, Managed, ManagedEnvelope, RecordKeeper, Rejected};
use crate::processing::logs::integrations::{
    Error, SerializedLogsIntegration, create_intermediates,
};
use crate::processing::{self, Intermediates, Nothing, Output};
use crate::services::outcome::DiscardReason;

#[derive(Debug)]
pub struct SerializedOtelLogsIntegration(SerializedLogsIntegration, OtelFormat);

impl Counted for SerializedOtelLogsIntegration {
    fn quantities(&self) -> crate::managed::Quantities {
        self.0.quantities()
    }
}

/// A processor for OTEL log integrations.
///
/// This processor only produces intermediates (items
/// that are put back into the processing pipeline) by
/// converting log integration items into actual logs. It
/// never passes anything on to the store or another Relay.
#[derive(Debug)]
pub struct OtelLogsIntegrationProcessor;

impl processing::Processor for OtelLogsIntegrationProcessor {
    type Input = SerializedOtelLogsIntegration;
    type Output = Nothing;
    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let meta = envelope.envelope().headers().meta().clone();

        let (item, format) =
            envelope
                .envelope_mut()
                .take_item_by_map(|item| match item.integration() {
                    Some(Integration::Logs(LogsIntegration::OtelV1 { format })) => Some(format),
                    _ => None,
                })?;

        let work = SerializedOtelLogsIntegration(SerializedLogsIntegration { meta, item }, format);
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let intermediates = work.try_map(|work, records| {
            expand(
                work,
                records,
                ctx.config.max_expanded_log_operations(),
                ctx.config.max_expanded_log_count(),
            )
        })?;
        Ok(Output::intermediates(intermediates))
    }
}

/// Expands a OTEL log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    work: SerializedOtelLogsIntegration,
    records: &mut RecordKeeper,
    max_ops: usize,
    max_logs: usize,
) -> Result<Intermediates, Error> {
    let SerializedOtelLogsIntegration(SerializedLogsIntegration { meta, item }, format) = work;

    // Log byte counts will change here as we go from an estimated count based on the body, to
    // accurately counted bytes.
    records.lenient(DataCategory::LogByte);

    let payload = item.payload();
    let mut logs = Vec::new();
    let parsed = parse_logs_data(format, &payload, max_ops)?;
    let mut log_count = 0;

    for resource_logs in parsed.resource_logs {
        let resource = resource_logs.resource.as_ref();
        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            for log_record in scope_logs.log_records {
                if log_count >= max_logs {
                    return Err(Error::Invalid(DiscardReason::RequestTooLarge));
                }
                log_count += 1;

                let log = relay_ourlogs::otel_to_sentry_log(log_record, resource, scope);
                // https://github.com/getsentry/relay/issues/6605: This is only used to keep records in case serializing
                // the logs back into an envelope fails. Consider removing it.
                let byte_size = relay_ourlogs::calculate_size(&log);

                records.add([
                    (DataCategory::LogItem, 1),
                    (DataCategory::LogByte, byte_size as usize),
                ]);

                logs.push(WithHeader::just(log.into()));
            }
        }
    }

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    records.remove(&item);

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Never),
        infer_ip: Some(InferIp::Never),
    };

    create_intermediates(meta, logs, settings)
}

fn parse_logs_data(format: OtelFormat, payload: &[u8], max_ops: usize) -> Result<LogsData, Error> {
    match format {
        OtelFormat::Json => {
            let mut de = serde_json::Deserializer::from_reader(payload);
            let result = relay_serialization::serde::deserialize(&mut de, max_ops).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            });
            de.end()
                .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?;
            result
        }
        OtelFormat::Protobuf => LogsData::decode_bounded(payload, max_ops).map_err(|e| {
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

    use prost::Message;

    use crate::integrations::OtelFormat;
    use crate::processing::logs::integrations::Error;
    use crate::processing::logs::integrations::otel::parse_logs_data;
    use crate::services::outcome::DiscardReason;

    fn logs_with_records(n: usize) -> LogsData {
        LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default(); n],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

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
                        key_strindex: 0,
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
                                key_strindex: 0,
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
                                key_strindex: 0,
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

    #[test]
    fn test_basic_protobuf() {
        let log_data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_owned(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_owned())),
                        }),
                        key_strindex: 0,
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 123,
                        severity_text: "Information".to_owned(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("a body".to_owned())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: "http://example.com".to_owned(),
            }],
        };

        let parsed =
            parse_logs_data(OtelFormat::Protobuf, &log_data.encode_to_vec(), 1000).unwrap();
        assert_eq!(parsed, log_data);
    }

    #[test]
    fn test_abusive_protobuf() {
        // resource_logs + scope_logs + one op per record.
        let payload = logs_with_records(1_001).encode_to_vec();

        assert!(matches!(
            parse_logs_data(OtelFormat::Protobuf, &payload, 1000),
            Err(Error::Invalid(DiscardReason::InvalidProtobuf))
        ));
        assert!(parse_logs_data(OtelFormat::Protobuf, &payload, 1003).is_ok());
    }

    #[test]
    fn test_protobuf_and_json_limits_agree() {
        // The same logical payload is accepted or rejected regardless of its encoding.
        let small = logs_with_records(10);
        let large = logs_with_records(2_000);

        for (logs, ok) in [(small, true), (large, false)] {
            let proto = parse_logs_data(OtelFormat::Protobuf, &logs.encode_to_vec(), 1000);
            let json = parse_logs_data(
                OtelFormat::Json,
                serde_json::to_string(&logs).unwrap().as_bytes(),
                1000,
            );
            assert_eq!(proto.is_ok(), ok);
            assert_eq!(json.is_ok(), ok);
        }
    }

    #[test]
    fn test_invalid_protobuf() {
        assert!(matches!(
            parse_logs_data(OtelFormat::Protobuf, b"\xff\xff\xff", 1000),
            Err(Error::Invalid(DiscardReason::InvalidProtobuf))
        ));
    }
}
