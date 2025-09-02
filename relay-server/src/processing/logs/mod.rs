use std::sync::Arc;

use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use prost::Message;
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::{
    Attribute, AttributeType, Attributes, OurLog, OurLogLevel, SpanId, Timestamp, TraceId,
};
use relay_filter::FilterStatKey;
use relay_pii::PiiConfigError;
use relay_protocol::{Annotated, Object, Value};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{
    ContainerItems, ContainerWriteError, ContentType, EnvelopeHeaders, Item, ItemContainer,
    ItemType, Items,
};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Quantities,
};
use crate::processing::{
    self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter, Rejected,
};
use crate::services::outcome::{DiscardReason, Outcome};

mod filter;
mod process;
#[cfg(feature = "processing")]
mod store;
mod utils;
mod validate;

pub use self::utils::get_calculated_byte_size;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A duplicated item container for logs.
    #[error("duplicate log container")]
    DuplicateContainer,
    /// Logs filtered because of a missing feature flag.
    #[error("logs feature flag missing")]
    FilterFeatureFlag,
    /// Logs filtered due to a filtering rule.
    #[error("log filtered")]
    Filtered(FilterStatKey),
    /// The logs are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Internal error, Pii config could not be loaded.
    #[error("Pii configuration error")]
    PiiConfig(PiiConfigError),
    /// A processor failed to process the logs.
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
    /// The log is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::DuplicateContainer => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::FilterFeatureFlag => None,
            Self::Filtered(f) => Some(Outcome::Filtered(f.clone())),
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::PiiConfig(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::ProcessingFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
            Self::Invalid(reason) => Some(Outcome::Invalid(*reason)),
        };

        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Logs.
///
/// It processes items of type: [`ItemType::Log`].
#[derive(Debug)]
pub struct LogsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl LogsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for LogsProcessor {
    type UnitOfWork = SerializedLogs;
    type Output = LogOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        // Convert OTLP logs data to individual log items
        convert_otel_logs_data(envelope);

        let logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Log))
            .into_vec();

        let work = SerializedLogs { headers, logs };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut logs: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        validate::container(&logs)?;

        if ctx.is_proxy() {
            // If running in proxy mode, just apply cached rate limits and forward without
            // processing.
            //
            // Static mode needs processing, as users can override project settings manually.
            self.limiter.enforce_quotas(&mut logs, ctx).await?;
            return Ok(Output::just(LogOutput::NotProcessed(logs)));
        }

        // Fast filters, which do not need expanded logs.
        filter::feature_flag(ctx).reject(&logs)?;

        let mut logs = process::expand(logs, ctx);
        process::normalize(&mut logs);
        filter::filter(&mut logs, ctx);
        process::scrub(&mut logs, ctx);

        self.limiter.enforce_quotas(&mut logs, ctx).await?;

        Ok(Output::just(LogOutput::Processed(logs)))
    }
}

/// Output produced by [`LogsProcessor`].
#[derive(Debug)]
pub enum LogOutput {
    NotProcessed(Managed<SerializedLogs>),
    Processed(Managed<ExpandedLogs>),
}

impl Forward for LogOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let logs = match self {
            Self::NotProcessed(logs) => logs,
            Self::Processed(logs) => logs.try_map(|logs, r| {
                r.lenient(DataCategory::LogByte);
                logs.serialize()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            })?,
        };

        Ok(logs.map(|logs, r| {
            r.lenient(DataCategory::LogByte);
            logs.serialize_envelope()
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        let logs = match self {
            LogOutput::NotProcessed(logs) => {
                return Err(logs.internal_error(
                    "logs must be processed before they can be forwarded to the store",
                ));
            }
            LogOutput::Processed(logs) => logs,
        };

        let scoping = logs.scoping();
        let received_at = logs.received_at();

        let (logs, retentions) = logs
            .split_with_context(|logs| (logs.logs, (logs.retention, logs.downsampled_retention)));
        let ctx = store::Context {
            scoping,
            received_at,
            // Hard-code retentions until we have a per data category retention
            retention: retentions.0,
            downsampled_retention: retentions.1,
        };

        for log in logs {
            if let Ok(log) = log.try_map(|log, _| store::convert(log, &ctx)) {
                s.send(log)
            };
        }

        Ok(())
    }
}

/// Logs in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Logs are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    logs: Vec<Item>,
}

impl SerializedLogs {
    fn serialize_envelope(self) -> Box<Envelope> {
        Envelope::from_parts(self.headers, Items::from_vec(self.logs))
    }

    /// Returns the total count of all logs contained.
    ///
    /// This contains all logical log items, not just envelope items and is safe
    /// to use for rate limiting.
    fn count(&self) -> usize {
        self.logs
            .iter()
            .map(|item| item.item_count().unwrap_or(1) as usize)
            .sum()
    }

    /// Returns the sum of bytes of all logs contained.
    fn bytes(&self) -> usize {
        self.logs.iter().map(|item| item.len()).sum()
    }
}

impl Counted for SerializedLogs {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::LogItem, self.count()),
            (DataCategory::LogByte, self.bytes())
        ]
    }
}

impl CountRateLimited for Managed<SerializedLogs> {
    type Error = Error;
}

/// Logs which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// Expanded and parsed logs.
    logs: ContainerItems<OurLog>,

    // These fields are currently necessary as we don't pass any project config context to the
    // store serialization. The plan is to get rid of them by giving the serialization context,
    // including the project info, where these are pulled from: #4878.
    /// Retention in days.
    #[cfg(feature = "processing")]
    retention: Option<u16>,
    /// Downsampled retention in days.
    #[cfg(feature = "processing")]
    downsampled_retention: Option<u16>,
}

impl Counted for ExpandedLogs {
    fn quantities(&self) -> Quantities {
        let count = self.logs.len();
        let bytes = self.logs.iter().map(get_calculated_byte_size).sum();

        smallvec::smallvec![
            (DataCategory::LogItem, count),
            (DataCategory::LogByte, bytes)
        ]
    }
}

impl ExpandedLogs {
    fn serialize(self) -> Result<SerializedLogs, ContainerWriteError> {
        let mut logs = Vec::new();

        if !self.logs.is_empty() {
            let mut item = Item::new(ItemType::Log);
            ItemContainer::from(self.logs)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize logs: {err}"))?;
            logs.push(item);
        }

        Ok(SerializedLogs {
            headers: self.headers,
            logs,
        })
    }
}

impl CountRateLimited for Managed<ExpandedLogs> {
    type Error = Error;
}

/// Converts OTLP logs data items to individual log items.
pub fn convert_otel_logs_data(envelope: &mut ManagedEnvelope) {
    for item in envelope
        .envelope_mut()
        .take_items_by(|item| item.ty() == &ItemType::OtelLogsData)
    {
        convert_logs_data(item, envelope);
    }
}

fn convert_logs_data(item: Item, envelope: &mut ManagedEnvelope) {
    let logs_request = match parse_logs_data(item) {
        Ok(logs_request) => logs_request,
        Err(reason) => {
            // NOTE: logging quantity=1 is semantically wrong, but we cannot know the real quantity
            // without parsing.
            envelope.track_outcome(
                crate::services::outcome::Outcome::Invalid(reason),
                DataCategory::LogItem,
                1,
            );
            envelope.track_outcome(
                crate::services::outcome::Outcome::Invalid(reason),
                DataCategory::LogByte,
                1,
            );
            return;
        }
    };

    for resource_logs in logs_request.resource_logs {
        for scope_logs in resource_logs.scope_logs {
            for mut log_record in scope_logs.log_records {
                // Denormalize instrumentation scope and resource attributes into every log.
                if let Some(ref scope) = scope_logs.scope {
                    if !scope.name.is_empty() {
                        log_record.attributes.push(KeyValue {
                            key: "instrumentation.name".to_owned(),
                            value: Some(AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(scope.name.clone())),
                            }),
                        });
                    }
                    if !scope.version.is_empty() {
                        log_record.attributes.push(KeyValue {
                            key: "instrumentation.version".to_owned(),
                            value: Some(AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(scope.version.clone())),
                            }),
                        });
                    }
                    scope.attributes.iter().for_each(|a| {
                        log_record.attributes.push(KeyValue {
                            key: format!("instrumentation.{}", a.key),
                            value: a.value.clone(),
                        });
                    });
                }
                if let Some(ref resource) = resource_logs.resource {
                    resource.attributes.iter().for_each(|a| {
                        log_record.attributes.push(KeyValue {
                            key: format!("resource.{}", a.key),
                            value: a.value.clone(),
                        });
                    });
                }

                // Convert OTLP log record to OurLog
                match otel_to_sentry_log(&log_record) {
                    Ok(our_log) => {
                        // Add individual log to container items
                        let mut logs_container = ContainerItems::new();
                        logs_container
                            .push(crate::envelope::WithHeader::just(Annotated::new(our_log)));

                        if !logs_container.is_empty() {
                            let mut item = Item::new(ItemType::Log);
                            if ItemContainer::from(logs_container)
                                .write_to(&mut item)
                                .is_err()
                            {
                                envelope.track_outcome(
                                    crate::services::outcome::Outcome::Invalid(
                                        crate::services::outcome::DiscardReason::Internal,
                                    ),
                                    DataCategory::LogItem,
                                    1,
                                );
                                continue;
                            }
                            envelope.envelope_mut().add_item(item);
                        }
                    }
                    Err(_) => {
                        envelope.track_outcome(
                            crate::services::outcome::Outcome::Invalid(
                                crate::services::outcome::DiscardReason::Internal,
                            ),
                            DataCategory::LogItem,
                            1,
                        );
                        continue;
                    }
                }
            }
        }
    }
}

fn parse_logs_data(
    item: Item,
) -> Result<ExportLogsServiceRequest, crate::services::outcome::DiscardReason> {
    match item.content_type() {
        Some(&ContentType::Json) => serde_json::from_slice(&item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as JSON"
            );
            crate::services::outcome::DiscardReason::InvalidJson
        }),
        Some(&ContentType::Protobuf) => {
            ExportLogsServiceRequest::decode(item.payload()).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as protobuf"
                );
                crate::services::outcome::DiscardReason::InvalidProtobuf
            })
        }
        _ => Err(crate::services::outcome::DiscardReason::ContentType),
    }
}

fn otel_value_to_log_attribute(value: &AnyValue) -> Option<Attribute> {
    match &value.value {
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(v)) => {
            Some(Attribute::new(AttributeType::Boolean, Value::Bool(*v)))
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(v)) => {
            Some(Attribute::new(AttributeType::Double, Value::F64(*v)))
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(v)) => {
            Some(Attribute::new(AttributeType::Integer, Value::I64(*v)))
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(v)) => Some(
            Attribute::new(AttributeType::String, Value::String(v.clone())),
        ),
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(v)) => {
            String::from_utf8(v.clone())
                .ok()
                .map(|s| Attribute::new(AttributeType::String, Value::String(s)))
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(_)) => None,
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(_)) => None,
        None => None,
    }
}

/// Transform an OpenTelemetry LogRecord to a Sentry log.
fn otel_to_sentry_log(log_record: &LogRecord) -> Result<OurLog, relay_protocol::Error> {
    let severity_number = log_record.severity_number;
    let severity_text = log_record.severity_text.clone();

    // Convert span_id and trace_id from bytes
    let span_id = if log_record.span_id.is_empty() {
        SpanId::default()
    } else {
        SpanId::try_from(log_record.span_id.as_slice())
            .map_err(|_| relay_protocol::Error::expected("valid span_id"))?
    };

    let trace_id = if log_record.trace_id.is_empty() {
        return Err(relay_protocol::Error::expected("valid trace_id"));
    } else {
        TraceId::try_from(log_record.trace_id.as_slice())
            .map_err(|_| relay_protocol::Error::expected("valid trace_id"))?
    };

    // Convert timestamp from nanoseconds
    let timestamp = if log_record.time_unix_nano > 0 {
        Utc.timestamp_nanos(log_record.time_unix_nano as i64)
    } else {
        return Err(relay_protocol::Error::expected("valid timestamp"));
    };

    // Extract log body
    let body = log_record
        .body
        .as_ref()
        .and_then(|v| v.value.as_ref())
        .and_then(|v| match v {
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                Some(s.clone())
            }
            _ => None,
        })
        .unwrap_or_default();

    // Initialize attributes with severity number
    let mut attribute_data = Attributes::new();
    attribute_data.insert("sentry.severity_number".to_owned(), severity_number as i64);

    // Convert OpenTelemetry attributes
    for attribute in &log_record.attributes {
        if let Some(value) = &attribute.value {
            let key = attribute.key.clone();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Envelope;
    use crate::managed::{ManagedEnvelope, TypedEnvelope};
    use crate::services::processor::{LogGroup, ProcessingGroup};
    use bytes::Bytes;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList};
    use relay_protocol::get_path;
    use relay_system::Addr;

    #[test]
    fn test_attribute_denormalization() {
        // Construct an OTLP logs payload with:
        // - a resource with one attribute, containing:
        // - an instrumentation scope with one attribute, containing:
        // - a log record with one attribute
        let logs_data = r#"
        {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "resource_key",
                                "value": {
                                    "stringValue": "resource_value"
                                }
                            }
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {
                                "name": "test_instrumentation",
                                "version": "0.0.1",
                                "attributes": [
                                    {
                                        "key": "scope_key",
                                        "value": {
                                            "stringValue": "scope_value"
                                        }
                                    }
                                ]
                            },
                            "logRecords": [
                                {
                                    "timeUnixNano": "1640995200000000000",
                                    "severityNumber": 9,
                                    "severityText": "INFO",
                                    "body": {
                                        "stringValue": "Test log message"
                                    },
                                    "attributes": [
                                        {
                                            "key": "log_key",
                                            "value": {
                                                "stringValue": "log_value"
                                            }
                                        }
                                    ],
                                    "traceId": "0102030405060708090a0b0c0d0e0f10",
                                    "spanId": "0102030405060708"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        "#;

        // Build an envelope containing the OTLP logs data.
        let bytes =
            Bytes::from(r#"{"dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}"#);
        let envelope = Envelope::parse_bytes(bytes).unwrap();
        let (outcome_aggregator, _) = Addr::custom();
        let managed_envelope = ManagedEnvelope::new(envelope, outcome_aggregator);
        let mut typed_envelope: TypedEnvelope<LogGroup> =
            (managed_envelope, ProcessingGroup::Log).try_into().unwrap();
        let mut item = Item::new(ItemType::OtelLogsData);
        item.set_payload(ContentType::Json, logs_data);
        typed_envelope.envelope_mut().add_item(item.clone());

        // Convert the OTLP logs data into Log item(s).
        convert_logs_data(item, &mut typed_envelope);

        // Assert that the attributes from the resource and instrumentation
        // scope were copied.
        let item = typed_envelope
            .envelope()
            .items()
            .find(|i| *i.ty() == ItemType::Log)
            .expect("converted log missing from envelope");

        let container =
            ItemContainer::<OurLog>::parse(item).expect("unable to parse log container");
        let log = container
            .into_items()
            .into_iter()
            .next()
            .expect("no log in container");
        let log = log.value().expect("log has no value");

        let attributes = &log.attributes.value().expect("log has no attributes").0;
        let get_attribute_value = |key: &str| -> String {
            let attr = attributes
                .get(key)
                .unwrap_or_else(|| panic!("attribute {key} missing"))
                .value()
                .expect("attribute has no value");
            match attr.value.value.value().expect("attribute value missing") {
                Value::String(s) => s.clone(),
                _ => panic!("attribute {key} not a string"),
            }
        };

        assert_eq!(
            get_attribute_value("log_key"),
            "log_value".to_owned(),
            "original log attribute should be present"
        );
        assert_eq!(
            get_attribute_value("instrumentation.name"),
            "test_instrumentation".to_owned(),
            "instrumentation name should be in attributes"
        );
        assert_eq!(
            get_attribute_value("instrumentation.version"),
            "0.0.1".to_owned(),
            "instrumentation version should be in attributes"
        );
        assert_eq!(
            get_attribute_value("resource.resource_key"),
            "resource_value".to_owned(),
            "resource attribute should be copied with prefix"
        );
        assert_eq!(
            get_attribute_value("instrumentation.scope_key"),
            "scope_value".to_owned(),
            "instrumentation scope attribute should be copied with prefix"
        );

        // Verify log content
        assert_eq!(
            log.body.value().expect("log has no body"),
            "Test log message"
        );
        assert_eq!(
            log.level.value().expect("log has no level"),
            &OurLogLevel::Info
        );

        // Verify severity number was added
        let severity_attr = attributes
            .get("sentry.severity_number")
            .expect("severity number attribute missing")
            .value()
            .expect("severity attribute has no value");
        match severity_attr
            .value
            .value
            .value()
            .expect("severity value missing")
        {
            Value::I64(9) => {}
            _ => panic!("severity number should be 9"),
        }
    }

    #[test]
    fn test_severity_mapping() {
        let test_cases = vec![
            (1, "trace", OurLogLevel::Trace),
            (5, "debug", OurLogLevel::Debug),
            (9, "info", OurLogLevel::Info),
            (13, "warn", OurLogLevel::Warn),
            (17, "error", OurLogLevel::Error),
            (21, "fatal", OurLogLevel::Fatal),
            (0, "unknown", OurLogLevel::Info), // fallback case
        ];

        for (severity_number, severity_text, expected_level) in test_cases {
            let log_record = LogRecord {
                time_unix_nano: 1640995200000000000,
                severity_number,
                severity_text: severity_text.to_owned(),
                body: Some(AnyValue {
                    value: Some(OtelValue::StringValue("test message".to_owned())),
                }),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                observed_time_unix_nano: 0,
                event_name: String::new(),
            };

            let result = otel_to_sentry_log(&log_record).expect("conversion should succeed");
            assert_eq!(
                result.level.value().expect("level should be present"),
                &expected_level,
                "severity {} should map to {:?}",
                severity_number,
                expected_level
            );
        }
    }

    #[test]
    fn test_attribute_types() {
        let log_record = LogRecord {
            time_unix_nano: 1640995200000000000,
            severity_number: 9,
            severity_text: "info".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("test".to_owned())),
            }),
            attributes: vec![
                KeyValue {
                    key: "string_attr".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::StringValue("test_string".to_owned())),
                    }),
                },
                KeyValue {
                    key: "int_attr".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::IntValue(42)),
                    }),
                },
                KeyValue {
                    key: "double_attr".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::DoubleValue(std::f64::consts::PI)),
                    }),
                },
                KeyValue {
                    key: "bool_attr".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::BoolValue(true)),
                    }),
                },
                KeyValue {
                    key: "bytes_attr".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::BytesValue(b"hello".to_vec())),
                    }),
                },
            ],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            observed_time_unix_nano: 0,
            event_name: String::new(),
        };

        let result = otel_to_sentry_log(&log_record).expect("conversion should succeed");
        let attributes = &result.attributes.value().expect("log has no attributes").0;

        // Check string attribute
        let string_attr = attributes.get("string_attr").unwrap().value().unwrap();
        assert_eq!(
            string_attr.value.ty.value().unwrap(),
            &AttributeType::String
        );
        match string_attr.value.value.value().unwrap() {
            Value::String(s) => assert_eq!(s, "test_string"),
            _ => panic!("expected string value"),
        }

        // Check integer attribute
        let int_attr = attributes.get("int_attr").unwrap().value().unwrap();
        assert_eq!(int_attr.value.ty.value().unwrap(), &AttributeType::Integer);
        match int_attr.value.value.value().unwrap() {
            Value::I64(i) => assert_eq!(*i, 42),
            _ => panic!("expected integer value"),
        }

        // Check double attribute
        let double_attr = attributes.get("double_attr").unwrap().value().unwrap();
        assert_eq!(
            double_attr.value.ty.value().unwrap(),
            &AttributeType::Double
        );
        match double_attr.value.value.value().unwrap() {
            Value::F64(f) => assert!((f - std::f64::consts::PI).abs() < f64::EPSILON),
            _ => panic!("expected double value"),
        }

        // Check boolean attribute
        let bool_attr = attributes.get("bool_attr").unwrap().value().unwrap();
        assert_eq!(bool_attr.value.ty.value().unwrap(), &AttributeType::Boolean);
        match bool_attr.value.value.value().unwrap() {
            Value::Bool(b) => assert!(b),
            _ => panic!("expected boolean value"),
        }

        // Check bytes attribute (converted to string)
        let bytes_attr = attributes.get("bytes_attr").unwrap().value().unwrap();
        assert_eq!(bytes_attr.value.ty.value().unwrap(), &AttributeType::String);
        match bytes_attr.value.value.value().unwrap() {
            Value::String(s) => assert_eq!(s, "hello"),
            _ => panic!("expected string value from bytes"),
        }
    }

    #[test]
    fn test_missing_trace_id_error() {
        let log_record = LogRecord {
            time_unix_nano: 1640995200000000000,
            severity_number: 9,
            severity_text: "info".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("test".to_owned())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![], // Empty trace_id should cause error
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            observed_time_unix_nano: 0,
            event_name: String::new(),
        };

        let result = otel_to_sentry_log(&log_record);
        assert!(result.is_err(), "empty trace_id should cause error");
    }

    #[test]
    fn test_missing_timestamp_error() {
        let log_record = LogRecord {
            time_unix_nano: 0, // Zero timestamp should cause error
            severity_number: 9,
            severity_text: "info".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("test".to_owned())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            observed_time_unix_nano: 0,
            event_name: String::new(),
        };

        let result = otel_to_sentry_log(&log_record);
        assert!(result.is_err(), "zero timestamp should cause error");
    }

    #[test]
    fn test_empty_span_id_defaults() {
        let log_record = LogRecord {
            time_unix_nano: 1640995200000000000,
            severity_number: 9,
            severity_text: "info".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("test".to_owned())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![], // Empty span_id should default
            observed_time_unix_nano: 0,
            event_name: String::new(),
        };

        let result = otel_to_sentry_log(&log_record).expect("conversion should succeed");
        let span_id = result.span_id.value().expect("span_id should be present");
        assert_eq!(*span_id, SpanId::default(), "empty span_id should default");
    }

    #[test]
    fn test_different_body_types() {
        let test_cases = vec![
            (
                Some(OtelValue::StringValue("string body".to_owned())),
                "string body",
            ),
            (Some(OtelValue::IntValue(42)), ""), // Non-string values should result in empty body
            (Some(OtelValue::BoolValue(true)), ""),
            (None, ""), // No body should result in empty body
        ];

        for (body_value, expected_body) in test_cases {
            let log_record = LogRecord {
                time_unix_nano: 1640995200000000000,
                severity_number: 9,
                severity_text: "info".to_owned(),
                body: body_value.clone().map(|v| AnyValue { value: Some(v) }),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                observed_time_unix_nano: 0,
                event_name: String::new(),
            };

            let result = otel_to_sentry_log(&log_record).expect("conversion should succeed");
            assert_eq!(
                result.body.value().expect("body should be present"),
                expected_body,
                "body value should match expected for {:?}",
                body_value
            );
        }
    }

    #[test]
    fn test_parse_otel_log() {
        // Based on https://github.com/open-telemetry/opentelemetry-proto/blob/c4214b8168d0ce2a5236185efb8a1c8950cccdd6/examples/logs.json
        let log_record = LogRecord {
            time_unix_nano: 1544712660300000000,
            observed_time_unix_nano: 1544712660300000000,
            severity_number: 10,
            severity_text: "Information".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("Example log record".to_owned())),
            }),
            attributes: vec![
                KeyValue {
                    key: "string.attribute".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::StringValue("some string".to_owned())),
                    }),
                },
                KeyValue {
                    key: "boolean.attribute".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::BoolValue(true)),
                    }),
                },
                KeyValue {
                    key: "int.attribute".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::IntValue(10)),
                    }),
                },
                KeyValue {
                    key: "double.attribute".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::DoubleValue(637.704)),
                    }),
                },
                KeyValue {
                    key: "array.attribute".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(OtelValue::StringValue("many".to_owned())),
                                },
                                AnyValue {
                                    value: Some(OtelValue::StringValue("values".to_owned())),
                                },
                            ],
                        })),
                    }),
                },
                KeyValue {
                    key: "map.attribute".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "some.map.key".to_owned(),
                                value: Some(AnyValue {
                                    value: Some(OtelValue::StringValue("some value".to_owned())),
                                }),
                            }],
                        })),
                    }),
                },
            ],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![
                0x5B, 0x8E, 0xFF, 0xF7, 0x98, 0x03, 0x81, 0x03, 0xD2, 0x69, 0xB6, 0x33, 0x81, 0x3F,
                0xC6, 0x0C,
            ],
            span_id: vec![0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x74],
            event_name: String::new(),
        };

        let our_log = otel_to_sentry_log(&log_record).expect("conversion should succeed");
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Example log record".into()))
        );

        // Verify that complex attributes (array, map) are not included since we skip them
        let attributes = &annotated_log.value().unwrap().attributes.value().unwrap().0;
        assert!(attributes.get("string.attribute").is_some());
        assert!(attributes.get("boolean.attribute").is_some());
        assert!(attributes.get("int.attribute").is_some());
        assert!(attributes.get("double.attribute").is_some());
        assert!(attributes.get("array.attribute").is_none()); // Should be skipped
        assert!(attributes.get("map.attribute").is_none()); // Should be skipped
    }

    #[test]
    fn test_parse_otellog_with_invalid_trace_id() {
        let log_record = LogRecord {
            time_unix_nano: 1544712660300000000,
            observed_time_unix_nano: 1544712660300000000,
            severity_number: 10,
            severity_text: "Information".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("Test log".to_owned())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![], // Empty trace_id should cause error
            span_id: vec![0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x74],
            event_name: String::new(),
        };

        let our_log = otel_to_sentry_log(&log_record);
        assert!(our_log.is_err(), "empty trace_id should cause error");
    }

    #[test]
    fn test_parse_otel_log_with_db_attributes() {
        let log_record = LogRecord {
            time_unix_nano: 1544712660300000000,
            observed_time_unix_nano: 1544712660300000000,
            severity_number: 10,
            severity_text: "Information".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("Database query executed".to_owned())),
            }),
            attributes: vec![
                KeyValue {
                    key: "db.name".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::StringValue("database".to_owned())),
                    }),
                },
                KeyValue {
                    key: "db.type".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::StringValue("sql".to_owned())),
                    }),
                },
                KeyValue {
                    key: "db.statement".to_owned(),
                    value: Some(AnyValue {
                        value: Some(OtelValue::StringValue(
                            "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
                                .to_owned(),
                        )),
                    }),
                },
            ],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![
                0x5B, 0x8E, 0xFF, 0xF7, 0x98, 0x03, 0x81, 0x03, 0xD2, 0x69, 0xB6, 0x33, 0x81, 0x3F,
                0xC6, 0x0C,
            ],
            span_id: vec![0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x74],
            event_name: String::new(),
        };

        let our_log = otel_to_sentry_log(&log_record).expect("conversion should succeed");
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Database query executed".into()))
        );

        let attributes = &annotated_log.value().unwrap().attributes.value().unwrap().0;
        let db_statement = attributes.get("db.statement").unwrap().value().unwrap();

        match db_statement.value.value.value().unwrap() {
            Value::String(s) => assert_eq!(
                s,
                "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
            ),
            _ => panic!("expected string value for db.statement"),
        }
    }

    #[test]
    fn test_severity_text_fallback() {
        // Test severity text fallback when severity number is not in standard range
        let test_cases = vec![
            (0, "trace", OurLogLevel::Trace),
            (0, "debug", OurLogLevel::Debug),
            (0, "info", OurLogLevel::Info),
            (0, "warn", OurLogLevel::Warn),
            (0, "error", OurLogLevel::Error),
            (0, "fatal", OurLogLevel::Fatal),
            (99, "unknown", OurLogLevel::Info), // fallback to Info
        ];

        for (severity_number, severity_text, expected_level) in test_cases {
            let log_record = LogRecord {
                time_unix_nano: 1544712660300000000,
                observed_time_unix_nano: 1544712660300000000,
                severity_number,
                severity_text: severity_text.to_owned(),
                body: Some(AnyValue {
                    value: Some(OtelValue::StringValue("test message".to_owned())),
                }),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![
                    0x5B, 0x8E, 0xFF, 0xF7, 0x98, 0x03, 0x81, 0x03, 0xD2, 0x69, 0xB6, 0x33, 0x81,
                    0x3F, 0xC6, 0x0C,
                ],
                span_id: vec![0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x74],
                event_name: String::new(),
            };

            let result = otel_to_sentry_log(&log_record).expect("conversion should succeed");
            assert_eq!(
                result.level.value().expect("level should be present"),
                &expected_level,
                "severity {} with text '{}' should map to {:?}",
                severity_number,
                severity_text,
                expected_level
            );
        }
    }

    #[test]
    fn test_hex_trace_and_span_id_conversion() {
        let log_record = LogRecord {
            time_unix_nano: 1544712660300000000,
            observed_time_unix_nano: 1544712660300000000,
            severity_number: 10,
            severity_text: "Information".to_owned(),
            body: Some(AnyValue {
                value: Some(OtelValue::StringValue("Test message".to_owned())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![
                0x5B, 0x8E, 0xFF, 0xF7, 0x98, 0x03, 0x81, 0x03, 0xD2, 0x69, 0xB6, 0x33, 0x81, 0x3F,
                0xC6, 0x0C,
            ],
            span_id: vec![0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x74],
            event_name: String::new(),
        };

        let result = otel_to_sentry_log(&log_record).expect("conversion should succeed");

        // Verify trace_id conversion
        let trace_id = result.trace_id.value().expect("trace_id should be present");
        assert_eq!(trace_id.to_string(), "5b8efff798038103d269b633813fc60c");

        // Verify span_id conversion
        let span_id = result.span_id.value().expect("span_id should be present");
        assert_eq!(span_id.to_string(), "eee19b7ec3c1b174");
    }
}
