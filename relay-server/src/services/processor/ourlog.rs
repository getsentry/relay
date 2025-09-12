use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use prost::Message;
use relay_ourlogs::{otel_logs::LogsData, otel_to_sentry_log};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, ContentType, Item, ItemContainer, ItemType, WithHeader};
use crate::managed::ManagedEnvelope;
use crate::services::outcome::{DiscardReason, Outcome};

pub fn convert_otel_logs(envelope: &mut ManagedEnvelope) {
    let items = envelope
        .envelope_mut()
        .take_items_by(|item| item.ty() == &ItemType::OtelLogsData);
    let mut logs = ContainerItems::new();

    for item in items {
        match parse_logs_data(item) {
            Ok(logs_data) => {
                for resource_logs in logs_data.resource_logs {
                    for scope_logs in resource_logs.scope_logs {
                        for mut log_record in scope_logs.log_records {
                            // Denormalize instrumentation scope and resource attributes into every log record.
                            if let Some(ref scope) = scope_logs.scope {
                                if !scope.name.is_empty() {
                                    log_record.attributes.push(KeyValue {
                                        key: "instrumentation.name".to_owned(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue(scope.name.clone())),
                                        }),
                                    })
                                }
                                if !scope.version.is_empty() {
                                    log_record.attributes.push(KeyValue {
                                        key: "instrumentation.version".to_owned(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue(scope.version.clone())),
                                        }),
                                    })
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

                            match otel_to_sentry_log(log_record) {
                                Ok(our_log) => {
                                    logs.push(WithHeader::just(Annotated::new(our_log)));
                                }
                                Err(_error) => {
                                    relay_log::debug!(
                                        "Failed to convert OTLP log record to Sentry log"
                                    );
                                    envelope.track_outcome(
                                        Outcome::Invalid(DiscardReason::Internal),
                                        DataCategory::LogItem,
                                        1,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Err(reason) => {
                relay_log::debug!("Failed to parse OTLP logs data");
                envelope.track_outcome(Outcome::Invalid(reason), DataCategory::LogItem, 1);
            }
        }
    }

    if logs.is_empty() {
        return;
    }

    let mut item = Item::new(ItemType::Log);
    if let Ok(()) = ItemContainer::from(logs).write_to(&mut item) {
        envelope.envelope_mut().add_item(item);
    }
}

fn parse_logs_data(item: Item) -> Result<LogsData, DiscardReason> {
    match item.content_type() {
        Some(&ContentType::Json) => serde_json::from_slice(&item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as JSON"
            );
            DiscardReason::InvalidJson
        }),
        Some(&ContentType::Protobuf) => LogsData::decode(item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as protobuf"
            );
            DiscardReason::InvalidProtobuf
        }),
        _ => Err(DiscardReason::ContentType),
    }
}
