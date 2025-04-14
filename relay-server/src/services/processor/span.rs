//! Processor code related to standalone spans.

use std::sync::Arc;

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use prost::Message;
use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_spans::otel_trace::TracesData;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{should_filter, SpanGroup};
use crate::utils::ItemAction;
use crate::utils::TypedEnvelope;

#[cfg(feature = "processing")]
mod processing;
#[cfg(feature = "processing")]
pub use processing::*;
use relay_config::Config;

use crate::services::projects::project::ProjectInfo;

pub fn filter(
    managed_envelope: &mut TypedEnvelope<SpanGroup>,
    config: Arc<Config>,
    project_info: Arc<ProjectInfo>,
) {
    let disabled = should_filter(&config, &project_info, Feature::StandaloneSpanIngestion);
    let otel_disabled = should_filter(&config, &project_info, Feature::OtelEndpoint);

    managed_envelope.retain_items(|item| {
        if disabled && item.is_span() {
            relay_log::debug!("dropping span because feature is disabled");
            ItemAction::DropSilently
        } else if otel_disabled && item.ty() == &ItemType::OtelTracesData {
            relay_log::debug!("dropping otel trace because feature is disabled");
            ItemAction::DropSilently
        } else {
            ItemAction::Keep
        }
    });
}

pub fn convert_otel_traces_data(managed_envelope: &mut TypedEnvelope<SpanGroup>) {
    let envelope = managed_envelope.envelope_mut();

    for item in envelope.take_items_by(|item| item.ty() == &ItemType::OtelTracesData) {
        convert_traces_data(item, managed_envelope);
    }
}

fn convert_traces_data(item: Item, managed_envelope: &mut TypedEnvelope<SpanGroup>) {
    let traces_data = match parse_traces_data(item) {
        Ok(traces_data) => traces_data,
        Err(reason) => {
            // NOTE: logging quantity=1 is semantically wrong, but we cannot know the real quantity
            // without parsing.
            track_invalid(managed_envelope, reason);
            return;
        }
    };
    for resource_spans in traces_data.resource_spans {
        for scope_spans in resource_spans.scope_spans {
            for mut span in scope_spans.spans {
                // Denormalize instrumentation scope and resource attributes into every span.
                if let Some(ref scope) = scope_spans.scope {
                    if !scope.name.is_empty() {
                        span.attributes.push(KeyValue {
                            key: "instrumentation.name".to_owned(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue(scope.name.clone())),
                            }),
                        })
                    }
                    if !scope.version.is_empty() {
                        span.attributes.push(KeyValue {
                            key: "instrumentation.version".to_owned(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue(scope.version.clone())),
                            }),
                        })
                    }
                    scope.attributes.iter().for_each(|a| {
                        span.attributes.push(KeyValue {
                            key: format!("instrumentation.{}", a.key),
                            value: a.value.clone(),
                        });
                    });
                }
                if let Some(ref resource) = resource_spans.resource {
                    resource.attributes.iter().for_each(|a| {
                        span.attributes.push(KeyValue {
                            key: format!("resource.{}", a.key),
                            value: a.value.clone(),
                        });
                    });
                }

                let Ok(payload) = serde_json::to_vec(&span) else {
                    track_invalid(managed_envelope, DiscardReason::Internal);
                    continue;
                };
                let mut item = Item::new(ItemType::OtelSpan);
                item.set_payload(ContentType::Json, payload);
                managed_envelope.envelope_mut().add_item(item);
            }
        }
    }
    managed_envelope.update(); // update envelope summary
}

fn track_invalid(managed_envelope: &mut TypedEnvelope<SpanGroup>, reason: DiscardReason) {
    managed_envelope.track_outcome(Outcome::Invalid(reason), DataCategory::Span, 1);
    managed_envelope.track_outcome(Outcome::Invalid(reason), DataCategory::SpanIndexed, 1);
}

fn parse_traces_data(item: Item) -> Result<TracesData, DiscardReason> {
    match item.content_type() {
        Some(&ContentType::Json) => serde_json::from_slice(&item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as JSON"
            );
            DiscardReason::InvalidJson
        }),
        Some(&ContentType::Protobuf) => TracesData::decode(item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as protobuf"
            );
            DiscardReason::InvalidProtobuf
        }),
        _ => Err(DiscardReason::ContentType),
    }
}

/// Creates a span from the transaction and applies tag extraction on it.
///
/// Returns `None` when [`tag_extraction::extract_span_tags`] clears the span, which it shouldn't.
pub fn extract_transaction_span(
    event: &Event,
    max_tag_value_size: usize,
    span_allowed_hosts: &[String],
) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size, span_allowed_hosts);
    tag_extraction::extract_segment_span_tags(event, &mut spans);

    spans.into_iter().next().and_then(Annotated::into_value)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use relay_spans::otel_trace::Span as OtelSpan;
    use relay_system::Addr;

    use super::*;
    use crate::services::processor::ProcessingGroup;
    use crate::utils::{ManagedEnvelope, TypedEnvelope};
    use crate::Envelope;

    #[test]
    fn attribute_denormalization() {
        // Construct an OTLP trace payload with:
        // - a resource with one attribute, containing:
        // - an instrumentation scope with one attribute, containing:
        // - a span with one attribute
        let traces_data = r#"
        {
            "resourceSpans": [
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
                    "scopeSpans": [
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
                            "spans": [
                                {
                                    "attributes": [
                                        {
                                            "key": "span_key",
                                            "value": {
                                                "stringValue": "span_value"
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        "#;

        // Build an envelope containing the OTLP trace data.
        let bytes =
            Bytes::from(r#"{"dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}"#);
        let envelope = Envelope::parse_bytes(bytes).unwrap();
        let (test_store, _) = Addr::custom();
        let (outcome_aggregator, _) = Addr::custom();
        let managed_envelope = ManagedEnvelope::new(
            envelope,
            outcome_aggregator,
            test_store,
            ProcessingGroup::Span,
        );
        let mut typed_envelope: TypedEnvelope<_> = managed_envelope.try_into().unwrap();
        let mut item = Item::new(ItemType::OtelTracesData);
        item.set_payload(ContentType::Json, traces_data);
        typed_envelope.envelope_mut().add_item(item.clone());

        // Convert the OTLP trace data into `OtelSpan` item(s).
        convert_traces_data(item, &mut typed_envelope);

        // Assert that the attributes from the resource and instrumentation
        // scope were copied.
        let item = typed_envelope
            .envelope()
            .items()
            .find(|i| *i.ty() == ItemType::OtelSpan)
            .expect("converted span missing from envelope");
        let attributes = serde_json::from_slice::<OtelSpan>(&item.payload())
            .expect("unable to deserialize otel span")
            .attributes
            .into_iter()
            .map(|kv| (kv.key, kv.value.unwrap()))
            .collect::<BTreeMap<_, _>>();
        let attribute_value = |key: &str| -> String {
            match attributes
                .get(key)
                .unwrap_or_else(|| panic!("attribute {} missing", key))
                .to_owned()
                .value
            {
                Some(Value::StringValue(str)) => str,
                _ => panic!("attribute {} not a string", key),
            }
        };
        assert_eq!(
            attribute_value("span_key"),
            "span_value".to_owned(),
            "original span attribute should be present"
        );
        assert_eq!(
            attribute_value("instrumentation.name"),
            "test_instrumentation".to_owned(),
            "instrumentation name should be in attributes"
        );
        assert_eq!(
            attribute_value("instrumentation.version"),
            "0.0.1".to_owned(),
            "instrumentation version should be in attributes"
        );
        assert_eq!(
            attribute_value("resource.resource_key"),
            "resource_value".to_owned(),
            "resource attribute should be copied with prefix"
        );
        assert_eq!(
            attribute_value("instrumentation.scope_key"),
            "scope_value".to_owned(),
            "instruementation scope attribute should be copied with prefix"
        );
    }
}
