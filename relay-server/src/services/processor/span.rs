//! Processor code related to standalone spans.

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use prost::Message;
use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_spans::otel_trace::TracesData;

use crate::envelope::{ContentType, Item, ItemContainer, ItemType};
use crate::integrations::{Integration, OtelFormat, SpansIntegration};
use crate::managed::{ItemAction, TypedEnvelope};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{SpanGroup, should_filter};
use crate::statsd::RelayTimers;

#[cfg(feature = "processing")]
mod processing;
use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
pub use processing::*;
use relay_config::Config;

use super::ProcessingError;

pub fn filter(
    managed_envelope: &mut TypedEnvelope<SpanGroup>,
    config: &Config,
    project_info: &ProjectInfo,
) {
    let disabled = should_filter(config, project_info, Feature::StandaloneSpanIngestion);
    if !disabled {
        return;
    }

    managed_envelope.retain_items(|item| {
        let is_span = matches!(item.ty(), &ItemType::Span | &ItemType::OtelSpan)
            || matches!(item.integration(), Some(Integration::Spans(_)));

        match is_span {
            true => {
                relay_log::debug!("dropping span because feature is disabled");
                ItemAction::DropSilently
            }
            false => ItemAction::Keep,
        }
    });
}

/// Expands V2 spans to V1 spans.
///
/// This expands one item (contanining multiple V2 spans) into several
/// (containing one V1 span each).
pub fn expand_v2_spans(
    managed_envelope: &mut TypedEnvelope<SpanGroup>,
) -> Result<(), ProcessingError> {
    let span_v2_items = managed_envelope
        .envelope_mut()
        .take_items_by(ItemContainer::<SpanV2>::is_container);

    // V2 spans must always be sent as an `ItemContainer`, currently it is not allowed to
    // send multiple containers for V2 spans.
    //
    // This restriction may be lifted in the future, this is why this validation only happens
    // when processing is enabled, allowing it to be changed easily in the future.
    //
    // This limit mostly exists to incentivise SDKs to batch multiple spans into a single container,
    // technically it can be removed without issues.
    if span_v2_items.len() > 1 {
        return Err(ProcessingError::DuplicateItem(ItemType::Span));
    }

    if span_v2_items.is_empty() {
        return Ok(());
    }

    let now = std::time::Instant::now();

    for span_v2_item in span_v2_items {
        let spans_v2 = match ItemContainer::parse(&span_v2_item) {
            Ok(spans_v2) => spans_v2,
            Err(err) => {
                relay_log::debug!("failed to parse V2 spans: {err}");
                track_invalid(
                    managed_envelope,
                    DiscardReason::InvalidSpan,
                    span_v2_item.item_count().unwrap_or(1) as usize,
                );
                continue;
            }
        };

        for span_v2 in spans_v2.into_items() {
            let span_v1 = span_v2.value.map_value(relay_spans::span_v2_to_span_v1);
            match span_v1.to_json() {
                Ok(payload) => {
                    let mut new_item = Item::new(ItemType::Span);
                    new_item.set_payload(ContentType::Json, payload);
                    managed_envelope.envelope_mut().add_item(new_item);
                }
                Err(err) => {
                    relay_log::debug!("failed to serialize span: {}", err);
                    track_invalid(managed_envelope, DiscardReason::Internal, 1);
                }
            }
        }
    }

    relay_statsd::metric!(timer(RelayTimers::SpanV2Expansion) = now.elapsed());

    Ok(())
}

pub fn convert_otel_traces_data(managed_envelope: &mut TypedEnvelope<SpanGroup>) {
    let envelope = managed_envelope.envelope_mut();

    for item in envelope.take_items_by(|item| {
        matches!(
            item.integration(),
            Some(Integration::Spans(SpansIntegration::OtelV1 { .. }))
        )
    }) {
        convert_traces_data(item, managed_envelope);
    }
}

fn convert_traces_data(item: Item, managed_envelope: &mut TypedEnvelope<SpanGroup>) {
    let traces_data = match parse_traces_data(item) {
        Ok(traces_data) => traces_data,
        Err(reason) => {
            // NOTE: logging quantity=1 is semantically wrong, but we cannot know the real quantity
            // without parsing.
            track_invalid(managed_envelope, reason, 1);
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
                    track_invalid(managed_envelope, DiscardReason::Internal, 1);
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

fn track_invalid(
    managed_envelope: &mut TypedEnvelope<SpanGroup>,
    reason: DiscardReason,
    quantity: usize,
) {
    managed_envelope.track_outcome(Outcome::Invalid(reason), DataCategory::Span, quantity);
    managed_envelope.track_outcome(
        Outcome::Invalid(reason),
        DataCategory::SpanIndexed,
        quantity,
    );
}

fn parse_traces_data(item: Item) -> Result<TracesData, DiscardReason> {
    let Some(Integration::Spans(SpansIntegration::OtelV1 { format })) = item.integration() else {
        return Err(DiscardReason::ContentType);
    };

    match format {
        OtelFormat::Json => serde_json::from_slice(&item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as JSON"
            );
            DiscardReason::InvalidJson
        }),
        OtelFormat::Protobuf => TracesData::decode(item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as protobuf"
            );
            DiscardReason::InvalidProtobuf
        }),
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

    use super::*;
    use crate::Envelope;
    use crate::managed::{ManagedEnvelope, TypedEnvelope};
    use crate::services::processor::ProcessingGroup;
    use bytes::Bytes;
    use relay_spans::otel_trace::Span as OtelSpan;
    use relay_system::Addr;

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
        let (outcome_aggregator, _) = Addr::custom();
        let managed_envelope = ManagedEnvelope::new(envelope, outcome_aggregator);
        let mut typed_envelope: TypedEnvelope<_> = (managed_envelope, ProcessingGroup::Span)
            .try_into()
            .unwrap();
        let mut item = Item::new(ItemType::Integration);
        item.set_payload(
            Integration::Spans(SpansIntegration::OtelV1 {
                format: OtelFormat::Json,
            })
            .into(),
            traces_data,
        );
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
                .unwrap_or_else(|| panic!("attribute {key} missing"))
                .to_owned()
                .value
            {
                Some(Value::StringValue(str)) => str,
                _ => panic!("attribute {key} not a string"),
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
