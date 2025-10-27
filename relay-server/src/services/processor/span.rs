//! Processor code related to standalone spans.

use prost::Message;
use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_spans::otel_trace::TracesData;

use crate::envelope::{ContentType, Item, ItemType};
use crate::integrations::{Integration, OtelFormat, SpansIntegration};
use crate::managed::{ItemAction, TypedEnvelope};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{SpanGroup, should_filter};

#[cfg(feature = "processing")]
mod processing;
use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
pub use processing::*;
use relay_config::Config;

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
        let is_span = matches!(item.ty(), &ItemType::Span)
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
            for span in scope_spans.spans {
                let span = relay_spans::otel_to_sentry_span_v1(
                    span,
                    resource_spans.resource.as_ref(),
                    scope_spans.scope.as_ref(),
                );

                let Ok(payload) = Annotated::new(span).to_json() else {
                    track_invalid(managed_envelope, DiscardReason::Internal, 1);
                    continue;
                };

                let mut item = Item::new(ItemType::Span);
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

    use super::*;
    use crate::Envelope;
    use crate::managed::{ManagedEnvelope, TypedEnvelope};
    use crate::services::processor::ProcessingGroup;
    use bytes::Bytes;
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
                                    "traceId": "89143b0763095bd9c9955e8175d1fb23",
                                    "spanId": "e342abb1214ca181",
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

        // Convert the OTLP trace data into `Span` item(s).
        convert_traces_data(item, &mut typed_envelope);

        // Assert that the attributes from the resource and instrumentation
        // scope were copied.
        let item = typed_envelope
            .envelope()
            .items()
            .find(|i| *i.ty() == ItemType::Span)
            .expect("converted span missing from envelope");

        let payload = serde_json::from_slice::<serde_json::Value>(&item.payload()).unwrap();
        insta::assert_json_snapshot!(payload, @r#"
        {
          "data": {
            "instrumentation.name": "test_instrumentation",
            "instrumentation.scope_key": "scope_value",
            "instrumentation.version": "0.0.1",
            "resource.resource_key": "resource_value",
            "sentry.origin": "auto.otlp.spans",
            "span_key": "span_value"
          },
          "exclusive_time": 0.0,
          "is_remote": false,
          "links": [],
          "op": "default",
          "span_id": "e342abb1214ca181",
          "start_timestamp": 0.0,
          "status": "ok",
          "timestamp": 0.0,
          "trace_id": "89143b0763095bd9c9955e8175d1fb23"
        }
        "#);
    }
}
