//! Processor code related to standalone spans.
#[cfg(feature = "processing")]
use {
    crate::actors::processor::ProcessingError,
    crate::envelope::{ContentType, Item},
    crate::metrics_extraction::generic::extract_metrics,
    chrono::{DateTime, Utc},
    relay_base_schema::events::EventType,
    relay_config::Config,
    relay_dynamic_config::ErrorBoundary,
    relay_event_normalization::span::tag_extraction,
    relay_event_normalization::NormalizeProcessorConfig,
    relay_event_schema::processor::{process_value, ProcessingState},
    relay_event_schema::protocol::{Contexts, Event, Span, Timestamp, TraceContext},
    relay_metrics::MetricNamespace,
    relay_protocol::{Annotated, Empty},
    std::error::Error,
    std::sync::Arc,
};

use {
    crate::actors::processor::ProcessEnvelopeState, crate::envelope::ItemType,
    crate::utils::ItemAction, relay_dynamic_config::Feature,
};

pub fn filter(state: &mut ProcessEnvelopeState) {
    let standalone_span_ingestion_enabled = state
        .project_state
        .has_feature(Feature::StandaloneSpanIngestion);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::OtelSpan | ItemType::Span => {
            if !standalone_span_ingestion_enabled {
                relay_log::warn!("dropping span because feature is disabled");
                ItemAction::DropSilently
            } else {
                ItemAction::Keep
            }
        }
        _ => ItemAction::Keep,
    });
}

/// Config needed to normalize a standalone span.
#[cfg(feature = "processing")]
struct NormalizeSpanConfig {
    /// The time at which the event was received in this Relay.
    pub received_at: DateTime<Utc>,
    /// The maximum amount of seconds an event can be dated in the past.
    pub max_secs_in_past: i64,
    /// The maximum amount of seconds an event can be predated into the future.
    pub max_secs_in_future: i64,
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_length: usize,
}

/// Normalizes a standalone span.
///
/// Internally encapsulates the span in a temporary event in order to run
/// full normalization on it.
#[cfg(feature = "processing")]
fn normalize(
    mut annotated_span: Annotated<Span>,
    config: NormalizeSpanConfig,
) -> Result<Annotated<Span>, ProcessingError> {
    let Some(span) = annotated_span.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    span.is_segment = Annotated::new(span.parent_span_id.is_empty());
    span.received = Annotated::new(Timestamp(Utc::now()));

    let mut pseudo_event = Annotated::new(Event {
        ty: EventType::Transaction.into(),
        timestamp: span.timestamp.clone(),
        start_timestamp: span.start_timestamp.clone(),
        received: span.received.clone(),
        contexts: {
            let mut contexts = Contexts::new();
            contexts.add(TraceContext {
                trace_id: span.trace_id.clone(),
                span_id: span.span_id.clone(),
                ..Default::default()
            });
            contexts.into()
        },
        spans: Annotated::new(vec![annotated_span]),
        ..Default::default()
    });

    let NormalizeSpanConfig {
        received_at,
        max_secs_in_past,
        max_secs_in_future,
        max_tag_value_length,
    } = config;

    let processor_config = NormalizeProcessorConfig {
        received_at: Some(received_at),
        max_secs_in_past: Some(max_secs_in_past),
        max_secs_in_future: Some(max_secs_in_future),
        enrich_spans: true,
        max_tag_value_length,
        is_renormalize: false,
        light_normalize_spans: true,
        enable_trimming: true,
        ..Default::default()
    };

    process_value(
        &mut pseudo_event,
        &mut relay_event_normalization::NormalizeProcessor::new(processor_config),
        ProcessingState::root(),
    )?;

    // Take the span back from the event
    match pseudo_event
        .into_value()
        .and_then(|e| e.spans.into_value().and_then(|mut spans| spans.pop()))
    {
        Some(mut annotated_span) => {
            // HACK remove auto-generated transaction tag:
            if let Some(span) = annotated_span.value_mut() {
                if let Some(tags) = span.sentry_tags.value_mut() {
                    tags.remove("transaction");
                }
            }
            Ok(annotated_span)
        }
        None => Err(ProcessingError::NoEventPayload),
    }
}

#[cfg(feature = "processing")]
pub fn process(state: &mut ProcessEnvelopeState, config: Arc<Config>) {
    let span_metrics_extraction_config = match state.project_state.config.metric_extraction {
        ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
        _ => None,
    };
    let received_at = state.managed_envelope.received_at();

    state.managed_envelope.retain_items(|item| {
        let annotated_span = match item.ty() {
            ItemType::OtelSpan => {
                match serde_json::from_slice::<relay_spans::OtelSpan>(&item.payload()) {
                    Ok(otel_span) => Annotated::new(otel_span.into()),
                    Err(err) => {
                        relay_log::debug!("failed to parse OTel span: {}", err);
                        return ItemAction::DropSilently;
                    }
                }
            }
            ItemType::Span => match Annotated::<Span>::from_json_bytes(&item.payload()) {
                Ok(span) => span,
                Err(err) => {
                    relay_log::debug!("failed to parse span: {}", err);
                    return ItemAction::DropSilently;
                }
            },

            _ => return ItemAction::Keep,
        };

        let config = NormalizeSpanConfig {
            received_at,
            max_secs_in_past: config.max_secs_in_past(),
            max_secs_in_future: config.max_secs_in_future(),
            max_tag_value_length: config
                .aggregator_config_for(MetricNamespace::Spans)
                .max_tag_value_length,
        };
        let mut annotated_span = match normalize(annotated_span, config) {
            Ok(s) => s,
            Err(e) => {
                relay_log::debug!("failed to normalize span: {}", e);
                return ItemAction::DropSilently;
            }
        };

        let Some(span) = annotated_span.value_mut() else {
            return ItemAction::DropSilently;
        };

        if let Some(config) = span_metrics_extraction_config {
            let metrics = extract_metrics(span, config);
            state.extracted_metrics.project_metrics.extend(metrics);
        }

        // TODO: dynamic sampling

        // TODO: retain processor
        // TODO: pii processor

        // TODO: rate limiting

        // Validate for kafka (TODO: this should be moved to kafka producer)
        let annotated_span = match validate(annotated_span) {
            Ok(res) => res,
            Err(err) => {
                relay_log::error!("invalid span: {err}");
                return ItemAction::DropSilently;
            }
        };

        // Write back:
        let mut new_item = Item::new(ItemType::Span);
        let payload = match annotated_span.to_json() {
            Ok(payload) => payload,
            Err(err) => {
                relay_log::debug!("failed to serialize span: {}", err);
                return ItemAction::DropSilently;
            }
        };
        new_item.set_payload(ContentType::Json, payload);

        *item = new_item;

        ItemAction::Keep
    });
}

/// We do not extract spans with missing fields if those fields are required on the Kafka topic.
#[cfg(feature = "processing")]
pub fn validate(mut span: Annotated<Span>) -> Result<Annotated<Span>, anyhow::Error> {
    let inner = span
        .value_mut()
        .as_mut()
        .ok_or(anyhow::anyhow!("empty span"))?;
    let Span {
        ref exclusive_time,
        ref mut tags,
        ref mut sentry_tags,
        ref mut start_timestamp,
        ref mut timestamp,
        ref mut span_id,
        ref mut trace_id,
        ..
    } = inner;

    trace_id
        .value()
        .ok_or(anyhow::anyhow!("span is missing trace_id"))?;
    span_id
        .value()
        .ok_or(anyhow::anyhow!("span is missing span_id"))?;

    match (start_timestamp.value(), timestamp.value()) {
        (Some(start), Some(end)) => {
            if end < start {
                return Err(anyhow::anyhow!(
                    "end timestamp is smaller than start timestamp"
                ));
            }
        }
        (_, None) => {
            return Err(anyhow::anyhow!("timestamp hard-required for spans"));
        }
        (None, _) => {
            return Err(anyhow::anyhow!("start_timestamp hard-required for spans"));
        }
    }

    // `is_segment` is set by `extract_span`.
    exclusive_time
        .value()
        .ok_or(anyhow::anyhow!("missing exclusive_time"))?;

    if let Some(sentry_tags) = sentry_tags.value_mut() {
        sentry_tags.retain(|key, value| match value.value() {
            Some(s) => {
                match key.as_str() {
                    "group" => {
                        // Only allow up to 16-char hex strings in group.
                        s.len() <= 16 && s.chars().all(|c| c.is_ascii_hexdigit())
                    }
                    "status_code" => s.parse::<u16>().is_ok(),
                    _ => true,
                }
            }
            // Drop empty string values.
            None => false,
        });
    }
    if let Some(tags) = tags.value_mut() {
        tags.retain(|_, value| !value.value().is_empty())
    }

    Ok(span)
}

#[cfg(feature = "processing")]
fn is_allowed(span: &Span) -> bool {
    let Some(op) = span.op.value() else {
        return false;
    };
    let Some(description) = span.description.value() else {
        return false;
    };
    let system: &str = span
        .data
        .value()
        .and_then(|v| v.get("span.system"))
        .and_then(|system| system.as_str())
        .unwrap_or_default();
    op.contains("resource.script")
        || op.contains("resource.css")
        || op == "http.client"
        || op.starts_with("app.")
        || op.starts_with("ui.load")
        || op.starts_with("file")
        || op.starts_with("db")
            && !(op.contains("clickhouse")
                || op.contains("mongodb")
                || op.contains("redis")
                || op.contains("compiler"))
            && !(op == "db.sql.query" && (description.contains("\"$") || system == "mongodb"))
}

#[cfg(feature = "processing")]
pub fn extract_from_event(state: &mut ProcessEnvelopeState) {
    // Only extract spans from transactions (not errors).
    if state.event_type() != Some(EventType::Transaction) {
        return;
    };

    // Check feature flag.
    if !state
        .project_state
        .has_feature(Feature::SpanMetricsExtraction)
    {
        return;
    };

    let mut add_span = |span: Annotated<Span>| {
        let span = match validate(span) {
            Ok(span) => span,
            Err(e) => {
                relay_log::error!("Invalid span: {e}");
                return;
            }
        };
        let span = match span.to_json() {
            Ok(span) => span,
            Err(e) => {
                relay_log::error!(error = &e as &dyn Error, "Failed to serialize span");
                return;
            }
        };
        let mut item = Item::new(ItemType::Span);
        item.set_payload(ContentType::Json, span);
        state.managed_envelope.envelope_mut().add_item(item);
    };

    let Some(event) = state.event.value() else {
        return;
    };

    // Extract transaction as a span.
    let mut transaction_span: Span = event.into();

    let all_modules_enabled = state
        .project_state
        .has_feature(Feature::SpanMetricsExtractionAllModules);

    // Add child spans as envelope items.
    if let Some(child_spans) = event.spans.value() {
        for span in child_spans {
            let Some(inner_span) = span.value() else {
                continue;
            };
            // HACK: filter spans based on module until we figure out grouping.
            if !all_modules_enabled && !is_allowed(inner_span) {
                continue;
            }
            // HACK: clone the span to set the segment_id. This should happen
            // as part of normalization once standalone spans reach wider adoption.
            let mut new_span = inner_span.clone();
            new_span.is_segment = Annotated::new(false);
            new_span.received = transaction_span.received.clone();
            new_span.segment_id = transaction_span.segment_id.clone();

            // If a profile is associated with the transaction, also associate it with its
            // child spans.
            new_span.profile_id = transaction_span.profile_id.clone();

            add_span(Annotated::new(new_span));
        }
    }

    // Extract tags to add to this span as well
    let shared_tags = tag_extraction::extract_shared_tags(event);
    transaction_span.sentry_tags = Annotated::new(
        shared_tags
            .clone()
            .into_iter()
            .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
            .collect(),
    );
    add_span(transaction_span.into());
}
