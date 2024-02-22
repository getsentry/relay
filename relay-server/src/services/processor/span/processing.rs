//! Contains the processing-only functionality.

use std::error::Error;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_dynamic_config::{ErrorBoundary, Feature, GlobalConfig, ProjectConfig};
use relay_event_normalization::{
    normalize_measurements, normalize_performance_score, normalize_user_agent_info_generic,
    span::tag_extraction, validate_span, DynamicMeasurementsConfig, MeasurementsConfig,
    PerformanceScoreConfig, RawUserAgentInfo, TransactionsProcessor,
};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{BrowserContext, Contexts, Event, Span, SpanData};
use relay_metrics::{aggregator::AggregatorConfig, MetricNamespace, UnixTimestamp};
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, Empty};
use relay_spans::{otel_to_sentry_span, otel_trace::Span as OtelSpan};

use crate::envelope::{ContentType, Item, ItemType};
use crate::metrics_extraction::generic::extract_metrics;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::state::{
    EnforcedQuotastate, ProcessedState, ScrubAttachementState,
};
use crate::services::processor::{
    ProcessEnvelopeState, ProcessingError, SpanGroup, TransactionGroup,
};
use crate::utils::ItemAction;

pub fn process<'a>(
    state: EnforcedQuotastate<'a, SpanGroup>,
    config: Arc<Config>,
    global_config: &'_ GlobalConfig,
) -> ProcessedState<'a, SpanGroup> {
    let mut state = state.inner();

    use relay_event_normalization::RemoveOtherProcessor;

    let span_metrics_extraction_config = match state.project_state.config.metric_extraction {
        ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
        _ => None,
    };
    let normalize_span_config = get_normalize_span_config(
        config,
        state.managed_envelope.received_at(),
        global_config.measurements.as_ref(),
        state.project_state.config().measurements.as_ref(),
        state.project_state.config().performance_score.as_ref(),
    );

    let meta = state.managed_envelope.envelope().meta();
    let mut contexts = Contexts::new();
    let user_agent_info = RawUserAgentInfo {
        user_agent: meta.user_agent(),
        client_hints: meta.client_hints().as_deref(),
    };

    normalize_user_agent_info_generic(
        &mut contexts,
        &Annotated::new("".to_string()),
        &user_agent_info,
    );

    state.managed_envelope.retain_items(|item| {
        let mut annotated_span = match item.ty() {
            ItemType::OtelSpan => match serde_json::from_slice::<OtelSpan>(&item.payload()) {
                Ok(otel_span) => Annotated::new(otel_to_sentry_span(otel_span)),
                Err(err) => {
                    relay_log::debug!("failed to parse OTel span: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidJson));
                }
            },
            ItemType::Span => match Annotated::<Span>::from_json_bytes(&item.payload()) {
                Ok(span) => span,
                Err(err) => {
                    relay_log::debug!("failed to parse span: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidJson));
                }
            },

            _ => return ItemAction::Keep,
        };

        if let Err(e) = normalize(
            &mut annotated_span,
            normalize_span_config.clone(),
            Annotated::new(contexts.clone()),
        ) {
            relay_log::debug!("failed to normalize span: {}", e);
            return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
        };

        if let Some(config) = span_metrics_extraction_config {
            let Some(span) = annotated_span.value_mut() else {
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            };
            let metrics = extract_metrics(span, config, Some(&global_config.options));
            state.extracted_metrics.project_metrics.extend(metrics);
            item.set_metrics_extracted(true);
        }

        // TODO: dynamic sampling

        if let Err(e) = scrub(&mut annotated_span, &state.project_state.config) {
            relay_log::error!("failed to scrub span: {e}");
        }

        // TODO: rate limiting

        // Remove additional fields.
        process_value(
            &mut annotated_span,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .ok();

        // Validate for kafka (TODO: this should be moved to kafka producer)
        let annotated_span = match validate(annotated_span) {
            Ok(res) => res,
            Err(err) => {
                relay_log::error!("invalid span: {err}");
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidSpan));
            }
        };

        // Write back:
        let mut new_item = Item::new(ItemType::Span);
        let payload = match annotated_span.to_json() {
            Ok(payload) => payload,
            Err(err) => {
                relay_log::debug!("failed to serialize span: {}", err);
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            }
        };
        new_item.set_payload(ContentType::Json, payload);
        new_item.set_metrics_extracted(item.metrics_extracted());

        *item = new_item;

        ItemAction::Keep
    });

    ProcessedState::new(state)
}

pub fn extract_from_event(
    mut state: ProcessEnvelopeState<TransactionGroup>,
) -> ScrubAttachementState<TransactionGroup> {
    // Only extract spans from transactions (not errors).
    if state.event_type() != Some(EventType::Transaction) {
        return ScrubAttachementState::new(state);
    };

    let mut add_span = |span: Annotated<Span>| {
        let span = match validate(span) {
            Ok(span) => span,
            Err(e) => {
                relay_log::error!("Invalid span: {e}");
                state.managed_envelope.track_outcome(
                    Outcome::Invalid(DiscardReason::InvalidSpan),
                    relay_quotas::DataCategory::SpanIndexed,
                    1,
                );
                return;
            }
        };
        let span = match span.to_json() {
            Ok(span) => span,
            Err(e) => {
                relay_log::error!(error = &e as &dyn Error, "Failed to serialize span");
                state.managed_envelope.track_outcome(
                    Outcome::Invalid(DiscardReason::InvalidSpan),
                    relay_quotas::DataCategory::SpanIndexed,
                    1,
                );
                return;
            }
        };
        let mut item = Item::new(ItemType::Span);
        item.set_payload(ContentType::Json, span);
        // If metrics extraction happened for the event, it also happened for its spans:
        item.set_metrics_extracted(state.event_metrics_extracted);
        state.managed_envelope.envelope_mut().add_item(item);
    };

    let span_metrics_extraction_enabled = state
        .project_state
        .has_feature(Feature::SpanMetricsExtraction);
    let custom_metrics_enabled = state.project_state.has_feature(Feature::CustomMetrics);

    let Some(event) = state.event.value() else {
        return ScrubAttachementState::new(state);
    };

    let extract_transaction_span = span_metrics_extraction_enabled
        || (custom_metrics_enabled && !event._metrics_summary.is_empty());
    let extract_child_spans = span_metrics_extraction_enabled;

    // Extract transaction as a span.
    let mut transaction_span: Span = event.into();

    if extract_child_spans {
        // Add child spans as envelope items.
        if let Some(child_spans) = event.spans.value() {
            for span in child_spans {
                let Some(inner_span) = span.value() else {
                    continue;
                };
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
    }

    if extract_transaction_span {
        // Extract tags to add to this span as well
        let mut shared_tags = tag_extraction::extract_shared_tags(event);

        if let Some(span_op) = transaction_span.op.value() {
            shared_tags.insert(tag_extraction::SpanTagKey::SpanOp, span_op.to_owned());
        }

        transaction_span.sentry_tags = Annotated::new(
            shared_tags
                .clone()
                .into_iter()
                .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
                .collect(),
        );
        add_span(transaction_span.into());
    }

    ScrubAttachementState::new(state)
}

/// Config needed to normalize a standalone span.
#[derive(Clone, Debug)]
struct NormalizeSpanConfig<'a> {
    /// The time at which the event was received in this Relay.
    pub received_at: DateTime<Utc>,
    /// Allowed time range for spans.
    pub timestamp_range: std::ops::Range<UnixTimestamp>,
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_size: usize,
    /// Configuration for generating performance score measurements for web vitals
    pub performance_score: Option<&'a PerformanceScoreConfig>,
    /// Configuration for measurement normalization in transaction events.
    ///
    /// Has an optional [`relay_event_normalization::MeasurementsConfig`] from both the project and the global level.
    /// If at least one is provided, then normalization will truncate custom measurements
    /// and add units of known built-in measurements.
    pub measurements: Option<DynamicMeasurementsConfig<'a>>,
    /// The maximum length for names of custom measurements.
    ///
    /// Measurements with longer names are removed from the transaction event and replaced with a
    /// metadata entry.
    pub max_name_and_unit_len: Option<usize>,
}

fn get_normalize_span_config<'a>(
    config: Arc<Config>,
    received_at: DateTime<Utc>,
    global_measurements_config: Option<&'a MeasurementsConfig>,
    project_measurements_config: Option<&'a MeasurementsConfig>,
    performance_score: Option<&'a PerformanceScoreConfig>,
) -> NormalizeSpanConfig<'a> {
    let aggregator_config =
        AggregatorConfig::from(config.aggregator_config_for(MetricNamespace::Spans));

    NormalizeSpanConfig {
        received_at,
        timestamp_range: aggregator_config.timestamp_range(),
        max_tag_value_size: config
            .aggregator_config_for(MetricNamespace::Spans)
            .max_tag_value_length,
        measurements: Some(DynamicMeasurementsConfig::new(
            project_measurements_config,
            global_measurements_config,
        )),
        max_name_and_unit_len: Some(
            aggregator_config
                .max_name_length
                .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),
        ),
        performance_score,
    }
}

/// Normalizes a standalone span.
fn normalize(
    annotated_span: &mut Annotated<Span>,
    config: NormalizeSpanConfig,
    contexts: Annotated<Contexts>,
) -> Result<(), ProcessingError> {
    use relay_event_normalization::{SchemaProcessor, TimestampProcessor, TrimmingProcessor};

    let NormalizeSpanConfig {
        received_at,
        timestamp_range,
        max_tag_value_size,
        performance_score,
        measurements,
        max_name_and_unit_len,
    } = config;

    // This follows the steps of `NormalizeProcessor::process_event`.
    // Ideally, `NormalizeProcessor` would execute these steps generically, i.e. also when calling
    // `process` on it.

    process_value(
        annotated_span,
        &mut SchemaProcessor,
        ProcessingState::root(),
    )?;

    process_value(
        annotated_span,
        &mut TimestampProcessor,
        ProcessingState::root(),
    )?;

    if let Some(span) = annotated_span.value() {
        validate_span(span, Some(&timestamp_range))?;
    }
    process_value(
        annotated_span,
        &mut TransactionsProcessor::new(Default::default()),
        ProcessingState::root(),
    )?;

    let Some(span) = annotated_span.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    if let Some(browser_name) = contexts
        .value()
        .and_then(|contexts| contexts.get::<BrowserContext>())
        .and_then(|v| v.name.value())
    {
        let data = span.data.value_mut().get_or_insert_with(SpanData::default);
        data.browser_name = Annotated::new(browser_name.to_owned().into());
    }

    if let Annotated(Some(ref mut measurement_values), ref mut meta) = span.measurements {
        normalize_measurements(
            measurement_values,
            meta,
            measurements,
            max_name_and_unit_len,
            span.start_timestamp.0,
            span.timestamp.0,
        );
    }

    let is_segment = span.parent_span_id.is_empty();
    span.is_segment = Annotated::new(is_segment);
    span.received = Annotated::new(received_at.into());

    if is_segment {
        span.segment_id = span.span_id.clone();
    }

    // Tag extraction:
    let config = tag_extraction::Config { max_tag_value_size };
    let is_mobile = false; // TODO: find a way to determine is_mobile from a standalone span.
    let tags = tag_extraction::extract_tags(span, &config, None, None, is_mobile, None);
    span.sentry_tags = Annotated::new(
        tags.into_iter()
            .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
            .collect(),
    );

    let mut event = Event {
        contexts,
        measurements: span.measurements.clone(),
        spans: Annotated::from(vec![Annotated::new(span.clone())]),
        ..Default::default()
    };
    normalize_performance_score(&mut event, performance_score);
    span.measurements = event.measurements;

    tag_extraction::extract_measurements(span);

    process_value(
        annotated_span,
        &mut TrimmingProcessor::new(),
        ProcessingState::root(),
    )?;

    Ok(())
}

#[cfg(feature = "processing")]
fn scrub(
    annotated_span: &mut Annotated<Span>,
    project_config: &ProjectConfig,
) -> Result<(), ProcessingError> {
    if let Some(ref config) = project_config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(annotated_span, &mut processor, ProcessingState::root())?;
    }
    let pii_config = project_config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(annotated_span, &mut processor, ProcessingState::root())?;
    }

    Ok(())
}

/// We do not extract or ingest spans with missing fields if those fields are required on the Kafka topic.
#[cfg(feature = "processing")]
fn validate(mut span: Annotated<Span>) -> Result<Annotated<Span>, anyhow::Error> {
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
