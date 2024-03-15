//! Contains the processing-only functionality.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_dynamic_config::{
    ErrorBoundary, Feature, GlobalConfig, MetricExtractionConfig, ProjectConfig,
};
use relay_event_normalization::normalize_transaction_name;
use relay_event_normalization::{
    normalize_measurements, normalize_performance_score, normalize_user_agent_info_generic,
    span::tag_extraction, validate_span, DynamicMeasurementsConfig, MeasurementsConfig,
    PerformanceScoreConfig, RawUserAgentInfo, TransactionsProcessor,
};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{BrowserContext, Contexts, Event, Span, SpanData};
use relay_metrics::Bucket;
use relay_metrics::{aggregator::AggregatorConfig, MetricNamespace, UnixTimestamp};
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, Empty};
use relay_spans::{otel_to_sentry_span, otel_trace::Span as OtelSpan};

use crate::envelope::{ContentType, Item, ItemType};
use crate::metrics_extraction::generic::extract_metrics;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{
    ProcessEnvelopeState, ProcessingError, SpanGroup, TransactionGroup,
};
use crate::statsd::RelayTimers;
use crate::utils::{ItemAction, ManagedEnvelope};

#[cfg(feature = "processing")]
use relay_protocol::Meta;

pub fn process(
    state: &mut ProcessEnvelopeState<SpanGroup>,
    config: Arc<Config>,
    global_config: &GlobalConfig,
) {
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
            state.project_state.config(),
        ) {
            relay_log::debug!("failed to normalize span: {}", e);
            return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
        };

        if let Some(config) = span_metrics_extraction_config {
            let Some(span) = annotated_span.value_mut() else {
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            };
            let metrics = extract_metrics(span, config);
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
            Ok((span, meta)) => Annotated(Some(span), meta),
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
}

/// Copies spans from the state's transaction event to individual span envelope items.
///
/// Also performs span normalization (tag extraction) and metrics extraction.
pub fn extract_from_event(state: &mut ProcessEnvelopeState<TransactionGroup>) {
    // Only extract spans from transactions (not errors).
    if state.event_type() != Some(EventType::Transaction) {
        return;
    };

    if !state
        .project_state
        .has_feature(Feature::ExtractSpansAndSpanMetricsFromEvent)
    {
        return;
    }

    let Some(event) = state.event.value() else {
        return;
    };

    let metrics_extraction_config = match state.project_state.config.metric_extraction {
        ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
        _ => None,
    };

    // Extract transaction as a span.
    let mut transaction_span: Span = event.into();

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
            new_span.platform = transaction_span.platform.clone();

            // If a profile is associated with the transaction, also associate it with its
            // child spans.
            new_span.profile_id = transaction_span.profile_id.clone();

            add_span(
                Annotated::new(new_span),
                metrics_extraction_config,
                &mut state.managed_envelope,
                &mut state.extracted_metrics.project_metrics,
            );
        }
    }

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
    add_span(
        transaction_span.into(),
        metrics_extraction_config,
        &mut state.managed_envelope,
        &mut state.extracted_metrics.project_metrics,
    );
}

fn add_span(
    span: Annotated<Span>,
    metrics_extraction_config: Option<&MetricExtractionConfig>,
    managed_envelope: &mut ManagedEnvelope,
    project_metrics: &mut Vec<Bucket>,
) {
    match into_item(span, metrics_extraction_config) {
        Ok((item, metrics)) => {
            managed_envelope.envelope_mut().add_item(item);
            project_metrics.extend(metrics);
        }
        Err(e) => {
            relay_log::error!("Invalid span: {e}");
            managed_envelope.track_outcome(
                Outcome::Invalid(DiscardReason::InvalidSpan),
                relay_quotas::DataCategory::SpanIndexed,
                1,
            );
        }
    }
}

/// Converts the span to an envelope item and extracts metrics for it.
fn into_item(
    span: Annotated<Span>,
    metrics_extraction_config: Option<&MetricExtractionConfig>,
) -> Result<(Item, Vec<Bucket>), anyhow::Error> {
    let (span, meta) = validate(span)?;

    let metrics = metrics_extraction_config.map(|config| {
        relay_statsd::metric!(timer(RelayTimers::EventProcessingSpanMetricsExtraction), {
            extract_metrics(&span, config)
        })
    });

    let span = Annotated(Some(span), meta).to_json()?;
    let mut item = Item::new(ItemType::Span);
    item.set_payload(ContentType::Json, span);
    item.set_metrics_extracted(metrics.is_some());

    Ok((item, metrics.unwrap_or_default()))
}

/// Removes the transaction in case the project has made the transition to spans-only.
pub fn maybe_discard_transaction(state: &mut ProcessEnvelopeState<TransactionGroup>) {
    if state.event_type() == Some(EventType::Transaction)
        && state.project_state.has_feature(Feature::DiscardTransaction)
    {
        state.remove_event();
        state.managed_envelope.update();
    }
}
/// Config needed to normalize a standalone span.
#[derive(Clone, Debug)]
struct NormalizeSpanConfig<'a> {
    /// The time at which the event was received in this Relay.
    received_at: DateTime<Utc>,
    /// Allowed time range for spans.
    timestamp_range: std::ops::Range<UnixTimestamp>,
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    max_tag_value_size: usize,
    /// Configuration for generating performance score measurements for web vitals
    performance_score: Option<&'a PerformanceScoreConfig>,
    /// Configuration for measurement normalization in transaction events.
    ///
    /// Has an optional [`relay_event_normalization::MeasurementsConfig`] from both the project and the global level.
    /// If at least one is provided, then normalization will truncate custom measurements
    /// and add units of known built-in measurements.
    measurements: Option<DynamicMeasurementsConfig<'a>>,
    /// The maximum length for names of custom measurements.
    ///
    /// Measurements with longer names are removed from the transaction event and replaced with a
    /// metadata entry.
    max_name_and_unit_len: Option<usize>,
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
    project_config: &ProjectConfig,
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

    if let Some(transaction) = span
        .data
        .value_mut()
        .as_mut()
        .map(|data| &mut data.transaction)
    {
        normalize_transaction_name(transaction, &project_config.tx_name_rules);
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
fn validate(span: Annotated<Span>) -> Result<(Span, Meta), anyhow::Error> {
    let Annotated(Some(mut inner), meta) = span else {
        return Err(anyhow::anyhow!("empty span"));
    };

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

    Ok((inner, meta))
}
