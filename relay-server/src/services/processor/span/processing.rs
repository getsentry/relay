//! Contains the processing-only functionality.

use std::error::Error;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_dynamic_config::{
    CombinedMetricExtractionConfig, ErrorBoundary, Feature, GlobalConfig, ProjectConfig,
};
use relay_event_normalization::{
    normalize_measurements, normalize_performance_score, normalize_user_agent_info_generic,
    span::tag_extraction, validate_span, CombinedMeasurementsConfig, MeasurementsConfig,
    PerformanceScoreConfig, RawUserAgentInfo, TransactionsProcessor,
};
use relay_event_normalization::{normalize_transaction_name, ModelCosts};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{BrowserContext, Contexts, Event, Span, SpanData};
use relay_log::protocol::{Attachment, AttachmentType};
use relay_metrics::{aggregator::AggregatorConfig, MetricNamespace, UnixTimestamp};
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, Empty};
use relay_quotas::DataCategory;
use relay_spans::{otel_to_sentry_span, otel_trace::Span as OtelSpan};

use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::metrics_extraction::generic::extract_metrics;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::span::extract_transaction_span;
use crate::services::processor::{
    dynamic_sampling, Addrs, ProcessEnvelope, ProcessEnvelopeState, ProcessingError,
    ProcessingGroup, SpanGroup, TransactionGroup,
};
use crate::statsd::{RelayCounters, RelayHistograms};
use crate::utils::{sample, BufferGuard, ItemAction};
use relay_event_normalization::span::ai::extract_ai_measurements;
use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
struct ValidationError(#[from] anyhow::Error);

pub fn process(
    state: &mut ProcessEnvelopeState<SpanGroup>,
    config: Arc<Config>,
    global_config: &GlobalConfig,
    addrs: &Addrs,
    buffer_guard: &BufferGuard,
) {
    use relay_event_normalization::RemoveOtherProcessor;

    // We only implement trace-based sampling rules for now, which can be computed
    // once for all spans in the envelope.
    let sampling_result = dynamic_sampling::run(state, &config);

    let span_metrics_extraction_config = match state.project_state.config.metric_extraction {
        ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
        _ => None,
    };
    let ai_model_costs_config = global_config.ai_model_costs.clone().ok();
    let normalize_span_config = get_normalize_span_config(
        Arc::clone(&config),
        state.managed_envelope.received_at(),
        global_config.measurements.as_ref(),
        state.project_state.config().measurements.as_ref(),
        state.project_state.config().performance_score.as_ref(),
        ai_model_costs_config.as_ref(),
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

    let mut extracted_transactions = vec![];
    let should_extract_transactions = state
        .project_state
        .has_feature(Feature::ExtractTransactionFromSegmentSpan);

    let client_ip = state.managed_envelope.envelope().meta().client_addr();
    let filter_settings = &state.project_state.config.filter_settings;

    let mut dynamic_sampling_dropped_spans = 0;
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

        set_segment_attributes(&mut annotated_span);

        if should_extract_transactions && !item.transaction_extracted() {
            if let Some(transaction) = convert_to_transaction(&annotated_span) {
                extracted_transactions.push(transaction);
                item.set_transaction_extracted(true);
            }
        }

        if let Err(e) = normalize(
            &mut annotated_span,
            normalize_span_config.clone(),
            Annotated::new(contexts.clone()),
            state.project_state.config(),
        ) {
            relay_log::debug!("failed to normalize span: {}", e);
            return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
        };

        if let Some(span) = annotated_span.value() {
            if let Err(filter_stat_key) = relay_filter::should_filter(
                span,
                client_ip,
                filter_settings,
                global_config.filters(),
            ) {
                relay_log::trace!(
                    "filtering span {:?} that matched an inbound filter",
                    span.span_id
                );
                return ItemAction::Drop(Outcome::Filtered(filter_stat_key));
            }
        }

        if let Some(config) = span_metrics_extraction_config {
            let Some(span) = annotated_span.value_mut() else {
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            };
            relay_log::trace!("extracting metrics from standalone span {:?}", span.span_id);

            let ErrorBoundary::Ok(global_metrics_config) = &global_config.metric_extraction else {
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            };

            let metrics = extract_metrics(
                span,
                CombinedMetricExtractionConfig::new(global_metrics_config, config),
            );
            state
                .extracted_metrics
                .extend_project_metrics(metrics, Some(sampling_result.decision()));
            item.set_metrics_extracted(true);
        }

        if sampling_result.decision().is_drop() {
            relay_log::trace!("Dropping span because of sampling rule {sampling_result:?}");
            dynamic_sampling_dropped_spans += 1;
            // Drop silently and not with an outcome, we only want to emit an outcome for the
            // indexed category if the span was dropped by dynamic sampling.
            // Dropping through the envelope will emit for both categories.
            return ItemAction::DropSilently;
        }

        if let Err(e) = scrub(&mut annotated_span, &state.project_state.config) {
            relay_log::error!("failed to scrub span: {e}");
        }

        // Remove additional fields.
        process_value(
            &mut annotated_span,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .ok();

        // Validate for kafka (TODO: this should be moved to kafka producer)
        match validate(&mut annotated_span) {
            Ok(res) => res,
            Err(err) => {
                relay_log::with_scope(
                    |scope| {
                        scope.add_attachment(Attachment {
                            buffer: annotated_span.to_json().unwrap_or_default().into(),
                            filename: "span.json".to_owned(),
                            content_type: Some("application/json".to_owned()),
                            ty: Some(AttachmentType::Attachment),
                        })
                    },
                    || {
                        relay_log::error!(
                            error = &err as &dyn Error,
                            source = "standalone",
                            "invalid span"
                        )
                    },
                );
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

    if let Some(outcome) = sampling_result.into_dropped_outcome() {
        state.managed_envelope.track_outcome(
            outcome,
            DataCategory::SpanIndexed,
            dynamic_sampling_dropped_spans,
        );
    }

    let mut transaction_count = 0;
    for transaction in extracted_transactions {
        // Enqueue a full processing request for every extracted transaction item.
        match Envelope::try_from_event(state.envelope().headers().clone(), transaction) {
            Ok(mut envelope) => {
                // In order to force normalization, treat as external:
                envelope.meta_mut().set_from_internal_relay(false);

                // We don't want to extract spans or span metrics from a transaction extracted from spans,
                // so set the spans_extracted flag:
                for item in envelope.items_mut() {
                    item.set_spans_extracted(true);
                }

                transaction_count += 1;

                let managed_envelope = buffer_guard.enter(
                    envelope,
                    addrs.outcome_aggregator.clone(),
                    addrs.test_store.clone(),
                    ProcessingGroup::Transaction,
                );

                match managed_envelope {
                    Ok(managed_envelope) => {
                        addrs.envelope_processor.send(ProcessEnvelope {
                            envelope: managed_envelope,
                            project_state: state.project_state.clone(),
                            sampling_project_state: state.sampling_project_state.clone(),
                            reservoir_counters: state.reservoir.counters(),
                        });
                    }
                    Err(e) => {
                        relay_log::error!(
                            error = &e as &dyn Error,
                            "Failed to obtain permit for spinoff envelope:"
                        );
                    }
                }
            }
            Err(e) => {
                relay_log::error!(
                    error = &e as &dyn Error,
                    "Failed to create spinoff envelope:"
                );
            }
        }
    }

    if transaction_count > 0 {
        relay_statsd::metric!(counter(RelayCounters::TransactionsFromSpans) += transaction_count);
        relay_statsd::metric!(
            histogram(RelayHistograms::TransactionsFromSpansPerEnvelope) = transaction_count as u64
        );
    }
}

pub fn extract_from_event(
    state: &mut ProcessEnvelopeState<TransactionGroup>,
    config: &Config,
    global_config: &GlobalConfig,
) {
    // Only extract spans from transactions (not errors).
    if state.event_type() != Some(EventType::Transaction) {
        return;
    };

    if state.spans_extracted {
        return;
    }

    if let Some(sample_rate) = global_config.options.span_extraction_sample_rate {
        if !sample(sample_rate) {
            return;
        }
    }

    let mut add_span = |mut span: Annotated<Span>| {
        match validate(&mut span) {
            Ok(span) => span,
            Err(e) => {
                relay_log::error!(
                    error = &e as &dyn Error,
                    span = ?span,
                    source = "event",
                    "invalid span"
                );

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

        relay_log::trace!("Adding span to envelope");
        state.managed_envelope.envelope_mut().add_item(item);
    };

    let Some(event) = state.event.value() else {
        return;
    };

    let Some(transaction_span) = extract_transaction_span(
        event,
        config
            .aggregator_config_for(MetricNamespace::Spans)
            .max_tag_value_length,
    ) else {
        return;
    };
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

            add_span(Annotated::new(new_span));
        }
    }

    add_span(transaction_span.into());

    state.spans_extracted = true;
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
    measurements: Option<CombinedMeasurementsConfig<'a>>,
    /// Configuration for AI model cost calculation
    ai_model_costs: Option<&'a ModelCosts>,
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
    ai_model_costs: Option<&'a ModelCosts>,
) -> NormalizeSpanConfig<'a> {
    let aggregator_config =
        AggregatorConfig::from(config.aggregator_config_for(MetricNamespace::Spans));

    NormalizeSpanConfig {
        received_at,
        timestamp_range: aggregator_config.timestamp_range(),
        max_tag_value_size: config
            .aggregator_config_for(MetricNamespace::Spans)
            .max_tag_value_length,
        measurements: Some(CombinedMeasurementsConfig::new(
            project_measurements_config,
            global_measurements_config,
        )),
        max_name_and_unit_len: Some(
            aggregator_config
                .max_name_length
                .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),
        ),
        performance_score,
        ai_model_costs,
    }
}

fn set_segment_attributes(span: &mut Annotated<Span>) {
    let Some(span) = span.value_mut() else { return };

    // Identify INP spans and make sure they are not wrapped in a segment.
    if let Some(span_op) = span.op.value() {
        if span_op.starts_with("ui.interaction.") {
            span.is_segment = None.into();
            span.parent_span_id = None.into();
            span.segment_id = None.into();
            return;
        }
    }

    let Some(span_id) = span.span_id.value() else {
        return;
    };

    if let Some(segment_id) = span.segment_id.value() {
        // The span is a segment if and only if the segment_id matches the span_id.
        span.is_segment = (segment_id == span_id).into();
    } else if span.parent_span_id.is_empty() {
        // If the span has no parent, it is automatically a segment:
        span.is_segment = true.into();
    }

    // If the span is a segment, always set the segment_id to the current span_id:
    if span.is_segment.value() == Some(&true) {
        span.segment_id = span.span_id.clone();
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
        ai_model_costs,
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
        data.browser_name = Annotated::new(browser_name.to_owned());
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

    span.received = Annotated::new(received_at.into());

    if let Some(transaction) = span
        .data
        .value_mut()
        .as_mut()
        .map(|data| &mut data.segment_name)
    {
        normalize_transaction_name(transaction, &project_config.tx_name_rules);
    }

    // Tag extraction:
    let is_mobile = false; // TODO: find a way to determine is_mobile from a standalone span.
    let tags = tag_extraction::extract_tags(span, max_tag_value_size, None, None, is_mobile, None);
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
    if let Some(model_costs_config) = ai_model_costs {
        extract_ai_measurements(span, model_costs_config);
    }
    span.measurements = event.measurements;

    tag_extraction::extract_measurements(span, is_mobile);

    process_value(
        annotated_span,
        &mut TrimmingProcessor::new(),
        ProcessingState::root(),
    )?;

    Ok(())
}

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
fn validate(span: &mut Annotated<Span>) -> Result<(), ValidationError> {
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
                return Err(ValidationError(anyhow::anyhow!(
                    "end timestamp is smaller than start timestamp"
                )));
            }
        }
        (_, None) => {
            return Err(ValidationError(anyhow::anyhow!(
                "timestamp hard-required for spans"
            )));
        }
        (None, _) => {
            return Err(ValidationError(anyhow::anyhow!(
                "start_timestamp hard-required for spans"
            )));
        }
    }

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

    Ok(())
}

fn convert_to_transaction(annotated_span: &Annotated<Span>) -> Option<Event> {
    let span = annotated_span.value()?;

    // HACK: This is an exception from the JS SDK v8 and we do not want to turn it into a transaction.
    if let Some(span_op) = span.op.value() {
        if span_op == "http.client" && span.parent_span_id.is_empty() {
            return None;
        }
    }

    relay_log::trace!("Extracting transaction for span {:?}", &span.span_id);
    Event::try_from(span).ok()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::{
        Context, ContextInner, SpanId, Timestamp, TraceContext, TraceId,
    };
    use relay_protocol::get_value;
    use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator};
    use relay_system::Addr;

    use crate::envelope::Envelope;
    use crate::services::processor::ProcessingGroup;
    use crate::services::project::ProjectState;
    use crate::utils::ManagedEnvelope;

    use super::*;

    fn state() -> ProcessEnvelopeState<'static, TransactionGroup> {
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"transaction\"}\n{}\n",
        );

        let dummy_envelope = Envelope::parse_bytes(bytes).unwrap();
        let mut project_state = ProjectState::allowed();
        project_state
            .config
            .features
            .0
            .insert(Feature::ExtractCommonSpanMetricsFromEvent);

        let event = Event {
            ty: EventType::Transaction.into(),
            start_timestamp: Timestamp(DateTime::from_timestamp(0, 0).unwrap()).into(),
            timestamp: Timestamp(DateTime::from_timestamp(1, 0).unwrap()).into(),

            contexts: Contexts(BTreeMap::from([(
                "trace".into(),
                ContextInner(Context::Trace(Box::new(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    exclusive_time: 1000.0.into(),
                    ..Default::default()
                })))
                .into(),
            )]))
            .into(),
            ..Default::default()
        };

        let managed_envelope = ManagedEnvelope::standalone(
            dummy_envelope,
            Addr::dummy(),
            Addr::dummy(),
            ProcessingGroup::Transaction,
        );

        ProcessEnvelopeState {
            event: Annotated::from(event),
            metrics: Default::default(),
            sample_rates: None,
            extracted_metrics: Default::default(),
            project_state: Arc::new(project_state),
            sampling_project_state: None,
            project_id: ProjectId::new(42),
            managed_envelope: managed_envelope.try_into().unwrap(),
            event_metrics_extracted: false,
            spans_extracted: false,
            reservoir: ReservoirEvaluator::new(ReservoirCounters::default()),
        }
    }

    #[test]
    fn extract_sampled_default() {
        let config = Config::default();
        let global_config = GlobalConfig::default();
        assert!(global_config.options.span_extraction_sample_rate.is_none());
        let mut state = state();
        extract_from_event(&mut state, &config, &global_config);
        assert!(
            state
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::Span),
            "{:?}",
            state.envelope()
        );
    }

    #[test]
    fn extract_sampled_explicit() {
        let config = Config::default();
        let mut global_config = GlobalConfig::default();
        global_config.options.span_extraction_sample_rate = Some(1.0);
        let mut state = state();
        extract_from_event(&mut state, &config, &global_config);
        assert!(
            state
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::Span),
            "{:?}",
            state.envelope()
        );
    }

    #[test]
    fn extract_sampled_dropped() {
        let config = Config::default();
        let mut global_config = GlobalConfig::default();
        global_config.options.span_extraction_sample_rate = Some(0.0);
        let mut state = state();
        extract_from_event(&mut state, &config, &global_config);
        assert!(
            !state
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::Span),
            "{:?}",
            state.envelope()
        );
    }

    #[test]
    fn segment_no_overwrite() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
            "is_segment": true,
            "span_id": "fa90fdead5f74052",
            "parent_span_id": "fa90fdead5f74051"
        }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &true);
        assert_eq!(get_value!(span.segment_id!).0.as_str(), "fa90fdead5f74052");
    }

    #[test]
    fn segment_overwrite_because_of_segment_id() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "is_segment": false,
         "span_id": "fa90fdead5f74052",
         "segment_id": "fa90fdead5f74052",
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &true);
    }

    #[test]
    fn segment_overwrite_because_of_missing_parent() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "is_segment": false,
         "span_id": "fa90fdead5f74052"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &true);
        assert_eq!(get_value!(span.segment_id!).0.as_str(), "fa90fdead5f74052");
    }

    #[test]
    fn segment_no_parent_but_segment() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "span_id": "fa90fdead5f74052",
         "segment_id": "ea90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &false);
        assert_eq!(get_value!(span.segment_id!).0.as_str(), "ea90fdead5f74051");
    }

    #[test]
    fn segment_only_parent() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment), None);
        assert_eq!(get_value!(span.segment_id), None);
    }

    #[test]
    fn not_segment_but_inp_span() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "op": "ui.interaction.click",
         "is_segment": false,
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment), None);
        assert_eq!(get_value!(span.segment_id), None);
    }

    #[test]
    fn segment_but_inp_span() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "op": "ui.interaction.click",
         "segment_id": "fa90fdead5f74051",
         "is_segment": true,
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment), None);
        assert_eq!(get_value!(span.segment_id), None);
    }
}
