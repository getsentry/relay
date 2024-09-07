//! Contains the processing-only functionality.

use std::error::Error;

use chrono::{DateTime, Utc};
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_dynamic_config::{
    CombinedMetricExtractionConfig, ErrorBoundary, Feature, GlobalConfig, ProjectConfig,
};
use relay_event_normalization::span::ai::extract_ai_measurements;
use relay_event_normalization::span::description::ScrubMongoDescription;
use relay_event_normalization::{
    normalize_measurements, normalize_performance_score, span::tag_extraction, validate_span,
    CombinedMeasurementsConfig, MeasurementsConfig, PerformanceScoreConfig, RawUserAgentInfo,
    TransactionsProcessor,
};
use relay_event_normalization::{
    normalize_transaction_name, ClientHints, FromUserAgentInfo, ModelCosts, SchemaProcessor,
    TimestampProcessor, TransactionNameRule, TrimmingProcessor,
};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{BrowserContext, Span, SpanData};
use relay_log::protocol::{Attachment, AttachmentType};
use relay_metrics::{MetricNamespace, UnixTimestamp};
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, Empty};
use relay_quotas::DataCategory;
use relay_spans::otel_trace::Span as OtelSpan;
use thiserror::Error;

use crate::envelope::{ContentType, Item, ItemType};
use crate::metrics_extraction::metrics_summary;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::span::extract_transaction_span;
use crate::services::processor::{
    dynamic_sampling, ProcessEnvelopeState, ProcessingError, SpanGroup, TransactionGroup,
};
use crate::utils::{sample, ItemAction, ManagedEnvelope};

#[derive(Error, Debug)]
#[error(transparent)]
struct ValidationError(#[from] anyhow::Error);

pub fn process(state: &mut ProcessEnvelopeState<SpanGroup>, global_config: &GlobalConfig) {
    use relay_event_normalization::RemoveOtherProcessor;

    // We only implement trace-based sampling rules for now, which can be computed
    // once for all spans in the envelope.
    let sampling_result = dynamic_sampling::run(state);

    let span_metrics_extraction_config = match state.project_state.config.metric_extraction {
        ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
        _ => None,
    };
    let normalize_span_config = NormalizeSpanConfig::new(
        &state.config,
        global_config,
        state.project_state.config(),
        &state.managed_envelope,
    );

    let client_ip = state.managed_envelope.envelope().meta().client_addr();
    let filter_settings = &state.project_state.config.filter_settings;

    let mut dynamic_sampling_dropped_spans = 0;
    state.managed_envelope.retain_items(|item| {
        let mut annotated_span = match item.ty() {
            ItemType::OtelSpan => match serde_json::from_slice::<OtelSpan>(&item.payload()) {
                Ok(otel_span) => Annotated::new(relay_spans::otel_to_sentry_span(otel_span)),
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

        if let Err(e) = normalize(&mut annotated_span, normalize_span_config.clone()) {
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

            let (metrics, metrics_summary) = metrics_summary::extract_and_summarize_metrics(
                span,
                CombinedMetricExtractionConfig::new(global_metrics_config, config),
            );
            metrics_summary.apply_on(&mut span._metrics_summary);

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
}

pub fn extract_from_event(
    state: &mut ProcessEnvelopeState<TransactionGroup>,
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
        state
            .config
            .aggregator_config_for(MetricNamespace::Spans)
            .aggregator
            .max_tag_value_length,
        &[],
        if state
            .project_state
            .config
            .features
            .has(Feature::ScrubMongoDbDescriptions)
        {
            ScrubMongoDescription::Enabled
        } else {
            ScrubMongoDescription::Disabled
        },
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
    max_name_and_unit_len: usize,
    /// Transaction name normalization rules.
    tx_name_rules: &'a [TransactionNameRule],
    /// The user agent parsed from the request.
    user_agent: Option<String>,
    /// Client hints parsed from the request.
    client_hints: ClientHints<String>,
    /// Hosts that are not replaced by "*" in HTTP span grouping.
    allowed_hosts: &'a [String],
    /// Whether or not to scrub MongoDB span descriptions during normalization.
    scrub_mongo_description: ScrubMongoDescription,
}

impl<'a> NormalizeSpanConfig<'a> {
    fn new(
        config: &'a Config,
        global_config: &'a GlobalConfig,
        project_config: &'a ProjectConfig,
        managed_envelope: &ManagedEnvelope,
    ) -> Self {
        let aggregator_config = config.aggregator_config_for(MetricNamespace::Spans);

        Self {
            received_at: managed_envelope.received_at(),
            timestamp_range: aggregator_config.aggregator.timestamp_range(),
            max_tag_value_size: aggregator_config.aggregator.max_tag_value_length,
            performance_score: project_config.performance_score.as_ref(),
            measurements: Some(CombinedMeasurementsConfig::new(
                project_config.measurements.as_ref(),
                global_config.measurements.as_ref(),
            )),
            ai_model_costs: match &global_config.ai_model_costs {
                ErrorBoundary::Err(_) => None,
                ErrorBoundary::Ok(costs) => Some(costs),
            },
            max_name_and_unit_len: aggregator_config
                .aggregator
                .max_name_length
                .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),

            tx_name_rules: &project_config.tx_name_rules,
            user_agent: managed_envelope
                .envelope()
                .meta()
                .user_agent()
                .map(String::from),
            client_hints: managed_envelope.meta().client_hints().clone(),
            allowed_hosts: global_config.options.http_span_allowed_hosts.as_slice(),
            scrub_mongo_description: if project_config
                .features
                .has(Feature::ScrubMongoDbDescriptions)
            {
                ScrubMongoDescription::Enabled
            } else {
                ScrubMongoDescription::Disabled
            },
        }
    }
}

fn set_segment_attributes(span: &mut Annotated<Span>) {
    let Some(span) = span.value_mut() else { return };

    // Identify INP spans or other WebVital spans and make sure they are not wrapped in a segment.
    if let Some(span_op) = span.op.value() {
        if span_op.starts_with("ui.interaction.") || span_op.starts_with("ui.webvital.") {
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
) -> Result<(), ProcessingError> {
    let NormalizeSpanConfig {
        received_at,
        timestamp_range,
        max_tag_value_size,
        performance_score,
        measurements,
        ai_model_costs,
        max_name_and_unit_len,
        tx_name_rules,
        user_agent,
        client_hints,
        allowed_hosts,
        scrub_mongo_description,
    } = config;

    set_segment_attributes(annotated_span);

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

    populate_ua_fields(span, user_agent.as_deref(), client_hints.as_deref());

    if let Annotated(Some(ref mut measurement_values), ref mut meta) = span.measurements {
        normalize_measurements(
            measurement_values,
            meta,
            measurements,
            Some(max_name_and_unit_len),
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
        normalize_transaction_name(transaction, tx_name_rules);
    }

    // Tag extraction:
    let is_mobile = false; // TODO: find a way to determine is_mobile from a standalone span.
    let tags = tag_extraction::extract_tags(
        span,
        max_tag_value_size,
        None,
        None,
        is_mobile,
        None,
        allowed_hosts,
        scrub_mongo_description,
    );
    span.sentry_tags = Annotated::new(
        tags.into_iter()
            .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
            .collect(),
    );

    normalize_performance_score(span, performance_score);
    if let Some(model_costs_config) = ai_model_costs {
        extract_ai_measurements(span, model_costs_config);
    }

    tag_extraction::extract_measurements(span, is_mobile);

    process_value(
        annotated_span,
        &mut TrimmingProcessor::new(),
        ProcessingState::root(),
    )?;

    Ok(())
}

fn populate_ua_fields(
    span: &mut Span,
    request_user_agent: Option<&str>,
    mut client_hints: ClientHints<&str>,
) {
    let data = span.data.value_mut().get_or_insert_with(SpanData::default);

    let user_agent = data.user_agent_original.value_mut();
    if user_agent.is_none() {
        *user_agent = request_user_agent.map(String::from);
    } else {
        // User agent in span payload should take precendence over request
        // client hints.
        client_hints = ClientHints::default();
    }

    if data.browser_name.value().is_none() {
        if let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
            user_agent: user_agent.as_deref(),
            client_hints,
        }) {
            data.browser_name = context.name;
        }
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::{
        Context, ContextInner, SpanId, Timestamp, TraceContext, TraceId,
    };
    use relay_event_schema::protocol::{Contexts, Event, Span};
    use relay_protocol::get_value;
    use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator};
    use relay_system::Addr;

    use crate::envelope::Envelope;
    use crate::services::processor::{ProcessingExtractedMetrics, ProcessingGroup};
    use crate::services::project::ProjectInfo;
    use crate::utils::ManagedEnvelope;

    use super::*;

    fn state() -> ProcessEnvelopeState<'static, TransactionGroup> {
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"transaction\"}\n{}\n",
        );

        let dummy_envelope = Envelope::parse_bytes(bytes).unwrap();
        let mut project_state = ProjectInfo::default();
        project_state
            .config
            .features
            .0
            .insert(Feature::ExtractCommonSpanMetricsFromEvent);
        let project_state = Arc::new(project_state);

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

        let managed_envelope = ManagedEnvelope::new(
            dummy_envelope,
            Addr::dummy(),
            Addr::dummy(),
            ProcessingGroup::Transaction,
        );

        ProcessEnvelopeState {
            event: Annotated::from(event),
            metrics: Default::default(),
            sample_rates: None,
            extracted_metrics: ProcessingExtractedMetrics::new(),
            config: Arc::new(Config::default()),
            project_state,
            sampling_project_state: None,
            project_id: ProjectId::new(42),
            managed_envelope: managed_envelope.try_into().unwrap(),
            event_metrics_extracted: false,
            spans_extracted: false,
            reservoir: ReservoirEvaluator::new(ReservoirCounters::default()),
            event_fully_normalized: false,
        }
    }

    #[test]
    fn extract_sampled_default() {
        let global_config = GlobalConfig::default();
        assert!(global_config.options.span_extraction_sample_rate.is_none());
        let mut state = state();
        extract_from_event(&mut state, &global_config);
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
        let mut global_config = GlobalConfig::default();
        global_config.options.span_extraction_sample_rate = Some(1.0);
        let mut state = state();
        extract_from_event(&mut state, &global_config);
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
        let mut global_config = GlobalConfig::default();
        global_config.options.span_extraction_sample_rate = Some(0.0);
        let mut state = state();
        extract_from_event(&mut state, &global_config);
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

    #[test]
    fn keep_browser_name() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "browser.name": "foo"
                }
            }"#,
        )
        .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints::default(),
        );
        assert_eq!(get_value!(span.data.browser_name!), "foo");
    }

    #[test]
    fn keep_browser_name_when_ua_present() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "browser.name": "foo",
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
        .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints::default(),
        );
        assert_eq!(get_value!(span.data.browser_name!), "foo");
    }

    #[test]
    fn derive_browser_name() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
        .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints::default(),
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
        );
        assert_eq!(get_value!(span.data.browser_name!), "Chrome Mobile");
    }

    #[test]
    fn keep_user_agent_when_meta_is_present() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
        .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            Some("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ONS Internet Explorer 6.1; .NET CLR 1.1.4322)"),
            ClientHints::default(),
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
        );
        assert_eq!(get_value!(span.data.browser_name!), "Chrome Mobile");
    }

    #[test]
    fn derive_user_agent() {
        let mut span: Annotated<Span> = Annotated::from_json(r#"{}"#).unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            Some("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ONS Internet Explorer 6.1; .NET CLR 1.1.4322)"),
            ClientHints::default(),
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ONS Internet Explorer 6.1; .NET CLR 1.1.4322)"
        );
        assert_eq!(get_value!(span.data.browser_name!), "IE");
    }

    #[test]
    fn keep_user_agent_when_client_hints_are_present() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
        .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints {
                sec_ch_ua: Some(r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#),
                ..Default::default()
            },
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
        );
        assert_eq!(get_value!(span.data.browser_name!), "Chrome Mobile");
    }

    #[test]
    fn derive_client_hints() {
        let mut span: Annotated<Span> = Annotated::from_json(r#"{}"#).unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints {
                sec_ch_ua: Some(r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#),
                ..Default::default()
            },
        );
        assert_eq!(get_value!(span.data.user_agent_original), None);
        assert_eq!(get_value!(span.data.browser_name!), "Opera");
    }
}
