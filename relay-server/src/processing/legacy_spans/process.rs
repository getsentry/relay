use relay_event_normalization::{CombinedMeasurementsConfig, GeoIpLookup, MeasurementsConfig};
use relay_event_schema::processor::{ProcessingAction, ProcessingState, process_value};
use relay_event_schema::protocol::Span;
use relay_metrics::MetricNamespace;
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;

use crate::managed::Managed;
use crate::processing::legacy_spans::{
    Error, ExpandedLegacySpans, Indexed, SerializedLegacySpans, TotalAndIndexed,
};
use crate::processing::{self, Context};
use crate::services::outcome::DiscardReason;
use crate::services::processor::span::NormalizeSpanConfig;
use crate::services::processor::{ProcessingError, span};
use relay_event_normalization::RemoveOtherProcessor;

pub fn expand(spans: Managed<SerializedLegacySpans>) -> Managed<ExpandedLegacySpans> {
    spans.map(|spans, records| {
        let SerializedLegacySpans { headers, spans } = spans;

        let spans = spans
            .into_iter()
            .filter_map(|item| {
                let Ok(span) = Annotated::<Span>::from_json_bytes(&item.payload()) else {
                    records.reject_err(Error::Invalid(DiscardReason::InvalidJson), item);
                    return None;
                };
                Some(span)
            })
            .collect();

        ExpandedLegacySpans {
            headers,
            spans,
            category: TotalAndIndexed,
        }
    })
}

pub fn normalize(
    spans: &mut Managed<ExpandedLegacySpans>,
    ctx: Context<'_>,
    geo_lookup: &GeoIpLookup,
) {
    let aggregator_config = ctx.config.aggregator_config_for(MetricNamespace::Spans);
    let model_data = ctx.global_config.ai_model_metadata();
    let norm = NormalizeSpanConfig {
        received_at: spans.received_at(),
        timestamp_range: aggregator_config.timestamp_range(),
        max_tag_value_size: aggregator_config.max_tag_value_length,
        performance_score: ctx.project_info.config.performance_score.as_ref(),
        measurements: Some(CombinedMeasurementsConfig::new(
            ctx.project_info.config.measurements.as_ref(),
            ctx.global_config.measurements.as_ref(),
        )),
        ai_model_metadata: model_data,
        max_name_and_unit_len: aggregator_config
            .max_name_length
            .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),

        tx_name_rules: &ctx.project_info.config.tx_name_rules,
        user_agent: spans.headers.meta().user_agent().map(Into::into),
        client_hints: spans.headers.meta().client_hints().to_owned(),
        allowed_hosts: ctx.global_config.options.http_span_allowed_hosts.as_slice(),
        client_ip: spans.headers.meta().client_addr().map(Into::into),
        geo_lookup,
        span_op_defaults: ctx.global_config.span_op_defaults.borrow(),
    };

    spans.retain(
        |spans| &mut spans.spans,
        |span, _| {
            span::normalize(span, norm.clone()).map_err(|err| {
                relay_log::debug!("failed to normalize span: {err}");
                Error::Invalid(match err {
                    ProcessingError::ProcessingFailed(ProcessingAction::InvalidTransaction(_))
                    | ProcessingError::InvalidTransaction => DiscardReason::InvalidSpan,
                    _ => DiscardReason::Internal,
                })
            })?;

            // Remove additional fields.
            let _ = process_value(span, &mut RemoveOtherProcessor, ProcessingState::root());

            processing::transactions::spans::validate(span).map_err(|err| {
                relay_log::debug!(
                    error = &err as &dyn std::error::Error,
                    source = "standalone",
                    "invalid span"
                );
                Error::Invalid(DiscardReason::InvalidSpan)
            })?;

            if span.value().is_none() {
                return Err(Error::Invalid(DiscardReason::Internal));
            }

            Ok(())
        },
    );
}

pub fn filter(spans: &mut Managed<ExpandedLegacySpans>, ctx: Context<'_>) {
    let client_ip = spans.headers.meta().client_addr();
    let filter_settings = &ctx.project_info.config.filter_settings;

    spans.retain(
        |spans| &mut spans.spans,
        |span, _| {
            let Some(span) = span.value() else {
                return Ok(());
            };

            relay_filter::should_filter(
                span,
                client_ip,
                filter_settings,
                ctx.global_config.filters(),
            )
            .map_err(|filter| {
                relay_log::trace!(
                    "filtering span {:?} that matched an inbound filter",
                    span.span_id
                );
                Error::Filtered(filter)
            })
        },
    );
}

pub fn scrub(spans: &mut Managed<ExpandedLegacySpans<Indexed>>, ctx: Context<'_>) {
    spans.modify(|spans, _| {
        for span in &mut spans.spans {
            if let Some(ref config) = ctx.project_info.config.pii_config {
                let mut processor = PiiProcessor::new(config.compiled());
                let _ = process_value(span, &mut processor, ProcessingState::root());
            }
            let pii_config = ctx.project_info.config.datascrubbing_settings.pii_config();
            if let Some(config) = pii_config {
                let mut processor = PiiProcessor::new(config.compiled());
                let _ = process_value(span, &mut processor, ProcessingState::root());
            }
        }
    });
}
