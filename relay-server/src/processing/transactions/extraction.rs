use relay_base_schema::project::ProjectId;
use relay_dynamic_config::{CombinedMetricExtractionConfig, ErrorBoundary, MetricExtractionGroups};
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::MetricNamespace;
use relay_protocol::Annotated;
use relay_sampling::DynamicSamplingContext;
use relay_sampling::evaluation::SamplingDecision;

use crate::processing::Context;
use crate::processing::utils::event::EventMetricsExtracted;
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};

/// Creates a span from the transaction and applies tag extraction on it.
///
/// Returns `None` when [`tag_extraction::extract_span_tags`] clears the span, which it shouldn't.
pub fn extract_segment_span(
    event: &Event,
    max_tag_value_size: usize,
    span_allowed_hosts: &[String],
) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size, span_allowed_hosts);
    tag_extraction::extract_segment_span_tags(event, &mut spans);

    spans.into_iter().next().and_then(Annotated::into_value)
}

/// Input arguments for [`extract_metrics`].
pub struct ExtractMetricsContext<'a> {
    pub dsc: Option<&'a DynamicSamplingContext>,
    pub project_id: ProjectId,
    pub ctx: Context<'a>,
    pub sampling_decision: SamplingDecision,
    pub metrics_extracted: bool,
    pub extract_span_metrics: bool,
}

/// Extracts metrics from a transaction and its spans.
pub fn extract_metrics(
    event: &mut Annotated<Event>,
    extracted_metrics: &mut ProcessingExtractedMetrics,
    ctx: ExtractMetricsContext,
) -> Result<EventMetricsExtracted, ProcessingError> {
    let ExtractMetricsContext {
        dsc,
        project_id,
        ctx,
        sampling_decision,
        metrics_extracted,
        extract_span_metrics,
    } = ctx;
    // TODO(follow-up): this function should always extract metrics. Dynamic sampling should validate
    // the full metrics extraction config and skip sampling if it is incomplete.

    if metrics_extracted {
        return Ok(EventMetricsExtracted(true));
    }
    let Some(event) = event.value_mut() else {
        // Nothing to extract, but metrics extraction was called.
        return Ok(EventMetricsExtracted(true));
    };

    // NOTE: This function requires a `metric_extraction` in the project config. Legacy configs
    // will upsert this configuration from transaction and conditional tagging fields, even if
    // it is not present in the actual project config payload.
    let combined_config = {
        let config = match &ctx.project_info.config.metric_extraction {
            ErrorBoundary::Ok(config) if config.is_supported() => config,
            _ => return Ok(EventMetricsExtracted(false)),
        };
        let global_config = match &ctx.global_config.metric_extraction {
            ErrorBoundary::Ok(global_config) => global_config,
            #[allow(unused_variables)]
            ErrorBoundary::Err(e) => {
                if cfg!(feature = "processing") && ctx.config.processing_enabled() {
                    // Config is invalid, but we will try to extract what we can with just the
                    // project config.
                    relay_log::error!("Failed to parse global extraction config {e}");
                    MetricExtractionGroups::EMPTY
                } else {
                    // If there's an error with global metrics extraction, it is safe to assume that this
                    // Relay instance is not up-to-date, and we should skip extraction.
                    relay_log::debug!("Failed to parse global extraction config: {e}");
                    return Ok(EventMetricsExtracted(false));
                }
            }
        };
        CombinedMetricExtractionConfig::new(global_config, config)
    };

    let metrics = crate::metrics_extraction::event::extract_metrics(
        event,
        crate::metrics_extraction::event::ExtractMetricsConfig {
            config: combined_config,
            sampling_decision,
            target_project_id: project_id,
            max_tag_value_size: ctx
                .config
                .aggregator_config_for(MetricNamespace::Spans)
                .max_tag_value_length,
            extract_spans: extract_span_metrics,
            transaction_from_dsc: dsc.and_then(|dsc| dsc.transaction.as_deref()),
        },
    );
    extracted_metrics.extend(metrics, Some(sampling_decision));

    Ok(EventMetricsExtracted(true))
}
