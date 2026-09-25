use relay_dynamic_config::CombinedMetricExtractionConfig;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::MetricNamespace;
use relay_protocol::Annotated;
use relay_sampling::evaluation::SamplingDecision;

use crate::processing::Context;
use crate::services::processor::ProcessingExtractedMetrics;

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
    pub config: CombinedMetricExtractionConfig<'a>,
    pub ctx: Context<'a>,
    pub sampling_decision: SamplingDecision,
    pub extract_span_metrics: bool,
}

/// Extracts metrics from a transaction and its spans.
pub fn extract_metrics(
    event: &mut Annotated<Event>,
    extracted_metrics: &mut ProcessingExtractedMetrics,
    ctx: ExtractMetricsContext,
) {
    let ExtractMetricsContext {
        config,
        ctx,
        sampling_decision,
        extract_span_metrics,
    } = ctx;
    let Some(event) = event.value_mut() else {
        // Nothing to extract.
        return;
    };

    let metrics = crate::metrics_extraction::event::extract_metrics(
        event,
        crate::metrics_extraction::event::ExtractMetricsConfig {
            config,
            max_tag_value_size: ctx
                .config
                .aggregator_config_for(MetricNamespace::Spans)
                .max_tag_value_length,
            extract_spans: extract_span_metrics,
        },
    );
    extracted_metrics.extend(metrics, Some(sampling_decision));
}
