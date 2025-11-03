use std::sync::Once;

use relay_base_schema::project::ProjectId;
use relay_dynamic_config::{
    CombinedMetricExtractionConfig, ErrorBoundary, Feature, MetricExtractionGroups,
};
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::MetricNamespace;
use relay_protocol::Annotated;
use relay_sampling::DynamicSamplingContext;
use relay_sampling::evaluation::SamplingDecision;

use crate::metrics_extraction::transactions::TransactionExtractor;
use crate::processing::Context;
use crate::processing::utils::event::{EventMetricsExtracted, SpansExtracted};
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
    pub ctx: &'a Context<'a>,
    pub sampling_decision: SamplingDecision,
    pub event_metrics_extracted: EventMetricsExtracted,
    pub spans_extracted: SpansExtracted,
}

/// Extract transaction metrics.
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
        event_metrics_extracted,
        spans_extracted,
    } = ctx;

    if event_metrics_extracted.0 {
        return Ok(event_metrics_extracted);
    }
    let Some(event) = event.value_mut() else {
        return Ok(event_metrics_extracted);
    };

    // NOTE: This function requires a `metric_extraction` in the project config. Legacy configs
    // will upsert this configuration from transaction and conditional tagging fields, even if
    // it is not present in the actual project config payload.
    let combined_config = {
        let config = match &ctx.project_info.config.metric_extraction {
            ErrorBoundary::Ok(config) if config.is_supported() => config,
            _ => return Ok(event_metrics_extracted),
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
                    return Ok(event_metrics_extracted);
                }
            }
        };
        CombinedMetricExtractionConfig::new(global_config, config)
    };

    // Require a valid transaction metrics config.
    let tx_config = match &ctx.project_info.config.transaction_metrics {
        Some(ErrorBoundary::Ok(tx_config)) => tx_config,
        Some(ErrorBoundary::Err(e)) => {
            relay_log::debug!("Failed to parse legacy transaction metrics config: {e}");
            return Ok(event_metrics_extracted);
        }
        None => {
            relay_log::debug!("Legacy transaction metrics config is missing");
            return Ok(event_metrics_extracted);
        }
    };

    if !tx_config.is_enabled() {
        static TX_CONFIG_ERROR: Once = Once::new();
        TX_CONFIG_ERROR.call_once(|| {
                if ctx.config.processing_enabled() {
                    relay_log::error!(
                        "Processing Relay outdated, received tx config in version {}, which is not supported",
                        tx_config.version
                    );
                }
            });

        return Ok(event_metrics_extracted);
    }

    // If spans were already extracted for an event, we rely on span processing to extract metrics.
    let extract_spans = !spans_extracted.0
        && crate::utils::sample(
            ctx.global_config
                .options
                .span_extraction_sample_rate
                .unwrap_or(1.0),
        )
        .is_keep();

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
            extract_spans,
            transaction_from_dsc: dsc.and_then(|dsc| dsc.transaction.as_deref()),
        },
    );

    extracted_metrics.extend(metrics, Some(sampling_decision));

    if !ctx.project_info.has_feature(Feature::DiscardTransaction) {
        let transaction_from_dsc = dsc.and_then(|dsc| dsc.transaction.as_deref());

        let extractor = TransactionExtractor {
            config: tx_config,
            generic_config: Some(combined_config),
            transaction_from_dsc,
            sampling_decision,
            target_project_id: project_id,
        };

        extracted_metrics.extend(extractor.extract(event)?, Some(sampling_decision));
    }

    Ok(EventMetricsExtracted(true))
}
