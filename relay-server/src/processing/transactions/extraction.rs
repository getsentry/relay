#![expect(unused)]
use std::sync::Once;

use relay_base_schema::project::ProjectId;
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
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

    if metrics_extracted {
        return Ok(EventMetricsExtracted(true));
    }
    let Some(event) = event.value_mut() else {
        // Nothing to extract, but metrics extraction was called.
        return Ok(EventMetricsExtracted(true));
    };

    // Require a valid transaction metrics config.
    let tx_config = match &ctx.project_info.config.transaction_metrics {
        Some(ErrorBoundary::Ok(tx_config)) => tx_config,
        Some(ErrorBoundary::Err(e)) => {
            relay_log::debug!("Failed to parse legacy transaction metrics config: {e}");
            return Ok(EventMetricsExtracted(false));
        }
        None => {
            relay_log::debug!("Legacy transaction metrics config is missing");
            return Ok(EventMetricsExtracted(false));
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

        return Ok(EventMetricsExtracted(false));
    }

    // Extract span metrics (usage and count_per_root_project).
    let span_metrics = crate::metrics_extraction::event::extract_metrics(
        event,
        crate::metrics_extraction::event::ExtractMetricsConfig {
            sampling_decision,
            target_project_id: project_id,
            extract_spans: extract_span_metrics,
            transaction_from_dsc: dsc.and_then(|dsc| dsc.transaction.as_deref()),
        },
    );

    extracted_metrics.extend(span_metrics, Some(sampling_decision));

    if !ctx.project_info.has_feature(Feature::DiscardTransaction) {
        let transaction_from_dsc = dsc.and_then(|dsc| dsc.transaction.as_deref());

        let extractor = TransactionExtractor {
            transaction_from_dsc,
            sampling_decision,
            target_project_id: project_id,
        };

        extracted_metrics.extend(extractor.extract(event)?, Some(sampling_decision));
    }

    Ok(EventMetricsExtracted(true))
}
