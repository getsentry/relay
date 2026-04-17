use relay_dynamic_config::{CombinedMetricExtractionConfig, ErrorBoundary};
use relay_event_schema::protocol::Span;
use relay_metrics::Bucket;
use relay_quotas::DataCategory;
use relay_sampling::evaluation::SamplingDecision;

use crate::managed::Managed;
use crate::metrics_extraction::{ExtractedMetrics, event, generic};
use crate::processing::legacy_spans::{Error, ExpandedLegacySpans, Indexed};
use crate::processing::{self, Context};
use crate::services::processor::ProcessingExtractedMetrics;
use crate::statsd::RelayCounters;
use crate::utils::SamplingResult;

pub async fn run(
    spans: Managed<ExpandedLegacySpans>,
    ctx: Context<'_>,
) -> (
    Option<Managed<ExpandedLegacySpans<Indexed>>>,
    Managed<ExtractedMetrics>,
) {
    // If no metrics could be extracted, do not sample anything.
    let should_sample = matches!(&ctx.project_info.config().metric_extraction, ErrorBoundary::Ok(c) if c.is_supported());
    let sampling_result = match should_sample {
        true => {
            // We only implement trace-based sampling rules for now, which can be computed
            // once for all spans in the envelope.
            processing::utils::dynamic_sampling::run(spans.headers.dsc(), None, &ctx, None).await
        }
        false => SamplingResult::Pending,
    };

    relay_statsd::metric!(
        counter(RelayCounters::SamplingDecision) += 1,
        decision = sampling_result.decision().as_str(),
        item = "span"
    );

    let sampling_decision = sampling_result.decision();

    let (indexed, metrics) = split_indexed_and_total(spans, sampling_decision, ctx);

    match sampling_result.into_dropped_outcome() {
        Some(outcome) => {
            let _ = indexed.reject_err(outcome);
            (None, metrics)
        }
        None => (Some(indexed), metrics),
    }
}

/// Rejects the indexed portion of the provided sampled spans and returns the total count as metrics.
///
/// This is used when the indexed payload is designated to be dropped *after* dynamic sampling (decision is keep),
/// but metrics have not yet been extracted.
pub fn reject_indexed_spans(
    spans: Managed<ExpandedLegacySpans>,
    ctx: Context<'_>,
    error: Error,
) -> Managed<ExtractedMetrics> {
    let (indexed, total) = split_indexed_and_total(spans, SamplingDecision::Keep, ctx);
    let _ = indexed.reject_err(error);
    total
}

fn split_indexed_and_total(
    spans: Managed<ExpandedLegacySpans>,
    sampling_decision: SamplingDecision,
    ctx: Context<'_>,
) -> (
    Managed<ExpandedLegacySpans<Indexed>>,
    Managed<ExtractedMetrics>,
) {
    let scoping = spans.scoping();
    let transaction_from_dsc = spans.headers.dsc().and_then(|dsc| dsc.transaction.clone());

    spans.split_once(|spans, r| {
        r.lenient(DataCategory::MetricBucket);

        let mut metrics = ProcessingExtractedMetrics::new();
        for span in spans.spans.iter().filter_map(|s| s.value()) {
            if let Some(buckets) = extract_generic_metrics(span, ctx) {
                metrics.extend_project_metrics(buckets, Some(sampling_decision));
            }

            let bucket = event::create_span_root_counter(
                span,
                transaction_from_dsc.clone(),
                1,
                span.is_segment.value().is_some_and(|s| *s),
                sampling_decision,
                scoping.project_id,
            );
            metrics.extend_sampling_metrics(bucket, Some(sampling_decision));
        }

        (spans.into_indexed(), metrics.into_inner())
    })
}

fn extract_generic_metrics(span: &Span, ctx: Context<'_>) -> Option<Vec<Bucket>> {
    let config = {
        let local = match ctx.project_info.config.metric_extraction {
            ErrorBoundary::Ok(ref config) if config.is_enabled() => config,
            _ => return None,
        };
        let global = ctx.global_config.metric_extraction.as_ref().ok()?;
        CombinedMetricExtractionConfig::new(global, local)
    };

    Some(generic::extract_metrics(span, config))
}
