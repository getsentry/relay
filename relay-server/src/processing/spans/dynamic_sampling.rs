use std::collections::BTreeMap;
use std::ops::ControlFlow;

use chrono::Utc;
use either::Either;
use relay_dynamic_config::ErrorBoundary;
use relay_metrics::{Bucket, BucketMetadata, BucketValue, UnixTimestamp};
use relay_protocol::get_value;
use relay_quotas::{DataCategory, Scoping};
use relay_sampling::config::RuleType;
use relay_sampling::evaluation::{SamplingDecision, SamplingEvaluator};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};

use crate::envelope::{ClientName, Item};
use crate::managed::{Counted, Managed, Quantities};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::processing::Context;
use crate::processing::spans::{
    Error, ExpandedSpans, Indexed, Result, SampledSpans, SerializedSpans, outcome_count,
};
use crate::services::outcome::Outcome;
use crate::services::projects::project::ProjectInfo;
use crate::statsd::RelayCounters;
use crate::utils::SamplingResult;

/// Validates all sampling configurations.
///
/// The function validates the sampling config of the project and sampling project (from the trace
/// root) are valid and supported.
/// This check is not required to run, but makes sure eventual mis-configurations and errors in the
/// project config are caught and not silently ignored.
pub fn validate_configs(ctx: Context<'_>) {
    // It is possible there is a new version of the dynamic sampling configuration rolled out
    // and only the innermost, processing, Relay supports it, in which case dynamic sampling
    // will be delayed to the processing Relay.
    //
    // If the processing Relay does not support it, there is a real problem.
    if !ctx.is_processing() {
        return;
    }

    if !is_sampling_config_supported(ctx.project_info)
        || !ctx
            .sampling_project_info
            .is_none_or(is_sampling_config_supported)
    {
        relay_log::error!(
            project_id = ?ctx.project_info.project_id,
            sampling_project_id = ?ctx.sampling_project_info.and_then(|c| c.project_id.as_ref()),
            "found unsupported dynamic sampling rules in a processing relay"
        );
    }
}

/// Validates the presence of a dynamic sampling context when processing Spans.
///
/// Each envelope received by Relay must contain a valid dynamic sampling context.
/// This is not a technical requirement as a missing dynamic sampling context is treated as having
/// a sample rate of 100%, but to ensure SDKs implement the protocol correctly it is validated.
///
/// An exception exists for our OTeL integration, which currently never sets a dynamic sampling
/// context. In the future we may want to extract a DSC from the OTeL payload to allow dynamic
/// sampling if the necessary attributes are present.
pub fn validate_dsc_presence(spans: &SerializedSpans) -> Result<()> {
    let dsc = spans.headers.dsc();

    // Envelopes created by Relay may not carry a DSC. This is currently the case for envelopes
    // created through the OTeL integration.
    //
    // This validation is only best effort (not a hard requirement) to get SDKs to implement DSC
    // correctly, so it is okay to be a bit more lenient here and trust ourselves.
    let client_is_relay = spans.headers.meta().client_name() == ClientName::Relay;

    if !client_is_relay && dsc.is_none() {
        return Err(Error::MissingDynamicSamplingContext);
    }

    Ok(())
}

/// Validates the dynamic sampling context against the parsed spans.
///
/// The values of the dynamic sampling context must match the values provided in the spans.
/// Currently this only validates the trace id.
pub fn validate_dsc(spans: &ExpandedSpans) -> Result<()> {
    let Some(dsc) = spans.headers.dsc() else {
        // It's okay if we don't have a DSC here. We may be in a processing path which has spans
        // from mixed traces without a DSC (=100% sample rate).
        // For example via the OTeL integration.
        return Ok(());
    };

    for span in &spans.spans {
        let span = &span.span;
        let trace_id = get_value!(span.trace_id);

        if trace_id != Some(&dsc.trace_id) {
            return Err(Error::DynamicSamplingContextMismatch);
        }
    }

    Ok(())
}

/// Computes the sampling decision for a batch of spans.
///
/// All spans are evaluated in one go as they are required by the protocol to share the same
/// DSC, which contains all the sampling relevant information.
pub async fn run(
    spans: Managed<SerializedSpans>,
    ctx: Context<'_>,
) -> Result<Managed<SampledSpans>, Managed<ExtractedMetrics>> {
    let sampling_result = compute(&spans, ctx).await;

    relay_statsd::metric!(
        counter(RelayCounters::SamplingDecision) += 1,
        decision = sampling_result.decision().as_str(),
        item = "span"
    );

    let sampling_match = match sampling_result {
        SamplingResult::Match(m) if m.decision().is_drop() => m,
        sampling_result => {
            let sample_rate = sampling_result.sample_rate();
            return Ok(spans.map(|spans, _| spans.sampled(sample_rate)));
        }
    };

    // At this point the decision is to drop the spans.
    let span_count = outcome_count(&spans.spans);
    let metrics = create_metrics(
        spans.scoping(),
        span_count,
        spans.headers.dsc(),
        SamplingDecision::Drop,
    );
    let (spans, metrics) = spans.split_once(|spans| (UnsampledSpans::from(spans), metrics));

    let outcome = Outcome::FilteredSampling(sampling_match.into_matched_rules().into());
    let _ = spans.reject_err(outcome);

    Err(metrics)
}

/// Type returned by [`create_indexed_metrics`].
///
/// Contains the indexed spans and the metrics extracted from the spans.
type SpansAndMetrics = (Managed<ExpandedSpans<Indexed>>, Managed<ExtractedMetrics>);

/// Creates/extracts metrics for spans which have been determined to be kept by dynamic sampling.
///
/// Indexed metrics can only be extracted from the Relay making the final sampling decision,
/// if the current Relay is not the final Relay, the function returns the original spans unchanged.
pub fn create_indexed_metrics(
    spans: Managed<ExpandedSpans>,
    ctx: Context<'_>,
) -> Either<Managed<ExpandedSpans>, SpansAndMetrics> {
    if !ctx.is_processing() {
        return Either::Left(spans);
    }

    let scoping = spans.scoping();
    let (indexed, metrics) = spans.split_once(|spans| {
        let metrics = create_metrics(
            scoping,
            spans.spans.len() as u32,
            spans.headers.dsc(),
            SamplingDecision::Keep,
        );

        (spans.into_indexed(), metrics)
    });

    Either::Right((indexed, metrics))
}

async fn compute(spans: &Managed<SerializedSpans>, ctx: Context<'_>) -> SamplingResult {
    // The DSC is always required, we need it to evaluate all rules, if it is missing,
    // no rules can be applied -> we sample the item.
    let Some(dsc) = spans.headers.dsc() else {
        return SamplingResult::NoMatch;
    };

    let project_sampling_config = get_sampling_config(ctx.project_info);
    let root_sampling_config = ctx
        .sampling_project_info
        // Fallback to current project if there is no trace root project, this may happen,
        // if the trace root is from a different organization.
        .or(Some(ctx.project_info))
        .and_then(get_sampling_config);

    // The root sampling config is always required for dynamic sampling. It determines the sample
    // rate which is applied to the item.
    let Some(root_sampling_config) = root_sampling_config else {
        return SamplingResult::NoMatch;
    };

    // Currently there is no support planned for reservoir sampling.
    let mut evaluator = SamplingEvaluator::new(Utc::now());

    // Apply project rules before trace rules, to give projects a chance to override the trace root
    // sample rate.
    if let Some(sampling_config) = project_sampling_config {
        let rules = sampling_config.filter_rules(RuleType::Project);

        // We need a segment consistent seed to make sure all spans of the same segment in this
        // project get a proper minimum sample rate applied.
        //
        // The trace id gives us this property and it will also have the upside of consistently
        // sampling multiple segments of the same trace.
        evaluator = match evaluator.match_rules(*dsc.trace_id, dsc, rules).await {
            ControlFlow::Continue(evaluator) => evaluator,
            ControlFlow::Break(sampling_match) => return SamplingResult::Match(sampling_match),
        }
    }

    let rules = root_sampling_config.filter_rules(RuleType::Trace);
    evaluator
        .match_rules(*dsc.trace_id, dsc, rules)
        .await
        .into()
}

fn get_sampling_config(info: &ProjectInfo) -> Option<&SamplingConfig> {
    let config = info.config.sampling.as_ref()?.as_ref().ok()?;
    (!config.unsupported()).then_some(config)
}

fn is_sampling_config_supported(project_info: &ProjectInfo) -> bool {
    let Some(config) = &project_info.config.sampling else {
        return true;
    };
    matches!(config, ErrorBoundary::Ok(config) if !config.unsupported())
}

fn create_metrics(
    scoping: Scoping,
    span_count: u32,
    dsc: Option<&DynamicSamplingContext>,
    sampling_decision: SamplingDecision,
) -> ExtractedMetrics {
    let mut metrics = ExtractedMetrics::default();

    if span_count == 0 {
        return metrics;
    }

    // For extracted metrics, Relay has always used the moment when the metrics were extracted
    // as the received time, instead of the source item's received time.
    let timestamp = UnixTimestamp::now();
    let mut metadata = BucketMetadata::new(timestamp);
    if sampling_decision.is_keep() {
        metadata.extracted_from_indexed = true;
    }

    metrics.sampling_metrics.push(Bucket {
        timestamp,
        width: 0,
        name: "c:spans/count_per_root_project@none".into(),
        value: BucketValue::counter(span_count.into()),
        tags: {
            let mut tags = BTreeMap::new();
            tags.insert("decision".to_owned(), sampling_decision.to_string());
            tags.insert(
                "target_project_id".to_owned(),
                scoping.project_id.to_string(),
            );
            if let Some(tx) = dsc.and_then(|dsc| dsc.transaction.clone()) {
                tags.insert("transaction".to_owned(), tx);
            }
            tags
        },
        metadata,
    });
    metrics.project_metrics.push(Bucket {
        timestamp,
        width: 0,
        name: "c:spans/usage@none".into(),
        value: BucketValue::counter(span_count.into()),
        tags: Default::default(),
        metadata,
    });

    metrics
}

/// Spans which have been rejected/dropped by dynamic sampling.
///
/// Contained spans will only count towards the [`DataCategory::SpanIndexed`] category,
/// as the total category is counted from now in in metrics.
struct UnsampledSpans {
    spans: Vec<Item>,
    span_attachments: Vec<Item>,
}

impl From<SerializedSpans> for UnsampledSpans {
    fn from(value: SerializedSpans) -> Self {
        Self {
            spans: value.spans,
            span_attachments: value.span_attachments,
        }
    }
}

impl Counted for UnsampledSpans {
    fn quantities(&self) -> Quantities {
        let quantity = outcome_count(&self.spans) as usize;
        let mut quantities = smallvec::smallvec![];

        if quantity > 0 {
            quantities.push((DataCategory::SpanIndexed, quantity));
        }
        if !self.span_attachments.is_empty() {
            quantities.push((
                DataCategory::Attachment,
                self.span_attachments
                    .iter()
                    .fold(0, |acc, cur| acc + cur.len()),
            ));
            quantities.push((DataCategory::AttachmentItem, self.span_attachments.len()));
        }

        quantities
    }
}
