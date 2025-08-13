use std::ops::ControlFlow;

use chrono::Utc;
use relay_dynamic_config::ErrorBoundary;
use relay_sampling::SamplingConfig;
use relay_sampling::config::RuleType;
use relay_sampling::evaluation::SamplingEvaluator;

use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::spans::SerializedSpans;
use crate::services::projects::project::ProjectInfo;
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

/// Computes the sampling decision for a batch of spans.
///
/// All spans are evaluated in one go as they are required by the protocol to share the same
/// DSC, which contains all the sampling relevant information.
pub async fn run(spans: &Managed<SerializedSpans>, ctx: Context<'_>) -> SamplingResult {
    // The DSC is always required, we need it to evaluate all rules, if it is missing,
    // no rules can be applied -> we sample the item.
    let Some(dsc) = spans.headers.dsc() else {
        return SamplingResult::NoMatch;
    };

    let project_sampling_config = get_sampling_config(ctx.project_info);
    let root_sampling_config = ctx.sampling_project_info.and_then(get_sampling_config);

    // The root sampling config is always required for dynamic sampling. It determines the sample
    // rate which is applied to the item.
    let Some(root_sampling_config) = root_sampling_config else {
        return SamplingResult::NoMatch;
    };

    // TODO: reservoir sampling
    let mut evaluator = SamplingEvaluator::new(Utc::now());

    // Apply transaction local rules.
    //
    // For spans we generally cannot apply transaction local rules, but some of the rules,
    // specifically the minimum sample rate rule can still be applied to individual spans.
    //
    // In the future this rule type will be replaced with an explicit project based rule type.
    if let Some(sampling_config) = project_sampling_config {
        let rules = sampling_config.filter_rules(RuleType::Transaction);

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
