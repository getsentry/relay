use chrono::Utc;
use relay_event_schema::protocol::{Contexts, TraceContext};
use relay_protocol::{Annotated, Empty as _};
use relay_sampling::config::RuleType;
use relay_sampling::evaluation::SamplingEvaluator;

use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::errors::ExpandedError;
use crate::utils::SamplingResult;

/// Applies a dynamic sampling decision onto the error.
///
/// The function validates the DSC as well as a tagging the error event with the sampling decision
/// of the associated trace.
pub async fn apply(error: &mut Managed<ExpandedError>, ctx: Context<'_>) {
    // Only run in processing to not compute the decision multiple times and it is the most
    // accurate place, as other Relays may have unsupported inbound filter or sampling configs.
    if !ctx.is_processing() {
        return;
    }

    if ctx.sampling_project_info.is_none() {
        // If there is a DSC, the current project does not have access to the sampling project
        // -> remove the DSC.
        error.modify(|error, _| error.headers.remove_dsc());
        return;
    }

    if let Some(sampled) = is_trace_fully_sampled(error, ctx).await {
        error.modify(|error, _| tag_error_with_sampling_decision(error, sampled));
    };
}

fn tag_error_with_sampling_decision(error: &mut ExpandedError, sampled: bool) {
    let Some(event) = error.event.value_mut() else {
        return;
    };

    // We want to get the trace context, in which we will inject the `sampled` field.
    let context = event
        .contexts
        .get_or_insert_with(Contexts::new)
        .get_or_default::<TraceContext>();

    // We want to update `sampled` only if it was not set, since if we don't check this
    // we will end up overriding the value set by downstream Relays and this will lead
    // to more complex debugging in case of problems.
    if context.sampled.is_empty() {
        relay_log::trace!("tagged error with `sampled = {}` flag", sampled);
        context.sampled = Annotated::new(sampled);
    }
}

/// Runs dynamic sampling if the dsc and root project state are not None and returns whether the
/// transactions received with such dsc and project state would be kept or dropped by dynamic
/// sampling.
async fn is_trace_fully_sampled(error: &ExpandedError, ctx: Context<'_>) -> Option<bool> {
    let dsc = error.headers.dsc()?;

    let sampling_config = ctx
        .sampling_project_info
        .and_then(|s| s.config.sampling.as_ref())
        .and_then(|s| s.as_ref().ok())?;

    if sampling_config.unsupported() {
        if ctx.is_processing() {
            relay_log::error!("found unsupported rules even as processing relay");
        }

        return None;
    }

    // If the sampled field is not set, we prefer to not tag the error since we have no clue on
    // whether the head of the trace was kept or dropped on the client side.
    // In addition, if the head of the trace was dropped on the client we will immediately mark
    // the trace as not fully sampled.
    if !dsc.sampled? {
        return Some(false);
    }

    let evaluator = SamplingEvaluator::new(Utc::now());

    let rules = sampling_config.filter_rules(RuleType::Trace);

    let evaluation = evaluator.match_rules(*dsc.trace_id, dsc, rules).await;
    Some(SamplingResult::from(evaluation).decision().is_keep())
}
