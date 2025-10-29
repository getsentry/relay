use relay_base_schema::{events::EventType, project::ProjectKey};
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use relay_sampling::{DynamicSamplingContext, dsc::TraceUserContext};

use sentry::protocol::TraceContext;

use crate::envelope::EnvelopeHeaders;
use crate::processing::Context;

/// Ensures there is a valid dynamic sampling context and corresponding project state.
///
/// The dynamic sampling context (DSC) specifies the project_key of the project that initiated
/// the trace. That project state should have been loaded previously by the project cache and is
/// available on the `ProcessEnvelopeState`. Under these conditions, this cannot happen:
///
///  - There is no DSC in the envelope headers. This occurs with older or third-party SDKs.
///  - The project key does not exist. This can happen if the project key was disabled, the
///    project removed, or in rare cases when a project from another Sentry instance is referred
///    to.
///  - The project key refers to a project from another organization. In this case the project
///    cache does not resolve the state and instead leaves it blank.
///  - The project state could not be fetched. This is a runtime error, but in this case Relay
///    should fall back to the next-best sampling rule set.
///
/// In all of the above cases, this function will compute a new DSC using information from the
/// event payload, similar to how SDKs do this. The `sampling_project_state` is also switched to
/// the main project state.
///
/// If there is no transaction event in the envelope, this function will do nothing.
///
/// The function will return the sampling project information of the root project for the event. If
/// no sampling project information is specified, the project information of the event’s project
/// will be returned.
pub fn validate_and_set_dsc<'a, T>(
    headers: &mut EnvelopeHeaders,
    event: &Annotated<Event>,
    ctx: &mut Context,
) {
    let original_dsc = headers.dsc();
    if original_dsc.is_some() && ctx.sampling_project_info.is_some() {
        return;
    }

    // The DSC can only be computed if there's a transaction event. Note that `dsc_from_event`
    // below already checks for the event type.
    if let Some(event) = event.value()
        && let Some(key_config) = ctx.project_info.get_public_key_config()
        && let Some(mut dsc) = dsc_from_event(key_config.public_key, event)
    {
        // All other information in the DSC must be discarded, but the sample rate was
        // actually applied by the client and is therefore correct.
        let original_sample_rate = original_dsc.and_then(|dsc| dsc.sample_rate);
        dsc.sample_rate = dsc.sample_rate.or(original_sample_rate);

        headers.set_dsc(dsc);

        ctx.sampling_project_info = Some(ctx.project_info);
        return;
    }

    // If we cannot compute a new DSC but the old one is incorrect, we need to remove it.
    headers.remove_dsc();
}

/// Computes a dynamic sampling context from a transaction event.
///
/// Returns `None` if the passed event is not a transaction event, or if it does not contain a
/// trace ID in its trace context. All optional fields in the dynamic sampling context are
/// populated with the corresponding attributes from the event payload if they are available.
///
/// Since sampling information is not available in the event payload, the `sample_rate` field
/// cannot be set when computing the dynamic sampling context from a transaction event.
fn dsc_from_event(public_key: ProjectKey, event: &Event) -> Option<DynamicSamplingContext> {
    if event.ty.value() != Some(&EventType::Transaction) {
        return None;
    }

    let trace = event.context::<TraceContext>()?;
    let trace_id = *trace.trace_id.value()?;
    let user = event.user.value();

    Some(DynamicSamplingContext {
        trace_id,
        public_key,
        release: event.release.as_str().map(str::to_owned),
        environment: event.environment.value().cloned(),
        transaction: event.transaction.value().cloned(),
        replay_id: None,
        sample_rate: None,
        user: TraceUserContext {
            user_segment: user
                .and_then(|u| u.segment.value().cloned())
                .unwrap_or_default(),
            user_id: user
                .and_then(|u| u.id.as_str())
                .unwrap_or_default()
                .to_owned(),
        },
        sampled: None,
        other: Default::default(),
    })
}
