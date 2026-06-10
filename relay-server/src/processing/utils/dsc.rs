use relay_base_schema::events::EventType;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_event_schema::protocol::{Event, TraceContext};
use relay_protocol::{Annotated, get_value};
use relay_sampling::{DynamicSamplingContext, dsc::TraceUserContext};

use crate::envelope::{ClientName, EnvelopeHeaders};
use crate::managed::{Managed, OutcomeError, Rejected};
use crate::processing::Context;
use crate::processing::spans::ExpandedSpans;
use crate::services::outcome::{DiscardReason, Outcome};

#[derive(Debug, thiserror::Error, Copy, Clone)]
pub enum DscError {
    #[error("the DSC is required but missing on the envelope")]
    MissingDynamicSamplingContext,
    #[error(
        "the DSC or attributes required for inferring it are missing or inconsistent across spans"
    )]
    InvalidDynamicSamplingContext,
}

impl DscError {
    pub fn to_outcome(self) -> Outcome {
        match &self {
            Self::MissingDynamicSamplingContext => {
                Outcome::Invalid(DiscardReason::MissingDynamicSamplingContext)
            }
            Self::InvalidDynamicSamplingContext => {
                Outcome::Invalid(DiscardReason::InvalidDynamicSamplingContext)
            }
        }
    }
}

impl OutcomeError for DscError {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = self.to_outcome();
        (Some(outcome), self)
    }
}

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
/// event payload, similar to how SDKs do this.
///
/// If there is no transaction event in the envelope, this function will do nothing.
///
/// The function will use the sampling project information of the trace root project. If
/// the sampling project information is missing - due to the project being disabled or belonging to
/// a separate org - the event’s own project information will be used instead.
pub fn validate_and_set_dsc_for_transaction(
    headers: &mut EnvelopeHeaders,
    event: &Annotated<Event>,
    ctx: &Context,
) {
    let mut original_dsc = headers.dsc_mut();
    if let Some(dsc) = original_dsc.as_mut()
        && let Some(sp) = ctx.sampling_project_info
    {
        dsc.project_id = sp.project_id;
        return;
    }

    // The DSC can only be computed if there's a transaction event. Note that `dsc_from_event`
    // below already checks for the event type.
    if let Some(event) = event.value()
        && let Some(key_config) = ctx.project_info.get_public_key_config()
        && let Some(mut dsc) =
            dsc_from_event(key_config.public_key, ctx.project_info.project_id, event)
    {
        // All other information in the DSC must be discarded, but the sample rate was
        // actually applied by the client and is therefore correct.
        dsc.sample_rate = original_dsc.as_ref().and_then(|dsc| dsc.sample_rate);

        headers.set_dsc(dsc);
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
pub fn dsc_from_event(
    public_key: ProjectKey,
    project_id: Option<ProjectId>,
    event: &Event,
) -> Option<DynamicSamplingContext> {
    if event.ty.value() != Some(&EventType::Transaction) {
        return None;
    }

    let trace = event.context::<TraceContext>()?;
    let trace_id = *trace.trace_id.value()?;
    let user = event.user.value();

    Some(DynamicSamplingContext {
        trace_id,
        public_key,
        project_id,
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

/// Validates and, if necessary, re-synthesizes the DSC for a batch of V2 spans.
///
/// Like [`validate_and_set_dsc_for_transaction`], an unresolved sampling project causes the
/// DSC to be rebuilt and the trace attributed to the spans' own project. However, this function
/// is stricter as it doesn't allow a missing DSC (except for OTel spans, recognized by the
/// client name being `Relay`) nor missing attributes required for DSC resynthesis (when resynthesis
/// is needed).
pub fn validate_and_set_dsc_for_v2_spans(
    spans: &mut Managed<ExpandedSpans>,
    ctx: &Context<'_>,
) -> Result<(), Rejected<DscError>> {
    spans.try_modify(|spans, _| {
        // Validate that DSC exists. If the client is `Relay`, the spans are assumed to be from an
        // OTel SDK in which case a missing DSC is allowed.
        let is_relay = spans.headers.meta().client_name() == ClientName::Relay;
        let dsc = match spans.headers.dsc_mut() {
            None if is_relay => return Ok(()),
            None => return Err(DscError::MissingDynamicSamplingContext),
            Some(dsc) => dsc,
        };

        // Validate that all spans share the same trace id
        for span in &spans.spans {
            let span = &span.span;
            if get_value!(span.trace_id) != Some(&dsc.trace_id) {
                return Err(DscError::InvalidDynamicSamplingContext);
            }
        }

        match ctx.sampling_project_info {
            // Resynthesize the DSC if the trace root (sampling) project could not be resolved. This
            // happens when the sampling project is disabled or belongs to a different org than the
            // spans.
            None => {
                let public_key = ctx
                    .project_info
                    .get_public_key_config()
                    .ok_or(DscError::InvalidDynamicSamplingContext)?
                    .public_key;
                *dsc = DynamicSamplingContext {
                    trace_id: dsc.trace_id,
                    public_key,
                    project_id: ctx.project_info.project_id,
                    release: None,
                    environment: None,
                    transaction: None,
                    sample_rate: dsc.sample_rate,
                    sampled: None,
                    user: TraceUserContext::default(),
                    replay_id: None,
                    other: Default::default(),
                };
            }
            Some(sampling_project_info) => {
                dsc.project_id = sampling_project_info.project_id;
            }
        };
        Ok(())
    })
}
