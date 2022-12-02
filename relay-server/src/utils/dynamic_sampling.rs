//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use relay_common::{EventType, ProjectKey, Uuid};
use relay_general::protocol::{Context, Event, TraceContext};
use relay_sampling::{
    pseudo_random_from_uuid, DynamicSamplingContext, RuleId, SamplingConfig, SamplingMode,
    TraceUserContext,
};

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

macro_rules! or_ok_none {
    ($e:expr) => {
        match $e {
            Some(x) => x,
            None => return Ok(None),
        }
    };
}

/// The result of a sampling operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event. Relay either applied a sampling rule or was unable to parse all rules (so
    /// it bailed out)
    Keep,
    /// Drop the event, due to the rule with provided identifier.
    Drop(RuleId),
}

fn check_unsupported_rules(
    processing_enabled: bool,
    sampling_config: &SamplingConfig,
) -> Result<(), SamplingResult> {
    // when we have unsupported rules disable sampling for non processing relays
    if sampling_config.has_unsupported_rules() {
        if !processing_enabled {
            return Err(SamplingResult::Keep);
        } else {
            relay_log::error!("found unsupported rules even as processing relay");
        }
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct SamplingSpec {
    sample_rate: f64,
    rule_id: RuleId,
    seed: Uuid,
}

fn get_trace_sampling_rule(
    processing_enabled: bool,
    sampling_project_state: Option<&ProjectState>,
    sampling_context: Option<&DynamicSamplingContext>,
    ip_addr: Option<IpAddr>,
) -> Result<Option<SamplingSpec>, SamplingResult> {
    let sampling_context = or_ok_none!(sampling_context);

    if sampling_project_state.is_none() {
        relay_log::trace!("found sampling context, but no corresponding project state");
    }
    let sampling_project_state = or_ok_none!(sampling_project_state);
    let sampling_config = or_ok_none!(&sampling_project_state.config.dynamic_sampling);
    check_unsupported_rules(processing_enabled, sampling_config)?;

    let rule = or_ok_none!(sampling_config.get_matching_trace_rule(sampling_context, ip_addr));
    let sample_rate = match sampling_config.mode {
        SamplingMode::Received => rule.sample_rate,
        SamplingMode::Total => sampling_context.adjusted_sample_rate(rule.sample_rate),
        SamplingMode::Unsupported => {
            if processing_enabled {
                relay_log::error!("Found unsupported sampling mode even as processing Relay, keep");
            }
            return Err(SamplingResult::Keep);
        }
    };

    Ok(Some(SamplingSpec {
        sample_rate,
        rule_id: rule.id,
        seed: sampling_context.trace_id,
    }))
}

fn get_event_sampling_rule(
    processing_enabled: bool,
    project_state: &ProjectState,
    sampling_context: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
) -> Result<Option<SamplingSpec>, SamplingResult> {
    let event = or_ok_none!(event);
    let event_id = or_ok_none!(event.id.value());

    let sampling_config = or_ok_none!(&project_state.config.dynamic_sampling);
    check_unsupported_rules(processing_enabled, sampling_config)?;

    let rule = or_ok_none!(sampling_config.get_matching_event_rule(event, ip_addr));
    let sample_rate = match (sampling_context, sampling_config.mode) {
        (Some(ctx), SamplingMode::Total) => ctx.adjusted_sample_rate(rule.sample_rate),
        _ => rule.sample_rate,
    };

    Ok(Some(SamplingSpec {
        sample_rate,
        rule_id: rule.id,
        seed: event_id.0,
    }))
}

/// TODO(ja): Doc
fn dsc_from_event(public_key: ProjectKey, event: &Event) -> Option<DynamicSamplingContext> {
    if event.ty.value() != Some(&EventType::Transaction) {
        return None;
    }

    let contexts = event.contexts.value()?;
    let context = contexts.get(TraceContext::default_key())?.value()?;
    let Context::Trace(ref trace) = context.0 else { return None };
    let trace_id = trace.trace_id.value()?;
    let trace_id = trace_id.0.parse().ok()?;

    let user = event.user.value();

    Some(DynamicSamplingContext {
        trace_id,
        public_key,
        release: event.release.as_str().map(str::to_owned),
        environment: event.environment.value().cloned(),
        transaction: event.transaction.value().cloned(),
        sample_rate: None,
        user: TraceUserContext {
            // TODO(ja): Should these be options?
            user_segment: user
                .and_then(|u| u.segment.value().cloned())
                .unwrap_or_default(),
            user_id: user
                .and_then(|u| u.id.as_str())
                .unwrap_or_default()
                .to_owned(),
        },
        other: Default::default(),
    })
}

/// Checks whether an event should be kept or removed by dynamic sampling.
///
/// This runs both trace- and event/transaction/error-based rules at once.
///
/// TODO(ja): Doc preconditions
pub fn should_keep_event(
    sampling_context: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    project_state: &ProjectState,
    sampling_project_state: Option<&ProjectState>,
    processing_enabled: bool,
) -> SamplingResult {
    // TODO(ja): Trace sampling seems to rely on the following causality, which should be made more
    // explicit and more local + tested:
    //  - get_sampling_key returns `None` without transaction
    //  - sampling_project_state therefore resolves to `None`
    //  - get_trace_sampling_rule bails

    // XXX: Rebind so we can assign local lifetimes
    let mut sampling_context = sampling_context;
    let mut sampling_project_state = sampling_project_state;

    // If the DSC is missing, try to create a partial context ad-hoc from the transaction event.
    // This should allow sampling using the target project's sampling rules, assuming that no trace
    // propagation has happened.
    //
    // TODO(ja): Find a better place for this normalization than sampling code
    let fallback_context;
    if sampling_context.is_none() {
        debug_assert!(sampling_project_state.is_none());

        if let Some(key_config) = project_state.get_public_key_config() {
            if let Some(event) = event {
                if let Some(dsc) = dsc_from_event(key_config.public_key, event) {
                    // TODO(ja): Check if this is safe under SamplingMode::Total since the sample
                    // rate isn't populated
                    fallback_context = dsc;
                    sampling_context = Some(&fallback_context);
                    sampling_project_state = Some(project_state);
                }
            }
        }
    }

    let matching_trace_rule = match get_trace_sampling_rule(
        processing_enabled,
        sampling_project_state,
        sampling_context,
        ip_addr,
    ) {
        Ok(spec) => spec,
        Err(sampling_result) => return sampling_result,
    };

    let matching_event_rule = match get_event_sampling_rule(
        processing_enabled,
        project_state,
        sampling_context,
        event,
        ip_addr,
    ) {
        Ok(spec) => spec,
        Err(sampling_result) => return sampling_result,
    };

    // NOTE: Event rules take precedence over trace rules. If the event rule has a lower sample rate
    // than the trace rule, this means that traces will be incomplete.
    // We could guarantee consistent traces if trace rules took precedence over event rules,
    // but we need the current behavior to allow health check rules
    // to take precedence over the overall base rate, which is set on the trace.
    if let Some(spec) = matching_event_rule.or(matching_trace_rule) {
        let random_number = pseudo_random_from_uuid(spec.seed);
        if random_number >= spec.sample_rate {
            return SamplingResult::Drop(spec.rule_id);
        } else {
            return SamplingResult::Keep;
        }
    }

    SamplingResult::Keep
}

/// Returns the project key defined in the `trace` header of the envelope.
///
/// This function returns `None` if:
///  - there is no [`DynamicSamplingContext`] in the envelope headers.
///  - there are no transactions in the envelope, since in this case sampling by trace is redundant.
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    let transaction_item = envelope.get_item_by(|item| item.ty() == &ItemType::Transaction);

    // if there are no transactions to sample, return here
    transaction_item?;

    envelope.sampling_context().map(|dsc| dsc.public_key)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use relay_common::EventType;
    use relay_general::protocol::EventId;
    use relay_general::types::Annotated;
    use relay_sampling::{RuleCondition, RuleId, RuleType, SamplingConfig, SamplingRule};

    use crate::envelope::Item;

    use super::*;

    fn state_with_config(sampling_config: SamplingConfig) -> ProjectState {
        let mut state = ProjectState::allowed();
        state.config.dynamic_sampling = Some(sampling_config);
        state
    }

    fn state_with_rule(
        sample_rate: Option<f64>,
        rule_type: RuleType,
        mode: SamplingMode,
    ) -> ProjectState {
        let rules = match sample_rate {
            Some(sample_rate) => vec![SamplingRule {
                condition: RuleCondition::all(),
                sample_rate,
                ty: rule_type,
                id: RuleId(1),
                time_range: Default::default(),
            }],
            None => Vec::new(),
        };

        state_with_config(SamplingConfig {
            rules,
            mode,
            next_id: None,
        })
    }

    fn create_sampling_context(sample_rate: Option<f64>) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: uuid::Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: None,
            environment: None,
            transaction: None,
            sample_rate,
            user: Default::default(),
            other: Default::default(),
        }
    }

    /// ugly hack to build an envelope with an optional trace context
    fn new_envelope(with_trace_context: bool) -> Envelope {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42";
        let event_id = EventId::new();

        let raw_event = if with_trace_context {
            format!(
                "{{\"event_id\":\"{}\",\"dsn\":\"{}\", \"trace\": {}}}\n",
                event_id.0.to_simple(),
                dsn,
                serde_json::to_string(&create_sampling_context(None)).unwrap(),
            )
        } else {
            format!(
                "{{\"event_id\":\"{}\",\"dsn\":\"{}\"}}\n",
                event_id.0.to_simple(),
                dsn,
            )
        };

        let bytes = Bytes::from(raw_event);

        let mut envelope = Envelope::parse_bytes(bytes).unwrap();

        let item1 = Item::new(ItemType::Transaction);
        envelope.add_item(item1);

        let item2 = Item::new(ItemType::Attachment);
        envelope.add_item(item2);

        let item3 = Item::new(ItemType::Attachment);
        envelope.add_item(item3);

        envelope
    }

    #[test]
    /// Should_keep_event returns the expected results.
    fn test_should_keep_event() {
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Error),
            ..Event::default()
        };

        let proj_state = state_with_rule(Some(0.0), RuleType::Error, SamplingMode::default());

        assert_eq!(
            SamplingResult::Drop(RuleId(1)),
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
        let proj_state = state_with_rule(Some(1.0), RuleType::Error, SamplingMode::default());
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
        let proj_state = state_with_rule(None, RuleType::Error, SamplingMode::default());
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
    }

    #[test]
    fn test_unsampled_envelope_with_sample_rate() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(true);
        let state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::default());
        let sampling_state = state_with_rule(Some(0.0), RuleType::Trace, SamplingMode::default());
        let result = should_keep_event(
            envelope.sampling_context(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::Drop(RuleId(1)));
    }

    #[test]
    /// Should keep transaction when no trace context is present
    fn test_should_keep_transaction_no_trace() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(false);
        let state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::default());
        let sampling_state = state_with_rule(Some(0.0), RuleType::Trace, SamplingMode::default());

        let result = should_keep_event(
            envelope.sampling_context(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::Keep);
        // both the event and the transaction item should have been left in the envelope
        assert_eq!(envelope.len(), 3);
    }

    #[test]
    /// When the envelope becomes empty due to sampling we should get back the rule that dropped the
    /// transaction
    fn test_should_signal_when_envelope_becomes_empty() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(true);
        let state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::default());
        let sampling_state = state_with_rule(Some(0.0), RuleType::Trace, SamplingMode::default());

        let result = should_keep_event(
            envelope.sampling_context(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::Drop(RuleId(1)));
    }

    #[test]
    /// When there's a mixture of event rules and trace rules, the event rules
    /// take precedence.
    fn test_event_rule_precedence() {
        let sampling_config = serde_json::json!(
            {
                "rules": [
                    {
                        "sampleRate": 0,
                        "type": "trace",
                        "active": true,
                        "condition": {
                            "op": "and",
                            "inner": []
                        },
                        "id": 1000
                    },
                    {
                        "sampleRate": 1,
                        "type": "transaction",
                        "condition": {
                            "op": "or",
                            "inner": [
                            {
                                "op": "glob",
                                "name": "event.transaction",
                                "value": [
                                    "my-important-transaction",
                                ],
                                "options": {
                                    "ignoreCase": true
                                }
                            }
                            ]
                        },
                        "active": true,
                        "id": 1002
                    }
                ]
            }
        );

        let sampling_config = serde_json::from_value(sampling_config).unwrap();
        let project_state = state_with_config(sampling_config);

        let envelope = new_envelope(true);

        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("my-important-transaction".to_owned()),
            ..Event::default()
        };

        let keep_event = should_keep_event(
            envelope.sampling_context(),
            Some(&event),
            None,
            &project_state,
            Some(&project_state),
            true,
        );

        assert_eq!(keep_event, SamplingResult::Keep);
    }

    #[test]
    fn test_trace_rule_received() {
        let project_state = state_with_rule(Some(0.1), RuleType::Trace, SamplingMode::Received);
        let sampling_context = create_sampling_context(Some(0.5));
        let spec = get_trace_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            Some(&project_state),
            Some(&sampling_context),
            None,
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.1);
    }

    #[test]
    fn test_trace_rule_adjusted() {
        let project_state = state_with_rule(Some(0.1), RuleType::Trace, SamplingMode::Total);
        let sampling_context = create_sampling_context(Some(0.5));
        let spec = get_trace_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            Some(&project_state),
            Some(&sampling_context),
            None,
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.2);
    }

    #[test]
    fn test_trace_rule_unsupported() {
        let project_state = state_with_rule(Some(0.1), RuleType::Trace, SamplingMode::Unsupported);
        let sampling_context = create_sampling_context(Some(0.5));
        let spec =
            get_trace_sampling_rule(true, Some(&project_state), Some(&sampling_context), None);

        assert!(matches!(spec, Err(SamplingResult::Keep)));
    }

    #[test]
    fn test_event_rule_received() {
        let project_state =
            state_with_rule(Some(0.1), RuleType::Transaction, SamplingMode::Received);
        let sampling_context = create_sampling_context(Some(0.5));
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };

        let spec = get_event_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            &project_state,
            Some(&sampling_context),
            Some(&event),
            None, // ip address not needed for uniform rule
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.1);
    }

    #[test]
    fn test_event_rule_adjusted() {
        let project_state = state_with_rule(Some(0.1), RuleType::Transaction, SamplingMode::Total);
        let sampling_context = create_sampling_context(Some(0.5));
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };

        let spec = get_event_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            &project_state,
            Some(&sampling_context),
            Some(&event),
            None, // ip address not needed for uniform rule
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.2);
    }
}
