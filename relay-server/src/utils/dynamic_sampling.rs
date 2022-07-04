//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use relay_common::ProjectKey;
use relay_general::protocol::Event;
use relay_sampling::{
    get_matching_event_rule, get_matching_trace_rule, pseudo_random_from_uuid,
    DynamicSamplingContext, RuleId, SamplingResult,
};

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

/// Checks whether an event should be kept or removed by dynamic sampling.
pub fn should_keep_event(
    sampling_context: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    project_state: &ProjectState,
    trace_root_project_state: Option<&ProjectState>,
    processing_enabled: bool,
) -> SamplingResult {
    let mut matching_rule = None;

    if let (Some(sampling_context), Some(trace_root_project_state)) =
        (sampling_context, trace_root_project_state)
    {
        if let Some(ref sampling_config) = trace_root_project_state.config.dynamic_sampling {
            // when we have unsupported rules disable sampling for non processing relays
            if !processing_enabled && sampling_config.has_unsupported_rules() {
                return SamplingResult::Keep;
            }

            if let Some(rule) = get_matching_trace_rule(sampling_config, sampling_context, ip_addr)
            {
                matching_rule = Some((rule, sampling_context.trace_id));
            }
        }
    }

    if matching_rule.is_none() {
        if let Some(event) = event {
            if let Some(event_id) = event.id.0 {
                if let Some(ref sampling_config) = project_state.config.dynamic_sampling {
                    // when we have unsupported rules disable sampling for non processing relays
                    if !processing_enabled && sampling_config.has_unsupported_rules() {
                        return SamplingResult::Keep;
                    }

                    if let Some(rule) = get_matching_event_rule(sampling_config, event, ip_addr) {
                        matching_rule = Some((rule, event_id.0));
                    }
                }
            }
        }
    }

    if let Some((rule, uuid)) = matching_rule {
        let adjusted_sample_rate = if let Some(sampling_context) = sampling_context {
            sampling_context.adjusted_sample_rate(rule.sample_rate)
        } else {
            rule.sample_rate
        };

        let random_number = pseudo_random_from_uuid(uuid);

        if random_number < adjusted_sample_rate {
            return SamplingResult::Keep;
        }
        return SamplingResult::Drop(rule.id);
    }

    SamplingResult::NoDecision
}

/// Returns the project key defined in the `trace` header of the envelope, if defined.
/// If there are no transactions in the envelope, we return None here, because there is nothing
/// to sample by trace.
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    let transaction_item = envelope.get_item_by(|item| item.ty() == &ItemType::Transaction);

    // if there are no transactions to sample, return here
    transaction_item?;

    envelope.sampling_context().map(|dsc| dsc.public_key)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use bytes::Bytes;
    use smallvec::SmallVec;

    use relay_common::EventType;
    use relay_general::protocol::EventId;
    use relay_general::types::Annotated;
    use relay_sampling::{RuleId, RuleType, SamplingConfig};

    use crate::actors::project::ProjectConfig;
    use crate::envelope::Item;

    use super::*;

    fn get_project_state(sample_rate: Option<f64>, rule_type: RuleType) -> ProjectState {
        let sampling_config_str = if let Some(sample_rate) = sample_rate {
            let rt = match rule_type {
                RuleType::Transaction => "transaction",
                RuleType::Error => "error",
                RuleType::Trace => "trace",
            };
            format!(
                r#"{{
                "rules":[{{
                    "condition": {{ "op": "and", "inner":[]}},
                    "sampleRate": {},
                    "type": "{}",
                    "id": 1
                }}]
            }}"#,
                sample_rate, rt
            )
        } else {
            "{\"rules\":[]}".to_owned()
        };
        let sampling_config = serde_json::from_str::<SamplingConfig>(&sampling_config_str).ok();

        ProjectState {
            project_id: None,
            disabled: false,
            public_keys: SmallVec::new(),
            slug: None,
            config: ProjectConfig {
                dynamic_sampling: sampling_config,
                ..ProjectConfig::default()
            },
            organization_id: None,
            last_change: None,
            last_fetch: Instant::now(),
            invalid: false,
        }
    }

    /// ugly hack to build an envelope with an optional trace context
    fn new_envelope(with_trace_context: bool) -> Envelope {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42";
        let event_id = EventId::new();

        let raw_event = if with_trace_context {
            let trace_id = uuid::Uuid::new_v4();
            let project_key = "12345678901234567890123456789012";
            let trace_context_raw = format!(
                r#"{{"trace_id": "{}", "public_key": "{}"}}"#,
                trace_id.to_simple(),
                project_key,
            );
            format!(
                "{{\"event_id\":\"{}\",\"dsn\":\"{}\", \"trace\": {}}}\n",
                event_id.0.to_simple(),
                dsn,
                trace_context_raw,
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

        let proj_state = get_project_state(Some(0.0), RuleType::Error);

        assert_eq!(
            SamplingResult::Drop(RuleId(1)),
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
        let proj_state = get_project_state(Some(1.0), RuleType::Error);
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
        let proj_state = get_project_state(None, RuleType::Error);
        assert_eq!(
            SamplingResult::NoDecision,
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
    }

    #[test]
    fn test_unsampled_envelope_with_sample_rate() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(true);
        let state = get_project_state(Some(1.0), RuleType::Trace);
        let sampling_state = get_project_state(Some(0.0), RuleType::Trace);
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
        let state = get_project_state(Some(1.0), RuleType::Trace);
        let sampling_state = get_project_state(Some(0.0), RuleType::Trace);

        let result = should_keep_event(
            envelope.sampling_context(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::NoDecision);
        // both the event and the transaction item should have been left in the envelope
        assert_eq!(envelope.len(), 3);
    }

    #[test]
    /// When the envelope becomes empty due to sampling we should get back the rule that dropped the
    /// transaction
    fn test_should_signal_when_envelope_becomes_empty() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(true);
        let state = get_project_state(Some(1.0), RuleType::Trace);
        let sampling_state = get_project_state(Some(0.0), RuleType::Trace);

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
}
