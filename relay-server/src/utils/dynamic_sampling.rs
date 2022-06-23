//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use actix::prelude::*;
use futures::{future, prelude::*};

use relay_common::ProjectKey;
use relay_general::protocol::{Event, EventId};
use relay_sampling::{
    get_matching_event_rule, pseudo_random_from_uuid, rule_type_for_event, RuleId, SamplingResult,
};

use crate::actors::envelopes::EnvelopeContext;
use crate::actors::outcome::Outcome::FilteredSampling;
use crate::actors::project::ProjectState;
use crate::actors::project_cache::{GetCachedProjectState, GetProjectState, ProjectCache};
use crate::envelope::{Envelope, ItemType};

/// Checks whether an event should be kept or removed by dynamic sampling.
pub fn should_keep_event(
    event: &Event,
    ip_addr: Option<IpAddr>,
    project_state: &ProjectState,
    processing_enabled: bool,
) -> SamplingResult {
    let sampling_config = match &project_state.config.dynamic_sampling {
        // without config there is not enough info to make up my mind
        None => return SamplingResult::NoDecision,
        Some(config) => config,
    };

    // when we have unsupported rules disable sampling for non processing relays
    if !processing_enabled && sampling_config.has_unsupported_rules() {
        return SamplingResult::Keep;
    }

    let event_id = match event.id.0 {
        // if no eventID we can't really do sampling so do not take a decision
        None => return SamplingResult::NoDecision,
        Some(EventId(id)) => id,
    };

    let ty = rule_type_for_event(event);
    if let Some(rule) = get_matching_event_rule(sampling_config, event, ip_addr, ty) {
        let random_number = pseudo_random_from_uuid(event_id);
        if random_number < rule.sample_rate {
            return SamplingResult::Keep;
        }
        return SamplingResult::Drop(rule.id);
    }
    // if there are no matching rules there is not enough info to make a sampling decision
    SamplingResult::NoDecision
}

/// Execute dynamic sampling on an envelope using the provided project state.
fn sample_transaction_internal(
    envelope: &Envelope,
    project_state: &ProjectState,
    processing_enabled: bool,
) -> Result<(), RuleId> {
    let sampling_config = match project_state.config.dynamic_sampling {
        // without sampling config we cannot sample transactions so give up here
        None => return Ok(()),
        Some(ref sampling_config) => sampling_config,
    };

    // when we have unsupported rules disable sampling for non processing relays
    if !processing_enabled && sampling_config.has_unsupported_rules() {
        return Ok(());
    }

    let trace_context = envelope.trace_context();
    // let transaction_item = envelope.get_item_by(|item| item.ty() == &ItemType::Transaction);

    let trace_context = match (trace_context) {
        // we don't have what we need, can't sample the transactions in this envelope
        None => {
            return Ok(());
        }
        // see if we need to sample the transaction
        Some(trace_context) => trace_context,
    };

    let client_ip = envelope.meta().client_addr();
    if let SamplingResult::Drop(rule_id) = trace_context.should_keep(client_ip, sampling_config) {
        Err(rule_id)
    } else {
        // if we don't have a decision yet keep the transaction
        Ok(())
    }
}

/// TODO: docs
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    let transaction_item = envelope.get_item_by(|item| item.ty() == &ItemType::Transaction);

    // if there are no transactions to sample, return here
    transaction_item?;

    envelope.trace_context().map(|tc| tc.public_key)
}

pub fn sample_trace(
    envelope: &Envelope,
    project_state: &ProjectState,
    processing_enabled: bool,
) -> Result<(), RuleId> {
    sample_transaction_internal(envelope, project_state, processing_enabled)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use bytes::Bytes;
    use smallvec::SmallVec;

    use relay_common::EventType;
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
            should_keep_event(&event, None, &proj_state, true)
        );
        let proj_state = get_project_state(Some(1.0), RuleType::Error);
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(&event, None, &proj_state, true)
        );
        let proj_state = get_project_state(None, RuleType::Error);
        assert_eq!(
            SamplingResult::NoDecision,
            should_keep_event(&event, None, &proj_state, true)
        );
    }

    #[test]
    /// Should remove transaction from envelope when a matching rule is detected
    fn test_should_drop_transaction() {
        //create an envelope with a event and a transaction
        let mut envelope = new_envelope(true);
        // add an item that is not dependent on the transaction (i.e. will not be dropped with it)
        let session_item = Item::new(ItemType::Session);
        envelope.add_item(session_item);

        let state = get_project_state(Some(0.0), RuleType::Trace);

        let result = sample_transaction_internal(&mut envelope, &state, true);
        assert!(result.is_ok());
        // the transaction item and dependent items should have been removed
        assert_eq!(envelope.len(), 1);
    }

    #[test]
    /// Should keep transaction when no trace context is present
    fn test_should_keep_transaction_no_trace() {
        //create an envelope with a event and a transaction
        let mut envelope = new_envelope(false);
        let state = get_project_state(Some(0.0), RuleType::Trace);

        let result = sample_transaction_internal(&mut envelope, &state, true);
        assert!(result.is_ok());
        // both the event and the transaction item should have been left in the envelope
        assert_eq!(envelope.len(), 3);
    }

    #[test]
    /// When the envelope becomes empty due to sampling we should get back the rule that dropped the
    /// transaction
    fn test_should_signal_when_envelope_becomes_empty() {
        //create an envelope with a event and a transaction
        let mut envelope = new_envelope(true);
        let state = get_project_state(Some(0.0), RuleType::Trace);

        let result = sample_transaction_internal(&mut envelope, &state, true);
        assert!(result.is_err());
        let rule_id = result.unwrap_err();
        // we got back the rule id
        assert_eq!(rule_id, RuleId(1));
    }
}
