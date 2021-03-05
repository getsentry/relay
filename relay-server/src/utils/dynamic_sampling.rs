//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use actix::prelude::*;
use futures::{future, prelude::*};

use relay_general::protocol::{Event, EventId};
use relay_sampling::{
    get_matching_event_rule, pseudo_random_from_uuid, rule_type_for_event, RuleId,
};

use crate::actors::project::{GetCachedProjectState, GetProjectState, Project, ProjectState};
use crate::envelope::{Envelope, ItemType};

/// The result of a Sampling operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event.
    Keep,
    /// Drop the event, due to the rule with provided Id.
    Drop(RuleId),
    /// No decision can be made.  
    NoDecision,
}

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

    let ty = rule_type_for_event(&event);
    if let Some(rule) = get_matching_event_rule(sampling_config, event, ip_addr, ty) {
        if let Some(random_number) = pseudo_random_from_uuid(event_id) {
            if random_number < rule.sample_rate {
                return SamplingResult::Keep;
            }
            return SamplingResult::Drop(rule.id);
        }
    }
    // if there are no matching rules there is not enough info to make a sampling decision
    SamplingResult::NoDecision
}

/// Takes an envelope and potentially removes the transaction item from it if that
/// transaction item should be sampled out according to the dynamic sampling configuration
/// and the trace context.
fn sample_transaction_internal(
    mut envelope: Envelope,
    project_state: Option<&ProjectState>,
    processing_enabled: bool,
) -> Envelope {
    let project_state = match project_state {
        None => return envelope,
        Some(project_state) => project_state,
    };

    let sampling_config = match project_state.config.dynamic_sampling {
        // without sampling config we cannot sample transactions so give up here
        None => return envelope,
        Some(ref sampling_config) => sampling_config,
    };

    // when we have unsupported rules disable sampling for non processing relays
    if !processing_enabled && sampling_config.has_unsupported_rules() {
        return envelope;
    }

    let trace_context = envelope.trace_context();
    let transaction_item = envelope.get_item_by(|item| item.ty() == ItemType::Transaction);

    let trace_context = match (trace_context, transaction_item) {
        // we don't have what we need, can't sample the transactions in this envelope
        (None, _) | (_, None) => return envelope,
        // see if we need to sample the transaction
        (Some(trace_context), Some(_)) => trace_context,
    };

    let client_ip = envelope.meta().client_addr();

    let should_sample = trace_context
        // see if we should sample
        .should_sample(client_ip, sampling_config)
        // TODO verify that this is the desired behaviour (i.e. if we can't find a rule
        // for sampling, include the transaction)
        .unwrap_or(true);

    if !should_sample {
        // finally we decided that we should sample the transaction
        envelope.take_item_by(|item| item.ty() == ItemType::Transaction);
    }

    envelope
}

/// Check if we should remove transactions from this envelope (because of trace sampling) and
/// return what is left of the envelope.
pub fn sample_transaction(
    envelope: Envelope,
    project: Option<Addr<Project>>,
    fast_processing: bool,
    processing_enabled: bool,
) -> ResponseFuture<Envelope, ()> {
    let project = match project {
        None => return Box::new(future::ok(envelope)),
        Some(project) => project,
    };
    let trace_context = envelope.trace_context();
    let transaction_item = envelope.get_item_by(|item| item.ty() == ItemType::Transaction);

    // if there is no trace context or there are no transactions to sample return here
    if trace_context.is_none() || transaction_item.is_none() {
        return Box::new(future::ok(envelope));
    }
    //we have a trace_context and we have a transaction_item see if we can sample them
    if fast_processing {
        let fut = project
            .send(GetCachedProjectState)
            .then(move |project_state| {
                let project_state = match project_state {
                    // error getting the project, give up and return envelope unchanged
                    Err(_) => return Ok(envelope),
                    Ok(project_state) => project_state,
                };
                Ok(sample_transaction_internal(
                    envelope,
                    project_state.as_deref(),
                    processing_enabled,
                ))
            });
        Box::new(fut) as ResponseFuture<_, _>
    } else {
        let fut = project
            .send(GetProjectState::new())
            .then(move |project_state| {
                let project_state = match project_state {
                    // error getting the project, give up and return envelope unchanged
                    Err(_) => return Ok(envelope),
                    Ok(project_state) => project_state,
                };
                Ok(sample_transaction_internal(
                    envelope,
                    project_state.ok().as_deref(),
                    processing_enabled,
                ))
            });
        Box::new(fut) as ResponseFuture<_, _>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::project::ProjectConfig;
    use relay_common::EventType;
    use relay_general::types::Annotated;
    use relay_sampling::SamplingConfig;
    use smallvec::SmallVec;
    use std::time::Instant;

    fn get_project_state(sample_rate: Option<f64>) -> ProjectState {
        let sampling_config_str = if let Some(sample_rate) = sample_rate {
            format!(
                r#"{{
                "rules":[{{
                    "condition": {{ "op": "and", "inner":[]}},
                    "sampleRate": {},
                    "type": "error",
                    "id": 1
                }}]
            }}"#,
                sample_rate
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

    #[test]
    /// Should_keep_event returns the expected results.
    fn test_should_keep_event() {
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Error),
            ..Event::default()
        };

        let proj_state = get_project_state(Some(0.0));

        assert_eq!(
            SamplingResult::Drop(RuleId(1)),
            should_keep_event(&event, None, &proj_state, true)
        );
        let proj_state = get_project_state(Some(1.0));
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(&event, None, &proj_state, true)
        );
        let proj_state = get_project_state(None);
        assert_eq!(
            SamplingResult::NoDecision,
            should_keep_event(&event, None, &proj_state, true)
        );
    }
}
