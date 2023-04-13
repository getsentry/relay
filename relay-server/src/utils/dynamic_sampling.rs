//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_common::ProjectKey;
use relay_general::protocol::Event;
use relay_sampling::{DynamicSamplingContext, MatchedRuleIds, SamplingConfig, SamplingMatch};

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

/// The result of a sampling operation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event.
    ///
    /// Relay either applied sampling rules and decided to keep the event, or was unable to parse
    /// the rules.
    #[default]
    Keep,

    /// Drop the event, due to a list of rules with provided identifiers.
    Drop(MatchedRuleIds),
}

/// Matches the incoming event with the SamplingConfig(s) of the project and root project.
fn get_sampling_match(
    processing_enabled: bool,
    project_state: &ProjectState,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    now: DateTime<Utc>,
) -> Option<SamplingMatch> {
    let event = event?;
    // We want to extract the SamplingConfig from each project state.
    let sampling_config = project_state.config.dynamic_sampling.as_ref()?;
    let root_sampling_config =
        root_project_state.and_then(|state| state.config.dynamic_sampling.as_ref());

    SamplingConfig::merge_configs_and_match(
        processing_enabled,
        sampling_config,
        root_sampling_config,
        dsc,
        event,
        ip_addr,
        now,
    )
}

/// Checks whether an incoming event should be kept or dropped based on the result of the sampling
/// configuration match.
pub fn should_keep_event(
    processing_enabled: bool,
    project_state: &ProjectState,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
) -> SamplingResult {
    match get_sampling_match(
        processing_enabled,
        project_state,
        root_project_state,
        dsc,
        event,
        ip_addr,
        // For consistency reasons we take a snapshot in time and use that time across all code that
        // requires it.
        Utc::now(),
    ) {
        Some(SamplingMatch {
            sample_rate,
            matched_rule_ids,
            seed,
        }) => {
            let random_number = relay_sampling::pseudo_random_from_uuid(seed);
            relay_log::trace!("sampling event with sample rate {}", sample_rate);
            if random_number >= sample_rate {
                relay_log::trace!("dropping event that matched the configuration");
                SamplingResult::Drop(matched_rule_ids)
            } else {
                relay_log::trace!("keeping event that matched the configuration");
                SamplingResult::Keep
            }
        }
        None => {
            relay_log::trace!("keeping event that didn't match the configuration");
            SamplingResult::Keep
        }
    }
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

    envelope.dsc().map(|dsc| dsc.public_key)
}

#[cfg(test)]
mod tests {
    use relay_common::EventType;
    use relay_general::protocol::{EventId, LenientString};
    use relay_general::types::Annotated;
    use relay_sampling::{
        EqCondOptions, EqCondition, RuleCondition, RuleId, RuleType, SamplingConfig, SamplingMode,
        SamplingRule, SamplingValue,
    };
    use similar_asserts::assert_eq;

    use super::*;
    use crate::testutils::project_state_with_config;

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            ..Event::default()
        }
    }

    fn mocked_sampling_rule(id: u32, ty: RuleType, sample_rate: f64) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: sample_rate },
            ty,
            id: RuleId(id),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }
    }

    #[test]
    /// Tests that an event is kept when there is a match and we have 100% sample rate.
    fn test_should_keep_event_return_keep_with_match_and_100_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_should_keep_event_return_drop_with_match_and_0_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(1)]))
        )
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_should_keep_event_return_keep_with_no_match() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![SamplingRule {
                condition: eq("event.transaction", &["foo"], true),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "bar", "2.0");

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that an event is kept when there are unsupported rules with no processing and vice versa.
    fn test_should_keep_event_return_keep_with_unsupported_rule() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 0.0),
                mocked_sampling_rule(2, RuleType::Transaction, 0.0),
            ],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event(false, &project_state, None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep);

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(2)]))
        )
    }
}
