//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_common::ProjectKey;
use relay_general::protocol::Event;
use relay_sampling::{
    merge_configs_and_match, DynamicSamplingContext, MatchedRuleIds, SamplingMatch,
};

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

impl SamplingResult {
    fn determine_from_sampling_match(sampling_match: Option<SamplingMatch>) -> Self {
        match sampling_match {
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
}

/// Gets the sampling match result by creating the merged configuration and matching it against
/// the sampling configuration.
fn get_sampling_match_result(
    processing_enabled: bool,
    project_state: Option<&ProjectState>,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    now: DateTime<Utc>,
) -> Option<SamplingMatch> {
    // We want to extract the SamplingConfig from each project state.
    let sampling_config = project_state.and_then(|state| state.config.dynamic_sampling.as_ref());
    let root_sampling_config =
        root_project_state.and_then(|state| state.config.dynamic_sampling.as_ref());

    merge_configs_and_match(
        processing_enabled,
        sampling_config,
        root_sampling_config,
        dsc,
        event,
        ip_addr,
        now,
    )
}

/// Runs dynamic sampling on an incoming event/dsc and returns whether or not the event should be
/// kept or dropped.
pub fn get_sampling_result(
    processing_enabled: bool,
    project_state: Option<&ProjectState>,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
) -> SamplingResult {
    let sampling_result = get_sampling_match_result(
        processing_enabled,
        project_state,
        root_project_state,
        dsc,
        event,
        ip_addr,
        // For consistency reasons we take a snapshot in time and use that time across all code that
        // requires it.
        Utc::now(),
    );
    SamplingResult::determine_from_sampling_match(sampling_result)
}

/// Returns the project key defined in the `trace` header of the envelope.
///
/// This function returns `None` if:
///  - there is no [`DynamicSamplingContext`] in the envelope headers.
///  - there are no transactions or events in the envelope, since in this case sampling by trace is redundant.
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    // If the envelope item is not of type transaction or event, we will not return a sampling key
    // because it doesn't make sense to load the root project state if we don't perform trace
    // sampling.
    envelope
        .get_item_by(|item| item.ty() == &ItemType::Transaction || item.ty() == &ItemType::Event)?;
    envelope.dsc().map(|dsc| dsc.public_key)
}

#[cfg(test)]
mod tests {
    use relay_common::{EventType, Uuid};
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

    fn mocked_simple_dynamic_sampling_context(
        sample_rate: Option<f64>,
        release: Option<&str>,
        transaction: Option<&str>,
        environment: Option<&str>,
    ) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: release.map(|value| value.to_string()),
            environment: environment.map(|value| value.to_string()),
            transaction: transaction.map(|value| value.to_string()),
            sample_rate,
            user: Default::default(),
            other: Default::default(),
            replay_id: None,
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
    fn test_get_sampling_result_return_keep_with_match_and_100_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result =
            get_sampling_result(true, Some(&project_state), None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_get_sampling_result_return_drop_with_match_and_0_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result =
            get_sampling_result(true, Some(&project_state), None, None, Some(&event), None);
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(1)]))
        )
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_get_sampling_result_return_keep_with_no_match() {
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

        let result =
            get_sampling_result(true, Some(&project_state), None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that an event is kept when there are unsupported rules with no processing and vice versa.
    fn test_get_sampling_result_return_keep_with_unsupported_rule() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 0.0),
                mocked_sampling_rule(2, RuleType::Transaction, 0.0),
            ],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result =
            get_sampling_result(false, Some(&project_state), None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep);

        let result =
            get_sampling_result(true, Some(&project_state), None, None, Some(&event), None);
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(2)]))
        )
    }

    #[test]
    /// Tests that an event is kept when there is a trace match and we have 100% sample rate.
    fn test_get_sampling_result_with_traces_rules_return_keep_when_match() {
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None);

        let result = get_sampling_result(
            true,
            None,
            Some(&root_project_state),
            Some(&dsc),
            None,
            None,
        );
        assert_eq!(result, SamplingResult::Keep)
    }
}
