//! Functionality for calculating if a trace should be processed or dropped.
use std::collections::BTreeMap;
use std::fmt::Display;

use chrono::{DateTime, Utc};
use relay_base_schema::project::ProjectKey;
use relay_event_schema::protocol::Event;
use relay_redis::{redis, RedisError, RedisPool};
use relay_sampling::config::RuleId;
use relay_sampling::evaluation::{MatchedRuleIds, SamplingMatch};
use relay_sampling::DynamicSamplingContext;
use relay_system::Addr;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::{DisableReservoir, ProjectCache, UpdateCount};
use crate::actors::project_redis::RedisProjectError;
use crate::actors::upstream::UpstreamRelay;
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
        let Some(sampling_match) = sampling_match else {
            relay_log::trace!("keeping event that didn't match the configuration");
            return SamplingResult::Keep;
        };

        match sampling_match {
            SamplingMatch::Bias { .. } => return SamplingResult::Keep,
            SamplingMatch::Other {
                sample_rate,
                seed,
                matched_rule_ids,
            } => {
                let random_number = relay_sampling::evaluation::pseudo_random_from_uuid(seed);
                relay_log::trace!(
                    sample_rate,
                    random_number,
                    "applying dynamic sampling to matching event"
                );

                if random_number >= sample_rate {
                    relay_log::trace!("dropping event that matched the configuration");
                    SamplingResult::Drop(matched_rule_ids)
                } else {
                    relay_log::trace!("keeping event that matched the configuration");
                    SamplingResult::Keep
                }
            }
        }
    }
}

pub struct BiasRedisKey(String);

impl BiasRedisKey {
    pub fn new(project_key: &ProjectKey, rule_id: RuleId) -> Self {
        Self(format!("bias:{}:{}", project_key, rule_id))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

pub fn thisfunctionshouldnotifysentrythatwehavereachedthereservoirlimit(
    upstream: Addr<UpstreamRelay>,
    project_key: &ProjectKey,
    rule_id: RuleId,
) {
    todo!()
}

pub fn delete_bias_rule(
    redis_pool: &RedisPool,
    key: &BiasRedisKey,
) -> Result<(), RedisProjectError> {
    let mut command = relay_redis::redis::cmd("DEL");
    command.arg(key.as_str());

    let _: i64 = command
        .query(&mut redis_pool.client()?.connection()?)
        .map_err(RedisError::Redis)?;

    Ok(())
}

pub fn get_bias_rule_count(
    redis_pool: &RedisPool,
    key: &BiasRedisKey,
) -> Result<Option<i64>, RedisProjectError> {
    let mut command = relay_redis::redis::cmd("GET");

    command.arg(key.as_str());

    let raw_response_opt: Option<Vec<u8>> = command
        .query(&mut redis_pool.client()?.connection()?)
        .map_err(RedisError::Redis)?;

    let response = match raw_response_opt {
        Some(response) => {
            let count = std::str::from_utf8(&response)
                .map_err(|_| {
                    RedisProjectError::Parsing(serde_json::Error::io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid UTF-8 sequence",
                    )))
                })?
                .parse::<i64>()
                .map_err(|_| {
                    RedisProjectError::Parsing(serde_json::Error::io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid number format",
                    )))
                })?;
            Some(count)
        }
        None => None,
    };

    Ok(response)
}

pub fn increment_bias_rule_count(
    redis_pool: &RedisPool,
    key: &BiasRedisKey,
) -> Result<i64, RedisProjectError> {
    let mut command = relay_redis::redis::cmd("INCR");

    command.arg(key.as_str());

    let new_count: i64 = command
        .query(&mut redis_pool.client()?.connection()?)
        .map_err(RedisError::Redis)?;

    Ok(new_count)
}

/// Runs dynamic sampling on an incoming event/dsc and returns whether or not the event should be
/// kept or dropped.
pub fn get_sampling_result(
    processing_enabled: bool,
    project_state: Option<&ProjectState>,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    reservoir_stuff: &BTreeMap<RuleId, usize>,
    projcache: Addr<ProjectCache>,
    project_key: Option<ProjectKey>,
    redis: Option<RedisPool>,
    upstream: Addr<UpstreamRelay>,
) -> SamplingResult {
    // We want to extract the SamplingConfig from each project state.
    let sampling_config: Option<&relay_sampling::SamplingConfig> =
        project_state.and_then(|state| state.config.dynamic_sampling.as_ref());
    let root_sampling_config =
        root_project_state.and_then(|state| state.config.dynamic_sampling.as_ref());

    let sampling_match = relay_sampling::evaluation::merge_configs_and_match(
        processing_enabled,
        sampling_config,
        root_sampling_config,
        dsc,
        event,
        Utc::now(),
        reservoir_stuff,
    );

    if let Some(SamplingMatch::Bias { rule_id }) = sampling_match {
        let project_key = project_key.unwrap();
        projcache.send(UpdateCount {
            project_key: project_key.clone(),
            rule_id,
        });

        if let Some(redis) = redis {
            if processing_enabled {
                let key = BiasRedisKey::new(&project_key, rule_id);
                // 1. update count in relay
                // 2. ask for total count
                // 3. if total count exceeds limit, signal to sentry to remove the bias

                increment_bias_rule_count(&redis, &key);
                let total_count = get_bias_rule_count(&redis, &key).unwrap().unwrap();
                if total_count as usize >= *reservoir_stuff.get(&rule_id).unwrap() {
                    delete_bias_rule(&redis, &key);
                    thisfunctionshouldnotifysentrythatwehavereachedthereservoirlimit(
                        upstream,
                        &project_key,
                        rule_id,
                    );
                }
            }
        }
    }

    SamplingResult::determine_from_sampling_match(sampling_match)
}

/// Runs dynamic sampling and returns whether the
/// transactions received with such dsc and project state would be kept or dropped by dynamic
/// sampling.
pub fn is_trace_fully_sampled(
    processing_enabled: bool,
    root_project_state: &ProjectState,
    dsc: &DynamicSamplingContext,
    projcache: Addr<ProjectCache>,
) -> Option<bool> {
    // If the sampled field is not set, we prefer to not tag the error since we have no clue on
    // whether the head of the trace was kept or dropped on the client side.
    // In addition, if the head of the trace was dropped on the client we will immediately mark
    // the trace as not fully sampled.
    if !(dsc.sampled?) {
        return Some(false);
    }

    let sampling_result = get_sampling_result(
        processing_enabled,
        None,
        Some(root_project_state),
        Some(dsc),
        None,
        &BTreeMap::default(),
        projcache,
        None,
    );

    Some(matches!(sampling_result, SamplingResult::Keep))
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
/*
#[cfg(test)]
mod tests {
    use relay_base_schema::events::EventType;
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::Annotated;
    use relay_sampling::condition::{EqCondOptions, EqCondition, RuleCondition};
    use relay_sampling::config::{
        RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue,
    };
    use similar_asserts::assert_eq;
    use uuid::Uuid;

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
        sampled: Option<bool>,
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
            sampled,
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

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
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

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
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

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
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

        let result = get_sampling_result(
            false,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
        assert_eq!(result, SamplingResult::Keep);

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
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
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = get_sampling_result(
            true,
            None,
            Some(&root_project_state),
            Some(&dsc),
            None,
            &BTreeMap::default(),
        );
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that a trace is marked as fully sampled correctly when dsc and project state are set.
    fn test_is_trace_fully_sampled_with_valid_dsc_and_project_state() {
        // We test with `sampled = true` and 100% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(true, Some(&project_state), Some(&dsc)).unwrap();
        assert!(result);

        // We test with `sampled = true` and 0% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 0.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(true, Some(&project_state), Some(&dsc)).unwrap();
        assert!(!result);

        // We test with `sampled = false` and 100% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(false));

        let result = is_trace_fully_sampled(true, Some(&project_state), Some(&dsc)).unwrap();
        assert!(!result);
    }

    #[test]
    /// Tests that a trace is not marked as fully sampled or not if inputs are invalid.
    fn test_is_trace_fully_sampled_with_invalid_inputs() {
        // We test with missing `sampled`.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = is_trace_fully_sampled(true, Some(&project_state), Some(&dsc));
        assert!(result.is_none());

        // We test with missing dsc and project config.
        let result = is_trace_fully_sampled(true, None, None);
        assert!(result.is_none())
    }
}

*/
