//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::convert::TryInto;

use actix::prelude::*;
use futures::{future, prelude::*};
use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::{Deserialize, Serialize};

use relay_common::{EventType, ProjectKey, Uuid};
use relay_filter::GlobPatterns;
use relay_general::protocol::{Event, EventId};

use crate::actors::project::{GetCachedProjectState, GetProjectState, Project, ProjectState};
use crate::envelope::{Envelope, ItemType};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum RuleType {
    /// A trace rule applies only to transactions and it is applied on the trace info
    Trace,
    /// A transaction rule applies to transactions and it is applied  on the transaction event
    Transaction,
    /// A non transaction rule applies to Errors, Security events...every type of event that
    /// is not a Transaction
    Error,
}

/// The value of a field extracted from a FieldValueProvider (Event or TraceContext)
#[derive(Debug, Clone)]
pub enum FieldValue<'a> {
    String(&'a str),
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EqCondData {
    pub name: String,
    pub value: Vec<String>,
    #[serde(default)]
    pub ignore_case: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobCondData {
    pub name: String,
    pub value: GlobPatterns,
}

/// Keeps inner conditions for combinator conditions
/// This structure is used to aid the serialisation of Rules.
/// Since we use internally tagged serialisation for RuleConditions
/// We need to add an inner level when serializing in order not to have a
/// clash fo the tag operator between the inner and the outer Conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerConditions {
    inner: Vec<RuleCondition>,
}

/// Keeps inner conditions for combinator conditions with one element (i.e. Not)
/// This structure is used to aid the serialisation of Rules.
/// See [InnerConditions] for further explanations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerNotCondition {
    inner: Box<RuleCondition>,
}

/// A condition from a sampling rule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "op")]
pub enum RuleCondition {
    Eq(EqCondData),
    Glob(GlobCondData),
    Or(InnerConditions),
    And(InnerConditions),
    Not(InnerNotCondition),
    #[serde(other)]
    Unsupported,
}

impl RuleCondition {
    /// Checks if Relay supports this condition (in other words if the condition had any unknown configuration
    /// which was serialized as "Unsupported" (because the configuration is either faulty or was created for a
    /// newer relay that supports some other condition types)
    fn is_not_supported(&self) -> bool {
        match self {
            RuleCondition::Unsupported => true,
            // dig down for embedded conditions
            RuleCondition::And(rules) => rules.inner.iter().any(RuleCondition::is_not_supported),
            RuleCondition::Or(rules) => rules.inner.iter().any(RuleCondition::is_not_supported),
            RuleCondition::Not(rule) => rule.inner.is_not_supported(),
            // we have a known condition
            _ => false,
        }
    }
    fn matches<T: FieldValueProvider>(&self, value_provider: &T) -> bool {
        match self {
            RuleCondition::Eq(cond) => {
                if cond.ignore_case {
                    str_eq_no_case(&cond.value, &value_provider.get_value(cond.name.as_str()))
                } else {
                    equal(&cond.value, &value_provider.get_value(cond.name.as_str()))
                }
            }
            RuleCondition::Glob(cond) => {
                match_glob(&cond.value, &value_provider.get_value(cond.name.as_str()))
            }
            RuleCondition::And(conditions) => conditions
                .inner
                .iter()
                .all(|cond| cond.matches(value_provider)),
            RuleCondition::Or(conditions) => conditions
                .inner
                .iter()
                .any(|cond| cond.matches(value_provider)),
            RuleCondition::Not(condition) => !condition.inner.matches(value_provider),
            _ => false,
        }
    }
}

/// A sampling rule as it is deserialized from the project configuration  
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    pub condition: RuleCondition,
    pub sample_rate: f64,
    pub ty: RuleType,
}

impl SamplingRule {
    fn is_not_supported(&self) -> bool {
        self.condition.is_not_supported()
    }
}

/// Trait implemented by providers of fields (Events and Trace Contexts).
/// The fields will be used by rules to check if they apply.
trait FieldValueProvider {
    /// gets the value of a field
    fn get_value(&self, path: &str) -> FieldValue;
    /// what type of rule can be applied to this provider
    fn get_rule_type(&self) -> RuleType;
}

impl FieldValueProvider for Event {
    fn get_value(&self, field_name: &str) -> FieldValue {
        match field_name {
            "event.release" => match self.release.as_str() {
                None => FieldValue::None,
                Some(s) => FieldValue::String(s),
            },
            "event.environment" => match self.environment.as_str() {
                None => FieldValue::None,
                Some(s) => FieldValue::String(s),
            },
            "event.user_segment" => FieldValue::None, // Not available at this time
            _ => FieldValue::None,
        }
    }
    fn get_rule_type(&self) -> RuleType {
        if let Some(ty) = self.ty.value() {
            if *ty == EventType::Transaction {
                return RuleType::Transaction;
            }
        }
        RuleType::Error
    }
}

impl FieldValueProvider for TraceContext {
    fn get_value(&self, field_name: &str) -> FieldValue {
        match field_name {
            "trace.release" => match &self.release {
                None => FieldValue::None,
                Some(s) => FieldValue::String(s.as_ref()),
            },
            "trace.environment" => match &self.environment {
                None => FieldValue::None,
                Some(s) => FieldValue::String(s.as_ref()),
            },
            "trace.user_segment" => match &self.user_segment {
                None => FieldValue::None,
                Some(s) => FieldValue::String(s.as_ref()),
            },
            _ => FieldValue::None,
        }
    }
    fn get_rule_type(&self) -> RuleType {
        RuleType::Trace
    }
}

//TODO make this more efficient
fn matches<T: FieldValueProvider>(value_provider: &T, rule: &SamplingRule, ty: RuleType) -> bool {
    if ty != rule.ty {
        return false;
    }
    rule.condition.matches(value_provider)
}

fn match_glob(rule_val: &GlobPatterns, field_val: &FieldValue) -> bool {
    match field_val {
        FieldValue::String(fv) => rule_val.is_match(fv),
        _ => false,
    }
}

fn equal(rule_val: &[String], field_val: &FieldValue) -> bool {
    match field_val {
        FieldValue::String(fv) => rule_val.iter().any(|v| v == fv),
        _ => false,
    }
}

// Note: this is horrible (we allocate strings at every comparison, when we
// move to an 'compiled' version where the rule value is already processed
// things should improve
fn str_eq_no_case(rule_val: &[String], field_val: &FieldValue) -> bool {
    match field_val {
        FieldValue::String(fv) => rule_val.iter().any(|val| unicase::eq(val.as_str(), fv)),
        _ => false,
    }
}

/// Represents the dynamic sampling configuration available to a project.
/// Note: This comes from the organization data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingConfig {
    /// The sampling rules for the project
    pub rules: Vec<SamplingRule>,
}

impl SamplingConfig {
    pub fn has_unsupported_rules(&self) -> bool {
        self.rules.iter().any(SamplingRule::is_not_supported)
    }
}

/// TraceContext created by the first Sentry SDK in the call chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// IID created by SDK to represent the current call flow
    pub trace_id: Uuid,
    /// The project key
    pub public_key: ProjectKey,
    /// the release
    #[serde(default)]
    pub release: Option<String>,
    /// the user segment
    #[serde(default)]
    pub user_segment: Option<String>,
    /// the environment
    #[serde(default)]
    pub environment: Option<String>,
}

impl TraceContext {
    /// Returns the decision of whether to sample or not a trace based on the configuration rules
    /// If None then a decision can't be made either because of an invalid of missing trace context or
    /// because no applicable sampling rule could be found.
    fn should_sample(&self, config: &SamplingConfig) -> Option<bool> {
        let rule = get_matching_rule(config, self, RuleType::Trace)?;
        let rate = pseudo_random_from_uuid(self.trace_id)?;
        Some(rate < rule.sample_rate)
    }
}

// Checks whether an event should be kept or removed by dynamic sampling
pub fn should_keep_event(
    event: &Event,
    project_state: &ProjectState,
    processing_enabled: bool,
) -> Option<bool> {
    let sampling_config = match &project_state.config.dynamic_sampling {
        None => return None, // without config there is not enough info to make up my mind
        Some(config) => config,
    };

    // when we have unsupported rules disable sampling for non processing relays
    if !processing_enabled && sampling_config.has_unsupported_rules() {
        return Some(true);
    }

    let event_id = match event.id.0 {
        None => return None, // if no eventID we can't really sample so keep everything
        Some(EventId(id)) => id,
    };

    let ty = rule_type_for_event(&event);
    if let Some(rule) = get_matching_rule(sampling_config, event, ty) {
        if let Some(random_number) = pseudo_random_from_uuid(event_id) {
            return Some(rule.sample_rate > random_number);
        }
    }
    None // if no matching rule there is not enough info to make a decision
}

// Returns the type of rule that applies to a particular event
fn rule_type_for_event(event: &Event) -> RuleType {
    if let Some(EventType::Transaction) = &event.ty.0 {
        RuleType::Transaction
    } else {
        RuleType::Error
    }
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

    let should_sample = trace_context
        // see if we should sample
        .should_sample(sampling_config)
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
/// return what is left of the envelope
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
        let fut = project.send(GetProjectState).then(move |project_state| {
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

fn get_matching_rule<'a, T>(
    config: &'a SamplingConfig,
    context: &T,
    ty: RuleType,
) -> Option<&'a SamplingRule>
where
    T: FieldValueProvider,
{
    config.rules.iter().find(|rule| matches(context, rule, ty))
}

/// Generates a pseudo random number by seeding the generator with the given id.
/// The return is deterministic, always generates the same number from the same id.
/// If there's an error in parsing the id into an UUID it will return None.
fn pseudo_random_from_uuid(id: Uuid) -> Option<f64> {
    let big_seed = id.as_u128();
    let seed: u64 = big_seed.overflowing_shr(64).0.try_into().ok()?;
    let stream: u64 = (big_seed & 0xffffffff00000000).try_into().ok()?;
    let mut generator = Pcg32::new(seed, stream);
    let dist = Uniform::new(0f64, 1f64);
    Some(generator.sample(dist))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_ron_snapshot;
    use std::str::FromStr;

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondData {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            ignore_case,
        })
    }

    fn glob(name: &str, value: &[&str]) -> RuleCondition {
        RuleCondition::Glob(GlobCondData {
            name: name.to_owned(),
            value: GlobPatterns::new(value.iter().map(|s| s.to_string()).collect()),
        })
    }

    fn and(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::And(InnerConditions { inner: conds })
    }

    fn or(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::Or(InnerConditions { inner: conds })
    }

    fn not(cond: RuleCondition) -> RuleCondition {
        RuleCondition::Not(InnerNotCondition {
            inner: Box::new(cond),
        })
    }

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let rules = [
            (
                "simple",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "glob releases",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.*"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple releases",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["2.1.1", "1.1.*"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple user segments",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["paid", "vip", "free"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "case insensitive user segments",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["ViP", "FrEe"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple user environments",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq(
                            "trace.environment",
                            &["integration", "debug", "production"],
                            true,
                        ),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "case insensitive environments",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["DeBuG", "PrOd"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "all environments",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "undefined environments",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "match no conditions",
                SamplingRule {
                    condition: and(vec![]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(matches(&tc, rule, RuleType::Trace), failure_name);
        }
    }

    #[test]
    fn test_or_combinator() {
        let rules = [
            (
                "both",
                true,
                SamplingRule {
                    condition: or(vec![
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "first",
                true,
                SamplingRule {
                    condition: or(vec![
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["all"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "second",
                true,
                SamplingRule {
                    condition: or(vec![
                        eq("trace.environment", &["prod"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "none",
                false,
                SamplingRule {
                    condition: or(vec![
                        eq("trace.environment", &["prod"], true),
                        eq("trace.user_segment", &["all"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, expected, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                matches(&tc, rule, RuleType::Trace) == *expected,
                failure_name
            );
        }
    }

    #[test]
    fn test_and_combinator() {
        let rules = [
            (
                "both",
                true,
                SamplingRule {
                    condition: and(vec![
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "first",
                false,
                SamplingRule {
                    condition: and(vec![
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["all"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "second",
                false,
                SamplingRule {
                    condition: and(vec![
                        eq("trace.environment", &["prod"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "none",
                false,
                SamplingRule {
                    condition: and(vec![
                        eq("trace.environment", &["prod"], true),
                        eq("trace.user_segment", &["all"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, expected, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                matches(&tc, rule, RuleType::Trace) == *expected,
                failure_name
            );
        }
    }

    #[test]
    fn test_not_combinator() {
        let rules = [
            (
                "not true",
                false,
                SamplingRule {
                    condition: not(eq("trace.environment", &["debug"], true)),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "not false",
                true,
                SamplingRule {
                    condition: not(eq("trace.environment", &["prod"], true)),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, expected, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                matches(&tc, rule, RuleType::Trace) == *expected,
                failure_name
            );
        }
    }

    #[test]
    // /// test various rules that do not match
    fn test_does_not_match() {
        let rules = [
            (
                "release",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.2"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "user segment",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["all"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "environment",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["prod"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "category",
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 1.0,
                    ty: RuleType::Error,
                },
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(!matches(&tc, rule, RuleType::Trace), failure_name);
        }
    }

    #[test]
    fn test_rule_condition_deserialization() {
        let serialized_rules = r#"[
        {
            "op":"eq",
            "name": "field_1",
            "value": ["UPPER","lower"],
            "ignoreCase": true
        },
        {
            "op":"eq",
            "name": "field_2",
            "value": ["UPPER","lower"]
        },
        {
            "op":"glob",
            "name": "field_3",
            "value": ["1.2.*","2.*"]
        },
        {
            "op":"not",
            "inner": {
                "op":"glob",
                "name": "field_4",
                "value": ["1.*"]
            }
        },        
        {
            "op":"and",
            "inner": [{
                "op":"glob",
                "name": "field_5",
                "value": ["2.*"]
            }]
        },        
        {
            "op":"or",
            "inner": [{
                "op":"glob",
                "name": "field_6",
                "value": ["3.*"]
            }]
        }        
        ]
        "#;
        let rules: Result<Vec<RuleCondition>, _> = serde_json::from_str(serialized_rules);
        relay_log::debug!("{:?}", rules);
        assert!(rules.is_ok());
        let rules = rules.unwrap();
        assert_ron_snapshot!(rules, @r###"
            [
              EqCondData(
                op: "eq",
                name: "field_1",
                value: [
                  "UPPER",
                  "lower",
                ],
                ignoreCase: true,
              ),
              EqCondData(
                op: "eq",
                name: "field_2",
                value: [
                  "UPPER",
                  "lower",
                ],
                ignoreCase: false,
              ),
              GlobCondData(
                op: "glob",
                name: "field_3",
                value: [
                  "1.2.*",
                  "2.*",
                ],
              ),
              InnerNotCondition(
                op: "not",
                inner: GlobCondData(
                  op: "glob",
                  name: "field_4",
                  value: [
                    "1.*",
                  ],
                ),
              ),
              InnerConditions(
                op: "and",
                inner: [
                  GlobCondData(
                    op: "glob",
                    name: "field_5",
                    value: [
                      "2.*",
                    ],
                  ),
                ],
              ),
              InnerConditions(
                op: "or",
                inner: [
                  GlobCondData(
                    op: "glob",
                    name: "field_6",
                    value: [
                      "3.*",
                    ],
                  ),
                ],
              ),
            ]"###);
    }

    #[test]
    ///Test SamplingRule deserialization
    fn test_sampling_rule_deserialization() {
        let serialized_rule = r#"{
            "condition":{
                "op":"and",
                "inner": [
                    { "op" : "glob", "name": "releases", "value":["1.1.1", "1.1.2"]}
                ]
            },                
            "sampleRate": 0.7,
            "ty": "trace"
        }"#;
        let rule: Result<SamplingRule, _> = serde_json::from_str(serialized_rule);

        assert!(rule.is_ok());
        let rule = rule.unwrap();
        assert!(approx_eq(rule.sample_rate, 0.7f64));
        assert_eq!(rule.ty, RuleType::Trace);
    }

    #[test]
    fn test_partial_trace_matches() {
        let rule = SamplingRule {
            condition: and(vec![
                eq("trace.environment", &["debug"], true),
                eq("trace.user_segment", &["vip"], true),
            ]),
            sample_rate: 1.0,
            ty: RuleType::Trace,
        };
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        assert!(
            matches(&tc, &rule, RuleType::Trace),
            "did not match with missing release"
        );

        let rule = SamplingRule {
            condition: and(vec![
                glob("trace.release", &["1.1.1"]),
                eq("trace.environment", &["debug"], true),
            ]),
            sample_rate: 1.0,
            ty: RuleType::Trace,
        };
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: None,
            environment: Some("debug".to_string()),
        };

        assert!(
            matches(&tc, &rule, RuleType::Trace),
            "did not match with missing user segment"
        );

        let rule = SamplingRule {
            condition: and(vec![
                glob("trace.release", &["1.1.1"]),
                eq("trace.user_segment", &["vip"], true),
            ]),
            sample_rate: 1.0,
            ty: RuleType::Trace,
        };
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: None,
        };

        assert!(
            matches(&tc, &rule, RuleType::Trace),
            "did not match with missing environment"
        );

        let rule = SamplingRule {
            condition: and(vec![]),
            sample_rate: 1.0,
            ty: RuleType::Trace,
        };
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user_segment: None,
            environment: None,
        };

        assert!(
            matches(&tc, &rule, RuleType::Trace),
            "did not match with missing release, user segment and environment"
        );
    }

    fn approx_eq(left: f64, right: f64) -> bool {
        let diff = left - right;
        diff < 0.001 && diff > -0.001
    }

    #[test]
    /// test that the first rule that matches is selected
    fn test_rule_precedence() {
        let rules = SamplingConfig {
            rules: vec![
                //everything specified
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 0.1,
                    ty: RuleType::Trace,
                },
                // no user segments
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.2"]),
                        eq("trace.environment", &["debug"], true),
                    ]),
                    sample_rate: 0.2,
                    ty: RuleType::Trace,
                },
                // no releases
                SamplingRule {
                    condition: and(vec![
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 0.3,
                    ty: RuleType::Trace,
                },
                // no environments
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.user_segment", &["vip"], true),
                    ]),
                    sample_rate: 0.4,
                    ty: RuleType::Trace,
                },
                // no user segments releases or environments
                SamplingRule {
                    condition: RuleCondition::And(InnerConditions { inner: vec![] }),
                    sample_rate: 0.5,
                    ty: RuleType::Trace,
                },
            ],
        };

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, RuleType::Trace);
        // complete match with first rule
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.1),
            "did not match the expected first rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.2".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, RuleType::Trace);
        // should mach the second rule because of the release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.2),
            "did not match the expected second rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.3".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, RuleType::Trace);
        // should match the third rule because of the unknown release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.3),
            "did not match the expected third rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("production".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, RuleType::Trace);
        // should match the fourth rule because of the unknown environment
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.4),
            "did not match the expected fourth rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("all".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, RuleType::Trace);
        // should match the fourth rule because of the unknown user segment
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.5),
            "did not match the expected fourth rule"
        );
    }

    #[test]
    /// Test that we can convert the full range of UUID into a pseudo random number
    fn test_id_range() {
        let highest = Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();

        let val = pseudo_random_from_uuid(highest);
        assert!(val.is_some());

        let lowest = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
        let val = pseudo_random_from_uuid(lowest);
        assert!(val.is_some());
    }

    #[test]
    /// Test that the we get the same sampling decision from the same trace id
    fn test_repeatable_sampling_decision() {
        let id = Uuid::new_v4();

        let val1 = pseudo_random_from_uuid(id);
        let val2 = pseudo_random_from_uuid(id);

        assert!(val1.is_some());
        assert_eq!(val1, val2);
    }
}
