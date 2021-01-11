//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::convert::TryInto;
use std::fmt;

use actix::prelude::*;
use futures::{future, prelude::*};
use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};

use relay_common::{EventType, ProjectId, ProjectKey, Uuid};
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
    //TODO find a better name for this
    /// A non transaction rule applies to Errors, Security events...every type of event that
    /// is not a Transaction
    NonTransaction,
}

/// The value of a field extracted from a FieldValueProvider (Event or TraceContext)
#[derive(Debug, Clone)]
pub enum FieldValue<'a> {
    String(&'a str),
    LoCaseString(LowerCaseString),
    None,
}

/// The value kept in a configuration rule
/// Note 1: this is serialized as an untagged struct so it will never
/// deserialize into a LoCaseStrList, LoCaseStr.
/// There should be a step after deserialization to convert StrList into
/// either a LoCaseStrList or a GlobPatterns based on the existing operator.
/// This is done to both simplify the serialisation format (no tag) and not
/// to have redundant (potentially contradicting) information between the
/// RuleValue and the Operator
/// Note 2: We bother in the first place with LoCaseStr and GlobPatterns in order
/// not to need to do the conversion every time we check a rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum RuleValue {
    StrList(Vec<String>),
    LoCaseStrList(Vec<LowerCaseString>),
    Globs(GlobPatterns),
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionData<T> {
    pub name: String,
    pub value: T,
}

/// A condition from a sampling rule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "operator")]
pub enum RuleCondition {
    Equal(ConditionData<Vec<String>>),
    StrEqualNoCase(ConditionData<Vec<LowerCaseString>>),
    GlobMatch(ConditionData<GlobPatterns>),
    #[serde(other)]
    Unsupported,
}

/// A sampling rule as it is deserialized from the project
/// configuration  
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    pub project_ids: Vec<ProjectId>,
    pub conditions: Vec<RuleCondition>,
    pub sample_rate: f64,
    pub ty: RuleType,
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
                Some(s) => FieldValue::LoCaseString(LowerCaseString::new(s)),
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
        RuleType::NonTransaction
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
                Some(s) => FieldValue::LoCaseString(LowerCaseString::new(s)),
            },
            "trace.user_segment" => match &self.user_segment {
                None => FieldValue::None,
                Some(s) => FieldValue::LoCaseString(LowerCaseString::new(s)),
            },
            _ => FieldValue::None,
        }
    }
    fn get_rule_type(&self) -> RuleType {
        RuleType::Trace
    }
}

//TODO make this more efficient
fn matches<T: FieldValueProvider>(
    value_provider: &T,
    project_id: ProjectId,
    rule: &SamplingRule,
    ty: RuleType,
) -> bool {
    if ty != rule.ty {
        return false;
    }
    if rule.project_ids.len() != 0 && !rule.project_ids.iter().any(|id| *id == project_id) {
        return false;
    }
    for cond in &rule.conditions {
        let passes = match cond {
            RuleCondition::Equal(cond) => {
                if cond.value.is_empty() {
                    continue;
                }
                equal(&cond.value, &value_provider.get_value(cond.name.as_str()))
            }
            RuleCondition::GlobMatch(cond) => {
                if cond.value.is_empty() {
                    continue;
                }
                match_glob(&cond.value, &value_provider.get_value(cond.name.as_str()))
            }
            RuleCondition::StrEqualNoCase(cond) => {
                if cond.value.is_empty() {
                    continue;
                }
                str_eq_no_case(&cond.value, &value_provider.get_value(cond.name.as_str()))
            }
            _ => false,
        };
        if !passes {
            return false;
        }
    }
    true
}

fn match_glob(rule_val: &GlobPatterns, field_val: &FieldValue) -> bool {
    match field_val {
        FieldValue::String(fv) => rule_val.is_match(fv),
        _ => false,
    }
}

fn equal(rule_val: &Vec<String>, field_val: &FieldValue) -> bool {
    match field_val {
        FieldValue::String(fv) => rule_val.iter().any(|v| v == fv),
        _ => false,
    }
}

// Note: this is horrible (we allocate strings at every comparison, when we
// move to an 'compiled' version where the rule value is already processed
// things should improve
fn str_eq_no_case(rule_val: &Vec<LowerCaseString>, field_val: &FieldValue) -> bool {
    match field_val {
        FieldValue::LoCaseString(fv) => rule_val.iter().any(|val| val == fv),
        _ => false,
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LowerCaseString(String);

impl LowerCaseString {
    pub fn new(val: &str) -> LowerCaseString {
        LowerCaseString(val.to_lowercase())
    }
}

impl From<&str> for LowerCaseString {
    fn from(val: &str) -> Self {
        LowerCaseString(val.to_lowercase())
    }
}

impl PartialEq for LowerCaseString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for LowerCaseString {}

struct LowerCaseStringVisitor;

impl<'de> Visitor<'de> for LowerCaseStringVisitor {
    type Value = LowerCaseString;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string to be converted to a lowercase string ")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(LowerCaseString::new(v))
    }
}

impl<'de> Deserialize<'de> for LowerCaseString {
    fn deserialize<D>(deserializer: D) -> Result<LowerCaseString, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(LowerCaseStringVisitor)
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
    fn should_sample(&self, config: &SamplingConfig, project_id: ProjectId) -> Option<bool> {
        let rule = get_matching_rule(config, self, project_id, RuleType::Trace)?;
        let rate = pseudo_random_from_uuid(self.trace_id)?;
        Some(rate < rule.sample_rate)
    }
}

// Checks whether an event should be kept or removed by dynamic sampling
pub fn should_keep_event(
    event: &Event,
    project_state: &ProjectState,
    project_id: ProjectId,
) -> Option<bool> {
    let sampling_config = match &project_state.config.sampling {
        None => return None, // without config there is not enough info to make up my mind
        Some(config) => config,
    };

    let event_id = match event.id.0 {
        None => return None, // if no eventID we can't really sample so keep everything
        Some(EventId(id)) => id,
    };

    let ty = rule_type_for_event(&event);
    if let Some(rule) = get_matching_rule(sampling_config, event, project_id, ty) {
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
        RuleType::NonTransaction
    }
}

/// Takes an envelope and potentially removes the transaction item from it if that
/// transaction item should be sampled out according to the dynamic sampling configuration
/// and the trace context.
fn sample_transaction_internal(
    mut envelope: Envelope,
    project_state: Option<&ProjectState>,
) -> Envelope {
    let project_state = match project_state {
        None => return envelope,
        Some(project_state) => project_state,
    };

    let sampling_config = match project_state.config.sampling {
        // without sampling config we cannot sample transactions so give up here
        None => return envelope,
        Some(ref sampling_config) => sampling_config,
    };

    let project_id = match project_state.project_id {
        None => return envelope,
        Some(project_id) => project_id,
    };

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
        .should_sample(sampling_config, project_id)
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
        let fut = project.send(GetCachedProjectState).then(|project_state| {
            let project_state = match project_state {
                // error getting the project, give up and return envelope unchanged
                Err(_) => return Ok(envelope),
                Ok(project_state) => project_state,
            };
            Ok(sample_transaction_internal(
                envelope,
                project_state.as_deref(),
            ))
        });
        Box::new(fut) as ResponseFuture<_, _>
    } else {
        let fut = project.send(GetProjectState).then(|project_state| {
            let project_state = match project_state {
                // error getting the project, give up and return envelope unchanged
                Err(_) => return Ok(envelope),
                Ok(project_state) => project_state,
            };
            Ok(sample_transaction_internal(
                envelope,
                project_state.ok().as_deref(),
            ))
        });
        Box::new(fut) as ResponseFuture<_, _>
    }
}

fn get_matching_rule<'a, T>(
    config: &'a SamplingConfig,
    context: &T,
    project_id: ProjectId,
    ty: RuleType,
) -> Option<&'a SamplingRule>
where
    T: FieldValueProvider,
{
    config
        .rules
        .iter()
        .find(|rule| matches(context, project_id, rule, ty))
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
    use relay_log;
    use std::str::FromStr;

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let project_id = ProjectId::new(22);
        let project_id2 = ProjectId::new(23);
        let project_id3 = ProjectId::new(24);
        let rules = [
            (
                "simple",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple projects",
                SamplingRule {
                    project_ids: vec![project_id2, project_id, project_id3],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "all projects",
                SamplingRule {
                    project_ids: vec![],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "glob releases",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.*".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple releases",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec![
                                "2.1.1".to_string(),
                                "1.1.*".to_string(),
                            ]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple user segments",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![
                                LowerCaseString::new("paid"),
                                LowerCaseString::new("vip"),
                                LowerCaseString::new("free"),
                            ]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "case insensitive user segments",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![
                                LowerCaseString::new("ViP"),
                                LowerCaseString::new("FrEe"),
                            ]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "multiple user environments",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![
                                LowerCaseString::new("integration"),
                                LowerCaseString::new("debug"),
                                LowerCaseString::new("production"),
                            ]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "case insensitive environments",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![
                                LowerCaseString::new("DeBuG"),
                                LowerCaseString::new("PrOd"),
                            ]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "all user environments",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "match all",
                SamplingRule {
                    project_ids: vec![],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![]),
                        }),
                    ],
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
            assert!(
                matches(&tc, project_id, rule, RuleType::Trace),
                failure_name
            );
        }
    }

    #[test]
    // /// test various rules that do not match
    fn test_not_matches() {
        let project_id = ProjectId::new(22);
        let project_id2 = ProjectId::new(23);
        let rules = [
            (
                "project id",
                SamplingRule {
                    project_ids: vec![project_id2],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "release",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.2".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "user segment",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("all")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "environment",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("prod")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::Trace,
                },
            ),
            (
                "category",
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 1.0,
                    ty: RuleType::NonTransaction,
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
            assert!(
                !matches(&tc, project_id, rule, RuleType::Trace),
                failure_name
            );
        }
    }

    #[test]
    fn test_rule_condition_deserialization() {
        let serialized_rules = r#"[
        {
            "operator":"equal",
            "name": "field_1",
            "value": ["UPPER","lower"]
        },
        {
            "operator":"strEqualNoCase",
            "name": "field_2",
            "value": ["UPPER","lower"]
        },
        {
            "operator":"globMatch",
            "name": "field_3",
            "value": ["1.2.*","2.*"]
        }
        ]
        "#;
        let rules: Result<Vec<RuleCondition>, _> = serde_json::from_str(serialized_rules);
        relay_log::debug!("{:?}", rules);
        assert!(rules.is_ok());
        let rules = rules.unwrap();
        assert_ron_snapshot!(rules, @r###"
            [
              ConditionData(
                operator: "equal",
                name: "field_1",
                value: [
                  "UPPER",
                  "lower",
                ],
              ),
              ConditionData(
                operator: "strEqualNoCase",
                name: "field_2",
                value: [
                  LowerCaseString("upper"),
                  LowerCaseString("lower"),
                ],
              ),
              ConditionData(
                operator: "globMatch",
                name: "field_3",
                value: [
                  "1.2.*",
                  "2.*",
                ],
              ),
            ]"###);
    }

    #[test]
    ///Test SamplingRuleOld deserialization
    fn test_sampling_rule_deserialization() {
        let serialized_rule = r#"{
            "projectIds": [1,2],
            "conditions":[
                { "operator" : "match", "name": "releases", "value":["1.1.1", "1.1.2"]},
                { "operator" : "strEqualNoCase", "name": "enviroments", "value":["DeV", "pRoD"]},
                { "operator" : "strEqualNoCase", "name": "userSegements", "value":["FirstSegment", "SeCoNd"]}
            ],                
            "sampleRate": 0.7,
            "ty": "trace"
        }"#;
        let rule: Result<SamplingRule, _> = serde_json::from_str(serialized_rule);

        assert!(rule.is_ok());
        let rule = rule.unwrap();
        assert_eq!(rule.project_ids, [ProjectId::new(1), ProjectId::new(2)]);
        assert!(approx_eq(rule.sample_rate, 0.7f64));
        assert_eq!(rule.ty, RuleType::Trace);
    }

    #[test]
    /// Test LowerCaseString deserialization
    fn test_sampling_lower_case_string_deserialization() {
        let some_string = r#""FiRsTSEGment""#;

        let lower_case: Result<LowerCaseString, _> = serde_json::from_str(some_string);

        assert!(lower_case.is_ok());
        let lower_case = lower_case.unwrap();
        assert_eq!(lower_case, LowerCaseString::new("firstsegment"));
    }

    #[test]
    fn test_partial_trace_matches() {
        let project_id = ProjectId::new(22);

        let rule = SamplingRule {
            project_ids: vec![project_id],
            conditions: vec![
                RuleCondition::GlobMatch(ConditionData {
                    name: "trace.release".to_owned(),
                    value: GlobPatterns::new(vec![]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.environment".to_owned(),
                    value: (vec![LowerCaseString::new("debug")]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.user_segment".to_owned(),
                    value: (vec![LowerCaseString::new("vip")]),
                }),
            ],
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
            matches(&tc, project_id, &rule, RuleType::Trace),
            "did not match with missing release"
        );

        let rule = SamplingRule {
            project_ids: vec![project_id],
            conditions: vec![
                RuleCondition::GlobMatch(ConditionData {
                    name: "trace.release".to_owned(),
                    value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.environment".to_owned(),
                    value: (vec![LowerCaseString::new("debug")]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.user_segment".to_owned(),
                    value: (vec![]),
                }),
            ],
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
            matches(&tc, project_id, &rule, RuleType::Trace),
            "did not match with missing user segment"
        );

        let rule = SamplingRule {
            project_ids: vec![project_id],
            conditions: vec![
                RuleCondition::GlobMatch(ConditionData {
                    name: "trace.release".to_owned(),
                    value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.environment".to_owned(),
                    value: (vec![]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.user_segment".to_owned(),
                    value: (vec![LowerCaseString::new("vip")]),
                }),
            ],
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
            matches(&tc, project_id, &rule, RuleType::Trace),
            "did not match with missing environment"
        );

        let rule = SamplingRule {
            project_ids: vec![project_id],
            conditions: vec![
                RuleCondition::GlobMatch(ConditionData {
                    name: "trace.release".to_owned(),
                    value: GlobPatterns::new(vec![]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.environment".to_owned(),
                    value: (vec![]),
                }),
                RuleCondition::StrEqualNoCase(ConditionData {
                    name: "trace.user_segment".to_owned(),
                    value: (vec![]),
                }),
            ],
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
            matches(&tc, project_id, &rule, RuleType::Trace),
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
        let project_id = ProjectId::new(22);

        let rules = SamplingConfig {
            rules: vec![
                //everything specified
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 0.1,
                    ty: RuleType::Trace,
                },
                // no user segments
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.2".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![]),
                        }),
                    ],
                    sample_rate: 0.2,
                    ty: RuleType::Trace,
                },
                // no releases
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![LowerCaseString::new("debug")]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 0.3,
                    ty: RuleType::Trace,
                },
                // no environments
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec!["1.1.1".to_string()]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![LowerCaseString::new("vip")]),
                        }),
                    ],
                    sample_rate: 0.4,
                    ty: RuleType::Trace,
                },
                // no user segments releases or environments
                SamplingRule {
                    project_ids: vec![project_id],
                    conditions: vec![
                        RuleCondition::GlobMatch(ConditionData {
                            name: "trace.release".to_owned(),
                            value: GlobPatterns::new(vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.environment".to_owned(),
                            value: (vec![]),
                        }),
                        RuleCondition::StrEqualNoCase(ConditionData {
                            name: "trace.user_segment".to_owned(),
                            value: (vec![]),
                        }),
                    ],
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

        let result = get_matching_rule(&rules, &trace_context, project_id, RuleType::Trace);
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

        let result = get_matching_rule(&rules, &trace_context, project_id, RuleType::Trace);
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

        let result = get_matching_rule(&rules, &trace_context, project_id, RuleType::Trace);
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

        let result = get_matching_rule(&rules, &trace_context, project_id, RuleType::Trace);
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

        let result = get_matching_rule(&rules, &trace_context, project_id, RuleType::Trace);
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
