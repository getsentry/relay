//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::convert::TryInto;

use actix::prelude::*;
use futures::{future, prelude::*};
use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::{Deserialize, Serialize};

use relay_common::{EventType, ProjectKey, Uuid};
use relay_filter::{
    has_bad_browser_extensions, is_legacy_browser, is_local_host, GlobPatterns, LegacyBrowser,
};
use relay_general::protocol::{Event, EventId};

use crate::actors::project::{GetCachedProjectState, GetProjectState, Project, ProjectState};
use crate::envelope::{Envelope, ItemType};
use std::collections::BTreeSet;

/// Defines the type of dynamic rule, i.e. to which type of events it will be applied and how.
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
    Bool(bool),
    None,
}

impl FieldValue<'_> {
    fn as_str(&self) -> Option<&str> {
        if let FieldValue::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn as_bool(&self) -> Option<&bool> {
        if let FieldValue::Bool(b) = &self {
            Some(b)
        } else {
            None
        }
    }
}

/// A condition that checks for equality
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EqCondition {
    pub name: String,
    pub value: Vec<String>,
    #[serde(default)]
    pub ignore_case: bool,
}

impl EqCondition {
    fn matches_event(&self, event: &Event) -> bool {
        self.matches(event)
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        self.matches(trace)
    }

    fn matches<T: FieldValueProvider>(&self, value_provider: &T) -> bool {
        value_provider
            .get_value(self.name.as_str())
            .as_str()
            .map_or(false, |fv| {
                if self.ignore_case {
                    self.value.iter().any(|val| unicase::eq(val.as_str(), fv))
                } else {
                    self.value.iter().any(|v| v == fv)
                }
            })
    }
}

/// A condition that uses glob matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobCondition {
    pub name: String,
    pub value: GlobPatterns,
}

impl GlobCondition {
    fn matches_event(&self, event: &Event) -> bool {
        self.matches(event)
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        self.matches(trace)
    }

    fn matches<T: FieldValueProvider>(&self, value_provider: &T) -> bool {
        value_provider
            .get_value(self.name.as_str())
            .as_str()
            .map_or(false, |fv| self.value.is_match(fv))
    }
}

/// Has some property, used for any boolean condition that doesn't need configuration.
///
/// The field provider should return a boolean signifying if it has the property or not.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HasCondition {
    pub name: String,
}

impl HasCondition {
    fn matches_event(&self, event: &Event) -> bool {
        self.matches(event)
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        self.matches(trace)
    }

    fn matches<T: FieldValueProvider>(&self, value_provider: &T) -> bool {
        value_provider
            .get_value(self.name.as_str())
            .as_bool()
            .map_or(false, |b| *b)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrCondition {
    inner: Vec<RuleCondition>,
}

impl OrCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }
    fn matches_event(&self, event: &Event) -> bool {
        self.inner.iter().any(|cond| cond.matches_event(event))
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        self.inner.iter().any(|cond| cond.matches_trace(trace))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndCondition {
    inner: Vec<RuleCondition>,
}

impl AndCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }
    fn matches_event(&self, event: &Event) -> bool {
        self.inner.iter().all(|cond| cond.matches_event(event))
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        self.inner.iter().all(|cond| cond.matches_trace(trace))
    }
}

/// Negates a wrapped condition.
///
/// This structure is used to aid the serialization of Rules.
/// See [Conditions] for further explanations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotCondition {
    inner: Box<RuleCondition>,
}

impl NotCondition {
    fn supported(&self) -> bool {
        self.inner.supported()
    }
    fn matches_event(&self, event: &Event) -> bool {
        !self.inner.matches_event(event)
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        !self.inner.matches_trace(trace)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyBrowserCondition {
    pub value: BTreeSet<LegacyBrowser>,
}

impl LegacyBrowserCondition {
    fn matches_event(&self, event: &Event) -> bool {
        is_legacy_browser(event, &self.value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CspCondition {
    pub value: Vec<String>,
}

impl CspCondition {
    fn matches_event(&self, _event: &Event) -> bool {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientIpCondition {
    pub value: Vec<String>,
}

impl ClientIpCondition {
    fn matches_event(&self, _event: &Event) -> bool {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessagesCondition {
    pub value: GlobPatterns,
}

impl ErrorMessagesCondition {
    fn matches_event(&self, _event: &Event) -> bool {
        unimplemented!()
    }
}

/// A condition from a sampling rule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "op")]
pub enum RuleCondition {
    Eq(EqCondition),
    Glob(GlobCondition),
    Or(OrCondition),
    And(AndCondition),
    Not(NotCondition),
    Has(HasCondition),
    LegacyBrowser(LegacyBrowserCondition),
    Csp(CspCondition),
    ClientIp(ClientIpCondition),
    ErrorMessages(ErrorMessagesCondition),
    #[serde(other)]
    Unsupported,
}

impl RuleCondition {
    /// Checks if Relay supports this condition (in other words if the condition had any unknown configuration
    /// which was serialized as "Unsupported" (because the configuration is either faulty or was created for a
    /// newer relay that supports some other condition types)
    fn supported(&self) -> bool {
        match self {
            RuleCondition::Unsupported => false,
            // we have a known condition
            RuleCondition::Eq(_)
            | RuleCondition::Glob(_)
            | RuleCondition::Has(_)
            | RuleCondition::LegacyBrowser(_)
            | RuleCondition::ClientIp(_)
            | RuleCondition::Csp(_) => true,
            RuleCondition::ErrorMessages(_) => true,
            // dig down for embedded conditions
            RuleCondition::And(rules) => rules.supported(),
            RuleCondition::Or(rules) => rules.supported(),
            RuleCondition::Not(rule) => rule.supported(),
        }
    }
    fn matches_event(&self, event: &Event) -> bool {
        match self {
            RuleCondition::Eq(condition) => condition.matches_event(event),
            RuleCondition::Glob(condition) => condition.matches_event(event),
            RuleCondition::Has(condition) => condition.matches_event(event),
            RuleCondition::And(conditions) => conditions.matches_event(event),
            RuleCondition::Or(conditions) => conditions.matches_event(event),
            RuleCondition::Not(condition) => condition.matches_event(event),
            RuleCondition::ClientIp(condition) => condition.matches_event(event),
            RuleCondition::LegacyBrowser(condition) => condition.matches_event(event),
            RuleCondition::Csp(condition) => condition.matches_event(event),
            RuleCondition::ErrorMessages(condition) => condition.matches_event(event),
            RuleCondition::Unsupported => false,
        }
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        match self {
            RuleCondition::Eq(condition) => condition.matches_trace(trace),
            RuleCondition::Glob(condition) => condition.matches_trace(trace),
            RuleCondition::Has(condition) => condition.matches_trace(trace),
            RuleCondition::And(conditions) => conditions.matches_trace(trace),
            RuleCondition::Or(conditions) => conditions.matches_trace(trace),
            RuleCondition::Not(condition) => condition.matches_trace(trace),
            RuleCondition::ClientIp(_)
            | RuleCondition::LegacyBrowser(_)
            | RuleCondition::Csp(_)
            | RuleCondition::ErrorMessages(_)
            | RuleCondition::Unsupported => false,
        }
    }
}

/// A sampling rule as it is deserialized from the project configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    pub condition: RuleCondition,
    pub sample_rate: f64,
    #[serde(rename = "type")]
    pub ty: RuleType,
}

impl SamplingRule {
    fn supported(&self) -> bool {
        self.condition.supported()
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
            "event.user" => FieldValue::None, // Not available at this time
            "event.is_local_ip" => FieldValue::Bool(is_local_host(&self)),
            "event.has_bad_browser_extensions" => {
                FieldValue::Bool(has_bad_browser_extensions(&self))
            }
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
            "trace.user" => match &self.user {
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
        !self.rules.iter().all(SamplingRule::supported)
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
    /// the user specific identifier ( e.g. a user segment, or similar created by the SDK
    /// from the user object)
    #[serde(default)]
    pub user: Option<String>,
    /// the environment
    #[serde(default)]
    pub environment: Option<String>,
}

impl TraceContext {
    /// Returns the decision of whether to sample or not a trace based on the configuration rules
    /// If None then a decision can't be made either because of an invalid of missing trace context or
    /// because no applicable sampling rule could be found.
    fn should_sample(&self, config: &SamplingConfig) -> Option<bool> {
        let rule = get_matching_trace_rule(config, self, RuleType::Trace)?;
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
    if let Some(rule) = get_matching_event_rule(sampling_config, event, ty) {
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

fn get_matching_event_rule<'a>(
    config: &'a SamplingConfig,
    event: &Event,
    ty: RuleType,
) -> Option<&'a SamplingRule> {
    config
        .rules
        .iter()
        .find(|rule| rule.ty == ty && rule.condition.matches_event(event))
}
fn get_matching_trace_rule<'a>(
    config: &'a SamplingConfig,
    trace: &TraceContext,
    ty: RuleType,
) -> Option<&'a SamplingRule> {
    config
        .rules
        .iter()
        .find(|rule| rule.ty == ty && rule.condition.matches_trace(trace))
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
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            ignore_case,
        })
    }

    fn glob(name: &str, value: &[&str]) -> RuleCondition {
        RuleCondition::Glob(GlobCondition {
            name: name.to_owned(),
            value: GlobPatterns::new(value.iter().map(|s| s.to_string()).collect()),
        })
    }

    fn has(name: &str) -> RuleCondition {
        RuleCondition::Has(HasCondition {
            name: name.to_owned(),
        })
    }

    fn and(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::And(AndCondition { inner: conds })
    }

    fn or(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::Or(OrCondition { inner: conds })
    }

    fn not(cond: RuleCondition) -> RuleCondition {
        RuleCondition::Not(NotCondition {
            inner: Box::new(cond),
        })
    }

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let conditions = [
            (
                "simple",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "glob releases",
                and(vec![
                    glob("trace.release", &["1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "multiple releases",
                and(vec![
                    glob("trace.release", &["2.1.1", "1.1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "multiple user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["paid", "vip", "free"], true),
                ]),
            ),
            (
                "case insensitive user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["ViP", "FrEe"], true),
                ]),
            ),
            (
                "multiple user environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq(
                        "trace.environment",
                        &["integration", "debug", "production"],
                        true,
                    ),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "case insensitive environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["DeBuG", "PrOd"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "all environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "undefined environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            ("match no conditions", and(vec![])),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(condition.matches_trace(&tc), failure_name);
        }
    }

    #[test]
    fn test_or_combinator() {
        let conditions = [
            (
                "both",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "first",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            (
                "second",
                true,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            ("empty", false, or(vec![])),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(condition.matches_trace(&tc) == *expected, failure_name);
        }
    }

    #[test]
    fn test_and_combinator() {
        let conditions = [
            (
                "both",
                true,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "first",
                false,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            (
                "second",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            ("empty", true, and(vec![])),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(condition.matches_trace(&tc) == *expected, failure_name);
        }
    }

    #[test]
    fn test_not_combinator() {
        let conditions = [
            (
                "not true",
                false,
                not(eq("trace.environment", &["debug"], true)),
            ),
            (
                "not false",
                true,
                not(eq("trace.environment", &["prod"], true)),
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(condition.matches_trace(&tc) == *expected, failure_name);
        }
    }

    #[test]
    // /// test various rules that do not match
    fn test_does_not_match() {
        let conditions = [
            (
                "release",
                and(vec![
                    glob("trace.release", &["1.1.2"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "user segment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            (
                "environment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(!condition.matches_trace(&tc), failure_name);
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
            "op":"has",
            "name": "has_field"
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
              EqCondition(
                op: "eq",
                name: "field_1",
                value: [
                  "UPPER",
                  "lower",
                ],
                ignoreCase: true,
              ),
              EqCondition(
                op: "eq",
                name: "field_2",
                value: [
                  "UPPER",
                  "lower",
                ],
                ignoreCase: false,
              ),
              GlobCondition(
                op: "glob",
                name: "field_3",
                value: [
                  "1.2.*",
                  "2.*",
                ],
              ),
              HasCondition(
                op: "has",
                name: "has_field",
              ),
              NotCondition(
                op: "not",
                inner: GlobCondition(
                  op: "glob",
                  name: "field_4",
                  value: [
                    "1.*",
                  ],
                ),
              ),
              AndCondition(
                op: "and",
                inner: [
                  GlobCondition(
                    op: "glob",
                    name: "field_5",
                    value: [
                      "2.*",
                    ],
                  ),
                ],
              ),
              OrCondition(
                op: "or",
                inner: [
                  GlobCondition(
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
            "type": "trace"
        }"#;
        let rule: Result<SamplingRule, _> = serde_json::from_str(serialized_rule);

        assert!(rule.is_ok());
        let rule = rule.unwrap();
        assert!(approx_eq(rule.sample_rate, 0.7f64));
        assert_eq!(rule.ty, RuleType::Trace);
    }

    #[test]
    fn test_partial_trace_matches() {
        let condition = and(vec![
            eq("trace.environment", &["debug"], true),
            eq("trace.user", &["vip"], true),
        ]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        assert!(
            condition.matches_trace(&tc),
            "did not match with missing release"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.environment", &["debug"], true),
        ]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: None,
            environment: Some("debug".to_string()),
        };

        assert!(
            condition.matches_trace(&tc),
            "did not match with missing user segment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user", &["vip"], true),
        ]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: None,
        };

        assert!(
            condition.matches_trace(&tc),
            "did not match with missing environment"
        );

        let condition = and(vec![]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: None,
            environment: None,
        };

        assert!(
            condition.matches_trace(&tc),
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
                        eq("trace.user", &["vip"], true),
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
                        eq("trace.user", &["vip"], true),
                    ]),
                    sample_rate: 0.3,
                    ty: RuleType::Trace,
                },
                // no environments
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.user", &["vip"], true),
                    ]),
                    sample_rate: 0.4,
                    ty: RuleType::Trace,
                },
                // no user segments releases or environments
                SamplingRule {
                    condition: RuleCondition::And(AndCondition { inner: vec![] }),
                    sample_rate: 0.5,
                    ty: RuleType::Trace,
                },
            ],
        };

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, RuleType::Trace);
        // complete match with first rule
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.1),
            "did not match the expected first rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.2".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, RuleType::Trace);
        // should mach the second rule because of the release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.2),
            "did not match the expected second rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.3".to_string()),
            user: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, RuleType::Trace);
        // should match the third rule because of the unknown release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.3),
            "did not match the expected third rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("vip".to_string()),
            environment: Some("production".to_string()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, RuleType::Trace);
        // should match the fourth rule because of the unknown environment
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.4),
            "did not match the expected fourth rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some("all".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, RuleType::Trace);
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
