//! Functionality for calculating if a trace should be processed or dropped.
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::net::IpAddr;

use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use relay_common::{EventType, ProjectKey, Uuid};
use relay_filter::GlobPatterns;
use relay_general::protocol::{Context, Event};
use relay_general::store;

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

/// The result of a sampling operation returned by [`TraceContext::should_keep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event.
    Keep,
    /// Drop the event, due to the rule with provided identifier.
    Drop(RuleId),
    /// No decision can be made.
    NoDecision,
}

/// A condition that checks the values using the equality operator.
///
/// For string values it supports case-insensitive comparison.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EqCondOptions {
    #[serde(default)]
    pub ignore_case: bool,
}

/// A condition that checks for equality
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EqCondition {
    pub name: String,
    pub value: Value,
    #[serde(default)]
    pub options: EqCondOptions,
}

impl EqCondition {
    fn matches_event(&self, event: &Event) -> bool {
        self.matches(event)
    }
    fn matches_trace(&self, trace: &TraceContext) -> bool {
        self.matches(trace)
    }

    fn matches<T: FieldValueProvider>(&self, value_provider: &T) -> bool {
        let value = value_provider.get_value(self.name.as_str());

        match value {
            Value::Null => self.value == Value::Null,
            Value::String(ref field) => match self.value {
                Value::String(ref val) => {
                    if self.options.ignore_case {
                        unicase::eq(field.as_str(), val.as_str())
                    } else {
                        field == val
                    }
                }
                Value::Array(ref val) => {
                    if self.options.ignore_case {
                        val.iter().any(|v| {
                            if let Some(v) = v.as_str() {
                                unicase::eq(v, field.as_str())
                            } else {
                                false
                            }
                        })
                    } else {
                        val.iter().any(|v| {
                            if let Some(v) = v.as_str() {
                                v == field.as_str()
                            } else {
                                false
                            }
                        })
                    }
                }
                _ => false,
            },
            Value::Bool(field) => {
                if let Value::Bool(val) = self.value {
                    field == val
                } else {
                    false
                }
            }
            _ => false, // unsupported types
        }
    }
}

macro_rules! impl_cmp_condition {
    ($struct_name:ident, $operator:tt) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct $struct_name {
            pub name: String,
            pub value: Number,
        }

        impl $struct_name {
            fn matches_event(&self, event: &Event) -> bool {
                self.matches(event)
            }
            fn matches_trace(&self, trace: &TraceContext) -> bool {
                self.matches(trace)
            }

            fn matches<T: FieldValueProvider>(&self, value_provider: &T) -> bool {
                let value = match value_provider.get_value(self.name.as_str()) {
                    Value::Number(x) => x,
                    _ => return false
                };

                // Try various conversion functions in order of expensiveness and likelihood
                // - as_i64 is not really fast, but most values in sampling rules can be i64, so we could
                //   return early
                // - f64 is more likely to succeed than u64, but we might lose precision
                if let (Some(a), Some(b)) = (value.as_i64(), self.value.as_i64()) {
                    a $operator b
                } else if let (Some(a), Some(b)) = (value.as_u64(), self.value.as_u64()) {
                    a $operator b
                } else if let (Some(a), Some(b)) = (value.as_f64(), self.value.as_f64()) {
                    a $operator b
                } else {
                    false
                }
            }
        }
    }
}

impl_cmp_condition!(GteCondition, >=);
impl_cmp_condition!(LteCondition, <=);
impl_cmp_condition!(LtCondition, <);
impl_cmp_condition!(GtCondition, >);

/// A condition that uses glob matching.
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

/// Condition that cover custom operators which need
/// special handling and have a custom implementation
/// for each case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomCondition {
    pub name: String,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub options: HashMap<String, Value>,
}

impl CustomCondition {
    fn matches_event(&self, event: &Event, ip_addr: Option<IpAddr>) -> bool {
        Event::get_custom_operator(&self.name)(self, event, ip_addr)
    }
    fn matches_trace(&self, trace: &TraceContext, ip_addr: Option<IpAddr>) -> bool {
        TraceContext::get_custom_operator(&self.name)(self, trace, ip_addr)
    }
}

/// Or condition combinator.
///
/// Creates a condition that is true when any
/// of the inner conditions are true
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrCondition {
    inner: Vec<RuleCondition>,
}

impl OrCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }
    fn matches_event(&self, event: &Event, ip_addr: Option<IpAddr>) -> bool {
        self.inner
            .iter()
            .any(|cond| cond.matches_event(event, ip_addr))
    }
    fn matches_trace(&self, trace: &TraceContext, ip_addr: Option<IpAddr>) -> bool {
        self.inner
            .iter()
            .any(|cond| cond.matches_trace(trace, ip_addr))
    }
}

/// And condition combinator.
///
/// Creates a condition that is true when all
/// inner conditions are true.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndCondition {
    inner: Vec<RuleCondition>,
}

impl AndCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }
    fn matches_event(&self, event: &Event, ip_addr: Option<IpAddr>) -> bool {
        self.inner
            .iter()
            .all(|cond| cond.matches_event(event, ip_addr))
    }
    fn matches_trace(&self, trace: &TraceContext, ip_addr: Option<IpAddr>) -> bool {
        self.inner
            .iter()
            .all(|cond| cond.matches_trace(trace, ip_addr))
    }
}

/// Not condition combinator.
///
/// Creates a condition that is true when the wrapped
/// condition si false.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotCondition {
    inner: Box<RuleCondition>,
}

impl NotCondition {
    fn supported(&self) -> bool {
        self.inner.supported()
    }
    fn matches_event(&self, event: &Event, ip_addr: Option<IpAddr>) -> bool {
        !self.inner.matches_event(event, ip_addr)
    }
    fn matches_trace(&self, trace: &TraceContext, ip_addr: Option<IpAddr>) -> bool {
        !self.inner.matches_trace(trace, ip_addr)
    }
}

/// A condition from a sampling rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "op")]
pub enum RuleCondition {
    Eq(EqCondition),
    Gte(GteCondition),
    Lte(LteCondition),
    Lt(LtCondition),
    Gt(GtCondition),
    Glob(GlobCondition),
    Or(OrCondition),
    And(AndCondition),
    Not(NotCondition),
    Custom(CustomCondition),
    #[serde(other)]
    Unsupported,
}

impl RuleCondition {
    /// Checks if Relay supports this condition (in other words if the condition had any unknown configuration
    /// which was serialized as "Unsupported" (because the configuration is either faulty or was created for a
    /// newer relay that supports some other condition types)
    pub fn supported(&self) -> bool {
        match self {
            RuleCondition::Unsupported => false,
            // we have a known condition
            RuleCondition::Gte(_)
            | RuleCondition::Lte(_)
            | RuleCondition::Gt(_)
            | RuleCondition::Lt(_)
            | RuleCondition::Eq(_)
            | RuleCondition::Glob(_) => true,
            // dig down for embedded conditions
            RuleCondition::And(rules) => rules.supported(),
            RuleCondition::Or(rules) => rules.supported(),
            RuleCondition::Not(rule) => rule.supported(),
            RuleCondition::Custom(_) => true,
        }
    }
    pub fn matches_event(&self, event: &Event, ip_addr: Option<IpAddr>) -> bool {
        match self {
            RuleCondition::Eq(condition) => condition.matches_event(event),
            RuleCondition::Lte(condition) => condition.matches_event(event),
            RuleCondition::Gte(condition) => condition.matches_event(event),
            RuleCondition::Gt(condition) => condition.matches_event(event),
            RuleCondition::Lt(condition) => condition.matches_event(event),
            RuleCondition::Glob(condition) => condition.matches_event(event),
            RuleCondition::And(conditions) => conditions.matches_event(event, ip_addr),
            RuleCondition::Or(conditions) => conditions.matches_event(event, ip_addr),
            RuleCondition::Not(condition) => condition.matches_event(event, ip_addr),
            RuleCondition::Unsupported => false,
            RuleCondition::Custom(condition) => condition.matches_event(event, ip_addr),
        }
    }
    pub fn matches_trace(&self, trace: &TraceContext, ip_addr: Option<IpAddr>) -> bool {
        match self {
            RuleCondition::Eq(condition) => condition.matches_trace(trace),
            RuleCondition::Gte(condition) => condition.matches_trace(trace),
            RuleCondition::Lte(condition) => condition.matches_trace(trace),
            RuleCondition::Gt(condition) => condition.matches_trace(trace),
            RuleCondition::Lt(condition) => condition.matches_trace(trace),
            RuleCondition::Glob(condition) => condition.matches_trace(trace),
            RuleCondition::And(conditions) => conditions.matches_trace(trace, ip_addr),
            RuleCondition::Or(conditions) => conditions.matches_trace(trace, ip_addr),
            RuleCondition::Not(condition) => condition.matches_trace(trace, ip_addr),
            RuleCondition::Unsupported => false,
            RuleCondition::Custom(condition) => condition.matches_trace(trace, ip_addr),
        }
    }
}

/// Sampling rule Id
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuleId(pub u32);

impl Display for RuleId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A sampling rule as it is deserialized from the project configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    pub condition: RuleCondition,
    pub sample_rate: f64,
    #[serde(rename = "type")]
    pub ty: RuleType,
    pub id: RuleId,
}

impl SamplingRule {
    fn supported(&self) -> bool {
        self.condition.supported()
    }
}

/// Trait implemented by providers of fields (Events and Trace Contexts).
///
/// The fields will be used by rules to check if they apply.
trait FieldValueProvider {
    /// gets the value of a field
    fn get_value(&self, path: &str) -> Value;
    /// what type of rule can be applied to this provider
    fn get_rule_type(&self) -> RuleType;
    /// returns a filtering function for custom operators.
    /// The function returned takes the provider and a condition definition and
    /// returns a match result
    fn get_custom_operator(
        name: &str,
    ) -> fn(condition: &CustomCondition, slf: &Self, ip_addr: Option<IpAddr>) -> bool;
}

fn no_match<T>(_condition: &CustomCondition, _slf: &T, _ip_addr: Option<IpAddr>) -> bool {
    false
}

impl FieldValueProvider for Event {
    fn get_value(&self, field_name: &str) -> Value {
        let field_name = match field_name.strip_prefix("event.") {
            Some(stripped) => stripped,
            None => return Value::Null,
        };

        match field_name {
            // Simple fields
            "release" => match self.release.value() {
                None => Value::Null,
                Some(s) => s.as_str().into(),
            },
            "environment" => match self.environment.value() {
                None => Value::Null,
                Some(s) => s.as_str().into(),
            },
            "transaction" => match self.transaction.value() {
                None => Value::Null,
                Some(s) => s.as_str().into(),
            },
            "platform" => match self.platform.value() {
                Some(platform) if store::is_valid_platform(platform) => {
                    Value::String(platform.clone())
                }
                _ => Value::from("other"),
            },
            "user.id" => self.user.value().map_or(Value::Null, |user| {
                user.id.value().map_or(Value::Null, |id| {
                    if id.is_empty() {
                        Value::Null // we don't serialize empty values but check it anyway
                    } else {
                        id.as_str().into()
                    }
                })
            }),
            "user.segment" => self.user.value().map_or(Value::Null, |user| {
                user.segment.value().map_or(Value::Null, |segment| {
                    if segment.is_empty() {
                        Value::Null
                    } else {
                        segment.as_str().into()
                    }
                })
            }),

            // Partial implementation of contexts.
            "contexts.device.name" => self
                .contexts
                .value()
                .and_then(|contexts| contexts.get("device"))
                .and_then(|annotated| annotated.value())
                .and_then(|context| match context.0 {
                    Context::Device(ref device) => device.name.as_str(),
                    _ => None,
                })
                .map_or(Value::Null, Value::from),
            "contexts.device.family" => self
                .contexts
                .value()
                .and_then(|contexts| contexts.get("device"))
                .and_then(|annotated| annotated.value())
                .and_then(|context| match context.0 {
                    Context::Device(ref device) => device.family.as_str(),
                    _ => None,
                })
                .map_or(Value::Null, Value::from),
            "contexts.os.name" => self
                .contexts
                .value()
                .and_then(|contexts| contexts.get("os"))
                .and_then(|annotated| annotated.value())
                .and_then(|context| match context.0 {
                    Context::Os(ref os) => os.name.as_str(),
                    _ => None,
                })
                .map_or(Value::Null, Value::from),
            "contexts.os.version" => self
                .contexts
                .value()
                .and_then(|contexts| contexts.get("os"))
                .and_then(|annotated| annotated.value())
                .and_then(|context| match context.0 {
                    Context::Os(ref os) => os.version.as_str(),
                    _ => None,
                })
                .map_or(Value::Null, Value::from),
            "contexts.trace.op" => match (self.ty.value(), store::get_transaction_op(self)) {
                (Some(&EventType::Transaction), Some(op_name)) => Value::String(op_name.to_owned()),
                _ => Value::Null,
            },

            // Computed fields (see Discover)
            "duration" => match (self.ty.value(), store::validate_timestamps(self)) {
                (Some(&EventType::Transaction), Ok((start, end))) => {
                    match Number::from_f64(relay_common::chrono_to_positive_millis(end - start)) {
                        Some(num) => Value::Number(num),
                        None => Value::Null,
                    }
                }
                _ => Value::Null,
            },

            // Inbound filter functions represented as fields
            "is_local_ip" => Value::Bool(relay_filter::localhost::matches(self)),
            "has_bad_browser_extensions" => {
                Value::Bool(relay_filter::browser_extensions::matches(self))
            }
            "web_crawlers" => Value::Bool(relay_filter::web_crawlers::matches(self)),

            // Dynamic access to certain data bags
            _ => {
                if let Some(rest) = field_name.strip_prefix("measurements.") {
                    rest.strip_suffix(".value")
                        .filter(|measurement_name| !measurement_name.is_empty())
                        .and_then(|measurement_name| store::get_measurement(self, measurement_name))
                        .map_or(Value::Null, Value::from)
                } else if let Some(rest) = field_name.strip_prefix("tags.") {
                    self.tags
                        .value()
                        .and_then(|tags| tags.get(rest))
                        .map_or(Value::Null, Value::from)
                } else {
                    Value::Null
                }
            }
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

    fn get_custom_operator(
        name: &str,
    ) -> fn(condition: &CustomCondition, slf: &Self, ip_addr: Option<IpAddr>) -> bool {
        match name {
            "event.client_ip" => client_ips_matcher,
            "event.legacy_browser" => legacy_browsers_matcher,
            "event.error_messages" => error_messages_matcher,
            "event.csp" => csp_matcher,
            _ => no_match,
        }
    }
}

fn client_ips_matcher(
    condition: &CustomCondition,
    _event: &Event,
    ip_addr: Option<IpAddr>,
) -> bool {
    let ips = condition
        .value
        .as_array()
        .map(|v| v.iter().map(|s| s.as_str().unwrap_or("")));

    if let Some(ips) = ips {
        relay_filter::client_ips::matches(ip_addr, ips)
    } else {
        false
    }
}

fn legacy_browsers_matcher(
    condition: &CustomCondition,
    event: &Event,
    _ip_addr: Option<IpAddr>,
) -> bool {
    let browsers = condition
        .value
        .as_array()
        .map(|v| v.iter().map(|s| s.as_str().unwrap_or("").parse().unwrap()));
    if let Some(browsers) = browsers {
        relay_filter::legacy_browsers::matches(event, &browsers.collect())
    } else {
        false
    }
}

fn error_messages_matcher(
    condition: &CustomCondition,
    event: &Event,
    _ip_addr: Option<IpAddr>,
) -> bool {
    let patterns = condition
        .value
        .as_array()
        .map(|v| v.iter().map(|s| s.as_str().unwrap_or("").to_owned()));

    if let Some(patterns) = patterns {
        let globs = GlobPatterns::new(patterns.collect());
        relay_filter::error_messages::matches(event, &globs)
    } else {
        false
    }
}

fn csp_matcher(condition: &CustomCondition, event: &Event, _ip_addr: Option<IpAddr>) -> bool {
    let sources = condition
        .value
        .as_array()
        .map(|v| v.iter().map(|s| s.as_str().unwrap_or("")));

    if let Some(sources) = sources {
        relay_filter::csp::matches(event, sources)
    } else {
        false
    }
}

impl FieldValueProvider for TraceContext {
    fn get_value(&self, field_name: &str) -> Value {
        match field_name {
            "trace.release" => match self.release {
                None => Value::Null,
                Some(ref s) => s.as_str().into(),
            },
            "trace.environment" => match self.environment {
                None => Value::Null,
                Some(ref s) => s.as_str().into(),
            },
            "trace.user.id" => self.user.as_ref().map_or(Value::Null, |user| {
                if user.id.is_empty() {
                    Value::Null
                } else {
                    user.id.as_str().into()
                }
            }),
            "trace.user.segment" => self.user.as_ref().map_or(Value::Null, |user| {
                if user.segment.is_empty() {
                    Value::Null
                } else {
                    user.segment.as_str().into()
                }
            }),
            "trace.transaction" => match self.transaction {
                None => Value::Null,
                Some(ref s) => s.as_str().into(),
            },
            _ => Value::Null,
        }
    }

    fn get_rule_type(&self) -> RuleType {
        RuleType::Trace
    }

    fn get_custom_operator(
        _name: &str,
    ) -> fn(condition: &CustomCondition, slf: &Self, ip_addr: Option<IpAddr>) -> bool {
        // no custom operators for trace
        no_match
    }
}

/// Represents the dynamic sampling configuration available to a project.
///
/// Note: This comes from the organization data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingConfig {
    /// The sampling rules for the project
    pub rules: Vec<SamplingRule>,
    /// The id of the next new Rule (used as a generator for unique rule ids)
    #[serde(default)]
    pub next_id: Option<u32>,
}

impl SamplingConfig {
    pub fn has_unsupported_rules(&self) -> bool {
        !self.rules.iter().all(SamplingRule::supported)
    }
}

/// The User related information in the trace context
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceUserContext {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub segment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
}

/// TraceContext created by the first Sentry SDK in the call chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// IID created by SDK to represent the current call flow
    pub trace_id: Uuid,
    /// the project key
    pub public_key: ProjectKey,
    /// the release
    #[serde(default)]
    pub release: Option<String>,
    /// the user specific identifier ( e.g. a user segment, or similar created by the SDK
    /// from the user object)
    #[serde(default)]
    pub user: Option<TraceUserContext>,
    /// the environment
    #[serde(default)]
    pub environment: Option<String>,
    /// the name of the transaction extracted from the `transaction` field in the starting transaction
    ///
    /// set on transaction start, or via `scope.transaction`
    #[serde(default)]
    pub transaction: Option<String>,
}

impl TraceContext {
    /// Returns whether a trace should be retained based on sampling rules.
    ///
    /// If [`SamplingResult::NoDecision`] is returned, then no rule matched this trace. In this
    /// case, the caller may decide whether to keep the trace or not. The same is returned if the
    /// configuration is invalid.
    pub fn should_keep(&self, ip_addr: Option<IpAddr>, config: &SamplingConfig) -> SamplingResult {
        if let Some(rule) = get_matching_trace_rule(config, self, ip_addr, RuleType::Trace) {
            let rate = pseudo_random_from_uuid(self.trace_id);

            if rate < rule.sample_rate {
                SamplingResult::Keep
            } else {
                SamplingResult::Drop(rule.id)
            }
        } else {
            SamplingResult::NoDecision
        }
    }
}

/// Returns the type of rule that applies to a particular event.
pub fn rule_type_for_event(event: &Event) -> RuleType {
    if let Some(EventType::Transaction) = &event.ty.0 {
        RuleType::Transaction
    } else {
        RuleType::Error
    }
}

/// Returns the first event rule that matches the event.
pub fn get_matching_event_rule<'a>(
    config: &'a SamplingConfig,
    event: &Event,
    ip_addr: Option<IpAddr>,
    ty: RuleType,
) -> Option<&'a SamplingRule> {
    config
        .rules
        .iter()
        .find(|rule| rule.ty == ty && rule.condition.matches_event(event, ip_addr))
}

fn get_matching_trace_rule<'a>(
    config: &'a SamplingConfig,
    trace: &TraceContext,
    ip_addr: Option<IpAddr>,
    ty: RuleType,
) -> Option<&'a SamplingRule> {
    config
        .rules
        .iter()
        .find(|rule| rule.ty == ty && rule.condition.matches_trace(trace, ip_addr))
}

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
pub fn pseudo_random_from_uuid(id: Uuid) -> f64 {
    let big_seed = id.as_u128();
    let mut generator = Pcg32::new((big_seed >> 64) as u64, big_seed as u64);
    let dist = Uniform::new(0f64, 1f64);
    generator.sample(dist)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr as NetIpAddr, Ipv4Addr};
    use std::str::FromStr;

    use insta::assert_ron_snapshot;

    use relay_general::protocol::{
        Contexts, Csp, DeviceContext, Exception, Headers, IpAddr, JsonLenientString, LenientString,
        LogEntry, OsContext, PairList, Request, TagEntry, Tags, User, Values,
    };
    use relay_general::types::Annotated;

    use super::*;

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

    fn eq_bool(name: &str, value: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: Value::Bool(value),
            options: EqCondOptions::default(),
        })
    }

    fn glob(name: &str, value: &[&str]) -> RuleCondition {
        RuleCondition::Glob(GlobCondition {
            name: name.to_owned(),
            value: GlobPatterns::new(value.iter().map(|s| s.to_string()).collect()),
        })
    }

    fn custom(name: &str, value: Value, options: HashMap<String, Value>) -> RuleCondition {
        RuleCondition::Custom(CustomCondition {
            name: name.to_owned(),
            value,
            options,
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

    /// test extraction of field values from event with everything
    #[test]
    fn test_field_value_provider_event_filled() {
        let event = Event {
            release: Annotated::new(LenientString("1.1.1".to_owned())),
            environment: Annotated::new("prod".to_owned()),
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_owned())),
                id: Annotated::new(LenientString("user-id".into())),
                segment: Annotated::new("user-seg".into()),
                ..Default::default()
            }),
            exceptions: Annotated::new(Values {
                values: Annotated::new(vec![Annotated::new(Exception {
                    value: Annotated::new(JsonLenientString::from(
                        "canvas.contentDocument".to_owned(),
                    )),
                    ..Default::default()
                })]),
                ..Default::default()
            }),
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(PairList(vec![Annotated::new((
                    Annotated::new("user-agent".into()),
                    Annotated::new("Slurp".into()),
                ))]))),
                ..Default::default()
            }),
            transaction: Annotated::new("some-transaction".into()),
            tags: {
                let items = vec![Annotated::new(TagEntry(
                    Annotated::new("custom".to_string()),
                    Annotated::new("custom-value".to_string()),
                ))];
                Annotated::new(Tags(items.into()))
            },
            contexts: Annotated::new({
                let mut contexts = Contexts::new();
                contexts.add(Context::Device(Box::new(DeviceContext {
                    name: Annotated::new("iphone".to_string()),
                    family: Annotated::new("iphone-fam".to_string()),
                    model: Annotated::new("iphone7,3".to_string()),
                    ..DeviceContext::default()
                })));
                contexts.add(Context::Os(Box::new(OsContext {
                    name: Annotated::new("iOS".to_string()),
                    version: Annotated::new("11.4.2".to_string()),
                    kernel_version: Annotated::new("17.4.0".to_string()),
                    ..OsContext::default()
                })));
                contexts
            }),
            ..Default::default()
        };

        assert_eq!(Some("1.1.1"), event.get_value("event.release").as_str());
        assert_eq!(Some("prod"), event.get_value("event.environment").as_str());
        assert_eq!(Some("user-id"), event.get_value("event.user.id").as_str());
        assert_eq!(
            Some("user-seg"),
            event.get_value("event.user.segment").as_str()
        );
        assert_eq!(Value::Bool(true), event.get_value("event.is_local_ip"),);
        assert_eq!(
            Value::Bool(true),
            event.get_value("event.has_bad_browser_extensions")
        );
        assert_eq!(Value::Bool(true), event.get_value("event.web_crawlers"));
        assert_eq!(
            Some("some-transaction"),
            event.get_value("event.transaction").as_str()
        );
        assert_eq!(
            Some("iphone"),
            event.get_value("event.contexts.device.name").as_str()
        );
        assert_eq!(
            Some("iphone-fam"),
            event.get_value("event.contexts.device.family").as_str()
        );
        assert_eq!(
            Some("iOS"),
            event.get_value("event.contexts.os.name").as_str()
        );
        assert_eq!(
            Some("11.4.2"),
            event.get_value("event.contexts.os.version").as_str()
        );
        assert_eq!(
            Some("custom-value"),
            event.get_value("event.tags.custom").as_str()
        );
        assert_eq!(Value::Null, event.get_value("event.tags.doesntexist"));
    }

    #[test]
    /// test extraction of field values from empty event
    fn test_field_value_provider_event_empty() {
        let event = Event::default();

        assert_eq!(Value::Null, event.get_value("event.release"));
        assert_eq!(Value::Null, event.get_value("event.environment"));
        assert_eq!(Value::Null, event.get_value("event.user.id"));
        assert_eq!(Value::Null, event.get_value("event.user.segment"));
        assert_eq!(Value::Bool(false), event.get_value("event.is_local_ip"),);
        assert_eq!(
            Value::Bool(false),
            event.get_value("event.has_bad_browser_extensions")
        );
        assert_eq!(Value::Bool(false), event.get_value("event.web_crawlers"));

        // now try with an empty user
        let event = Event {
            user: Annotated::new(User {
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(Value::Null, event.get_value("event.user.id"));
        assert_eq!(Value::Null, event.get_value("event.user.segment"));
        assert_eq!(Value::Null, event.get_value("event.transaction"));
    }

    #[test]
    fn test_field_value_provider_trace_filled() {
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: Some(TraceUserContext {
                segment: "user-seg".into(),
                id: "user-id".into(),
            }),
            environment: Some("prod".into()),
            transaction: Some("transaction1".into()),
        };

        assert_eq!(Value::String("1.1.1".into()), tc.get_value("trace.release"));
        assert_eq!(
            Value::String("prod".into()),
            tc.get_value("trace.environment")
        );
        assert_eq!(
            Value::String("user-id".into()),
            tc.get_value("trace.user.id")
        );
        assert_eq!(
            Value::String("user-seg".into()),
            tc.get_value("trace.user.segment")
        );
        assert_eq!(
            Value::String("transaction1".into()),
            tc.get_value("trace.transaction")
        )
    }

    #[test]
    fn test_field_value_provider_trace_empty() {
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: None,
            environment: None,
            transaction: None,
        };
        assert_eq!(Value::Null, tc.get_value("trace.release"));
        assert_eq!(Value::Null, tc.get_value("trace.environment"));
        assert_eq!(Value::Null, tc.get_value("trace.user.id"));
        assert_eq!(Value::Null, tc.get_value("trace.user.segment"));
        assert_eq!(Value::Null, tc.get_value("trace.user.transaction"));

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: Some(TraceUserContext::default()),
            environment: None,
            transaction: None,
        };
        assert_eq!(Value::Null, tc.get_value("trace.user.id"));
        assert_eq!(Value::Null, tc.get_value("trace.user.segment"));
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
                    eq("trace.user.segment", &["vip"], true),
                    eq("trace.transaction", &["transaction1"], true),
                ]),
            ),
            (
                "glob releases",
                and(vec![
                    glob("trace.release", &["1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "glob transaction",
                and(vec![glob("trace.transaction", &["trans*"])]),
            ),
            (
                "multiple releases",
                and(vec![
                    glob("trace.release", &["2.1.1", "1.1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "multiple user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["paid", "vip", "free"], true),
                ]),
            ),
            (
                "multiple transactions",
                and(vec![glob("trace.transaction", &["t22", "trans*", "t33"])]),
            ),
            (
                "case insensitive user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["ViP", "FrEe"], true),
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
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "case insensitive environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["DeBuG", "PrOd"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "all environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "undefined environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            ("match no conditions", and(vec![])),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: Some(TraceUserContext {
                segment: "vip".into(),
                id: "user-id".into(),
            }),
            environment: Some("debug".into()),
            transaction: Some("transaction1".into()),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(condition.matches_trace(&tc, None), "{}", failure_name);
        }
    }

    #[test]
    /// test matching for various rules
    fn test_matches_events() {
        let conditions = [
            ("release", and(vec![glob("event.release", &["1.1.1"])])),
            (
                "transaction",
                and(vec![glob("event.transaction", &["trans*"])]),
            ),
            (
                "environment",
                or(vec![eq("event.environment", &["prod"], true)]),
            ),
            ("local ip", eq_bool("event.is_local_ip", true)),
            (
                "bad browser extensions",
                eq_bool("event.has_bad_browser_extensions", true),
            ),
            (
                "error messages",
                custom(
                    "event.error_messages",
                    Value::Array(vec![Value::String("abc".to_string())]),
                    HashMap::new(),
                ),
            ),
            (
                "legacy browsers",
                custom(
                    "event.legacy_browser",
                    Value::Array(vec![
                        Value::String("ie10".to_string()),
                        Value::String("safari_pre_6".to_string()),
                    ]),
                    HashMap::new(),
                ),
            ),
        ];

        let evt = Event {
            release: Annotated::new(LenientString("1.1.1".to_owned())),
            environment: Annotated::new("prod".to_owned()),
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_owned())),
                ..Default::default()
            }),
            exceptions: Annotated::new(Values {
                values: Annotated::new(vec![Annotated::new(Exception {
                    value: Annotated::new(JsonLenientString::from(
                        "canvas.contentDocument".to_owned(),
                    )),
                    ..Default::default()
                })]),
                ..Default::default()
            }),
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(
                    PairList(vec![Annotated::new((
                        Annotated::new("user-agent".into()),
                        Annotated::new("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)".into()),
                    ))]))),
                ..Default::default()
            }),
            logentry: Annotated::new(LogEntry {
                formatted: Annotated::new("abc".to_owned().into()),
                ..Default::default()
            }),
            transaction: Annotated::new("transaction1".into()),
            ..Default::default()
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            let ip_addr = Some(NetIpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
            assert!(condition.matches_event(&evt, ip_addr), "{}", failure_name);
        }
    }

    #[test]
    /// test matching web crawlers
    fn test_matches_web_crawlers() {
        let condition = eq_bool("event.web_crawlers", true);

        let evt = Event {
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(PairList(vec![Annotated::new((
                    Annotated::new("user-agent".into()),
                    Annotated::new("some crawler user agent: BingBot".into()),
                ))]))),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(condition.matches_event(&evt, None));
    }

    #[test]
    /// test matching for csp
    fn test_matches_csp_events() {
        let blocked_url = "bbc.com";
        let condition = custom(
            "event.csp",
            Value::Array(vec![Value::String(blocked_url.to_owned())]),
            HashMap::new(),
        );

        let evt = Event {
            ty: Annotated::from(EventType::Csp),
            csp: Annotated::from(Csp {
                blocked_uri: Annotated::from(blocked_url.to_string()),
                ..Csp::default()
            }),
            ..Event::default()
        };
        assert!(condition.matches_event(&evt, None));
    }

    #[test]
    fn test_or_combinator() {
        let conditions = [
            (
                "both",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                true,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", false, or(vec![])),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                condition.matches_trace(&tc, None) == *expected,
                "{}",
                failure_name
            );
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
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                false,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", true, and(vec![])),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                condition.matches_trace(&tc, None) == *expected,
                "{}",
                failure_name
            );
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
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                condition.matches_trace(&tc, None) == *expected,
                "{}",
                failure_name
            );
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
            (
                "transaction",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    glob("trace.transaction", &["t22"]),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
        ];

        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(!condition.matches_trace(&tc, None), "{}", failure_name);
        }
    }

    #[test]
    fn test_rule_condition_deserialization() {
        let serialized_rules = r#"[
        {
            "op":"eq",
            "name": "field_1",
            "value": ["UPPER","lower"],
            "options":{
                "ignoreCase": true
            }
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
        },
        {
            "op":"custom",
            "name": "some_custom_op",
            "value":["default","ie_pre_9"],
            "options": { "o1": [1,2,3]}
        },
        {
            "op": "custom",
            "name": "some_custom_op",
            "options": {"o1":[1,2,3]}
        },
        {
            "op":"custom",
            "name": "some_custom_op",
            "value": "some val"
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
                options: EqCondOptions(
                  ignoreCase: true,
                ),
              ),
              EqCondition(
                op: "eq",
                name: "field_2",
                value: [
                  "UPPER",
                  "lower",
                ],
                options: EqCondOptions(
                  ignoreCase: false,
                ),
              ),
              GlobCondition(
                op: "glob",
                name: "field_3",
                value: [
                  "1.2.*",
                  "2.*",
                ],
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
              CustomCondition(
                op: "custom",
                name: "some_custom_op",
                value: [
                  "default",
                  "ie_pre_9",
                ],
                options: {
                  "o1": [
                    1,
                    2,
                    3,
                  ],
                },
              ),
              CustomCondition(
                op: "custom",
                name: "some_custom_op",
                value: (),
                options: {
                  "o1": [
                    1,
                    2,
                    3,
                  ],
                },
              ),
              CustomCondition(
                op: "custom",
                name: "some_custom_op",
                value: "some val",
                options: {},
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
            "type": "trace",
            "id": 1
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
            eq("trace.user.segment", &["vip"], true),
        ]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        assert!(
            condition.matches_trace(&tc, None),
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
            transaction: Some("transaction1".into()),
        };

        assert!(
            condition.matches_trace(&tc, None),
            "did not match with missing user segment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: None,
            transaction: Some("transaction1".into()),
        };

        assert!(
            condition.matches_trace(&tc, None),
            "did not match with missing environment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: None,
        };

        assert!(
            condition.matches_trace(&tc, None),
            "did not match with missing transaction"
        );
        let condition = and(vec![]);
        let tc = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: None,
            environment: None,
            transaction: None,
        };

        assert!(
            condition.matches_trace(&tc, None),
            "did not match with missing release, user segment, environment and transaction"
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
                        eq("trace.user.segment", &["vip"], true),
                    ]),
                    sample_rate: 0.1,
                    ty: RuleType::Trace,
                    id: RuleId(1),
                },
                // no user segments
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.2"]),
                        eq("trace.environment", &["debug"], true),
                    ]),
                    sample_rate: 0.2,
                    ty: RuleType::Trace,
                    id: RuleId(2),
                },
                // no releases
                SamplingRule {
                    condition: and(vec![
                        eq("trace.environment", &["debug"], true),
                        eq("trace.user.segment", &["vip"], true),
                    ]),
                    sample_rate: 0.3,
                    ty: RuleType::Trace,
                    id: RuleId(3),
                },
                // no environments
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.user.segment", &["vip"], true),
                    ]),
                    sample_rate: 0.4,
                    ty: RuleType::Trace,
                    id: RuleId(4),
                },
                // no user segments releases or environments
                SamplingRule {
                    condition: RuleCondition::And(AndCondition { inner: vec![] }),
                    sample_rate: 0.5,
                    ty: RuleType::Trace,
                    id: RuleId(5),
                },
            ],
            next_id: None,
        };

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, None, RuleType::Trace);
        // complete match with first rule
        assert_eq!(
            result.unwrap().id,
            RuleId(1),
            "did not match the expected first rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.2".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, None, RuleType::Trace);
        // should mach the second rule because of the release
        assert_eq!(
            result.unwrap().id,
            RuleId(2),
            "did not match the expected second rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.3".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, None, RuleType::Trace);
        // should match the third rule because of the unknown release
        assert_eq!(
            result.unwrap().id,
            RuleId(3),
            "did not match the expected third rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "vip".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("production".to_string()),
            transaction: Some("transaction1".into()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, None, RuleType::Trace);
        // should match the fourth rule because of the unknown environment
        assert_eq!(
            result.unwrap().id,
            RuleId(4),
            "did not match the expected fourth rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Some(TraceUserContext {
                segment: "all".to_owned(),
                id: "user-id".to_owned(),
            }),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
        };

        let result = get_matching_trace_rule(&rules, &trace_context, None, RuleType::Trace);
        // should match the fourth rule because of the unknown user segment
        assert_eq!(
            result.unwrap().id,
            RuleId(5),
            "did not match the expected fourth rule"
        );
    }

    #[test]
    /// Test that we can convert the full range of UUID into a number without panicking
    fn test_id_range() {
        let highest = Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
        pseudo_random_from_uuid(highest);
        let lowest = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
        pseudo_random_from_uuid(lowest);
    }

    #[test]
    /// Test that the we get the same sampling decision from the same trace id
    fn test_repeatable_sampling_decision() {
        let id = Uuid::from_str("4a106cf6-b151-44eb-9131-ae7db1a157a3").unwrap();

        let val1 = pseudo_random_from_uuid(id);
        let val2 = pseudo_random_from_uuid(id);
        assert!(val1 + f64::EPSILON > val2 && val2 + f64::EPSILON > val1);
    }
}
