//! Sampling logic for performing sampling decisions of incoming events.
//!
//! In order to allow Sentry to offer performance at scale, Relay extracts key `metrics` from
//! all transactions, but only forwards a random sample of raw transaction payloads to the upstream.
//! What exact percentage is sampled is determined by `dynamic sampling rules`, and depends on
//! the project, the environment, the transaction name, etc.
//!
//! In order to determine the sample rate, Relay uses a [`SamplingConfig`] which contains a set of
//! [`SamplingRule`]s that are matched against the incoming [`Event`] or [`DynamicSamplingContext`].
//!
//! # Trace and Transaction Sampling
//!
//! Relay samples both transactions looking at [`Event`] and traces looking at [`DynamicSamplingContext`]:
//! - **Trace sampling**: ensures that either all transactions of a trace are sampled or none.
//! - **Transaction sampling**: does not guarantee complete traces and instead applies to individual transactions.
//!
//! # Components
//!
//! The sampling system implemented in Relay is composed of the following components:
//! - [`DynamicSamplingContext`]: a struct that contains the trace information.
//! - [`FieldValueProvider`]: an abstraction implemented by [`Event`] and [`DynamicSamplingContext`] to
//! expose fields that are read during matching.
//! - [`SamplingRule`]: a rule that is matched against [`Event`] or [`DynamicSamplingContext`] that
//! can contain a [`RuleCondition`] for expressing predicates on the incoming payload.
//! - [`SamplingMatch`]: the result of the matching of one or more [`SamplingRule`].
//!
//! # How It Works
//! - The incoming [`Event`] and optionally [`DynamicSamplingContext`] are received by Relay.
//! - Relay fetches the [`SamplingConfig`] of the project to which the [`Event`] belongs and (if exists) the
//! [`SamplingConfig`] of the root project of the trace.
//! - The [`SamplingConfig`]s are merged together and the matching algorithm in
//! [`SamplingMatch::match_against_rules`] is executed.
//! - The sampling algorithm will go over each [`SamplingRule`] and compute either a factor or
//! sample rate based on the [`SamplingValue`] of the rule.
//! - The [`SamplingMatch`] is finally returned containing the final `sample_rate` and some additional
//! data that will be used in `relay_server` to perform the sampling decision.
//!
//! # Sampling Determinism
//! The concept of determinism is extremely important for sampling. We want to be able to make the
//! a deterministic sampling decision for a wide variety of reasons, including:
//! - Across a **chain of Relays** (e.g., we don't want to drop an event that was retained by a previous
//! Relay and vice-versa).
//! - Across **transactions of the same trace** (e.g., we want to be able to sample all the transactions
//! of the same trace, even though some exceptions apply).
//!
//! In order to perform deterministic sampling, we use the id of the event or trace as the seed
//! for the random number generator (e.g., all transactions with the same trace id will have the same
//! random number being generated). _Since we allow the matching of both transaction and trace rules, we might
//! end up in cases in which we perform inconsistent trace sampling but this is something we decided
//! to live with as long as there are no big implications on the product._
//!
//! # Examples
//!
//! ## [`SamplingConfig`]
//!
//! ```json
#![doc = include_str!("../tests/fixtures/sampling_config.json")]
//! ```
//!
//! ## [`DynamicSamplingContext`]
//!
//! ```json
#![doc = include_str!("../tests/fixtures/dynamic_sampling_context.json")]
//! ```
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

extern crate core;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::num::ParseIntError;

use chrono::{DateTime, Utc};
use rand::distributions::Uniform;
use rand::Rng;
use rand_pcg::Pcg32;
use relay_protocol::Annotated;
use sentry_release_parser::Release;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Number, Value};

use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectKey;
use relay_common::glob3::GlobPatterns;
use relay_common::time;
use relay_common::uuid::Uuid;
use relay_event_schema::protocol::{
    BrowserContext, DeviceContext, Event, OsContext, ResponseContext, Span, TraceContext,
};

/// Defines the type of dynamic rule, i.e. to which type of events it will be applied and how.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum RuleType {
    /// A trace rule applies only to transactions and it is applied on the trace info
    Trace,
    /// A transaction rule applies to transactions and it is applied  on the transaction event
    Transaction,
    // If you add a new `RuleType` that is not supposed to sample transactions, you need to edit the
    // `sample_envelope` function in `EnvelopeProcessorService`.
    /// If the sampling config contains new rule types, do not sample at all.
    #[serde(other)]
    Unsupported,
}

/// A condition that checks the values using the equality operator.
///
/// For string values it supports case-insensitive comparison.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EqCondOptions {
    #[serde(default)]
    pub ignore_case: bool,
}

/// A condition that checks for equality
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EqCondition {
    pub name: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "is_default")]
    pub options: EqCondOptions,
}

impl EqCondition {
    fn matches<T>(&self, value_provider: &T) -> bool
    where
        T: FieldValueProvider,
    {
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
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        pub struct $struct_name {
            pub name: String,
            pub value: Number,
        }

        impl $struct_name {
            fn matches<T>(&self, value_provider: &T) -> bool where T: FieldValueProvider{
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobCondition {
    pub name: String,
    pub value: GlobPatterns,
}

impl GlobCondition {
    fn matches<T>(&self, value_provider: &T) -> bool
    where
        T: FieldValueProvider,
    {
        value_provider
            .get_value(self.name.as_str())
            .as_str()
            .map_or(false, |fv| self.value.is_match(fv))
    }
}

/// Or condition combinator.
///
/// Creates a condition that is true when any
/// of the inner conditions are true
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrCondition {
    inner: Vec<RuleCondition>,
}

impl OrCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }

    fn matches<T>(&self, value: &T) -> bool
    where
        T: FieldValueProvider,
    {
        self.inner.iter().any(|cond| cond.matches(value))
    }
}

/// And condition combinator.
///
/// Creates a condition that is true when all
/// inner conditions are true.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AndCondition {
    inner: Vec<RuleCondition>,
}

impl AndCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }
    fn matches<T>(&self, value: &T) -> bool
    where
        T: FieldValueProvider,
    {
        self.inner.iter().all(|cond| cond.matches(value))
    }
}

/// Not condition combinator.
///
/// Creates a condition that is true when the wrapped
/// condition si false.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotCondition {
    inner: Box<RuleCondition>,
}

impl NotCondition {
    fn supported(&self) -> bool {
        self.inner.supported()
    }

    fn matches<T>(&self, value: &T) -> bool
    where
        T: FieldValueProvider,
    {
        !self.inner.matches(value)
    }
}

/// A condition from a sampling rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(other)]
    Unsupported,
}

impl RuleCondition {
    /// Returns a condition that matches everything.
    pub fn all() -> Self {
        Self::And(AndCondition { inner: Vec::new() })
    }

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
        }
    }

    pub fn matches<T>(&self, value: &T) -> bool
    where
        T: FieldValueProvider,
    {
        match self {
            RuleCondition::Eq(condition) => condition.matches(value),
            RuleCondition::Lte(condition) => condition.matches(value),
            RuleCondition::Gte(condition) => condition.matches(value),
            RuleCondition::Gt(condition) => condition.matches(value),
            RuleCondition::Lt(condition) => condition.matches(value),
            RuleCondition::Glob(condition) => condition.matches(value),
            RuleCondition::And(conditions) => conditions.matches(value),
            RuleCondition::Or(conditions) => conditions.matches(value),
            RuleCondition::Not(condition) => condition.matches(value),
            RuleCondition::Unsupported => false,
        }
    }
}

/// The id of the [`SamplingRule`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuleId(pub u32);

impl fmt::Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A range of time.
///
/// The time range should be applicable between the start time, inclusive, and
/// end time, exclusive. There aren't any explicit checks to ensure the end
/// time is equal to or greater than the start time; the time range isn't valid
/// in such cases.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

impl TimeRange {
    /// Returns true if neither the start nor end time limits are set.
    pub fn is_empty(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }

    /// Returns whether the provided time matches the time range.
    ///
    /// For a time to match a time range, the following conditions must match:
    /// - The start time must be smaller than or equal to the given time, if provided.
    /// - The end time must be greater than the given time, if provided.
    ///
    /// If one of the limits isn't provided, the range is considered open in
    /// that limit. A time range open on both sides matches with any given time.
    pub fn contains(&self, time: DateTime<Utc>) -> bool {
        self.start.map_or(true, |s| s <= time) && self.end.map_or(true, |e| time < e)
    }
}

/// A sampling strategy definition.
///
/// A sampling strategy refers to the strategy that we want to use for sampling a specific rule.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum SamplingValue {
    /// A rule with a sample rate will be matched and the final sample rate will be computed by
    /// multiplying its sample rate with the accumulated factors from previous rules.
    SampleRate { value: f64 },
    /// A rule with a factor will be matched and the matching will continue onto the next rules until
    /// a sample rate rule is found. The matched rule's factor will be multiplied with the accumulated
    /// factors before moving onto the next possible match.
    Factor { value: f64 },
}

impl SamplingValue {
    fn value(&self) -> f64 {
        *match self {
            SamplingValue::SampleRate { value: sample_rate } => sample_rate,
            SamplingValue::Factor { value: factor } => factor,
        }
    }
}

/// A decaying function definition.
///
/// A decaying function is responsible of decaying the sample rate from a value to another following
/// a given curve.
#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum DecayingFunction {
    #[serde(rename_all = "camelCase")]
    Linear { decayed_value: f64 },
    #[default]
    Constant,
}

/// A struct representing the evaluation context of a sample rate.
#[derive(Debug, Clone, Copy)]
enum SamplingValueEvaluator {
    Linear {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        initial_value: f64,
        decayed_value: f64,
    },
    Constant {
        initial_value: f64,
    },
}

impl SamplingValueEvaluator {
    /// Evaluates the value of the sampling strategy given a the current time.
    fn evaluate(&self, now: DateTime<Utc>) -> f64 {
        match self {
            SamplingValueEvaluator::Linear {
                start,
                end,
                initial_value,
                decayed_value,
            } => {
                let now_timestamp = now.timestamp() as f64;
                let start_timestamp = start.timestamp() as f64;
                let end_timestamp = end.timestamp() as f64;
                let progress_ratio = ((now_timestamp - start_timestamp)
                    / (end_timestamp - start_timestamp))
                    .clamp(0.0, 1.0);

                // This interval will always be < 0.
                let interval = decayed_value - initial_value;
                initial_value + (interval * progress_ratio)
            }
            SamplingValueEvaluator::Constant { initial_value } => *initial_value,
        }
    }
}

/// A sampling rule that has been successfully matched and that contains all the required data
/// to return the sample rate.
#[derive(Debug, Clone, Copy)]
pub struct ActiveRule {
    pub id: RuleId,
    evaluator: SamplingValueEvaluator,
}

impl ActiveRule {
    /// Gets the sample rate for the specific rule.
    pub fn sampling_value(&self, now: DateTime<Utc>) -> f64 {
        self.evaluator.evaluate(now)
    }
}

/// Returns `true` if this value is equal to `Default::default()`.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

/// A sampling rule as it is deserialized from the project configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    pub condition: RuleCondition,
    pub sampling_value: SamplingValue,
    #[serde(rename = "type")]
    pub ty: RuleType,
    pub id: RuleId,
    /// The time range the rule should be applicable in.
    ///
    /// The time range is open on both ends by default. If a time range is
    /// closed on at least one end, the rule is considered a decaying rule.
    #[serde(default, skip_serializing_if = "TimeRange::is_empty")]
    pub time_range: TimeRange,
    #[serde(default, skip_serializing_if = "is_default")]
    pub decaying_fn: DecayingFunction,
}

impl SamplingRule {
    fn supported(&self) -> bool {
        self.condition.supported() && self.ty != RuleType::Unsupported
    }

    /// Returns an ActiveRule is the SamplingRule is active.
    ///
    /// The checking of the "active" state of a SamplingRule is performed independently
    /// based on the specified DecayingFunction, which defaults to constant.
    fn is_active(&self, now: DateTime<Utc>) -> Option<ActiveRule> {
        let sampling_base_value = self.sampling_value.value();

        match self.decaying_fn {
            DecayingFunction::Linear { decayed_value } => {
                if let TimeRange {
                    start: Some(start),
                    end: Some(end),
                } = self.time_range
                {
                    // As in the TimeRange::contains method we use a right non-inclusive time bound.
                    if sampling_base_value > decayed_value && start <= now && now < end {
                        return Some(ActiveRule {
                            id: self.id,
                            evaluator: SamplingValueEvaluator::Linear {
                                start,
                                end,
                                initial_value: sampling_base_value,
                                decayed_value,
                            },
                        });
                    }
                }
            }
            DecayingFunction::Constant => {
                if self.time_range.contains(now) {
                    return Some(ActiveRule {
                        id: self.id,
                        evaluator: SamplingValueEvaluator::Constant {
                            initial_value: sampling_base_value,
                        },
                    });
                }
            }
        }

        None
    }

    fn is_sample_rate_rule(&self) -> bool {
        matches!(self.sampling_value, SamplingValue::SampleRate { .. })
    }
}

/// Get the numeric measurement value.
///
/// The name is provided without a prefix, for example `"lcp"` loads `event.measurements.lcp`.
fn get_measurement(event: &Event, name: &str) -> Option<f64> {
    let measurements = event.measurements.value()?;
    let annotated = measurements.get(name)?;
    let value = annotated.value().and_then(|m| m.value.value())?;
    Some(*value)
}

/// Trait implemented by providers of fields (Events and Trace Contexts).
///
/// The fields will be used by rules to check if they apply.
// TODO: This trait is used for more than dynamic sampling now. Move it to relay-protocol.
pub trait FieldValueProvider {
    /// gets the value of a field
    fn get_value(&self, path: &str) -> Value;
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
            "dist" => match self.dist.value() {
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
                Some(platform) => platform.clone().into(),
                None => Value::from("other"),
            },

            // Fields in top level structures (called "interfaces" in Sentry)
            "user.email" => self
                .user
                .value()
                .and_then(|user| user.email.as_str())
                .filter(|id| !id.is_empty())
                .map_or(Value::Null, Value::from),
            "user.id" => self
                .user
                .value()
                .and_then(|user| user.id.as_str())
                .filter(|id| !id.is_empty())
                .map_or(Value::Null, Value::from),
            "user.ip_address" => self
                .user
                .value()
                .and_then(|user| user.ip_address.as_str())
                .map_or(Value::Null, Value::from),
            "user.name" => self
                .user
                .value()
                .and_then(|user| user.name.as_str())
                .filter(|id| !id.is_empty())
                .map_or(Value::Null, Value::from),
            "user.segment" => self
                .user
                .value()
                .and_then(|user| user.segment.as_str())
                .filter(|id| !id.is_empty())
                .map_or(Value::Null, Value::from),
            "user.geo.city" => self
                .user
                .value()
                .and_then(|user| user.geo.value())
                .and_then(|geo| geo.city.as_str())
                .map_or(Value::Null, Value::from),
            "user.geo.country_code" => self
                .user
                .value()
                .and_then(|user| user.geo.value())
                .and_then(|geo| geo.country_code.as_str())
                .map_or(Value::Null, Value::from),
            "user.geo.region" => self
                .user
                .value()
                .and_then(|user| user.geo.value())
                .and_then(|geo| geo.region.as_str())
                .map_or(Value::Null, Value::from),
            "user.geo.subdivision" => self
                .user
                .value()
                .and_then(|user| user.geo.value())
                .and_then(|geo| geo.subdivision.as_str())
                .map_or(Value::Null, Value::from),
            "request.method" => self
                .request
                .value()
                .and_then(|request| request.method.as_str())
                .map_or(Value::Null, Value::from),

            // Partial implementation of contexts.
            "contexts.device.name" => self
                .context::<DeviceContext>()
                .and_then(|device| device.name.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.device.family" => self
                .context::<DeviceContext>()
                .and_then(|device| device.family.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.os.name" => self
                .context::<OsContext>()
                .and_then(|os| os.name.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.os.version" => self
                .context::<OsContext>()
                .and_then(|os| os.version.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.browser.name" => self
                .context::<BrowserContext>()
                .and_then(|browser| browser.name.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.browser.version" => self
                .context::<BrowserContext>()
                .and_then(|browser| browser.version.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.trace.status" => self
                .context::<TraceContext>()
                .and_then(|trace| trace.status.value())
                .map_or(Value::Null, |status| status.as_str().into()),
            "contexts.trace.op" => self
                .context::<TraceContext>()
                .and_then(|trace| trace.op.as_str())
                .map_or(Value::Null, Value::from),
            "contexts.response.status_code" => self
                .context::<ResponseContext>()
                .and_then(|response| response.status_code.value().copied())
                .map_or(Value::Null, Value::from),

            // Computed fields (see Discover)
            "duration" => match (
                self.ty.value(),
                self.start_timestamp.value(),
                self.timestamp.value(),
            ) {
                (Some(&EventType::Transaction), Some(&start), Some(&end)) if start <= end => {
                    match Number::from_f64(time::chrono_to_positive_millis(end - start)) {
                        Some(num) => Value::Number(num),
                        None => Value::Null,
                    }
                }
                _ => Value::Null,
            },
            "release.build" => self
                .release
                .as_str()
                .and_then(|r| Release::parse(r).ok())
                .and_then(|r| r.build_hash().map(Value::from))
                .unwrap_or(Value::Null),
            "release.package" => self
                .release
                .as_str()
                .and_then(|r| Release::parse(r).ok())
                .and_then(|r| r.package().map(Value::from))
                .unwrap_or(Value::Null),
            "release.version.short" => self
                .release
                .as_str()
                .and_then(|r| Release::parse(r).ok())
                .and_then(|r| r.version().map(|v| v.raw_short().into()))
                .unwrap_or(Value::Null),

            // Dynamic access to certain data bags
            _ => {
                if let Some(rest) = field_name.strip_prefix("measurements.") {
                    rest.strip_suffix(".value")
                        .filter(|measurement_name| !measurement_name.is_empty())
                        .and_then(|measurement_name| get_measurement(self, measurement_name))
                        .map_or(Value::Null, Value::from)
                } else if let Some(rest) = field_name.strip_prefix("breakdowns.") {
                    rest.split_once('.')
                        .and_then(|(breakdown, measurement)| {
                            self.breakdowns
                                .value()
                                .and_then(|breakdowns| breakdowns.get(breakdown))
                                .and_then(|annotated| annotated.value())
                                .and_then(|measurements| measurements.get(measurement))
                                .and_then(|annotated| annotated.value())
                                .and_then(|measurement| measurement.value.value())
                        })
                        .map_or(Value::Null, |f| Value::from(*f))
                } else if let Some(rest) = field_name.strip_prefix("extra.") {
                    self.extra_at(rest)
                        .map_or(Value::Null, |v| v.clone().into())
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
}

impl FieldValueProvider for DynamicSamplingContext {
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
            "trace.user.id" => {
                if self.user.user_id.is_empty() {
                    Value::Null
                } else {
                    self.user.user_id.as_str().into()
                }
            }
            "trace.user.segment" => {
                if self.user.user_segment.is_empty() {
                    Value::Null
                } else {
                    self.user.user_segment.as_str().into()
                }
            }
            "trace.transaction" => match self.transaction {
                None => Value::Null,
                Some(ref s) => s.as_str().into(),
            },
            "trace.replay_id" => match self.replay_id {
                None => Value::Null,
                Some(ref s) => Value::String(s.to_string()),
            },
            _ => Value::Null,
        }
    }
}

impl FieldValueProvider for Span {
    fn get_value(&self, path: &str) -> Value {
        let Some(path) = path.strip_prefix("span.") else {
            return Value::Null;
        };

        match path {
            "exclusive_time" => self
                .exclusive_time
                .value()
                .map_or(Value::Null, |&v| v.into()),
            "description" => self.description.as_str().map_or(Value::Null, Value::from),
            "op" => self.op.as_str().map_or(Value::Null, Value::from),
            "span_id" => self
                .span_id
                .value()
                .map_or(Value::Null, |s| s.0.as_str().into()),
            "parent_span_id" => self
                .parent_span_id
                .value()
                .map_or(Value::Null, |s| s.0.as_str().into()),
            "trace_id" => self
                .trace_id
                .value()
                .map_or(Value::Null, |s| s.0.as_str().into()),
            "status" => self
                .status
                .value()
                .map_or(Value::Null, |s| s.as_str().into()),
            "origin" => self.origin.as_str().map_or(Value::Null, Value::from),
            _ => {
                if let Some(key) = path.strip_prefix("tags.") {
                    if let Some(v) = self
                        .tags
                        .value()
                        .and_then(|tags| tags.get(key))
                        .and_then(Annotated::value)
                    {
                        return Value::from(v.as_str());
                    }
                }
                if let Some(key) = path.strip_prefix("data.") {
                    let escaped = key.replace("\\.", "\0");
                    let mut path = escaped.split('.').map(|s| s.replace('\0', "."));
                    let Some(root) = path.next() else {
                        return Value::Null;
                    };
                    let Some(mut val) = self
                        .data
                        .value()
                        .and_then(|data| data.get(&root))
                        .and_then(Annotated::value)
                    else {
                        return Value::Null;
                    };
                    for part in path {
                        // While there is path segments left, `val` has to be an Object.
                        let relay_protocol::Value::Object(map) = val else {
                            return Value::Null;
                        };
                        let Some(child) = map.get(&part).and_then(Annotated::value) else {
                            return Value::Null;
                        };
                        val = child;
                    }
                    return Value::from(val.clone());
                }
                Value::Null
            }
        }
    }
}

/// Defines which population of items a dynamic sample rate applies to.
///
/// SDKs with client side sampling reduce the number of items sent to Relay, where dynamic sampling
/// occurs. The sampling mode controlls whether the sample rate is relative to the original
/// population of items before client-side sampling, or relative to the number received by Relay
/// after client-side sampling.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SamplingMode {
    /// The sample rate is based on the number of events received by Relay.
    ///
    /// Server-side dynamic sampling occurs on top of potential client-side sampling in the SDK. For
    /// example, if the SDK samples at 50% and the server sampling rate is set at 10%, the resulting
    /// effective sample rate is 5%.
    Received,
    /// The sample rate is based on the original number of events in the client.
    ///
    /// Server-side sampling compensates potential client-side sampling in the SDK. For example, if
    /// the SDK samples at 50% and the server sampling rate is set at 10%, the resulting effective
    /// sample rate is 10%.
    ///
    /// In this mode, the server sampling rate is capped by the client's sampling rate. Rules with a
    /// higher sample rate than what the client is sending are effectively inactive.
    Total,

    /// Catch-all variant for forward compatibility.
    #[serde(other)]
    Unsupported,
}

impl Default for SamplingMode {
    fn default() -> Self {
        Self::Received
    }
}

/// Represents a list of rule ids which is used for outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct MatchedRuleIds(pub Vec<RuleId>);

impl MatchedRuleIds {
    /// Creates a MatchedRuleIds struct from a string formatted with the following format:
    /// rule_id_1,rule_id_2,...
    pub fn from_string(value: &str) -> Result<MatchedRuleIds, ParseIntError> {
        let mut rule_ids = vec![];

        for rule_id in value.split(',') {
            let int_rule_id = rule_id.parse()?;
            rule_ids.push(RuleId(int_rule_id));
        }

        Ok(MatchedRuleIds(rule_ids))
    }
}

impl fmt::Display for MatchedRuleIds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|rule_id| format!("{rule_id}"))
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

/// Checks whether unsupported rules result in a direct keep of the event or depending on the
/// type of Relay an ignore of unsupported rules.
fn check_unsupported_rules(
    processing_enabled: bool,
    sampling_config: Option<&SamplingConfig>,
    root_sampling_config: Option<&SamplingConfig>,
) -> Result<(), ()> {
    // When we have unsupported rules disable sampling for non processing relays.
    if sampling_config.map_or(false, |config| config.has_unsupported_rules())
        || root_sampling_config.map_or(false, |config| config.has_unsupported_rules())
    {
        if !processing_enabled {
            return Err(());
        } else {
            relay_log::error!("found unsupported rules even as processing relay");
        }
    }

    Ok(())
}

/// Returns an iterator of references that chains together and merges rules.
///
/// The chaining logic will take all the non-trace rules from the project and all the trace/unsupported
/// rules from the root project and concatenate them.
pub fn merge_rules_from_configs<'a>(
    sampling_config: Option<&'a SamplingConfig>,
    root_sampling_config: Option<&'a SamplingConfig>,
) -> impl Iterator<Item = &'a SamplingRule> {
    let transaction_rules = sampling_config
        .into_iter()
        .flat_map(|config| config.rules_v2.iter())
        .filter(|&rule| rule.ty == RuleType::Transaction);

    let trace_rules = root_sampling_config
        .into_iter()
        .flat_map(|config| config.rules_v2.iter())
        .filter(|&rule| rule.ty == RuleType::Trace);

    transaction_rules.chain(trace_rules)
}

/// Gets the sampling match result by creating the merged configuration and matching it against
/// the sampling configuration.
pub fn merge_configs_and_match(
    processing_enabled: bool,
    sampling_config: Option<&SamplingConfig>,
    root_sampling_config: Option<&SamplingConfig>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    now: DateTime<Utc>,
) -> Option<SamplingMatch> {
    // We check if there are unsupported rules in any of the two configurations.
    check_unsupported_rules(processing_enabled, sampling_config, root_sampling_config).ok()?;

    // We perform the rule matching with the multi-matching logic on the merged rules.
    let rules = merge_rules_from_configs(sampling_config, root_sampling_config);
    let mut match_result = SamplingMatch::match_against_rules(rules, event, dsc, now)?;

    // If we have a match, we will try to derive the sample rate based on the sampling mode.
    //
    // Keep in mind that the sample rate received here has already been derived by the matching
    // logic, based on multiple matches and decaying functions.
    //
    // The determination of the sampling mode occurs with the following priority:
    // 1. Non-root project sampling mode
    // 2. Root project sampling mode
    let Some(primary_config) = sampling_config.or(root_sampling_config) else {
        relay_log::error!("cannot sample without at least one sampling config");
        return None;
    };
    let sample_rate = match primary_config.mode {
        SamplingMode::Received => match_result.sample_rate,
        SamplingMode::Total => match dsc {
            Some(dsc) => dsc.adjusted_sample_rate(match_result.sample_rate),
            None => match_result.sample_rate,
        },
        SamplingMode::Unsupported => {
            if processing_enabled {
                relay_log::error!("found unsupported sampling mode even as processing Relay");
            }

            return None;
        }
    };
    match_result.set_sample_rate(sample_rate);

    // Only if we arrive at this stage, it means that we have found a match and we want to prepare
    // the data for making the sampling decision.
    Some(match_result)
}

/// Represents the specification for sampling an incoming event.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SamplingMatch {
    /// The sample rate to use for the incoming event.
    pub sample_rate: f64,
    /// The seed to feed to the random number generator which allows the same number to be
    /// generated given the same seed.
    ///
    /// This is especially important for trace sampling, even though we can have inconsistent
    /// traces due to multi-matching.
    pub seed: Uuid,
    /// The list of rule ids that have matched the incoming event and/or dynamic sampling context.
    pub matched_rule_ids: MatchedRuleIds,
}

impl SamplingMatch {
    /// Setter for `sample_rate`.
    pub fn set_sample_rate(&mut self, new_sample_rate: f64) {
        self.sample_rate = new_sample_rate;
    }

    /// Matches an event and/or dynamic sampling context against the rules of the sampling configuration.
    ///
    /// The multi-matching algorithm used iterates by collecting and multiplying factor rules until
    /// it finds a sample rate rule. Once a sample rate rule is found, the final sample rate is
    /// computed by multiplying it with the previously accumulated factors.
    ///
    /// The default accumulated factors equal to 1 because it is the identity of the multiplication
    /// operation, thus in case no factor rules are matched, the final result will just be the
    /// sample rate of the matching rule.
    ///
    /// In case no sample rate rule is matched, we are going to return a None, signaling that no
    /// match has been found.
    pub fn match_against_rules<'a, I>(
        rules: I,
        event: Option<&Event>,
        dsc: Option<&DynamicSamplingContext>,
        now: DateTime<Utc>,
    ) -> Option<SamplingMatch>
    where
        I: Iterator<Item = &'a SamplingRule>,
    {
        let mut matched_rule_ids = vec![];
        // Even though this seed is changed based on whether we match event or trace rules, we will
        // still incur in inconsistent trace sampling because of multi-matching of rules across event
        // and trace rules.
        //
        // An example of inconsistent trace sampling could be:
        // /hello -> /world -> /transaction belong to trace_id = abc
        // * /hello has uniform rule with 0.2 sample rate which will match all the transactions of the trace
        // * each project has a single transaction rule with different factors (2, 3, 4)
        //
        // 1. /hello is matched with a transaction rule with a factor of 2 and uses as seed abc -> 0.2 * 2 = 0.4 sample rate
        // 2. /world is matched with a transaction rule with a factor of 3 and uses as seed abc -> 0.2 * 3 = 0.6 sample rate
        // 3. /transaction is matched with a transaction rule with a factor of 4 and uses as seed abc -> 0.2 * 4 = 0.8 sample rate
        //
        // We can see that we have 3 different samples rates but given the same seed, the random number generated will be the same.
        let mut seed = event.and_then(|e| e.id.value()).map(|id| id.0);
        let mut accumulated_factors = 1.0;

        for rule in rules {
            let matches = match rule.ty {
                RuleType::Trace => match dsc {
                    Some(dsc) => rule.condition.matches(dsc),
                    _ => false,
                },
                RuleType::Transaction => event.map_or(false, |event| match event.ty.0 {
                    Some(EventType::Transaction) => rule.condition.matches(event),
                    _ => false,
                }),
                _ => false,
            };

            if matches {
                if let Some(active_rule) = rule.is_active(now) {
                    matched_rule_ids.push(rule.id);

                    if rule.ty == RuleType::Trace {
                        if let Some(dsc) = dsc {
                            seed = Some(dsc.trace_id);
                        }
                    }

                    let value = active_rule.sampling_value(now);
                    if rule.is_sample_rate_rule() {
                        return Some(SamplingMatch {
                            sample_rate: (value * accumulated_factors).clamp(0.0, 1.0),
                            seed: match seed {
                                Some(seed) => seed,
                                // In case we are not able to generate a seed, we will return a no
                                // match.
                                None => return None,
                            },
                            matched_rule_ids: MatchedRuleIds(matched_rule_ids),
                        });
                    } else {
                        accumulated_factors *= value
                    }
                }
            }
        }

        // In case no match is available, we won't return any specification.
        None
    }
}

/// Represents the dynamic sampling configuration available to a project.
///
/// Note: This comes from the organization data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingConfig {
    /// The ordered sampling rules for the project.
    ///
    /// This field will remain here to serve only for old customer Relays to which we will
    /// forward the sampling config. The idea is that those Relays will get the old rules as
    /// empty array, which will result in them not sampling and forwarding sampling decisions to
    /// upstream Relays.
    #[serde(default, skip_deserializing)]
    pub rules: Vec<SamplingRule>,
    /// The ordered sampling rules v2 for the project.
    pub rules_v2: Vec<SamplingRule>,
    /// Defines which population of items a dynamic sample rate applies to.
    #[serde(default, skip_serializing_if = "is_default")]
    pub mode: SamplingMode,
}

impl SamplingConfig {
    pub fn has_unsupported_rules(&self) -> bool {
        !self.rules_v2.iter().all(SamplingRule::supported)
    }
}

/// The User related information in the trace context
///
/// This is more of a mixin to be used in the DynamicSamplingContext
#[derive(Debug, Clone, Serialize, Default)]
pub struct TraceUserContext {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_segment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_id: String,
}

impl<'de> Deserialize<'de> for TraceUserContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Default)]
        struct Nested {
            #[serde(default)]
            pub segment: String,
            #[serde(default)]
            pub id: String,
        }

        #[derive(Deserialize)]
        struct Helper {
            // Nested implements default, but we used to accept user=null (not sure if any SDK
            // sends this though)
            #[serde(default)]
            user: Option<Nested>,
            #[serde(default)]
            user_segment: String,
            #[serde(default)]
            user_id: String,
        }

        let helper = Helper::deserialize(deserializer)?;

        if helper.user_id.is_empty() && helper.user_segment.is_empty() {
            let user = helper.user.unwrap_or_default();
            Ok(TraceUserContext {
                user_segment: user.segment,
                user_id: user.id,
            })
        } else {
            Ok(TraceUserContext {
                user_segment: helper.user_segment,
                user_id: helper.user_id,
            })
        }
    }
}

mod sample_rate_as_string {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = match Option::<Cow<'_, str>>::deserialize(deserializer)? {
            Some(value) => value,
            None => return Ok(None),
        };

        let parsed_value =
            serde_json::from_str(&value).map_err(|e| serde::de::Error::custom(e.to_string()))?;

        if parsed_value < 0.0 {
            return Err(serde::de::Error::custom("sample rate cannot be negative"));
        }

        Ok(Some(parsed_value))
    }

    pub fn serialize<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(float) => serde_json::to_string(float)
                .map_err(|e| serde::ser::Error::custom(e.to_string()))?
                .serialize(serializer),
            None => value.serialize(serializer),
        }
    }
}

struct BoolOptionVisitor;

impl<'de> Visitor<'de> for BoolOptionVisitor {
    type Value = Option<bool>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("`true` or `false` as boolean or string")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(match v {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }
}

pub fn deserialize_bool_option<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(BoolOptionVisitor)
}

/// DynamicSamplingContext created by the first Sentry SDK in the call chain.
///
/// Because SDKs need to funnel this data through the baggage header, this needs to be
/// representable as `HashMap<String, String>`, meaning no nested dictionaries/objects, arrays or
/// other non-string values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSamplingContext {
    /// ID created by clients to represent the current call flow.
    pub trace_id: Uuid,
    /// The project key.
    pub public_key: ProjectKey,
    /// The release.
    #[serde(default)]
    pub release: Option<String>,
    /// The environment.
    #[serde(default)]
    pub environment: Option<String>,
    /// The name of the transaction extracted from the `transaction` field in the starting
    /// transaction.
    ///
    /// Set on transaction start, or via `scope.transaction`.
    #[serde(default)]
    pub transaction: Option<String>,
    /// The sample rate with which this trace was sampled in the client. This is a float between
    /// `0.0` and `1.0`.
    #[serde(
        default,
        with = "sample_rate_as_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub sample_rate: Option<f64>,
    /// The user specific identifier (e.g. a user segment, or similar created by the SDK from the
    /// user object).
    #[serde(flatten, default)]
    pub user: TraceUserContext,
    /// If the event occurred during a session replay, the associated replay_id is added to the DSC.
    pub replay_id: Option<Uuid>,
    /// Set to true if the transaction starting the trace has been kept by client side sampling.
    #[serde(
        default,
        deserialize_with = "deserialize_bool_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub sampled: Option<bool>,
    /// Additional arbitrary fields for forwards compatibility.
    #[serde(flatten, default)]
    pub other: BTreeMap<String, Value>,
}

impl DynamicSamplingContext {
    /// Computes a dynamic sampling context from a transaction event.
    ///
    /// Returns `None` if the passed event is not a transaction event, or if it does not contain a
    /// trace ID in its trace context. All optional fields in the dynamic sampling context are
    /// populated with the corresponding attributes from the event payload if they are available.
    ///
    /// Since sampling information is not available in the event payload, the `sample_rate` field
    /// cannot be set when computing the dynamic sampling context from a transaction event.
    pub fn from_transaction(public_key: ProjectKey, event: &Event) -> Option<Self> {
        if event.ty.value() != Some(&EventType::Transaction) {
            return None;
        }

        let Some(trace) = event.context::<TraceContext>() else {
            return None;
        };
        let trace_id = trace.trace_id.value()?.0.parse().ok()?;
        let user = event.user.value();

        Some(Self {
            trace_id,
            public_key,
            release: event.release.as_str().map(str::to_owned),
            environment: event.environment.value().cloned(),
            transaction: event.transaction.value().cloned(),
            replay_id: None,
            sample_rate: None,
            user: TraceUserContext {
                user_segment: user
                    .and_then(|u| u.segment.value().cloned())
                    .unwrap_or_default(),
                user_id: user
                    .and_then(|u| u.id.as_str())
                    .unwrap_or_default()
                    .to_owned(),
            },
            sampled: None,
            other: Default::default(),
        })
    }

    /// Compute the effective sampling rate based on the random "diceroll" and the sample rate from
    /// the matching rule.
    pub fn adjusted_sample_rate(&self, rule_sample_rate: f64) -> f64 {
        let client_sample_rate = self.sample_rate.unwrap_or(1.0);
        if client_sample_rate <= 0.0 {
            // client_sample_rate is 0, which is bogus because the SDK should've dropped the
            // envelope. In that case let's pretend the sample rate was not sent, because clearly
            // the sampling decision across the trace is still 1. The most likely explanation is
            // that the SDK is reporting its own sample rate setting instead of the one from the
            // continued trace.
            //
            // since we write back the client_sample_rate into the event's trace context, it should
            // be possible to find those values + sdk versions via snuba
            relay_log::warn!("client sample rate is <= 0");
            rule_sample_rate
        } else {
            let adjusted_sample_rate = (rule_sample_rate / client_sample_rate).clamp(0.0, 1.0);
            if adjusted_sample_rate.is_infinite() || adjusted_sample_rate.is_nan() {
                relay_log::error!("adjusted sample rate ended up being nan/inf");
                debug_assert!(false);
                rule_sample_rate
            } else {
                adjusted_sample_rate
            }
        }
    }
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
    use std::str::FromStr;

    use chrono::{Duration as DateDuration, TimeZone, Utc};
    use serde_json::json;
    use similar_asserts::assert_eq;

    use relay_event_schema::protocol::{
        Contexts, DeviceContext, EventId, Exception, Headers, IpAddr, JsonLenientString,
        LenientString, LogEntry, OsContext, PairList, Request, TagEntry, Tags, User, Values,
    };
    use relay_protocol::Annotated;

    use super::*;

    macro_rules! assert_transaction_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                Some(SamplingMatch {
                    sample_rate: $sr,
                    seed: $sd.id.value().unwrap().0,
                    matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                })
            )
        }
    }

    macro_rules! assert_trace_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                Some(SamplingMatch {
                    sample_rate: $sr,
                    seed: $sd.trace_id,
                    matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                })
            )
        }
    }

    macro_rules! assert_rule_ids_eq {
        ($exc:expr, $res:expr) => {
            if ($exc.len() != $res.len()) {
                panic!("The rule ids don't match.")
            }

            for (index, rule) in $res.iter().enumerate() {
                assert_eq!(rule.id.0, $exc[index])
            }
        };
    }

    macro_rules! assert_no_match {
        ($res:expr) => {
            assert_eq!($res, None)
        };
    }

    fn default_sampling_context() -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::default(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            environment: None,
            transaction: None,
            sample_rate: None,
            user: TraceUserContext::default(),
            replay_id: None,
            sampled: None,
            other: BTreeMap::new(),
        }
    }

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

    fn eq_null(name: &str) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: Value::Null,
            options: EqCondOptions { ignore_case: true },
        })
    }

    fn glob(name: &str, value: &[&str]) -> RuleCondition {
        RuleCondition::Glob(GlobCondition {
            name: name.to_owned(),
            value: GlobPatterns::new(value.iter().map(|s| s.to_string()).collect()),
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

    fn mocked_sampling_config_with_rules(rules: Vec<SamplingRule>) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: rules,
            mode: SamplingMode::Received,
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

    fn mocked_event(
        event_type: EventType,
        transaction: &str,
        release: &str,
        environment: &str,
    ) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            environment: Annotated::new(environment.to_string()),
            ..Event::default()
        }
    }

    fn mocked_dynamic_sampling_context(
        transaction: &str,
        release: &str,
        environment: &str,
        user_segment: &str,
        user_id: &str,
        replay_id: Option<Uuid>,
    ) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: Some(release.to_string()),
            environment: Some(environment.to_string()),
            transaction: Some(transaction.to_string()),
            sample_rate: Some(1.0),
            user: TraceUserContext {
                user_segment: user_segment.to_string(),
                user_id: user_id.to_string(),
            },
            replay_id,
            sampled: None,
            other: Default::default(),
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
            sampled: None,
        }
    }

    fn mocked_sampling_config(mode: SamplingMode) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                SamplingRule {
                    condition: eq("event.transaction", &["healthcheck"], true),
                    sampling_value: SamplingValue::SampleRate { value: 0.1 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["bar"], true),
                    sampling_value: SamplingValue::Factor { value: 1.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["foo"], true),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Transaction,
                    id: RuleId(3),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                // We put this trace rule here just for testing purposes, even though it will never
                // be considered if put within a non-root project.
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(4),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
        }
    }

    fn mocked_root_project_sampling_config(mode: SamplingMode) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                SamplingRule {
                    condition: eq("trace.release", &["3.0"], true),
                    sampling_value: SamplingValue::Factor { value: 1.5 },
                    ty: RuleType::Trace,
                    id: RuleId(5),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("trace.environment", &["dev"], true),
                    sampling_value: SamplingValue::SampleRate { value: 1.0 },
                    ty: RuleType::Trace,
                    id: RuleId(6),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(7),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
        }
    }

    fn mocked_decaying_sampling_rule(
        id: u32,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        sampling_value: SamplingValue,
        decaying_fn: DecayingFunction,
    ) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value,
            ty: RuleType::Transaction,
            id: RuleId(id),
            time_range: TimeRange { start, end },
            decaying_fn,
        }
    }

    fn add_sampling_rule_to_config(
        sampling_config: &mut SamplingConfig,
        sampling_rule: SamplingRule,
    ) {
        sampling_config.rules_v2.push(sampling_rule);
    }

    fn match_against_rules(
        config: &SamplingConfig,
        event: &Event,
        dsc: &DynamicSamplingContext,
        now: DateTime<Utc>,
    ) -> Option<SamplingMatch> {
        SamplingMatch::match_against_rules(config.rules_v2.iter(), Some(event), Some(dsc), now)
    }

    fn merge_root_and_non_root_configs_with(
        rules: Vec<SamplingRule>,
        root_rules: Vec<SamplingRule>,
    ) -> Vec<SamplingRule> {
        let sampling_config = mocked_sampling_config_with_rules(rules);
        let root_sampling_config = mocked_sampling_config_with_rules(root_rules);

        merge_rules_from_configs(Some(&sampling_config), Some(&root_sampling_config))
            .cloned()
            .collect()
    }

    #[test]
    fn test_unmatching_json_rule_is_unsupported() {
        let bad_json = r#"{
            "op": "BadOperator",
            "name": "foo",
            "value": "bar"
        }"#;

        let rule: RuleCondition = serde_json::from_str(bad_json).unwrap();
        assert!(matches!(rule, RuleCondition::Unsupported));
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
                contexts.add(DeviceContext {
                    name: Annotated::new("iphone".to_string()),
                    family: Annotated::new("iphone-fam".to_string()),
                    model: Annotated::new("iphone7,3".to_string()),
                    ..DeviceContext::default()
                });
                contexts.add(OsContext {
                    name: Annotated::new("iOS".to_string()),
                    version: Annotated::new("11.4.2".to_string()),
                    kernel_version: Annotated::new("17.4.0".to_string()),
                    ..OsContext::default()
                });
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

    /// test extraction of field values from empty event
    #[test]
    fn test_field_value_provider_event_empty() {
        let event = Event::default();

        assert_eq!(Value::Null, event.get_value("event.release"));
        assert_eq!(Value::Null, event.get_value("event.environment"));
        assert_eq!(Value::Null, event.get_value("event.user.id"));
        assert_eq!(Value::Null, event.get_value("event.user.segment"));

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
        let replay_id = Uuid::new_v4();
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: TraceUserContext {
                user_segment: "user-seg".into(),
                user_id: "user-id".into(),
            },
            environment: Some("prod".into()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            replay_id: Some(replay_id),
            sampled: None,
            other: BTreeMap::new(),
        };

        assert_eq!(
            Value::String("1.1.1".into()),
            dsc.get_value("trace.release")
        );
        assert_eq!(
            Value::String("prod".into()),
            dsc.get_value("trace.environment")
        );
        assert_eq!(
            Value::String("user-id".into()),
            dsc.get_value("trace.user.id")
        );
        assert_eq!(
            Value::String("user-seg".into()),
            dsc.get_value("trace.user.segment")
        );
        assert_eq!(
            Value::String("transaction1".into()),
            dsc.get_value("trace.transaction")
        );
        assert_eq!(
            Value::String(replay_id.to_string()),
            dsc.get_value("trace.replay_id")
        );
    }

    #[test]
    fn test_field_value_provider_trace_empty() {
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            environment: None,
            transaction: None,
            sample_rate: None,
            replay_id: None,
            sampled: None,
            other: BTreeMap::new(),
        };
        assert_eq!(Value::Null, dsc.get_value("trace.release"));
        assert_eq!(Value::Null, dsc.get_value("trace.environment"));
        assert_eq!(Value::Null, dsc.get_value("trace.user.id"));
        assert_eq!(Value::Null, dsc.get_value("trace.user.segment"));
        assert_eq!(Value::Null, dsc.get_value("trace.user.transaction"));
        assert_eq!(Value::Null, dsc.get_value("trace.replay_id"));

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            environment: None,
            transaction: None,
            sample_rate: None,
            replay_id: None,
            sampled: None,
            other: BTreeMap::new(),
        };
        assert_eq!(Value::Null, dsc.get_value("trace.user.id"));
        assert_eq!(Value::Null, dsc.get_value("trace.user.segment"));
    }

    #[test]
    fn test_field_value_provider_span_data() {
        let span = Annotated::<Span>::from_json(
            r#" {
            "data": {
                "foo": {
                    "bar": 1
                },
                "foo.bar": 2
            }
        }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        assert_eq!(span.get_value("span.data.foo.bar"), json!(1));
        assert_eq!(span.get_value(r"span.data.foo\.bar"), json!(2));

        assert_eq!(span.get_value("span.data"), Value::Null);
        assert_eq!(span.get_value("span.data."), Value::Null);
        assert_eq!(span.get_value("span.data.x"), Value::Null);
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

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: TraceUserContext {
                user_segment: "vip".into(),
                user_id: "user-id".into(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".into()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc), "{failure_name}");
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
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&evt), "{failure_name}");
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

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
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

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
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

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    /// test various rules that do not match
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

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(!condition.matches(&dsc), "{failure_name}");
        }
    }

    #[test]
    fn test_sampling_config_deserialization() {
        let json = include_str!("../tests/fixtures/sampling_config.json");
        serde_json::from_str::<SamplingConfig>(json).unwrap();
    }

    #[test]
    fn test_dynamic_sampling_context_deserialization() {
        let json = include_str!("../tests/fixtures/dynamic_sampling_context.json");
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
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
        }
        ]
        "#;
        let rules: Result<Vec<RuleCondition>, _> = serde_json::from_str(serialized_rules);
        assert!(rules.is_ok());
        let rules = rules.unwrap();
        insta::assert_ron_snapshot!(rules, @r#"
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
            ]"#);
    }

    #[test]
    fn test_non_decaying_sampling_rule_deserialization() {
        let serialized_rule = r#"{
            "condition":{
                "op":"and",
                "inner": [
                    { "op" : "glob", "name": "releases", "value":["1.1.1", "1.1.2"]}
                ]
            },
            "samplingValue": {"type": "sampleRate", "value": 0.7},
            "type": "trace",
            "id": 1
        }"#;

        let rule: SamplingRule = serde_json::from_str(serialized_rule).unwrap();
        assert_eq!(
            rule.sampling_value,
            SamplingValue::SampleRate { value: 0.7f64 }
        );
        assert_eq!(rule.ty, RuleType::Trace);
    }

    #[test]
    fn test_non_decaying_sampling_rule_deserialization_with_factor() {
        let serialized_rule = r#"{
            "condition":{
                "op":"and",
                "inner": [
                    { "op" : "glob", "name": "releases", "value":["1.1.1", "1.1.2"]}
                ]
            },
            "samplingValue": {"type": "factor", "value": 5.0},
            "type": "trace",
            "id": 1
        }"#;

        let rule: SamplingRule = serde_json::from_str(serialized_rule).unwrap();
        assert_eq!(rule.sampling_value, SamplingValue::Factor { value: 5.0 });
        assert_eq!(rule.ty, RuleType::Trace);
    }

    #[test]
    fn test_sampling_rule_with_constant_decaying_function_deserialization() {
        let serialized_rule = r#"{
            "condition":{
                "op":"and",
                "inner": [
                    { "op" : "glob", "name": "releases", "value":["1.1.1", "1.1.2"]}
                ]
            },
            "samplingValue": {"type": "factor", "value": 5.0},
            "type": "trace",
            "id": 1,
            "timeRange": {
                "start": "2022-10-10T00:00:00.000000Z",
                "end": "2022-10-20T00:00:00.000000Z"
            }
        }"#;
        let rule: Result<SamplingRule, _> = serde_json::from_str(serialized_rule);
        let rule = rule.unwrap();
        let time_range = rule.time_range;
        let decaying_function = rule.decaying_fn;

        assert_eq!(
            time_range.start,
            Some(Utc.with_ymd_and_hms(2022, 10, 10, 0, 0, 0).unwrap())
        );
        assert_eq!(
            time_range.end,
            Some(Utc.with_ymd_and_hms(2022, 10, 20, 0, 0, 0).unwrap())
        );
        assert_eq!(decaying_function, DecayingFunction::Constant);
    }

    #[test]
    fn test_sampling_rule_with_linear_decaying_function_deserialization() {
        let serialized_rule = r#"{
            "condition":{
                "op":"and",
                "inner": [
                    { "op" : "glob", "name": "releases", "value":["1.1.1", "1.1.2"]}
                ]
            },
            "samplingValue": {"type": "sampleRate", "value": 1.0},
            "type": "trace",
            "id": 1,
            "timeRange": {
                "start": "2022-10-10T00:00:00.000000Z",
                "end": "2022-10-20T00:00:00.000000Z"
            },
            "decayingFn": {
                "type": "linear",
                "decayedValue": 0.9
            }
        }"#;
        let rule: Result<SamplingRule, _> = serde_json::from_str(serialized_rule);
        let rule = rule.unwrap();
        let decaying_function = rule.decaying_fn;

        assert_eq!(
            decaying_function,
            DecayingFunction::Linear { decayed_value: 0.9 }
        );
    }

    #[test]
    fn test_sampling_config_with_rules_and_rules_v2_deserialization() {
        let serialized_rule = r#"{
               "rules": [
                  {
                     "sampleRate": 0.5,
                     "type": "trace",
                     "active": true,
                     "condition": {
                        "op": "and",
                        "inner": []
                     },
                     "id": 1000
                 }
               ],
               "rulesV2": [
                  {
                     "samplingValue":{
                        "type": "sampleRate",
                        "value": 0.5
                     },
                     "type": "trace",
                     "active": true,
                     "condition": {
                        "op": "and",
                        "inner": []
                     },
                     "id": 1000
                  }
               ],
               "mode": "received"
        }"#;
        let config: SamplingConfig = serde_json::from_str(serialized_rule).unwrap();

        // We want to make sure that we serialize an empty array of rule, irrespectively of the
        // received payload.
        assert!(config.rules.is_empty());
        assert_eq!(
            config.rules_v2[0].sampling_value,
            SamplingValue::SampleRate { value: 0.5 }
        );
    }

    #[test]
    fn test_sampling_config_with_rules_and_rules_v2_serialization() {
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![SamplingRule {
                condition: and(vec![eq("event.transaction", &["foo"], true)]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            mode: SamplingMode::Received,
        };

        let serialized_config = serde_json::to_string_pretty(&config).unwrap();
        let expected_serialized_config = r#"{
  "rules": [],
  "rulesV2": [
    {
      "condition": {
        "op": "and",
        "inner": [
          {
            "op": "eq",
            "name": "event.transaction",
            "value": [
              "foo"
            ],
            "options": {
              "ignoreCase": true
            }
          }
        ]
      },
      "samplingValue": {
        "type": "factor",
        "value": 2.0
      },
      "type": "transaction",
      "id": 1
    }
  ]
}"#;

        assert_eq!(serialized_config, expected_serialized_config)
    }

    #[test]
    fn test_partial_trace_matches() {
        let condition = and(vec![
            eq("trace.environment", &["debug"], true),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing release"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.environment", &["debug"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext::default(),
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing user segment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: None,
            sampled: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing environment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: None,
            sampled: None,
            environment: Some("debug".to_string()),
            transaction: None,
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing transaction"
        );
        let condition = and(vec![]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            replay_id: None,
            sampled: None,
            environment: None,
            transaction: None,
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing release, user segment, environment and transaction"
        );
    }

    #[test]
    /// Tests if the MatchedRuleIds struct is displayed correctly as string.
    fn test_matched_rule_ids_to_string() {
        let matched_rule_ids = MatchedRuleIds(vec![RuleId(123), RuleId(456)]);
        assert_eq!(format!("{matched_rule_ids}"), "123,456");

        let matched_rule_ids = MatchedRuleIds(vec![RuleId(123)]);
        assert_eq!(format!("{matched_rule_ids}"), "123");

        let matched_rule_ids = MatchedRuleIds(vec![]);
        assert_eq!(format!("{matched_rule_ids}"), "")
    }

    #[test]
    /// Tests if the MatchRuleIds struct is created correctly from its string representation.
    fn test_matched_rule_ids_from_string() {
        assert_eq!(
            MatchedRuleIds::from_string("123,456"),
            Ok(MatchedRuleIds(vec![RuleId(123), RuleId(456)]))
        );

        assert_eq!(
            MatchedRuleIds::from_string("123"),
            Ok(MatchedRuleIds(vec![RuleId(123)]))
        );

        assert!(MatchedRuleIds::from_string("").is_err());

        assert!(MatchedRuleIds::from_string(",").is_err());

        assert!(MatchedRuleIds::from_string("123.456").is_err());

        assert!(MatchedRuleIds::from_string("a,b").is_err());
    }

    #[test]
    /// test that the multi-matching returns none in case there is no match.
    fn test_multi_matching_with_transaction_event_non_decaying_rules_and_no_match() {
        let result = match_against_rules(
            &mocked_sampling_config_with_rules(vec![
                SamplingRule {
                    condition: and(vec![eq("event.transaction", &["foo"], true)]),
                    sampling_value: SamplingValue::Factor { value: 2.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["prod"], true),
                    ]),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ]),
            &mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing"),
            &mocked_dynamic_sampling_context(
                "root_transaction",
                "1.1.1",
                "debug",
                "vip",
                "user-id",
                None,
            ),
            Utc::now(),
        );
        assert_eq!(result, None, "did not return none for no match");
    }

    #[test]
    /// Tests that the multi-matching works for a mixture of trace and transaction rules with interleaved strategies.
    fn test_match_against_rules_with_multiple_event_types_non_decaying_rules_and_matches() {
        let config = mocked_sampling_config_with_rules(vec![
            SamplingRule {
                condition: and(vec![glob("event.transaction", &["*healthcheck*"])]),
                sampling_value: SamplingValue::SampleRate { value: 0.1 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![glob("trace.environment", &["*dev*"])]),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: RuleType::Trace,
                id: RuleId(2),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![eq("event.transaction", &["foo"], true)]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Transaction,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Trace,
                id: RuleId(4),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                sampling_value: SamplingValue::Factor { value: 1.5 },
                ty: RuleType::Trace,
                id: RuleId(5),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![]),
                sampling_value: SamplingValue::SampleRate { value: 0.02 },
                ty: RuleType::Trace,
                id: RuleId(6),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        ]);

        // early return of first rule
        let event = mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "debug",
            "vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_transaction_match!(result, 0.1, event, 1);

        // early return of second rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "dev",
            "vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 1.0, dsc, 2);

        // factor match third rule and early return sixth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.2",
            "testing",
            "non-vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 0.04, dsc, 3, 6);

        // factor match third rule and early return fourth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 1.0, dsc, 3, 4);

        // factor match third, fifth rule and early return sixth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "non-vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 0.06, dsc, 3, 5, 6);

        // factor match fifth and early return sixth rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "non-vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 0.03, dsc, 5, 6);
    }

    #[test]
    /// Test that the multi-matching works for a mixture of decaying and non-decaying rules.
    fn test_match_against_rules_with_trace_event_type_decaying_rules_and_matches() {
        let config = mocked_sampling_config_with_rules(vec![
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["dev"], true),
                ]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: TimeRange {
                    start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
                },
                decaying_fn: DecayingFunction::Linear { decayed_value: 1.0 },
            },
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                sampling_value: SamplingValue::SampleRate { value: 0.6 },
                ty: RuleType::Trace,
                id: RuleId(2),
                time_range: TimeRange {
                    start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
                },
                decaying_fn: DecayingFunction::Linear { decayed_value: 0.3 },
            },
            SamplingRule {
                condition: and(vec![]),
                sampling_value: SamplingValue::SampleRate { value: 0.02 },
                ty: RuleType::Trace,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        ]);

        // factor match first rule and early return third rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "dev",
            "vip",
            "user-id",
            None,
        );
        // We will use a time in the middle of 10th and 11th.
        let result = match_against_rules(
            &config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert_trace_match!(result, 0.03, dsc, 1, 3);

        // early return second rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "vip",
            "user-id",
            None,
        );
        // We will use a time in the middle of 10th and 11th.
        let result = match_against_rules(
            &config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert!(matches!(result, Some(SamplingMatch { .. })));
        if let Some(spec) = result {
            assert!(
                (spec.sample_rate - 0.45).abs() < f64::EPSILON, // 0.45
                "did not use the sample rate of the second rule"
            )
        }

        // early return third rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "testing",
            "vip",
            "user-id",
            None,
        );
        // We will use a time in the middle of 10th and 11th.
        let result = match_against_rules(
            &config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert_trace_match!(result, 0.02, dsc, 3);
    }

    #[test]
    /// test that the correct match is performed when replay id is present in the dsc.
    fn test_sampling_match_with_trace_replay_id() {
        let event = mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "vip",
            "user-id",
            Some(Uuid::new_v4()),
        );

        let result = match_against_rules(
            &mocked_sampling_config_with_rules(vec![SamplingRule {
                condition: and(vec![not(eq_null("trace.replay_id"))]),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }]),
            &event,
            &dsc,
            Utc::now(),
        );
        assert_trace_match!(result, 1.0, dsc, 1)
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

    #[test]
    /// Test parse user
    fn parse_user() {
        let jsons = [
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user": {
                    "id": "some-id",
                    "segment": "all"
                }
            }"#,
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "some-id",
                "user_segment": "all"
            }"#,
            // testing some edgecases to see whether they behave as expected, but we don't actually
            // rely on this behavior anywhere (ignoring Hyrum's law). it would be fine for them to
            // change, we just have to be conscious about it.
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "",
                "user_segment": "",
                "user": {
                    "id": "some-id",
                    "segment": "all"
                }
            }"#,
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "some-id",
                "user_segment": "all",
                "user": {
                    "id": "bogus-id",
                    "segment": "nothing"
                }
            }"#,
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "some-id",
                "user_segment": "all",
                "user": null
            }"#,
        ];

        for json in jsons {
            let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
            assert_eq!(dsc.user.user_id, "some-id");
            assert_eq!(dsc.user.user_segment, "all");
        }
    }

    #[test]
    fn test_parse_user_partial() {
        // in that case we might have two different sdks merging data and we at least shouldn't mix
        // data together
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "user": {
                "segment": "all"
            }
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        insta::assert_ron_snapshot!(dsc, @r#"
        {
          "trace_id": "00000000-0000-0000-0000-000000000000",
          "public_key": "abd0f232775f45feab79864e580d160b",
          "release": None,
          "environment": None,
          "transaction": None,
          "user_id": "hello",
          "replay_id": None,
        }
        "#);
    }

    #[test]
    fn test_parse_sample_rate() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "0.5"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        insta::assert_ron_snapshot!(dsc, @r#"
        {
          "trace_id": "00000000-0000-0000-0000-000000000000",
          "public_key": "abd0f232775f45feab79864e580d160b",
          "release": None,
          "environment": None,
          "transaction": None,
          "sample_rate": "0.5",
          "user_id": "hello",
          "replay_id": None,
        }
        "#);
    }

    #[test]
    fn test_parse_sample_rate_scientific_notation() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "1e-5"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        insta::assert_ron_snapshot!(dsc, @r#"
        {
          "trace_id": "00000000-0000-0000-0000-000000000000",
          "public_key": "abd0f232775f45feab79864e580d160b",
          "release": None,
          "environment": None,
          "transaction": None,
          "sample_rate": "0.00001",
          "user_id": "hello",
          "replay_id": None,
        }
        "#);
    }

    #[test]
    fn test_parse_sample_rate_bogus() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "bogus"
        }
        "#;
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap_err();
    }

    #[test]
    fn test_parse_sample_rate_number() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": 0.1
        }
        "#;
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap_err();
    }

    #[test]
    fn test_parse_sample_rate_negative() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "-0.1"
        }
        "#;
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap_err();
    }

    #[test]
    fn test_parse_sampled_with_incoming_boolean() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": true
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null,
  "sampled": true
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_parse_sampled_with_incoming_boolean_as_string() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": "false"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null,
  "sampled": false
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_parse_sampled_with_incoming_invalid_boolean_as_string() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": "tru"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_parse_sampled_with_incoming_null_value() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": null
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_adjust_sample_rate() {
        let mut dsc = default_sampling_context();

        dsc.sample_rate = Some(0.0);
        assert_eq!(dsc.adjusted_sample_rate(0.5), 0.5);

        dsc.sample_rate = Some(1.0);
        assert_eq!(dsc.adjusted_sample_rate(0.5), 0.5);

        dsc.sample_rate = Some(0.1);
        assert_eq!(dsc.adjusted_sample_rate(0.5), 1.0);

        dsc.sample_rate = Some(0.5);
        assert_eq!(dsc.adjusted_sample_rate(0.1), 0.2);

        dsc.sample_rate = Some(-0.5);
        assert_eq!(dsc.adjusted_sample_rate(0.5), 0.5);
    }

    #[test]
    fn test_supported() {
        let rule: SamplingRule = serde_json::from_value(serde_json::json!({
            "id": 1,
            "type": "trace",
            "samplingValue": {"type": "sampleRate", "value": 1.0},
            "condition": {"op": "and", "inner": []}
        }))
        .unwrap();
        assert!(rule.supported());
    }

    #[test]
    fn test_unsupported_rule_type() {
        let rule: SamplingRule = serde_json::from_value(serde_json::json!({
            "id": 1,
            "type": "new_rule_type_unknown_to_this_relay",
            "samplingValue": {"type": "sampleRate", "value": 1.0},
            "condition": {"op": "and", "inner": []}
        }))
        .unwrap();
        assert!(!rule.supported());
    }

    #[test]
    /// Tests the merged config of the two configs with rules.
    fn test_get_merged_config_with_rules_in_both_project_config_and_root_project_config() {
        assert_rule_ids_eq!(
            [1, 7],
            merge_root_and_non_root_configs_with(
                vec![
                    mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                    mocked_sampling_rule(3, RuleType::Trace, 0.3),
                    mocked_sampling_rule(4, RuleType::Unsupported, 0.1),
                ],
                vec![
                    mocked_sampling_rule(5, RuleType::Transaction, 0.4),
                    mocked_sampling_rule(7, RuleType::Trace, 0.6),
                    mocked_sampling_rule(8, RuleType::Unsupported, 0.1),
                ],
            )
        );
    }

    #[test]
    /// Tests the merged config of the two configs without rules.
    fn test_get_merged_config_with_no_rules_in_both_project_config_and_root_project_config() {
        assert!(merge_root_and_non_root_configs_with(vec![], vec![]).is_empty());
    }

    #[test]
    /// Tests the merged config of the project config with rules and the root project config
    /// without rules.
    fn test_get_merged_config_with_rules_in_project_config_and_no_rules_in_root_project_config() {
        assert_rule_ids_eq!(
            [1],
            merge_root_and_non_root_configs_with(
                vec![
                    mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                    mocked_sampling_rule(3, RuleType::Trace, 0.3),
                    mocked_sampling_rule(4, RuleType::Unsupported, 0.1),
                ],
                vec![],
            )
        );
    }

    #[test]
    /// Tests the merged config of the project config without rules and the root project config
    /// with rules.
    fn test_get_merged_config_with_no_rules_in_project_config_and_with_rules_in_root_project_config(
    ) {
        assert_rule_ids_eq!(
            [6],
            merge_root_and_non_root_configs_with(
                vec![],
                vec![
                    mocked_sampling_rule(4, RuleType::Transaction, 0.4),
                    mocked_sampling_rule(6, RuleType::Trace, 0.6),
                    mocked_sampling_rule(7, RuleType::Unsupported, 0.1),
                ]
            )
        );
    }

    #[test]
    /// Tests that no match is done when there are no matching rules.
    fn test_get_sampling_match_result_with_no_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            None,
            Some(&event),
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that matching is still done on the transaction rules in case trace params are invalid.
    fn test_get_sampling_match_result_with_invalid_trace_params() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);

        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            None,
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);

        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the project sampling config.
    fn test_get_sampling_match_result_with_project_config_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the root project sampling config.
    fn test_get_sampling_match_result_with_root_project_config_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let event = mocked_event(EventType::Transaction, "my_transaction", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_trace_match!(result, 1.0, dsc, 6);
    }

    #[test]
    /// Tests that the multiple matches are done across root and non-root project sampling configs.
    fn test_get_sampling_match_result_with_both_project_configs_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None);
        let event = mocked_event(EventType::Transaction, "bar", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_trace_match!(result, 0.75, dsc, 2, 5, 7);
    }

    #[test]
    /// Tests that a match is done when no dynamic sampling context and root project state are
    /// available.
    fn test_get_sampling_match_result_with_no_dynamic_sampling_context_and_no_root_project_state() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "foo", "1.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            None,
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a match is done and the sample rate is adjusted when sampling mode is total.
    fn test_get_sampling_match_result_with_total_sampling_mode_in_project_state() {
        let sampling_config = mocked_sampling_config(SamplingMode::Total);
        let root_project_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);
        let dsc = mocked_simple_dynamic_sampling_context(Some(0.8), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.625, event, 3);
    }

    #[test]
    /// Tests that the correct match is raised in case we have unsupported rules with processing both
    /// enabled and disabled.
    fn test_get_sampling_match_result_with_unsupported_rules() {
        let mut sampling_config = mocked_sampling_config(SamplingMode::Received);
        add_sampling_rule_to_config(
            &mut sampling_config,
            SamplingRule {
                condition: RuleCondition::Unsupported,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );

        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = merge_configs_and_match(
            false,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_no_match!(result);

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a no match is raised in case we have an unsupported sampling mode and a match.
    fn test_get_sampling_match_result_with_unsupported_sampling_mode_and_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Unsupported);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Unsupported);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that only transaction rules are matched in case no root project or dsc are supplied.
    fn test_get_sampling_match_result_with_invalid_root_project_and_dsc_combination() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");

        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);

        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            None,
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that match is returned with sample rate value interpolated with linear decaying function.
    fn test_get_sampling_match_result_with_linear_decaying_function() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_transaction_match!(result, 0.75, event, 1);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_transaction_match!(result, 1.0, event, 1);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);
    }

    #[test]
    /// Tests that no match is returned when the linear decaying function has invalid time range.
    fn test_get_sampling_match_result_with_linear_decaying_function_and_invalid_time_range() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);
    }

    #[test]
    /// Tests that match is returned when there are multiple decaying rules with factor and sample rate.
    fn test_get_sampling_match_result_with_multiple_decaying_functions_with_factor_and_sample_rate()
    {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_decaying_sampling_rule(
                    1,
                    Some(now - DateDuration::days(1)),
                    Some(now + DateDuration::days(1)),
                    SamplingValue::Factor { value: 5.0 },
                    DecayingFunction::Linear { decayed_value: 1.0 },
                ),
                mocked_decaying_sampling_rule(
                    2,
                    Some(now - DateDuration::days(1)),
                    Some(now + DateDuration::days(1)),
                    SamplingValue::SampleRate { value: 0.3 },
                    DecayingFunction::Constant,
                ),
            ],
            mode: SamplingMode::Received,
        };

        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert!(result.is_some());
        if let Some(SamplingMatch {
            sample_rate,
            seed,
            matched_rule_ids,
        }) = result
        {
            assert!((sample_rate - 0.9).abs() < f64::EPSILON);
            assert_eq!(seed, event.id.0.unwrap().0);
            assert_eq!(matched_rule_ids, MatchedRuleIds(vec![RuleId(1), RuleId(2)]))
        }
    }

    #[test]
    /// Tests that match is returned when there are only dsc and root sampling config.
    fn test_get_sampling_match_result_with_no_event_and_with_dsc() {
        let now = Utc::now();

        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let root_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);
        let result = merge_configs_and_match(
            true,
            None,
            Some(&root_sampling_config),
            Some(&dsc),
            None,
            now,
        );
        assert_trace_match!(result, 1.0, dsc, 6);
    }

    #[test]
    /// Tests that no match is returned when no event and no dsc are passed.
    fn test_get_sampling_match_result_with_no_event_and_no_dsc() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_sampling_config),
            None,
            None,
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that no match is returned when no sampling configs are passed.
    fn test_get_sampling_match_result_with_no_sampling_configs() {
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));

        let result =
            merge_configs_and_match(true, None, None, Some(&dsc), Some(&event), Utc::now());
        assert_no_match!(result);
    }
}
