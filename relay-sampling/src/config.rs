//! Dynamic sampling rule configuration.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use relay_protocol::RuleCondition;

/// Maximum supported version of dynamic sampling.
///
/// The version is an integer scalar, incremented by one on each new version:
///  - 1: Initial version that uses `rules_v2`.
///  - 2: Moves back to `rules` and adds support for `RuleConfigs` with string comparisons.
const SAMPLING_CONFIG_VERSION: u16 = 2;

/// Represents the dynamic sampling configuration available to a project.
///
/// Note: This comes from the organization data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingConfig {
    /// The required version to run dynamic sampling.
    ///
    /// Defaults to legacy version (`1`) when missing.
    #[serde(default = "SamplingConfig::legacy_version")]
    pub version: u16,

    /// The ordered sampling rules for the project.
    #[serde(default)]
    pub rules: Vec<SamplingRule>,

    /// **Deprecated**. The ordered sampling rules for the project in legacy format.
    ///
    /// Removed in favor of `Self::rules` in version `2`. This field remains here to parse rules
    /// from old Sentry instances and convert them into the new format. The legacy format contained
    /// both an empty `rules` as well as the actual rules in `rules_v2`. During normalization, these
    /// two arrays are merged together.
    #[serde(default, skip_serializing)]
    pub rules_v2: Vec<SamplingRule>,

    /// Defines which population of items a dynamic sample rate applies to.
    #[serde(default, skip_serializing_if = "is_default")]
    pub mode: SamplingMode,
}

impl SamplingConfig {
    /// Creates an enabled configuration with empty defaults and the latest version.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if any of the rules in this configuration is unsupported.
    pub fn unsupported(&self) -> bool {
        debug_assert!(self.version > 1, "SamplingConfig not normalized");
        self.version > SAMPLING_CONFIG_VERSION || !self.rules.iter().all(SamplingRule::supported)
    }

    /// Filters the sampling rules by the given [`RuleType`].
    pub fn filter_rules(&self, rule_type: RuleType) -> impl Iterator<Item = &SamplingRule> {
        self.rules.iter().filter(move |rule| rule.ty == rule_type)
    }

    /// Upgrades legacy sampling configs into the latest format.
    pub fn normalize(&mut self) {
        if self.version == Self::legacy_version() {
            self.rules.append(&mut self.rules_v2);
            self.version = SAMPLING_CONFIG_VERSION;
        }
    }

    const fn legacy_version() -> u16 {
        1
    }
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            version: SAMPLING_CONFIG_VERSION,
            rules: vec![],
            rules_v2: vec![],
            mode: SamplingMode::default(),
        }
    }
}

/// A sampling rule as it is deserialized from the project configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    /// A condition to match for this sampling rule.
    ///
    /// Sampling rules do not run if their condition does not match.
    pub condition: RuleCondition,

    /// The sample rate to apply when this rule matches.
    pub sampling_value: SamplingValue,

    /// The rule type declares what to apply a dynamic sampling rule to and how.
    #[serde(rename = "type")]
    pub ty: RuleType,

    /// The unique identifier of this rule.
    pub id: RuleId,

    /// The time range the rule should be applicable in.
    ///
    /// The time range is open on both ends by default. If a time range is
    /// closed on at least one end, the rule is considered a decaying rule.
    #[serde(default, skip_serializing_if = "TimeRange::is_empty")]
    pub time_range: TimeRange,

    /// Declares how to interpolate the sample rate for rules with bounded time range.
    #[serde(default, skip_serializing_if = "is_default")]
    pub decaying_fn: DecayingFunction,
}

impl SamplingRule {
    fn supported(&self) -> bool {
        self.condition.supported() && self.ty != RuleType::Unsupported
    }

    /// Applies its decaying function to the given sample rate.
    pub fn apply_decaying_fn(&self, sample_rate: f64, now: DateTime<Utc>) -> Option<f64> {
        self.decaying_fn
            .adjust_sample_rate(sample_rate, now, self.time_range)
    }
}

/// Returns `true` if this value is equal to `Default::default()`.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

/// A sampling strategy definition.
///
/// A sampling strategy refers to the strategy that we want to use for sampling a specific rule.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum SamplingValue {
    /// A direct sample rate to apply.
    ///
    /// A rule with a sample rate will be matched and the final sample rate will be computed by
    /// multiplying its sample rate with the accumulated factors from previous rules.
    SampleRate {
        /// The sample rate to apply to the rule.
        value: f64,
    },

    /// A factor to apply on a subsequently matching rule.
    ///
    /// A rule with a factor will be matched and the matching will continue onto the next rules
    /// until a sample rate rule is found. The matched rule's factor will be multiplied with the
    /// accumulated factors before moving onto the next possible match.
    Factor {
        /// The factor to apply on another matched sample rate.
        value: f64,
    },

    /// A reservoir limit.
    ///
    /// A rule with a reservoir limit will be sampled if the rule have been matched fewer times
    /// than the limit.
    Reservoir {
        /// The limit of how many times this rule will be sampled before this rule is invalid.
        limit: i64,
    },
}

/// Defines what a dynamic sampling rule applies to.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum RuleType {
    /// A trace rule matches on the [`DynamicSamplingContext`](crate::DynamicSamplingContext) and
    /// applies to all transactions in a trace.
    Trace,
    /// A transaction rule matches directly on the transaction event independent of the trace.
    Transaction,
    // NOTE: If you add a new `RuleType` that is not supposed to sample transactions, you need to
    // edit the `sample_envelope` function in `EnvelopeProcessorService`.
    /// If the sampling config contains new rule types, do not sample at all.
    #[serde(other)]
    Unsupported,
}

/// The identifier of a [`SamplingRule`].
///
/// This number must be unique within a Sentry organization, as it is recorded in outcomes and used
/// to infer which sampling rule caused data to be dropped.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    /// The inclusive start of the time range.
    pub start: Option<DateTime<Utc>>,

    /// The exclusive end of the time range.
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

/// Specifies how to interpolate sample rates for rules with bounded time window.
#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum DecayingFunction {
    /// Apply linear interpolation of the sample rate in the time window.
    ///
    /// The rule will start to apply with the configured sample rate at the beginning of the time
    /// window and end with `decayed_value` at the end of the time window.
    #[serde(rename_all = "camelCase")]
    Linear {
        /// The target value at the end of the time window.
        decayed_value: f64,
    },

    /// Apply the sample rate of the rule for the full time window with hard cutoff.
    #[default]
    Constant,
}

impl DecayingFunction {
    /// Applies the decaying function to the given sample rate.
    pub fn adjust_sample_rate(
        &self,
        sample_rate: f64,
        now: DateTime<Utc>,
        time_range: TimeRange,
    ) -> Option<f64> {
        match self {
            DecayingFunction::Linear { decayed_value } => {
                let (Some(start), Some(end)) = (time_range.start, time_range.end) else {
                    return None;
                };

                if sample_rate < *decayed_value {
                    return None;
                }

                let now = now.timestamp() as f64;
                let start = start.timestamp() as f64;
                let end = end.timestamp() as f64;

                let progress_ratio = ((now - start) / (end - start)).clamp(0.0, 1.0);

                // This interval will always be < 0.
                let interval = decayed_value - sample_rate;
                Some(sample_rate + (interval * progress_ratio))
            }
            DecayingFunction::Constant => Some(sample_rate),
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

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn config_deserialize() {
        let json = include_str!("../tests/fixtures/sampling_config.json");
        serde_json::from_str::<SamplingConfig>(json).unwrap();
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
    fn test_legacy_deserialization() {
        let serialized_rule = r#"{
               "rules": [],
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
        let mut config: SamplingConfig = serde_json::from_str(serialized_rule).unwrap();
        config.normalize();

        // We want to make sure that we serialize an empty array of rule, irrespectively of the
        // received payload.
        assert_eq!(config.version, SAMPLING_CONFIG_VERSION);
        assert_eq!(
            config.rules[0].sampling_value,
            SamplingValue::SampleRate { value: 0.5 }
        );
        assert!(config.rules_v2.is_empty());
    }

    #[test]
    fn test_sampling_config_with_rules_and_rules_v2_serialization() {
        let config = SamplingConfig {
            rules: vec![SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            ..SamplingConfig::new()
        };

        let serialized_config = serde_json::to_string_pretty(&config).unwrap();
        let expected_serialized_config = r#"{
  "version": 2,
  "rules": [
    {
      "condition": {
        "op": "and",
        "inner": []
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

    /// Checks that the sample rate stays constant if `DecayingFunction::Constant` is set.
    #[test]
    fn test_decay_fn_constant() {
        let sample_rate = 0.5;

        assert_eq!(
            DecayingFunction::Constant.adjust_sample_rate(
                sample_rate,
                Utc::now(),
                TimeRange::default()
            ),
            Some(sample_rate)
        );
    }

    /// Checks if the sample rate decays linearly if `DecayingFunction::Linear` is set.
    #[test]
    fn test_decay_fn_linear() {
        let decaying_fn = DecayingFunction::Linear { decayed_value: 0.5 };
        let time_range = TimeRange {
            start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
            end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
        };

        let start = Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap();
        let halfway = Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(1970, 10, 11, 23, 59, 59).unwrap();

        // At the start of the time range, sample rate is equal to the rule's initial sampling value.
        assert_eq!(
            decaying_fn.adjust_sample_rate(1.0, start, time_range),
            Some(1.0)
        );

        // Halfway in the time range, the value is exactly between 1.0 and 0.5.
        assert_eq!(
            decaying_fn.adjust_sample_rate(1.0, halfway, time_range),
            Some(0.75)
        );

        // Approaches 0.5 at the end.
        assert_eq!(
            decaying_fn.adjust_sample_rate(1.0, end, time_range),
            // It won't go to exactly 0.5 because the time range is end-exclusive.
            Some(0.5000028935185186)
        );

        // If the end or beginning is missing, the linear decay shouldn't be run.
        let mut time_range_without_start = time_range;
        time_range_without_start.start = None;

        assert!(decaying_fn
            .adjust_sample_rate(1.0, halfway, time_range_without_start)
            .is_none());

        let mut time_range_without_end = time_range;
        time_range_without_end.end = None;

        assert!(decaying_fn
            .adjust_sample_rate(1.0, halfway, time_range_without_end)
            .is_none());
    }
}
