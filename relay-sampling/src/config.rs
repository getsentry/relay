//! Dynamic sampling rule configuration.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::condition::RuleCondition;
use crate::utils;

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
    #[serde(default, skip_serializing_if = "utils::is_default")]
    pub mode: SamplingMode,
}

impl SamplingConfig {
    /// Returns `true` if any of the rules in this configuration is unsupported.
    pub fn unsupported(&self) -> bool {
        !self.rules_v2.iter().all(SamplingRule::supported)
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
    #[serde(default, skip_serializing_if = "utils::is_default")]
    pub decaying_fn: DecayingFunction,
}

impl SamplingRule {
    fn supported(&self) -> bool {
        self.condition.supported() && self.ty != RuleType::Unsupported
    }
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
        /// The fator to apply on another matched sample rate.
        value: f64,
    },
}

impl SamplingValue {
    pub(crate) fn value(&self) -> f64 {
        *match self {
            SamplingValue::SampleRate { value } => value,
            SamplingValue::Factor { value } => value,
        }
    }
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Ord, PartialOrd)]
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

    /// This represents a query that should be biased, if the reservoir limit hasn't been reached,
    /// we will keep the event, otherwise, normal sampling rates apply.
    InspectionBias {
        /// The limit to how many transactions we will fill our reservoir before we stop the bias.
        reservoir_limit: usize,
    },
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

    use crate::condition::AndCondition;

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
                condition: RuleCondition::And(AndCondition { inner: vec![] }),
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
}
