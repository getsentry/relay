use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

type TagMap = BTreeMap<String, String>;

/// Represents all feature flags of a project.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureDump {
    pub feature_flags: BTreeMap<String, FeatureFlag>,
}

/// A single feature flag.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureFlag {
    #[serde(default)]
    pub tags: TagMap,
    pub evaluation: Vec<EvaluationRule>,
}

/// Potential values for a feature flag post evaluation.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FlagValue {
    String(String),
    Bool(bool),
    I64(i64),
    F32(f32),
}

/// The type of evaluation.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationType {
    /// If tags match, return value.
    Match,
    /// If tags match, run a sticky rollout evaluation and return value.
    Rollout,
}

/// Represents a single evaluation rule.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvaluationRule {
    /// The type of the evaluation.
    #[serde(rename = "type")]
    pub ty: EvaluationType,
    /// For rollouts the configured percentage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentage: Option<f32>,
    /// The result value.
    pub result: Option<FlagValue>,
    /// The tags that need to match for this evaluation rule.
    #[serde(default)]
    pub tags: TagMap,
}
