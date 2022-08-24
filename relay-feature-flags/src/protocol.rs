use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TagValue {
    String(String),
    List(Vec<String>),
}

impl<T: Into<String>> From<T> for TagValue {
    fn from(value: T) -> Self {
        TagValue::String(value.into())
    }
}

type TagMap = BTreeMap<String, TagValue>;

pub use serde_json::Value as PayloadType;

/// Represents all feature flags of a project.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureDump {
    pub feature_flags: BTreeMap<String, FeatureFlag>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlagKind {
    Boolean,
    Number,
    String,
}

/// A single feature flag.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureFlag {
    #[serde(default, skip_serializing_if = "TagMap::is_empty")]
    pub tags: TagMap,
    pub evaluation: Vec<EvaluationRule>,
    pub kind: FlagKind,
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

impl From<f32> for FlagValue {
    fn from(v: f32) -> Self {
        Self::F32(v)
    }
}

impl From<i64> for FlagValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<bool> for FlagValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<String> for FlagValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
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
    /// The optional payload to carry with the evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<PayloadType>,
    /// The tags that need to match for this evaluation rule.
    #[serde(default, skip_serializing_if = "TagMap::is_empty")]
    pub tags: TagMap,
}
