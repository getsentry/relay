use std::hash::Hash;
use std::{collections::HashMap, sync::LazyLock};

use regex::Regex;
use relay_base_schema::metrics::MetricUnit;
use relay_event_schema::protocol::{Event, VALID_PLATFORMS};
use relay_pattern::Pattern;
use relay_protocol::{FiniteF64, RuleCondition};
use serde::{Deserialize, Serialize};

pub mod breakdowns;
pub mod contexts;
pub mod nel;
pub mod request;
pub mod span;
pub mod user_agent;
pub mod utils;

/// Defines a builtin measurement.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct BuiltinMeasurementKey {
    name: String,
    unit: MetricUnit,
    #[serde(skip_serializing_if = "is_false")]
    allow_negative: bool,
}

fn is_false(b: &bool) -> bool {
    !b
}

impl BuiltinMeasurementKey {
    /// Creates a new [`BuiltinMeasurementKey`].
    pub fn new(name: impl Into<String>, unit: MetricUnit) -> Self {
        Self {
            name: name.into(),
            unit,
            allow_negative: false,
        }
    }

    /// Returns the name of the built in measurement key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the unit of the built in measurement key.
    pub fn unit(&self) -> &MetricUnit {
        &self.unit
    }

    /// Return true if the built in measurement key allows negative values.
    pub fn allow_negative(&self) -> &bool {
        &self.allow_negative
    }
}

/// Configuration for measurements normalization.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash)]
#[serde(default, rename_all = "camelCase")]
pub struct MeasurementsConfig {
    /// A list of measurements that are built-in and are not subject to custom measurement limits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builtin_measurements: Vec<BuiltinMeasurementKey>,

    /// The maximum number of measurements allowed per event that are not known measurements.
    pub max_custom_measurements: usize,
}

impl MeasurementsConfig {
    /// The length of a full measurement MRI, minus the name and the unit. This length is the same
    /// for every measurement-mri.
    pub const MEASUREMENT_MRI_OVERHEAD: usize = 29;
}

/// Returns `true` if the given platform string is a known platform identifier.
///
/// See [`VALID_PLATFORMS`] for a list of all known platforms.
pub fn is_valid_platform(platform: &str) -> bool {
    VALID_PLATFORMS.contains(&platform)
}

/// Replaces snake_case app start spans op with dot.case op.
///
/// This is done for the affected React Native SDK versions (from 3 to 4.4).
pub fn normalize_app_start_spans(event: &mut Event) {
    if !event.sdk_name().eq("sentry.javascript.react-native")
        || !(event.sdk_version().starts_with("4.4")
            || event.sdk_version().starts_with("4.3")
            || event.sdk_version().starts_with("4.2")
            || event.sdk_version().starts_with("4.1")
            || event.sdk_version().starts_with("4.0")
            || event.sdk_version().starts_with('3'))
    {
        return;
    }

    if let Some(spans) = event.spans.value_mut() {
        for span in spans {
            if let Some(span) = span.value_mut()
                && let Some(op) = span.op.value()
            {
                if op == "app_start_cold" {
                    span.op.set_value(Some("app.start.cold".to_owned()));
                    break;
                } else if op == "app_start_warm" {
                    span.op.set_value(Some("app.start.warm".to_owned()));
                    break;
                }
            }
        }
    }
}

/// Container for global and project level [`MeasurementsConfig`]. The purpose is to handle
/// the merging logic.
#[derive(Clone, Debug)]
pub struct CombinedMeasurementsConfig<'a> {
    project: Option<&'a MeasurementsConfig>,
    global: Option<&'a MeasurementsConfig>,
}

impl<'a> CombinedMeasurementsConfig<'a> {
    /// Constructor for [`CombinedMeasurementsConfig`].
    pub fn new(
        project: Option<&'a MeasurementsConfig>,
        global: Option<&'a MeasurementsConfig>,
    ) -> Self {
        CombinedMeasurementsConfig { project, global }
    }

    /// Returns an iterator over the merged builtin measurement keys.
    ///
    /// Items from the project config are prioritized over global config, and
    /// there are no duplicates.
    pub fn builtin_measurement_keys(
        &'a self,
    ) -> impl Iterator<Item = &'a BuiltinMeasurementKey> + 'a {
        let project = self
            .project
            .map(|p| p.builtin_measurements.as_slice())
            .unwrap_or_default();

        let global = self
            .global
            .map(|g| g.builtin_measurements.as_slice())
            .unwrap_or_default();

        project
            .iter()
            .chain(global.iter().filter(|key| !project.contains(key)))
    }

    /// Gets the max custom measurements value from the [`MeasurementsConfig`] from project level or
    /// global level. If both of them are available, it will choose the most restrictive.
    pub fn max_custom_measurements(&'a self) -> Option<usize> {
        match (&self.project, &self.global) {
            (None, None) => None,
            (None, Some(global)) => Some(global.max_custom_measurements),
            (Some(project), None) => Some(project.max_custom_measurements),
            (Some(project), Some(global)) => Some(std::cmp::min(
                project.max_custom_measurements,
                global.max_custom_measurements,
            )),
        }
    }
}

/// Defines a weighted component for a performance score.
///
/// Weight is the % of score it can take up (eg. LCP is a max of 35% weight for desktops)
/// Currently also contains (p10, p50) which are used for log CDF normalization of the weight score
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PerformanceScoreWeightedComponent {
    /// Measurement (eg. measurements.lcp) to be matched against. If this measurement is missing the entire
    /// profile will be discarded.
    pub measurement: String,
    /// Weight [0,1.0] of this component in the performance score
    pub weight: FiniteF64,
    /// p10 used to define the log-normal for calculation
    pub p10: FiniteF64,
    /// Median used to define the log-normal for calculation
    pub p50: FiniteF64,
    /// Whether the measurement is optional. If the measurement is missing, performance score processing
    /// may still continue, and the weight will be 0.
    #[serde(default)]
    pub optional: bool,
}

/// Defines a profile for performance score.
///
/// A profile contains weights for a score of 100% and match against an event using a condition.
/// eg. Desktop vs. Mobile(web) profiles for better web vital score calculation.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceScoreProfile {
    /// Name of the profile, used for debugging and faceting multiple profiles
    pub name: Option<String>,
    /// Score components
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub score_components: Vec<PerformanceScoreWeightedComponent>,
    /// See [`RuleCondition`] for all available options to specify and combine conditions.
    pub condition: Option<RuleCondition>,
    /// The version of the profile, used to isolate changes to score calculations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Defines the performance configuration for the project.
///
/// Includes profiles matching different behaviour (desktop / mobile) and weights matching those
/// specific conditions.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PerformanceScoreConfig {
    /// List of performance profiles, only the first with matching conditions will be applied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<PerformanceScoreProfile>,
}

/// A mapping of AI model types (like GPT-4) to their respective costs.
///
/// This struct uses a dictionary-based cost structure with exact model ID keys and granular
/// token pricing.
///
/// Example JSON:
/// ```json
/// {
///   "version": 2,
///   "models": {
///     "gpt-4": {
///       "inputPerToken": 0.03,
///       "outputPerToken": 0.06,
///       "outputReasoningPerToken": 0.12,
///       "inputCachedPerToken": 0.015
///     }
///   }
/// }
/// ```
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCosts {
    /// The version of the model cost struct
    pub version: u16,

    /// The mappings of model ID => cost as a dictionary
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub models: HashMap<Pattern, ModelCostV2>,
}

impl ModelCosts {
    const SUPPORTED_VERSION: u16 = 2;

    /// `true` if the model costs are empty and the version is supported.
    pub fn is_empty(&self) -> bool {
        self.models.is_empty() || !self.is_enabled()
    }

    /// `false` if measurement and metrics extraction should be skipped.
    pub fn is_enabled(&self) -> bool {
        self.version == Self::SUPPORTED_VERSION
    }

    /// Gets the cost per token, if defined for the given model.
    pub fn cost_per_token(&self, model_id: &str) -> Option<&ModelCostV2> {
        if !self.is_enabled() {
            return None;
        }

        let normalized_model_id = normalize_ai_model_name(model_id);

        // First try exact match by creating a Pattern from the model_id
        if let Some(value) = self.models.get(normalized_model_id) {
            return Some(value);
        }

        // if there is not a direct match, try to find the match using a pattern
        // since the name is already normalized, there are still patterns where the
        // model name can have a prefix e.g. "us.antrophic.claude-sonnet-4" and this
        // will be matched via glob "*claude-sonnet-4"
        self.models.iter().find_map(|(key, value)| {
            if key.is_match(normalized_model_id) {
                Some(value)
            } else {
                None
            }
        })
    }
}

/// Regex that matches version and/or date patterns at the end of a model name
///
/// Examples matched:
/// - "-20250805-v1:0" in "claude-opus-4-1-20250805-v1:0" (both)
/// - "-20250514-v1:0" in "claude-sonnet-4-20250514-v1:0" (both)
/// - "_v2" in "model_v2" (version only)
/// - "-v1.0" in "gpt-4-v1.0" (version only)
/// - "-20250522" in "claude-4-sonnet-20250522" (date only)
/// - "-2025-06-10" in "o3-pro-2025-06-10" (date only)
/// - "@20241022" in "claude-3-5-haiku@20241022" (date only)
static VERSION_OR_DATE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        (
            ([-_@])                                    # Required separator before date (-, _, or @)
            (\d{4}[-/\.]\d{2}[-/\.]\d{2}|\d{8})        # Date: YYYY-MM-DD/YYYY/MM/DD/YYYY.MM.DD or YYYYMMDD
        )?                                              # Date is optional
        (
            [-_]                                        # Required separator before version (- or _)
            v\d+[:\.]?\d*                               # Version: v1, v1.0, v1:0
            ([:\-].*)?                                  # Optional trailing content
        )?                                              # Version is optional
        $  # Must be at the end of the string
        ",
    )
    .unwrap()
});

/// Normalizes an AI model name by stripping date and version patterns.
///
/// This function converts specific model versions into normalized names suitable for matching
/// against cost configurations, ensuring that different versions of the same model can share
/// cost information.
///
/// The normalization strips version and/or date patterns from the end of the string in a single pass.
///
/// Examples:
/// - "claude-4-sonnet-20250522" -> "claude-4-sonnet"
/// - "o3-pro-2025-06-10" -> "o3-pro"
/// - "claude-3-5-haiku@20241022" -> "claude-3-5-haiku"
/// - "claude-opus-4-1-20250805-v1:0" -> "claude-opus-4-1"
/// - "us.anthropic.claude-sonnet-4-20250514-v1:0" -> "us.anthropic.claude-sonnet-4"
/// - "gpt-4-v1.0" -> "gpt-4"
/// - "gpt-4-v1:0" -> "gpt-4"
/// - "model_v2" -> "model"
///
/// Version patterns:
/// - Must start with 'v': v1, v1.0, v1:0 (requires -, _ separator)
///
/// Date patterns:
/// - YYYYMMDD: 20250522
/// - YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD: 2025-06-10
///
/// Args:
///     model_id: The original model ID
///
/// Returns:
///     The normalized model name without date and version patterns
fn normalize_ai_model_name(model_id: &str) -> &str {
    if let Some(captures) = VERSION_OR_DATE_REGEX.captures(model_id) {
        let match_idx = captures.get(0).map(|m| m.start()).unwrap_or(model_id.len());
        &model_id[..match_idx]
    } else {
        model_id
    }
}

/// Version 2 of a mapping of AI model types (like GPT-4) to their respective costs.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelCostV2 {
    /// The cost per input token
    pub input_per_token: f64,
    /// The cost per output token
    pub output_per_token: f64,
    /// The cost per output reasoning token
    pub output_reasoning_per_token: f64,
    /// The cost per input cached token
    pub input_cached_per_token: f64,
}

/// A mapping of AI operation types from span.op to gen_ai.operation.type.
///
/// This struct uses a dictionary-based mapping structure with pattern-based span operation keys
/// and corresponding AI operation type values.
///
/// Example JSON:
/// ```json
/// {
///   "version": 1,
///   "operation_types": {
///     "gen_ai.execute_tool": "tool",
///     "gen_ai.handoff": "handoff",
///     "gen_ai.invoke_agent": "agent",
///   }
/// }
/// ```
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiOperationTypeMap {
    /// The version of the operation type mapping struct
    pub version: u16,

    /// The mappings of span.op => gen_ai.operation.type as a dictionary
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub operation_types: HashMap<Pattern, String>,
}

impl AiOperationTypeMap {
    const SUPPORTED_VERSION: u16 = 1;

    /// `true` if the operation type mapping is empty and the version is supported.
    pub fn is_empty(&self) -> bool {
        self.operation_types.is_empty() || !self.is_enabled()
    }

    /// `false` if operation type mapping should be skipped.
    pub fn is_enabled(&self) -> bool {
        self.version == Self::SUPPORTED_VERSION
    }

    /// Gets the AI operation type for the given span operation, if defined.
    pub fn get_operation_type(&self, span_op: &str) -> Option<&str> {
        if !self.is_enabled() {
            return None;
        }

        // try first direct match with span_op
        if let Some(value) = self.operation_types.get(span_op) {
            return Some(value.as_str());
        }

        // if there is not a direct match, try to find the match using a pattern
        let operation_type = self.operation_types.iter().find_map(|(key, value)| {
            if key.is_match(span_op) {
                Some(value)
            } else {
                None
            }
        });

        operation_type.map(String::as_str)
    }
}
#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use insta::{assert_debug_snapshot, assert_json_snapshot};
    use itertools::Itertools;
    use relay_base_schema::events::EventType;
    use relay_base_schema::metrics::DurationUnit;
    use relay_base_schema::spans::SpanStatus;
    use relay_event_schema::protocol::{
        ClientSdkInfo, Context, ContextInner, Contexts, DebugImage, DebugMeta, EventId, Exception,
        Frame, Geo, IpAddr, LenientString, Level, LogEntry, PairList, RawStacktrace, ReplayContext,
        Request, Span, Stacktrace, TagEntry, Tags, TraceContext, User, Values,
    };
    use relay_protocol::{
        Annotated, Error, ErrorKind, FromValue, Object, SerializableAnnotated, Value,
        assert_annotated_snapshot, get_path, get_value,
    };
    use serde_json::json;
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use crate::stacktrace::normalize_non_raw_frame;
    use crate::validation::validate_event;
    use crate::{EventValidationConfig, GeoIpLookup, NormalizationConfig, normalize_event};

    use super::*;

    /// Test that integer versions are handled correctly in the struct format
    #[test]
    fn test_model_cost_version_sent_as_number() {
        // Test integer version 2
        let original_v2 = r#"{"version":2,"models":{"gpt-4":{"inputPerToken":0.03,"outputPerToken":0.06,"outputReasoningPerToken":0.12,"inputCachedPerToken":0.015}}}"#;
        let deserialized_v2: ModelCosts = serde_json::from_str(original_v2).unwrap();
        assert_debug_snapshot!(
            deserialized_v2,
            @r#"
        ModelCosts {
            version: 2,
            models: {
                Pattern {
                    pattern: "gpt-4",
                    options: Options {
                        case_insensitive: false,
                    },
                    strategy: Literal(
                        Literal(
                            "gpt-4",
                        ),
                    ),
                }: ModelCostV2 {
                    input_per_token: 0.03,
                    output_per_token: 0.06,
                    output_reasoning_per_token: 0.12,
                    input_cached_per_token: 0.015,
                },
            },
        }
        "#,
        );

        // Test unknown integer version
        let original_unknown = r#"{"version":99,"models":{}}"#;
        let deserialized_unknown: ModelCosts = serde_json::from_str(original_unknown).unwrap();
        assert_eq!(deserialized_unknown.version, 99);
        assert!(!deserialized_unknown.is_enabled());
    }

    #[test]
    fn test_model_cost_config_v2() {
        let original = r#"{"version":2,"models":{"gpt-4":{"inputPerToken":0.03,"outputPerToken":0.06,"outputReasoningPerToken":0.12,"inputCachedPerToken":0.015}}}"#;
        let deserialized: ModelCosts = serde_json::from_str(original).unwrap();
        assert_debug_snapshot!(deserialized, @r#"
        ModelCosts {
            version: 2,
            models: {
                Pattern {
                    pattern: "gpt-4",
                    options: Options {
                        case_insensitive: false,
                    },
                    strategy: Literal(
                        Literal(
                            "gpt-4",
                        ),
                    ),
                }: ModelCostV2 {
                    input_per_token: 0.03,
                    output_per_token: 0.06,
                    output_reasoning_per_token: 0.12,
                    input_cached_per_token: 0.015,
                },
            },
        }
        "#);

        let serialized = serde_json::to_string(&deserialized).unwrap();
        assert_eq!(&serialized, original);
    }

    #[test]
    fn test_model_cost_functionality_v2() {
        // Test V2 functionality
        let mut models_map = HashMap::new();
        models_map.insert(
            Pattern::new("gpt-4").unwrap(),
            ModelCostV2 {
                input_per_token: 0.03,
                output_per_token: 0.06,
                output_reasoning_per_token: 0.12,
                input_cached_per_token: 0.015,
            },
        );
        let v2_config = ModelCosts {
            version: 2,
            models: models_map,
        };
        assert!(v2_config.is_enabled());
        let cost = v2_config.cost_per_token("gpt-4").unwrap();
        assert_eq!(
            cost,
            &ModelCostV2 {
                input_per_token: 0.03,
                output_per_token: 0.06,
                output_reasoning_per_token: 0.12,
                input_cached_per_token: 0.015,
            }
        );
    }

    #[test]
    fn test_model_cost_glob_matching() {
        // Test glob matching functionality in cost_per_token
        let mut models_map = HashMap::new();
        models_map.insert(
            Pattern::new("gpt-4*").unwrap(),
            ModelCostV2 {
                input_per_token: 0.03,
                output_per_token: 0.06,
                output_reasoning_per_token: 0.12,
                input_cached_per_token: 0.015,
            },
        );
        models_map.insert(
            Pattern::new("gpt-4-2xxx").unwrap(),
            ModelCostV2 {
                input_per_token: 0.0007,
                output_per_token: 0.0008,
                output_reasoning_per_token: 0.0016,
                input_cached_per_token: 0.00035,
            },
        );

        let v2_config = ModelCosts {
            version: 2,
            models: models_map,
        };
        assert!(v2_config.is_enabled());

        // Test glob matching with gpt-4 variants (prefix matching)
        let cost = v2_config.cost_per_token("gpt-4-v1").unwrap();
        assert_eq!(
            cost,
            &ModelCostV2 {
                input_per_token: 0.03,
                output_per_token: 0.06,
                output_reasoning_per_token: 0.12,
                input_cached_per_token: 0.015,
            }
        );

        let cost = v2_config.cost_per_token("gpt-4-2xxx").unwrap();
        assert_eq!(
            cost,
            &ModelCostV2 {
                input_per_token: 0.0007,
                output_per_token: 0.0008,
                output_reasoning_per_token: 0.0016,
                input_cached_per_token: 0.00035,
            }
        );

        assert_eq!(v2_config.cost_per_token("unknown-model"), None);
    }

    #[test]
    fn test_model_cost_unknown_version() {
        // Test that unknown versions are handled properly
        let unknown_version_json = r#"{"version":3,"models":{"some-model":{"inputPerToken":0.01,"outputPerToken":0.02,"outputReasoningPerToken":0.03,"inputCachedPerToken":0.005}}}"#;
        let deserialized: ModelCosts = serde_json::from_str(unknown_version_json).unwrap();
        assert_eq!(deserialized.version, 3);
        assert!(!deserialized.is_enabled());
        assert_eq!(deserialized.cost_per_token("some-model"), None);

        // Test version 0 (invalid)
        let version_zero_json = r#"{"version":0,"models":{}}"#;
        let deserialized: ModelCosts = serde_json::from_str(version_zero_json).unwrap();
        assert_eq!(deserialized.version, 0);
        assert!(!deserialized.is_enabled());
    }

    #[test]
    fn test_ai_operation_type_map_serialization() {
        // Test serialization and deserialization with patterns
        let mut operation_types = HashMap::new();
        operation_types.insert(
            Pattern::new("gen_ai.chat*").unwrap(),
            "Inference".to_owned(),
        );
        operation_types.insert(
            Pattern::new("gen_ai.execute_tool").unwrap(),
            "Tool".to_owned(),
        );

        let original = AiOperationTypeMap {
            version: 1,
            operation_types,
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: AiOperationTypeMap = serde_json::from_str(&json).unwrap();

        assert!(deserialized.is_enabled());
        assert_eq!(
            deserialized.get_operation_type("gen_ai.chat.completions"),
            Some("Inference")
        );
        assert_eq!(
            deserialized.get_operation_type("gen_ai.execute_tool"),
            Some("Tool")
        );
        assert_eq!(deserialized.get_operation_type("unknown_op"), None);
    }

    #[test]
    fn test_ai_operation_type_map_pattern_matching() {
        let mut operation_types = HashMap::new();
        operation_types.insert(Pattern::new("gen_ai.*").unwrap(), "default".to_owned());
        operation_types.insert(Pattern::new("gen_ai.chat").unwrap(), "chat".to_owned());

        let map = AiOperationTypeMap {
            version: 1,
            operation_types,
        };

        let result = map.get_operation_type("gen_ai.chat");
        assert!(Some("chat") == result);

        let result = map.get_operation_type("gen_ai.chat.completions");
        assert!(Some("default") == result);

        assert_eq!(map.get_operation_type("gen_ai.other"), Some("default"));

        assert_eq!(map.get_operation_type("other.operation"), None);
    }

    #[test]
    fn test_normalize_ai_model_name() {
        // Test date patterns
        assert_eq!(
            normalize_ai_model_name("claude-4-sonnet-20250522"),
            "claude-4-sonnet"
        );
        assert_eq!(normalize_ai_model_name("o3-pro-2025-06-10"), "o3-pro");
        assert_eq!(
            normalize_ai_model_name("claude-3-5-haiku@20241022"),
            "claude-3-5-haiku"
        );

        // Test date with version patterns
        assert_eq!(
            normalize_ai_model_name("claude-opus-4-1-20250805-v1:0"),
            "claude-opus-4-1"
        );
        assert_eq!(
            normalize_ai_model_name("us.anthropic.claude-sonnet-4-20250514-v1:0"),
            "us.anthropic.claude-sonnet-4"
        );

        // Test standalone version patterns with 'v' prefix
        assert_eq!(normalize_ai_model_name("gpt-4-v1.0"), "gpt-4");
        assert_eq!(normalize_ai_model_name("gpt-4-v1:0"), "gpt-4");
        assert_eq!(normalize_ai_model_name("gpt-4-v1"), "gpt-4");
        assert_eq!(normalize_ai_model_name("model_v2"), "model");

        // Test that version patterns WITHOUT 'v' prefix are NOT stripped
        assert_eq!(normalize_ai_model_name("gpt-4.5"), "gpt-4.5");

        // Test that if version without 'v' comes after date, nothing is stripped
        // (date regex requires end of string, so date won't match if followed by version)
        assert_eq!(
            normalize_ai_model_name("anthropic.claude-3-5-sonnet-20241022-v2:0"),
            "anthropic.claude-3-5-sonnet"
        );

        // Test no pattern (should return original)
        assert_eq!(normalize_ai_model_name("gpt-4"), "gpt-4");
        assert_eq!(normalize_ai_model_name("claude-3-opus"), "claude-3-opus");
    }

    #[test]
    fn test_merge_builtin_measurement_keys() {
        let foo = BuiltinMeasurementKey::new("foo", MetricUnit::Duration(DurationUnit::Hour));
        let bar = BuiltinMeasurementKey::new("bar", MetricUnit::Duration(DurationUnit::Day));
        let baz = BuiltinMeasurementKey::new("baz", MetricUnit::Duration(DurationUnit::Week));

        let proj = MeasurementsConfig {
            builtin_measurements: vec![foo.clone(), bar.clone()],
            max_custom_measurements: 4,
        };

        let glob = MeasurementsConfig {
            // The 'bar' here will be ignored since it's a duplicate from the project level.
            builtin_measurements: vec![baz.clone(), bar.clone()],
            max_custom_measurements: 4,
        };
        let dynamic_config = CombinedMeasurementsConfig::new(Some(&proj), Some(&glob));

        let keys = dynamic_config.builtin_measurement_keys().collect_vec();

        assert_eq!(keys, vec![&foo, &bar, &baz]);
    }

    #[test]
    fn test_max_custom_measurement() {
        // Empty configs will return a None value for max measurements.
        let dynamic_config = CombinedMeasurementsConfig::new(None, None);
        assert!(dynamic_config.max_custom_measurements().is_none());

        let proj = MeasurementsConfig {
            builtin_measurements: vec![],
            max_custom_measurements: 3,
        };

        let glob = MeasurementsConfig {
            builtin_measurements: vec![],
            max_custom_measurements: 4,
        };

        // If only project level measurement config is there, return its max custom measurement variable.
        let dynamic_config = CombinedMeasurementsConfig::new(Some(&proj), None);
        assert_eq!(dynamic_config.max_custom_measurements().unwrap(), 3);

        // Same logic for when only global level measurement config exists.
        let dynamic_config = CombinedMeasurementsConfig::new(None, Some(&glob));
        assert_eq!(dynamic_config.max_custom_measurements().unwrap(), 4);

        // If both is available, pick the smallest number.
        let dynamic_config = CombinedMeasurementsConfig::new(Some(&proj), Some(&glob));
        assert_eq!(dynamic_config.max_custom_measurements().unwrap(), 3);
    }

    #[test]
    fn test_geo_from_ip_address() {
        let lookup = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();

        let json = r#"{
            "user": {
                "ip_address": "2.125.160.216"
            }
        }"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );

        let expected = Annotated::new(Geo {
            country_code: Annotated::new("GB".to_owned()),
            city: Annotated::new("Boxford".to_owned()),
            subdivision: Annotated::new("England".to_owned()),
            region: Annotated::new("United Kingdom".to_owned()),
            ..Geo::default()
        });
        assert_eq!(get_value!(event.user!).geo, expected);
    }

    #[test]
    fn test_user_ip_from_remote_addr() {
        let mut event = Annotated::new(Event {
            request: Annotated::from(Request {
                env: Annotated::new({
                    let mut map = Object::new();
                    map.insert(
                        "REMOTE_ADDR".to_owned(),
                        Annotated::new(Value::String("2.125.160.216".to_owned())),
                    );
                    map
                }),
                ..Request::default()
            }),
            platform: Annotated::new("javascript".to_owned()),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        let ip_addr = get_value!(event.user.ip_address!);
        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_owned()));
    }

    #[test]
    fn test_user_ip_from_invalid_remote_addr() {
        let mut event = Annotated::new(Event {
            request: Annotated::from(Request {
                env: Annotated::new({
                    let mut map = Object::new();
                    map.insert(
                        "REMOTE_ADDR".to_owned(),
                        Annotated::new(Value::String("whoops".to_owned())),
                    );
                    map
                }),
                ..Request::default()
            }),
            platform: Annotated::new("javascript".to_owned()),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(Annotated::empty(), event.value().unwrap().user);
    }

    #[test]
    fn test_user_ip_from_client_ip_without_auto() {
        let mut event = Annotated::new(Event {
            platform: Annotated::new("javascript".to_owned()),
            ..Default::default()
        });

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                infer_ip_address: true,
                ..Default::default()
            },
        );

        let ip_addr = get_value!(event.user.ip_address!);
        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_owned()));
    }

    #[test]
    fn test_user_ip_from_client_ip_with_auto() {
        let mut event = Annotated::new(Event {
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr::auto()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();

        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                geoip_lookup: Some(&geo),
                infer_ip_address: true,
                ..Default::default()
            },
        );

        let user = get_value!(event.user!);
        let ip_addr = user.ip_address.value().expect("ip address missing");

        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_owned()));
        assert!(user.geo.value().is_some());
    }

    #[test]
    fn test_user_ip_from_client_ip_without_appropriate_platform() {
        let mut event = Annotated::new(Event::default());

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();
        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                geoip_lookup: Some(&geo),
                ..Default::default()
            },
        );

        let user = get_value!(event.user!);
        assert!(user.ip_address.value().is_none());
        assert!(user.geo.value().is_some());
    }

    #[test]
    fn test_geo_present_if_ip_inferring_disabled() {
        let mut event = Annotated::new(Event {
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr::auto()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();
        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                geoip_lookup: Some(&geo),
                infer_ip_address: false,
                ..Default::default()
            },
        );

        let user = get_value!(event.user!);
        assert!(user.ip_address.value().unwrap().is_auto());
        assert!(user.geo.value().is_some());
    }

    #[test]
    fn test_geo_and_ip_present_if_ip_inferring_enabled() {
        let mut event = Annotated::new(Event {
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr::auto()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();
        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                geoip_lookup: Some(&geo),
                infer_ip_address: true,
                ..Default::default()
            },
        );

        let user = get_value!(event.user!);
        assert_eq!(
            user.ip_address.value(),
            Some(&IpAddr::parse("2.125.160.216").unwrap())
        );
        assert!(user.geo.value().is_some());
    }

    #[test]
    fn test_event_level_defaulted() {
        let mut event = Annotated::new(Event::default());
        normalize_event(&mut event, &NormalizationConfig::default());
        assert_eq!(get_value!(event.level), Some(&Level::Error));
    }

    #[test]
    fn test_transaction_level_untouched() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(1987, 6, 5, 4, 3, 2).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1987, 6, 5, 4, 3, 2).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Event::default()
        });
        normalize_event(&mut event, &NormalizationConfig::default());
        assert_eq!(get_value!(event.level), Some(&Level::Info));
    }

    #[test]
    fn test_environment_tag_is_moved() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![Annotated::new(TagEntry(
                Annotated::new("environment".to_owned()),
                Annotated::new("despacito".to_owned()),
            ))]))),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        let event = event.value().unwrap();

        assert_eq!(event.environment.as_str(), Some("despacito"));
        assert_eq!(event.tags.value(), Some(&Tags(vec![].into())));
    }

    #[test]
    fn test_empty_environment_is_removed_and_overwritten_with_tag() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![Annotated::new(TagEntry(
                Annotated::new("environment".to_owned()),
                Annotated::new("despacito".to_owned()),
            ))]))),
            environment: Annotated::new("".to_owned()),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        let event = event.value().unwrap();

        assert_eq!(event.environment.as_str(), Some("despacito"));
        assert_eq!(event.tags.value(), Some(&Tags(vec![].into())));
    }

    #[test]
    fn test_empty_environment_is_removed() {
        let mut event = Annotated::new(Event {
            environment: Annotated::new("".to_owned()),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_eq!(get_value!(event.environment), None);
    }
    #[test]
    fn test_replay_id_added_from_dsc() {
        let replay_id = Uuid::new_v4();
        let mut event = Annotated::new(Event {
            contexts: Annotated::new(Contexts(Object::new())),
            ..Event::default()
        });
        normalize_event(
            &mut event,
            &NormalizationConfig {
                replay_id: Some(replay_id),
                ..Default::default()
            },
        );

        let event = event.value().unwrap();

        assert_eq!(event.contexts, {
            let mut contexts = Contexts::new();
            contexts.add(ReplayContext {
                replay_id: Annotated::new(EventId(replay_id)),
                other: Object::default(),
            });
            Annotated::new(contexts)
        })
    }

    #[test]
    fn test_none_environment_errors() {
        let mut event = Annotated::new(Event {
            environment: Annotated::new("none".to_owned()),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        let environment = get_path!(event.environment!);
        let expected_original = &Value::String("none".to_owned());

        assert_eq!(
            environment.meta().iter_errors().collect::<Vec<&Error>>(),
            vec![&Error::new(ErrorKind::InvalidData)],
        );
        assert_eq!(
            environment.meta().original_value().unwrap(),
            expected_original
        );
        assert_eq!(environment.value(), None);
    }

    #[test]
    fn test_invalid_release_removed() {
        let mut event = Annotated::new(Event {
            release: Annotated::new(LenientString("Latest".to_owned())),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        let release = get_path!(event.release!);
        let expected_original = &Value::String("Latest".to_owned());

        assert_eq!(
            release.meta().iter_errors().collect::<Vec<&Error>>(),
            vec![&Error::new(ErrorKind::InvalidData)],
        );
        assert_eq!(release.meta().original_value().unwrap(), expected_original);
        assert_eq!(release.value(), None);
    }

    #[test]
    fn test_top_level_keys_moved_into_tags() {
        let mut event = Annotated::new(Event {
            server_name: Annotated::new("foo".to_owned()),
            site: Annotated::new("foo".to_owned()),
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("site".to_owned()),
                    Annotated::new("old".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("server_name".to_owned()),
                    Annotated::new("old".to_owned()),
                )),
            ]))),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(get_value!(event.site), None);
        assert_eq!(get_value!(event.server_name), None);

        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("site".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("server_name".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
            ]))
        );
    }

    #[test]
    fn test_internal_tags_removed() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("release".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("dist".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("user".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("filename".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("function".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("something".to_owned()),
                    Annotated::new("else".to_owned()),
                )),
            ]))),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(get_value!(event.tags!).len(), 1);
    }

    #[test]
    fn test_empty_tags_removed() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("".to_owned()),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_owned()),
                    Annotated::new("".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("something".to_owned()),
                    Annotated::new("else".to_owned()),
                )),
            ]))),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::from_error(Error::nonempty(), None),
                    Annotated::new("foo".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_owned()),
                    Annotated::from_error(Error::nonempty(), None),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("something".to_owned()),
                    Annotated::new("else".to_owned()),
                )),
            ]))
        );
    }

    #[test]
    fn test_tags_deduplicated() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_owned()),
                    Annotated::new("1".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("bar".to_owned()),
                    Annotated::new("1".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_owned()),
                    Annotated::new("2".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("bar".to_owned()),
                    Annotated::new("2".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_owned()),
                    Annotated::new("3".to_owned()),
                )),
            ]))),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        // should keep the first occurrence of every tag
        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_owned()),
                    Annotated::new("1".to_owned()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("bar".to_owned()),
                    Annotated::new("1".to_owned()),
                )),
            ]))
        );
    }

    #[test]
    fn test_transaction_status_defaulted_to_unknown() {
        let mut object = Object::new();
        let trace_context = TraceContext {
            // We assume the status to be null.
            status: Annotated::empty(),
            ..TraceContext::default()
        };
        object.insert(
            "trace".to_owned(),
            Annotated::new(ContextInner(Context::Trace(Box::new(trace_context)))),
        );

        let mut event = Annotated::new(Event {
            contexts: Annotated::new(Contexts(object)),
            ..Event::default()
        });
        normalize_event(&mut event, &NormalizationConfig::default());

        let event = event.value().unwrap();

        let event_trace_context = event.context::<TraceContext>().unwrap();
        assert_eq!(
            event_trace_context.status,
            Annotated::new(SpanStatus::Unknown)
        )
    }

    #[test]
    fn test_unknown_debug_image() {
        let mut event = Annotated::new(Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(DebugImage::Other(Object::default()))]),
                ..DebugMeta::default()
            }),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(
            get_path!(event.debug_meta!),
            &Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::from_error(
                    Error::invalid("unsupported debug image type"),
                    Some(Value::Object(Object::default())),
                )]),
                ..DebugMeta::default()
            })
        );
    }

    #[test]
    fn test_context_line_default() {
        let mut frame = Annotated::new(Frame {
            pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_owned())]),
            post_context: Annotated::new(vec![Annotated::new("".to_owned()), Annotated::default()]),
            ..Frame::default()
        });

        normalize_non_raw_frame(&mut frame);

        let frame = frame.value().unwrap();
        assert_eq!(frame.context_line.as_str(), Some(""));
    }

    #[test]
    fn test_context_line_retain() {
        let mut frame = Annotated::new(Frame {
            pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_owned())]),
            post_context: Annotated::new(vec![Annotated::new("".to_owned()), Annotated::default()]),
            context_line: Annotated::new("some line".to_owned()),
            ..Frame::default()
        });

        normalize_non_raw_frame(&mut frame);

        let frame = frame.value().unwrap();
        assert_eq!(frame.context_line.as_str(), Some("some line"));
    }

    #[test]
    fn test_frame_null_context_lines() {
        let mut frame = Annotated::new(Frame {
            pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_owned())]),
            post_context: Annotated::new(vec![Annotated::new("".to_owned()), Annotated::default()]),
            ..Frame::default()
        });

        normalize_non_raw_frame(&mut frame);

        assert_eq!(
            *get_value!(frame.pre_context!),
            vec![Annotated::new("".to_owned()), Annotated::new("".to_owned())],
        );
        assert_eq!(
            *get_value!(frame.post_context!),
            vec![Annotated::new("".to_owned()), Annotated::new("".to_owned())],
        );
    }

    #[test]
    fn test_too_long_distribution() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "fingerprint": [
    "{{ default }}"
  ],
  "platform": "other",
  "dist": "52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059"
}"#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(&mut event, &NormalizationConfig::default());

        let dist = &event.value().unwrap().dist;
        let result = &Annotated::<String>::from_error(
            Error::new(ErrorKind::ValueTooLong),
            Some(Value::String("52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059".to_owned()))
        );
        assert_eq!(dist, result);
    }

    #[test]
    fn test_regression_backfills_abs_path_even_when_moving_stacktrace() {
        let mut event = Annotated::new(Event {
            exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
                ty: Annotated::new("FooDivisionError".to_owned()),
                value: Annotated::new("hi".to_owned().into()),
                ..Exception::default()
            })])),
            stacktrace: Annotated::new(
                RawStacktrace {
                    frames: Annotated::new(vec![Annotated::new(Frame {
                        module: Annotated::new("MyModule".to_owned()),
                        filename: Annotated::new("MyFilename".into()),
                        function: Annotated::new("Void FooBar()".to_owned()),
                        ..Frame::default()
                    })]),
                    ..RawStacktrace::default()
                }
                .into(),
            ),
            ..Event::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(
            get_value!(event.exceptions.values[0].stacktrace!),
            &Stacktrace(RawStacktrace {
                frames: Annotated::new(vec![Annotated::new(Frame {
                    module: Annotated::new("MyModule".to_owned()),
                    filename: Annotated::new("MyFilename".into()),
                    abs_path: Annotated::new("MyFilename".into()),
                    function: Annotated::new("Void FooBar()".to_owned()),
                    ..Frame::default()
                })]),
                ..RawStacktrace::default()
            })
        );
    }

    #[test]
    fn test_parses_sdk_info_from_header() {
        let mut event = Annotated::new(Event::default());

        normalize_event(
            &mut event,
            &NormalizationConfig {
                client: Some("_fooBar/0.0.0".to_owned()),
                ..Default::default()
            },
        );

        assert_eq!(
            get_path!(event.client_sdk!),
            &Annotated::new(ClientSdkInfo {
                name: Annotated::new("_fooBar".to_owned()),
                version: Annotated::new("0.0.0".to_owned()),
                ..ClientSdkInfo::default()
            })
        );
    }

    #[test]
    fn test_discards_received() {
        let mut event = Annotated::new(Event {
            received: FromValue::from_value(Annotated::new(Value::U64(696_969_696_969))),
            ..Default::default()
        });

        validate_event(&mut event, &EventValidationConfig::default()).unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());

        assert_eq!(get_value!(event.received!), get_value!(event.timestamp!));
    }

    #[test]
    fn test_grouping_config() {
        let mut event = Annotated::new(Event {
            logentry: Annotated::from(LogEntry {
                message: Annotated::new("Hello World!".to_owned().into()),
                ..Default::default()
            }),
            ..Default::default()
        });

        validate_event(&mut event, &EventValidationConfig::default()).unwrap();
        normalize_event(
            &mut event,
            &NormalizationConfig {
                grouping_config: Some(json!({
                    "id": "legacy:1234-12-12".to_owned(),
                })),
                ..Default::default()
            },
        );

        insta::assert_ron_snapshot!(SerializableAnnotated(&event), {
            ".event_id" => "[event-id]",
            ".received" => "[received]",
            ".timestamp" => "[timestamp]"
        }, @r###"
        {
          "event_id": "[event-id]",
          "level": "error",
          "type": "default",
          "logentry": {
            "formatted": "Hello World!",
          },
          "logger": "",
          "platform": "other",
          "timestamp": "[timestamp]",
          "received": "[received]",
          "grouping_config": {
            "id": "legacy:1234-12-12",
          },
        }
        "###);
    }

    #[test]
    fn test_logentry_error() {
        let json = r#"
{
    "event_id": "74ad1301f4df489ead37d757295442b1",
    "timestamp": 1668148328.308933,
    "received": 1668148328.308933,
    "level": "error",
    "platform": "python",
    "logentry": {
        "params": [
            "bogus"
        ],
        "formatted": 42
    }
}
"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(&mut event, &NormalizationConfig::default());

        assert_json_snapshot!(SerializableAnnotated(&event), {".received" => "[received]"}, @r###"
        {
          "event_id": "74ad1301f4df489ead37d757295442b1",
          "level": "error",
          "type": "default",
          "logentry": null,
          "logger": "",
          "platform": "python",
          "timestamp": 1668148328.308933,
          "received": "[received]",
          "_meta": {
            "logentry": {
              "": {
                "err": [
                  [
                    "invalid_data",
                    {
                      "reason": "no message present"
                    }
                  ]
                ],
                "val": {
                  "formatted": null,
                  "message": null,
                  "params": [
                    "bogus"
                  ]
                }
              }
            }
          }
        }
        "###)
    }

    #[test]
    fn test_future_timestamp() {
        let mut event = Annotated::new(Event {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 3, 0, 2, 0).unwrap().into()),
            ..Default::default()
        });

        let received_at = Some(Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap());
        let max_secs_in_past = Some(30 * 24 * 3600);
        let max_secs_in_future = Some(60);

        validate_event(
            &mut event,
            &EventValidationConfig {
                received_at,
                max_secs_in_past,
                max_secs_in_future,
                is_validated: false,
                ..Default::default()
            },
        )
        .unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());

        insta::assert_ron_snapshot!(SerializableAnnotated(&event), {
        ".event_id" => "[event-id]",
    }, @r###"
        {
          "event_id": "[event-id]",
          "level": "error",
          "type": "default",
          "logger": "",
          "platform": "other",
          "timestamp": 946857600.0,
          "received": 946857600.0,
          "_meta": {
            "timestamp": {
              "": Meta(Some(MetaInner(
                err: [
                  [
                    "future_timestamp",
                    {
                      "sdk_time": "2000-01-03T00:02:00+00:00",
                      "server_time": "2000-01-03T00:00:00+00:00",
                    },
                  ],
                ],
              ))),
            },
          },
        }
        "###);
    }

    #[test]
    fn test_past_timestamp() {
        let mut event = Annotated::new(Event {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap().into()),
            ..Default::default()
        });

        let received_at = Some(Utc.with_ymd_and_hms(2000, 3, 3, 0, 0, 0).unwrap());
        let max_secs_in_past = Some(30 * 24 * 3600);
        let max_secs_in_future = Some(60);

        validate_event(
            &mut event,
            &EventValidationConfig {
                received_at,
                max_secs_in_past,
                max_secs_in_future,
                is_validated: false,
                ..Default::default()
            },
        )
        .unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());

        insta::assert_ron_snapshot!(SerializableAnnotated(&event), {
        ".event_id" => "[event-id]",
    }, @r###"
        {
          "event_id": "[event-id]",
          "level": "error",
          "type": "default",
          "logger": "",
          "platform": "other",
          "timestamp": 952041600.0,
          "received": 952041600.0,
          "_meta": {
            "timestamp": {
              "": Meta(Some(MetaInner(
                err: [
                  [
                    "past_timestamp",
                    {
                      "sdk_time": "2000-01-03T00:00:00+00:00",
                      "server_time": "2000-03-03T00:00:00+00:00",
                    },
                  ],
                ],
              ))),
            },
          },
        }
        "###);
    }

    #[test]
    fn test_normalize_logger_empty() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_trimmed() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": " \t  \t   ",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_short_no_trimming() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "my.short-logger.isnt_trimmed",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_exact_length() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "this_is-exactly-the_max_len.012345678901234567890123456789012345",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_too_long_single_word() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "this_is-way_too_long-and_we_only_have_one_word-so_we_cant_smart_trim",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_word_trimmed_at_max() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "already_out.out.in.this_part-is-kept.this_right_here-is_an-extremely_long_word",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_word_trimmed_before_max() {
        // This test verifies the "smart" trimming on words -- the logger name
        // should be cut before the max limit, removing entire words.
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "super_out.this_is_already_out_too.this_part-is-kept.this_right_here-is_a-very_long_word",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_word_leading_dots() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "io.this-tests-the-smart-trimming-and-word-removal-around-dot.words",
            })
            .into(),
        );

        normalize_event(&mut event, &NormalizationConfig::default());
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalization_is_idempotent() {
        // get an event, normalize it. the result of that must be the same as normalizing it once more
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),

                ..Default::default()
            })]),
            ..Default::default()
        });

        fn remove_received_from_event(event: &mut Annotated<Event>) -> &mut Annotated<Event> {
            relay_event_schema::processor::apply(event, |e, _m| {
                e.received = Annotated::empty();
                Ok(())
            })
            .unwrap();
            event
        }

        normalize_event(&mut event, &NormalizationConfig::default());
        let first = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        // Expected some fields (such as timestamps) exist after first normalization.

        normalize_event(&mut event, &NormalizationConfig::default());
        let second = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&first, &second, "idempotency check failed");

        normalize_event(&mut event, &NormalizationConfig::default());
        let third = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&second, &third, "idempotency check failed");
    }

    /// Validate full normalization is idempotent.
    ///
    /// Both PoPs and processing relays will temporarily run full normalization
    /// in events, during the rollout of running normalization once in internal
    /// relays.
    // TODO(iker): remove this test after the rollout is done.
    #[test]
    fn test_full_normalization_is_idempotent() {
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),

                ..Default::default()
            })]),
            ..Default::default()
        });

        fn remove_received_from_event(event: &mut Annotated<Event>) -> &mut Annotated<Event> {
            relay_event_schema::processor::apply(event, |e, _m| {
                e.received = Annotated::empty();
                Ok(())
            })
            .unwrap();
            event
        }

        let full_normalization_config = NormalizationConfig {
            is_renormalize: false,
            enable_trimming: true,
            remove_other: true,
            emit_event_errors: true,
            ..Default::default()
        };

        normalize_event(&mut event, &full_normalization_config);
        let first = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        // Expected some fields (such as timestamps) exist after first normalization.

        normalize_event(&mut event, &full_normalization_config);
        let second = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&first, &second, "idempotency check failed");

        normalize_event(&mut event, &full_normalization_config);
        let third = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&second, &third, "idempotency check failed");
    }

    #[test]
    fn test_normalize_validates_spans() {
        let event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "transaction",
                "transaction": "/",
                "timestamp": 946684810.0,
                "start_timestamp": 946684800.0,
                "contexts": {
                    "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "http.server",
                    "type": "trace"
                    }
                },
                "spans": []
            }
            "#,
        )
        .unwrap();

        // All incomplete spans should be caught by normalization:
        for span in [
            r#"null"#,
            r#"{
              "timestamp": 946684810.0,
              "start_timestamp": 946684900.0,
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }"#,
            r#"{
              "timestamp": 946684810.0,
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }"#,
            r#"{
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }"#,
            r#"{
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "span_id": "fa90fdead5f74053"
            }"#,
        ] {
            let mut modified_event = event.clone();
            let event_ref = modified_event.value_mut().as_mut().unwrap();
            event_ref
                .spans
                .set_value(Some(vec![Annotated::<Span>::from_json(span).unwrap()]));

            let res = validate_event(&mut modified_event, &EventValidationConfig::default());

            assert!(res.is_err(), "{span:?}");
        }
    }

    #[test]
    fn test_normalization_respects_is_renormalize() {
        let mut event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "default",
                "tags": [["environment", "some_environment"]]
            }
            "#,
        )
        .unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                is_renormalize: true,
                ..Default::default()
            },
        );

        assert_debug_snapshot!(event.value().unwrap().tags, @r###"
        Tags(
            PairList(
                [
                    TagEntry(
                        "environment",
                        "some_environment",
                    ),
                ],
            ),
        )
        "###);
    }

    #[test]
    fn test_geo_in_normalize() {
        let mut event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "transaction",
                "transaction": "/foo/",
                "timestamp": 946684810.0,
                "start_timestamp": 946684800.0,
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053",
                        "op": "http.server",
                        "type": "trace"
                    }
                },
                "transaction_info": {
                    "source": "url"
                },
                "user": {
                    "ip_address": "2.125.160.216"
                }
            }
            "#,
        )
        .unwrap();

        let lookup = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();

        // Extract user's geo information before normalization.
        let user_geo = event.value().unwrap().user.value().unwrap().geo.value();
        assert!(user_geo.is_none());

        normalize_event(
            &mut event,
            &NormalizationConfig {
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );

        // Extract user's geo information after normalization.
        let user_geo = event
            .value()
            .unwrap()
            .user
            .value()
            .unwrap()
            .geo
            .value()
            .unwrap();

        assert_eq!(user_geo.country_code.value().unwrap(), "GB");
        assert_eq!(user_geo.city.value().unwrap(), "Boxford");
    }

    #[test]
    fn test_normalize_app_start_spans_only_for_react_native_3_to_4_4() {
        let mut event = Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("app_start_cold".to_owned()),
                ..Default::default()
            })]),
            client_sdk: Annotated::new(ClientSdkInfo {
                name: Annotated::new("sentry.javascript.react-native".to_owned()),
                version: Annotated::new("4.5.0".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_app_start_spans(&mut event);
        assert_debug_snapshot!(event.spans, @r###"
        [
            Span {
                timestamp: ~,
                start_timestamp: ~,
                exclusive_time: ~,
                op: "app_start_cold",
                span_id: ~,
                parent_span_id: ~,
                trace_id: ~,
                segment_id: ~,
                is_segment: ~,
                is_remote: ~,
                status: ~,
                description: ~,
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                links: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                platform: ~,
                was_transaction: ~,
                kind: ~,
                performance_issues_spans: ~,
                other: {},
            },
        ]
        "###);
    }

    #[test]
    fn test_normalize_app_start_cold_spans_for_react_native() {
        let mut event = Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("app_start_cold".to_owned()),
                ..Default::default()
            })]),
            client_sdk: Annotated::new(ClientSdkInfo {
                name: Annotated::new("sentry.javascript.react-native".to_owned()),
                version: Annotated::new("4.4.0".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_app_start_spans(&mut event);
        assert_debug_snapshot!(event.spans, @r###"
        [
            Span {
                timestamp: ~,
                start_timestamp: ~,
                exclusive_time: ~,
                op: "app.start.cold",
                span_id: ~,
                parent_span_id: ~,
                trace_id: ~,
                segment_id: ~,
                is_segment: ~,
                is_remote: ~,
                status: ~,
                description: ~,
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                links: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                platform: ~,
                was_transaction: ~,
                kind: ~,
                performance_issues_spans: ~,
                other: {},
            },
        ]
        "###);
    }

    #[test]
    fn test_normalize_app_start_warm_spans_for_react_native() {
        let mut event = Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("app_start_warm".to_owned()),
                ..Default::default()
            })]),
            client_sdk: Annotated::new(ClientSdkInfo {
                name: Annotated::new("sentry.javascript.react-native".to_owned()),
                version: Annotated::new("4.4.0".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_app_start_spans(&mut event);
        assert_debug_snapshot!(event.spans, @r###"
        [
            Span {
                timestamp: ~,
                start_timestamp: ~,
                exclusive_time: ~,
                op: "app.start.warm",
                span_id: ~,
                parent_span_id: ~,
                trace_id: ~,
                segment_id: ~,
                is_segment: ~,
                is_remote: ~,
                status: ~,
                description: ~,
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                links: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                platform: ~,
                was_transaction: ~,
                kind: ~,
                performance_issues_spans: ~,
                other: {},
            },
        ]
        "###);
    }
}
