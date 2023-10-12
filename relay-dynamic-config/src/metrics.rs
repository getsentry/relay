//! Dynamic configuration for metrics extraction from sessions and transactions.

use std::collections::BTreeSet;

use relay_base_schema::data_category::DataCategory;
use relay_common::glob2::LazyGlob;
use relay_sampling::condition::RuleCondition;
use serde::{Deserialize, Serialize};

use crate::project::ProjectConfig;

/// Rule defining when a target tag should be set on a metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaggingRule {
    // note: could add relay_sampling::RuleType here, but right now we only support transaction
    // events
    /// Condition that defines when to set the tag.
    pub condition: RuleCondition,
    /// Metrics on which the tag is set.
    pub target_metrics: BTreeSet<String>,
    /// Name of the tag that is set.
    pub target_tag: String,
    /// Value of the tag that is set.
    pub tag_value: String,
}

/// Current version of metrics extraction.
const SESSION_EXTRACT_VERSION: u16 = 3;
const EXTRACT_ABNORMAL_MECHANISM_VERSION: u16 = 2;

/// Configuration for metric extraction from sessions.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionMetricsConfig {
    /// The revision of the extraction algorithm.
    ///
    /// Provided the revision is lower than or equal to the revision supported by this Relay,
    /// metrics are extracted. If the revision is higher than what this Relay supports, it does not
    /// extract metrics from sessions, and instead forwards them to the upstream.
    ///
    /// Version `0` (default) disables extraction.
    version: u16,

    /// Drop sessions after successfully extracting metrics.
    drop: bool,
}

impl SessionMetricsConfig {
    /// Returns `true` if session metrics is enabled and compatible.
    pub fn is_enabled(&self) -> bool {
        self.version > 0 && self.version <= SESSION_EXTRACT_VERSION
    }

    /// Returns `true` if Relay should not extract metrics from sessions.
    pub fn is_disabled(&self) -> bool {
        !self.is_enabled()
    }

    /// Whether or not the abnormal mechanism should be extracted as a tag.
    pub fn should_extract_abnormal_mechanism(&self) -> bool {
        self.version >= EXTRACT_ABNORMAL_MECHANISM_VERSION
    }

    /// Returns `true` if the session should be dropped after extracting metrics.
    pub fn should_drop(&self) -> bool {
        self.drop
    }
}

/// Configuration for extracting custom measurements from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CustomMeasurementConfig {
    /// The maximum number of custom measurements to extract. Defaults to zero.
    limit: usize,
}

/// Maximum supported version of metrics extraction from transactions.
///
/// The version is an integer scalar, incremented by one on each new version:
///  - 1: Initial version.
///  - 2: Moves `acceptTransactionNames` to global config.
///  - 3:
///      - Emit a `usage` metric and use it for rate limiting.
///      - Delay metrics extraction for indexed transactions.
const TRANSACTION_EXTRACT_VERSION: u16 = 3;

/// Deprecated. Defines whether URL transactions should be considered low cardinality.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AcceptTransactionNames {
    /// Only accept transaction names with a low-cardinality source.
    Strict,

    /// For some SDKs, accept all transaction names, while for others, apply strict rules.
    #[serde(other)]
    ClientBased,
}

impl Default for AcceptTransactionNames {
    fn default() -> Self {
        Self::ClientBased
    }
}

/// Configuration for extracting metrics from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TransactionMetricsConfig {
    /// The required version to extract transaction metrics.
    pub version: u16,
    /// Custom event tags that are transferred from the transaction to metrics.
    pub extract_custom_tags: BTreeSet<String>,
    /// Deprecated in favor of top-level config field. Still here to be forwarded to external relays.
    pub custom_measurements: CustomMeasurementConfig,
    /// Deprecated. Defines whether URL transactions should be considered low cardinality.
    /// Keep this around for external Relays.
    #[serde(rename = "acceptTransactionNames")]
    pub deprecated1: AcceptTransactionNames,
}

impl TransactionMetricsConfig {
    /// Creates an enabled configuration with empty defaults.
    pub fn new() -> Self {
        Self {
            version: 1,
            ..Self::default()
        }
    }

    /// Returns `true` if metrics extraction is enabled and compatible with this Relay.
    pub fn is_enabled(&self) -> bool {
        self.version > 0 && self.version <= TRANSACTION_EXTRACT_VERSION
    }

    /// Returns `true` if usage should be tracked through a dedicated metric.
    pub fn usage_metric(&self) -> bool {
        self.version >= 3
    }
}

/// Configuration for generic extraction of metrics from all data categories.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricExtractionConfig {
    /// Versioning of metrics extraction. Relay skips extraction if the version is not supported.
    pub version: u16,

    /// A list of metric specifications to extract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<MetricSpec>,

    /// A list of tags to add to previously extracted metrics.
    ///
    /// These tags add further tags to a range of metrics. If some metrics already have a matching
    /// tag extracted, the existing tag is left unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<TagMapping>,

    /// This config has been extended with fields from `conditional_tagging`.
    ///
    /// At the moment, Relay will parse `conditional_tagging` rules and insert them into the `tags`
    /// mapping in this struct. If the flag is `true`, this has already happened and should not be
    /// repeated.
    ///
    /// This is a temporary flag that will be removed once the transaction metric extraction version
    /// is bumped to `2`.
    #[serde(default)]
    pub _conditional_tags_extended: bool,

    /// This config has been extended with default span metrics.
    ///
    /// Relay checks for the span extraction flag and adds built-in metrics and tags to this struct.
    /// If the flag is `true`, this has already happened and should not be repeated.
    ///
    /// This is a temporary flag that will be removed once the transaction metric extraction version
    /// is bumped to `2`.
    #[serde(default)]
    pub _span_metrics_extended: bool,
}

impl MetricExtractionConfig {
    /// The latest version for this config struct.
    pub const VERSION: u16 = 1;

    /// Returns an empty `MetricExtractionConfig` with the latest version.
    ///
    /// As opposed to `default()`, this will be enabled once populated with specs.
    pub fn empty() -> Self {
        Self {
            version: Self::VERSION,
            metrics: Vec::new(),
            tags: Vec::new(),
            _conditional_tags_extended: false,
            _span_metrics_extended: false,
        }
    }

    /// Returns `true` if the version of this metric extraction config is supported.
    pub fn is_supported(&self) -> bool {
        self.version <= Self::VERSION
    }

    /// Returns `true` if metric extraction is configured and compatible with this Relay.
    pub fn is_enabled(&self) -> bool {
        self.version > 0
            && self.is_supported()
            && !(self.metrics.is_empty() && self.tags.is_empty())
    }
}

/// Specification for a metric to extract from some data.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSpec {
    /// Category of data to extract this metric for.
    pub category: DataCategory,

    /// The Metric Resource Identifier (MRI) of the metric to extract.
    pub mri: String,

    /// A path to the field to extract the metric from.
    ///
    /// This value contains a fully qualified expression pointing at the data field in the payload
    /// to extract the metric from. It follows the `Getter` syntax that is also used for dynamic
    /// sampling.
    ///
    /// How the value is treated depends on the metric type:
    ///
    /// - **Counter** metrics are a special case, since the default product counters do not count
    ///   any specific field but rather the occurrence of the event. As such, there is no value
    ///   expression, and the field is set to `None`. Semantics of specifying remain undefined at
    ///   this point.
    /// - **Distribution** metrics require a numeric value. If the value at the specified path is
    ///   not numeric, metric extraction will be skipped.
    /// - **Set** metrics require a string value, which is then emitted into the set as unique
    ///   value. Insertion of numbers and other types is undefined.
    ///
    /// If the field does not exist, extraction is skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    /// An optional condition to meet before extraction.
    ///
    /// See [`RuleCondition`] for all available options to specify and combine conditions. If no
    /// condition is specified, the metric is extracted unconditionally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<RuleCondition>,

    /// A list of tags to add to the metric.
    ///
    /// Tags can be conditional, see [`TagSpec`] for configuration options. For this reason, it is
    /// possible to list tag keys multiple times, each with different conditions. The first matching
    /// condition will be applied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<TagSpec>,
}

/// Mapping between extracted metrics and additional tags to extract.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagMapping {
    /// A list of Metric Resource Identifiers (MRI) to apply tags to.
    ///
    /// Entries in this list can contain wildcards to match metrics with dynamic MRIs.
    #[serde(default)]
    pub metrics: Vec<LazyGlob>,

    /// A list of tags to add to the metric.
    ///
    /// Tags can be conditional, see [`TagSpec`] for configuration options. For this reason, it is
    /// possible to list tag keys multiple times, each with different conditions. The first matching
    /// condition will be applied.
    #[serde(default)]
    pub tags: Vec<TagSpec>,
}

impl TagMapping {
    /// Returns `true` if this mapping matches the provided MRI.
    pub fn matches(&self, mri: &str) -> bool {
        // TODO: Use a globset, instead.
        self.metrics
            .iter()
            .any(|glob| glob.compiled().is_match(mri))
    }
}

/// Configuration for a tag to add to a metric.
///
/// Tags values can be static if defined through `value` or dynamically queried from the payload if
/// defined through `field`. These two options are mutually exclusive, behavior is undefined if both
/// are specified.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagSpec {
    /// The key of the tag to extract.
    pub key: String,

    /// Path to a field containing the tag's value.
    ///
    /// It follows the `Getter` syntax to read data from the payload.
    ///
    /// Mutually exclusive with `value`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    /// Literal value of the tag.
    ///
    /// Mutually exclusive with `field`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// An optional condition to meet before extraction.
    ///
    /// See [`RuleCondition`] for all available options to specify and combine conditions. If no
    /// condition is specified, the tag is added unconditionally, provided it is not already there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<RuleCondition>,
}

impl TagSpec {
    /// Returns the source of tag values, either literal or a field.
    pub fn source(&self) -> TagSource<'_> {
        if let Some(ref field) = self.field {
            TagSource::Field(field)
        } else if let Some(ref value) = self.value {
            TagSource::Literal(value)
        } else {
            TagSource::Unknown
        }
    }
}

/// Specifies how to obtain the value of a tag in [`TagSpec`].
#[derive(Clone, Debug, PartialEq)]
pub enum TagSource<'a> {
    /// A literal value.
    Literal(&'a str),
    /// Path to a field to evaluate.
    Field(&'a str),
    /// An unsupported or unknown source.
    Unknown,
}

/// Converts the given tagging rules from `conditional_tagging` to the newer metric extraction
/// config.
pub fn convert_conditional_tagging(project_config: &mut ProjectConfig) {
    // NOTE: This clones the rules so that they remain in the project state for old Relays that
    // do not support generic metrics extraction. Once the migration is complete, this can be
    // removed with a version bump of the transaction metrics config.
    let rules = &project_config.metric_conditional_tagging;
    if rules.is_empty() {
        return;
    }

    let config = project_config
        .metric_extraction
        .get_or_insert_with(MetricExtractionConfig::empty);

    if !config.is_supported() || config._conditional_tags_extended {
        return;
    }

    config.tags.extend(TaggingRuleConverter {
        rules: rules.iter().cloned().peekable(),
        tags: Vec::new(),
    });

    config._conditional_tags_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}

struct TaggingRuleConverter<I: Iterator<Item = TaggingRule>> {
    rules: std::iter::Peekable<I>,
    tags: Vec<TagSpec>,
}

impl<I> Iterator for TaggingRuleConverter<I>
where
    I: Iterator<Item = TaggingRule>,
{
    type Item = TagMapping;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let old = self.rules.next()?;

            self.tags.push(TagSpec {
                key: old.target_tag,
                field: None,
                value: Some(old.tag_value),
                condition: Some(old.condition),
            });

            // Optimization: Collect tags for consecutive tagging rules for the same set of metrics.
            // Then, emit a single entry with all tag specs at once.
            if self.rules.peek().map(|r| &r.target_metrics) == Some(&old.target_metrics) {
                continue;
            }

            return Some(TagMapping {
                metrics: old.target_metrics.into_iter().map(LazyGlob::new).collect(),
                tags: std::mem::take(&mut self.tags),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use similar_asserts::assert_eq;

    #[test]
    fn parse_tag_spec_value() {
        let json = r#"{"key":"foo","value":"bar"}"#;
        let spec: TagSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.source(), TagSource::Literal("bar"));
    }

    #[test]
    fn parse_tag_spec_field() {
        let json = r#"{"key":"foo","field":"bar"}"#;
        let spec: TagSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.source(), TagSource::Field("bar"));
    }

    #[test]
    fn parse_tag_spec_unsupported() {
        let json = r#"{"key":"foo","somethingNew":"bar"}"#;
        let spec: TagSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.source(), TagSource::Unknown);
    }

    #[test]
    fn parse_tag_mapping() {
        let json = r#"{"metrics": ["d:spans/*"], "tags": [{"key":"foo","field":"bar"}]}"#;
        let mapping: TagMapping = serde_json::from_str(json).unwrap();
        assert!(mapping.metrics[0].compiled().is_match("d:spans/foo"));
    }
}
