//! Dynamic configuration for metrics extraction from sessions and transactions.

use std::collections::BTreeSet;

use relay_common::DataCategory;
use relay_general::store::LazyGlob;
use relay_sampling::RuleCondition;
use serde::{Deserialize, Serialize};

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
/// The version is an integer scalar, incremented by one on each new version.
const TRANSACTION_EXTRACT_VERSION: u16 = 1;

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
}

impl MetricExtractionConfig {
    /// The latest version for this config struct.
    pub const VERSION: u16 = 1;

    /// Returns `true` if metric extraction is configured.
    pub fn is_enabled(&self) -> bool {
        self.version > 0
            && self.version <= Self::VERSION
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
    /// to extract the metric from. It follows the
    /// [`FieldValueProvider`](relay_sampling::FieldValueProvider) syntax that is also used for
    /// dynamic sampling.
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
    /// It follows the [`FieldValueProvider`](relay_sampling::FieldValueProvider) syntax to read
    /// data from the payload.
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
