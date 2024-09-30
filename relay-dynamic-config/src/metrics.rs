//! Dynamic configuration for metrics extraction from sessions and transactions.

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::str::FromStr;

use relay_base_schema::data_category::DataCategory;
use relay_cardinality::CardinalityLimit;
use relay_common::glob2::LazyGlob;
use relay_common::glob3::GlobPatterns;
use relay_common::impl_str_serde;
use relay_protocol::RuleCondition;
use serde::{Deserialize, Serialize};

use crate::project::ProjectConfig;

/// Configuration for metrics filtering.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
pub struct Metrics {
    /// List of cardinality limits to enforce for this project.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cardinality_limits: Vec<CardinalityLimit>,
    /// List of patterns for blocking metrics based on their name.
    #[serde(skip_serializing_if = "GlobPatterns::is_empty")]
    pub denied_names: GlobPatterns,
    /// Configuration for removing tags from a bucket.
    ///
    /// Note that removing tags does not drop the overall metric bucket.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub denied_tags: Vec<TagBlock>,
}

impl Metrics {
    /// Returns `true` if there are no changes to the metrics config.
    pub fn is_empty(&self) -> bool {
        self.cardinality_limits.is_empty()
            && self.denied_names.is_empty()
            && self.denied_tags.is_empty()
    }
}

/// Configuration for removing tags matching the `tag` pattern on metrics whose name matches the `name` pattern.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct TagBlock {
    /// Name of metric of which we want to remove certain tags.
    pub name: GlobPatterns,
    /// Pattern to match keys of tags that we want to remove.
    pub tags: GlobPatterns,
}

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
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
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
///  - 4: Adds support for `RuleConfigs` with string comparisons.
///  - 5: No change, bumped together with [`MetricExtractionConfig::MAX_SUPPORTED_VERSION`].
///  - 6: Bugfix to make transaction metrics extraction apply globally defined tag mappings.
const TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION: u16 = 6;

/// Minimum supported version of metrics extraction from transaction.
const TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION: u16 = 3;

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
            version: TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION,
            ..Self::default()
        }
    }

    /// Returns `true` if metrics extraction is enabled and compatible with this Relay.
    pub fn is_enabled(&self) -> bool {
        self.version >= TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
            && self.version <= TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION
    }
}

/// Combined view of global and project-specific metrics extraction configs.
#[derive(Debug, Clone, Copy)]
pub struct CombinedMetricExtractionConfig<'a> {
    global: &'a MetricExtractionGroups,
    project: &'a MetricExtractionConfig,
}

impl<'a> CombinedMetricExtractionConfig<'a> {
    /// Creates a new combined view from two references.
    pub fn new(global: &'a MetricExtractionGroups, project: &'a MetricExtractionConfig) -> Self {
        for key in project.global_groups.keys() {
            if !global.groups.contains_key(key) {
                relay_log::error!(
                    "Metrics group configured for project missing in global config: {key:?}"
                )
            }
        }

        Self { global, project }
    }

    /// Returns an iterator of metric specs.
    pub fn metrics(&self) -> impl Iterator<Item = &MetricSpec> {
        let project = self.project.metrics.iter();
        let enabled_global = self
            .enabled_groups()
            .flat_map(|template| template.metrics.iter());

        project.chain(enabled_global)
    }

    /// Returns an iterator of tag mappings.
    pub fn tags(&self) -> impl Iterator<Item = &TagMapping> {
        let project = self.project.tags.iter();
        let enabled_global = self
            .enabled_groups()
            .flat_map(|template| template.tags.iter());

        project.chain(enabled_global)
    }

    fn enabled_groups(&self) -> impl Iterator<Item = &MetricExtractionGroup> {
        self.global.groups.iter().filter_map(|(key, template)| {
            let is_enabled_by_override = self.project.global_groups.get(key).map(|c| c.is_enabled);
            let is_enabled = is_enabled_by_override.unwrap_or(template.is_enabled);

            is_enabled.then_some(template)
        })
    }
}

impl<'a> From<&'a MetricExtractionConfig> for CombinedMetricExtractionConfig<'a> {
    /// Creates a combined config with an empty global component. Used in tests.
    fn from(value: &'a MetricExtractionConfig) -> Self {
        Self::new(MetricExtractionGroups::EMPTY, value)
    }
}

/// Global groups for metric extraction.
///
/// Templates can be enabled or disabled by project configs.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricExtractionGroups {
    /// Mapping from group name to metrics specs & tags.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub groups: BTreeMap<GroupKey, MetricExtractionGroup>,
}

impl MetricExtractionGroups {
    /// Empty config, used in tests and as a fallback.
    pub const EMPTY: &'static Self = &Self {
        groups: BTreeMap::new(),
    };

    /// Returns `true` if the contained groups are empty.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

/// Group of metrics & tags that can be enabled or disabled as a group.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricExtractionGroup {
    /// Whether the set is enabled by default.
    ///
    /// Project configs can overwrite this flag to opt-in or out of a set.
    pub is_enabled: bool,

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

/// Configuration for generic extraction of metrics from all data categories.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricExtractionConfig {
    /// Versioning of metrics extraction. Relay skips extraction if the version is not supported.
    pub version: u16,

    /// Configuration of global metric groups.
    ///
    /// The groups themselves are configured in [`crate::GlobalConfig`],
    /// but can be enabled or disabled here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub global_groups: BTreeMap<GroupKey, MetricExtractionGroupOverride>,

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
    ///
    /// This is the maximum version supported by this Relay instance.
    pub const MAX_SUPPORTED_VERSION: u16 = 4;

    /// Returns an empty `MetricExtractionConfig` with the latest version.
    ///
    /// As opposed to `default()`, this will be enabled once populated with specs.
    pub fn empty() -> Self {
        Self {
            version: Self::MAX_SUPPORTED_VERSION,
            global_groups: BTreeMap::new(),
            metrics: Default::default(),
            tags: Default::default(),
            _conditional_tags_extended: false,
            _span_metrics_extended: false,
        }
    }

    /// Returns `true` if the version of this metric extraction config is supported.
    pub fn is_supported(&self) -> bool {
        self.version <= Self::MAX_SUPPORTED_VERSION
    }

    /// Returns `true` if metric extraction is configured and compatible with this Relay.
    pub fn is_enabled(&self) -> bool {
        self.version > 0
            && self.is_supported()
            && !(self.metrics.is_empty() && self.tags.is_empty() && self.global_groups.is_empty())
    }
}

/// Configures global metrics extraction groups.
///
/// Project configs can enable or disable globally defined groups.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricExtractionGroupOverride {
    /// `true` if a template should be enabled.
    pub is_enabled: bool,
}

/// Enumeration of keys in [`MetricExtractionGroups`]. In JSON, this is simply a string.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GroupKey {
    /// Metric extracted for all plans.
    SpanMetricsCommon,
    /// "addon" metrics.
    SpanMetricsAddons,
    /// Metrics extracted from spans in the transaction namespace.
    SpanMetricsTx,
    /// Any other group defined by the upstream.
    Other(String),
}

impl fmt::Display for GroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                GroupKey::SpanMetricsCommon => "span_metrics_common",
                GroupKey::SpanMetricsAddons => "span_metrics_addons",
                GroupKey::SpanMetricsTx => "span_metrics_tx",
                GroupKey::Other(s) => &s,
            }
        )
    }
}

impl FromStr for GroupKey {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "span_metrics_common" => GroupKey::SpanMetricsCommon,
            "span_metrics_addons" => GroupKey::SpanMetricsAddons,
            "span_metrics_tx" => GroupKey::SpanMetricsTx,
            s => GroupKey::Other(s.to_owned()),
        })
    }
}

impl_str_serde!(GroupKey, "a metrics extraction group key");

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

/// Builder for [`TagSpec`].
pub struct Tag {
    key: String,
}

impl Tag {
    /// Prepares a tag with a given tag name.
    pub fn with_key(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Defines the field from which the tag value gets its data.
    pub fn from_field(self, field_name: impl Into<String>) -> TagWithSource {
        let Self { key } = self;
        TagWithSource {
            key,
            field: Some(field_name.into()),
            value: None,
        }
    }

    /// Defines what value to set for a tag.
    pub fn with_value(self, value: impl Into<String>) -> TagWithSource {
        let Self { key } = self;
        TagWithSource {
            key,
            field: None,
            value: Some(value.into()),
        }
    }
}

/// Intermediate result of the tag spec builder.
///
/// Can be transformed into [`TagSpec`].
pub struct TagWithSource {
    key: String,
    field: Option<String>,
    value: Option<String>,
}

impl TagWithSource {
    /// Defines a tag that is extracted unconditionally.
    pub fn always(self) -> TagSpec {
        let Self { key, field, value } = self;
        TagSpec {
            key,
            field,
            value,
            condition: None,
        }
    }

    /// Defines a tag that is extracted under the given condition.
    pub fn when(self, condition: RuleCondition) -> TagSpec {
        let Self { key, field, value } = self;
        TagSpec {
            key,
            field,
            value,
            condition: Some(condition),
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
        config.version = MetricExtractionConfig::MAX_SUPPORTED_VERSION;
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
    fn test_empty_metrics_deserialize() {
        let m: Metrics = serde_json::from_str("{}").unwrap();
        assert!(m.is_empty());
        assert_eq!(m, Metrics::default());
    }

    #[test]
    fn test_serialize_metrics_config_denied_names() {
        let input_str = r#"{"deniedNames":["foo","bar"]}"#;
        let deny_list: Metrics = serde_json::from_str(input_str).unwrap();
        let back_to_str = serde_json::to_string(&deny_list).unwrap();
        assert_eq!(input_str, back_to_str);
    }

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

    fn groups() -> MetricExtractionGroups {
        serde_json::from_value::<MetricExtractionGroups>(serde_json::json!({
            "groups": {
                "group1": {
                    "isEnabled": false,
                    "metrics": [{
                        "category": "transaction",
                        "mri": "c:metric1/counter@none",
                    }],
                    "tags": [
                        {
                            "metrics": ["c:metric1/counter@none"],
                            "tags": [{
                                "key": "tag1",
                                "value": "value1"
                            }]
                        }
                    ]
                },
                "group2": {
                    "isEnabled": true,
                    "metrics": [{
                        "category": "transaction",
                        "mri": "c:metric2/counter@none",
                    }],
                    "tags": [
                        {
                            "metrics": ["c:metric2/counter@none"],
                            "tags": [{
                                "key": "tag2",
                                "value": "value2"
                            }]
                        }
                    ]
                }
            }
        }))
        .unwrap()
    }

    #[test]
    fn metric_extraction_global_defaults() {
        let global = groups();
        let project: MetricExtractionConfig = serde_json::from_value(serde_json::json!({
            "version": 1,
            "global_templates": {}
        }))
        .unwrap();
        let combined = CombinedMetricExtractionConfig::new(&global, &project);

        assert_eq!(
            combined
                .metrics()
                .map(|m| m.mri.as_str())
                .collect::<Vec<_>>(),
            vec!["c:metric2/counter@none"]
        );
        assert_eq!(
            combined
                .tags()
                .map(|t| t.tags[0].key.as_str())
                .collect::<Vec<_>>(),
            vec!["tag2"]
        );
    }

    #[test]
    fn metric_extraction_override() {
        let global = groups();
        let project: MetricExtractionConfig = serde_json::from_value(serde_json::json!({
            "version": 1,
            "globalGroups": {
                "group1": {"isEnabled": true},
                "group2": {"isEnabled": false}
            }
        }))
        .unwrap();
        let combined = CombinedMetricExtractionConfig::new(&global, &project);

        assert_eq!(
            combined
                .metrics()
                .map(|m| m.mri.as_str())
                .collect::<Vec<_>>(),
            vec!["c:metric1/counter@none"]
        );
        assert_eq!(
            combined
                .tags()
                .map(|t| t.tags[0].key.as_str())
                .collect::<Vec<_>>(),
            vec!["tag1"]
        );
    }
}
