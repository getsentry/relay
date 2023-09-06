use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::hash::Hasher as _;

use hash32::{FnvHasher, Hasher as _};
use serde::{Deserialize, Serialize};

#[doc(inline)]
pub use relay_base_schema::metrics::{
    CustomUnit, DurationUnit, FractionUnit, InformationUnit, MetricUnit, ParseMetricUnitError,
};
#[doc(inline)]
pub use relay_common::time::UnixTimestamp;

/// Type used for Counter metric
pub type CounterType = f64;

/// Type of distribution entries
pub type DistributionType = f64;

/// Type used for set elements in Set metric
pub type SetType = u32;

/// Type used for Gauge entries
pub type GaugeType = f64;

/// The [typed value](Metric::value) of a metric.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", content = "value")]
pub enum MetricValue {
    /// Counts instances of an event. See [`MetricType::Counter`].
    #[serde(rename = "c")]
    Counter(CounterType),
    /// Builds a statistical distribution over values reported. See [`MetricType::Distribution`].
    #[serde(rename = "d")]
    Distribution(DistributionType),
    /// Counts the number of unique reported values. See [`MetricType::Set`].
    ///
    /// Set values can be specified as strings in the submission protocol. They are always hashed
    /// into a 32-bit value and the original value is dropped. If the submission protocol contains a
    /// 32-bit integer, it will be used directly, instead.
    #[serde(rename = "s")]
    Set(SetType),
    /// Stores absolute snapshots of values. See [`MetricType::Gauge`].
    #[serde(rename = "g")]
    Gauge(GaugeType),
}

impl MetricValue {
    /// Creates a [`MetricValue::Set`] from the given string.
    pub fn set_from_str(string: &str) -> Self {
        Self::Set(hash_set_value(string))
    }

    /// Creates a [`MetricValue::Set`] from any type that implements `Display`.
    pub fn set_from_display(display: impl fmt::Display) -> Self {
        Self::set_from_str(&display.to_string())
    }

    /// Returns the type of this value.
    pub fn ty(&self) -> MetricType {
        match self {
            Self::Counter(_) => MetricType::Counter,
            Self::Distribution(_) => MetricType::Distribution,
            Self::Set(_) => MetricType::Set,
            Self::Gauge(_) => MetricType::Gauge,
        }
    }
}

impl fmt::Display for MetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricValue::Counter(value) => value.fmt(f),
            MetricValue::Distribution(value) => value.fmt(f),
            MetricValue::Set(value) => value.fmt(f),
            MetricValue::Gauge(value) => value.fmt(f),
        }
    }
}

/// The type of a [`MetricValue`], determining its aggregation and evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum MetricType {
    /// Counts instances of an event.
    ///
    /// Counters can be incremented and decremented. The default operation is to increment a counter
    /// by `1`, although increments by larger values are equally possible.
    Counter,
    /// Builds a statistical distribution over values reported.
    ///
    /// Based on individual reported values, distributions allow to query the maximum, minimum, or
    /// average of the reported values, as well as statistical quantiles. With an increasing number
    /// of values in the distribution, its accuracy becomes approximate.
    Distribution,
    /// Counts the number of unique reported values.
    ///
    /// Sets allow sending arbitrary discrete values, including strings, and store the deduplicated
    /// count. With an increasing number of unique values in the set, its accuracy becomes
    /// approximate. It is not possible to query individual values from a set.
    Set,
    /// Stores absolute snapshots of values.
    ///
    /// In addition to plain [counters](Self::Counter), gauges store a snapshot of the maximum,
    /// minimum and sum of all values, as well as the last reported value.
    Gauge,
}

impl MetricType {
    /// Return the shortcode for this metric type.
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Counter => "c",
            MetricType::Distribution => "d",
            MetricType::Set => "s",
            MetricType::Gauge => "g",
        }
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MetricType {
    type Err = ParseMetricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "c" | "m" => Self::Counter,
            "h" | "d" | "ms" => Self::Distribution,
            "s" => Self::Set,
            "g" => Self::Gauge,
            _ => return Err(ParseMetricError(())),
        })
    }
}

relay_common::impl_str_serde!(MetricType, "a metric type string");

/// An error returned when metrics or MRIs cannot be parsed.
#[derive(Clone, Copy, Debug)]
pub struct ParseMetricError(pub(crate) ());

impl fmt::Display for ParseMetricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse metric")
    }
}

impl Error for ParseMetricError {}

/// The namespace of a metric.
///
/// Namespaces allow to identify the product entity that the metric got extracted from, and/or
/// identify the use case that the metric belongs to. These namespaces cannot be defined freely,
/// instead they are defined by Sentry. Over time, there will be more namespaces as we introduce
/// new metrics-based products.
///
/// Right now this successfully deserializes any kind of string, but in reality only `"sessions"`
/// (for release health) and `"transactions"` (for metrics-enhanced performance) is supported.
/// Everything else is dropped both in the metrics aggregator and in the store service.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetricNamespace {
    /// Metrics extracted from sessions.
    Sessions,
    /// Metrics extracted from transaction events.
    Transactions,
    /// Metrics extracted from spans.
    Spans,
    /// User-defined metrics directly sent by SDKs and applications.
    Custom,
    /// An unknown and unsupported metric.
    ///
    /// Metrics that Relay either doesn't know or recognize the namespace of will be dropped before
    /// aggregating. For instance, an MRI of `c:something_new/foo@none` has the namespace
    /// `something_new`, but as Relay doesn't support that namespace, it gets deserialized into
    /// this variant.
    ///
    /// Relay currently drops all metrics whose namespace ends up being deserialized as
    /// `unsupported`. We may revise that in the future.
    Unsupported,
}

impl std::str::FromStr for MetricNamespace {
    type Err = ParseMetricError;

    fn from_str(ns: &str) -> Result<Self, Self::Err> {
        match ns {
            "sessions" => Ok(MetricNamespace::Sessions),
            "transactions" => Ok(MetricNamespace::Transactions),
            "spans" => Ok(MetricNamespace::Spans),
            "custom" => Ok(MetricNamespace::Custom),
            _ => Ok(MetricNamespace::Unsupported),
        }
    }
}

relay_common::impl_str_serde!(MetricNamespace, "a valid metric namespace");

impl fmt::Display for MetricNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricNamespace::Sessions => write!(f, "sessions"),
            MetricNamespace::Transactions => write!(f, "transactions"),
            MetricNamespace::Spans => write!(f, "spans"),
            MetricNamespace::Custom => write!(f, "custom"),
            MetricNamespace::Unsupported => write!(f, "unsupported"),
        }
    }
}

/// A metric name parsed as MRI, a naming scheme which includes most of the metric's bucket key
/// (excl. timestamp and tags).
///
/// For more information see [`Metric::name`].
#[derive(Debug)]
pub struct MetricResourceIdentifier<'a> {
    /// The metric type.
    pub ty: MetricType,
    /// The namespace/usecase for this metric. For example `sessions` or `transactions`. In the
    /// case of the statsd protocol, a missing namespace is converted into the valueconverted into
    /// the value `"custom"`.
    pub namespace: MetricNamespace,
    /// The actual name, such as `duration` as part of `d:transactions/duration@ms`
    pub name: &'a str,
    /// The metric unit.
    pub unit: MetricUnit,
}

impl<'a> MetricResourceIdentifier<'a> {
    /// Parses and validates an MRI of the form `<ty>:<ns>/<name>@<unit>`
    pub fn parse(name: &'a str) -> Result<Self, ParseMetricError> {
        // Note that this is NOT `VALUE_SEPARATOR`:
        let (raw_ty, rest) = name.split_once(':').ok_or(ParseMetricError(()))?;
        let ty = raw_ty.parse()?;

        let (raw_namespace, rest) = rest.split_once('/').ok_or(ParseMetricError(()))?;
        let (name, unit) = parse_name_unit(rest).ok_or(ParseMetricError(()))?;

        Ok(Self {
            ty,
            namespace: raw_namespace.parse()?,
            name,
            unit,
        })
    }
}

impl<'a> fmt::Display for MetricResourceIdentifier<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `<ty>:<ns>/<name>@<unit>`
        write!(
            f,
            "{}:{}/{}@{}",
            self.ty, self.namespace, self.name, self.unit
        )
    }
}

/// Validates a tag key.
///
/// Tag keys currently only need to not contain ASCII control characters. This might change.
pub(crate) fn is_valid_tag_key(tag_key: &str) -> bool {
    // iterating over bytes produces better asm, and we're only checking for ascii chars
    for &byte in tag_key.as_bytes() {
        if (byte as char).is_ascii_control() {
            return false;
        }
    }
    true
}

/// Validates a tag value.
///
/// Tag values are never entirely rejected, but invalid characters (ASCII control characters) are
/// stripped out.
pub(crate) fn validate_tag_value(tag_value: &mut String) {
    tag_value.retain(|c| !c.is_ascii_control());
}

/// Parses the `name[@unit]` part of a metric string.
///
/// Returns [`MetricUnit::None`] if no unit is specified. Returns `None` if the name or value are
/// invalid.
pub(crate) fn parse_name_unit(string: &str) -> Option<(&str, MetricUnit)> {
    let mut components = string.split('@');
    let name = components.next()?;
    if !relay_base_schema::metrics::is_valid_metric_name(name) {
        return None;
    }

    let unit = match components.next() {
        Some(s) => s.parse().ok()?,
        None => MetricUnit::default(),
    };

    Some((name, unit))
}

/// Hashes the given set value.
///
/// Sets only guarantee 32-bit accuracy, but arbitrary strings are allowed on the protocol. Upon
/// parsing, they are hashed and only used as hashes subsequently.
pub(crate) fn hash_set_value(string: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(string.as_bytes());
    hasher.finish32()
}

/// A single metric value representing the payload sent from clients.
///
/// As opposed to bucketed metric aggregations, this single metrics always represent a single
/// submission and cannot store multiple values.
///
/// See the [crate documentation](crate) for general information on Metrics.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Metric {
    /// The metric resource identifier, in short MRI.
    ///
    /// MRIs follow three core principles:

    /// 1. **Robustness:** Metrics must be addressed via a stable identifier. During ingestion in
    ///    Relay and Snuba, metrics are preaggregated and bucketed based on this identifier, so it
    ///    cannot change over time without breaking bucketing.
    /// 2. **Uniqueness:** The identifier for metrics must be unique across variations of units and
    ///    metric types, within and across use cases, as well as between projects and
    ///    organizations.
    /// 3. **Abstraction:** The user-facing product changes its terminology over time, and splits
    ///    concepts into smaller parts. The internal metric identifiers must abstract from that,
    ///    and offer sufficient granularity to allow for such changes.
    ///
    /// MRIs have the format `<type>:<ns>/<name>@<unit>`, comprising the following components:
    ///
    /// * **Type:** counter (`c`), set (`s`), distribution (`d`), gauge (`g`), and evaluated (`e`)
    ///   for derived numeric metrics (the latter is a pure query-time construct and is not relevant
    ///   to Relay or ingestion). See [`MetricType`].
    /// * **Namespace:** Identifying the product entity and use case affiliation of the metric. See
    /// [`MetricNamespace`].
    /// * **Name:** The display name of the metric in the allowed character set.
    /// * **Unit:** The verbatim unit name. See [`MetricUnit`].
    ///
    /// Parsing a metric (or set of metrics) should not fail hard if the MRI is invalid, so this is
    /// typed as string. Later in the metrics aggregator, the MRI is parsed using
    /// [`MetricResourceIdentifier`] and validated for invalid characters as well.
    /// [`MetricResourceIdentifier`] is also used in the kafka producer to route certain namespaces
    /// to certain topics.
    pub name: String,
    /// The value of the metric.
    ///
    /// [Distributions](MetricType::Distribution) and [counters](MetricType::Counter) require numeric
    /// values which can either be integral or floating point. In contrast, [sets](MetricType::Set)
    /// and [gauges](MetricType::Gauge) can store any unique value including custom strings.
    #[serde(flatten)]
    pub value: MetricValue,
    /// The timestamp for this metric value.
    pub timestamp: UnixTimestamp,
    /// A list of tags adding dimensions to the metric for filtering and aggregation.
    ///
    /// Tags are preceded with a hash `#` and specified in a comma (`,`) separated list. Each tag
    /// can either be a tag name, or a `name:value` combination. For tags with missing values, an
    /// empty `""` value is assumed.
    ///
    /// Tags are optional and can be omitted.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl Metric {
    /// Creates a new metric using the MRI naming format.
    ///
    /// See [`Metric::name`].
    ///
    /// MRI is the metric resource identifier in the format `<type>:<ns>/<name>@<unit>`. This name
    /// ensures that just the name determines correct bucketing of metrics with name collisions.
    pub fn new_mri(
        namespace: MetricNamespace,
        name: impl AsRef<str>,
        unit: MetricUnit,
        value: MetricValue,
        timestamp: UnixTimestamp,
        tags: BTreeMap<String, String>,
    ) -> Self {
        Self {
            name: MetricResourceIdentifier {
                ty: value.ty(),
                name: name.as_ref(),
                namespace,
                unit,
            }
            .to_string(),
            value,
            timestamp,
            tags,
        }
    }
}

/// Common interface for `Metric` and `Bucket`.
pub trait MetricsContainer {
    /// Returns the full metric name (MRI) of this container.
    fn name(&self) -> &str;

    /// Returns the number of raw data points in this container.
    ///
    /// See [`BucketValue::len`](crate::BucketValue::len).
    fn len(&self) -> usize;

    /// Returns `true` if this container contains no values.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the value of the given tag, if present.
    fn tag(&self, name: &str) -> Option<&str>;

    /// Removes the given tag, if present.
    fn remove_tag(&mut self, name: &str);
}

impl MetricsContainer for Metric {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn len(&self) -> usize {
        1
    }

    fn tag(&self, name: &str) -> Option<&str> {
        self.tags.get(name).map(|s| s.as_str())
    }

    fn remove_tag(&mut self, name: &str) {
        self.tags.remove(name);
    }
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_sizeof_unit() {
        assert_eq!(std::mem::size_of::<MetricUnit>(), 16);
        assert_eq!(std::mem::align_of::<MetricUnit>(), 1);
    }

    #[test]
    fn test_serde_json() {
        let json = r#"{
  "name": "foo",
  "type": "c",
  "value": 42.0,
  "timestamp": 4711,
  "tags": {
    "empty": "",
    "full": "value"
  }
}"#;

        let metric = serde_json::from_str::<Metric>(json).unwrap();
        insta::assert_debug_snapshot!(metric, @r#"
        Metric {
            name: "foo",
            value: Counter(
                42.0,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {
                "empty": "",
                "full": "value",
            },
        }
        "#);

        let string = serde_json::to_string_pretty(&metric).unwrap();
        assert_eq!(string, json);
    }

    #[test]
    fn test_serde_json_defaults() {
        // NB: timestamp is required in JSON as opposed to the text representation
        let json = r#"{
            "name": "foo",
            "value": 42,
            "type": "c",
            "timestamp": 4711
        }"#;

        let metric = serde_json::from_str::<Metric>(json).unwrap();
        insta::assert_debug_snapshot!(metric, @r#"
        Metric {
            name: "foo",
            value: Counter(
                42.0,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "#);
    }
}
