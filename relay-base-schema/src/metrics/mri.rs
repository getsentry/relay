use std::fmt;
use std::{borrow::Cow, error::Error};

use crate::metrics::MetricUnit;
use serde::{Deserialize, Serialize};

/// The type of a [`MetricResourceIdentifier`], determining its aggregation and evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum MetricType {
    /// Counts instances of an event.
    ///
    /// Counters can be incremented and decremented. The default operation is to increment a counter
    /// by `1`, although increments by larger values are equally possible.
    ///
    /// Counters are declared as `"c"`. Alternatively, `"m"` is allowed.
    Counter,
    /// Builds a statistical distribution over values reported.
    ///
    /// Based on individual reported values, distributions allow to query the maximum, minimum, or
    /// average of the reported values, as well as statistical quantiles. With an increasing number
    /// of values in the distribution, its accuracy becomes approximate.
    ///
    /// Distributions are declared as `"d"`. Alternatively, `"d"` and `"ms"` are allowed.
    Distribution,
    /// Counts the number of unique reported values.
    ///
    /// Sets allow sending arbitrary discrete values, including strings, and store the deduplicated
    /// count. With an increasing number of unique values in the set, its accuracy becomes
    /// approximate. It is not possible to query individual values from a set.
    ///
    /// Sets are declared as `"s"`.
    Set,
    /// Stores absolute snapshots of values.
    ///
    /// In addition to plain [counters](Self::Counter), gauges store a snapshot of the maximum,
    /// minimum and sum of all values, as well as the last reported value.
    ///
    /// Gauges are declared as `"g"`.
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
            _ => return Err(ParseMetricError),
        })
    }
}

relay_common::impl_str_serde!(MetricType, "a metric type string");

/// An error returned when metrics or MRIs cannot be parsed.
#[derive(Clone, Copy, Debug)]
pub struct ParseMetricError;

impl fmt::Display for ParseMetricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse metric")
    }
}

impl Error for ParseMetricError {}

/// The namespace of a metric.
///
/// Namespaces allow to identify the product entity that the metric got extracted from, and identify
/// the use case that the metric belongs to. These namespaces cannot be defined freely, instead they
/// are defined by Sentry. Over time, there will be more namespaces as we introduce new
/// metrics-based functionality.
///
/// # Parsing
///
/// Parsing a metric namespace from strings is infallible. Unknown strings are mapped to
/// [`MetricNamespace::Unsupported`]. Metrics with such a namespace will be dropped.
///
/// # Ingestion
///
/// During ingestion, the metric namespace is validated against a list of known and enabled
/// namespaces. Metrics in disabled namespaces are dropped during ingestion.
///
/// At a later stage, namespaces are used to route metrics to their associated infra structure and
/// enforce usecase-specific configuration.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

impl MetricNamespace {
    /// Returns the string representation for this metric type.
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricNamespace::Sessions => "sessions",
            MetricNamespace::Transactions => "transactions",
            MetricNamespace::Spans => "spans",
            MetricNamespace::Custom => "custom",
            MetricNamespace::Unsupported => "unsupported",
        }
    }
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
        f.write_str(self.as_str())
    }
}

/// A unique identifier for metrics including typing and namespacing.
///
/// MRIs have the format `<type>:<namespace>/<name>[@<unit>]`. The unit is optional and defaults to
/// [`MetricUnit::None`].
///
/// # Statsd Format
///
/// In the statsd submission payload, MRIs are sent in a more relaxed format:
/// `[<namespace>/]<name>[@<unit>]`. The differences to the internal MRI format are:
///  - Types are not part of metric naming. Instead, the type is declared in a separate field
///    following the value.
///  - The namespace is optional. If missing, `"custom"` is assumed.
///
/// # Background
///
/// MRIs follow three core principles:
///
/// 1. **Robustness:** Metrics must be addressed via a stable identifier. During ingestion in Relay
///    and Snuba, metrics are preaggregated and bucketed based on this identifier, so it cannot
///    change over time without breaking bucketing.
/// 2. **Uniqueness:** The identifier for metrics must be unique across variations of units and
///    metric types, within and across use cases, as well as between projects and organizations.
/// 3. **Abstraction:** The user-facing product changes its terminology over time, and splits
///    concepts into smaller parts. The internal metric identifiers must abstract from that, and
///    offer sufficient granularity to allow for such changes.
///
/// # Example
///
/// ```
/// use relay_base_schema::metrics::MetricResourceIdentifier;
///
/// let string = "c:custom/test@second";
/// let mri = MetricResourceIdentifier::parse(string).expect("should parse");
/// assert_eq!(mri.to_string(), string);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetricResourceIdentifier<'a> {
    /// The type of a metric, determining its aggregation and evaluation.
    ///
    /// In MRIs, the type is specified with its short name: counter (`c`), set (`s`), distribution
    /// (`d`), and gauge (`g`). See [`MetricType`] for more information.
    pub ty: MetricType,

    /// The namespace for this metric.
    ///
    /// In statsd submissions payloads, the namespace is optional and defaults to `"custom"`.
    /// Otherwise, the namespace must be declared explicitly.
    ///
    /// Note that in Sentry the namespace is also referred to as "use case" or "usecase". There is a
    /// list of known and enabled namespaces. Metrics of unknown or disabled namespaces are dropped
    /// during ingestion.
    pub namespace: MetricNamespace,

    /// The display name of the metric in the allowed character set.
    pub name: Cow<'a, str>,

    /// The verbatim unit name of the metric value.
    ///
    /// The unit is optional and defaults to [`MetricUnit::None`] (`"none"`).
    pub unit: MetricUnit,
}

impl<'a> MetricResourceIdentifier<'a> {
    /// Parses and validates an MRI.
    pub fn parse(name: &'a str) -> Result<Self, ParseMetricError> {
        // Note that this is NOT `VALUE_SEPARATOR`:
        let (raw_ty, rest) = name.split_once(':').ok_or(ParseMetricError)?;
        let ty = raw_ty.parse()?;

        Self::parse_with_type(rest, ty)
    }

    /// Parses an MRI from a string and a separate type.
    ///
    /// The given string must be a part of the MRI, including the following components:
    ///  - (optional) The namespace. If missing, it is defaulted to `"custom"`
    ///  - (required) The metric name.
    ///  - (optional) The unit. If missing, it is defaulted to "none".
    ///
    /// The metric type is never part of this string and must be supplied separately.
    pub fn parse_with_type(string: &'a str, ty: MetricType) -> Result<Self, ParseMetricError> {
        let (name_and_namespace, unit) = parse_name_unit(string).ok_or(ParseMetricError)?;

        let (namespace, name) = match name_and_namespace.split_once('/') {
            Some((raw_namespace, name)) => (raw_namespace.parse()?, name),
            None => (MetricNamespace::Custom, name_and_namespace),
        };

        let name = crate::metrics::try_normalize_metric_name(name).ok_or(ParseMetricError)?;

        Ok(MetricResourceIdentifier {
            ty,
            name,
            namespace,
            unit,
        })
    }

    /// Converts the MRI into an owned version with a static lifetime.
    pub fn into_owned(self) -> MetricResourceIdentifier<'static> {
        MetricResourceIdentifier {
            ty: self.ty,
            namespace: self.namespace,
            name: Cow::Owned(self.name.into_owned()),
            unit: self.unit,
        }
    }
}

impl<'de> Deserialize<'de> for MetricResourceIdentifier<'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Deserialize without allocation, if possible.
        let string = <Cow<'de, str>>::deserialize(deserializer)?;
        let result = MetricResourceIdentifier::parse(&string)
            .map_err(serde::de::Error::custom)?
            .into_owned();

        Ok(result)
    }
}

impl Serialize for MetricResourceIdentifier<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
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

/// Parses the `name[@unit]` part of a metric string.
///
/// Returns [`MetricUnit::None`] if no unit is specified. Returns `None` if value is invalid.
/// The name is not normalized.
fn parse_name_unit(string: &str) -> Option<(&str, MetricUnit)> {
    let mut components = string.split('@');
    let name = components.next()?;

    let unit = match components.next() {
        Some(s) => s.parse().ok()?,
        None => MetricUnit::default(),
    };

    Some((name, unit))
}

#[cfg(test)]
mod tests {
    use crate::metrics::{CustomUnit, DurationUnit};

    use super::*;

    #[test]
    fn test_sizeof_unit() {
        assert_eq!(std::mem::size_of::<MetricUnit>(), 16);
        assert_eq!(std::mem::align_of::<MetricUnit>(), 1);
    }

    #[test]
    fn test_parse_mri_lenient() {
        assert_eq!(
            MetricResourceIdentifier::parse("c:foo@none").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::None,
            },
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:foo").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::None,
            },
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:custom/foo").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::None,
            },
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:custom/foo@millisecond").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::Duration(DurationUnit::MilliSecond),
            },
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:something/foo").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Unsupported,
                name: "foo".into(),
                unit: MetricUnit::None,
            },
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:foo@something").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::Custom(CustomUnit::parse("something").unwrap()),
            },
        );
        assert!(MetricResourceIdentifier::parse("foo").is_err());
    }

    #[test]
    fn test_invalid_names_should_normalize() {
        assert_eq!(
            MetricResourceIdentifier::parse("c:f?o").unwrap().name,
            "f_o"
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:f??o").unwrap().name,
            "f_o"
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:föo").unwrap().name,
            "f_o"
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:custom/f?o")
                .unwrap()
                .name,
            "f_o"
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:custom/f??o")
                .unwrap()
                .name,
            "f_o"
        );
        assert_eq!(
            MetricResourceIdentifier::parse("c:custom/föo")
                .unwrap()
                .name,
            "f_o"
        );
    }

    #[test]
    fn test_normalize_dash_to_underscore() {
        assert_eq!(
            MetricResourceIdentifier::parse("d:foo.bar.blob-size@second").unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Distribution,
                namespace: MetricNamespace::Custom,
                name: "foo.bar.blob_size".into(),
                unit: MetricUnit::Duration(DurationUnit::Second),
            },
        );
    }

    #[test]
    fn test_deserialize_mri() {
        assert_eq!(
            serde_json::from_str::<MetricResourceIdentifier<'static>>(
                "\"c:custom/foo@millisecond\""
            )
            .unwrap(),
            MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::Duration(DurationUnit::MilliSecond),
            },
        );
    }

    #[test]
    fn test_serialize() {
        assert_eq!(
            serde_json::to_string(&MetricResourceIdentifier {
                ty: MetricType::Counter,
                namespace: MetricNamespace::Custom,
                name: "foo".into(),
                unit: MetricUnit::Duration(DurationUnit::MilliSecond),
            })
            .unwrap(),
            "\"c:custom/foo@millisecond\"".to_owned(),
        );
    }
}
