use std::collections::BTreeMap;
use std::fmt::Display;

use relay_common::{MetricUnit, UnixTimestamp};
use relay_metrics::{
    CounterType, DistributionType, DurationUnit, Metric, MetricNamespace, MetricResourceIdentifier,
    MetricType, MetricValue,
};

use crate::metrics_extraction::IntoMetric;

/// Enumerates the metrics extracted from transaction payloads.
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionMetric {
    /// A set metric counting unique users.
    User { value: String, tags: CommonTags },
    /// A distribution metric for the transaction duration.
    ///
    /// Also used to count transactions, as any distribution metric features a counter.
    Duration {
        unit: DurationUnit,
        value: DistributionType,
        tags: CommonTags,
    },
    /// An internal counter metric used to compute dynamic sampling biases.
    ///
    /// See https://github.com/getsentry/sentry/blob/d3d9ed6cfa6e06aa402ab1d496dedbb22b3eabd7/src/sentry/dynamic_sampling/prioritise_projects.py#L40.
    CountPerRootProject {
        value: CounterType,
        tags: TransactionCPRTags,
    },
    A metric created from [`relay_general::Breakdowns`].
    Breakdown {
        name: String,
        value: DistributionType,
        tags: CommonTags,
    },
    Measurement {
        name: String,
        value: DistributionType,
        unit: MetricUnit,
        tags: TransactionMeasurementTags,
    },
}

impl IntoMetric for TransactionMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric {
        let namespace = MetricNamespace::Transactions;
        match self {
            TransactionMetric::User { value, tags } => Metric::new(
                MetricResourceIdentifier {
                    ty: MetricType::Set,
                    namespace,
                    name: "user",
                    unit: MetricUnit::None,
                },
                MetricValue::set_from_str(&value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::Breakdown { value, tags, name } => Metric::new(
                MetricResourceIdentifier {
                    ty: MetricType::Distribution,
                    namespace,
                    name: format!("breakdowns.{name}").as_str(),
                    unit: MetricUnit::Duration(DurationUnit::MilliSecond),
                },
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::CountPerRootProject { value, tags } => Metric::new(
                MetricResourceIdentifier {
                    ty: MetricType::Counter,
                    namespace,
                    name: "count_per_root_project",
                    unit: MetricUnit::None,
                },
                MetricValue::Counter(value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::Duration { unit, value, tags } => Metric::new(
                MetricResourceIdentifier {
                    ty: MetricType::Distribution,
                    namespace,
                    name: "duration",
                    unit: MetricUnit::Duration(unit),
                },
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::Measurement {
                name: kind,
                value,
                unit,
                tags,
            } => Metric::new(
                MetricResourceIdentifier {
                    ty: MetricType::Distribution,
                    namespace,
                    name: format!("measurements.{kind}").as_str(),
                    unit,
                },
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct TransactionMeasurementTags {
    pub measurement_rating: Option<String>,
    pub universal_tags: CommonTags,
}

impl From<TransactionMeasurementTags> for BTreeMap<String, String> {
    fn from(value: TransactionMeasurementTags) -> Self {
        let mut map: BTreeMap<String, String> = value.universal_tags.into();
        if let Some(decision) = value.measurement_rating {
            map.insert("measurement_rating".to_string(), decision);
        }
        map
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionCPRTags {
    pub decision: String,
    pub universal_tags: CommonTags,
}

impl From<TransactionCPRTags> for BTreeMap<String, String> {
    fn from(value: TransactionCPRTags) -> Self {
        let mut map: BTreeMap<String, String> = value.universal_tags.into();
        map.insert("decision".to_string(), value.decision);
        map
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct CommonTags(pub BTreeMap<CommonTag, String>);

impl From<CommonTags> for BTreeMap<String, String> {
    fn from(value: CommonTags) -> Self {
        value
            .0
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

/// The most common tags for transaction metrics.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CommonTag {
    Release,
    Dist,
    Environment,
    Transaction,
    Platform,
    TransactionStatus,
    TransactionOp,
    HttpMethod,
    Custom(String),
}

impl Display for CommonTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CommonTag::Release => "release",
            CommonTag::Dist => "dist",
            CommonTag::Environment => "environment",
            CommonTag::Transaction => "transaction",
            CommonTag::Platform => "platform",
            CommonTag::TransactionStatus => "transaction.status",
            CommonTag::TransactionOp => "transaction.op",
            CommonTag::HttpMethod => "http.method",
            CommonTag::Custom(s) => s,
        };
        write!(f, "{name}")
    }
}

/// Error returned from [`extract_transaction_metrics`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExtractMetricsError {
    /// The start or end timestamps are missing from the event payload.
    #[error("no valid timestamp could be found in the event")]
    MissingTimestamp,
    /// The event timestamp is outside the supported range.
    ///
    /// The supported range is derived from the
    /// [`max_secs_in_past`](AggregatorConfig::max_secs_in_past) and
    /// [`max_secs_in_future`](AggregatorConfig::max_secs_in_future) configuration options.
    #[error("timestamp too old or too far in the future")]
    InvalidTimestamp,
}
