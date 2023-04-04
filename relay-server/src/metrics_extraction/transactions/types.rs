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
    User {
        value: String,
        tags: TransactionUserTags,
    },
    Duration {
        unit: DurationUnit,
        value: DistributionType,
        tags: TransactionDurationTags,
    },
    CountPerRootProject {
        value: CounterType,
        tags: TransactionCPRTags,
    },
    Breakdown {
        name: String,
        value: DistributionType,
        tags: TransactionBreakdownTags,
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
    pub universal_tags: BTreeMap<UniversalTags, String>,
}

impl From<TransactionMeasurementTags> for BTreeMap<String, String> {
    fn from(value: TransactionMeasurementTags) -> Self {
        let mut map = UniversalTags::into_str_keys(value.universal_tags);
        if let Some(decision) = value.measurement_rating {
            map.insert("measurement_rating".to_string(), decision);
        }
        map
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionBreakdownTags {
    pub universal_tags: BTreeMap<UniversalTags, String>,
}

impl From<TransactionBreakdownTags> for BTreeMap<String, String> {
    fn from(value: TransactionBreakdownTags) -> Self {
        UniversalTags::into_str_keys(value.universal_tags)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionCPRTags {
    pub decision: String,
    pub universal_tags: BTreeMap<UniversalTags, String>,
}

impl From<TransactionCPRTags> for BTreeMap<String, String> {
    fn from(value: TransactionCPRTags) -> Self {
        let mut map = UniversalTags::into_str_keys(value.universal_tags);
        map.insert("decision".to_string(), value.decision);
        map
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionDurationTags {
    pub universal_tags: BTreeMap<UniversalTags, String>,
}

impl From<TransactionDurationTags> for BTreeMap<String, String> {
    fn from(value: TransactionDurationTags) -> Self {
        UniversalTags::into_str_keys(value.universal_tags)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionUserTags {
    pub universal_tags: BTreeMap<UniversalTags, String>,
}

impl From<TransactionUserTags> for BTreeMap<String, String> {
    fn from(value: TransactionUserTags) -> Self {
        UniversalTags::into_str_keys(value.universal_tags)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UniversalTags {
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

impl Display for UniversalTags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            UniversalTags::Release => "release",
            UniversalTags::Dist => "dist",
            UniversalTags::Environment => "environment",
            UniversalTags::Transaction => "transaction",
            UniversalTags::Platform => "platform",
            UniversalTags::TransactionStatus => "transaction.status",
            UniversalTags::TransactionOp => "transaction.op",
            UniversalTags::HttpMethod => "http.method",
            UniversalTags::Custom(s) => s,
        };
        write!(f, "{name}")
    }
}

impl UniversalTags {
    fn into_str_keys(map: BTreeMap<UniversalTags, String>) -> BTreeMap<String, String> {
        map.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
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
