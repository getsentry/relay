use std::collections::BTreeMap;
use std::fmt::Display;

use relay_common::time::UnixTimestamp;
use relay_metrics::{
    CounterType, DistributionType, DurationUnit, Metric, MetricNamespace, MetricUnit, MetricValue,
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
        tags: TransactionDurationTags,
    },
    /// An internal counter metric used to compute dynamic sampling biases.
    ///
    /// See '<https://github.com/getsentry/sentry/blob/d3d9ed6cfa6e06aa402ab1d496dedbb22b3eabd7/src/sentry/dynamic_sampling/prioritise_projects.py#L40>'.
    CountPerRootProject {
        value: CounterType,
        tags: TransactionCPRTags,
    },
    /// A metric created from [`relay_event_schema::protocol::Breakdowns`].
    Breakdown {
        name: String,
        value: DistributionType,
        tags: CommonTags,
    },
    /// A metric created from a [`relay_event_schema::protocol::Measurement`].
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
            TransactionMetric::User { value, tags } => Metric::new_mri(
                namespace,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(&value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::Breakdown { value, tags, name } => Metric::new_mri(
                namespace,
                format!("breakdowns.{name}").as_str(),
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::CountPerRootProject { value, tags } => Metric::new_mri(
                namespace,
                "count_per_root_project",
                MetricUnit::None,
                MetricValue::Counter(value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::Duration { unit, value, tags } => Metric::new_mri(
                namespace,
                "duration",
                MetricUnit::Duration(unit),
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
            TransactionMetric::Measurement {
                name: kind,
                value,
                unit,
                tags,
            } => Metric::new_mri(
                namespace,
                format!("measurements.{kind}").as_str(),
                unit,
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct TransactionDurationTags {
    pub has_profile: bool,
    pub universal_tags: CommonTags,
}

impl From<TransactionDurationTags> for BTreeMap<String, String> {
    fn from(tags: TransactionDurationTags) -> Self {
        let mut map: BTreeMap<String, String> = tags.universal_tags.into();
        if tags.has_profile {
            map.insert("has_profile".to_string(), "true".to_string());
        }
        map
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
    HttpStatusCode,
    BrowserName,
    OsName,
    GeoCountryCode,
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
            CommonTag::HttpStatusCode => "http.status_code",
            CommonTag::BrowserName => "browser.name",
            CommonTag::OsName => "os.name",
            CommonTag::GeoCountryCode => "geo.country_code",
            CommonTag::Custom(s) => s,
        };
        write!(f, "{name}")
    }
}

/// Error returned from transaction metrics extraction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExtractMetricsError {
    /// The start or end timestamps are missing from the event payload.
    #[error("no valid timestamp could be found in the event")]
    MissingTimestamp,
    /// The event timestamp is outside the supported range.
    ///
    /// The supported range is derived from the
    /// [`max_secs_in_past`](relay_metrics::AggregatorConfig::max_secs_in_past) and
    /// [`max_secs_in_future`](relay_metrics::AggregatorConfig::max_secs_in_future) configuration options.
    #[error("timestamp too old or too far in the future")]
    InvalidTimestamp,
}
