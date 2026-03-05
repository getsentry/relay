use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;

use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_metrics::{
    Bucket, BucketMetadata, BucketValue, MetricNamespace, MetricResourceIdentifier, MetricUnit,
};

use crate::metrics_extraction::IntoMetric;

/// Enumerates the metrics extracted from transaction payloads.
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionMetric {
    /// An internal counter metric that tracks transaction usage.
    ///
    /// This metric does not have any of the common tags for the performance product, but instead
    /// carries internal information for accounting purposes.
    Usage,
    /// An internal counter metric used to compute dynamic sampling biases.
    ///
    /// See '<https://github.com/getsentry/sentry/blob/d3d9ed6cfa6e06aa402ab1d496dedbb22b3eabd7/src/sentry/dynamic_sampling/prioritise_projects.py#L40>'.
    CountPerRootProject { tags: TransactionCPRTags },
}

impl IntoMetric for TransactionMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Bucket {
        let namespace = MetricNamespace::Transactions;

        let (name, value, unit, tags) = match self {
            Self::Usage => (
                Cow::Borrowed("usage"),
                BucketValue::counter(1.into()),
                MetricUnit::None,
                Default::default(),
            ),
            Self::CountPerRootProject { tags } => (
                Cow::Borrowed("count_per_root_project"),
                BucketValue::counter(1.into()),
                MetricUnit::None,
                tags.into(),
            ),
        };

        let mri = MetricResourceIdentifier {
            ty: value.ty(),
            namespace,
            name,
            unit,
        };

        // For extracted metrics we assume the `received_at` timestamp is equivalent to the time
        // in which the metric is extracted.
        let received_at = if cfg!(not(test)) {
            UnixTimestamp::now()
        } else {
            UnixTimestamp::from_secs(0)
        };

        Bucket {
            timestamp,
            width: 0,
            name: mri.to_string().into(),
            value,
            tags,
            metadata: BucketMetadata::new(received_at),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionCPRTags {
    pub decision: String,
    pub target_project_id: ProjectId,
    pub universal_tags: CommonTags,
}

impl From<TransactionCPRTags> for BTreeMap<String, String> {
    fn from(value: TransactionCPRTags) -> Self {
        let mut map: BTreeMap<String, String> = value.universal_tags.into();
        map.insert("decision".to_owned(), value.decision);
        map.insert(
            "target_project_id".to_owned(),
            value.target_project_id.to_string(),
        );
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
    UserSubregion,
    DeviceClass,
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
            CommonTag::UserSubregion => "user.geo.subregion",
            CommonTag::DeviceClass => "device.class",
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
    /// [`max_secs_in_past`](relay_metrics::aggregator::AggregatorConfig::max_secs_in_past) and
    /// [`max_secs_in_future`](relay_metrics::aggregator::AggregatorConfig::max_secs_in_future) configuration options.
    #[error("timestamp too old or too far in the future")]
    InvalidTimestamp,
}
