use std::collections::BTreeMap;
use std::fmt::Display;

use chrono::Duration;
use relay_common::{DurationUnit, MetricUnit, UnixTimestamp};
use relay_metrics::{Metric, MetricNamespace, MetricValue};

use crate::metrics_extraction::IntoMetric;

#[derive(Clone, Debug)]
pub(crate) enum SpanMetric {
    User { value: String, tags: SpanTags },
    Duration { value: Duration, tags: SpanTags },
    ExclusiveTime { value: f64, tags: SpanTags },
}

impl IntoMetric for SpanMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric {
        let namespace = MetricNamespace::Spans;

        match self {
            SpanMetric::User { value, tags } => Metric::new_mri(
                namespace,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(&value),
                timestamp,
                tags.into(),
            ),
            SpanMetric::Duration { value, tags } => Metric::new_mri(
                namespace,
                "duration",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(relay_common::chrono_to_positive_millis(value)),
                timestamp,
                tags.into(),
            ),
            SpanMetric::ExclusiveTime { value, tags } => Metric::new_mri(
                namespace,
                "exclusive_time",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(value),
                timestamp,
                tags.into(),
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SpanTags(pub(crate) BTreeMap<SpanTagKey, String>);

impl From<SpanTags> for BTreeMap<String, String> {
    fn from(tags: SpanTags) -> Self {
        tags.0
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SpanTagKey {
    // Specific to a transaction
    Release,
    User,
    Environment,
    Transaction,
    TransactionMethod,
    TransactionOp,
    HttpStatusCode,

    // Specific to spans
    Description,
    Group,
    SpanOp,
    Category,
    Module,
    Action,
    Domain,
    System,
    Status,
    StatusCode,
}

impl Display for SpanTagKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            SpanTagKey::Release => "release",
            SpanTagKey::User => "user",
            SpanTagKey::Environment => "environment",
            SpanTagKey::Transaction => "transaction",
            SpanTagKey::TransactionMethod => "transaction.method",
            SpanTagKey::TransactionOp => "transaction.op",
            SpanTagKey::HttpStatusCode => "http.status_code",

            SpanTagKey::Description => "span.description",
            SpanTagKey::Group => "span.group",
            SpanTagKey::SpanOp => "span.op",
            SpanTagKey::Category => "span.category",
            SpanTagKey::Module => "span.module",
            SpanTagKey::Action => "span.action",
            SpanTagKey::Domain => "span.domain",
            SpanTagKey::System => "span.system",
            SpanTagKey::Status => "span.status",
            SpanTagKey::StatusCode => "span.status_code",
        };
        write!(f, "{name}")
    }
}
