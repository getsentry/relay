use std::collections::BTreeMap;
use std::fmt::Display;

use relay_common::{DurationUnit, MetricUnit, UnixTimestamp};
use relay_metrics::{Metric, MetricNamespace, MetricValue};

use crate::metrics_extraction::IntoMetric;

pub(crate) enum SpanMetric {
    User { value: MetricValue, tags: SpanTags },
    Duration { value: MetricValue, tags: SpanTags },
    ExclusiveTime { value: MetricValue, tags: SpanTags },
}

impl IntoMetric for SpanMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric {
        let namespace = MetricNamespace::Spans;
        match self {
            SpanMetric::User { value, tags } => Metric::new_mri(
                namespace,
                "user",
                MetricUnit::None,
                value,
                timestamp,
                tags.into(),
            ),
            SpanMetric::Duration { value, tags } => Metric::new_mri(
                namespace,
                "duration",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                value,
                timestamp,
                tags.into(),
            ),
            SpanMetric::ExclusiveTime { value, tags } => Metric::new_mri(
                namespace,
                "exclusive_time",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                value,
                timestamp,
                tags.into(),
            ),
        }
    }
}

pub(crate) struct SpanTags(BTreeMap<SpanTagKey, String>);

impl From<SpanTags> for BTreeMap<String, String> {
    fn from(tags: SpanTags) -> Self {
        tags.0
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

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
    Op,
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
            SpanTagKey::Op => "span.op",
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
