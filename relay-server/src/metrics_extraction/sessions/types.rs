use std::collections::BTreeMap;
use std::fmt::{self, Display};

use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::SessionStatus;
use relay_metrics::{
    Bucket, BucketMetadata, BucketValue, CounterType, MetricNamespace, MetricResourceIdentifier,
    MetricUnit,
};
use uuid::Uuid;

use crate::metrics_extraction::IntoMetric;

/// Enumerates the metrics extracted from session payloads.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionMetric {
    /// The number of sessions collected in a given time frame.
    Session {
        counter: CounterType,
        tags: SessionSessionTags,
    },
    /// The number of unique session users for a given time frame.
    User {
        distinct_id: String,
        tags: SessionUserTags,
    },
    /// The number of sessions that errored in a given time frame.
    ///
    /// Because multiple session updates can be received for the same session ID,
    /// this is collected as a [`relay_metrics::MetricType::Set`] metric rather than a simple counter.
    Error { session_id: Uuid, tags: CommonTags },
}

/// Tags that are set on the `session` counter metric.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSessionTags {
    pub status: String,
    pub common_tags: CommonTags,
}

/// Tags that are set on the `user` set metric.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionUserTags {
    pub status: Option<SessionStatus>,
    pub abnormal_mechanism: Option<String>,
    pub common_tags: CommonTags,
}

/// Tags that are set on all session metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommonTags {
    pub release: String,
    pub environment: Option<String>,
    pub sdk: Option<String>,
}

impl From<CommonTags> for BTreeMap<String, String> {
    fn from(value: CommonTags) -> Self {
        let mut map = BTreeMap::new();

        map.insert("release".to_owned(), value.release);

        if let Some(environment) = value.environment {
            map.insert("environment".into(), environment);
        }

        if let Some(sdk) = value.sdk {
            map.insert("sdk".to_owned(), sdk);
        }
        map
    }
}

impl From<SessionUserTags> for BTreeMap<String, String> {
    fn from(value: SessionUserTags) -> Self {
        let mut map: BTreeMap<String, String> = value.common_tags.into();
        if let Some(status) = value.status {
            map.insert("session.status".to_owned(), status.to_string());
        }

        if let Some(abnormal_mechanism) = value.abnormal_mechanism {
            map.insert("abnormal_mechanism".to_owned(), abnormal_mechanism);
        }

        map
    }
}

impl From<SessionSessionTags> for BTreeMap<String, String> {
    fn from(value: SessionSessionTags) -> Self {
        let mut map: BTreeMap<String, String> = value.common_tags.into();
        map.insert("session.status".to_owned(), value.status);

        map
    }
}

impl IntoMetric for SessionMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Bucket {
        let name = self.to_string();

        let (value, tags) = match self {
            SessionMetric::Error {
                session_id: id,
                tags,
            } => (BucketValue::set_from_display(id), tags.into()),
            SessionMetric::User { distinct_id, tags } => {
                (BucketValue::set_from_display(distinct_id), tags.into())
            }
            SessionMetric::Session { counter, tags } => {
                (BucketValue::Counter(counter), tags.into())
            }
        };

        let mri = MetricResourceIdentifier {
            ty: value.ty(),
            namespace: MetricNamespace::Sessions,
            name: name.into(),
            unit: MetricUnit::None,
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

impl Display for SessionMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session { .. } => write!(f, "session"),
            Self::User { .. } => write!(f, "user"),
            Self::Error { .. } => write!(f, "error"),
        }
    }
}
