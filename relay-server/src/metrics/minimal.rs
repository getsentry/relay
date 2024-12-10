use relay_metrics::{
    BucketMetadata, CounterType, MetricName, MetricNamespace, MetricResourceIdentifier, MetricType,
};
use serde::de::IgnoredAny;
use serde::Deserialize;

use crate::metrics::{BucketSummary, TrackableBucket};

/// Bucket which parses only the minimally required information to implement [`TrackableBucket`].
///
/// Note: this can not be used to parse untrusted buckets, there is no normalization of data
/// happening, e.g. a missing namespace will not turn the bucket into a custom metric bucket.
#[derive(Deserialize)]
pub struct MinimalTrackableBucket {
    name: MetricName,
    #[serde(flatten)]
    value: MinimalValue,
    #[serde(default)]
    metadata: BucketMetadata,
}

impl TrackableBucket for MinimalTrackableBucket {
    fn name(&self) -> &relay_metrics::MetricName {
        &self.name
    }

    fn ty(&self) -> relay_metrics::MetricType {
        self.value.ty()
    }

    fn summary(&self) -> BucketSummary {
        let mri = match MetricResourceIdentifier::parse(self.name()) {
            Ok(mri) => mri,
            Err(_) => return BucketSummary::default(),
        };

        match mri.namespace {
            MetricNamespace::Transactions => {
                let count = match self.value {
                    MinimalValue::Counter(c) if mri.name == "usage" => c.to_f64() as usize,
                    _ => 0,
                };
                BucketSummary::Transactions(count)
            }
            MetricNamespace::Spans => BucketSummary::Spans(match self.value {
                MinimalValue::Counter(c) if mri.name == "usage" => c.to_f64() as usize,
                _ => 0,
            }),
            _ => BucketSummary::default(),
        }
    }

    fn metadata(&self) -> BucketMetadata {
        self.metadata
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
enum MinimalValue {
    #[serde(rename = "c")]
    Counter(CounterType),
    #[serde(rename = "d")]
    Distribution(IgnoredAny),
    #[serde(rename = "s")]
    Set(IgnoredAny),
    #[serde(rename = "g")]
    Gauge(IgnoredAny),
}

impl MinimalValue {
    fn ty(self) -> MetricType {
        match self {
            MinimalValue::Counter(_) => MetricType::Counter,
            MinimalValue::Distribution(_) => MetricType::Distribution,
            MinimalValue::Set(_) => MetricType::Set,
            MinimalValue::Gauge(_) => MetricType::Gauge,
        }
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use relay_metrics::Bucket;

    use super::*;

    #[test]
    fn test_buckets() {
        let json = r#"[
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "d:transactions/duration@none",
    "type": "d",
    "value": [
      36.0,
      49.0,
      57.0,
      68.0
    ],
    "tags": {
      "has_profile": "true"
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "c:transactions/usage@none",
    "type": "c",
    "value": 3.0,
    "tags": {
      "route": "user_index"
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "c:spans/usage@none",
    "type": "c",
    "value": 3.0,
    "tags": {
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "g:custom/unrelated@none",
    "type": "g",
    "value": {
      "last": 25.0,
      "min": 17.0,
      "max": 42.0,
      "sum": 2210.0,
      "count": 85
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "s:custom/endpoint.users@none",
    "type": "s",
    "value": [
      3182887624,
      4267882815
    ],
    "tags": {
      "route": "user_index"
    }
  }
]"#;
        let buckets: Vec<Bucket> = serde_json::from_str(json).unwrap();
        let min_buckets: Vec<MinimalTrackableBucket> = serde_json::from_str(json).unwrap();

        for (b, mb) in buckets.iter().zip(min_buckets.iter()) {
            assert_eq!(b.name(), mb.name());
            assert_eq!(b.summary(), mb.summary());
            assert_eq!(b.metadata, mb.metadata);
        }

        let summary = min_buckets.iter().map(|b| b.summary()).collect::<Vec<_>>();
        assert_debug_snapshot!(summary, @r###"
        [
            Transactions(
                0,
            ),
            Transactions(
                3,
            ),
            Spans(
                3,
            ),
            None,
            None,
        ]
        "###);
    }
}
