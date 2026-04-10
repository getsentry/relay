use std::fmt;

use relay_metrics::{CounterType, MetricName, MetricNamespace, MetricResourceIdentifier};
use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::metrics::{BucketSummary, TrackableBucket};

/// Bucket which parses only the minimally required information to implement [`TrackableBucket`].
///
/// Note: this can not be used to parse untrusted buckets, there is no normalization of data
/// happening, e.g. a missing namespace will not turn the bucket into a custom metric bucket.
#[derive(Deserialize)]
pub struct MinimalTrackableBucket {
    name: MetricName,
    #[serde(default)]
    tags: Tags,
    #[serde(flatten)]
    value: MinimalValue,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
struct Tags {
    #[serde(default, deserialize_with = "bool_de")]
    is_segment: bool,
    #[serde(default, deserialize_with = "bool_de")]
    was_transaction: bool,
}

fn bool_de<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = bool;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a boolean or a string representing a boolean")
        }

        fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<bool, E> {
            // Every non true value is considered false.
            Ok(v == "true")
        }
    }

    // Never fail, default to false.
    Ok(deserializer.deserialize_any(Visitor).unwrap_or(false))
}

impl TrackableBucket for MinimalTrackableBucket {
    fn name(&self) -> &relay_metrics::MetricName {
        &self.name
    }

    fn summary(&self) -> BucketSummary {
        let mri = match MetricResourceIdentifier::parse(self.name()) {
            Ok(mri) => mri,
            Err(_) => return BucketSummary::default(),
        };

        match mri.namespace {
            MetricNamespace::Spans => match self.value {
                MinimalValue::Counter(c) if mri.name == "usage" => BucketSummary::Spans {
                    count: c.to_f64() as usize,
                    is_segment: self.tags.is_segment,
                    was_transaction: self.tags.was_transaction,
                },
                _ => BucketSummary::Spans {
                    count: 0,
                    is_segment: false,
                    was_transaction: false,
                },
            },
            _ => BucketSummary::default(),
        }
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
    "name": "c:spans/usage@none",
    "type": "c",
    "value": 2.0,
    "tags": {
      "is_segment": "true"
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "c:spans/unrelated@none",
    "type": "c",
    "value": 2.0
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
        }

        let summary = min_buckets.iter().map(|b| b.summary()).collect::<Vec<_>>();
        assert_debug_snapshot!(summary, @r"
        [
            Spans {
                count: 2,
                is_segment: true,
                was_transaction: false,
            },
            Spans {
                count: 0,
                is_segment: false,
                was_transaction: false,
            },
            Spans {
                count: 3,
                is_segment: false,
                was_transaction: false,
            },
            None,
            None,
        ]
        ");
    }
}
