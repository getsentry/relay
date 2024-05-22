use relay_metrics::{
    BucketMetadata, CounterType, MetricName, MetricNamespace, MetricResourceIdentifier, MetricType,
};
use serde::de::IgnoredAny;
use serde::{de, Deserialize, Deserializer};

use crate::metrics::{BucketSummary, ExtractionMode, TrackableBucket};

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
    tags: Tags,
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

    fn summary(&self, mode: ExtractionMode) -> BucketSummary {
        let mri = match MetricResourceIdentifier::parse(self.name()) {
            Ok(mri) => mri,
            Err(_) => return BucketSummary::default(),
        };

        match mri.namespace {
            MetricNamespace::Transactions => {
                let usage = matches!(mode, ExtractionMode::Usage);
                let count = match self.value {
                    MinimalValue::Counter(c) if usage && mri.name == "usage" => c.to_f64() as usize,
                    MinimalValue::Distribution(d) if !usage && mri.name == "duration" => d.0,
                    _ => 0,
                };
                let has_profile = matches!(mri.name.as_ref(), "usage" | "duration")
                    && self.tags.has_profile.is_some();
                BucketSummary::Transactions { count, has_profile }
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
    Distribution(SeqCount),
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

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(default)]
struct Tags {
    has_profile: Option<IgnoredAny>,
}

/// Deserializes only the count of a sequence ingoring all individual items.
#[derive(Clone, Copy, Debug, Default)]
struct SeqCount(usize);

impl<'de> Deserialize<'de> for SeqCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'a> de::Visitor<'a> for Visitor {
            type Value = SeqCount;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'a>,
            {
                let mut count = 0;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    count += 1;
                }

                Ok(SeqCount(count))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use relay_metrics::Bucket;

    use super::*;

    #[test]
    fn test_seq_count() {
        let SeqCount(s) = serde_json::from_str("[1, 2, 3, 4, 5]").unwrap();
        assert_eq!(s, 5);

        let SeqCount(s) = serde_json::from_str("[1, 2, \"mixed\", 4, 5]").unwrap();
        assert_eq!(s, 5);

        let SeqCount(s) = serde_json::from_str("[]").unwrap();
        assert_eq!(s, 0);
    }

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
            assert_eq!(
                b.summary(ExtractionMode::Usage),
                mb.summary(ExtractionMode::Usage)
            );
            assert_eq!(
                b.summary(ExtractionMode::Duration),
                mb.summary(ExtractionMode::Duration)
            );
            assert_eq!(b.metadata, mb.metadata);
        }

        let duration = min_buckets
            .iter()
            .map(|b| b.summary(ExtractionMode::Duration))
            .collect::<Vec<_>>();
        let usage = min_buckets
            .iter()
            .map(|b| b.summary(ExtractionMode::Usage))
            .collect::<Vec<_>>();

        assert_debug_snapshot!(duration, @r###"
        [
            Transactions {
                count: 4,
                has_profile: true,
            },
            Transactions {
                count: 0,
                has_profile: false,
            },
            Spans(
                3,
            ),
            None,
            None,
        ]
        "###);
        assert_debug_snapshot!(usage, @r###"
        [
            Transactions {
                count: 0,
                has_profile: true,
            },
            Transactions {
                count: 3,
                has_profile: false,
            },
            Spans(
                3,
            ),
            None,
            None,
        ]
        "###);
    }
}
