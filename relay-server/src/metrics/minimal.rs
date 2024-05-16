use relay_metrics::{
    BucketMetadata, CounterType, MetricName, MetricNamespace, MetricResourceIdentifier, MetricType,
};
use serde::de::IgnoredAny;
use serde::{de, Deserialize, Deserializer};

use crate::metrics::{BucketSummary, TrackableBucket};
use crate::utils::ExtractionMode;

#[derive(Deserialize)]
pub struct MinimalTrackableBucket {
    name: MetricName,
    tags: Tags,
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
