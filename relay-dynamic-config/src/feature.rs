use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Features exposed by project config.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Feature {
    /// Enables ingestion of Session Replays (Replay Recordings and Replay Events).
    #[serde(rename = "organizations:session-replay")]
    SessionReplay,
    /// Enables data scrubbing of replay recording payloads.
    #[serde(rename = "organizations:session-replay-recording-scrubbing")]
    SessionReplayRecordingScrubbing,
    /// Enables new User Feedback ingest.
    ///
    /// TODO(jferg): rename to UserFeedbackIngest once old UserReport logic is deprecated.
    #[serde(rename = "organizations:user-feedback-ingest")]
    UserReportV2Ingest,
    /// Enables device.class synthesis
    ///
    /// Enables device.class tag synthesis on mobile events.
    #[serde(rename = "organizations:device-class-synthesis")]
    DeviceClassSynthesis,
    /// Enables metric extraction from spans.
    #[serde(rename = "projects:span-metrics-extraction")]
    SpanMetricsExtraction,
    /// Allow ingestion of metrics in the "custom" namespace.
    #[serde(rename = "organizations:custom-metrics")]
    CustomMetrics,

    /// Enable processing profiles
    #[serde(rename = "organizations:profiling")]
    Profiling,
    /// Enable standalone span ingestion.
    #[serde(rename = "organizations:standalone-span-ingestion")]
    StandaloneSpanIngestion,
    /// Enable metric metadata.
    #[serde(rename = "organizations:metric-meta")]
    MetricMeta,
    /// Enable processing and extracting data from profiles that would normally be dropped by dynamic sampling.
    ///
    /// This is required for [slowest function aggregation](https://github.com/getsentry/snuba/blob/b5311b404a6bd73a9e1997a46d38e7df88e5f391/snuba/snuba_migrations/functions/0001_functions.py#L209-L256). The profile payload will be dropped on the sentry side.
    #[serde(rename = "projects:profiling-ingest-unsampled-profiles")]
    IngestUnsampledProfiles,

    /// Deprecated, still forwarded for older downstream Relays.
    #[serde(rename = "organizations:transaction-name-mark-scrubbed-as-sanitized")]
    Deprecated1,
    /// Deprecated, still forwarded for older downstream Relays.
    #[serde(rename = "organizations:transaction-name-normalize")]
    Deprecated2,
    /// Deprecated, still forwarded for older downstream Relays.
    #[serde(rename = "projects:extract-standalone-spans")]
    Deprecated4,
    /// Deprecated, still forwarded for older downstream Relays.
    #[serde(rename = "projects:span-metrics-extraction-resource")]
    Deprecated5,
    /// Deprecated, still forwarded for older downstream Relays.
    #[serde(rename = "projects:span-metrics-extraction-all-modules")]
    Deprected6,
    /// Forward compatibility.
    #[serde(other)]
    Unknown,
}

/// A set of [`Feature`]s.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct FeatureSet(pub BTreeSet<Feature>);

impl FeatureSet {
    /// Returns `true` if the set of features is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` if the given feature is in the set.
    pub fn has(&self, feature: Feature) -> bool {
        self.0.contains(&feature)
    }
}

impl FromIterator<Feature> for FeatureSet {
    fn from_iter<T: IntoIterator<Item = Feature>>(iter: T) -> Self {
        Self(BTreeSet::from_iter(iter))
    }
}

impl<'de> Deserialize<'de> for FeatureSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut set = BTreeSet::<Feature>::deserialize(deserializer)?;
        set.remove(&Feature::Unknown);
        Ok(Self(set))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let features: FeatureSet =
            serde_json::from_str(r#"["organizations:session-replay", "foo"]"#).unwrap();
        assert_eq!(
            &features,
            &FeatureSet(BTreeSet::from([Feature::SessionReplay]))
        );
        assert_eq!(
            serde_json::to_string(&features).unwrap(),
            r#"["organizations:session-replay"]"#
        );
    }
}
