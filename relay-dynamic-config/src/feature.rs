use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Features exposed by project config.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Feature {
    /// Enables ingestion of Session Replays (Replay Recordings and Replay Events).
    ///
    /// Serialized as `organizations:session-replay`.
    #[serde(rename = "organizations:session-replay")]
    SessionReplay,
    /// Enables data scrubbing of replay recording payloads.
    ///
    /// Serialized as `organizations:session-replay-recording-scrubbing`.
    #[serde(rename = "organizations:session-replay-recording-scrubbing")]
    SessionReplayRecordingScrubbing,
    /// Enables combining session replay envelope items (Replay Recordings and Replay Events).
    /// into one Kafka message.
    ///
    /// Serialized as `organizations:session-replay-combined-envelope-items`.
    #[serde(rename = "organizations:session-replay-combined-envelope-items")]
    SessionReplayCombinedEnvelopeItems,
    /// Enables new User Feedback ingest.
    ///
    /// TODO(jferg): rename to UserFeedbackIngest once old UserReport logic is deprecated.
    ///
    /// Serialized as `organizations:user-feedback-ingest`.
    #[serde(rename = "organizations:user-feedback-ingest")]
    UserReportV2Ingest,
    /// Enables device.class synthesis
    ///
    /// Enables device.class tag synthesis on mobile events.
    ///
    /// Serialized as `organizations:device-class-synthesis`.
    #[serde(rename = "organizations:device-class-synthesis")]
    DeviceClassSynthesis,
    /// Enables metric extraction from spans.
    ///
    /// Serialized as `projects:span-metrics-extraction`.
    #[serde(rename = "projects:span-metrics-extraction")]
    ExtractSpansAndSpanMetricsFromEvent,
    /// Allow ingestion of metrics in the "custom" namespace.
    ///
    /// Serialized as `organizations:custom-metrics`.
    #[serde(rename = "organizations:custom-metrics")]
    CustomMetrics,

    /// Enable processing profiles.
    ///
    /// Serialized as `organizations:profiling`.
    #[serde(rename = "organizations:profiling")]
    Profiling,
    /// Enable standalone span ingestion.
    ///
    /// Serialized as `organizations:standalone-span-ingestion`.
    #[serde(rename = "organizations:standalone-span-ingestion")]
    StandaloneSpanIngestion,
    /// Enable metric metadata.
    ///
    /// Serialized as `organizations:metric-meta`.
    #[serde(rename = "organizations:metric-meta")]
    MetricMeta,
    /// Enable processing and extracting data from profiles that would normally be dropped by dynamic sampling.
    ///
    /// This is required for [slowest function aggregation](https://github.com/getsentry/snuba/blob/b5311b404a6bd73a9e1997a46d38e7df88e5f391/snuba/snuba_migrations/functions/0001_functions.py#L209-L256). The profile payload will be dropped on the sentry side.
    ///
    /// Serialized as `projects:profiling-ingest-unsampled-profiles`.
    #[serde(rename = "projects:profiling-ingest-unsampled-profiles")]
    IngestUnsampledProfiles,

    /// Discard transactions in a spans-only world.
    ///
    /// Serialized as `projects:discard-transaction`.
    #[serde(rename = "projects:discard-transaction")]
    DiscardTransaction,

    /// Enable continuous profiling.
    ///
    /// Serialized as `organizations:continuous-profiling`.
    #[serde(rename = "organizations:continuous-profiling")]
    ContinuousProfiling,

    /// When enabled, every standalone segment span will be duplicated as a transaction.
    ///
    /// This allows support of product features that rely on transactions for SDKs that only
    /// send spans.
    ///
    /// Serialized as `projects:extract-transaction-from-segment-span`.
    #[serde(rename = "projects:extract-transaction-from-segment-span")]
    ExtractTransactionFromSegmentSpan,

    /// Deprecated, still forwarded for older downstream Relays.
    #[doc(hidden)]
    #[serde(rename = "organizations:transaction-name-mark-scrubbed-as-sanitized")]
    Deprecated1,
    /// Deprecated, still forwarded for older downstream Relays.
    #[doc(hidden)]
    #[serde(rename = "organizations:transaction-name-normalize")]
    Deprecated2,
    /// Deprecated, still forwarded for older downstream Relays.
    #[doc(hidden)]
    #[serde(rename = "projects:extract-standalone-spans")]
    Deprecated4,
    /// Deprecated, still forwarded for older downstream Relays.
    #[doc(hidden)]
    #[serde(rename = "projects:span-metrics-extraction-resource")]
    Deprecated5,
    /// Deprecated, still forwarded for older downstream Relays.
    #[doc(hidden)]
    #[serde(rename = "projects:span-metrics-extraction-all-modules")]
    Deprected6,
    /// Forward compatibility.
    #[doc(hidden)]
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
