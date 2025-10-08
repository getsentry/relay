use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Feature flags of graduated features are no longer sent by sentry, but Relay needs to insert them
/// for outdated downstream Relays that may still rely on the feature flag.
pub const GRADUATED_FEATURE_FLAGS: &[Feature] = &[
    Feature::UserReportV2Ingest,
    Feature::IngestUnsampledProfiles,
    Feature::ScrubMongoDbDescriptions,
];

/// Features exposed by project config.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    /// Disables select organizations from processing mobile replay events.
    ///
    /// Serialized as `organizations:session-replay-video-disabled`.
    #[serde(rename = "organizations:session-replay-video-disabled")]
    SessionReplayVideoDisabled,
    /// Enables device.class synthesis
    ///
    /// Enables device.class tag synthesis on mobile events.
    ///
    /// Serialized as `organizations:device-class-synthesis`.
    #[serde(rename = "organizations:device-class-synthesis")]
    DeviceClassSynthesis,
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
    /// Enable standalone span ingestion via the `/traces/` OTel endpoint.
    ///
    /// Serialized as `organizations:relay-otlp-traces-endpoint`.
    #[serde(rename = "organizations:relay-otlp-traces-endpoint")]
    OtelEndpoint,
    /// Enable logs ingestion via the `/logs/` OTel endpoint.
    ///
    /// Serialized as `organizations:relay-otel-logs-endpoint`.
    #[serde(rename = "organizations:relay-otel-logs-endpoint")]
    OtelLogsEndpoint,
    /// Enable logs ingestion via the Vercel Log Drain endpoint.
    ///
    /// Serialized as `organizations:relay-vercel-log-drain-endpoint`.
    #[serde(rename = "organizations:relay-vercel-log-drain-endpoint")]
    VercelLogDrainEndpoint,
    /// Enable playstation crash dump ingestion via the `/playstation/` endpoint.
    ///
    /// Serialized as `organizations:relay-playstation-ingestion`.
    #[serde(rename = "organizations:relay-playstation-ingestion")]
    PlaystationIngestion,
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
    /// Enabled for beta orgs
    ///
    /// Serialized as `organizations:continuous-profiling-beta`.
    #[serde(rename = "organizations:continuous-profiling-beta")]
    ContinuousProfilingBeta,
    /// Enabled when only beta orgs are allowed to send continuous profiles.
    ///
    /// Serialized as `organizations:continuous-profiling-beta-ingest`.
    #[serde(rename = "organizations:continuous-profiling-beta-ingest")]
    ContinuousProfilingBetaIngest,
    /// When enabled, spans will be extracted from a transaction.
    ///
    /// Serialized as `organizations:indexed-spans-extraction`.
    #[serde(rename = "organizations:indexed-spans-extraction")]
    ExtractSpansFromEvent,
    /// Enable log ingestion for our log product (this is not internal logging).
    ///
    /// Serialized as `organizations:ourlogs-ingestion`.
    #[serde(rename = "organizations:ourlogs-ingestion")]
    OurLogsIngestion,
    /// Enable trace metric ingestion for our trace metric product.
    ///
    /// Serialized as `organizations:tracemetrics-ingestion`.
    #[serde(rename = "organizations:tracemetrics-ingestion")]
    TraceMetricsIngestion,
    /// This feature has graduated ant is hard-coded for external Relays.
    #[doc(hidden)]
    #[serde(rename = "projects:profiling-ingest-unsampled-profiles")]
    IngestUnsampledProfiles,
    /// This feature has graduated and is hard-coded for external Relays.
    #[doc(hidden)]
    #[serde(rename = "organizations:user-feedback-ingest")]
    UserReportV2Ingest,
    /// This feature has graduated and is hard-coded for external Relays.
    #[doc(hidden)]
    #[serde(rename = "organizations:performance-queries-mongodb-extraction")]
    ScrubMongoDbDescriptions,
    #[doc(hidden)]
    #[serde(rename = "organizations:view-hierarchy-scrubbing")]
    ViewHierarchyScrubbing,
    /// Detect performance issues in the new standalone spans pipeline instead of on transactions.
    #[serde(rename = "organizations:performance-issues-spans")]
    PerformanceIssuesSpans,
    /// Enables the experimental Span V2 processing pipeline in Relay.
    #[serde(rename = "projects:span-v2-experimental-processing")]
    SpanV2ExperimentalProcessing,
    /// This feature has deprecated and is kept for external Relays.
    #[doc(hidden)]
    #[serde(rename = "projects:span-metrics-extraction")]
    DeprecatedExtractCommonSpanMetricsFromEvent,
    /// This feature has been deprecated and is kept for external Relays.
    #[doc(hidden)]
    #[serde(rename = "projects:span-metrics-extraction-addons")]
    DeprecatedExtractAddonsSpanMetricsFromEvent,
    /// Forward compatibility.
    #[doc(hidden)]
    #[serde(other)]
    Unknown,
}

/// A set of [`Feature`]s.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
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

    /// Returns `true` if any spans are produced for this project.
    pub fn produces_spans(&self) -> bool {
        self.has(Feature::ExtractSpansFromEvent) || self.has(Feature::StandaloneSpanIngestion)
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
