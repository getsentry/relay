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
    /// Enables combining session replay envelope items (Replay Recordings and Replay Events).
    /// into one Kafka message.
    ///
    /// Serialized as `organizations:session-replay-combined-envelope-items`.
    #[serde(rename = "organizations:session-replay-combined-envelope-items")]
    SessionReplayCombinedEnvelopeItems,
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
    /// Serialized as `projects:relay-otel-endpoint`.
    #[serde(rename = "projects:relay-otel-endpoint")]
    OtelEndpoint,
    /// Enable playstation crash dump ingestion via the `/playstation/` endpoint.
    ///
    /// Serialized as `project:relay-playstation-ingestion`.
    #[serde(rename = "projects:relay-playstation-ingestion")]
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
    /// Enables metric extraction from spans for common modules.
    ///
    /// Serialized as `projects:span-metrics-extraction`.
    #[serde(rename = "projects:span-metrics-extraction")]
    ExtractCommonSpanMetricsFromEvent,

    /// Enables metric extraction from spans for addon modules.
    ///
    /// Serialized as `projects:span-metrics-extraction-addons`.
    #[serde(rename = "projects:span-metrics-extraction-addons")]
    ExtractAddonsSpanMetricsFromEvent,
    /// When enabled, spans will be extracted from a transaction.
    ///
    /// Serialized as `organizations:indexed-spans-extraction`.
    #[serde(rename = "organizations:indexed-spans-extraction")]
    ExtractSpansFromEvent,
    /// Indicate if the EAP consumers should ingest a span for a given organization.
    ///
    /// Serialized as `organizations:ingest-spans-in-eap`
    #[serde(rename = "organizations:ingest-spans-in-eap")]
    IngestSpansInEapForOrganization,
    /// Indicate if the EAP consumers should ingest a span for a given project.
    ///
    /// Serialized as `projects:ingest-spans-in-eap`
    #[serde(rename = "projects:ingest-spans-in-eap")]
    IngestSpansInEapForProject,
    /// Enable log ingestion for our log product (this is not internal logging).
    ///
    /// Serialized as `organizations:ourlogs-ingestion`.
    #[serde(rename = "organizations:ourlogs-ingestion")]
    OurLogsIngestion,
    /// Controls log ingestion on a per project basis, only used to disable log ingestion for infrastructure concerns.
    /// Set to true by default, use the organization feature flag to control the default behavior.
    ///
    /// Serialized as `projects:ourlogs-ingestion`.
    #[serde(rename = "projects:ourlogs-ingestion")]
    OurLogsIngestionForProject,
    /// This feature has graduated and is hard-coded for external Relays.
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
        self.has(Feature::ExtractSpansFromEvent)
            || self.has(Feature::StandaloneSpanIngestion)
            || self.has(Feature::ExtractCommonSpanMetricsFromEvent)
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
