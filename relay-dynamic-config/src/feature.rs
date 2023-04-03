use serde::{Deserialize, Serialize};

/// Features exposed by project config.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Feature {
    /// Enables ingestion and normalization of profiles.
    #[serde(rename = "organizations:profiling")]
    Profiling,
    /// Enables ingestion of Session Replays (Replay Recordings and Replay Events).
    #[serde(rename = "organizations:session-replay")]
    SessionReplay,
    /// Enables data scrubbing of replay recording payloads.
    #[serde(rename = "organizations:session-replay-recording-scrubbing")]
    SessionReplayRecordingScrubbing,
    /// Enables transaction names normalization.
    ///
    /// Replacing UUIDs, SHAs and numerical IDs by placeholders.
    #[serde(rename = "organizations:transaction-name-normalize")]
    TransactionNameNormalize,
    /// True if transaction names scrubbed by regex patterns should be marked as "sanitized".
    ///
    /// Transaction names modified by clusterer rules are always marked as such.
    #[serde(rename = "organizations:transaction-name-mark-scrubbed-as-sanitized")]
    TransactionNameMarkScrubbedAsSanitized,
    /// Enables device.class synthesis
    ///
    /// Enables device.class tag synthesis on mobile events.
    #[serde(rename = "organizations:device-class-synthesis")]
    DeviceClassSynthesis,
    /// Unused.
    ///
    /// This used to control the initial experimental metrics extraction for sessions and has been
    /// discontinued.
    #[serde(rename = "organizations:metrics-extraction")]
    Deprecated1,

    /// Forward compatibility.
    #[serde(other)]
    Unknown,
}
