use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// Features exposed by project config.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum Feature {
    /// Enables ingestion and normalization of profiles.
    Profiling,
    /// Enables ingestion of Session Replays (Replay Recordings and Replay Events).
    SessionReplay,
    /// Enables data scrubbing of replay recording payloads.
    SessionReplayRecordingScrubbing,
    /// Enables device.class synthesis
    ///
    /// Enables device.class tag synthesis on mobile events.
    DeviceClassSynthesis,
    /// Enables metric extraction from spans.
    SpanMetricsExtraction,
    /// Forward compatibility.
    Unknown(String),
}

impl<'de> Deserialize<'de> for Feature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let feature_name = Cow::<str>::deserialize(deserializer)?;
        Ok(match feature_name.as_ref() {
            "organizations:profiling" => Feature::Profiling,
            "organizations:session-replay" => Feature::SessionReplay,
            "organizations:session-replay-recording-scrubbing" => {
                Feature::SessionReplayRecordingScrubbing
            }
            "organizations:device-class-synthesis" => Feature::DeviceClassSynthesis,
            "projects:span-metrics-extraction" => Feature::SpanMetricsExtraction,
            _ => Feature::Unknown(feature_name.to_string()),
        })
    }
}

impl Serialize for Feature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Feature::Profiling => "organizations:profiling",
            Feature::SessionReplay => "organizations:session-replay",
            Feature::SessionReplayRecordingScrubbing => {
                "organizations:session-replay-recording-scrubbing"
            }
            Feature::DeviceClassSynthesis => "organizations:device-class-synthesis",
            Feature::SpanMetricsExtraction => "projects:span-metrics-extraction",
            Feature::Unknown(s) => s,
        })
    }
}
