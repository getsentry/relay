//! Envelope item wire-tag names and conversions.

use std::fmt;

use relay_event_schema::protocol::EventType;

use super::ItemType;

impl ItemType {
    /// Returns the event item type corresponding to the given `EventType`.
    pub fn from_event_type(event_type: EventType) -> Self {
        match event_type {
            EventType::Default | EventType::Error => ItemType::Event,
            EventType::Transaction => ItemType::Transaction,
            EventType::UserReportV2 => ItemType::UserReportV2,
            EventType::Csp => ItemType::Security,
        }
    }

    /// Returns the variant name of the item type.
    ///
    /// Unlike [`Self::as_str`] this returns an unknown value as `unknown`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Transaction => "transaction",
            Self::Security => "security",
            Self::Attachment => "attachment",
            Self::FormData => "form_data",
            Self::RawSecurity => "raw_security",
            Self::UnrealReport => "unreal_report",
            Self::UserReport => "user_report",
            Self::UserReportV2 => "feedback",
            Self::Session => "session",
            Self::Sessions => "sessions",
            Self::MetricBuckets => "metric_buckets",
            Self::ClientReport => "client_report",
            Self::Profile => "profile",
            Self::ReplayEvent => "replay_event",
            Self::ReplayRecording => "replay_recording",
            Self::ReplayVideo => "replay_video",
            Self::CheckIn => "check_in",
            Self::Log => "log",
            Self::TraceMetric => "trace_metric",
            Self::Span => "span",
            Self::ProfileChunk => "profile_chunk",
            Self::Integration => "integration",
            Self::Unknown(_) => "unknown",
        }
    }

    /// Returns the item type as a string.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown(s) => s,
            _ => self.name(),
        }
    }

    /// Returns `true` if the item is a metric type.
    pub fn is_metrics(&self) -> bool {
        matches!(self, ItemType::MetricBuckets)
    }

    /// Returns `true` if the item is a Relay internal item type.
    ///
    /// Internal items are not allowed to be forwarded or sent upstream and will be rejected by
    /// other Relays. Internal items must be converted to Sentry native items before forwarded.
    pub fn is_internal(&self) -> bool {
        matches!(self, ItemType::Integration)
    }
}

impl fmt::Display for ItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for ItemType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "event" => Self::Event,
            "transaction" => Self::Transaction,
            "security" => Self::Security,
            "attachment" => Self::Attachment,
            "form_data" => Self::FormData,
            "raw_security" => Self::RawSecurity,
            "unreal_report" => Self::UnrealReport,
            "user_report" => Self::UserReport,
            "feedback" => Self::UserReportV2,
            "session" => Self::Session,
            "sessions" => Self::Sessions,
            "metric_buckets" => Self::MetricBuckets,
            "client_report" => Self::ClientReport,
            "profile" => Self::Profile,
            "replay_event" => Self::ReplayEvent,
            "replay_recording" => Self::ReplayRecording,
            "replay_video" => Self::ReplayVideo,
            "check_in" => Self::CheckIn,
            "log" => Self::Log,
            "trace_metric" => Self::TraceMetric,
            "span" => Self::Span,
            "profile_chunk" => Self::ProfileChunk,
            // "profile_chunk_ui" is to be treated as an alias for `ProfileChunk`
            // because Android 8.10.0 and 8.11.0 is sending it as the item type.
            "profile_chunk_ui" => Self::ProfileChunk,
            "integration" => Self::Integration,
            other => Self::Unknown(other.to_owned()),
        })
    }
}

relay_common::impl_str_serde!(ItemType, "an envelope item type (see sentry develop docs)");
