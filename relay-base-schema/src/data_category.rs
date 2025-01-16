//! Defines the [`DataCategory`] type that classifies data Relay can handle.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::events::EventType;

/// Classifies the type of data that is being ingested.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(i8)]
pub enum DataCategory {
    /// Reserved and unused.
    Default = 0,
    /// Error events and Events with an `event_type` not explicitly listed below.
    Error = 1,
    /// Transaction events.
    Transaction = 2,
    /// Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
    Security = 3,
    /// An attachment. Quantity is the size of the attachment in bytes.
    Attachment = 4,
    /// Session updates. Quantity is the number of updates in the batch.
    Session = 5,
    /// Profile
    ///
    /// This is the category for processed profiles (all profiles, whether or not we store them).
    Profile = 6,
    /// Session Replays
    Replay = 7,
    /// DEPRECATED: A transaction for which metrics were extracted.
    ///
    /// This category is now obsolete because the `Transaction` variant will represent
    /// processed transactions from now on.
    TransactionProcessed = 8,
    /// Indexed transaction events.
    ///
    /// This is the category for transaction payloads that were accepted and stored in full. In
    /// contrast, `transaction` only guarantees that metrics have been accepted for the transaction.
    TransactionIndexed = 9,
    /// Monitor check-ins.
    Monitor = 10,
    /// Indexed Profile
    ///
    /// This is the category for indexed profiles that will be stored later.
    ProfileIndexed = 11,
    /// Span
    ///
    /// This is the category for spans from which we extracted metrics from.
    Span = 12,
    /// Monitor Seat
    ///
    /// Represents a monitor job that has scheduled monitor checkins. The seats are not ingested
    /// but we define it here to prevent clashing values since this data category enumeration
    /// is also used outside of Relay via the Python package.
    MonitorSeat = 13,
    /// User Feedback
    ///
    /// Represents a User Feedback processed.
    /// Currently standardized on name UserReportV2 to avoid clashing with the old UserReport.
    /// TODO(jferg): Rename this to UserFeedback once old UserReport is deprecated.
    UserReportV2 = 14,
    /// Metric buckets.
    MetricBucket = 15,
    /// SpanIndexed
    ///
    /// This is the category for spans we store in full.
    SpanIndexed = 16,
    /// ProfileDuration
    ///
    /// This data category is used to count the number of milliseconds we have per indexed profile chunk.
    /// We will then bill per second.
    ProfileDuration = 17,
    /// ProfileChunk
    ///
    /// This is a count of profile chunks received. It will not be used for billing but will be
    /// useful for customers to track what's being dropped.
    ProfileChunk = 18,
    /// MetricSecond
    ///
    /// Reserved by billing to summarize the bucketed product of metric volume
    /// and metric cardinality. Defined here so as not to clash with future
    /// categories.
    MetricSecond = 19,
    /// Replay Video
    ///
    /// This is the data category for Session Replays produced via a video recording.
    ReplayVideo = 20,
    /// This is the data category for Uptime monitors.
    Uptime = 21,
    /// Counts the number of individual attachments, as opposed to the number of bytes in an attachment.
    AttachmentItem = 22,
    /// LogItem
    ///
    /// This is the category for logs for which we store the count log events for users for measuring
    /// missing breadcrumbs, and count of logs for rate limiting purposes.
    LogItem = 23,
    /// LogByte
    ///
    /// This is the category for logs for which we store log event total bytes for users.
    LogByte = 24,
    //
    // IMPORTANT: After adding a new entry to DataCategory, go to the `relay-cabi` subfolder and run
    // `make header` to regenerate the C-binding. This allows using the data category from Python.
    // Rerun this step every time the **code name** of the variant is updated.
    //
    /// Any other data category not known by this Relay.
    #[serde(other)]
    Unknown = -1,
}

impl DataCategory {
    /// Returns the data category corresponding to the given name.
    pub fn from_name(string: &str) -> Self {
        // TODO: This should probably use serde.
        match string {
            "default" => Self::Default,
            "error" => Self::Error,
            "transaction" => Self::Transaction,
            "security" => Self::Security,
            "attachment" => Self::Attachment,
            "session" => Self::Session,
            "profile" => Self::Profile,
            "profile_indexed" => Self::ProfileIndexed,
            "replay" => Self::Replay,
            "transaction_processed" => Self::TransactionProcessed,
            "transaction_indexed" => Self::TransactionIndexed,
            "monitor" => Self::Monitor,
            "span" => Self::Span,
            "log_item" => Self::LogItem,
            "log_byte" => Self::LogByte,
            "monitor_seat" => Self::MonitorSeat,
            "feedback" => Self::UserReportV2,
            "user_report_v2" => Self::UserReportV2,
            "metric_bucket" => Self::MetricBucket,
            "span_indexed" => Self::SpanIndexed,
            "profile_duration" => Self::ProfileDuration,
            "profile_chunk" => Self::ProfileChunk,
            "metric_second" => Self::MetricSecond,
            "replay_video" => Self::ReplayVideo,
            "uptime" => Self::Uptime,
            "attachment_item" => Self::AttachmentItem,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this data category.
    pub fn name(self) -> &'static str {
        // TODO: This should probably use serde.
        match self {
            Self::Default => "default",
            Self::Error => "error",
            Self::Transaction => "transaction",
            Self::Security => "security",
            Self::Attachment => "attachment",
            Self::Session => "session",
            Self::Profile => "profile",
            Self::ProfileIndexed => "profile_indexed",
            Self::Replay => "replay",
            Self::TransactionProcessed => "transaction_processed",
            Self::TransactionIndexed => "transaction_indexed",
            Self::Monitor => "monitor",
            Self::Span => "span",
            Self::LogItem => "log_item",
            Self::LogByte => "log_byte",
            Self::MonitorSeat => "monitor_seat",
            Self::UserReportV2 => "feedback",
            Self::MetricBucket => "metric_bucket",
            Self::SpanIndexed => "span_indexed",
            Self::ProfileDuration => "profile_duration",
            Self::ProfileChunk => "profile_chunk",
            Self::MetricSecond => "metric_second",
            Self::ReplayVideo => "replay_video",
            Self::Uptime => "uptime",
            Self::AttachmentItem => "attachment_item",
            Self::Unknown => "unknown",
        }
    }

    /// Returns true if the DataCategory refers to an error (i.e an error event).
    pub fn is_error(self) -> bool {
        matches!(self, Self::Error | Self::Default | Self::Security)
    }

    /// Returns the numeric value for this outcome.
    pub fn value(self) -> Option<u8> {
        // negative values (Internal and Unknown) cannot be sent as
        // outcomes (internally so!)
        (self as i8).try_into().ok()
    }

    /// Returns a dedicated category for indexing if this data can be converted to metrics.
    ///
    /// This returns `None` for most data categories.
    pub fn index_category(self) -> Option<Self> {
        match self {
            Self::Transaction => Some(Self::TransactionIndexed),
            Self::Span => Some(Self::SpanIndexed),
            Self::Profile => Some(Self::ProfileIndexed),
            _ => None,
        }
    }

    /// Returns `true` if this data category is an indexed data category.
    pub fn is_indexed(self) -> bool {
        matches!(
            self,
            Self::TransactionIndexed | Self::SpanIndexed | Self::ProfileIndexed
        )
    }
}

impl fmt::Display for DataCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for DataCategory {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_name(string))
    }
}

impl From<EventType> for DataCategory {
    fn from(ty: EventType) -> Self {
        match ty {
            EventType::Default | EventType::Error | EventType::Nel => Self::Error,
            EventType::Transaction => Self::Transaction,
            EventType::Csp | EventType::Hpkp | EventType::ExpectCt | EventType::ExpectStaple => {
                Self::Security
            }
            EventType::UserReportV2 => Self::UserReportV2,
        }
    }
}
