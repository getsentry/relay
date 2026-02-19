//! Defines the [`DataCategory`] type that classifies data Relay can handle.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::events::EventType;

/// An error that occurs if a number cannot be converted into a [`DataCategory`].
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("Unknown numeric data category {0} can not be converted into a DataCategory.")]
pub struct UnknownDataCategory(pub u8);

/// Classifies the type of data that is being ingested.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum DataCategory {
    /// Reserved and unused.
    ///
    /// SDK rate limiting behavior: ignore.
    Default = 0,
    /// Error events and Events with an `event_type` not explicitly listed below.
    ///
    /// SDK rate limiting behavior: apply to the entire envelope if it contains an item type `event`.
    Error = 1,
    /// Transaction events.
    ///
    /// SDK rate limiting behavior: apply to the entire envelope if it contains an item `transaction`.
    Transaction = 2,
    /// Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
    ///
    /// SDK rate limiting behavior: ignore.
    Security = 3,
    /// An attachment. Quantity is the size of the attachment in bytes.
    ///
    /// SDK rate limiting behavior: apply to all attachments.
    Attachment = 4,
    /// Session updates. Quantity is the number of updates in the batch.
    ///
    /// SDK rate limiting behavior: apply to all sessions and session aggregates.
    Session = 5,
    /// Profile
    ///
    /// This is the category for processed profiles (all profiles, whether or not we store them).
    ///
    /// SDK rate limiting behavior: apply to all profiles.
    Profile = 6,
    /// Session Replays
    ///
    /// SDK rate limiting behavior: apply to all Session Replay data.
    Replay = 7,
    /// DEPRECATED: A transaction for which metrics were extracted.
    ///
    /// This category is now obsolete because the `Transaction` variant will represent
    /// processed transactions from now on.
    ///
    /// SDK rate limiting behavior: ignore.
    TransactionProcessed = 8,
    /// Indexed transaction events.
    ///
    /// This is the category for transaction payloads that were accepted and stored in full. In
    /// contrast, `transaction` only guarantees that metrics have been accepted for the transaction.
    ///
    /// SDK rate limiting behavior: ignore.
    TransactionIndexed = 9,
    /// Monitor check-ins.
    ///
    /// SDK rate limiting behavior: apply to items of type `check_in`.
    Monitor = 10,
    /// Indexed Profile
    ///
    /// This is the category for indexed profiles that will be stored later.
    ///
    /// SDK rate limiting behavior: ignore.
    ProfileIndexed = 11,
    /// Span
    ///
    /// This is the category for spans from which we extracted metrics from.
    ///
    /// SDK rate limiting behavior: apply to spans that are not sent in a transaction.
    Span = 12,
    /// Monitor Seat
    ///
    /// Represents a monitor job that has scheduled monitor checkins. The seats are not ingested
    /// but we define it here to prevent clashing values since this data category enumeration
    /// is also used outside of Relay via the Python package.
    ///
    /// SDK rate limiting behavior: ignore.
    MonitorSeat = 13,
    /// User Feedback
    ///
    /// Represents a User Feedback processed.
    /// Currently standardized on name UserReportV2 to avoid clashing with the old UserReport.
    /// TODO(jferg): Rename this to UserFeedback once old UserReport is deprecated.
    ///
    /// SDK rate limiting behavior: apply to items of type 'feedback'.
    UserReportV2 = 14,
    /// Metric buckets.
    ///
    /// SDK rate limiting behavior: apply to `statsd` and `metrics` items.
    MetricBucket = 15,
    /// SpanIndexed
    ///
    /// This is the category for spans we store in full.
    ///
    /// SDK rate limiting behavior: ignore.
    SpanIndexed = 16,
    /// ProfileDuration
    ///
    /// This data category is used to count the number of milliseconds per indexed profile chunk,
    /// excluding UI profile chunks.
    ///
    /// SDK rate limiting behavior: apply to profile chunks.
    ProfileDuration = 17,
    /// ProfileChunk
    ///
    /// This is a count of profile chunks received. It will not be used for billing but will be
    /// useful for customers to track what's being dropped.
    ///
    /// SDK rate limiting behavior: apply to profile chunks.
    ProfileChunk = 18,
    /// MetricSecond
    ///
    /// Reserved by billing to summarize the bucketed product of metric volume
    /// and metric cardinality. Defined here so as not to clash with future
    /// categories.
    ///
    /// SDK rate limiting behavior: ignore.
    MetricSecond = 19,
    /// Replay Video
    ///
    /// This is the data category for Session Replays produced via a video recording.
    ///
    /// SDK rate limiting behavior: ignore.
    DoNotUseReplayVideo = 20,
    /// This is the data category for Uptime monitors.
    ///
    /// SDK rate limiting behavior: ignore.
    Uptime = 21,
    /// Counts the number of individual attachments, as opposed to the number of bytes in an attachment.
    ///
    /// SDK rate limiting behavior: apply to attachments.
    AttachmentItem = 22,
    /// LogItem
    ///
    /// This is the category for logs for which we store the count log events for users for measuring
    /// missing breadcrumbs, and count of logs for rate limiting purposes.
    ///
    /// SDK rate limiting behavior: apply to logs.
    LogItem = 23,
    /// LogByte
    ///
    /// This is the category for logs for which we store log event total bytes for users.
    ///
    /// SDK rate limiting behavior: apply to logs.
    LogByte = 24,
    /// Profile duration of a UI profile.
    ///
    /// This data category is used to count the number of milliseconds per indexed UI profile
    /// chunk.
    ///
    /// See also: [`Self::ProfileDuration`]
    ///
    /// SDK rate limiting behavior: apply to profile chunks.
    ProfileDurationUi = 25,
    /// UI Profile Chunk.
    ///
    /// This data category is used to count the number of milliseconds per indexed UI profile
    /// chunk.
    ///
    /// See also: [`Self::ProfileChunk`]
    ///
    /// SDK rate limiting behavior: apply to profile chunks.
    ProfileChunkUi = 26,
    /// This is the data category to count Seer Autofix run events.
    ///
    /// SDK rate limiting behavior: ignore.
    SeerAutofix = 27,
    /// This is the data category to count Seer Scanner run events.
    ///
    /// SDK rate limiting behavior: ignore.
    SeerScanner = 28,
    /// DEPRECATED: Use SeerUser instead.
    ///
    /// PreventUser
    ///
    /// This is the data category to count the number of assigned Prevent Users.
    ///
    /// SDK rate limiting behavior: ignore.
    PreventUser = 29,
    /// PreventReview
    ///
    /// This is the data category to count the number of Prevent review events.
    ///
    /// SDK rate limiting behavior: ignore.
    PreventReview = 30,
    /// Size analysis
    ///
    /// This is the data category to count the number of size analyses performed.
    /// 'Size analysis' a static binary analysis of a preprod build artifact
    /// (e.g. the .apk of an Android app or MacOS .app).
    /// When enabled there will typically be one such analysis per uploaded artifact.
    ///
    /// SDK rate limiting behavior: ignore.
    SizeAnalysis = 31,
    /// InstallableBuild
    ///
    /// This is the data category to count the number of installable builds.
    /// It counts the number of artifacts uploaded *not* the number of times the
    /// artifacts are downloaded for installation.
    /// When enabled there will typically be one 'InstallableBuild' per uploaded artifact.
    ///
    /// SDK rate limiting behavior: ignore.
    InstallableBuild = 32,
    /// TraceMetric
    ///
    /// This is the data category to count the number of trace metric items.
    TraceMetric = 33,
    /// SeerUser
    ///
    /// This is the data category to count the number of Seer users.
    ///
    /// SDK rate limiting behavior: ignore.
    SeerUser = 34,
    /// Transaction profiles for backend platforms.
    ///
    /// This is an extension of [`Self::Profile`], but additional discriminates on the profile
    /// platform, see also [`Self::ProfileUi`].
    ///
    /// Continuous profiling uses [`Self::ProfileChunk`] and [`Self::ProfileChunkUi`].
    ///
    /// SDK rate limiting behavior: optional, apply to transaction profiles on "backend platforms'.
    ProfileBackend = 35,
    /// Transaction profiles for ui platforms.
    ///
    /// This is an extension of [`Self::Profile`], but additional discriminates on the profile
    /// platform, see also [`Self::ProfileBackend`].
    ///
    /// Continuous profiling uses [`Self::ProfileChunk`] and [`Self::ProfileChunkUi`].
    ///
    /// SDK rate limiting behavior: optional, apply to transaction profiles on "ui platforms'.
    ProfileUi = 36,
    //
    // IMPORTANT: After adding a new entry to DataCategory, go to the `relay-cabi` subfolder and run
    // `make header` to regenerate the C-binding. This allows using the data category from Python.
    // Rerun this step every time the **code name** of the variant is updated.
    //
    /// Any other data category not known by this Relay.
    Unknown = -1,
}

impl DataCategory {
    /// Returns the data category corresponding to the given name.
    pub fn from_name(string: &str) -> Self {
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
            "profile_duration_ui" => Self::ProfileDurationUi,
            "profile_chunk" => Self::ProfileChunk,
            "profile_chunk_ui" => Self::ProfileChunkUi,
            "metric_second" => Self::MetricSecond,
            "replay_video" => Self::DoNotUseReplayVideo,
            "uptime" => Self::Uptime,
            "attachment_item" => Self::AttachmentItem,
            "seer_autofix" => Self::SeerAutofix,
            "seer_scanner" => Self::SeerScanner,
            "prevent_user" => Self::PreventUser,
            "prevent_review" => Self::PreventReview,
            "size_analysis" => Self::SizeAnalysis,
            "installable_build" => Self::InstallableBuild,
            "trace_metric" => Self::TraceMetric,
            "seer_user" => Self::SeerUser,
            "profile_backend" => Self::ProfileBackend,
            "profile_ui" => Self::ProfileUi,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this data category.
    pub fn name(self) -> &'static str {
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
            Self::DoNotUseReplayVideo => "replay_video",
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
            Self::ProfileDurationUi => "profile_duration_ui",
            Self::ProfileChunk => "profile_chunk",
            Self::ProfileChunkUi => "profile_chunk_ui",
            Self::MetricSecond => "metric_second",
            Self::Uptime => "uptime",
            Self::AttachmentItem => "attachment_item",
            Self::SeerAutofix => "seer_autofix",
            Self::SeerScanner => "seer_scanner",
            Self::PreventUser => "prevent_user",
            Self::PreventReview => "prevent_review",
            Self::SizeAnalysis => "size_analysis",
            Self::InstallableBuild => "installable_build",
            Self::TraceMetric => "trace_metric",
            Self::SeerUser => "seer_user",
            Self::ProfileBackend => "profile_backend",
            Self::ProfileUi => "profile_ui",
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

relay_common::impl_str_serde!(DataCategory, "a data category");

impl fmt::Display for DataCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for DataCategory {
    type Err = std::convert::Infallible;

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

impl TryFrom<u8> for DataCategory {
    type Error = UnknownDataCategory;

    fn try_from(value: u8) -> Result<Self, UnknownDataCategory> {
        match value {
            0 => Ok(Self::Default),
            1 => Ok(Self::Error),
            2 => Ok(Self::Transaction),
            3 => Ok(Self::Security),
            4 => Ok(Self::Attachment),
            5 => Ok(Self::Session),
            6 => Ok(Self::Profile),
            7 => Ok(Self::Replay),
            8 => Ok(Self::TransactionProcessed),
            9 => Ok(Self::TransactionIndexed),
            10 => Ok(Self::Monitor),
            11 => Ok(Self::ProfileIndexed),
            12 => Ok(Self::Span),
            13 => Ok(Self::MonitorSeat),
            14 => Ok(Self::UserReportV2),
            15 => Ok(Self::MetricBucket),
            16 => Ok(Self::SpanIndexed),
            17 => Ok(Self::ProfileDuration),
            18 => Ok(Self::ProfileChunk),
            19 => Ok(Self::MetricSecond),
            20 => Ok(Self::DoNotUseReplayVideo),
            21 => Ok(Self::Uptime),
            22 => Ok(Self::AttachmentItem),
            23 => Ok(Self::LogItem),
            24 => Ok(Self::LogByte),
            25 => Ok(Self::ProfileDurationUi),
            26 => Ok(Self::ProfileChunkUi),
            27 => Ok(Self::SeerAutofix),
            28 => Ok(Self::SeerScanner),
            29 => Ok(Self::PreventUser),
            30 => Ok(Self::PreventReview),
            31 => Ok(Self::SizeAnalysis),
            32 => Ok(Self::InstallableBuild),
            33 => Ok(Self::TraceMetric),
            34 => Ok(Self::SeerUser),
            35 => Ok(Self::ProfileBackend),
            36 => Ok(Self::ProfileUi),
            other => Err(UnknownDataCategory(other)),
        }
    }
}

/// The unit in which a data category is measured.
///
/// This enum specifies how quantities for different data categories are measured,
/// which affects how quota limits are interpreted and enforced.
///
/// Note: There is no `Unknown` variant. For categories without a defined unit
/// (e.g., `DataCategory::Unknown`), methods return `Option::None`.
//
// IMPORTANT: After adding a new entry to CategoryUnit, go to the `relay-cabi` subfolder and run
// `make header` to regenerate the C-binding. This allows using the category unit from Python.
//
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i8)]
pub enum CategoryUnit {
    /// Counts the number of discrete items.
    Count = 0,
    /// Counts the number of bytes across items.
    Bytes = 1,
    /// Counts the accumulated time in milliseconds across items.
    Milliseconds = 2,
}

impl CategoryUnit {
    /// Returns the canonical name of this category unit.
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Bytes => "bytes",
            Self::Milliseconds => "milliseconds",
        }
    }

    /// Returns the category unit corresponding to the given name string.
    ///
    /// Returns `None` if the string doesn't match any known unit.
    pub fn from_name(string: &str) -> Option<Self> {
        match string {
            "count" => Some(Self::Count),
            "bytes" => Some(Self::Bytes),
            "milliseconds" => Some(Self::Milliseconds),
            _ => None,
        }
    }

    /// Returns the `CategoryUnit` for the given `DataCategory`.
    ///
    /// Returns `None` for `DataCategory::Unknown`.
    ///
    /// Note: Takes a reference to avoid unnecessary copying and allow direct use with iterators.
    pub fn from_category(category: &DataCategory) -> Option<Self> {
        match category {
            DataCategory::Default
            | DataCategory::Error
            | DataCategory::Transaction
            | DataCategory::Replay
            | DataCategory::DoNotUseReplayVideo
            | DataCategory::Security
            | DataCategory::Profile
            | DataCategory::ProfileIndexed
            | DataCategory::TransactionProcessed
            | DataCategory::TransactionIndexed
            | DataCategory::LogItem
            | DataCategory::Span
            | DataCategory::SpanIndexed
            | DataCategory::MonitorSeat
            | DataCategory::Monitor
            | DataCategory::MetricBucket
            | DataCategory::UserReportV2
            | DataCategory::ProfileChunk
            | DataCategory::ProfileChunkUi
            | DataCategory::Uptime
            | DataCategory::MetricSecond
            | DataCategory::AttachmentItem
            | DataCategory::SeerAutofix
            | DataCategory::SeerScanner
            | DataCategory::PreventUser
            | DataCategory::PreventReview
            | DataCategory::Session
            | DataCategory::SizeAnalysis
            | DataCategory::InstallableBuild
            | DataCategory::TraceMetric
            | DataCategory::SeerUser
            | DataCategory::ProfileBackend
            | DataCategory::ProfileUi => Some(Self::Count),

            DataCategory::Attachment | DataCategory::LogByte => Some(Self::Bytes),

            DataCategory::ProfileDuration | DataCategory::ProfileDurationUi => {
                Some(Self::Milliseconds)
            }

            DataCategory::Unknown => None,
        }
    }
}

impl fmt::Display for CategoryUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for CategoryUnit {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Self::from_name(string).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_last_variant_conversion() {
        // If this test fails, update the numeric bounds so that the first assertion
        // maps to the last variant in the enum and the second assertion produces an error
        // that the DataCategory does not exist.
        assert_eq!(DataCategory::try_from(36), Ok(DataCategory::ProfileUi));
        assert_eq!(DataCategory::try_from(37), Err(UnknownDataCategory(37)));
    }

    #[test]
    fn test_data_category_alias() {
        assert_eq!("feedback".parse(), Ok(DataCategory::UserReportV2));
        assert_eq!("user_report_v2".parse(), Ok(DataCategory::UserReportV2));
        assert_eq!(&DataCategory::UserReportV2.to_string(), "feedback");

        assert_eq!(
            serde_json::from_str::<DataCategory>(r#""feedback""#).unwrap(),
            DataCategory::UserReportV2,
        );
        assert_eq!(
            serde_json::from_str::<DataCategory>(r#""user_report_v2""#).unwrap(),
            DataCategory::UserReportV2,
        );
        assert_eq!(
            &serde_json::to_string(&DataCategory::UserReportV2).unwrap(),
            r#""feedback""#
        )
    }

    #[test]
    fn test_category_unit_name() {
        assert_eq!(CategoryUnit::Count.name(), "count");
        assert_eq!(CategoryUnit::Bytes.name(), "bytes");
        assert_eq!(CategoryUnit::Milliseconds.name(), "milliseconds");
    }

    #[test]
    fn test_category_unit_from_name() {
        assert_eq!(CategoryUnit::from_name("count"), Some(CategoryUnit::Count));
        assert_eq!(CategoryUnit::from_name("bytes"), Some(CategoryUnit::Bytes));
        assert_eq!(
            CategoryUnit::from_name("milliseconds"),
            Some(CategoryUnit::Milliseconds)
        );
        assert_eq!(CategoryUnit::from_name("unknown"), None);
        assert_eq!(CategoryUnit::from_name(""), None);
    }

    #[test]
    fn test_category_unit_from_category() {
        // Count categories
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::Error),
            Some(CategoryUnit::Count)
        );
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::Transaction),
            Some(CategoryUnit::Count)
        );
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::Span),
            Some(CategoryUnit::Count)
        );

        // Bytes categories
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::Attachment),
            Some(CategoryUnit::Bytes)
        );
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::LogByte),
            Some(CategoryUnit::Bytes)
        );

        // Milliseconds categories
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::ProfileDuration),
            Some(CategoryUnit::Milliseconds)
        );
        assert_eq!(
            CategoryUnit::from_category(&DataCategory::ProfileDurationUi),
            Some(CategoryUnit::Milliseconds)
        );

        // Unknown returns None
        assert_eq!(CategoryUnit::from_category(&DataCategory::Unknown), None);
    }

    #[test]
    fn test_category_unit_display() {
        assert_eq!(format!("{}", CategoryUnit::Count), "count");
        assert_eq!(format!("{}", CategoryUnit::Bytes), "bytes");
        assert_eq!(format!("{}", CategoryUnit::Milliseconds), "milliseconds");
    }

    #[test]
    fn test_category_unit_from_str() {
        assert_eq!("count".parse::<CategoryUnit>(), Ok(CategoryUnit::Count));
        assert_eq!("bytes".parse::<CategoryUnit>(), Ok(CategoryUnit::Bytes));
        assert_eq!(
            "milliseconds".parse::<CategoryUnit>(),
            Ok(CategoryUnit::Milliseconds)
        );
        assert!("invalid".parse::<CategoryUnit>().is_err());
    }

    #[test]
    fn test_category_unit_repr_values() {
        // Verify the repr(i8) values are correct for FFI
        assert_eq!(CategoryUnit::Count as i8, 0);
        assert_eq!(CategoryUnit::Bytes as i8, 1);
        assert_eq!(CategoryUnit::Milliseconds as i8, 2);
    }
}
