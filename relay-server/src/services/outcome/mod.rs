use relay_dynamic_config::Feature;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;

use relay_filter::FilterStatKey;
use relay_quotas::ReasonCode;
use relay_sampling::config::RuleId;
use relay_sampling::evaluation::MatchedRuleIds;

mod aggregator;
pub mod metric;
mod service;

use crate::envelope::{AttachmentType, ItemType};

pub use self::aggregator::OutcomeAggregator;
pub use self::service::*;

/// The numerical identifier of the outcome category (Accepted, Filtered, ...)
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub struct OutcomeId(u8);

impl OutcomeId {
    // This is not an enum because we still want to forward unknown outcome IDs transparently
    const ACCEPTED: OutcomeId = OutcomeId(0);
    const FILTERED: OutcomeId = OutcomeId(1);
    const RATE_LIMITED: OutcomeId = OutcomeId(2);
    const INVALID: OutcomeId = OutcomeId(3);
    const ABUSE: OutcomeId = OutcomeId(4);
    const CLIENT_DISCARD: OutcomeId = OutcomeId(5);
    #[cfg_attr(not(any(test, feature = "processing")), expect(unused))]
    const CARDINALITY_LIMITED: OutcomeId = OutcomeId(6);

    pub fn as_u8(self) -> u8 {
        self.0
    }

    /// Returns `true` for outcomes which are critical for billing.
    pub fn is_billing(self) -> bool {
        matches!(self, OutcomeId::ACCEPTED | OutcomeId::RATE_LIMITED)
    }
}

/// Defines the possible outcomes from processing an event.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// The event has been accepted and handled completely.
    ///
    /// This is only emitted for items going from Relay to Snuba directly.
    #[allow(dead_code)]
    Accepted,

    /// The event has been filtered due to a configured filter.
    Filtered(FilterStatKey),

    /// The event has been filtered by a Sampling Rule
    FilteredSampling(RuleCategories),

    /// The event has been rate limited.
    RateLimited(Option<ReasonCode>),

    /// The event has been discarded because of invalid data.
    Invalid(DiscardReason),

    /// Reserved but unused in Relay.
    #[allow(dead_code)]
    Abuse,

    /// The event has already been discarded on the client side.
    ClientDiscard(String),
}

impl Outcome {
    /// Returns the raw numeric value of this outcome for the JSON and Kafka schema.
    pub fn to_outcome_id(&self) -> OutcomeId {
        match self {
            Outcome::Filtered(_) | Outcome::FilteredSampling(_) => OutcomeId::FILTERED,
            Outcome::RateLimited(_) => OutcomeId::RATE_LIMITED,
            Outcome::Invalid(_) => OutcomeId::INVALID,
            Outcome::Abuse => OutcomeId::ABUSE,
            Outcome::ClientDiscard(_) => OutcomeId::CLIENT_DISCARD,
            Outcome::Accepted => OutcomeId::ACCEPTED,
        }
    }

    /// Returns the `reason` code field of this outcome.
    pub fn to_reason(&self) -> Option<Cow<'_, str>> {
        match self {
            Outcome::Invalid(DiscardReason::ItemTooLarge(too_large_reason)) => Some(Cow::Owned(
                format!("too_large:{}", too_large_reason.as_str()),
            )),
            Outcome::Invalid(discard_reason) => Some(Cow::Borrowed(discard_reason.name())),
            Outcome::Filtered(filter_key) => Some(filter_key.clone().name()),
            Outcome::FilteredSampling(rule_ids) => Some(Cow::Owned(format!("Sampled:{rule_ids}"))),
            Outcome::RateLimited(code_opt) => {
                code_opt.as_ref().map(|code| Cow::Borrowed(code.as_str()))
            }
            Outcome::ClientDiscard(discard_reason) => Some(Cow::Borrowed(discard_reason)),
            Outcome::Abuse => None,
            Outcome::Accepted => None,
        }
    }

    /// Returns true if there is a bug or an infrastructure problem causing event loss.
    ///
    /// This can happen when we introduce bugs or during incidents.
    ///
    /// During healthy operation, this should always return false.
    pub fn is_unexpected(&self) -> bool {
        matches!(
            self,
            Outcome::Invalid(
                DiscardReason::Internal
                    | DiscardReason::ProjectState
                    | DiscardReason::ProjectStatePii,
            )
        )
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Filtered(key) => write!(f, "filtered by {key}"),
            Outcome::FilteredSampling(rule_ids) => write!(f, "sampling rule {rule_ids}"),
            Outcome::RateLimited(None) => write!(f, "rate limited"),
            Outcome::RateLimited(Some(reason)) => write!(f, "rate limited with reason {reason}"),
            Outcome::Invalid(DiscardReason::Internal) => write!(f, "internal error"),
            Outcome::Invalid(reason) => write!(f, "invalid data ({reason})"),
            Outcome::Abuse => write!(f, "abuse limit reached"),
            Outcome::ClientDiscard(reason) => write!(f, "discarded by client ({reason})"),
            Outcome::Accepted => write!(f, "accepted"),
        }
    }
}

/// A lower-cardinality version of [`RuleId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuleCategory {
    BoostLowVolumeProjects,
    BoostEnvironments,
    IgnoreHealthChecks,
    BoostKeyTransactions,
    Recalibration,
    BoostReplayId,
    BoostLowVolumeTransactions,
    BoostLatestReleases,
    Custom,
    Other,
}

impl RuleCategory {
    fn as_str(&self) -> &'static str {
        match self {
            Self::BoostLowVolumeProjects => "1000",
            Self::BoostEnvironments => "1001",
            Self::IgnoreHealthChecks => "1002",
            Self::BoostKeyTransactions => "1003",
            Self::Recalibration => "1004",
            Self::BoostReplayId => "1005",
            Self::BoostLowVolumeTransactions => "1400",
            Self::BoostLatestReleases => "1500",
            Self::Custom => "3000",
            Self::Other => "0",
        }
    }
}

impl From<RuleId> for RuleCategory {
    fn from(value: RuleId) -> Self {
        match value.0 {
            1000 => Self::BoostLowVolumeProjects,
            1001 => Self::BoostEnvironments,
            1002 => Self::IgnoreHealthChecks,
            1003 => Self::BoostKeyTransactions,
            1004 => Self::Recalibration,
            1005 => Self::BoostReplayId,
            1400..=1499 => Self::BoostLowVolumeTransactions,
            1500..=1599 => Self::BoostLatestReleases,
            3000..=4999 => Self::Custom,
            _ => Self::Other,
        }
    }
}

/// An ordered set of categories that can be used as outcome reason.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RuleCategories(pub BTreeSet<RuleCategory>);

impl fmt::Display for RuleCategories {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, c) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", c.as_str())?;
        }
        Ok(())
    }
}

impl From<MatchedRuleIds> for RuleCategories {
    fn from(value: MatchedRuleIds) -> Self {
        RuleCategories(BTreeSet::from_iter(
            value.0.into_iter().map(RuleCategory::from),
        ))
    }
}

/// Reason for a discarded invalid event.
///
/// Used in `Outcome::Invalid`. Synchronize overlap with Sentry.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum DiscardReason {
    /// (Post Processing) An event with the same id has already been processed for this project.
    /// Sentry does not allow duplicate events and only stores the first one.
    Duplicate,

    /// (Relay) There was no valid project id in the request or the required project does not exist.
    ProjectId,

    /// (Relay) The request had an invalid event id.
    InvalidEventId,

    /// (Relay) The protocol version sent by the SDK is not supported and parts of the payload may
    /// be invalid.
    AuthVersion,

    /// (Legacy) The SDK did not send a client identifier.
    ///
    /// In Relay, this is no longer required.
    AuthClient,

    /// (Relay) The store request was missing an event payload.
    NoData,

    /// (Relay) The size limit was exceeded for the specified type.
    ItemTooLarge(DiscardItemType),

    /// (Relay) The request exceeded the endpoint's size limit.
    RequestTooLarge,

    /// (Relay) Sentry dropped data due to a quota or internal rate limit being reached.
    RateLimited,

    /// (Legacy) A store request was received with an invalid method.
    ///
    /// This outcome is no longer emitted by Relay, as HTTP method validation occurs before an event
    /// id or project id are extracted for a request.
    DisallowedMethod,

    /// (Relay) The content type for a specific endpoint was not allowed.
    ///
    /// While the standard store endpoint allows all content types, other endpoints may have
    /// stricter requirements.
    ContentType,

    /// (Legacy) The project id in the URL does not match the one specified for the public key.
    ///
    /// This outcome is no longer emitted by Relay. Instead, Relay will emit a standard `ProjectId`
    /// since it resolves the project first, and then checks for the valid project key.
    MultiProjectId,

    /// (Relay) A request without a prosperodump was made to the playstation endpoint.
    MissingProsperodump,

    /// (Relay) The prosperodump submitted to the playstation endpoint was invalid.
    InvalidProsperodump,

    /// (Relay) A minidump file was missing for the minidump endpoint.
    MissingMinidump,

    /// (Relay) The file submitted as minidump is not a valid minidump file.
    InvalidMinidump,

    /// (Relay) The security report was not recognized due to missing data.
    SecurityReportType,

    /// (Relay) The security report did not pass schema validation.
    SecurityReport,

    /// (Relay) The request origin is not allowed for the project.
    Cors,

    /// (Relay) Reading or decoding the payload from the socket failed for any reason.
    Payload,

    /// (Relay) The request body was empty.
    EmptyBody,

    /// (Relay) The request body was invalid.
    InvalidBody,

    /// (Relay) Parsing the event JSON payload failed due to a syntax error.
    InvalidJson,

    /// (Relay) Parsing an OTLP payload failed.
    InvalidProtobuf,

    /// (Relay) Parsing the event msgpack payload failed due to a syntax error.
    InvalidMsgpack,

    /// (Relay) Parsing a multipart form-data request failed.
    InvalidMultipart,

    /// (Relay) The event is parseable but semantically invalid. This should only happen with
    /// transaction events.
    InvalidTransaction,

    /// (Relay) Parsing an event envelope failed (likely missing a required header).
    InvalidEnvelope,

    /// (Relay) The payload had an invalid compression format.
    InvalidCompression,

    /// (Relay) The envelope contains internal items.
    InternalEnvelope,

    /// (Relay) Failed to queue the envelope.
    QueueFailed,

    /// (Relay) The project could not be retrieved in time.
    ProjectUnavailable,

    /// (Relay) A project state returned by the upstream could not be parsed.
    ProjectState,

    /// (Relay) A project state returned by the upstream contained datascrubbing settings
    /// that could not be converted to PII config.
    ProjectStatePii,

    /// (Relay) An envelope was submitted with two items that need to be unique.
    DuplicateItem,

    /// (Relay) An event envelope was submitted but no payload could be extracted.
    NoEventPayload,

    /// (Relay) The timestamp of an event was required for processing and either missing out of the
    /// supported time range for ingestion.
    Timestamp,

    /// (All) An error in Relay caused event ingestion to fail. This is the catch-all and usually
    /// indicates bugs in Relay, rather than an expected failure.
    Internal,

    /// (Relay) The Unreal Crash report submitted to the Unreal endpoint was invalid.
    InvalidUnrealReport,

    /// (Relay) Symbolic failed to extract an Unreal Crash report from a request sent to the
    /// Unreal endpoint.
    ProcessUnreal,

    /// (Relay) The envelope, which contained only a transaction, was discarded by the
    /// dynamic sampling rules.
    TransactionSampled,

    /// (Relay) We failed to parse the replay so we discard it.
    InvalidReplayEvent,
    InvalidReplayEventNoPayload,
    InvalidReplayEventPii,
    InvalidReplayRecordingEvent,
    InvalidReplayVideoEvent,
    InvalidReplayMissingRecording,

    /// (Relay) Profiling related discard reasons
    Profiling(&'static str),

    /// (Relay) A log that is not valid after normalization.
    InvalidLog,

    /// (Relay) A trace metric that is not valid after normalization.
    InvalidTraceMetric,

    /// (Relay) A span is not valid after normalization.
    InvalidSpan,

    /// (Relay) A span attachment that has invalid item headers or attachment meta-data.
    InvalidSpanAttachment,

    /// (Relay) A trace attachment that has invalid item headers or attachment meta-data.
    InvalidTraceAttachment,

    /// (Relay) An attachment ref that has invalid item headers or payload.
    #[cfg(feature = "processing")]
    InvalidAttachmentRef,

    /// (Relay) A required feature is not enabled.
    FeatureDisabled(Feature),

    /// An attachment was submitted with a transaction.
    TransactionAttachment,

    /// (Relay) The signature from a trusted Relay was invalid.
    InvalidSignature,

    /// (Relay) The signature from a trusted Relay was missing but required.
    MissingSignature,

    /// (Relay) The signature from a trusted Relay was missing but required.
    InvalidCheckIn,

    /// (Relay) The dynamic sampling context is required but missing on the envelope.
    MissingDynamicSamplingContext,

    /// (Relay) The dynamic sampling context is invalid or does not match the payload.
    InvalidDynamicSamplingContext,

    /// (Relay) Failed to upload to objectstore.
    ObjectstoreUploadFailed,
}

impl DiscardReason {
    pub fn name(self) -> &'static str {
        match self {
            DiscardReason::Duplicate => "duplicate",
            DiscardReason::ProjectId => "project_id",
            DiscardReason::AuthVersion => "auth_version",
            DiscardReason::AuthClient => "auth_client",
            DiscardReason::NoData => "no_data",
            DiscardReason::ItemTooLarge(discard) => discard.as_str(),
            DiscardReason::RequestTooLarge => "request_too_large",
            DiscardReason::RateLimited => "rate_limited",
            DiscardReason::DisallowedMethod => "disallowed_method",
            DiscardReason::ContentType => "content_type",
            DiscardReason::MultiProjectId => "multi_project_id",
            DiscardReason::MissingProsperodump => "missing_prosperodump_upload",
            DiscardReason::InvalidProsperodump => "invalid_prosperodump",
            DiscardReason::MissingMinidump => "missing_minidump_upload",
            DiscardReason::InvalidMinidump => "invalid_minidump",
            DiscardReason::SecurityReportType => "security_report_type",
            DiscardReason::SecurityReport => "security_report",
            DiscardReason::Cors => "cors",
            DiscardReason::InvalidUnrealReport => "invalid_unreal_report",
            DiscardReason::ProcessUnreal => "process_unreal",
            DiscardReason::InvalidSignature => "invalid_signature",
            DiscardReason::MissingSignature => "missing_signature",
            DiscardReason::Payload => "payload",
            DiscardReason::EmptyBody => "empty_body",
            DiscardReason::InvalidBody => "invalid_body",
            DiscardReason::InvalidJson => "invalid_json",
            DiscardReason::InvalidMultipart => "invalid_multipart",
            DiscardReason::InvalidMsgpack => "invalid_msgpack",
            DiscardReason::InvalidProtobuf => "invalid_proto",
            DiscardReason::InvalidTransaction => "invalid_transaction",
            DiscardReason::InvalidEnvelope => "invalid_envelope",
            DiscardReason::InvalidCompression => "invalid_compression",
            DiscardReason::InvalidEventId => "invalid_event_id",
            DiscardReason::InternalEnvelope => "internal_envelope",
            DiscardReason::QueueFailed => "queue_failed",
            DiscardReason::Timestamp => "timestamp",
            DiscardReason::ProjectState => "project_state",
            DiscardReason::ProjectStatePii => "project_state_pii",
            DiscardReason::ProjectUnavailable => "project_unavailable",
            DiscardReason::DuplicateItem => "duplicate_item",
            DiscardReason::NoEventPayload => "no_event_payload",
            DiscardReason::Internal => "internal",
            DiscardReason::TransactionSampled => "transaction_sampled",
            DiscardReason::InvalidReplayEvent => "invalid_replay",
            DiscardReason::InvalidReplayEventNoPayload => "invalid_replay_no_payload",
            DiscardReason::InvalidReplayEventPii => "invalid_replay_pii_scrubber_failed",
            DiscardReason::InvalidReplayRecordingEvent => "invalid_replay_recording",
            DiscardReason::InvalidReplayVideoEvent => "invalid_replay_video",
            DiscardReason::InvalidReplayMissingRecording => "invalid_replay_missing_recording",
            DiscardReason::Profiling(reason) => reason,
            DiscardReason::InvalidLog => "invalid_log",
            DiscardReason::InvalidTraceMetric => "invalid_trace_metric",
            DiscardReason::InvalidSpan => "invalid_span",
            DiscardReason::InvalidSpanAttachment => "invalid_span_attachment",
            DiscardReason::InvalidTraceAttachment => "invalid_trace_attachment",
            #[cfg(feature = "processing")]
            DiscardReason::InvalidAttachmentRef => "invalid_placeholder_attachment",
            DiscardReason::FeatureDisabled(_) => "feature_disabled",
            DiscardReason::TransactionAttachment => "transaction_attachment",
            DiscardReason::InvalidCheckIn => "invalid_check_in",
            DiscardReason::MissingDynamicSamplingContext => "missing_dsc",
            DiscardReason::InvalidDynamicSamplingContext => "invalid_dsc",
            DiscardReason::ObjectstoreUploadFailed => "objectstore_upload_failed",
        }
    }
}

impl fmt::Display for DiscardReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Similar to [`ItemType`] but it does not have any additional information in the
/// Unknown variant so that it can derive [`Copy`] and be used from [`DiscardReason`].
/// The variants should be the same as [`ItemType`] except for `Attachment` which has a
/// [`DiscardAttachmentType`] parameter.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DiscardItemType {
    /// Event payload encoded in JSON.
    Event,
    /// Transaction event payload encoded in JSON.
    Transaction,
    /// Security report event payload encoded in JSON.
    Security,
    /// Raw payload of an arbitrary attachment.
    Attachment(DiscardAttachmentType),
    /// Multipart form data collected into a stream of JSON tuples.
    FormData,
    /// Security report as sent by the browser in JSON.
    RawSecurity,
    /// Raw compressed UE4 crash report.
    UnrealReport,
    /// User feedback encoded as JSON.
    UserReport,
    /// Session update data.
    Session,
    /// Aggregated session data.
    Sessions,
    /// Individual metrics in text encoding.
    Statsd,
    /// Buckets of preaggregated metrics encoded as JSON.
    MetricBuckets,
    /// Client internal report (eg: outcomes).
    ClientReport,
    /// Profile event payload encoded as JSON.
    Profile,
    /// Replay metadata and breadcrumb payload.
    ReplayEvent,
    /// Replay Recording data.
    ReplayRecording,
    /// Replay Video data.
    ReplayVideo,
    /// Monitor check-in encoded as JSON.
    CheckIn,
    /// A log for the log product, not internal logs.
    Log,
    /// A trace metric item.
    TraceMetric,
    /// A standalone span.
    Span,
    /// UserReport as an Event
    UserReportV2,
    /// ProfileChunk is a chunk of a profiling session.
    ProfileChunk,
    /// An integration item.
    Integration,
    /// A new item type that is yet unknown by this version of Relay.
    ///
    /// By default, items of this type are forwarded without modification. Processing Relays and
    /// Relays explicitly configured to do so will instead drop those items. This allows
    /// forward-compatibility with new item types where we expect outdated Relays.
    Unknown,
    // Keep `Unknown` last in the list. Add new items above `Unknown`.
}

impl DiscardItemType {
    /// Returns the enum variant as string representation in snake case.
    ///
    /// For example, `DiscardItemType::UserReportV2` gets converted into `user_report_v2`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Event => "event",
            Self::Transaction => "transaction",
            Self::Security => "security",
            Self::Attachment(DiscardAttachmentType::Attachment) => "attachment:attachment",
            Self::Attachment(DiscardAttachmentType::Minidump) => "attachment:minidump",
            Self::Attachment(DiscardAttachmentType::AppleCrashReport) => {
                "attachment:apple_crash_report"
            }
            Self::Attachment(DiscardAttachmentType::EventPayload) => "attachment:event_payload",
            Self::Attachment(DiscardAttachmentType::Breadcrumbs) => "attachment:breadcrumbs",
            Self::Attachment(DiscardAttachmentType::Prosperodump) => "attachment:prosperodump",
            Self::Attachment(DiscardAttachmentType::NnswitchDyingMessage) => {
                "attachment:nnswitch_dying_message"
            }
            Self::Attachment(DiscardAttachmentType::UnrealContext) => "attachment:unreal_context",
            Self::Attachment(DiscardAttachmentType::UnrealLogs) => "attachment:unreal_logs",
            Self::Attachment(DiscardAttachmentType::ViewHierarchy) => "attachment:view_hierarchy",
            Self::FormData => "form_data",
            Self::RawSecurity => "raw_security",
            Self::UnrealReport => "unreal_report",
            Self::UserReport => "user_report",
            Self::Session => "session",
            Self::Sessions => "sessions",
            Self::Statsd => "statsd",
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
            Self::UserReportV2 => "user_report_v2",
            Self::ProfileChunk => "profile_chunk",
            Self::Integration => "integration",
        }
    }
}

impl From<&ItemType> for DiscardItemType {
    fn from(value: &ItemType) -> Self {
        match value {
            ItemType::Event => Self::Event,
            ItemType::Transaction => Self::Transaction,
            ItemType::Security => Self::Security,
            ItemType::Attachment => Self::Attachment(DiscardAttachmentType::Attachment),
            ItemType::FormData => Self::FormData,
            ItemType::RawSecurity => Self::RawSecurity,
            ItemType::UnrealReport => Self::UnrealReport,
            ItemType::UserReport => Self::UserReport,
            ItemType::Session => Self::Session,
            ItemType::Sessions => Self::Sessions,
            ItemType::Statsd => Self::Statsd,
            ItemType::MetricBuckets => Self::MetricBuckets,
            ItemType::ClientReport => Self::ClientReport,
            ItemType::Profile => Self::Profile,
            ItemType::ReplayEvent => Self::ReplayEvent,
            ItemType::ReplayRecording => Self::ReplayRecording,
            ItemType::ReplayVideo => Self::ReplayVideo,
            ItemType::CheckIn => Self::CheckIn,
            ItemType::Log => Self::Log,
            ItemType::TraceMetric => Self::TraceMetric,
            ItemType::Span => Self::Span,
            ItemType::UserReportV2 => Self::UserReportV2,
            ItemType::ProfileChunk => Self::ProfileChunk,
            ItemType::Integration => Self::Integration,
            ItemType::Unknown(_) => Self::Unknown,
        }
    }
}

impl From<ItemType> for DiscardItemType {
    fn from(value: ItemType) -> Self {
        From::from(&value)
    }
}

impl From<&AttachmentType> for DiscardItemType {
    fn from(value: &AttachmentType) -> Self {
        Self::Attachment(value.into())
    }
}

impl From<AttachmentType> for DiscardItemType {
    fn from(value: AttachmentType) -> Self {
        From::from(&value)
    }
}

/// Similar to [`AttachmentType`] but it does not have any additional information in the
/// Unknown variant so that it can derive [`Copy`] and be used from [`DiscardReason`].
/// The variants should be the same as [`AttachmentType`].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DiscardAttachmentType {
    /// A regular attachment without special meaning.
    Attachment,
    /// A minidump crash report (binary data).
    Minidump,
    /// An apple crash report (text data).
    AppleCrashReport,
    /// A msgpack-encoded event payload submitted as part of multipart uploads.
    EventPayload,
    /// A msgpack-encoded list of payloads.
    Breadcrumbs,
    /// A Nintendo switch dying message.
    NnswitchDyingMessage,
    // A prosperodump crash report (binary data)
    Prosperodump,
    /// Binary attachment present in Unreal 4 events containing event context information.
    UnrealContext,
    /// Binary attachment present in Unreal 4 events containing event Logs.
    UnrealLogs,
    /// An application UI view hierarchy (json payload).
    ViewHierarchy,
}

impl From<&AttachmentType> for DiscardAttachmentType {
    fn from(value: &AttachmentType) -> Self {
        match value {
            AttachmentType::Attachment => Self::Attachment,
            AttachmentType::Minidump => Self::Minidump,
            AttachmentType::AppleCrashReport => Self::AppleCrashReport,
            AttachmentType::EventPayload => Self::EventPayload,
            AttachmentType::Breadcrumbs => Self::Breadcrumbs,
            AttachmentType::Prosperodump => Self::Prosperodump,
            AttachmentType::NintendoSwitchDyingMessage => Self::NnswitchDyingMessage,
            AttachmentType::UnrealContext => Self::UnrealContext,
            AttachmentType::UnrealLogs => Self::UnrealLogs,
            AttachmentType::ViewHierarchy => Self::ViewHierarchy,
        }
    }
}

impl From<AttachmentType> for DiscardAttachmentType {
    fn from(value: AttachmentType) -> Self {
        From::from(&value)
    }
}

impl fmt::Display for DiscardItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_category_roundtrip() {
        let input = "123,1004,1500,1403,1403,1404,1000";
        let rule_ids = MatchedRuleIds::parse(input).unwrap();
        let rule_categories = RuleCategories::from(rule_ids);

        let serialized = rule_categories.to_string();
        assert_eq!(&serialized, "1000,1004,1400,1500,0");

        assert_eq!(
            MatchedRuleIds::parse(&serialized).unwrap(),
            MatchedRuleIds([1000, 1004, 1400, 1500, 0].map(RuleId).into())
        );
    }

    #[test]
    fn test_outcome_discard_reason() {
        assert_eq!(
            Some(Cow::from("too_large:attachment:attachment")),
            Outcome::Invalid(DiscardReason::ItemTooLarge(DiscardItemType::Attachment(
                DiscardAttachmentType::Attachment
            )))
            .to_reason()
        );
        assert_eq!(
            Some(Cow::from("too_large:unknown")),
            Outcome::Invalid(DiscardReason::ItemTooLarge(DiscardItemType::Unknown)).to_reason()
        );
        assert_eq!(
            Some(Cow::from("too_large:unreal_report")),
            Outcome::Invalid(DiscardReason::ItemTooLarge(DiscardItemType::UnrealReport))
                .to_reason()
        );

        assert_eq!(
            Some(Cow::from("too_large:attachment:breadcrumbs")),
            Outcome::Invalid(DiscardReason::ItemTooLarge(DiscardItemType::Attachment(
                DiscardAttachmentType::Breadcrumbs
            )))
            .to_reason()
        );
    }
}
