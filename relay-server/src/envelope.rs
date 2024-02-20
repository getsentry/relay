//! Implementation of event envelopes.
//!
//! Envelopes are containers for payloads related to Sentry events. Similar to multipart form data
//! requests, each envelope has global headers and a set of items, such as the event payload, an
//! attachment, or intermediate payloads.
//!
//! During event ingestion, envelope items are normalized. While incoming envelopes may contain
//! items like security reports or raw form data, outgoing envelopes can only contain events and
//! attachments. All other items have to be transformed into one or the other (usually merged into
//! the event).
//!
//! Envelopes have a well-defined serialization format. It is roughly:
//!
//! ```plain
//! <json headers>\n
//! <item headers>\n
//! payload\n
//! ...
//! ```
//!
//! JSON headers and item headers must not contain line breaks. Payloads can be any binary encoding.
//! This is enabled by declaring an explicit length in the item headers. Example:
//!
//! ```plain
//! {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn": "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
//! {"type":"event","length":41,"content_type":"application/json"}
//! {"message":"hello world","level":"error"}
//! {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
//! Hello
//!
//! ```

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::ops::AddAssign;
use std::time::Instant;
use uuid::Uuid;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use relay_dynamic_config::ErrorBoundary;
use relay_event_schema::protocol::{EventId, EventType};
use relay_protocol::Value;
use relay_quotas::DataCategory;
use relay_sampling::DynamicSamplingContext;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::extractors::{PartialMeta, RequestMeta};

pub const CONTENT_TYPE: &str = "application/x-sentry-envelope";

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("unexpected end of file")]
    UnexpectedEof,
    #[error("missing envelope header")]
    MissingHeader,
    #[error("missing newline after header or payload")]
    MissingNewline,
    #[error("invalid envelope header")]
    InvalidHeader(#[source] serde_json::Error),
    #[error("{0} header mismatch between envelope and request")]
    HeaderMismatch(&'static str),
    #[error("invalid item header")]
    InvalidItemHeader(#[source] serde_json::Error),
    #[error("failed to write header")]
    HeaderIoFailed(#[source] serde_json::Error),
    #[error("failed to write payload")]
    PayloadIoFailed(#[source] io::Error),
}

/// The type of an envelope item.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ItemType {
    /// Event payload encoded in JSON.
    Event,
    /// Transaction event payload encoded in JSON.
    Transaction,
    /// Security report event payload encoded in JSON.
    Security,
    /// Raw payload of an arbitrary attachment.
    Attachment,
    /// Multipart form data collected into a stream of JSON tuples.
    FormData,
    /// Security report as sent by the browser in JSON.
    RawSecurity,
    /// NEL report as sent by the browser.
    Nel,
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
    /// Additional metadata for metrics
    MetricMeta,
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
    /// A standalone span.
    Span,
    /// A standalone OpenTelemetry span.
    OtelSpan,
    /// UserReport as an Event
    UserReportV2,
    /// A new item type that is yet unknown by this version of Relay.
    ///
    /// By default, items of this type are forwarded without modification. Processing Relays and
    /// Relays explicitly configured to do so will instead drop those items. This allows
    /// forward-compatibility with new item types where we expect outdated Relays.
    Unknown(String),
    // Keep `Unknown` last in the list. Add new items above `Unknown`.
}

impl ItemType {
    /// Returns the event item type corresponding to the given `EventType`.
    pub fn from_event_type(event_type: EventType) -> Self {
        match event_type {
            EventType::Default | EventType::Error | EventType::Nel => ItemType::Event,
            EventType::Transaction => ItemType::Transaction,
            EventType::UserReportV2 => ItemType::UserReportV2,
            EventType::Csp | EventType::Hpkp | EventType::ExpectCt | EventType::ExpectStaple => {
                ItemType::Security
            }
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
            Self::Nel => "nel",
            Self::UnrealReport => "unreal_report",
            Self::UserReport => "user_report",
            Self::UserReportV2 => "feedback",
            Self::Session => "session",
            Self::Sessions => "sessions",
            Self::Statsd => "statsd",
            Self::MetricBuckets => "metric_buckets",
            Self::MetricMeta => "metric_meta",
            Self::ClientReport => "client_report",
            Self::Profile => "profile",
            Self::ReplayEvent => "replay_event",
            Self::ReplayRecording => "replay_recording",
            Self::ReplayVideo => "replay_video",
            Self::CheckIn => "check_in",
            Self::Span => "span",
            Self::OtelSpan => "otel_span",
            Self::Unknown(_) => "unknown",
        }
    }

    /// Returns the item type as a string.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown(ref s) => s,
            _ => self.name(),
        }
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
            "nel" => Self::Nel,
            "unreal_report" => Self::UnrealReport,
            "user_report" => Self::UserReport,
            "feedback" => Self::UserReportV2,
            "session" => Self::Session,
            "sessions" => Self::Sessions,
            "statsd" => Self::Statsd,
            "metric_buckets" => Self::MetricBuckets,
            "metric_meta" => Self::MetricMeta,
            "client_report" => Self::ClientReport,
            "profile" => Self::Profile,
            "replay_event" => Self::ReplayEvent,
            "replay_recording" => Self::ReplayRecording,
            "replay_video" => Self::ReplayVideo,
            "check_in" => Self::CheckIn,
            "span" => Self::Span,
            "otel_span" => Self::OtelSpan,
            other => Self::Unknown(other.to_owned()),
        })
    }
}

relay_common::impl_str_serde!(ItemType, "an envelope item type (see sentry develop docs)");

/// Payload content types.
///
/// This is an optimized enum intended to reduce allocations for common content types.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ContentType {
    /// text/plain
    Text,
    /// application/json
    Json,
    /// application/x-msgpack
    MsgPack,
    /// application/octet-stream
    OctetStream,
    /// application/x-dmp
    Minidump,
    /// text/xml and application/xml
    Xml,
    /// application/x-sentry-envelope
    Envelope,
    /// Any arbitrary content type not listed explicitly.
    Other(String),
}

impl ContentType {
    #[inline]
    pub fn as_str(&self) -> &str {
        match *self {
            Self::Text => "text/plain",
            Self::Json => "application/json",
            Self::MsgPack => "application/x-msgpack",
            Self::OctetStream => "application/octet-stream",
            Self::Minidump => "application/x-dmp",
            Self::Xml => "text/xml",
            Self::Envelope => self::CONTENT_TYPE,
            Self::Other(ref other) => other,
        }
    }

    fn from_str(ct: &str) -> Option<Self> {
        if ct.eq_ignore_ascii_case(Self::Text.as_str()) {
            Some(Self::Text)
        } else if ct.eq_ignore_ascii_case(Self::Json.as_str()) {
            Some(Self::Json)
        } else if ct.eq_ignore_ascii_case(Self::MsgPack.as_str()) {
            Some(Self::MsgPack)
        } else if ct.eq_ignore_ascii_case(Self::OctetStream.as_str()) {
            Some(Self::OctetStream)
        } else if ct.eq_ignore_ascii_case(Self::Minidump.as_str()) {
            Some(Self::Minidump)
        } else if ct.eq_ignore_ascii_case(Self::Xml.as_str())
            || ct.eq_ignore_ascii_case("application/xml")
        {
            Some(Self::Xml)
        } else if ct.eq_ignore_ascii_case(Self::Envelope.as_str()) {
            Some(Self::Envelope)
        } else {
            None
        }
    }
}

impl From<String> for ContentType {
    fn from(mut content_type: String) -> Self {
        Self::from_str(&content_type).unwrap_or_else(|| {
            content_type.make_ascii_lowercase();
            ContentType::Other(content_type)
        })
    }
}

impl From<&'_ str> for ContentType {
    fn from(content_type: &str) -> Self {
        Self::from_str(content_type)
            .unwrap_or_else(|| ContentType::Other(content_type.to_ascii_lowercase()))
    }
}

impl std::str::FromStr for ContentType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl PartialEq<str> for ContentType {
    fn eq(&self, other: &str) -> bool {
        // Take an indirection via ContentType::from_str to also check aliases. Do not allocate in
        // case there is no mapping.
        match ContentType::from_str(other) {
            Some(ct) => ct == *self,
            None => other.eq_ignore_ascii_case(self.as_str()),
        }
    }
}

impl PartialEq<&'_ str> for ContentType {
    fn eq(&self, other: &&'_ str) -> bool {
        *self == **other
    }
}

impl PartialEq<String> for ContentType {
    fn eq(&self, other: &String) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<ContentType> for &'_ str {
    fn eq(&self, other: &ContentType) -> bool {
        *other == *self
    }
}

impl PartialEq<ContentType> for str {
    fn eq(&self, other: &ContentType) -> bool {
        *other == *self
    }
}

impl PartialEq<ContentType> for String {
    fn eq(&self, other: &ContentType) -> bool {
        *other == *self
    }
}

impl Serialize for ContentType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

relay_common::impl_str_de!(ContentType, "a content type string");

/// The type of an event attachment.
///
/// These item types must align with the Sentry processing pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttachmentType {
    /// A regular attachment without special meaning.
    Attachment,

    /// A minidump crash report (binary data).
    Minidump,

    /// An apple crash report (text data).
    AppleCrashReport,

    /// A msgpack-encoded event payload submitted as part of multipart uploads.
    ///
    /// This attachment is processed by Relay immediately and never forwarded or persisted.
    EventPayload,

    /// A msgpack-encoded list of payloads.
    ///
    /// There can be two attachments that the SDK may use as swappable buffers. Both attachments
    /// will be merged and truncated to the maxmimum number of allowed attachments.
    ///
    /// This attachment is processed by Relay immediately and never forwarded or persisted.
    Breadcrumbs,

    /// This is a binary attachment present in Unreal 4 events containing event context information.
    ///
    /// This can be deserialized using the `symbolic` crate see
    /// [`symbolic_unreal::Unreal4Context`].
    ///
    /// [`symbolic_unreal::Unreal4Context`]: https://docs.rs/symbolic/*/symbolic/unreal/struct.Unreal4Context.html
    UnrealContext,

    /// This is a binary attachment present in Unreal 4 events containing event Logs.
    ///
    /// This can be deserialized using the `symbolic` crate see
    /// [`symbolic_unreal::Unreal4LogEntry`].
    ///
    /// [`symbolic_unreal::Unreal4LogEntry`]: https://docs.rs/symbolic/*/symbolic/unreal/struct.Unreal4LogEntry.html
    UnrealLogs,

    /// An application UI view hierarchy (json payload).
    ViewHierarchy,

    /// Unknown attachment type, forwarded for compatibility.
    /// Attachments with this type will be dropped if `accept_unknown_items` is set to false.
    Unknown(String),
}

impl Default for AttachmentType {
    fn default() -> Self {
        Self::Attachment
    }
}

impl fmt::Display for AttachmentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttachmentType::Attachment => write!(f, "event.attachment"),
            AttachmentType::Minidump => write!(f, "event.minidump"),
            AttachmentType::AppleCrashReport => write!(f, "event.applecrashreport"),
            AttachmentType::EventPayload => write!(f, "event.payload"),
            AttachmentType::Breadcrumbs => write!(f, "event.breadcrumbs"),
            AttachmentType::UnrealContext => write!(f, "unreal.context"),
            AttachmentType::UnrealLogs => write!(f, "unreal.logs"),
            AttachmentType::ViewHierarchy => write!(f, "event.view_hierarchy"),
            AttachmentType::Unknown(s) => s.fmt(f),
        }
    }
}

impl std::str::FromStr for AttachmentType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "event.attachment" => AttachmentType::Attachment,
            "event.minidump" => AttachmentType::Minidump,
            "event.applecrashreport" => AttachmentType::AppleCrashReport,
            "event.payload" => AttachmentType::EventPayload,
            "event.breadcrumbs" => AttachmentType::Breadcrumbs,
            "event.view_hierarchy" => AttachmentType::ViewHierarchy,
            "unreal.context" => AttachmentType::UnrealContext,
            "unreal.logs" => AttachmentType::UnrealLogs,
            other => AttachmentType::Unknown(other.to_owned()),
        })
    }
}

relay_common::impl_str_serde!(
    AttachmentType,
    "an attachment type (see sentry develop docs)"
);

fn is_false(val: &bool) -> bool {
    !*val
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ItemHeaders {
    /// The type of the item.
    #[serde(rename = "type")]
    ty: ItemType,

    /// Content length of the item.
    ///
    /// Can be omitted if the item does not contain new lines. In this case, the item payload is
    /// parsed until the first newline is encountered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    length: Option<u32>,

    /// If this is an attachment item, this may contain the attachment type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attachment_type: Option<AttachmentType>,

    /// Content type of the payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<ContentType>,

    /// If this is an attachment item, this may contain the original file name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    filename: Option<String>,

    /// The routing_hint may be used to specify how the envelpope should be routed in when
    /// published to kafka.
    ///
    /// XXX(epurkhiser): This is currently ONLY used for [`ItemType::CheckIn`]'s when publishing
    /// the envelope into kafka.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing_hint: Option<Uuid>,

    /// Indicates that this item is being rate limited.
    ///
    /// By default, rate limited items are immediately removed from Envelopes. For processing,
    /// native crash reports still need to be retained. These attachments are marked with the
    /// `rate_limited` header, which signals to the processing pipeline that the attachment should
    /// not be persisted after processing.
    ///
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    rate_limited: bool,

    /// Indicates that this item should be combined into one payload with other replay item.
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    replay_combined_payload: bool,

    /// The parsed replay-event and replay-recording payloads.
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    replay_video_events: Option<(Vec<u8>, Vec<u8>)>,

    /// Contains the amount of events this item was generated and aggregated from.
    ///
    /// A [metrics buckets](`ItemType::MetricBuckets`) item contains metrics extracted and
    /// aggregated from (currently) transactions and profiles.
    ///
    /// This information can not be directly inferred from the item itself anymore.
    /// The amount of events this item/metric represents is instead stored here.
    ///
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    source_quantities: Option<SourceQuantities>,

    /// A list of cumulative sample rates applied to this event.
    ///
    /// Multiple entries in `sample_rates` mean that the event was sampled multiple times. The
    /// effective sample rate is multiplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sample_rates: Option<Value>,

    /// Flag indicating if metrics have already been extracted from the item.
    ///
    /// In order to only extract metrics once from an item while through a
    /// chain of Relays, a Relay that extracts metrics from an item (typically
    /// the first Relay) MUST set this flat to true so that upstream Relays do
    /// not extract the metric again causing double counting of the metric.
    #[serde(default, skip_serializing_if = "is_false")]
    metrics_extracted: bool,

    /// `false` if the sampling decision is "drop".
    ///
    /// In the most common use case, the item is dropped when the sampling decision is "drop".
    /// For profiles with the feature enabled, however, we keep all profile items and mark the ones
    /// for which the transaction was dropped as `sampled: false`.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    sampled: bool,

    /// Other attributes for forward compatibility.
    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

/// Container for item quantities that the item was derived from.
///
/// For example a metric bucket may be derived and aggregated from multiple transactions.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceQuantities {
    /// Transaction quantity.
    pub transactions: usize,
    /// Profile quantity.
    pub profiles: usize,
    /// Total number of buckets.
    pub buckets: usize,
}

impl AddAssign for SourceQuantities {
    fn add_assign(&mut self, other: Self) {
        self.transactions += other.transactions;
        self.profiles += other.profiles;
    }
}

#[derive(Clone, Debug)]
pub struct Item {
    headers: ItemHeaders,
    payload: Bytes,
}

impl Item {
    /// Creates a new item with the given type.
    pub fn new(ty: ItemType) -> Self {
        Self {
            headers: ItemHeaders {
                ty,
                length: Some(0),
                attachment_type: None,
                content_type: None,
                filename: None,
                routing_hint: None,
                rate_limited: false,
                replay_combined_payload: false,
                replay_video_events: None,
                source_quantities: None,
                sample_rates: None,
                other: BTreeMap::new(),
                metrics_extracted: false,
                sampled: true,
            },
            payload: Bytes::new(),
        }
    }

    /// Returns the `ItemType` of this item.
    pub fn ty(&self) -> &ItemType {
        &self.headers.ty
    }

    /// Returns the length of this item's payload.
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    /// Returns the number used for counting towards rate limits and producing outcomes.
    ///
    /// For attachments, we count the number of bytes. Other items are counted as 1.
    pub fn quantity(&self) -> usize {
        match self.ty() {
            ItemType::Attachment => self.len().max(1),
            _ => 1,
        }
    }

    /// Returns the data category used for generating outcomes.
    ///
    /// Returns `None` if outcomes are not generated for this type (e.g. sessions).
    pub fn outcome_category(&self, indexed: bool) -> Option<DataCategory> {
        match self.ty() {
            ItemType::Event => Some(DataCategory::Error),
            ItemType::Transaction => Some(if indexed {
                DataCategory::TransactionIndexed
            } else {
                DataCategory::Transaction
            }),
            ItemType::Security | ItemType::RawSecurity => Some(DataCategory::Security),
            ItemType::Nel => None,
            ItemType::UnrealReport => Some(DataCategory::Error),
            ItemType::Attachment => Some(DataCategory::Attachment),
            ItemType::Session | ItemType::Sessions => None,
            ItemType::Statsd | ItemType::MetricBuckets | ItemType::MetricMeta => None,
            ItemType::FormData => None,
            ItemType::UserReport => None,
            ItemType::UserReportV2 => None,
            ItemType::Profile => Some(if indexed {
                DataCategory::ProfileIndexed
            } else {
                DataCategory::Profile
            }),
            ItemType::ReplayEvent | ItemType::ReplayRecording | ItemType::ReplayVideo => {
                Some(DataCategory::Replay)
            }
            ItemType::ClientReport => None,
            ItemType::CheckIn => Some(DataCategory::Monitor),
            ItemType::Span | ItemType::OtelSpan => Some(if indexed {
                DataCategory::SpanIndexed
            } else {
                DataCategory::Span
            }),
            ItemType::Unknown(_) => None,
        }
    }

    /// Returns `true` if this item's payload is empty.
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Returns the content type of this item's payload.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn content_type(&self) -> Option<&ContentType> {
        self.headers.content_type.as_ref()
    }

    /// Returns the attachment type if this item is an attachment.
    pub fn attachment_type(&self) -> Option<&AttachmentType> {
        // TODO: consider to replace this with an ItemType?
        self.headers.attachment_type.as_ref()
    }

    /// Sets the attachment type of this item.
    pub fn set_attachment_type(&mut self, attachment_type: AttachmentType) {
        self.headers.attachment_type = Some(attachment_type);
    }

    /// Returns the payload of this item.
    ///
    /// Envelope payloads are ref-counted. The bytes object is a reference to the original data, but
    /// cannot be used to mutate data in this envelope. In order to change data, use `set_payload`.
    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    /// Sets the payload and content-type of this envelope.
    pub fn set_payload<B>(&mut self, content_type: ContentType, payload: B)
    where
        B: Into<Bytes>,
    {
        let mut payload = payload.into();

        let length = std::cmp::min(u32::max_value() as usize, payload.len());
        payload.truncate(length);

        self.headers.length = Some(length as u32);
        self.headers.content_type = Some(content_type);
        self.payload = payload;
    }

    /// Returns the file name of this item, if it is an attachment.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn filename(&self) -> Option<&str> {
        self.headers.filename.as_deref()
    }

    /// Sets the file name of this item.
    pub fn set_filename<S>(&mut self, filename: S)
    where
        S: Into<String>,
    {
        self.headers.filename = Some(filename.into());
    }

    /// Returns the routing_hint of this item.
    #[cfg(feature = "processing")]
    pub fn routing_hint(&self) -> Option<Uuid> {
        self.headers.routing_hint
    }

    /// Set the routing_hint of this item.
    #[cfg(feature = "processing")]
    pub fn set_routing_hint(&mut self, routing_hint: Uuid) {
        self.headers.routing_hint = Some(routing_hint);
    }

    /// Returns whether this item should be rate limited.
    pub fn rate_limited(&self) -> bool {
        self.headers.rate_limited
    }

    /// Sets whether this item should be rate limited.
    pub fn set_rate_limited(&mut self, rate_limited: bool) {
        self.headers.rate_limited = rate_limited;
    }

    /// Removes sample rates from the headers, if any.
    pub fn take_sample_rates(&mut self) -> Option<Value> {
        self.headers.sample_rates.take()
    }

    /// Returns the contained source quantities.
    pub fn source_quantities(&self) -> Option<SourceQuantities> {
        self.headers.source_quantities
    }

    /// Sets new source quantities.
    pub fn set_source_quantities(&mut self, source_quantities: SourceQuantities) {
        self.headers.source_quantities = Some(source_quantities);
    }

    /// Returns if the payload's replay items should be combined into one kafka message.
    #[cfg(feature = "processing")]
    pub fn replay_combined_payload(&self) -> bool {
        self.headers.replay_combined_payload
    }

    /// Sets the replay_combined_payload for this item.
    pub fn set_replay_combined_payload(&mut self, combined_payload: bool) {
        self.headers.replay_combined_payload = combined_payload;
    }

    /// Returns the payload's replay video events.
    #[cfg(feature = "processing")]
    pub fn replay_video_events(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.headers.replay_video_events.clone()
    }

    /// Set the replay video events attribute for this item.
    pub fn set_replay_video_events(&mut self, replay_event: Vec<u8>, replay_recording: Vec<u8>) {
        self.headers.replay_video_events = Some((replay_event, replay_recording));
    }

    /// Sets sample rates for this item.
    pub fn set_sample_rates(&mut self, sample_rates: Value) {
        if matches!(sample_rates, Value::Array(ref a) if !a.is_empty()) {
            self.headers.sample_rates = Some(sample_rates);
        }
    }

    /// Returns the metrics extracted flag.
    pub fn metrics_extracted(&self) -> bool {
        self.headers.metrics_extracted
    }

    /// Sets the metrics extracted flag.
    pub fn set_metrics_extracted(&mut self, metrics_extracted: bool) {
        self.headers.metrics_extracted = metrics_extracted;
    }

    /// Gets the `sampled` flag.
    pub fn sampled(&self) -> bool {
        self.headers.sampled
    }

    /// Sets the `sampled` flag.
    pub fn set_sampled(&mut self, sampled: bool) {
        self.headers.sampled = sampled;
    }

    /// Returns the specified header value, if present.
    pub fn get_header<K>(&self, name: &K) -> Option<&Value>
    where
        String: Borrow<K>,
        K: Ord + ?Sized,
    {
        self.headers.other.get(name)
    }

    /// Sets the specified header value, returning the previous one if present.
    pub fn set_header<S, V>(&mut self, name: S, value: V) -> Option<Value>
    where
        S: Into<String>,
        V: Into<Value>,
    {
        self.headers.other.insert(name.into(), value.into())
    }

    /// Determines whether the given item creates an event.
    ///
    /// This is only true for literal events and crash report attachments.
    pub fn creates_event(&self) -> bool {
        match self.ty() {
            // These items are direct event types.
            ItemType::Event
            | ItemType::Transaction
            | ItemType::Security
            | ItemType::RawSecurity
            | ItemType::Nel
            | ItemType::UnrealReport
            | ItemType::UserReportV2 => true,

            // Attachments are only event items if they are crash reports or if they carry partial
            // event payloads. Plain attachments never create event payloads.
            ItemType::Attachment => {
                match self.attachment_type().unwrap_or(&AttachmentType::default()) {
                    AttachmentType::AppleCrashReport
                    | AttachmentType::Minidump
                    | AttachmentType::EventPayload
                    | AttachmentType::Breadcrumbs => true,
                    AttachmentType::Attachment
                    | AttachmentType::UnrealContext
                    | AttachmentType::UnrealLogs
                    | AttachmentType::ViewHierarchy => false,
                    // When an outdated Relay instance forwards an unknown attachment type for compatibility,
                    // we assume that the attachment does not create a new event. This will make it hard
                    // to introduce new attachment types which _do_ create a new event.
                    AttachmentType::Unknown(_) => false,
                }
            }

            // Form data items may contain partial event payloads, but those are only ever valid if
            // they occur together with an explicit event item, such as a minidump or apple crash
            // report. For this reason, FormData alone does not constitute an event item.
            ItemType::FormData => false,

            // The remaining item types cannot carry event payloads.
            ItemType::UserReport
            | ItemType::Session
            | ItemType::Sessions
            | ItemType::Statsd
            | ItemType::MetricBuckets
            | ItemType::MetricMeta
            | ItemType::ClientReport
            | ItemType::ReplayEvent
            | ItemType::ReplayRecording
            | ItemType::ReplayVideo
            | ItemType::Profile
            | ItemType::CheckIn
            | ItemType::Span
            | ItemType::OtelSpan => false,

            // The unknown item type can observe any behavior, most likely there are going to be no
            // item types added that create events.
            ItemType::Unknown(_) => false,
        }
    }

    /// Determines whether the given item requires an event with identifier.
    ///
    /// This is true for all items except session health events.
    pub fn requires_event(&self) -> bool {
        match self.ty() {
            ItemType::Event => true,
            ItemType::Transaction => true,
            ItemType::Security => true,
            ItemType::Attachment => true,
            ItemType::FormData => true,
            ItemType::RawSecurity => true,
            ItemType::Nel => false,
            ItemType::UnrealReport => true,
            ItemType::UserReport => true,
            ItemType::UserReportV2 => true,
            ItemType::ReplayEvent => true,
            ItemType::Session => false,
            ItemType::Sessions => false,
            ItemType::Statsd => false,
            ItemType::MetricBuckets => false,
            ItemType::MetricMeta => false,
            ItemType::ClientReport => false,
            ItemType::ReplayRecording => false,
            ItemType::ReplayVideo => false,
            ItemType::Profile => true,
            ItemType::CheckIn => false,
            ItemType::Span => false,
            ItemType::OtelSpan => false,

            // Since this Relay cannot interpret the semantics of this item, it does not know
            // whether it requires an event or not. Depending on the strategy, this can cause two
            // wrong actions here:
            //  1. return false, but the item requires an event. It is split off by Relay and
            //     handled separately. If the event is rate limited or filtered, the item still gets
            //     ingested and needs to be pruned at a later point in the pipeline. This also
            //     happens with standalone attachments.
            //  2. return true, but the item does not require an event. It is kept in the same
            //     envelope and dropped along with the event, even though it should be ingested.
            // Realistically, most new item types should be ingested largely independent of events,
            // and the ingest pipeline needs to assume split submission from clients. This makes
            // returning `false` the safer option.
            ItemType::Unknown(_) => false,
        }
    }
}

pub type Items = SmallVec<[Item; 3]>;
pub type ItemIter<'a> = std::slice::Iter<'a, Item>;
pub type ItemIterMut<'a> = std::slice::IterMut<'a, Item>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EnvelopeHeaders<M = RequestMeta> {
    /// Unique identifier of the event associated to this envelope.
    ///
    /// Envelopes without contained events do not contain an event id.  This is for instance
    /// the case for session metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,

    /// Further event information derived from a store request.
    #[serde(flatten)]
    meta: M,

    /// Data retention in days for the items of this envelope.
    ///
    /// This value is always overwritten in processing mode by the value specified in the project
    /// configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retention: Option<u16>,

    /// Timestamp when the event has been sent, according to the SDK.
    ///
    /// This can be used to perform drift correction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sent_at: Option<DateTime<Utc>>,

    /// Trace context associated with the request
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace: Option<ErrorBoundary<DynamicSamplingContext>>,

    /// Other attributes for forward compatibility.
    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

impl EnvelopeHeaders<PartialMeta> {
    /// Validate and apply request meta into partial envelope headers.
    ///
    /// If there is a mismatch, `EnvelopeError::HeaderMismatch` is returned.
    ///
    /// This method validates the following headers:
    ///  - DSN project (required)
    ///  - DSN public key (required)
    ///  - Origin (if present)
    fn complete(self, request_meta: RequestMeta) -> Result<EnvelopeHeaders, EnvelopeError> {
        let meta = self.meta;

        // Relay does not read the envelope's headers before running initial validation and fully
        // relies on request headers at the moment. Technically, the envelope's meta is checked
        // again once the event goes through validation, but we want to be as accurate as possible
        // in the endpoint already.
        if meta.origin().is_some() && meta.origin() != request_meta.origin() {
            return Err(EnvelopeError::HeaderMismatch("origin"));
        }

        if let Some(dsn) = meta.dsn() {
            if dsn.project_id() != request_meta.dsn().project_id() {
                return Err(EnvelopeError::HeaderMismatch("project id"));
            }
            if dsn.public_key() != request_meta.dsn().public_key() {
                return Err(EnvelopeError::HeaderMismatch("public key"));
            }
        }

        Ok(EnvelopeHeaders {
            event_id: self.event_id,
            meta: meta.copy_to(request_meta),
            retention: self.retention,
            sent_at: self.sent_at,
            trace: self.trace,
            other: self.other,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Envelope {
    headers: EnvelopeHeaders,
    items: Items,
}

impl Envelope {
    /// Creates an envelope from the provided parts.
    pub fn from_parts(headers: EnvelopeHeaders, items: Items) -> Box<Self> {
        Box::new(Self { items, headers })
    }

    /// Creates an envelope from request information.
    pub fn from_request(event_id: Option<EventId>, meta: RequestMeta) -> Box<Self> {
        Box::new(Self {
            headers: EnvelopeHeaders {
                event_id,
                meta,
                retention: None,
                sent_at: None,
                other: BTreeMap::new(),
                trace: None,
            },
            items: Items::new(),
        })
    }

    /// Parses an envelope from bytes.
    #[allow(dead_code)]
    pub fn parse_bytes(bytes: Bytes) -> Result<Box<Self>, EnvelopeError> {
        let (headers, offset) = Self::parse_headers(&bytes)?;
        let items = Self::parse_items(&bytes, offset)?;

        Ok(Box::new(Envelope { headers, items }))
    }

    /// Parses an envelope taking into account a request.
    ///
    /// This method is intended to be used when parsing an envelope that was sent as part of a web
    /// request. It validates that request headers are in line with the envelope's headers.
    ///
    /// If no event id is provided explicitly, one is created on the fly.
    pub fn parse_request(
        bytes: Bytes,
        request_meta: RequestMeta,
    ) -> Result<Box<Self>, EnvelopeError> {
        let (partial_headers, offset) = Self::parse_headers::<PartialMeta>(&bytes)?;
        let mut headers = partial_headers.complete(request_meta)?;

        // Event-related envelopes *must* contain an event id.
        let items = Self::parse_items(&bytes, offset)?;
        if items.iter().any(Item::requires_event) {
            headers.event_id.get_or_insert_with(EventId::new);
        }

        Ok(Box::new(Envelope { headers, items }))
    }

    /// Move the envelope's items into an envelope with the same headers.
    pub fn take_items(&mut self) -> Envelope {
        let Self { headers, items } = self;
        Self {
            headers: headers.clone(),
            items: std::mem::take(items),
        }
    }

    /// Returns reference to the [`EnvelopeHeaders`].
    pub fn headers(&self) -> &EnvelopeHeaders {
        &self.headers
    }

    /// Returns the number of items in this envelope.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if this envelope does not contain any items.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Unique identifier of the event associated to this envelope.
    ///
    /// The envelope may directly contain an event which has this id. Alternatively, it can contain
    /// payloads that can be transformed into events (such as security reports). Lastly, there can
    /// be additional information to an event, such as attachments.
    ///
    /// It's permissible for envelopes to not contain event bound information such as session data
    /// in which case this returns None.
    pub fn event_id(&self) -> Option<EventId> {
        self.headers.event_id
    }

    /// Returns event metadata information.
    pub fn meta(&self) -> &RequestMeta {
        &self.headers.meta
    }

    /// Returns a mutable reference to event metadata information.
    pub fn meta_mut(&mut self) -> &mut RequestMeta {
        &mut self.headers.meta
    }

    /// Returns the data retention in days for items in this envelope.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn retention(&self) -> u16 {
        self.headers.retention.unwrap_or(DEFAULT_EVENT_RETENTION)
    }

    /// When the event has been sent, according to the SDK.
    pub fn sent_at(&self) -> Option<DateTime<Utc>> {
        self.headers.sent_at
    }

    /// Sets the event id on the envelope.
    pub fn set_event_id(&mut self, event_id: EventId) {
        self.headers.event_id = Some(event_id);
    }

    /// Sets the timestamp at which an envelope is sent to the upstream.
    pub fn set_sent_at(&mut self, sent_at: DateTime<Utc>) {
        self.headers.sent_at = Some(sent_at);
    }

    /// Sets the start time to the provided `Instant`.
    pub fn set_start_time(&mut self, start_time: Instant) {
        self.headers.meta.set_start_time(start_time)
    }

    /// Sets the data retention in days for items in this envelope.
    pub fn set_retention(&mut self, retention: u16) {
        self.headers.retention = Some(retention);
    }

    /// Returns the dynamic sampling context from envelope headers, if present.
    pub fn dsc(&self) -> Option<&DynamicSamplingContext> {
        match &self.headers.trace {
            None => None,
            Some(ErrorBoundary::Err(e)) => {
                relay_log::debug!(error = e.as_ref(), "failed to parse sampling context");
                None
            }
            Some(ErrorBoundary::Ok(t)) => Some(t),
        }
    }

    /// Overrides the dynamic sampling context in envelope headers.
    pub fn set_dsc(&mut self, dsc: DynamicSamplingContext) {
        self.headers.trace = Some(ErrorBoundary::Ok(dsc));
    }

    /// Returns the specified header value, if present.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn get_header<K>(&self, name: &K) -> Option<&Value>
    where
        String: Borrow<K>,
        K: Ord + ?Sized,
    {
        self.headers.other.get(name)
    }

    /// Sets the specified header value, returning the previous one if present.
    pub fn set_header<S, V>(&mut self, name: S, value: V) -> Option<Value>
    where
        S: Into<String>,
        V: Into<Value>,
    {
        self.headers.other.insert(name.into(), value.into())
    }

    /// Returns an iterator over items in this envelope.
    ///
    /// Note that iteration order may change when using `take_item`.
    pub fn items(&self) -> ItemIter<'_> {
        self.items.iter()
    }

    /// Returns a mutable iterator over items in this envelope.
    ///
    /// Note that iteration order may change when using `take_item`.
    pub fn items_mut(&mut self) -> ItemIterMut<'_> {
        self.items.iter_mut()
    }

    /// Returns an option with a reference to the first item that matches
    /// the predicate, or None if the predicate is not matched by any item.
    pub fn get_item_by<F>(&self, mut pred: F) -> Option<&Item>
    where
        F: FnMut(&Item) -> bool,
    {
        self.items().find(|item| pred(item))
    }

    /// Returns an option with a mutable reference to the first item that matches
    /// the predicate, or None if the predicate is not matched by any item.
    pub fn get_item_by_mut<F>(&mut self, mut pred: F) -> Option<&mut Item>
    where
        F: FnMut(&Item) -> bool,
    {
        self.items_mut().find(|item| pred(item))
    }

    /// Removes and returns the first item that matches the given condition.
    pub fn take_item_by<F>(&mut self, cond: F) -> Option<Item>
    where
        F: FnMut(&Item) -> bool,
    {
        let index = self.items.iter().position(cond);
        index.map(|index| self.items.swap_remove(index))
    }

    /// Removes and returns the all items that match the given condition.
    pub fn take_items_by<F>(&mut self, mut cond: F) -> SmallVec<[Item; 3]>
    where
        F: FnMut(&Item) -> bool,
    {
        self.items.drain_filter(|item| cond(item)).collect()
    }

    /// Adds a new item to this envelope.
    pub fn add_item(&mut self, item: Item) {
        self.items.push(item)
    }

    /// Splits off the items from the envelope using provided predicates.
    ///
    /// First predicate is the additional condition on the count of found items by second
    /// predicate.
    #[cfg(test)]
    fn split_off_items<C, F>(&mut self, cond: C, mut f: F) -> Option<SmallVec<[Item; 3]>>
    where
        C: Fn(usize) -> bool,
        F: FnMut(&Item) -> bool,
    {
        let split_count = self.items().filter(|item| f(item)).count();
        if cond(split_count) {
            return None;
        }

        let old_items = std::mem::take(&mut self.items);
        let (split_items, own_items) = old_items.into_iter().partition(f);
        self.items = own_items;

        Some(split_items)
    }

    /// Splits the envelope by the given predicate.
    ///
    /// The predicate passed to `split_by()` can return `true`, or `false`. If it returns `true` or
    /// `false` for all items, then this returns `None`. Otherwise, a new envelope is constructed
    /// with all items that return `true`. Items that return `false` remain in this envelope.
    ///
    /// The returned envelope assumes the same headers.
    #[cfg(test)]
    pub fn split_by<F>(&mut self, f: F) -> Option<Box<Self>>
    where
        F: FnMut(&Item) -> bool,
    {
        let items_count = self.len();
        let split_items = self.split_off_items(|count| count == 0 || count == items_count, f)?;
        Some(Box::new(Envelope {
            headers: self.headers.clone(),
            items: split_items,
        }))
    }

    /// Retains only the items specified by the predicate.
    ///
    /// In other words, remove all elements where `f(&item)` returns `false`. This method operates
    /// in place and preserves the order of the retained items.
    pub fn retain_items<F>(&mut self, f: F)
    where
        F: FnMut(&mut Item) -> bool,
    {
        self.items.retain(f)
    }

    /// Serializes this envelope into the given writer.
    pub fn serialize<W>(&self, mut writer: W) -> Result<(), EnvelopeError>
    where
        W: Write,
    {
        serde_json::to_writer(&mut writer, &self.headers).map_err(EnvelopeError::HeaderIoFailed)?;
        self.write(&mut writer, b"\n")?;

        for item in &self.items {
            serde_json::to_writer(&mut writer, &item.headers)
                .map_err(EnvelopeError::HeaderIoFailed)?;
            self.write(&mut writer, b"\n")?;

            self.write(&mut writer, &item.payload)?;
            self.write(&mut writer, b"\n")?;
        }

        Ok(())
    }

    /// Serializes this envelope into a buffer.
    pub fn to_vec(&self) -> Result<Vec<u8>, EnvelopeError> {
        let mut vec = Vec::new(); // TODO: Preallocate?
        self.serialize(&mut vec)?;
        Ok(vec)
    }

    fn parse_headers<M>(slice: &[u8]) -> Result<(EnvelopeHeaders<M>, usize), EnvelopeError>
    where
        M: DeserializeOwned,
    {
        let mut stream = serde_json::Deserializer::from_slice(slice).into_iter();

        let headers = match stream.next() {
            None => return Err(EnvelopeError::MissingHeader),
            Some(Err(error)) => return Err(EnvelopeError::InvalidHeader(error)),
            Some(Ok(headers)) => headers,
        };

        // Each header is terminated by a UNIX newline.
        Self::require_termination(slice, stream.byte_offset())?;

        Ok((headers, stream.byte_offset() + 1))
    }

    fn parse_items(bytes: &Bytes, mut offset: usize) -> Result<Items, EnvelopeError> {
        let mut items = Items::new();

        while offset < bytes.len() {
            let (item, item_size) = Self::parse_item(bytes.slice(offset..))?;
            offset += item_size;
            items.push(item);
        }

        Ok(items)
    }

    fn parse_item(bytes: Bytes) -> Result<(Item, usize), EnvelopeError> {
        let slice = bytes.as_ref();
        let mut stream = serde_json::Deserializer::from_slice(slice).into_iter();

        let headers: ItemHeaders = match stream.next() {
            None => return Err(EnvelopeError::UnexpectedEof),
            Some(Err(error)) => return Err(EnvelopeError::InvalidItemHeader(error)),
            Some(Ok(headers)) => headers,
        };

        // Each header is terminated by a UNIX newline.
        let headers_end = stream.byte_offset();
        Self::require_termination(slice, headers_end)?;

        // The last header does not require a trailing newline, so `payload_start` may point
        // past the end of the buffer.
        let payload_start = std::cmp::min(headers_end + 1, bytes.len());
        let payload_end = match headers.length {
            Some(len) => {
                let payload_end = payload_start + len as usize;
                if bytes.len() < payload_end {
                    // NB: `Bytes::slice` panics if the indices are out of range.
                    return Err(EnvelopeError::UnexpectedEof);
                }

                // Each payload is terminated by a UNIX newline.
                Self::require_termination(slice, payload_end)?;
                payload_end
            }
            None => match bytes[payload_start..].iter().position(|b| *b == b'\n') {
                Some(relative_end) => payload_start + relative_end,
                None => bytes.len(),
            },
        };

        let payload = bytes.slice(payload_start..payload_end);
        let item = Item { headers, payload };

        Ok((item, payload_end + 1))
    }

    fn require_termination(slice: &[u8], offset: usize) -> Result<(), EnvelopeError> {
        match slice.get(offset) {
            Some(&b'\n') | None => Ok(()),
            Some(_) => Err(EnvelopeError::MissingNewline),
        }
    }

    fn write<W>(&self, mut writer: W, buf: &[u8]) -> Result<(), EnvelopeError>
    where
        W: Write,
    {
        writer
            .write_all(buf)
            .map_err(EnvelopeError::PayloadIoFailed)
    }
}

#[cfg(test)]
mod tests {
    use relay_base_schema::project::ProjectId;

    use super::*;

    fn request_meta() -> RequestMeta {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
    }

    #[test]
    fn test_item_empty() {
        let item = Item::new(ItemType::Attachment);

        assert_eq!(item.payload(), Bytes::new());
        assert_eq!(item.len(), 0);
        assert!(item.is_empty());

        assert_eq!(item.content_type(), None);
    }

    #[test]
    fn test_item_set_payload() {
        let mut item = Item::new(ItemType::Event);

        let payload = Bytes::from(&br#"{"event_id":"3adcb99a1be84a5d8057f2eb9a0161ce"}"#[..]);
        item.set_payload(ContentType::Json, payload.clone());

        // Payload
        assert_eq!(item.payload(), payload);
        assert_eq!(item.len(), payload.len());
        assert!(!item.is_empty());

        // Meta data
        assert_eq!(item.content_type(), Some(&ContentType::Json));
    }

    #[test]
    fn test_item_set_header() {
        let mut item = Item::new(ItemType::Event);
        item.set_header("custom", 42u64);

        assert_eq!(item.get_header("custom"), Some(&Value::from(42u64)));
        assert_eq!(item.get_header("anything"), None);
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_item_set_routing_hint() {
        let uuid = Uuid::parse_str("8a4ab00f-fba2-4f7b-a164-b58199d55c95").unwrap();

        let mut item = Item::new(ItemType::Event);
        item.set_routing_hint(uuid);

        assert_eq!(item.routing_hint(), Some(uuid));
    }

    #[test]
    fn test_item_source_quantities() {
        let mut item = Item::new(ItemType::MetricBuckets);
        assert!(item.source_quantities().is_none());

        let source_quantities = SourceQuantities {
            transactions: 12,
            ..Default::default()
        };
        item.set_source_quantities(source_quantities);

        assert_eq!(item.source_quantities(), Some(source_quantities));
    }

    #[test]
    fn test_item_type_names() {
        assert_eq!(ItemType::Span.name(), "span");
        assert_eq!(ItemType::Unknown("test".to_owned()).name(), "unknown");
        assert_eq!(ItemType::Span.as_str(), "span");
        assert_eq!(ItemType::Unknown("test".to_owned()).as_str(), "test");
        assert_eq!(&ItemType::Span.to_string(), "span");
        assert_eq!(&ItemType::Unknown("test".to_owned()).to_string(), "test");
    }

    #[test]
    fn test_envelope_empty() {
        let event_id = EventId::new();
        let envelope = Envelope::from_request(Some(event_id), request_meta());

        assert_eq!(envelope.event_id(), Some(event_id));
        assert_eq!(envelope.len(), 0);
        assert!(envelope.is_empty());

        assert!(envelope.items().next().is_none());
    }

    #[test]
    fn test_envelope_add_item() {
        let event_id = EventId::new();
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());
        envelope.add_item(Item::new(ItemType::Attachment));

        assert_eq!(envelope.len(), 1);
        assert!(!envelope.is_empty());

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].ty(), &ItemType::Attachment);
    }

    #[test]
    fn test_envelope_take_item() {
        let event_id = EventId::new();
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());

        let mut item1 = Item::new(ItemType::Attachment);
        item1.set_filename("item1");
        envelope.add_item(item1);

        let mut item2 = Item::new(ItemType::Attachment);
        item2.set_filename("item2");
        envelope.add_item(item2);

        let taken = envelope
            .take_item_by(|item| item.ty() == &ItemType::Attachment)
            .expect("should return some item");

        assert_eq!(taken.filename(), Some("item1"));

        assert!(envelope
            .take_item_by(|item| item.ty() == &ItemType::Event)
            .is_none());
    }

    #[test]
    fn test_deserialize_envelope_empty() {
        // Without terminating newline after header
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}");
        let envelope = Envelope::parse_bytes(bytes).unwrap();

        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        assert_eq!(envelope.event_id(), Some(event_id));
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_request_meta() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@other.sentry.io/42\",\"client\":\"sentry/javascript\",\"version\":6,\"origin\":\"http://localhost/\",\"remote_addr\":\"127.0.0.1\",\"forwarded_for\":\"8.8.8.8\",\"user_agent\":\"sentry-cli/1.0\"}");
        let envelope = Envelope::parse_bytes(bytes).unwrap();
        let meta = envelope.meta();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@other.sentry.io/42"
            .parse()
            .unwrap();
        assert_eq!(*meta.dsn(), dsn);
        assert_eq!(meta.project_id(), Some(ProjectId::new(42)));
        assert_eq!(
            meta.public_key().as_str(),
            "e12d836b15bb49d7bbf99e64295d995b"
        );
        assert_eq!(meta.client(), Some("sentry/javascript"));
        assert_eq!(meta.version(), 6);
        assert_eq!(meta.origin(), Some(&"http://localhost/".parse().unwrap()));
        assert_eq!(meta.remote_addr(), Some("127.0.0.1".parse().unwrap()));
        assert_eq!(meta.forwarded_for(), "8.8.8.8");
        assert_eq!(meta.user_agent(), Some("sentry-cli/1.0"));
    }

    #[test]
    fn test_deserialize_envelope_empty_newline() {
        // With terminating newline after header
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n");
        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_empty_item_newline() {
        // With terminating newline after item payload
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"attachment\",\"length\":0}\n\
             \n\
             {\"type\":\"attachment\",\"length\":0}\n\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 2);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 0);
        assert_eq!(items[1].len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_empty_item_eof() {
        // Without terminating newline after item payload
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"attachment\",\"length\":0}\n\
             \n\
             {\"type\":\"attachment\",\"length\":0}\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 2);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 0);
        assert_eq!(items[1].len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_implicit_length() {
        // With terminating newline after item payload
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"attachment\"}\n\
             helloworld\n\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 10);
    }

    #[test]
    fn test_deserialize_envelope_implicit_length_eof() {
        // With item ending the envelope
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"attachment\"}\n\
             helloworld\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 10);
    }

    #[test]
    fn test_deserialize_envelope_implicit_length_empty_eof() {
        // Empty item with implicit length ending the envelope
        // Panic regression test.
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"attachment\"}\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_multiple_items() {
        // With terminating newline
        let bytes = Bytes::from(&b"\
            {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
            {\"type\":\"attachment\",\"length\":10,\"content_type\":\"text/plain\",\"filename\":\"hello.txt\"}\n\
            \xef\xbb\xbfHello\r\n\n\
            {\"type\":\"event\",\"length\":41,\"content_type\":\"application/json\",\"filename\":\"application.log\"}\n\
            {\"message\":\"hello world\",\"level\":\"error\"}\n\
        "[..]);

        let envelope = Envelope::parse_bytes(bytes).unwrap();

        assert_eq!(envelope.len(), 2);
        let items: Vec<_> = envelope.items().collect();

        assert_eq!(items[0].ty(), &ItemType::Attachment);
        assert_eq!(items[0].len(), 10);
        assert_eq!(
            items[0].payload(),
            Bytes::from(&b"\xef\xbb\xbfHello\r\n"[..])
        );
        assert_eq!(items[0].content_type(), Some(&ContentType::Text));

        assert_eq!(items[1].ty(), &ItemType::Event);
        assert_eq!(items[1].len(), 41);
        assert_eq!(
            items[1].payload(),
            Bytes::from("{\"message\":\"hello world\",\"level\":\"error\"}")
        );
        assert_eq!(items[1].content_type(), Some(&ContentType::Json));
        assert_eq!(items[1].filename(), Some("application.log"));
    }

    #[test]
    fn test_deserialize_envelope_unknown_item() {
        // With terminating newline after item payload
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"invalid_unknown\"}\n\
             helloworld\n\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 10);
    }

    #[test]
    fn test_deserialize_envelope_replay_recording() {
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"replay_recording\"}\n\
             helloworld\n\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);
        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].ty(), &ItemType::ReplayRecording);
    }

    #[test]
    fn test_deserialize_envelope_replay_video() {
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"replay_video\"}\n\
             helloworld\n\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);
        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].ty(), &ItemType::ReplayVideo);
    }

    #[test]
    fn test_deserialize_envelope_view_hierarchy() {
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
             {\"type\":\"attachment\",\"length\":44,\"content_type\":\"application/json\",\"attachment_type\":\"event.view_hierarchy\"}\n\
             {\"rendering_system\":\"compose\",\"windows\":[]}\n\
             ",
        );

        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 1);
        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].ty(), &ItemType::Attachment);
        assert_eq!(
            items[0].attachment_type(),
            Some(&AttachmentType::ViewHierarchy)
        );
    }

    #[test]
    fn test_parse_request_envelope() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}");
        let envelope = Envelope::parse_request(bytes, request_meta()).unwrap();
        let meta = envelope.meta();

        // This test asserts that all information from the envelope is overwritten with request
        // information. Note that the envelope's DSN points to "other.sentry.io", but all other
        // information matches the DSN.

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        assert_eq!(*meta.dsn(), dsn);
        assert_eq!(meta.project_id(), Some(ProjectId::new(42)));
        assert_eq!(
            meta.public_key().as_str(),
            "e12d836b15bb49d7bbf99e64295d995b"
        );
        assert_eq!(meta.client(), Some("sentry/client"));
        assert_eq!(meta.version(), 7);
        assert_eq!(meta.origin(), Some(&"http://origin/".parse().unwrap()));
        assert_eq!(meta.remote_addr(), Some("192.168.0.1".parse().unwrap()));
        assert_eq!(meta.forwarded_for(), "");
        assert_eq!(meta.user_agent(), Some("sentry/agent"));
    }

    #[test]
    fn test_parse_request_no_dsn() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}");
        let envelope = Envelope::parse_request(bytes, request_meta()).unwrap();
        let meta = envelope.meta();

        // DSN should be assumed from the request.
        assert_eq!(meta.dsn(), request_meta().dsn());
    }

    #[test]
    fn test_parse_request_sent_at() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\", \"sent_at\": \"1970-01-01T00:02:03Z\"}");
        let envelope = Envelope::parse_request(bytes, request_meta()).unwrap();
        let sent_at = envelope.sent_at().unwrap();

        // DSN should be assumed from the request.
        assert_eq!(sent_at.timestamp(), 123);
    }

    #[test]
    fn test_parse_request_sent_at_null() {
        let bytes =
            Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\", \"sent_at\": null}");
        let envelope = Envelope::parse_request(bytes, request_meta()).unwrap();
        assert!(envelope.sent_at().is_none());
    }

    #[test]
    fn test_parse_request_no_origin() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}");
        let envelope = Envelope::parse_request(bytes, request_meta()).unwrap();
        let meta = envelope.meta();

        // Origin validation should skip a missing origin.
        assert_eq!(meta.origin(), Some(&"http://origin/".parse().unwrap()));
    }

    #[test]
    #[should_panic(expected = "project id")]
    fn test_parse_request_validate_project() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/99\"}");
        Envelope::parse_request(bytes, request_meta()).unwrap();
    }

    #[test]
    #[should_panic(expected = "public key")]
    fn test_parse_request_validate_key() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:@sentry.io/42\"}");
        Envelope::parse_request(bytes, request_meta()).unwrap();
    }

    #[test]
    #[should_panic(expected = "origin")]
    fn test_parse_request_validate_origin() {
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\",\"origin\":\"http://localhost/\"}");
        Envelope::parse_request(bytes, request_meta()).unwrap();
    }

    #[test]
    fn test_serialize_envelope_empty() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let envelope = Envelope::from_request(Some(event_id), request_meta());

        let mut buffer = Vec::new();
        envelope.serialize(&mut buffer).unwrap();

        let stringified = String::from_utf8_lossy(&buffer);
        insta::assert_snapshot!(stringified, @r#"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","client":"sentry/client","version":7,"origin":"http://origin/","remote_addr":"192.168.0.1","user_agent":"sentry/agent"}
"#);
    }

    #[test]
    fn test_serialize_envelope_attachments() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());

        let mut item = Item::new(ItemType::Event);
        item.set_payload(
            ContentType::Json,
            "{\"message\":\"hello world\",\"level\":\"error\"}",
        );
        envelope.add_item(item);

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::Text, &b"Hello\r\n"[..]);
        item.set_filename("application.log");
        envelope.add_item(item);

        let mut buffer = Vec::new();
        envelope.serialize(&mut buffer).unwrap();

        let stringified = String::from_utf8_lossy(&buffer);
        insta::assert_snapshot!(stringified, @r#"
        {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","client":"sentry/client","version":7,"origin":"http://origin/","remote_addr":"192.168.0.1","user_agent":"sentry/agent"}
        {"type":"event","length":41,"content_type":"application/json"}
        {"message":"hello world","level":"error"}
        {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
        Hello

        "#);
    }

    #[test]
    fn test_split_envelope_none() {
        let mut envelope = Envelope::from_request(Some(EventId::new()), request_meta());
        envelope.add_item(Item::new(ItemType::Attachment));
        envelope.add_item(Item::new(ItemType::Attachment));

        // Does not split when no item matches.
        let split_opt = envelope.split_by(|item| item.ty() == &ItemType::Session);
        assert!(split_opt.is_none());
    }

    #[test]
    fn test_split_envelope_all() {
        let mut envelope = Envelope::from_request(Some(EventId::new()), request_meta());
        envelope.add_item(Item::new(ItemType::Session));
        envelope.add_item(Item::new(ItemType::Session));

        // Does not split when all items match.
        let split_opt = envelope.split_by(|item| item.ty() == &ItemType::Session);
        assert!(split_opt.is_none());
    }

    #[test]
    fn test_split_envelope_some() {
        let mut envelope = Envelope::from_request(Some(EventId::new()), request_meta());
        envelope.add_item(Item::new(ItemType::Session));
        envelope.add_item(Item::new(ItemType::Attachment));

        let split_opt = envelope.split_by(|item| item.ty() == &ItemType::Session);
        let split_envelope = split_opt.expect("split_by returns an Envelope");

        assert_eq!(split_envelope.len(), 1);
        assert_eq!(split_envelope.event_id(), envelope.event_id());

        // Matching items have moved into the split envelope.
        for item in split_envelope.items() {
            assert_eq!(item.ty(), &ItemType::Session);
        }

        for item in envelope.items() {
            assert_eq!(item.ty(), &ItemType::Attachment);
        }
    }
}
