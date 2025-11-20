use relay_profiling::ProfileType;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::AddAssign;
use uuid::Uuid;

use bytes::Bytes;
use relay_event_schema::protocol::{EventType, SpanId};
use relay_protocol::Value;
use relay_quotas::DataCategory;
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};

use crate::envelope::{AttachmentType, ContentType, EnvelopeError};
use crate::integrations::{Integration, LogsIntegration, SpansIntegration};

#[derive(Clone, Debug)]
pub struct Item {
    pub(super) headers: ItemHeaders,
    pub(super) payload: Bytes,
}

impl Item {
    /// Creates a new item with the given type.
    pub fn new(ty: ItemType) -> Self {
        Self {
            headers: ItemHeaders {
                ty,
                length: Some(0),
                item_count: None,
                attachment_type: None,
                content_type: None,
                filename: None,
                routing_hint: None,
                rate_limited: false,
                source_quantities: None,
                other: BTreeMap::new(),
                metrics_extracted: false,
                spans_extracted: false,
                sampled: true,
                fully_normalized: false,
                profile_type: None,
                platform: None,
                parent_id: None,
                meta_length: None,
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

    /// Parses an [`Item`] from raw bytes.
    pub fn parse(bytes: Bytes) -> Result<(Item, usize), EnvelopeError> {
        let slice = bytes.as_ref();
        let mut stream = serde_json::Deserializer::from_slice(slice).into_iter();

        let headers: ItemHeaders = match stream.next() {
            None => return Err(EnvelopeError::UnexpectedEof),
            Some(Err(error)) => return Err(EnvelopeError::InvalidItemHeader(error)),
            Some(Ok(headers)) => headers,
        };

        // Each header is terminated by a UNIX newline.
        let headers_end = stream.byte_offset();
        super::require_termination(slice, headers_end)?;

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
                super::require_termination(slice, payload_end)?;
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

    /// Returns the number used for counting towards rate limits and producing outcomes.
    ///
    /// For attachments, we count the number of bytes. Other items are counted as 1.
    pub fn quantities(&self) -> SmallVec<[(DataCategory, usize); 2]> {
        let item_count = self.item_count().unwrap_or(1) as usize;

        match self.ty() {
            ItemType::Event => smallvec![(DataCategory::Error, item_count)],
            ItemType::Transaction => smallvec![
                (DataCategory::Transaction, item_count),
                (DataCategory::TransactionIndexed, item_count),
            ],
            ItemType::Security | ItemType::RawSecurity => {
                smallvec![(DataCategory::Security, item_count)]
            }
            ItemType::Nel => smallvec![],
            ItemType::UnrealReport => smallvec![(DataCategory::Error, item_count)],
            ItemType::Attachment => smallvec![
                (DataCategory::Attachment, self.len().max(1)),
                (DataCategory::AttachmentItem, item_count),
            ],
            ItemType::Session | ItemType::Sessions => {
                smallvec![(DataCategory::Session, item_count)]
            }
            ItemType::Statsd | ItemType::MetricBuckets => smallvec![],
            ItemType::Log => smallvec![
                (DataCategory::LogByte, self.len().max(1)),
                (DataCategory::LogItem, item_count)
            ],
            ItemType::TraceMetric => smallvec![(DataCategory::TraceMetric, item_count)],
            ItemType::FormData => smallvec![],
            ItemType::UserReport => smallvec![(DataCategory::UserReportV2, item_count)],
            ItemType::UserReportV2 => smallvec![(DataCategory::UserReportV2, item_count)],
            ItemType::Profile => smallvec![
                (DataCategory::Profile, item_count),
                (DataCategory::ProfileIndexed, item_count)
            ],
            ItemType::ReplayEvent | ItemType::ReplayRecording | ItemType::ReplayVideo => {
                smallvec![(DataCategory::Replay, item_count)]
            }
            ItemType::ClientReport => smallvec![],
            ItemType::CheckIn => smallvec![(DataCategory::Monitor, item_count)],
            ItemType::Span => smallvec![
                (DataCategory::Span, item_count),
                (DataCategory::SpanIndexed, item_count),
            ],
            // NOTE: semantically wrong, but too expensive to parse.
            ItemType::ProfileChunk => match self.profile_type() {
                Some(ProfileType::Backend) => smallvec![(DataCategory::ProfileChunk, item_count)],
                Some(ProfileType::Ui) => smallvec![(DataCategory::ProfileChunkUi, item_count)],
                None => smallvec![],
            },
            ItemType::Integration => match self.integration() {
                Some(Integration::Logs(LogsIntegration::OtelV1 { .. })) => smallvec![
                    (DataCategory::LogByte, self.len().max(1)),
                    (DataCategory::LogItem, item_count),
                ],
                Some(Integration::Logs(LogsIntegration::VercelDrainLog { .. })) => smallvec![
                    (DataCategory::LogByte, self.len().max(1)),
                    (DataCategory::LogItem, item_count),
                ],
                Some(Integration::Spans(SpansIntegration::OtelV1 { .. })) => {
                    smallvec![
                        (DataCategory::Span, item_count),
                        (DataCategory::SpanIndexed, item_count),
                    ]
                }
                None => smallvec![],
            },
            ItemType::Unknown(_) => smallvec![],
        }
    }

    /// Returns `true` if this item's payload is empty.
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Returns the amount of items contained in the item body.
    ///
    /// An envelope item can hold multiple items of the same type by using an [`super::ItemContainer`].
    /// In that case the single envelope item represents multiple items of type [`Self::ty`].
    ///
    /// This method can be safely used to generate outcomes.
    pub fn item_count(&self) -> Option<u32> {
        match self.ty().can_support_container() {
            true => self.headers.item_count,
            false => None,
        }
    }

    /// Returns the content type of this item's payload.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn content_type(&self) -> Option<&ContentType> {
        self.headers.content_type.as_ref()
    }

    /// Returns the [`Integration`] the item belongs.
    pub fn integration(&self) -> Option<Integration> {
        if !matches!(self.ty(), ItemType::Integration) {
            return None;
        }

        match self.content_type() {
            Some(ContentType::Integration(integration)) => Some(*integration),
            _ => {
                // This is a bug which should never happen.
                debug_assert!(false, "integration item, but no integration content type");
                None
            }
        }
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

    /// Sets the payload of this envelope item without specifying a content-type.
    /// Use `set_payload` if you want to define a content-type for the payload.
    pub fn set_payload_without_content_type<B>(&mut self, payload: B)
    where
        B: Into<Bytes>,
    {
        let mut payload = payload.into();

        let length = std::cmp::min(u32::MAX as usize, payload.len());
        payload.truncate(length);

        self.headers.length = Some(length as u32);
        self.payload = payload;
    }

    /// Sets the payload and content-type of this envelope item. Use
    /// `set_payload_without_content_type` if you need to set the payload without a content-type.
    pub fn set_payload<B>(&mut self, content_type: ContentType, payload: B)
    where
        B: Into<Bytes>,
    {
        self.headers.content_type = Some(content_type);
        self.set_payload_without_content_type(payload);
    }

    /// Sets the payload, content-type and item count of this envelope item.
    pub fn set_payload_with_item_count<B>(
        &mut self,
        content_type: ContentType,
        payload: B,
        item_count: u32,
    ) where
        B: Into<Bytes>,
    {
        self.headers.content_type = Some(content_type);
        self.headers.item_count = Some(item_count);
        self.set_payload_without_content_type(payload);
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
    pub fn routing_hint(&self) -> Option<Uuid> {
        self.headers.routing_hint
    }

    /// Set the routing_hint of this item.
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

    /// Returns the contained source quantities.
    pub fn source_quantities(&self) -> Option<SourceQuantities> {
        self.headers.source_quantities
    }

    /// Sets new source quantities.
    pub fn set_source_quantities(&mut self, source_quantities: SourceQuantities) {
        self.headers.source_quantities = Some(source_quantities);
    }

    /// Returns the metrics extracted flag.
    pub fn metrics_extracted(&self) -> bool {
        self.headers.metrics_extracted
    }

    /// Sets the metrics extracted flag.
    pub fn set_metrics_extracted(&mut self, metrics_extracted: bool) {
        self.headers.metrics_extracted = metrics_extracted;
    }

    /// Returns the spans extracted flag.
    pub fn spans_extracted(&self) -> bool {
        self.headers.spans_extracted
    }

    /// Sets the spans extracted flag.
    pub fn set_spans_extracted(&mut self, spans_extracted: bool) {
        self.headers.spans_extracted = spans_extracted;
    }

    /// Returns the fully normalized flag.
    pub fn fully_normalized(&self) -> bool {
        self.headers.fully_normalized
    }

    /// Sets the fully normalized flag.
    pub fn set_fully_normalized(&mut self, fully_normalized: bool) {
        self.headers.fully_normalized = fully_normalized;
    }

    /// Returns the associated platform.
    ///
    /// Note: this is currently only used for [`ItemType::ProfileChunk`].
    pub fn platform(&self) -> Option<&str> {
        self.headers.platform.as_deref()
    }

    /// Returns the associated profile type of a profile chunk.
    ///
    /// This primarily uses the profile type set via [`Self::set_profile_type`],
    /// but if not set, it infers the [`ProfileType`] from the [`Self::platform`].
    ///
    /// Returns `None`, if neither source is available.
    pub fn profile_type(&self) -> Option<ProfileType> {
        self.headers
            .profile_type
            .or_else(|| self.platform().map(ProfileType::from_platform))
    }

    /// Set the profile type of the profile chunk.
    pub fn set_profile_type(&mut self, profile_type: ProfileType) {
        self.headers.profile_type = Some(profile_type);
    }

    /// Gets the `sampled` flag.
    pub fn sampled(&self) -> bool {
        self.headers.sampled
    }

    /// Sets the `sampled` flag.
    pub fn set_sampled(&mut self, sampled: bool) {
        self.headers.sampled = sampled;
    }

    /// Returns the length of the item.
    pub fn meta_length(&self) -> Option<u32> {
        self.headers.meta_length
    }

    /// Returns the parent entity that this item is associated with, if any.
    ///
    /// Only applicable if the item is an attachment.
    pub fn parent_id(&self) -> Option<&ParentId> {
        self.headers.parent_id.as_ref()
    }

    /// Sets the length of the optional meta segment.
    pub fn set_meta_length(&mut self, meta_length: u32) {
        self.headers.meta_length = Some(meta_length);
    }

    /// Sets the parent entity that this item is associated with.
    pub fn set_parent_id(&mut self, parent_id: ParentId) {
        self.headers.parent_id = Some(parent_id);
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
            | ItemType::UnrealReport
            | ItemType::UserReportV2 => true,

            // Attachments are only event items if they are crash reports or if they carry partial
            // event payloads. Plain attachments never create event payloads.
            ItemType::Attachment => {
                match self.attachment_type().unwrap_or(&AttachmentType::default()) {
                    AttachmentType::AppleCrashReport
                    | AttachmentType::Minidump
                    | AttachmentType::EventPayload
                    | AttachmentType::Prosperodump
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
            | ItemType::ClientReport
            | ItemType::ReplayEvent
            | ItemType::ReplayRecording
            | ItemType::ReplayVideo
            | ItemType::Profile
            | ItemType::CheckIn
            | ItemType::Span
            | ItemType::Nel
            | ItemType::Log
            | ItemType::TraceMetric
            | ItemType::ProfileChunk => false,

            // For now integrations can not create events, we may need to revisit this in the
            // future and break down different types of integrations here, similar to attachments.
            ItemType::Integration => false,

            // The unknown item type can observe any behavior, most likely there are going to be no
            // item types added that create events.
            ItemType::Unknown(_) => false,
        }
    }

    /// Determines whether the given item requires an event with identifier.
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
            ItemType::ClientReport => false,
            ItemType::ReplayRecording => false,
            ItemType::ReplayVideo => false,
            ItemType::Profile => true,
            ItemType::CheckIn => false,
            ItemType::Span => false,
            ItemType::Log => false,
            ItemType::TraceMetric => false,
            ItemType::ProfileChunk => false,
            ItemType::Integration => false,

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

    /// Determines if this item is an `ItemContainer` based on its content type.
    pub fn is_container(&self) -> bool {
        self.content_type().is_some_and(ContentType::is_container)
    }
}

pub type Items = SmallVec<[Item; 3]>;
pub type ItemIter<'a> = std::slice::Iter<'a, Item>;
pub type ItemIterMut<'a> = std::slice::IterMut<'a, Item>;

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
    /// Integrations are a vendor specific set of endpoints providing integrations with external
    /// systems, standards and vendors.
    ///
    /// Relay stores payloads received from integration endpoints in envelope items of this type.
    ///
    /// This is an [internal item type](`Self::is_internal`) and must be converted within the same
    /// instance of Relay.
    Integration,
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
        matches!(self, ItemType::Statsd | ItemType::MetricBuckets)
    }

    /// Returns `true` if the item is a Relay internal item type.
    ///
    /// Internal items are not allowed to be forwarded or sent upstream and will be rejected by
    /// other Relays. Internal items must be converted to Sentry native items before forwarded.
    pub fn is_internal(&self) -> bool {
        matches!(self, ItemType::Integration)
    }

    /// Returns `true` if the specified [`ItemType`] can occur in an [`super::ItemContainer`].
    ///
    /// Note: this will return `true` even if Relay does not currently support item containers
    /// for this item type to guarantee forward compatibility.
    ///
    /// Every item which will eventually be turned into an event, cannot support containers,
    /// due to envelope limits.
    /// Attachments and other binary items will also never support the container format.
    fn can_support_container(&self) -> bool {
        match self {
            ItemType::Event => false,
            ItemType::Transaction => false,
            ItemType::Security => false,
            ItemType::Attachment => false,
            ItemType::FormData => false,
            ItemType::RawSecurity => false,
            ItemType::Nel => false,
            ItemType::UnrealReport => false,
            ItemType::UserReport => false,
            ItemType::Session => true,
            ItemType::Sessions => true,
            ItemType::Statsd => true,
            ItemType::MetricBuckets => true,
            ItemType::ClientReport => true,
            ItemType::Profile => true,
            ItemType::ReplayEvent => false,
            ItemType::ReplayRecording => false,
            ItemType::ReplayVideo => false,
            ItemType::CheckIn => true,
            ItemType::Log => true,
            ItemType::TraceMetric => true,
            ItemType::Span => true,
            ItemType::UserReportV2 => false,
            ItemType::ProfileChunk => true,
            ItemType::Integration => false,
            ItemType::Unknown(_) => true,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ItemHeaders {
    /// The type of the item.
    #[serde(rename = "type")]
    ty: ItemType,

    /// Content length of the item.
    ///
    /// Can be omitted if the item does not contain new lines. In this case, the item payload is
    /// parsed until the first newline is encountered.
    #[serde(skip_serializing_if = "Option::is_none")]
    length: Option<u32>,

    /// The amount of contained items.
    ///
    /// This header is required for all items that are transmitted in an envelope [`super::ItemContainer`].
    /// The amount specified must match the amount of items contained in the container exactly.
    ///
    /// Failing to specify the count or a mismatching count will be treated as an invalid envelope.
    #[serde(skip_serializing_if = "Option::is_none")]
    item_count: Option<u32>,

    /// If this is an attachment item, this may contain the attachment type.
    #[serde(skip_serializing_if = "Option::is_none")]
    attachment_type: Option<AttachmentType>,

    /// Content type of the payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<ContentType>,

    /// If this is an attachment item, this may contain the original file name.
    #[serde(skip_serializing_if = "Option::is_none")]
    filename: Option<String>,

    /// The platform this item was produced for.
    ///
    /// Currently only used for [`ItemType::ProfileChunk`].
    /// It contains the same platform as specified in the profile chunk payload,
    /// hoisted into the header to be able to determine the correct data category.
    ///
    /// This is currently considered optional for profile chunks, but may change
    /// to required in the future.
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<String>,

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

    /// Flag indicating if metrics have already been extracted from the item.
    ///
    /// In order to only extract metrics once from an item while through a
    /// chain of Relays, a Relay that extracts metrics from an item (typically
    /// the first Relay) MUST set this flat to true so that upstream Relays do
    /// not extract the metric again causing double counting of the metric.
    #[serde(default, skip_serializing_if = "is_false")]
    metrics_extracted: bool,

    /// Whether or not spans and span metrics have been extracted from a transaction.
    ///
    /// This header is set to `true` after both span extraction and span metrics extraction,
    /// and can be used to skip extraction.
    ///
    /// NOTE: This header is also set to `true` for transactions that are themselves extracted
    /// from spans (the opposite direction), to prevent going in circles.
    #[serde(default, skip_serializing_if = "is_false")]
    spans_extracted: bool,

    /// Whether the event has been _fully_ normalized.
    ///
    /// If the event has been partially normalized, this flag is false. By
    /// default, all Relays run some normalization.
    ///
    /// Currently only used for events.
    #[serde(default, skip_serializing_if = "is_false")]
    fully_normalized: bool,

    /// `false` if the sampling decision is "drop".
    ///
    /// In the most common use case, the item is dropped when the sampling decision is "drop".
    /// For profiles with the feature enabled, however, we keep all profile items and mark the ones
    /// for which the transaction was dropped as `sampled: false`.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    sampled: bool,

    /// Tracks whether the item is a backend or ui profile chunk.
    ///
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    profile_type: Option<ProfileType>,

    /// Content length of an optional meta segment that might be contained in the item.
    ///
    /// For the time being such an meta segment is only present for span attachments.
    #[serde(skip_serializing_if = "Option::is_none")]
    meta_length: Option<u32>,

    /// Parent entity that this item is associated with, if any.
    ///
    /// For the time being only applicable if the item is a span-attachment.
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    parent_id: Option<ParentId>,

    /// Other attributes for forward compatibility.
    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

/// Container for item quantities that the item was derived from.
///
/// For example a metric bucket may be derived and aggregated from multiple transactions.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceQuantities {
    /// Transaction quantity.
    pub transactions: usize,
    /// Spans quantity.
    pub spans: usize,
    /// Profile quantity.
    pub profiles: usize,
    /// Total number of buckets.
    pub buckets: usize,
}

impl AddAssign for SourceQuantities {
    fn add_assign(&mut self, other: Self) {
        let Self {
            transactions,
            spans,
            profiles,
            buckets,
        } = self;
        *transactions += other.transactions;
        *spans += other.spans;
        *profiles += other.profiles;
        *buckets += other.buckets;
    }
}

fn is_false(val: &bool) -> bool {
    !*val
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

/// Parent identifier for an attachment-v2.
///
/// Attachments can be associated with different types of parent entities (only spans for now).
///
/// SpanId(None) indicates that the item is a span-attachment that is associated with no specific
/// span.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentId {
    SpanId(Option<SpanId>),
}

#[cfg(test)]
mod tests {
    use crate::integrations::OtelFormat;

    use super::*;

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
    fn test_internal_content_type_does_parse() {
        let (item, _) = Item::parse(Bytes::from_static(concat!(
            r#"{"type":"attachment","content_type":"application/vnd.sentry.integration.otel.logs+json","length":5}"#,
            "\n",
            "12345"
        ).as_bytes()))
        .unwrap();

        assert_eq!(
            item.content_type(),
            Some(&ContentType::Integration(Integration::Logs(
                LogsIntegration::OtelV1 {
                    format: OtelFormat::Json
                }
            )))
        );
        assert_eq!(item.integration(), None);
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
}
