use relay_profiling::ProfileType;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::AddAssign;
use uuid::Uuid;

#[cfg(test)]
use crate::managed::Counted;
use bytes::Bytes;
use relay_event_schema::protocol::{EventType, SpanId};
#[cfg(test)]
use relay_quotas::DataCategory;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::envelope::{AttachmentType, ContentType, EnvelopeError};
use crate::integrations::Integration;
use crate::utils::DebugBytes;

/// A payload type that can be selected from an [`Item`].
///
/// Borrowed extraction and `TryFrom<Item>` must accept the same variant.
pub trait ItemPayload: TryFrom<Item, Error = Item> {
    fn from_item_ref(item: &Item) -> Option<&Self>;

    /// Returns the declared `item_count` header when supported by this payload type.
    fn item_count(&self) -> Option<u32> {
        None
    }
}

/// Payload types that can be written as item containers.
pub trait ContainerPayload: ItemPayload + std::ops::DerefMut<Target = ItemData> {
    /// Sets the payload, its content type, and its declared container count.
    fn set_payload_with_item_count(
        &mut self,
        content_type: ContentType,
        payload: impl Into<Bytes>,
        item_count: u32,
    );
}

/// Raw headers and payload shared by all item types.
#[derive(Clone)]
pub struct ItemData {
    pub(super) headers: CommonItemHeaders,
    pub(super) payload: Bytes,
}

impl ItemData {
    /// Creates empty storage for a serialized item.
    fn new() -> Self {
        Self {
            headers: CommonItemHeaders {
                stored_key: None,
                rate_limited: false,
                source_quantities: None,
                routing_hint: None,
                inner: BTreeMap::new(),
            },
            payload: Bytes::new(),
        }
    }

    /// Returns the length of this item's payload.
    pub fn len(&self) -> usize {
        self.payload.len()
    }
}

impl Item {
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

        let length = headers
            .try_get::<usize>(ItemHeaderKey::Length)
            .map_err(EnvelopeError::InvalidItemHeader)?;
        let payload_end = match length {
            Some(len) => {
                let payload_end = payload_start
                    .checked_add(len)
                    .ok_or(EnvelopeError::UnexpectedEof)?;

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
        let ItemHeaders { ty, data: headers } = headers;
        let item = Item::from_parts(ty, ItemData { headers, payload });

        Ok((item, payload_end + 1))
    }
}

impl ItemData {
    /// Returns `true` if this item's payload is empty.
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

impl TransactionItem {
    /// Returns the declared number of embedded spans, or `None` for an absent or invalid header.
    pub fn span_count(&self) -> Option<u32> {
        self.headers.get(ItemHeaderKey::SpanCount)
    }

    /// Counts embedded spans using the header, falling back to scanning the `spans` payload array.
    /// Invalid payloads count as zero.
    pub fn count_embedded_spans(&self) -> usize {
        #[derive(serde::Deserialize)]
        struct PartialEvent {
            spans: crate::utils::SeqCount,
        }

        self.span_count()
            .map(|count| count as usize)
            .unwrap_or_else(|| {
                relay_statsd::metric!(timer(crate::statsd::RelayTimers::CheckNestedSpans), {
                    serde_json::from_slice::<PartialEvent>(&self.payload())
                        .map_or(0, |event| event.spans.0)
                })
            })
    }

    /// Sets the number of spans in the transaction payload.
    pub fn set_span_count(&mut self, value: Option<u32>) {
        self.headers.set_or_remove(ItemHeaderKey::SpanCount, value);
    }
}

impl ItemData {
    /// Returns the content type of this item's payload.
    pub fn content_type(&self) -> Option<ContentType> {
        self.headers.get(ItemHeaderKey::ContentType)
    }

    /// Returns the raw (unparsed) content type, as specified by the SDK.
    pub fn raw_content_type(&self) -> Option<&str> {
        self.headers.get(ItemHeaderKey::ContentType)
    }

    /// Sets the raw content type, overriding the one specified by the SDK.
    pub fn set_raw_content_type<S>(&mut self, content_type: S)
    where
        S: Into<String>,
    {
        self.headers
            .set(ItemHeaderKey::ContentType, content_type.into());
    }

    /// Sets the content type if there isn't already one set.
    pub fn set_default_content_type(&mut self, content_type: ContentType) {
        if !self.headers.contains(ItemHeaderKey::ContentType) {
            self.headers.set(ItemHeaderKey::ContentType, content_type);
        }
    }
}

impl IntegrationItem {
    /// Returns the integration named by the content-type header, if present and valid.
    pub fn integration_header(&self) -> Option<Integration> {
        match self.content_type() {
            Some(ContentType::Integration(integration)) => Some(integration),
            _ => None,
        }
    }

    /// Returns the integration, asserting that internally constructed items are valid.
    pub fn integration(&self) -> Option<Integration> {
        let integration = self.integration_header();
        debug_assert!(
            integration.is_some(),
            "integration item, but no integration content type"
        );
        integration
    }
}

impl AttachmentItem {
    /// Returns the attachment type.
    pub fn attachment_type(&self) -> Option<AttachmentType> {
        if let Some(ty) = self.headers.get(ItemHeaderKey::AttachmentType) {
            return Some(ty);
        }

        // Unfortunately when the switch protocol was decided on, it was missed to assign it a new
        // attachment type, that's why we have to infer it here from the filename and contents.
        if self.filename() == Some(crate::constants::NNSWITCH_DYING_MESSAGE_FILENAME)
            && self
                .payload
                .starts_with(crate::constants::NNSWITCH_SENTRY_MAGIC)
        {
            return Some(AttachmentType::NintendoSwitchDyingMessage);
        }

        None
    }

    /// Sets the attachment type of this item.
    pub fn set_attachment_type(&mut self, attachment_type: AttachmentType) {
        self.headers
            .set(ItemHeaderKey::AttachmentType, attachment_type);
    }
}

impl ItemData {
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

        self.headers.set(ItemHeaderKey::Length, length);
        self.payload = payload;
    }

    /// Sets the payload and content-type of this envelope item. Use
    /// `set_payload_without_content_type` if you need to set the payload without a content-type.
    pub fn set_payload<B>(&mut self, content_type: ContentType, payload: B)
    where
        B: Into<Bytes>,
    {
        self.headers.set(ItemHeaderKey::ContentType, content_type);
        self.set_payload_without_content_type(payload);
    }

    /// Sets the payload, content-type and item count of this envelope item.
    fn set_payload_with_item_count<B>(
        &mut self,
        content_type: ContentType,
        payload: B,
        item_count: u32,
    ) where
        B: Into<Bytes>,
    {
        self.headers
            .set(ItemHeaderKey::ContentType, content_type)
            .set(ItemHeaderKey::ItemCount, item_count);
        self.set_payload_without_content_type(payload);
    }
}

impl AttachmentItem {
    /// Returns the attachment file name.
    pub fn filename(&self) -> Option<&str> {
        self.headers.get(ItemHeaderKey::Filename)
    }

    /// Sets the file name of this item.
    pub fn set_filename<S>(&mut self, filename: S)
    where
        S: Into<String>,
    {
        self.headers.set(ItemHeaderKey::Filename, filename.into());
    }

    /// Returns the objectstore key for a stored attachment.
    pub fn stored_key(&self) -> Option<&str> {
        self.headers.stored_key.as_deref()
    }

    /// Sets the objectstore key of this attachment item.
    pub fn set_stored_key(&mut self, stored_key: String) {
        self.headers.stored_key = Some(stored_key);
    }
}

impl ItemData {
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

    /// Returns the fully normalized flag.
    pub fn fully_normalized(&self) -> bool {
        self.headers
            .get(ItemHeaderKey::FullyNormalized)
            .unwrap_or_default()
    }

    /// Sets the fully normalized flag.
    pub fn set_fully_normalized(&mut self, fully_normalized: bool) {
        self.headers
            .set(ItemHeaderKey::FullyNormalized, fully_normalized);
    }
}

impl ProfileItem {
    /// Returns the associated platform.
    pub fn platform(&self) -> Option<&str> {
        self.headers.get(ItemHeaderKey::Platform)
    }

    /// Set the associated platform.
    pub fn set_platform(&mut self, platform: String) {
        self.headers.set(ItemHeaderKey::Platform, platform);
    }

    /// Infers the [`ProfileType`] from [`Self::platform`].
    pub fn profile_type(&self) -> Option<ProfileType> {
        self.platform().map(ProfileType::from_platform)
    }

    /// Gets the profile sampling flag.
    pub fn sampled(&self) -> bool {
        self.headers.get(ItemHeaderKey::Sampled).unwrap_or(true)
    }

    /// Sets the `sampled` flag.
    pub fn set_sampled(&mut self, sampled: bool) {
        self.headers.set(ItemHeaderKey::Sampled, sampled);
    }
}

impl ProfileChunkItem {
    /// Returns the associated platform.
    pub fn platform(&self) -> Option<&str> {
        self.headers.get(ItemHeaderKey::Platform)
    }

    /// Set the associated platform.
    pub fn set_platform(&mut self, platform: String) {
        self.headers.set(ItemHeaderKey::Platform, platform);
    }

    /// Infers the [`ProfileType`] from [`Self::platform`].
    pub fn profile_type(&self) -> Option<ProfileType> {
        self.platform().map(ProfileType::from_platform)
    }

    /// Returns the length of the metadata segment for a Perfetto profile chunk.
    pub fn meta_length(&self) -> Option<u32> {
        self.headers.get(ItemHeaderKey::MetaLength)
    }
}

impl AttachmentItem {
    /// Returns the length of the optional attachment metadata segment.
    pub fn meta_length(&self) -> Option<u32> {
        self.headers.get(ItemHeaderKey::MetaLength)
    }

    /// Sets the length of the optional meta segment.
    pub fn set_meta_length(&mut self, meta_length: u32) {
        self.headers.set(ItemHeaderKey::MetaLength, meta_length);
    }

    /// Sets the original payload length for an attachment with [`ContentType::AttachmentRef`].
    pub fn set_attachment_length(&mut self, original_length: usize) {
        debug_assert!(self.is_attachment_ref());
        self.headers
            .set(ItemHeaderKey::AttachmentLength, original_length);
    }
}

impl ItemData {
    /// Retrieves the sentry release from an item header.
    pub fn sentry_release(&self) -> Option<&str> {
        self.headers.get(ItemHeaderKey::SentryRelease)
    }

    /// Sets the sentry release on an item header.
    pub fn set_sentry_release(&mut self, release: String) {
        self.headers.set(ItemHeaderKey::SentryRelease, release);
    }

    /// Retrieves the sentry environment from an item header.
    pub fn sentry_environment(&self) -> Option<&str> {
        self.headers.get(ItemHeaderKey::SentryEnvironment)
    }

    /// Sets the sentry environment on an item header.
    pub fn set_sentry_environment(&mut self, environment: String) {
        self.headers
            .set(ItemHeaderKey::SentryEnvironment, environment);
    }
}

impl AttachmentItem {
    /// Returns the parent entity associated with this attachment, if any.
    pub fn parent_id(&self) -> Option<ParentId> {
        // Currently only `span_id` supported in `ParentId`.
        let span_id = self.headers.get(ItemHeaderKey::SpanId)?;
        Some(ParentId::SpanId(span_id))
    }

    /// Sets the parent entity that this item is associated with.
    pub fn set_parent_id(&mut self, parent_id: Option<ParentId>) {
        let span_id = parent_id.map(|p| match p {
            ParentId::SpanId(span_id) => span_id,
        });
        self.headers.set_or_remove(ItemHeaderKey::SpanId, span_id);
    }

    /// Returns `true` if this item is an attachment with AttachmentV2 content type.
    fn is_attachment_v2(&self) -> bool {
        self.content_type() == Some(ContentType::TraceAttachment)
    }

    /// Returns `true` if this item is a V2 attachment owned by spans.
    pub fn is_span_attachment(&self) -> bool {
        self.is_attachment_v2() && matches!(self.parent_id(), Some(ParentId::SpanId(_)))
    }

    /// Returns `true` if this item is a V2 attachment without any span/log/etc. association.
    pub fn is_trace_attachment(&self) -> bool {
        self.is_attachment_v2() && self.parent_id().is_none()
    }

    /// Returns `true` if this item is an attachment placeholder.
    pub fn is_attachment_ref(&self) -> bool {
        self.content_type() == Some(ContentType::AttachmentRef)
    }

    /// Returns the [`AttachmentParentType`] of an attachment.
    ///
    /// For standard attachments (V1) always returns [`AttachmentParentType::Event`].
    pub fn attachment_parent_type(&self) -> AttachmentParentType {
        let is_trace_attachment = self.content_type() == Some(ContentType::TraceAttachment);

        if is_trace_attachment {
            match self.parent_id() {
                Some(ParentId::SpanId(_)) => AttachmentParentType::Span,
                None => AttachmentParentType::Trace,
            }
        } else {
            AttachmentParentType::Event
        }
    }

    /// Returns the attachment payload size.
    ///
    /// - For trace attachments, returns only the size of the actual payload, excluding the attachment meta.
    /// - For attachment placeholders, returns the size represented by the placeholder.
    /// - For Attachment, returns the size of entire payload.
    ///
    /// **Note:** This relies on the `meta_length` header which might not be correct as such this
    /// is best effort.
    pub fn attachment_body_size(&self) -> usize {
        if self.is_attachment_v2() {
            self.len()
                .saturating_sub(self.meta_length().unwrap_or(0) as usize)
        } else if self.is_attachment_ref() {
            self.headers
                .get(ItemHeaderKey::AttachmentLength)
                .unwrap_or(0)
        } else {
            self.len()
        }
        .max(1)
    }

    /// Whether this attachment contains data used to build an event.
    pub fn carries_event_payload(&self) -> bool {
        match self.attachment_type() {
            Some(
                AttachmentType::Minidump
                | AttachmentType::AppleCrashReport
                | AttachmentType::EventPayload
                | AttachmentType::Prosperodump
                | AttachmentType::Breadcrumbs
                | AttachmentType::NvGpuDump
                | AttachmentType::NintendoSwitchDyingMessage,
            ) => true,
            Some(
                AttachmentType::Attachment
                | AttachmentType::UnrealContext
                | AttachmentType::UnrealLogs
                | AttachmentType::ViewHierarchy
                | AttachmentType::Flamegraph
                | AttachmentType::NvShaderDebug,
            )
            | None => false,
        }
    }
}

impl Item {
    /// Returns `true` if this item contains a payload which is unknown to this version of Relay.
    pub fn is_unknown(&self) -> bool {
        match self {
            Self::Unknown(_) => true,
            Self::Attachment(item) => item
                .headers
                .try_get::<AttachmentType>(ItemHeaderKey::AttachmentType)
                .is_err(),
            _ => false,
        }
    }

    /// Whether this item contains data used to build an event.
    pub fn carries_event_payload(&self) -> bool {
        match self {
            Self::Event(_)
            | Self::Transaction(_)
            | Self::Security(_)
            | Self::RawSecurity(_)
            | Self::UnrealReport(_)
            | Self::UserReportV2(_) => true,
            Self::Attachment(item) => item.carries_event_payload(),
            Self::FormData(_)
            | Self::UserReport(_)
            | Self::Session(_)
            | Self::Sessions(_)
            | Self::MetricBuckets(_)
            | Self::ClientReport(_)
            | Self::Profile(_)
            | Self::ReplayEvent(_)
            | Self::ReplayRecording(_)
            | Self::ReplayVideo(_)
            | Self::CheckIn(_)
            | Self::Log(_)
            | Self::TraceMetric(_)
            | Self::Span(_)
            | Self::ProfileChunk(_)
            | Self::Integration(_)
            | Self::Unknown(_) => false,
        }
    }

    /// Whether error processing receives this item if earlier processors leave it behind.
    pub fn routes_with_event(&self) -> bool {
        match self {
            Self::Event(_)
            | Self::Transaction(_)
            | Self::Security(_)
            | Self::Attachment(_)
            | Self::RawSecurity(_)
            | Self::UnrealReport(_)
            | Self::UserReportV2(_)
            | Self::FormData(_)
            | Self::UserReport(_)
            | Self::Profile(_)
            | Self::ReplayEvent(_) => true,
            Self::Session(_)
            | Self::Sessions(_)
            | Self::MetricBuckets(_)
            | Self::ClientReport(_)
            | Self::ReplayRecording(_)
            | Self::ReplayVideo(_)
            | Self::CheckIn(_)
            | Self::Log(_)
            | Self::TraceMetric(_)
            | Self::Span(_)
            | Self::ProfileChunk(_)
            | Self::Integration(_)
            | Self::Unknown(_) => false,
        }
    }
}

impl ItemData {
    /// Whether the content-type header names a container format.
    pub fn is_container(&self) -> bool {
        self.content_type().is_some_and(ContentType::is_container)
    }

    /// Sets arbitrary headers for tests, including invalid and misplaced values.
    #[cfg(test)]
    pub(crate) fn set_test_header<T: Serialize>(&mut self, key: ItemHeaderKey, value: Option<T>) {
        self.headers.set_or_remove(key, value);
    }

    fn debug_with_type(&self, f: &mut fmt::Formatter<'_>, ty: &ItemType) -> fmt::Result {
        f.debug_struct("Item")
            .field(
                "headers",
                &ItemHeadersRef {
                    ty,
                    data: &self.headers,
                },
            )
            .field("payload", &DebugBytes(&self.payload))
            .finish()
    }
}

impl fmt::Debug for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Item")
            .field("headers", &self.wire_headers())
            .field("payload", &DebugBytes(&self.payload))
            .finish()
    }
}

pub type Items = SmallVec<[Item; 3]>;
pub type ItemIter<'a> = std::slice::Iter<'a, Item>;
pub type ItemIterMut<'a> = std::slice::IterMut<'a, Item>;

/// An envelope item with an unparsed payload.
///
/// `TypedItems` generates the payload structs and wire tags.
/// `#[item(item_count)]` enables reading the `item_count` header.
/// `#[item(container)]` additionally enables writing item containers.
#[derive(Clone, relay_server_macros::TypedItems)]
pub enum Item {
    /// Event payload encoded in JSON.
    Event(EventItem),
    /// Transaction event payload encoded in JSON.
    Transaction(TransactionItem),
    /// Security report event payload encoded in JSON.
    Security(SecurityItem),
    /// Raw payload of an arbitrary attachment.
    Attachment(AttachmentItem),
    /// Multipart form data collected into a stream of JSON tuples.
    FormData(FormDataItem),
    /// Security report as sent by the browser in JSON.
    RawSecurity(RawSecurityItem),
    /// Raw compressed UE4 crash report.
    UnrealReport(UnrealReportItem),
    /// User feedback encoded as JSON.
    UserReport(UserReportItem),
    /// Session update data.
    #[item(item_count)]
    Session(SessionItem),
    /// Aggregated session data.
    #[item(item_count)]
    Sessions(SessionsItem),
    /// Buckets of preaggregated metrics encoded as JSON.
    #[item(item_count)]
    MetricBuckets(MetricBucketsItem),
    /// Client internal report (eg: outcomes).
    #[item(item_count)]
    ClientReport(ClientReportItem),
    /// Profile event payload encoded as JSON.
    #[item(item_count)]
    Profile(ProfileItem),
    /// Replay metadata and breadcrumb payload.
    ReplayEvent(ReplayEventItem),
    /// Replay Recording data.
    ReplayRecording(ReplayRecordingItem),
    /// Replay Video data.
    ReplayVideo(ReplayVideoItem),
    /// Monitor check-in encoded as JSON.
    #[item(item_count)]
    CheckIn(CheckInItem),
    /// A log for the log product, not internal logs.
    #[item(container)]
    Log(LogItem),
    /// A trace metric item.
    #[item(container)]
    TraceMetric(TraceMetricItem),
    /// A standalone span.
    #[item(container)]
    Span(SpanItem),
    /// UserReport as an Event
    UserReportV2(UserReportV2Item),
    /// ProfileChunk is a chunk of a profiling session.
    #[item(item_count)]
    ProfileChunk(ProfileChunkItem),
    /// Integrations are a vendor specific set of endpoints providing integrations with external
    /// systems, standards and vendors.
    ///
    /// Relay stores payloads received from integration endpoints in envelope items of this type.
    ///
    /// This is an [internal item type](`ItemType::is_internal`) and must be converted within the same
    /// instance of Relay.
    Integration(IntegrationItem),
    /// A new item type that is yet unknown by this version of Relay.
    ///
    /// By default, items of this type are forwarded without modification. Processing Relays and
    /// Relays explicitly configured to do so will instead drop those items. This allows
    /// forward-compatibility with new item types where we expect outdated Relays.
    #[item(item_count)]
    Unknown(UnknownItem),
    // Keep `Unknown` last in the list. Add new items above `Unknown`.
}

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

/// AN item header stored in [`ItemHeaders`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemHeaderKey {
    /// Content length of the item.
    ///
    /// Can be omitted if the item does not contain new lines. In this case, the item payload is
    /// parsed until the first newline is encountered.
    Length,
    /// The amount of contained items.
    ///
    /// This header is required for all items that are transmitted in an envelope [`super::ItemContainer`].
    /// The amount specified must match the amount of items contained in the container exactly.
    ///
    /// Failing to specify the count or a mismatching count will be treated as an invalid envelope.
    ItemCount,
    /// If this is an attachment item, this may contain the attachment type.
    AttachmentType,
    /// Content type of the payload.
    ContentType,
    /// If this is an attachment item, this may contain the original file name.
    Filename,
    /// The platform this item was produced for.
    ///
    /// Used for [`ItemType::Profile`] and [`ItemType::ProfileChunk`].
    /// It contains the same platform as specified in the profile chunk payload,
    /// hoisted into the header to be able to determine the correct data category.
    ///
    /// This is currently considered optional for profile chunks, but may change
    /// to required in the future.
    Platform,
    /// The number of spans in the `event.spans` array.
    ///
    /// Should never be set except for transaction items.
    ///
    /// When a transaction is dropped before spans were extracted from a transaction,
    /// this number is used to emit correct outcomes for the spans category.
    ///
    /// This number does *not* count the transaction itself.
    SpanCount,
    /// Whether the event has been _fully_ normalized.
    ///
    /// If the event has been partially normalized, this flag is false. By
    /// default, all Relays run some normalization.
    ///
    /// Currently only used for events.
    FullyNormalized,
    /// `false` if the sampling decision is "drop".
    ///
    /// In the most common use case, the item is dropped when the sampling decision is "drop".
    /// For profiles with the feature enabled, however, we keep all profile items and mark the ones
    /// for which the transaction was dropped as `sampled: false`.
    Sampled,
    /// Content length of an optional meta segment that might be contained in the item.
    ///
    /// For the time being such an meta segment is only present for trace attachments.
    MetaLength,
    /// Parent span entity that this item is associated with, if any.
    ///
    /// For the time being only applicable if the item is a trace attachment.
    SpanId,
    /// Size of the attachment that an attachment placeholder represents.
    ///
    /// Only valid in combination with [`ContentType::AttachmentRef`]. This untrusted header is used
    /// to emit negative outcomes, but must not be used for consistent rate limiting.
    AttachmentLength,
    /// The Sentry release stored in a header.
    SentryRelease,
    /// The Sentry environment stored in a header.
    SentryEnvironment,
}

/// The value of an item header.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemHeaderValue(serde_json::Value);

#[derive(Clone, Deserialize, Serialize)]
pub struct CommonItemHeaders {
    /// If this is an attachment item, this may contain the storage key in case it is uploaded to objectstore.
    ///
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    stored_key: Option<String>,
    /// The routing_hint may be used to specify how the envelpope should be routed in when
    /// published to Kafka.
    #[serde(default, skip)]
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

    /// Other attributes for forward compatibility.
    #[serde(flatten)]
    inner: BTreeMap<MaybeKnown<ItemHeaderKey>, ItemHeaderValue>,
}

/// Wire headers used while parsing, before the tag selects an owning variant.
#[derive(Clone, Deserialize, Serialize)]
pub struct ItemHeaders {
    #[serde(rename = "type")]
    ty: ItemType,
    #[serde(flatten)]
    data: CommonItemHeaders,
}
impl std::ops::Deref for ItemHeaders {
    type Target = CommonItemHeaders;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl std::ops::DerefMut for ItemHeaders {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
#[derive(Serialize)]
pub(super) struct ItemHeadersRef<'a> {
    #[serde(rename = "type")]
    ty: &'a ItemType,
    #[serde(flatten)]
    data: &'a CommonItemHeaders,
}
impl fmt::Debug for ItemHeadersRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.data.debug_with_type(f, Some(self.ty))
    }
}
impl Item {
    pub(super) fn wire_headers(&self) -> ItemHeadersRef<'_> {
        ItemHeadersRef {
            ty: self.ty(),
            data: &self.headers,
        }
    }
}

impl CommonItemHeaders {
    fn contains(&self, key: ItemHeaderKey) -> bool {
        self.inner.contains_key(&MaybeKnown::Known(key))
    }

    fn get<'a, T>(&'a self, key: ItemHeaderKey) -> Option<T>
    where
        T: Deserialize<'a>,
    {
        self.inner
            .get(&MaybeKnown::Known(key))
            .and_then(|v| T::deserialize(&v.0).ok())
    }

    fn try_get<'a, T>(&'a self, key: ItemHeaderKey) -> Result<Option<T>, serde_json::Error>
    where
        T: Deserialize<'a>,
    {
        self.inner
            .get(&MaybeKnown::Known(key))
            .map(|v| T::deserialize(&v.0))
            .transpose()
    }

    fn set<T>(&mut self, key: ItemHeaderKey, value: T) -> &mut Self
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).expect("header value must be json serializable");
        self.inner
            .insert(MaybeKnown::Known(key), ItemHeaderValue(value));
        self
    }

    fn set_or_remove<T>(&mut self, key: ItemHeaderKey, value: Option<T>) -> &mut Self
    where
        T: Serialize,
    {
        if let Some(value) = value {
            self.set(key, value);
        } else {
            self.inner.remove(&MaybeKnown::Known(key));
        }
        self
    }
}

impl fmt::Debug for CommonItemHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.debug_with_type(f, None)
    }
}
impl CommonItemHeaders {
    fn debug_with_type(&self, f: &mut fmt::Formatter<'_>, ty: Option<&ItemType>) -> fmt::Result {
        let Self {
            stored_key,
            routing_hint,
            rate_limited,
            source_quantities,
            inner,
        } = self;

        let mut map = f.debug_map();

        if let Some(ty) = ty {
            map.entry(&"type", ty);
        }
        if let Some(stored_key) = stored_key {
            map.entry(&"stored_key", stored_key);
        }
        if let Some(routing_hint) = routing_hint {
            map.entry(&"routing_hint", routing_hint);
        }
        if *rate_limited {
            map.entry(&"rate_limited", rate_limited);
        }
        if let Some(source_quantities) = source_quantities {
            map.entry(&"source_quantities", source_quantities);
        }

        for (k, v) in inner {
            match k {
                MaybeKnown::Known(v) => map.key(&v),
                MaybeKnown::Unknown(s) => map.key(&s),
            };

            match &v.0 {
                serde_json::Value::Null => map.value(&"null"),
                serde_json::Value::Bool(b) => map.value(&b),
                serde_json::Value::Number(number) => {
                    if let Some(n) = number.as_u128() {
                        map.value(&n)
                    } else if let Some(n) = number.as_f64() {
                        map.value(&n)
                    } else {
                        map.value(&number)
                    }
                }
                serde_json::Value::String(s) => map.value(&s),
                v @ serde_json::Value::Array(_) => map.value(&v),
                v @ serde_json::Value::Object(_) => map.value(&v),
            };
        }

        map.finish()
    }
}

/// An item key which is potentially known to Relay.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(untagged)]
pub enum MaybeKnown<T> {
    /// An item known to this Relay.
    Known(T),
    /// An unknown item.
    ///
    /// Kept for forward compatibility.
    Unknown(String),
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
    /// Total number of buckets.
    pub buckets: usize,
}

impl AddAssign for SourceQuantities {
    fn add_assign(&mut self, other: Self) {
        let Self {
            transactions,
            spans,
            buckets,
        } = self;
        *transactions += other.transactions;
        *spans += other.spans;
        *buckets += other.buckets;
    }
}

/// Parent identifier for an attachment-v2.
///
/// Attachments can be associated with different types of parent entities (only spans for now).
///
/// SpanId(None) indicates that the item is a span-attachment that is associated with no specific
/// span.
#[derive(Clone, Debug)]
pub enum ParentId {
    SpanId(Option<SpanId>),
}

impl ParentId {
    /// Converts the ID to a span ID (if applicable).
    pub fn as_span_id(&self) -> Option<SpanId> {
        match self {
            ParentId::SpanId(span_id) => *span_id,
        }
    }
}

/// The type of parent entity an attachment is associated with.
///
/// This is used to route attachments to different rate limiting buckets, since
/// depending on the parent the limiting logic is different. E.g. if the attachment has
/// [`AttachmentParentType::Span`] than it should be dropped if there are span limits.
///
/// See [`AttachmentItem::attachment_parent_type`] for how this is determined from an attachment.
#[derive(Debug)]
pub enum AttachmentParentType {
    /// The parent type for all V1 attachments (e.g. minidumps)
    Event,
    /// The parent type for all span V2 attachments.
    Span,
    /// The parent type for all trace V2 attachments.
    Trace,
}

#[cfg(test)]
mod tests {
    #[test]
    fn misplaced_attachment_headers_do_not_change_item_classification() {
        use super::*;
        for ty in ["event", "user_report", "profile"] {
            for attachment_type in ["event.minidump", "future_attachment"] {
                let header = serde_json::json!({
                    "type": ty, "attachment_type": attachment_type,
                    "filename": "preserved.bin", "length": 2,
                });
                let (item, _) = Item::parse(Bytes::from(format!("{header}\n{{}}\n"))).unwrap();
                assert!(!matches!(item, Item::Attachment(_)));
                assert!(!item.is_unknown());
                assert_eq!(serde_json::to_value(item.wire_headers()).unwrap(), header);
            }
        }
        let (attachment, _) = Item::parse(Bytes::from_static(
            b"{\"type\":\"attachment\",\"attachment_type\":\"future_attachment\"}\nbody\n",
        ))
        .unwrap();
        assert!(attachment.is_unknown());
    }

    use crate::integrations::LogsIntegration;
    use crate::integrations::OtelFormat;

    use super::*;

    #[test]
    fn payload_classification_and_routing_are_separate_facts() {
        // These are the pre-refactor routing/classification contracts. ReplayEvent's
        // routing is intentionally preserved even though its quota is independent.
        for (ty, payload, routed) in [
            (ItemType::Event, true, true),
            (ItemType::Transaction, true, true),
            (ItemType::Security, true, true),
            (ItemType::RawSecurity, true, true),
            (ItemType::UnrealReport, true, true),
            (ItemType::UserReportV2, true, true),
            (ItemType::Attachment, false, true),
            (ItemType::FormData, false, true),
            (ItemType::UserReport, false, true),
            (ItemType::Profile, false, true),
            (ItemType::ReplayEvent, false, true),
            (ItemType::ReplayRecording, false, false),
            (ItemType::ReplayVideo, false, false),
            (ItemType::ProfileChunk, false, false),
            (ItemType::Span, false, false),
            (ItemType::Session, false, false),
            (ItemType::Sessions, false, false),
            (ItemType::MetricBuckets, false, false),
            (ItemType::ClientReport, false, false),
            (ItemType::CheckIn, false, false),
            (ItemType::Log, false, false),
            (ItemType::TraceMetric, false, false),
            (ItemType::Integration, false, false),
            (ItemType::Unknown("future".into()), false, false),
        ] {
            let item = Item::new(ty);
            assert_eq!(item.carries_event_payload(), payload, "{:?}", item.ty());
            assert_eq!(item.routes_with_event(), routed, "{:?}", item.ty());
        }
        for parent in [
            AttachmentParentType::Event,
            AttachmentParentType::Span,
            AttachmentParentType::Trace,
        ] {
            let mut dump = AttachmentItem::new();
            dump.set_attachment_type(AttachmentType::Minidump);
            if !matches!(parent, AttachmentParentType::Event) {
                dump.set_payload(ContentType::TraceAttachment, "{}");
                if matches!(parent, AttachmentParentType::Span) {
                    dump.set_parent_id(Some(ParentId::SpanId(None)));
                }
            }
            assert!(dump.carries_event_payload());
        }
    }

    #[test]
    fn typed_items_preserve_wire_headers_and_variant_identity() {
        // Preserve unknown tags and misplaced or invalid headers for forwarding.
        for tag in ["transaction", "profile", "attachment", "future_item"] {
            let header = serde_json::json!({
                "type": tag, "length": 2, "span_count": "invalid", "future_header": [1, 2],
            });
            let wire = format!("{header}\n{{}}\n");
            let (item, consumed) = Item::parse(Bytes::from(wire.clone())).unwrap();
            assert_eq!(consumed, wire.len());
            assert_eq!(item.ty().as_str(), tag);
            assert_eq!(serde_json::to_value(item.wire_headers()).unwrap(), header);
            assert_eq!(item.payload(), "{}");
            match item {
                Item::Transaction(transaction) => assert_eq!(transaction.span_count(), None),
                Item::Profile(profile) => assert_eq!(profile.profile_type(), None),
                Item::Attachment(attachment) => assert_eq!(attachment.attachment_body_size(), 2),
                Item::Unknown(_) => {}
                _ => unreachable!(),
            }
        }
        // Swapping common storage cannot turn a ProfileItem into a TransactionItem.
        let mut transaction = TransactionItem::new();
        transaction.set_span_count(Some(3));
        let mut profile = ProfileItem::new();
        std::mem::swap(&mut *transaction, &mut *profile);
        assert_eq!(transaction.span_count(), None);
        let transaction = Item::from(transaction);
        assert_eq!(
            serde_json::to_value(transaction.wire_headers()).unwrap()["type"],
            "transaction"
        );
        let profile = Item::from(profile);
        assert_eq!(
            serde_json::to_value(profile.wire_headers()).unwrap()["type"],
            "profile"
        );
    }

    #[test]
    fn concrete_accounting_preserves_container_counts() {
        let (profile, _) = Item::parse(Bytes::from_static(
            b"{\"type\":\"profile\",\"platform\":\"python\",\"item_count\":3}\n{}\n",
        ))
        .unwrap();
        let Item::Profile(profile) = profile else {
            panic!("expected a profile");
        };
        assert_eq!(
            profile.quantities().as_slice(),
            [
                (DataCategory::Profile, 3),
                (DataCategory::ProfileIndexed, 3),
                (DataCategory::ProfileBackend, 3),
            ]
        );
        let mut transaction = TransactionItem::new();
        // Transactions never support containers: `1` still means one transaction.
        transaction.set_payload(ContentType::Json, "{}");
        transaction.set_test_header(ItemHeaderKey::ItemCount, Some(99u32));
        transaction.set_span_count(Some(2));
        assert_eq!(
            transaction.quantities().as_slice(),
            [
                (DataCategory::Transaction, 1),
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Span, 3),
                (DataCategory::SpanIndexed, 3),
            ]
        );
    }

    #[test]
    fn test_item_quantities_preserve_order_and_zero_counts() {
        let mut span = SpanItem::new();
        span.set_payload(ContentType::Json, "{}");
        span.set_test_header(ItemHeaderKey::ItemCount, Some(3u32));
        let mut log = LogItem::new();
        log.set_payload(ContentType::Json, "");
        log.set_test_header(ItemHeaderKey::ItemCount, Some(0u32));
        let mut profile = ProfileItem::new();
        profile.set_platform("python".into());
        let mut transaction = TransactionItem::new();
        transaction.set_span_count(Some(2));
        let mut attachment = AttachmentItem::new();
        attachment.set_rate_limited(true);
        let mut minidump = AttachmentItem::new();
        minidump.set_attachment_type(AttachmentType::Minidump);
        minidump.set_payload(ContentType::OctetStream, "MDMP");
        let mut trace_attachment = AttachmentItem::new();
        trace_attachment.set_payload(ContentType::TraceAttachment, "{}body");
        trace_attachment.set_meta_length(2);
        let mut nintendo = AttachmentItem::new();
        nintendo.set_attachment_type(AttachmentType::NintendoSwitchDyingMessage);
        nintendo.set_payload(ContentType::OctetStream, "dying message");
        let mut misplaced = UserReportItem::new();
        misplaced.set_test_header(
            crate::envelope::ItemHeaderKey::AttachmentType,
            Some(AttachmentType::NintendoSwitchDyingMessage),
        );
        let cases: [(Item, Vec<_>); 11] = [
            (nintendo.into(), vec![]),
            (misplaced.into(), vec![(DataCategory::UserReportV2, 1)]),
            (UnknownItem::new("future".into()).into(), vec![]),
            (attachment.into(), vec![]),
            // Implicit events belong to envelope accounting, not each crash attachment.
            (
                minidump.into(),
                vec![
                    (DataCategory::Attachment, 4),
                    (DataCategory::AttachmentItem, 1),
                ],
            ),
            (
                trace_attachment.into(),
                vec![
                    (DataCategory::Attachment, 4),
                    (DataCategory::AttachmentItem, 1),
                ],
            ),
            (
                UserReportItem::new().into(),
                vec![(DataCategory::UserReportV2, 1)],
            ),
            (
                span.into(),
                vec![(DataCategory::Span, 3), (DataCategory::SpanIndexed, 3)],
            ),
            (
                log.into(),
                vec![(DataCategory::LogItem, 0), (DataCategory::LogByte, 1)],
            ),
            (
                profile.into(),
                vec![
                    (DataCategory::Profile, 1),
                    (DataCategory::ProfileIndexed, 1),
                    (DataCategory::ProfileBackend, 1),
                ],
            ),
            (
                transaction.into(),
                vec![
                    (DataCategory::Transaction, 1),
                    (DataCategory::TransactionIndexed, 1),
                    (DataCategory::Span, 3),
                    (DataCategory::SpanIndexed, 3),
                ],
            ),
        ];
        for (item, expected) in cases {
            assert_eq!(item.quantities().as_slice(), expected.as_slice());
        }
    }

    #[test]
    fn test_item_empty() {
        let item = Item::Attachment(AttachmentItem::new());

        assert_eq!(item.payload(), Bytes::new());
        assert_eq!(item.len(), 0);
        assert!(item.is_empty());

        assert_eq!(item.content_type(), None);
    }

    #[test]
    fn test_item_set_payload() {
        let mut item = EventItem::new();

        let payload = Bytes::from(&br#"{"event_id":"3adcb99a1be84a5d8057f2eb9a0161ce"}"#[..]);
        item.set_payload(ContentType::Json, payload.clone());

        // Payload
        assert_eq!(item.payload(), payload);
        assert_eq!(item.len(), payload.len());
        assert!(!item.is_empty());

        // Meta data
        assert_eq!(item.content_type(), Some(ContentType::Json));
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_item_set_routing_hint() {
        let uuid = Uuid::parse_str("8a4ab00f-fba2-4f7b-a164-b58199d55c95").unwrap();

        let mut item = EventItem::new();
        item.set_routing_hint(uuid);

        assert_eq!(item.routing_hint(), Some(uuid));
    }

    #[test]
    fn test_item_source_quantities() {
        let mut item = MetricBucketsItem::new();
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
            Some(ContentType::Integration(Integration::Logs(
                LogsIntegration::OtelV1 {
                    format: OtelFormat::Json
                }
            )))
        );
        assert!(!matches!(item, Item::Integration(_)));
    }

    #[test]
    fn test_item_parse_length_overflow_does_not_panic() {
        let header = format!(r#"{{"type":"attachment","length":{}}}"#, usize::MAX)
            .into_bytes()
            .into();

        let err = Item::parse(header).unwrap_err();

        std::assert_matches!(err, EnvelopeError::UnexpectedEof);
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
    fn test_item_headers() {
        let mut headers: ItemHeaders = serde_json::from_str(
            r#"{
            "type":"attachment",
            "length":42,
            "content_type": "application/json",
            "filename":"test.txt"
        }"#,
        )
        .unwrap();

        assert_eq!(headers.ty, ItemType::Attachment);
        assert!(headers.contains(ItemHeaderKey::Length));
        assert_eq!(headers.get(ItemHeaderKey::Length), Some(42u32));
        assert_eq!(headers.get(ItemHeaderKey::Filename), Some("test.txt"));
        assert_eq!(
            headers.get(ItemHeaderKey::ContentType),
            Some("application/json")
        );

        headers.set_or_remove(ItemHeaderKey::Length, Some(1337u32));
        assert_eq!(headers.get(ItemHeaderKey::Length), Some(1337));
        headers.set_or_remove(ItemHeaderKey::Length, None::<u32>);
        assert!(!headers.contains(ItemHeaderKey::Length));

        let s = serde_json::to_string(&headers).unwrap();
        insta::assert_snapshot!(s, @r#"{"type":"attachment","content_type":"application/json","filename":"test.txt"}"#);
    }

    #[test]
    fn test_item_headers_unknown() {
        let (item, _) = Item::parse(Bytes::from_static(
            concat!(
                r#"{"type":"attachment","unknown1":"foo","unknown2":"bar","length":5}"#,
                "\n",
                "12345"
            )
            .as_bytes(),
        ))
        .unwrap();

        insta::assert_debug_snapshot!(&item.wire_headers(), @r#"
        {
            "type": Attachment,
            Length: 5,
            "unknown1": "foo",
            "unknown2": "bar",
        }
        "#);

        // Round trip -> serializes the headers again.
        let s = serde_json::to_string(&item.wire_headers()).unwrap();
        insta::assert_snapshot!(s, @r#"{"type":"attachment","length":5,"unknown1":"foo","unknown2":"bar"}"#);
    }
}
