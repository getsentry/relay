use relay_profiling::ProfileType;
use std::fmt;
use std::ops::AddAssign;
use uuid::Uuid;

use bytes::Bytes;
use relay_event_schema::protocol::{EventType, SpanId};
use relay_quotas::DataCategory;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::{SmallVec, smallvec};

use crate::envelope::{AttachmentType, ContentType, EnvelopeError};
use crate::integrations::{Integration, LogsIntegration, SpansIntegration};
use crate::statsd::RelayTimers;

#[derive(Clone, Debug)]
pub struct Item {
    pub(super) headers: ItemHeaders,
    pub(super) payload: Bytes,
}

impl Item {
    /// Creates a new item with the given type.
    pub fn new(ty: ItemType) -> Self {
        Self {
            headers: ItemHeaders::new(ty),
            payload: Bytes::new(),
        }
    }

    /// Returns the `ItemType` of this item.
    pub fn ty(&self) -> ItemType {
        self.headers.ty()
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
        let payload_end = match headers.length() {
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
            ItemType::Transaction => {
                let mut quantities = smallvec![
                    (DataCategory::Transaction, item_count),
                    (DataCategory::TransactionIndexed, item_count),
                ];
                if !self.spans_extracted() {
                    quantities.extend([
                        (DataCategory::Span, item_count + self.span_count()),
                        (DataCategory::SpanIndexed, item_count + self.span_count()),
                    ]);
                }
                quantities
            }
            ItemType::Security | ItemType::RawSecurity => {
                smallvec![(DataCategory::Security, item_count)]
            }
            ItemType::Nel => smallvec![],
            ItemType::UnrealReport => smallvec![(DataCategory::Error, item_count)],
            ItemType::Attachment => smallvec![
                (DataCategory::Attachment, self.attachment_body_size()),
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
            ItemType::Profile => match self.profile_type() {
                Some(ProfileType::Backend) => smallvec![
                    (DataCategory::Profile, item_count),
                    (DataCategory::ProfileIndexed, item_count),
                    (DataCategory::ProfileBackend, item_count),
                ],
                Some(ProfileType::Ui) => smallvec![
                    (DataCategory::Profile, item_count),
                    (DataCategory::ProfileIndexed, item_count),
                    (DataCategory::ProfileUi, item_count)
                ],
                // Note: parsing the profile type (and validity) requires parsing the payload,
                // which makes this semantically wrong but it is too expensive here to parse the
                // payload.
                None => smallvec![
                    (DataCategory::Profile, item_count),
                    (DataCategory::ProfileIndexed, item_count),
                ],
            },
            ItemType::ProfileChunk => match self.profile_type() {
                Some(ProfileType::Backend) => smallvec![(DataCategory::ProfileChunk, item_count)],
                Some(ProfileType::Ui) => smallvec![(DataCategory::ProfileChunkUi, item_count)],
                // Note: parsing the profile type (and validity) requires parsing the payload,
                // which makes this semantically wrong but it is too expensive here to parse the
                // payload.
                None => smallvec![],
            },
            ItemType::ReplayEvent | ItemType::ReplayRecording | ItemType::ReplayVideo => {
                smallvec![(DataCategory::Replay, item_count)]
            }
            ItemType::ClientReport => smallvec![],
            ItemType::CheckIn => smallvec![(DataCategory::Monitor, item_count)],
            ItemType::Span => smallvec![
                (DataCategory::Span, item_count),
                (DataCategory::SpanIndexed, item_count),
            ],
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
            true => self.headers.item_count(),
            false => None,
        }
    }

    /// Returns the number of spans in the `event.spans` array.
    ///
    /// Should always be 0 except for transaction items.
    ///
    /// When a transaction is dropped before spans were extracted from a transaction,
    /// this number is used to emit correct outcomes for the spans category.
    ///
    /// This number does *not* count the transaction itself.
    pub fn span_count(&self) -> usize {
        self.headers.span_count().unwrap_or(0)
    }

    /// Sets the number of spans in the transaction payload.
    pub fn set_span_count(&mut self, value: Option<usize>) {
        self.headers.set_span_count(value);
    }

    /// Sets the `span_count` item header by shallow parsing the event.
    ///
    /// Returns the recomputed count.
    fn refresh_span_count(&mut self) -> usize {
        let count = self.parse_span_count();
        self.headers.set_span_count(count);
        count.unwrap_or(0)
    }

    /// Returns the `span_count`` header, and computes it if it has not yet been set.
    pub fn ensure_span_count(&mut self) -> usize {
        match self.headers.span_count() {
            Some(count) => count,
            None => self.refresh_span_count(),
        }
    }

    /// Returns the content type of this item's payload.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn content_type(&self) -> Option<ContentType> {
        self.headers.content_type()
    }

    /// Returns the [`Integration`] the item belongs.
    pub fn integration(&self) -> Option<Integration> {
        if !matches!(self.ty(), ItemType::Integration) {
            return None;
        }

        match self.content_type() {
            Some(ContentType::Integration(integration)) => Some(integration),
            _ => {
                // This is a bug which should never happen.
                debug_assert!(false, "integration item, but no integration content type");
                None
            }
        }
    }

    /// Returns the attachment type if this item is an attachment.
    pub fn attachment_type(&self) -> Option<AttachmentType> {
        self.headers.attachment_type()
    }

    /// Sets the attachment type of this item.
    pub fn set_attachment_type(&mut self, attachment_type: AttachmentType) {
        self.headers.set_attachment_type(attachment_type);
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

        self.headers.set_length(Some(length as u32));
        self.payload = payload;
    }

    /// Sets the payload and content-type of this envelope item. Use
    /// `set_payload_without_content_type` if you need to set the payload without a content-type.
    pub fn set_payload<B>(&mut self, content_type: ContentType, payload: B)
    where
        B: Into<Bytes>,
    {
        self.headers.set_content_type(content_type);
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
        self.headers.set_content_type(content_type);
        self.headers.set_item_count(Some(item_count));
        self.set_payload_without_content_type(payload);
    }

    /// Returns the file name of this item, if it is an attachment.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn filename(&self) -> Option<&str> {
        self.headers.filename()
    }

    /// Sets the file name of this item.
    pub fn set_filename<S>(&mut self, filename: S)
    where
        S: Into<String>,
    {
        self.headers.set_filename(filename.into());
    }

    /// Returns the objectstore key, if it is an attachment stored in objectstore.
    pub fn stored_key(&self) -> Option<&str> {
        self.headers.stored_key.as_deref()
    }

    /// Sets the objectstore key of this attachment item.
    pub fn set_stored_key(&mut self, stored_key: String) {
        self.headers.stored_key = Some(stored_key);
    }

    /// Returns the routing_hint of this item.
    pub fn routing_hint(&self) -> Option<Uuid> {
        self.headers.routing_hint()
    }

    /// Set the routing_hint of this item.
    pub fn set_routing_hint(&mut self, routing_hint: Uuid) {
        self.headers.set_routing_hint(routing_hint);
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
        self.headers.metrics_extracted()
    }

    /// Sets the metrics extracted flag.
    pub fn set_metrics_extracted(&mut self, metrics_extracted: bool) {
        self.headers.set_metrics_extracted(metrics_extracted);
    }

    /// Returns the spans extracted flag.
    pub fn spans_extracted(&self) -> bool {
        self.headers.spans_extracted()
    }

    /// Sets the spans extracted flag.
    pub fn set_spans_extracted(&mut self, spans_extracted: bool) {
        self.headers.set_spans_extracted(spans_extracted);
    }

    /// Returns the fully normalized flag.
    pub fn fully_normalized(&self) -> bool {
        self.headers.fully_normalized()
    }

    /// Sets the fully normalized flag.
    pub fn set_fully_normalized(&mut self, fully_normalized: bool) {
        self.headers.set_fully_normalized(fully_normalized);
    }

    /// Returns the associated platform.
    ///
    /// Note: this is currently only used for [`ItemType::ProfileChunk`].
    pub fn platform(&self) -> Option<&str> {
        self.headers.platform()
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
        self.headers.sampled()
    }

    /// Sets the `sampled` flag.
    pub fn set_sampled(&mut self, sampled: bool) {
        self.headers.set_sampled(sampled);
    }

    /// Returns the length of the item.
    pub fn meta_length(&self) -> Option<u32> {
        self.headers.meta_length()
    }

    /// Sets the length of the optional meta segment.
    ///
    /// Only applicable if the item is an attachment.
    pub fn set_meta_length(&mut self, meta_length: u32) {
        self.headers.set_meta_length(meta_length);
    }

    /// Sets the length of the attachment referenced by this item.
    ///
    /// Only applicable if the item is an attachment with [`ContentType::AttachmentRef`].
    pub fn set_attachment_length(&mut self, original_length: u64) {
        debug_assert!(self.is_attachment_ref());
        self.headers.set_attachment_length(original_length);
    }

    /// Returns the parent entity that this item is associated with, if any.
    ///
    /// Only applicable if the item is an attachment.
    pub fn parent_id(&self) -> Option<ParentId> {
        self.headers.parent_id()
    }

    /// Sets the parent entity that this item is associated with.
    pub fn set_parent_id(&mut self, parent_id: Option<ParentId>) {
        self.headers.set_parent_id(parent_id);
    }

    /// Returns `true` if this item is an attachment with AttachmentV2 content type.
    fn is_attachment_v2(&self) -> bool {
        self.ty() == ItemType::Attachment
            && self.content_type() == Some(ContentType::TraceAttachment)
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
    fn is_attachment_ref(&self) -> bool {
        self.ty() == ItemType::Attachment && self.content_type() == Some(ContentType::AttachmentRef)
    }

    /// Returns the [`AttachmentParentType`] of an attachment.
    ///
    /// For standard attachments (V1) always returns [`AttachmentParentType::Event`].
    pub fn attachment_parent_type(&self) -> AttachmentParentType {
        let is_attachment = self.ty() == ItemType::Attachment;
        debug_assert!(
            is_attachment,
            "function should only be called on attachments"
        );
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
            self.headers.attachment_length().unwrap_or(0) as usize
        } else {
            self.len()
        }
        .max(1)
    }

    /// Returns the specified header value, if present.
    pub fn get_header(&self, name: &str) -> Option<&serde_json::Value> {
        self.headers.get(name)
    }

    /// Sets the specified header value, returning the previous one if present.
    pub fn set_header(
        &mut self,
        name: &str,
        value: impl Into<serde_json::Value>,
    ) -> Option<serde_json::Value> {
        self.headers.set(name, value)
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
                match self.attachment_type().unwrap_or_default() {
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
        self.content_type().is_some_and(|ct| ct.is_container())
    }

    fn parse_span_count(&self) -> Option<usize> {
        #[derive(Debug, serde::Deserialize)]
        struct PartialEvent {
            spans: crate::utils::SeqCount,
        }

        if self.ty() != ItemType::Transaction || self.spans_extracted() {
            return None;
        }

        let event = relay_statsd::metric!(timer(RelayTimers::CheckNestedSpans), {
            serde_json::from_slice::<PartialEvent>(&self.payload()).ok()?
        });

        Some(event.spans.0)
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

#[derive(Clone, Debug)]
pub struct ItemHeaders {
    /// All serializable header fields stored as a JSON object.
    inner: serde_json::Value,

    // Internal-only fields (never serialized to the wire format).
    stored_key: Option<String>,
    rate_limited: bool,
    source_quantities: Option<SourceQuantities>,
    profile_type: Option<ProfileType>,
}

impl ItemHeaders {
    fn new(ty: ItemType) -> Self {
        Self {
            inner: serde_json::json!({
                "type": ty.as_str(),
                "length": 0,
            }),
            stored_key: None,
            rate_limited: false,
            source_quantities: None,
            profile_type: None,
        }
    }

    // --- Getters ---

    /// Returns the type of the item.
    pub fn ty(&self) -> ItemType {
        self.inner
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.parse().unwrap())
            .unwrap_or_else(|| ItemType::Unknown(String::new()))
    }

    /// Returns the content length of the item payload.
    ///
    /// Can be `None` if the item does not contain new lines, in which case the payload is
    /// parsed until the first newline.
    pub fn length(&self) -> Option<u32> {
        self.inner
            .get("length")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
    }

    /// Returns the number of contained items in an [`super::ItemContainer`].
    ///
    /// The amount specified must match the amount of items contained in the container exactly.
    /// Failing to specify the count or a mismatching count will be treated as an invalid envelope.
    pub fn item_count(&self) -> Option<u32> {
        self.inner
            .get("item_count")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
    }

    /// Returns the content type of this item's payload.
    pub fn content_type(&self) -> Option<ContentType> {
        self.inner
            .get("content_type")
            .and_then(|v| v.as_str())
            .map(|s| s.parse().unwrap())
    }

    /// Returns the attachment type if this is an attachment item.
    pub fn attachment_type(&self) -> Option<AttachmentType> {
        self.inner
            .get("attachment_type")
            .and_then(|v| v.as_str())
            .map(|s| s.parse().unwrap())
    }

    /// Returns the original file name if this is an attachment item.
    pub fn filename(&self) -> Option<&str> {
        self.inner.get("filename").and_then(|v| v.as_str())
    }

    /// Returns the platform this item was produced for.
    ///
    /// Currently only used for [`ItemType::ProfileChunk`] to determine the correct data category.
    pub fn platform(&self) -> Option<&str> {
        self.inner.get("platform").and_then(|v| v.as_str())
    }

    /// Returns the routing hint for publishing to kafka.
    ///
    /// Currently only used for [`ItemType::CheckIn`].
    pub fn routing_hint(&self) -> Option<Uuid> {
        self.inner
            .get("routing_hint")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
    }

    /// Returns whether metrics have already been extracted from the item.
    ///
    /// The first Relay that extracts metrics sets this to `true` so upstream Relays
    /// do not extract the metric again causing double counting.
    pub fn metrics_extracted(&self) -> bool {
        self.inner
            .get("metrics_extracted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Returns whether spans and span metrics have been extracted from a transaction.
    ///
    /// Also set to `true` for transactions extracted from spans, to prevent going in circles.
    pub fn spans_extracted(&self) -> bool {
        self.inner
            .get("spans_extracted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Returns the number of spans in the `event.spans` array.
    ///
    /// Should never be set except for transaction items. This number does *not* count the
    /// transaction itself.
    pub fn span_count(&self) -> Option<usize> {
        self.inner
            .get("span_count")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
    }

    /// Returns whether the event has been fully normalized.
    ///
    /// If the event has been partially normalized, this returns `false`.
    pub fn fully_normalized(&self) -> bool {
        self.inner
            .get("fully_normalized")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Returns `false` if the sampling decision is "drop".
    ///
    /// For profiles with the feature enabled, dropped items are kept but marked `sampled: false`.
    pub fn sampled(&self) -> bool {
        self.inner
            .get("sampled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    }

    /// Returns the content length of an optional meta segment contained in the item.
    ///
    /// Currently only present for trace attachments.
    pub fn meta_length(&self) -> Option<u32> {
        self.inner
            .get("meta_length")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
    }

    /// Returns the parent entity that this item is associated with.
    ///
    /// Currently only applicable for trace attachments.
    pub fn parent_id(&self) -> Option<ParentId> {
        // ParentId was #[serde(flatten)], so "span_id" appears as a direct key.
        let val = self.inner.get("span_id")?;
        if val.is_null() {
            Some(ParentId::SpanId(None))
        } else {
            let span_id = serde_json::from_value::<SpanId>(val.clone()).ok();
            Some(ParentId::SpanId(span_id))
        }
    }

    /// Returns the size of the attachment that an attachment placeholder represents.
    ///
    /// Only valid in combination with [`ContentType::AttachmentRef`]. This untrusted header
    /// is used to emit negative outcomes, but must not be used for consistent rate limiting.
    pub fn attachment_length(&self) -> Option<u64> {
        self.inner.get("attachment_length").and_then(|v| v.as_u64())
    }

    /// Returns a header value by key name.
    pub fn get(&self, name: &str) -> Option<&serde_json::Value> {
        self.inner.get(name)
    }

    // --- Setters ---

    /// Sets a header value by key name, returning the previous value if present.
    pub fn set(
        &mut self,
        key: &str,
        value: impl Into<serde_json::Value>,
    ) -> Option<serde_json::Value> {
        self.inner
            .as_object_mut()
            .unwrap()
            .insert(key.to_owned(), value.into())
    }

    fn remove(&mut self, key: &str) -> Option<serde_json::Value> {
        self.inner.as_object_mut().unwrap().remove(key)
    }

    /// Sets the content length of the item payload.
    ///
    /// See [`Self::length`].
    pub fn set_length(&mut self, length: Option<u32>) {
        if let Some(v) = length {
            self.set("length", v);
        } else {
            self.remove("length");
        }
    }

    /// Sets the number of contained items.
    ///
    /// See [`Self::item_count`].
    pub fn set_item_count(&mut self, item_count: Option<u32>) {
        if let Some(v) = item_count {
            self.set("item_count", v);
        } else {
            self.remove("item_count");
        }
    }

    /// Sets the content type of this item's payload.
    ///
    /// See [`Self::content_type`].
    pub fn set_content_type(&mut self, content_type: ContentType) {
        self.set("content_type", content_type.as_str());
    }

    /// Sets the attachment type.
    ///
    /// See [`Self::attachment_type`].
    pub fn set_attachment_type(&mut self, attachment_type: AttachmentType) {
        self.set("attachment_type", attachment_type.to_string());
    }

    /// Sets the original file name.
    ///
    /// See [`Self::filename`].
    pub fn set_filename(&mut self, filename: String) {
        self.set("filename", filename);
    }

    /// Sets the routing hint for publishing to kafka.
    ///
    /// See [`Self::routing_hint`].
    pub fn set_routing_hint(&mut self, routing_hint: Uuid) {
        self.set("routing_hint", routing_hint.to_string());
    }

    /// Sets the metrics extracted flag.
    ///
    /// See [`Self::metrics_extracted`].
    pub fn set_metrics_extracted(&mut self, val: bool) {
        self.set("metrics_extracted", val);
    }

    /// Sets the spans extracted flag.
    ///
    /// See [`Self::spans_extracted`].
    pub fn set_spans_extracted(&mut self, val: bool) {
        self.set("spans_extracted", val);
    }

    /// Sets the span count.
    ///
    /// See [`Self::span_count`].
    pub fn set_span_count(&mut self, count: Option<usize>) {
        if let Some(v) = count {
            self.set("span_count", v);
        } else {
            self.remove("span_count");
        }
    }

    /// Sets the fully normalized flag.
    ///
    /// See [`Self::fully_normalized`].
    pub fn set_fully_normalized(&mut self, val: bool) {
        self.set("fully_normalized", val);
    }

    /// Sets the sampled flag.
    ///
    /// See [`Self::sampled`].
    pub fn set_sampled(&mut self, val: bool) {
        self.set("sampled", val);
    }

    /// Sets the meta segment length.
    ///
    /// See [`Self::meta_length`].
    pub fn set_meta_length(&mut self, meta_length: u32) {
        self.set("meta_length", meta_length);
    }

    /// Sets the attachment placeholder size.
    ///
    /// See [`Self::attachment_length`].
    pub fn set_attachment_length(&mut self, length: u64) {
        self.set("attachment_length", length);
    }

    /// Sets the parent entity association.
    ///
    /// See [`Self::parent_id`].
    pub fn set_parent_id(&mut self, parent_id: Option<ParentId>) {
        match parent_id {
            Some(ParentId::SpanId(span_id)) => {
                let val = match span_id {
                    Some(id) => serde_json::to_value(id).unwrap(),
                    None => serde_json::Value::Null,
                };
                self.set("span_id", val);
            }
            None => {
                self.remove("span_id");
            }
        }
    }
}

impl Serialize for ItemHeaders {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ItemHeaders {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = serde_json::Value::deserialize(deserializer)?;
        Ok(Self {
            inner,
            stored_key: None,
            rate_limited: false,
            source_quantities: None,
            profile_type: None,
        })
    }
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
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
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
/// See [`Item::attachment_parent_type`] for how this is determined from an item.
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
        assert_eq!(item.content_type(), Some(ContentType::Json));
    }

    #[test]
    fn test_item_set_header() {
        let mut item = Item::new(ItemType::Event);
        item.set_header("custom", 42u64);

        let expected = serde_json::Value::from(42u64);
        assert_eq!(item.get_header("custom"), Some(&expected));
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
            Some(ContentType::Integration(Integration::Logs(
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
