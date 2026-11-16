//! Typed envelope items and their shared interfaces.

use std::fmt;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::envelope::{AttachmentType, ContentType, EnvelopeError};
use crate::integrations::Integration;
use crate::utils::DebugBytes;

mod attachment;
mod check_in;
mod client_report;
mod data;
mod event;
mod form_data;
mod headers;
mod integration;
mod item_type;
mod log;
mod metric_buckets;
mod profile;
mod profile_chunk;
mod raw_security;
mod replay_event;
mod replay_recording;
mod replay_video;
mod security;
mod session;
mod sessions;
mod span;
mod trace_metric;
mod transaction;
mod unknown;
mod unreal_report;
mod user_report;
mod user_report_v2;

#[cfg(test)]
mod tests;

pub use self::attachment::{AttachmentParentType, ParentId};
pub use self::data::{ItemData, SourceQuantities};
pub(super) use self::headers::ItemHeadersRef;
pub use self::headers::*;

/// A concrete variant of the serialized [`Item`] enum.
///
/// Borrowed extraction and `TryFrom<Item>` must accept the same variant.
pub trait TypedItem: TryFrom<Item, Error = Item> {
    /// Returns the shared headers and serialized payload.
    fn data(&self) -> &ItemData;

    /// Returns mutable access to the shared headers and serialized payload.
    fn data_mut(&mut self) -> &mut ItemData;

    /// Borrows the concrete item if the enum contains this variant.
    fn downcast_ref(item: &Item) -> Option<&Self>;
}

/// Typed items that support the `item_count` header.
pub trait ItemCount: TypedItem {
    /// Returns the declared count, or `None` if the header is absent or invalid.
    fn item_count(&self) -> Option<u32>;
}

/// Payload types that can be written in the shared [`crate::envelope::ItemContainer`] format.
pub trait ContainerPayload: ItemCount {
    /// Sets the payload, its content type, and its declared container count.
    fn set_payload_with_item_count(
        &mut self,
        content_type: ContentType,
        payload: impl Into<Bytes>,
        item_count: u32,
    );
}

impl Item {
    /// Returns whether this item contains the concrete type `T`.
    pub fn is<T: TypedItem>(&self) -> bool {
        self.downcast_ref::<T>().is_some()
    }

    /// Borrows the concrete item if it has type `T`.
    pub fn downcast_ref<T: TypedItem>(&self) -> Option<&T> {
        T::downcast_ref(self)
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

    /// Returns `true` if this item contains a payload which is unknown to this version of Relay.
    pub fn is_unknown(&self) -> bool {
        match self {
            Self::Unknown(_) => true,
            Self::Attachment(item) => item
                .data()
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
            Self::Integration(item) => {
                matches!(item.integration_header(), Some(Integration::Errors(_)))
            }
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
            Self::Integration(item) => {
                matches!(item.integration_header(), Some(Integration::Errors(_)))
            }
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
            | Self::Unknown(_) => false,
        }
    }

    pub(super) fn wire_headers(&self) -> ItemHeadersRef<'_> {
        ItemHeadersRef {
            ty: self.ty(),
            data: &self.data().headers,
        }
    }
}

impl fmt::Debug for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Item")
            .field("headers", &self.wire_headers())
            .field("payload", &DebugBytes(&self.data().payload))
            .finish()
    }
}

pub type Items = SmallVec<[Item; 3]>;
pub type ItemIter<'a> = std::slice::Iter<'a, Item>;
pub type ItemIterMut<'a> = std::slice::IterMut<'a, Item>;

/// An envelope item with an unparsed payload.
///
/// `TypedItems` generates the concrete variant structs and wire tags.
/// `#[typed_item(item_count_header)]` implements [`ItemCount`] for reading the count header.
/// `#[typed_item(container)]` additionally implements [`ContainerPayload`] for writing containers.
#[derive(Clone, relay_server_derive::TypedItems)]
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
    ///
    /// Relay's Unreal endpoint creates an [`ItemType::Integration`] item with
    /// [`crate::integrations::ErrorsIntegration::Unreal`] instead. This item type is still accepted for compatibility
    /// with envelopes forwarded by older Relays.
    UnrealReport(UnrealReportItem),
    /// User feedback encoded as JSON.
    UserReport(UserReportItem),
    /// Session update data.
    #[typed_item(item_count_header)]
    Session(SessionItem),
    /// Aggregated session data.
    #[typed_item(item_count_header)]
    Sessions(SessionsItem),
    /// Buckets of preaggregated metrics encoded as JSON.
    #[typed_item(item_count_header)]
    MetricBuckets(MetricBucketsItem),
    /// Client internal report (eg: outcomes).
    #[typed_item(item_count_header)]
    ClientReport(ClientReportItem),
    /// Profile event payload encoded as JSON.
    #[typed_item(item_count_header)]
    Profile(ProfileItem),
    /// Replay metadata and breadcrumb payload.
    ReplayEvent(ReplayEventItem),
    /// Replay Recording data.
    ReplayRecording(ReplayRecordingItem),
    /// Replay Video data.
    ReplayVideo(ReplayVideoItem),
    /// Monitor check-in encoded as JSON.
    #[typed_item(item_count_header)]
    CheckIn(CheckInItem),
    /// A log for the log product, not internal logs.
    #[typed_item(container)]
    Log(LogItem),
    /// A trace metric item.
    #[typed_item(container)]
    TraceMetric(TraceMetricItem),
    /// A standalone span.
    #[typed_item(container)]
    Span(SpanItem),
    /// UserReport as an Event
    UserReportV2(UserReportV2Item),
    /// ProfileChunk is a chunk of a profiling session.
    #[typed_item(item_count_header)]
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
    #[typed_item(item_count_header)]
    Unknown(UnknownItem),
    // Keep `Unknown` last in the list. Add new items above `Unknown`.
}
