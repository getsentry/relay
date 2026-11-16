//! AttachmentItem APIs and accounting.

use super::{AttachmentItem, AttachmentType, ContentType, ItemHeaderKey};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_event_schema::protocol::SpanId;
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl AttachmentItem {
    /// Returns the attachment type.
    pub fn attachment_type(&self) -> Option<AttachmentType> {
        if let Some(ty) = self.data().headers.get(ItemHeaderKey::AttachmentType) {
            return Some(ty);
        }

        // Unfortunately when the switch protocol was decided on, it was missed to assign it a new
        // attachment type, that's why we have to infer it here from the filename and contents.
        if self.filename() == Some(crate::constants::NNSWITCH_DYING_MESSAGE_FILENAME)
            && self
                .data()
                .payload
                .starts_with(crate::constants::NNSWITCH_SENTRY_MAGIC)
        {
            return Some(AttachmentType::NintendoSwitchDyingMessage);
        }

        None
    }

    /// Sets the attachment type of this item.
    pub fn set_attachment_type(&mut self, attachment_type: AttachmentType) {
        self.data_mut()
            .headers
            .set(ItemHeaderKey::AttachmentType, attachment_type);
    }

    /// Returns the attachment file name.
    pub fn filename(&self) -> Option<&str> {
        self.data().headers.get(ItemHeaderKey::Filename)
    }

    /// Sets the file name of this item.
    pub fn set_filename<S>(&mut self, filename: S)
    where
        S: Into<String>,
    {
        self.data_mut()
            .headers
            .set(ItemHeaderKey::Filename, filename.into());
    }

    /// Returns the objectstore key for a stored attachment.
    pub fn stored_key(&self) -> Option<&str> {
        self.data().headers.stored_key.as_deref()
    }

    /// Sets the objectstore key of this attachment item.
    pub fn set_stored_key(&mut self, stored_key: String) {
        self.data_mut().headers.stored_key = Some(stored_key);
    }

    /// Returns the length of the optional attachment metadata segment.
    pub fn meta_length(&self) -> Option<u32> {
        self.data().headers.get(ItemHeaderKey::MetaLength)
    }

    /// Sets the length of the optional meta segment.
    pub fn set_meta_length(&mut self, meta_length: u32) {
        self.data_mut()
            .headers
            .set(ItemHeaderKey::MetaLength, meta_length);
    }

    /// Sets the original payload length for an attachment with [`ContentType::AttachmentRef`].
    pub fn set_attachment_length(&mut self, original_length: usize) {
        debug_assert!(self.is_attachment_ref());
        self.data_mut()
            .headers
            .set(ItemHeaderKey::AttachmentLength, original_length);
    }

    /// Returns the parent entity associated with this attachment, if any.
    pub fn parent_id(&self) -> Option<ParentId> {
        // Currently only `span_id` supported in `ParentId`.
        let span_id = self.data().headers.get(ItemHeaderKey::SpanId)?;
        Some(ParentId::SpanId(span_id))
    }

    /// Sets the parent entity that this item is associated with.
    pub fn set_parent_id(&mut self, parent_id: Option<ParentId>) {
        let span_id = parent_id.map(|p| match p {
            ParentId::SpanId(span_id) => span_id,
        });
        self.data_mut()
            .headers
            .set_or_remove(ItemHeaderKey::SpanId, span_id);
    }

    /// Returns `true` if this item is an attachment with AttachmentV2 content type.
    fn is_attachment_v2(&self) -> bool {
        self.data().content_type() == Some(ContentType::TraceAttachment)
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
        self.data().content_type() == Some(ContentType::AttachmentRef)
    }

    /// Returns the [`AttachmentParentType`] of an attachment.
    ///
    /// For standard attachments (V1) always returns [`AttachmentParentType::Event`].
    pub fn attachment_parent_type(&self) -> AttachmentParentType {
        let is_trace_attachment = self.data().content_type() == Some(ContentType::TraceAttachment);

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
            self.data()
                .len()
                .saturating_sub(self.meta_length().unwrap_or(0) as usize)
        } else if self.is_attachment_ref() {
            self.data()
                .headers
                .get(ItemHeaderKey::AttachmentLength)
                .unwrap_or(0)
        } else {
            self.data().len()
        }
        .max(1)
    }

    /// Whether this attachment contains data used to build an event.
    ///
    /// Span and trace attachments never create events, regardless of subtype.
    pub fn carries_event_payload(&self) -> bool {
        if !matches!(self.attachment_parent_type(), AttachmentParentType::Event) {
            return false;
        }
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
                | AttachmentType::NvShaderDebug
                | AttachmentType::CompiledShader,
            )
            | None => false,
        }
    }
}

impl Counted for AttachmentItem {
    fn quantities(&self) -> Quantities {
        if self.data().rate_limited() {
            return Quantities::new();
        }

        // New attachment types require an explicit accounting decision.
        match self.attachment_type() {
            Some(AttachmentType::NintendoSwitchDyingMessage) => {
                // Event-parent dying messages are merged into event data.
                if matches!(self.attachment_parent_type(), AttachmentParentType::Event) {
                    return Quantities::new();
                }
            }
            Some(
                AttachmentType::Attachment
                | AttachmentType::Minidump
                | AttachmentType::AppleCrashReport
                | AttachmentType::EventPayload
                | AttachmentType::Breadcrumbs
                | AttachmentType::Prosperodump
                | AttachmentType::UnrealContext
                | AttachmentType::UnrealLogs
                | AttachmentType::ViewHierarchy
                | AttachmentType::Flamegraph
                | AttachmentType::NvGpuDump
                | AttachmentType::NvShaderDebug
                | AttachmentType::CompiledShader,
            )
            | None => {}
        }

        smallvec![
            (DataCategory::Attachment, self.attachment_body_size()),
            (DataCategory::AttachmentItem, 1),
        ]
    }
}

impl RejectionQuantities for AttachmentItem {}

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
