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

use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use failure::Fail;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use smallvec::SmallVec;

use relay_general::protocol::{EventId, EventType};
use relay_general::types::Value;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::extractors::{PartialMeta, RequestMeta};

pub const CONTENT_TYPE: &str = "application/x-sentry-envelope";

#[derive(Debug, Fail)]
pub enum EnvelopeError {
    #[fail(display = "unexpected end of file")]
    UnexpectedEof,
    #[fail(display = "missing envelope header")]
    MissingHeader,
    #[fail(display = "missing newline after header or payload")]
    MissingNewline,
    #[fail(display = "invalid envelope header")]
    InvalidHeader(#[cause] serde_json::Error),
    #[fail(display = "{} header mismatch between envelope and request", _0)]
    HeaderMismatch(&'static str),
    #[fail(display = "invalid item header")]
    InvalidItemHeader(#[cause] serde_json::Error),
    #[fail(display = "failed to write header")]
    HeaderIoFailed(#[cause] serde_json::Error),
    #[fail(display = "failed to write payload")]
    PayloadIoFailed(#[cause] io::Error),
}

/// The type of an envelope item.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemType {
    /// Event payload encoded in JSON.
    Event,
    /// Raw payload of an arbitrary attachment.
    Attachment,
    /// Multipart form data collected into a stream of JSON tuples.
    FormData,
    /// Security report as sent by the browser in JSON.
    SecurityReport,
    /// Raw compressed UE4 crash report.
    UnrealReport,
    /// User feedback encoded as JSON.
    UserReport,
    /// Session update data.
    Session,
}

impl fmt::Display for ItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Event => write!(f, "event"),
            Self::Attachment => write!(f, "attachment"),
            Self::FormData => write!(f, "form data"),
            Self::SecurityReport => write!(f, "security report"),
            Self::UnrealReport => write!(f, "unreal report"),
            Self::UserReport => write!(f, "user feedback"),
            Self::Session => write!(f, "session"),
        }
    }
}

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
    /// Any arbitrary content type not listed explicitly.
    Other(String),
}

impl ContentType {
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn as_str(&self) -> &str {
        match *self {
            Self::Text => "text/plain",
            Self::Json => "application/json",
            Self::MsgPack => "application/x-msgpack",
            Self::OctetStream => "application/octet-stream",
            Self::Other(ref other) => &other,
        }
    }

    fn from_str(content_type: &str) -> Option<Self> {
        match content_type {
            "text/plain" => Some(Self::Text),
            "application/json" => Some(Self::Json),
            "application/x-msgpack" => Some(Self::MsgPack),
            "application/octet-stream" => Some(Self::OctetStream),
            _ => None,
        }
    }
}

impl From<String> for ContentType {
    fn from(content_type: String) -> Self {
        Self::from_str(&content_type).unwrap_or_else(|| ContentType::Other(content_type))
    }
}

impl From<&'_ str> for ContentType {
    fn from(content_type: &str) -> Self {
        Self::from_str(&content_type).unwrap_or_else(|| ContentType::Other(content_type.to_owned()))
    }
}

impl Serialize for ContentType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let string = match *self {
            Self::Text => "text/plain",
            Self::Json => "application/json",
            Self::MsgPack => "application/x-msgpack",
            Self::OctetStream => "application/octet-stream",
            Self::Other(ref other) => other,
        };

        serializer.serialize_str(string)
    }
}

impl<'de> Deserialize<'de> for ContentType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let content_type = Cow::<'_, str>::deserialize(deserializer)?;
        Ok(Self::from_str(&content_type)
            .unwrap_or_else(|| ContentType::Other(content_type.into_owned())))
    }
}

/// The type of an event attachment.
///
/// These item types must align with the Sentry processing pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum AttachmentType {
    /// A regular attachment without special meaning.
    #[serde(rename = "event.attachment")]
    Attachment,

    /// A minidump crash report (binary data).
    #[serde(rename = "event.minidump")]
    Minidump,

    /// An apple crash report (text data).
    #[serde(rename = "event.applecrashreport")]
    AppleCrashReport,

    /// A msgpack-encoded event payload submitted as part of multipart uploads.
    ///
    /// This attachment is processed by Relay immediately and never forwarded or persisted.
    #[serde(rename = "event.payload")]
    EventPayload,

    /// A msgpack-encoded list of payloads.
    ///
    /// There can be two attachments that the SDK may use as swappable buffers. Both attachments
    /// will be merged and truncated to the maxmimum number of allowed attachments.
    ///
    /// This attachment is processed by Relay immediately and never forwarded or persisted.
    #[serde(rename = "event.breadcrumbs")]
    Breadcrumbs,

    /// This is a binary attachment present in Unreal 4 events containing event context information.
    /// This can be deserialized using the `symbolic` crate see [unreal::Unreal4Context]
    #[serde(rename = "unreal.context")]
    UnrealContext,

    /// This is a binary attachment present in Unreal 4 events containing event Logs.
    /// This can be deserialized using the `symbolic` crate see [unreal::Unreal4LogEntry]
    #[serde(rename = "unreal.logs")]
    UnrealLogs,
}

impl Default for AttachmentType {
    fn default() -> Self {
        Self::Attachment
    }
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

    /// If this is an event item, this may contain the event type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_type: Option<EventType>,

    /// If this is an attachment item, this may contain the attachment type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attachment_type: Option<AttachmentType>,

    /// Content type of the payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<ContentType>,

    /// If this is an attachment item, this may contain the original file name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    filename: Option<String>,

    /// If this item came from a multipart request, this may contain the field name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    /// Other attributes for forward compatibility.
    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Item {
    headers: ItemHeaders,
    payload: Bytes,
}

impl Item {
    fn new_unchecked(ty: ItemType) -> Self {
        Self {
            headers: ItemHeaders {
                ty,
                length: Some(0),
                event_type: None,
                attachment_type: None,
                content_type: None,
                filename: None,
                name: None,
                other: BTreeMap::new(),
            },
            payload: Bytes::new(),
        }
    }

    /// Creates a new item with the given type.
    pub fn new(ty: ItemType) -> Self {
        debug_assert!(ty != ItemType::Event, "use Item::new_event instead");
        Self::new_unchecked(ty)
    }

    /// Creates an `Event` item with the given event type.
    pub fn new_event(event_type: EventType) -> Self {
        let mut item = Self::new_unchecked(ItemType::Event);
        item.headers.event_type = Some(event_type);
        item
    }

    /// Returns the `ItemType` of this item.
    pub fn ty(&self) -> ItemType {
        self.headers.ty
    }

    /// Returns the length of this item's payload.
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    /// Returns `true` if this item's payload is empty.
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Returns the content type of this item's payload.
    pub fn content_type(&self) -> Option<&ContentType> {
        self.headers.content_type.as_ref()
    }

    /// Returns the event type if this item is an event.
    pub fn event_type(&self) -> Option<EventType> {
        self.headers.event_type
    }

    /// Sets the event type of this item.
    pub fn set_event_type(&mut self, event_type: EventType) {
        self.headers.event_type = Some(event_type);
    }

    /// Returns the attachment type if this item is an attachment.
    pub fn attachment_type(&self) -> Option<AttachmentType> {
        self.headers.attachment_type
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

    /// Returns the name header of the item.
    pub fn name(&self) -> Option<&str> {
        self.headers.name.as_deref()
    }

    /// Sets the name header of the item.
    pub fn set_name<S>(&mut self, name: S)
    where
        S: Into<String>,
    {
        self.headers.name = Some(name.into());
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
            ItemType::Event | ItemType::SecurityReport | ItemType::UnrealReport => true,

            // Attachments are only event items if they are crash reports.
            ItemType::Attachment => match self.attachment_type().unwrap_or_default() {
                AttachmentType::AppleCrashReport
                | AttachmentType::Minidump
                | AttachmentType::EventPayload
                | AttachmentType::Breadcrumbs => true,
                AttachmentType::Attachment
                | AttachmentType::UnrealContext
                | AttachmentType::UnrealLogs => false,
            },

            // Form data items may contain partial event payloads, but those are only ever valid if
            // they occur together with an explicit event item, such as a minidump or apple crash
            // report. For this reason, FormData alone does not constitute an event item.
            ItemType::FormData => false,

            // The remaining item types cannot carry event payloads.
            ItemType::UserReport | ItemType::Session => false,
        }
    }

    /// Determines whether the given item requires an event with identifier.
    ///
    /// This is true for all items except session health events.
    pub fn requires_event(&self) -> bool {
        match self.ty() {
            ItemType::Event => true,
            ItemType::Attachment => true,
            ItemType::FormData => true,
            ItemType::SecurityReport => true,
            ItemType::UnrealReport => true,
            ItemType::UserReport => true,
            ItemType::Session => false,
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
        // again once the event goes into the EventManager, but we want to be as accurate as
        // possible in the endpoint already.
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
    /// Creates an envelope from request information.
    pub fn from_request(event_id: Option<EventId>, meta: RequestMeta) -> Self {
        Self {
            headers: EnvelopeHeaders {
                event_id,
                meta,
                retention: None,
                sent_at: None,
                other: BTreeMap::new(),
            },
            items: Items::new(),
        }
    }

    /// Parses an envelope from bytes.
    #[allow(dead_code)]
    pub fn parse_bytes(bytes: Bytes) -> Result<Self, EnvelopeError> {
        let (headers, offset) = Self::parse_headers(&bytes)?;
        let items = Self::parse_items(&bytes, offset)?;

        Ok(Envelope { headers, items })
    }

    /// Parses an envelope taking into account a request.
    ///
    /// This method is intended to be used when parsing an envelope that was sent as part of a web
    /// request. It validates that request headers are in line with the envelope's headers.
    ///
    /// If no event id is provided explicitly, one is created on the fly.
    pub fn parse_request(bytes: Bytes, request_meta: RequestMeta) -> Result<Self, EnvelopeError> {
        let (partial_headers, offset) = Self::parse_headers::<PartialMeta>(&bytes)?;
        let mut headers = partial_headers.complete(request_meta)?;

        // Event-related envelopes *must* contain an event id.
        let items = Self::parse_items(&bytes, offset)?;
        if items.iter().any(Item::requires_event) {
            headers.event_id.get_or_insert_with(EventId::new);
        }

        Ok(Envelope { headers, items })
    }

    /// Splits the envelope by the given predicate.
    ///
    /// The predicate passed to `split_by()` can return `true`, or `false`. If it returns `true` or
    /// `false` for all items, then this returns `None`. Otherwise, a new envelope is constructed
    /// with all items that return `true`. Items that return `false` remain in this envelope.
    ///
    /// The returned envelope assumes the same headers.
    pub fn split_by<F>(&mut self, mut f: F) -> Option<Self>
    where
        F: FnMut(&Item) -> bool,
    {
        let split_count = self.items().filter(|item| f(item)).count();
        if split_count == self.len() || split_count == 0 {
            return None;
        }

        let old_items = std::mem::take(&mut self.items);
        let (split_items, own_items) = old_items.into_iter().partition(f);
        self.items = own_items;

        Some(Envelope {
            headers: self.headers.clone(),
            items: split_items,
        })
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

    /// Returns the data retention in days for items in this envelope.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn retention(&self) -> u16 {
        self.headers.retention.unwrap_or(DEFAULT_EVENT_RETENTION)
    }

    /// When the event has been sent, according to the SDK.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn sent_at(&self) -> Option<DateTime<Utc>> {
        self.headers.sent_at
    }

    /// Sets the data retention in days for items in this envelope.
    pub fn set_retention(&mut self, retention: u16) {
        self.headers.retention = Some(retention);
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

    /// Returns an iterator over items in this envelope.
    ///
    /// Note that iteration order may change when using `take_item`.
    pub fn items_mut(&mut self) -> ItemIterMut<'_> {
        self.items.iter_mut()
    }

    /// Returns the an option with a reference to the first item that matches
    /// the predicate, or None if the predicate is not matched by any item.
    pub fn get_item_by<F>(&self, mut pred: F) -> Option<&Item>
    where
        F: FnMut(&Item) -> bool,
    {
        self.items().find(|item| pred(item))
    }

    /// Removes and returns the first item that matches the given condition.
    pub fn take_item_by<F>(&mut self, cond: F) -> Option<Item>
    where
        F: FnMut(&Item) -> bool,
    {
        let index = self.items.iter().position(cond);
        index.map(|index| self.items.swap_remove(index))
    }

    /// Adds a new item to this envelope.
    pub fn add_item(&mut self, item: Item) {
        self.items.push(item)
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
            let (item, item_size) = Self::parse_item(bytes.slice_from(offset))?;
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

        let payload_start = headers_end + 1;
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

        let payload = bytes.slice(payload_start, payload_end);
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
    use super::*;

    use relay_common::ProjectId;

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
        let mut item = Item::new_event(EventType::Default);

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
        let mut item = Item::new_event(EventType::Default);
        item.set_header("custom", 42u64);

        assert_eq!(item.get_header("custom"), Some(&Value::from(42u64)));
        assert_eq!(item.get_header("anything"), None);
    }

    #[test]
    fn test_envelope_empty() {
        let event_id = EventId::new();
        let envelope = Envelope::from_request(Some(event_id), request_meta());

        assert_eq!(envelope.event_id(), Some(event_id));
        assert_eq!(envelope.len(), 0);
        assert!(envelope.is_empty());

        let items: Vec<_> = envelope.items().collect();
        assert!(items.is_empty());
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
        assert_eq!(items[0].ty(), ItemType::Attachment);
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
            .take_item_by(|item| item.ty() == ItemType::Attachment)
            .expect("should return some item");

        assert_eq!(taken.filename(), Some("item1"));

        assert!(envelope
            .take_item_by(|item| item.ty() == ItemType::Event)
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
        assert_eq!(meta.project_id(), ProjectId::new(42));
        assert_eq!(meta.public_key(), "e12d836b15bb49d7bbf99e64295d995b");
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

        assert_eq!(items[0].ty(), ItemType::Attachment);
        assert_eq!(items[0].len(), 10);
        assert_eq!(
            items[0].payload(),
            Bytes::from(&b"\xef\xbb\xbfHello\r\n"[..])
        );
        assert_eq!(items[0].content_type(), Some(&ContentType::Text));

        assert_eq!(items[1].ty(), ItemType::Event);
        assert_eq!(items[1].len(), 41);
        assert_eq!(
            items[1].payload(),
            Bytes::from("{\"message\":\"hello world\",\"level\":\"error\"}")
        );
        assert_eq!(items[1].content_type(), Some(&ContentType::Json));
        assert_eq!(items[1].filename(), Some("application.log"));
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
        assert_eq!(meta.project_id(), ProjectId::new(42));
        assert_eq!(meta.public_key(), "e12d836b15bb49d7bbf99e64295d995b");
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
        insta::assert_snapshot!(stringified, @r###"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","client":"sentry/client","version":7,"origin":"http://origin/","remote_addr":"192.168.0.1","user_agent":"sentry/agent"}
"###);
    }

    #[test]
    fn test_serialize_envelope_attachments() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());

        let mut item = Item::new_event(EventType::Default);
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
        insta::assert_snapshot!(stringified, @r###"
        {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","client":"sentry/client","version":7,"origin":"http://origin/","remote_addr":"192.168.0.1","user_agent":"sentry/agent"}
        {"type":"event","length":41,"event_type":"default","content_type":"application/json"}
        {"message":"hello world","level":"error"}
        {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
        Hello

        "###);
    }
}
