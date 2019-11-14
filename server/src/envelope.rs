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
//! {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","auth":"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b, sentry_version=7"}
//! {"type":"event","length":41,"content_type":"application/json"}
//! {"message":"hello world","level":"error"}
//! {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
//! Hello
//!
//! ```

#![allow(unused)]

use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};

use bytes::Bytes;
use failure::Fail;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use semaphore_general::protocol::{EventId, EventType};
use semaphore_general::types::Value;

use crate::extractors::EventMeta;

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
    #[fail(display = "invalid item header")]
    InvalidItemHeader(#[cause] serde_json::Error),
    #[fail(display = "failed to write header")]
    HeaderIoFailed(#[cause] serde_json::Error),
    #[fail(display = "failed to write payload")]
    PayloadIoFailed(#[cause] io::Error),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemType {
    Event,
    Attachment,
    FormData,
    SecurityReport,
}

impl fmt::Display for ItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Event => write!(f, "event"),
            Self::Attachment => write!(f, "attachment"),
            Self::FormData => write!(f, "form data"),
            Self::SecurityReport => write!(f, "security report"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ContentType {
    Text,
    Json,
    MsgPack,
    OctetStream,
    Other(String),
}

impl ContentType {
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ItemHeaders {
    #[serde(rename = "type")]
    ty: ItemType,

    length: u32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_type: Option<EventType>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<ContentType>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    filename: Option<String>,

    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Item {
    headers: ItemHeaders,
    payload: Bytes,
}

impl Item {
    pub fn new(ty: ItemType) -> Self {
        Self {
            headers: ItemHeaders {
                ty,
                length: 0,
                event_type: None,
                content_type: None,
                filename: None,
                other: BTreeMap::new(),
            },
            payload: Bytes::new(),
        }
    }

    pub fn ty(&self) -> ItemType {
        self.headers.ty
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    pub fn content_type(&self) -> Option<&ContentType> {
        self.headers.content_type.as_ref()
    }

    pub fn event_type(&self) -> Option<EventType> {
        self.headers.event_type
    }

    pub fn set_event_type(&mut self, event_type: EventType) {
        self.headers.event_type = Some(event_type);
    }

    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    pub fn set_payload<B>(&mut self, content_type: ContentType, payload: B)
    where
        B: Into<Bytes>,
    {
        let mut payload = payload.into();

        let length = std::cmp::min(u32::max_value() as usize, payload.len());
        payload.truncate(length);

        self.headers.length = length as u32;
        self.headers.content_type = Some(content_type);
        self.payload = payload;
    }

    pub fn filename(&self) -> Option<&str> {
        self.headers.filename.as_ref().map(String::as_str)
    }

    pub fn set_filename<S>(&mut self, filename: S)
    where
        S: Into<String>,
    {
        self.headers.filename = Some(filename.into());
    }

    pub fn get_header<K>(&self, name: &K) -> Option<&Value>
    where
        String: Borrow<K>,
        K: Ord + ?Sized,
    {
        self.headers.other.get(name)
    }

    pub fn set_header<S, V>(&mut self, name: S, value: V) -> Option<Value>
    where
        S: Into<String>,
        V: Into<Value>,
    {
        self.headers.other.insert(name.into(), value.into())
    }
}

pub type ItemIter<'a> = std::slice::Iter<'a, Item>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EnvelopeHeaders {
    /// Unique identifier of the event associated to this envelope.
    event_id: EventId,

    #[serde(flatten)]
    meta: EventMeta,

    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Envelope {
    headers: EnvelopeHeaders,
    items: SmallVec<[Item; 3]>,
}

impl Envelope {
    pub fn from_request(event_id: EventId, meta: EventMeta) -> Self {
        Self {
            headers: EnvelopeHeaders {
                event_id,
                meta,
                other: BTreeMap::new(),
            },
            items: SmallVec::new(),
        }
    }

    pub fn parse_bytes(bytes: Bytes) -> Result<Self, EnvelopeError> {
        let (headers, mut offset) = Self::parse_headers(&bytes)?;

        let mut envelope = Envelope {
            headers,
            items: SmallVec::new(),
        };

        while offset < bytes.len() {
            let (item, item_size) = Self::parse_item(bytes.slice_from(offset))?;
            offset += item_size;
            envelope.items.push(item);
        }

        Ok(envelope)
    }

    pub fn parse_request(bytes: Bytes, meta: EventMeta) -> Result<Self, EnvelopeError> {
        let mut envelope = Self::parse_bytes(bytes)?;
        envelope.headers.meta.default_to(meta);
        Ok(envelope)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Unique identifier of the event associated to this envelope.
    ///
    /// The envelope may directly contain an event which has this id. Alternatively, it can contain
    /// payloads that can be transformed into events (such as security reports). Lastly, there can
    /// be additional information to an event, such as attachments.
    pub fn event_id(&self) -> EventId {
        self.headers.event_id
    }

    pub fn meta(&self) -> &EventMeta {
        &self.headers.meta
    }

    pub fn items(&self) -> ItemIter<'_> {
        self.items.iter()
    }

    pub fn get_item(&self, ty: ItemType) -> Option<&Item> {
        self.items().find(|item| item.ty() == ty)
    }

    pub fn take_item(&mut self, ty: ItemType) -> Option<Item> {
        let index = self.items.iter().position(|item| item.ty() == ty);
        index.map(|index| self.items.swap_remove(index))
    }

    pub fn add_item(&mut self, item: Item) {
        self.items.push(item)
    }

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

    pub fn to_vec(&self) -> Result<Vec<u8>, EnvelopeError> {
        let mut vec = Vec::new(); // TODO: Preallocate?
        self.serialize(&mut vec)?;
        Ok(vec)
    }

    fn parse_headers(slice: &[u8]) -> Result<(EnvelopeHeaders, usize), EnvelopeError> {
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
        let payload_end = payload_start + headers.length as usize;
        if bytes.len() < payload_end {
            // NB: `Bytes::slice` panics if the indices are out of range.
            return Err(EnvelopeError::UnexpectedEof);
        }

        // Each payload is terminated by a UNIX newline.
        Self::require_termination(slice, headers_end)?;

        let payload = bytes.slice(payload_start, payload_end);
        let item = Item { headers, payload };

        Ok((item, payload_end + 1))
    }

    fn require_termination(slice: &[u8], offset: usize) -> Result<(), EnvelopeError> {
        match slice.get(offset) {
            Some(&b'\n') | None => Ok(()),
            _ => Err(EnvelopeError::MissingNewline),
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

    fn event_meta() -> EventMeta {
        let auth = "Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b"
            .parse()
            .unwrap();

        EventMeta::new(auth)
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
    fn test_envelope_empty() {
        let event_id = EventId::new();
        let envelope = Envelope::from_request(event_id, event_meta());

        assert_eq!(envelope.event_id(), event_id);
        assert_eq!(envelope.len(), 0);
        assert!(envelope.is_empty());

        let items: Vec<_> = envelope.items().collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_envelope_add_item() {
        let event_id = EventId::new();
        let mut envelope = Envelope::from_request(event_id, event_meta());
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
        let mut envelope = Envelope::from_request(event_id, event_meta());

        let mut item1 = Item::new(ItemType::Attachment);
        item1.set_filename("item1");
        envelope.add_item(item1);

        let mut item2 = Item::new(ItemType::Attachment);
        item2.set_filename("item2");
        envelope.add_item(item2);

        let taken = envelope
            .take_item(ItemType::Attachment)
            .expect("should return some item");

        assert_eq!(taken.filename(), Some("item1"));

        assert!(envelope.take_item(ItemType::Event).is_none());
    }

    #[test]
    fn test_deserialize_envelope_empty() {
        // Without terminating newline after header
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"auth\":\"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b\"}");
        let envelope = Envelope::parse_bytes(bytes).unwrap();

        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        assert_eq!(envelope.event_id(), event_id);
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_empty_newline() {
        // With terminating newline after header
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"auth\":\"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b\"}\n");
        let envelope = Envelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_empty_item_newline() {
        // With terminating newline after item payload
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"auth\":\"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b\"}\n\
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
    fn test_deserialize_envelope_multiple_items() {
        // With terminating newline
        let bytes = Bytes::from(&b"\
            {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"auth\":\"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b\"}\n\
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
    fn test_serialize_envelope_empty() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let envelope = Envelope::from_request(event_id, event_meta());

        let mut buffer = Vec::new();
        envelope.serialize(&mut buffer).unwrap();

        let stringified = String::from_utf8_lossy(&buffer);
        insta::assert_snapshot!(stringified, @r###"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","auth":"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b, sentry_version=7"}
"###);
    }

    #[test]
    fn test_serialize_envelope_attachments() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let mut envelope = Envelope::from_request(event_id, event_meta());

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
        insta::assert_snapshot!(stringified, @r###"
        {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","auth":"Sentry sentry_key=e12d836b15bb49d7bbf99e64295d995b, sentry_version=7"}
        {"type":"event","length":41,"content_type":"application/json"}
        {"message":"hello world","level":"error"}
        {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
        Hello

        "###);
    }
}
