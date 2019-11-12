use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};

use bytes::Bytes;
use failure::Fail;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use smallvec::SmallVec;

use semaphore_general::protocol::EventId;

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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutgoingItemType {
    Event,
    Attachment,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncomingItemType {
    Event,
    Attachment,
    FormData,
    SecurityReport,
}

impl fmt::Display for IncomingItemType {
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
        Ok(match content_type.as_ref() {
            "text/plain" => Self::Text,
            "application/json" => Self::Json,
            "application/x-msgpack" => Self::MsgPack,
            "application/octet-stream" => Self::OctetStream,
            _ => Self::Other(content_type.into_owned()),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ItemHeaders<T> {
    #[serde(rename = "type")]
    ty: T,

    length: u32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<ContentType>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    filename: Option<String>,

    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Item<T> {
    headers: ItemHeaders<T>,
    payload: Bytes,
}

impl<T> Item<T>
where
    T: Copy,
{
    pub fn new(ty: T) -> Self {
        Self {
            headers: ItemHeaders {
                ty,
                length: 0,
                content_type: None,
                filename: None,
                other: BTreeMap::new(),
            },
            payload: Bytes::new(),
        }
    }

    pub fn ty(&self) -> T {
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

pub type ItemIter<'a, T> = std::slice::Iter<'a, Item<T>>;
pub type ItemIterMut<'a, T> = std::slice::IterMut<'a, Item<T>>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EnvelopeHeaders {
    event_id: EventId,
    #[serde(flatten)]
    other: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Envelope<T> {
    headers: EnvelopeHeaders,
    items: SmallVec<[Item<T>; 3]>,
}

impl<T> Envelope<T>
where
    T: Copy + PartialEq,
{
    pub fn new(event_id: EventId) -> Self {
        Self {
            headers: EnvelopeHeaders {
                event_id,
                other: BTreeMap::new(),
            },
            items: SmallVec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn event_id(&self) -> EventId {
        self.headers.event_id
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

    pub fn items(&self) -> ItemIter<'_, T> {
        self.items.iter()
    }

    pub fn take_item(&mut self, ty: T) -> Option<Item<T>> {
        let index = self.items.iter().position(|i| i.ty() == ty);
        index.map(|index| self.items.swap_remove(index))
    }

    pub fn add_item(&mut self, item: Item<T>) {
        self.items.push(item)
    }
}

impl<T> Envelope<T>
where
    T: DeserializeOwned,
{
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

    fn parse_item(bytes: Bytes) -> Result<(Item<T>, usize), EnvelopeError> {
        let slice = bytes.as_ref();
        let mut stream = serde_json::Deserializer::from_slice(slice).into_iter();

        let headers: ItemHeaders<T> = match stream.next() {
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
}

impl<T> Envelope<T>
where
    T: Serialize,
{
    pub fn to_vec(&self) -> Result<Vec<u8>, EnvelopeError> {
        let mut vec = Vec::new(); // TODO: Preallocate?
        self.serialize(&mut vec)?;
        Ok(vec)
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

    fn write<W>(&self, mut writer: W, buf: &[u8]) -> Result<(), EnvelopeError>
    where
        W: Write,
    {
        writer
            .write_all(buf)
            .map_err(EnvelopeError::PayloadIoFailed)
    }
}

pub type IncomingEnvelope = Envelope<IncomingItemType>;
pub type IncomingItem = Item<IncomingItemType>;

pub type OutgoingEnvelope = Envelope<OutgoingItemType>;
pub type OutgoingItem = Item<OutgoingItemType>;

#[cfg(test)]
mod tests {
    use super::*;

    use semaphore_common::Uuid;

    #[test]
    fn test_item_empty() {
        let item = Item::new(OutgoingItemType::Attachment);

        assert_eq!(item.payload(), Bytes::new());
        assert_eq!(item.len(), 0);
        assert!(item.is_empty());

        assert_eq!(item.content_type(), None);
    }

    #[test]
    fn test_item_set_payload() {
        let mut item = Item::new(OutgoingItemType::Event);

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
        let mut item = Item::new(OutgoingItemType::Event);
        item.set_header("custom", 42);

        assert_eq!(item.get_header("custom"), Some(&Value::from(42)));
        assert_eq!(item.get_header("anything"), None);
    }

    #[test]
    fn test_envelope_empty() {
        let event_id = EventId::new();
        let envelope = OutgoingEnvelope::new(event_id);

        assert_eq!(envelope.event_id(), event_id);
        assert_eq!(envelope.len(), 0);
        assert!(envelope.is_empty());

        let items: Vec<_> = envelope.items().collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_envelope_add_item() {
        let event_id = EventId::new();
        let mut envelope = OutgoingEnvelope::new(event_id);
        envelope.add_item(Item::new(OutgoingItemType::Attachment));

        assert_eq!(envelope.len(), 1);
        assert!(!envelope.is_empty());

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].ty(), OutgoingItemType::Attachment);
    }

    #[test]
    fn test_envelope_set_header() {
        let event_id = EventId::new();
        let mut envelope = OutgoingEnvelope::new(event_id);
        envelope.set_header("custom", 42);

        assert_eq!(envelope.get_header("custom"), Some(&Value::from(42)));
        assert_eq!(envelope.get_header("anything"), None);
    }

    #[test]
    fn test_envelope_take_item() {
        let event_id = EventId::new();
        let mut envelope = OutgoingEnvelope::new(event_id);

        let mut item1 = Item::new(OutgoingItemType::Attachment);
        item1.set_filename("item1");
        envelope.add_item(item1);

        let mut item2 = Item::new(OutgoingItemType::Attachment);
        item2.set_filename("item2");
        envelope.add_item(item2);

        let taken = envelope
            .take_item(OutgoingItemType::Attachment)
            .expect("should return some item");

        assert_eq!(taken.filename(), Some("item1"));

        assert!(envelope.take_item(OutgoingItemType::Event).is_none());
    }

    #[test]
    fn test_deserialize_envelope_empty() {
        // Without terminating newline after header
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}");
        let envelope = IncomingEnvelope::parse_bytes(bytes).unwrap();

        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        assert_eq!(envelope.event_id(), event_id);
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_empty_newline() {
        // With terminating newline after header
        let bytes = Bytes::from("{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}\n");
        let envelope = IncomingEnvelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_empty_item_newline() {
        // With terminating newline after item payload
        let bytes = Bytes::from(
            "\
             {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}\n\
             {\"type\":\"attachment\",\"length\":0}\n\
             \n\
             {\"type\":\"attachment\",\"length\":0}\n\
             ",
        );

        let envelope = IncomingEnvelope::parse_bytes(bytes).unwrap();
        assert_eq!(envelope.len(), 2);

        let items: Vec<_> = envelope.items().collect();
        assert_eq!(items[0].len(), 0);
        assert_eq!(items[1].len(), 0);
    }

    #[test]
    fn test_deserialize_envelope_multiple_items() {
        // With terminating newline
        let bytes = Bytes::from(&b"\
            {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}\n\
            {\"type\":\"attachment\",\"length\":10,\"content_type\":\"text/plain\",\"filename\":\"hello.txt\"}\n\
            \xef\xbb\xbfHello\r\n\n\
            {\"type\":\"event\",\"length\":41,\"content_type\":\"application/json\",\"filename\":\"application.log\"}\n\
            {\"message\":\"hello world\",\"level\":\"error\"}\n\
        "[..]);

        let envelope = IncomingEnvelope::parse_bytes(bytes).unwrap();

        assert_eq!(envelope.len(), 2);
        let items: Vec<_> = envelope.items().collect();

        assert_eq!(items[0].ty(), IncomingItemType::Attachment);
        assert_eq!(items[0].len(), 10);
        assert_eq!(
            items[0].payload(),
            Bytes::from(&b"\xef\xbb\xbfHello\r\n"[..])
        );
        assert_eq!(items[0].content_type(), Some(&ContentType::Text));

        assert_eq!(items[1].ty(), IncomingItemType::Event);
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
        let envelope = OutgoingEnvelope::new(event_id);

        let mut buffer = Vec::new();
        envelope.serialize(&mut buffer).unwrap();

        let stringified = String::from_utf8_lossy(&buffer);
        insta::assert_snapshot!(stringified, @r###"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc"}
"###);
    }

    #[test]
    fn test_serialize_envelope_attachments() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let mut envelope = OutgoingEnvelope::new(event_id);

        let mut item = Item::new(OutgoingItemType::Event);
        item.set_payload(
            ContentType::Json,
            "{\"message\":\"hello world\",\"level\":\"error\"}",
        );
        envelope.add_item(item);

        let mut item = Item::new(OutgoingItemType::Attachment);
        item.set_payload(ContentType::Text, &b"Hello\r\n"[..]);
        item.set_filename("application.log");
        envelope.add_item(item);

        let mut buffer = Vec::new();
        envelope.serialize(&mut buffer).unwrap();

        let stringified = String::from_utf8_lossy(&buffer);
        insta::assert_snapshot!(stringified, @r###"
        {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc"}
        {"type":"event","length":41,"content_type":"application/json"}
        {"message":"hello world","level":"error"}
        {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
        Hello

        "###);
    }
}
