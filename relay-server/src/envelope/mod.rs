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

use relay_base_schema::project::ProjectKey;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::{TransactionNameRule, normalize_transaction_name};
use relay_event_schema::protocol::{Event, EventId};
use relay_protocol::{Annotated, Value};
use relay_sampling::DynamicSamplingContext;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::extractors::{PartialMeta, RequestMeta};

mod attachment;
mod container;
mod content_type;
mod item;

pub use self::attachment::*;
pub use self::container::*;
pub use self::content_type::*;
pub use self::item::*;

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

    /// Data retention in days for the items of this envelope.
    ///
    /// This value is always overwritten in processing mode by the value specified in the project
    /// configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    downsampled_retention: Option<u16>,

    /// Timestamp when the event has been sent, according to the SDK.
    ///
    /// This can be used to perform drift correction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sent_at: Option<DateTime<Utc>>,

    /// Trace context associated with the request
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace: Option<ErrorBoundary<DynamicSamplingContext>>,

    /// A list of features required to process this envelope.
    ///
    /// This is an internal field that should only be set by Relay.
    /// It is a serializable header such that it persists when spooling.
    #[serde(default, skip_serializing_if = "SmallVec::is_empty")]
    required_features: SmallVec<[Feature; 1]>,

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
            downsampled_retention: self.downsampled_retention,
            sent_at: self.sent_at,
            trace: self.trace,
            required_features: self.required_features,
            other: self.other,
        })
    }
}

impl<M> EnvelopeHeaders<M> {
    /// Returns a reference to the contained meta.
    pub fn meta(&self) -> &M {
        &self.meta
    }
}

#[doc(hidden)]
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

    /// Creates an envelope from headers and an envelope.
    pub fn try_from_event(
        mut headers: EnvelopeHeaders,
        event: Event,
    ) -> Result<Box<Self>, serde_json::Error> {
        headers.event_id = event.id.value().copied();
        let event_type = event.ty.value().copied().unwrap_or_default();

        let serialized = Annotated::new(event).to_json()?;
        let mut item = Item::new(ItemType::from_event_type(event_type));
        item.set_payload(ContentType::Json, serialized);

        Ok(Self::from_parts(headers, smallvec::smallvec![item]))
    }

    /// Creates an envelope from request information.
    pub fn from_request(event_id: Option<EventId>, meta: RequestMeta) -> Box<Self> {
        Box::new(Self {
            headers: EnvelopeHeaders {
                event_id,
                meta,
                retention: None,
                downsampled_retention: None,
                sent_at: None,
                other: BTreeMap::new(),
                trace: None,
                required_features: smallvec::smallvec![],
            },
            items: Items::new(),
        })
    }

    /// Parses an envelope from bytes.
    pub fn parse_bytes(bytes: Bytes) -> Result<Box<Self>, EnvelopeError> {
        let (headers, offset) = Self::parse_headers(&bytes)?;
        let items = Self::parse_items(&bytes, offset)?;

        Ok(Box::new(Envelope { headers, items }))
    }

    /// Parse envelope items from bytes buffer that doesn't contain a complete envelope.
    ///
    /// Note: the envelope header must not be present in the data. Use [`Self::parse_bytes`] instead.
    pub fn parse_items_bytes(bytes: Bytes) -> Result<Items, EnvelopeError> {
        Self::parse_items(&bytes, 0)
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
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if this envelope does not contain any items.
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

    /// Returns the data retention in days for items in this envelope.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn downsampled_retention(&self) -> u16 {
        self.headers
            .downsampled_retention
            .unwrap_or(self.retention())
    }

    /// When the event has been sent, according to the SDK.
    pub fn sent_at(&self) -> Option<DateTime<Utc>> {
        self.headers.sent_at
    }

    /// Returns the project key defined in the `trace` header of the envelope.
    ///
    /// This function returns `None` if:
    ///  - there is no [`DynamicSamplingContext`] in the envelope headers.
    ///  - there are no transactions or events in the envelope, since in this case sampling by trace is redundant.
    pub fn sampling_key(&self) -> Option<ProjectKey> {
        // If the envelope item is not of type transaction or event, we will not return a sampling key
        // because it doesn't make sense to load the root project state if we don't perform trace
        // sampling.
        self.get_item_by(|item| {
            matches!(
                item.ty(),
                ItemType::Transaction | ItemType::Event | ItemType::Span
            )
        })?;
        self.dsc().map(|dsc| dsc.public_key)
    }

    /// Returns the time at which the envelope was received at this Relay.
    pub fn received_at(&self) -> DateTime<Utc> {
        self.meta().received_at()
    }

    /// Returns the time elapsed in seconds since the envelope was received by this Relay.
    ///
    /// In case the elapsed time is negative, it is assumed that no time elapsed.
    pub fn age(&self) -> Duration {
        (Utc::now() - self.received_at())
            .to_std()
            .unwrap_or(Duration::ZERO)
    }

    /// Sets the event id on the envelope.
    pub fn set_event_id(&mut self, event_id: EventId) {
        self.headers.event_id = Some(event_id);
    }

    /// Sets the timestamp at which an envelope is sent to the upstream.
    pub fn set_sent_at(&mut self, sent_at: DateTime<Utc>) {
        self.headers.sent_at = Some(sent_at);
    }

    /// Sets the received at to the provided `DateTime`.
    pub fn set_received_at(&mut self, start_time: DateTime<Utc>) {
        self.headers.meta.set_received_at(start_time)
    }

    /// Sets the data retention in days for items in this envelope.
    pub fn set_retention(&mut self, retention: u16) {
        self.headers.retention = Some(retention);
    }

    /// Sets the data retention in days for items in this envelope.
    pub fn set_downsampled_retention(&mut self, retention: u16) {
        self.headers.downsampled_retention = Some(retention);
    }

    /// Runs transaction parametrization on the DSC trace transaction.
    ///
    /// The purpose is for trace rules to match on the parametrized version of the transaction.
    pub fn parametrize_dsc_transaction(&mut self, rules: &[TransactionNameRule]) {
        let Some(ErrorBoundary::Ok(dsc)) = &mut self.headers.trace else {
            return;
        };

        let parametrized_transaction = match &dsc.transaction {
            Some(transaction) if transaction.contains('/') => {
                // Ideally we would only apply transaction rules to transactions with source `url`,
                // but the DSC does not contain this information. The chance of a transaction rename rule
                // accidentially matching a non-URL transaction should be very low.
                let mut annotated = Annotated::new(transaction.clone());
                normalize_transaction_name(&mut annotated, rules);
                annotated.into_value()
            }
            _ => return,
        };

        dsc.transaction = parametrized_transaction;
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

    /// Removes the dynamic sampling context from envelope headers.
    pub fn remove_dsc(&mut self) {
        self.headers.trace = None;
    }

    /// Features required to process this envelope.
    pub fn required_features(&self) -> &[Feature] {
        &self.headers.required_features
    }

    /// Add a feature requirement to this envelope.
    pub fn require_feature(&mut self, feature: Feature) {
        self.headers.required_features.push(feature)
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

    /// Drops every item in the envelope.
    pub fn drop_items_silently(&mut self) {
        self.items.clear()
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
        require_termination(slice, stream.byte_offset())?;

        Ok((headers, stream.byte_offset() + 1))
    }

    fn parse_items(bytes: &Bytes, mut offset: usize) -> Result<Items, EnvelopeError> {
        let mut items = Items::new();

        while offset < bytes.len() {
            let (item, item_size) = Item::parse(bytes.slice(offset..))?;
            offset += item_size;
            items.push(item);
        }

        Ok(items)
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

fn require_termination(slice: &[u8], offset: usize) -> Result<(), EnvelopeError> {
    match slice.get(offset) {
        Some(&b'\n') | None => Ok(()),
        Some(_) => Err(EnvelopeError::MissingNewline),
    }
}

#[cfg(test)]
mod tests {
    use relay_base_schema::project::{ProjectId, ProjectKey};

    use super::*;

    fn request_meta() -> RequestMeta {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
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

        assert!(
            envelope
                .take_item_by(|item| item.ty() == &ItemType::Event)
                .is_none()
        );
    }

    #[test]
    fn test_deserialize_envelope_empty() {
        // Without terminating newline after header
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}",
        );
        let envelope = Envelope::parse_bytes(bytes).unwrap();

        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        assert_eq!(envelope.event_id(), Some(event_id));
        assert_eq!(envelope.len(), 0);
    }

    #[test]
    fn test_deserialize_request_meta() {
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@other.sentry.io/42\",\"client\":\"sentry/javascript\",\"version\":6,\"origin\":\"http://localhost/\",\"remote_addr\":\"127.0.0.1\",\"forwarded_for\":\"8.8.8.8\",\"user_agent\":\"sentry-cli/1.0\"}",
        );
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
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n",
        );
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
    fn test_parse_empty_items() {
        // Without terminating newline after header
        let items = Envelope::parse_items_bytes(Bytes::from("")).unwrap();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_parse_multiple_items() {
        let bytes = Bytes::from(
            "\
             {\"type\":\"attachment\"}\n\
             helloworld\n\
             {\"type\":\"replay_recording\"}\n\
             helloworld\
             ",
        );

        // Without terminating newline after header
        let items = Envelope::parse_items_bytes(bytes).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].len(), 10);
        assert_eq!(items[0].ty(), &ItemType::Attachment);
        assert_eq!(items[1].len(), 10);
        assert_eq!(items[1].ty(), &ItemType::ReplayRecording);
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
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\", \"sent_at\": \"1970-01-01T00:02:03Z\"}",
        );
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
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}",
        );
        let envelope = Envelope::parse_request(bytes, request_meta()).unwrap();
        let meta = envelope.meta();

        // Origin validation should skip a missing origin.
        assert_eq!(meta.origin(), Some(&"http://origin/".parse().unwrap()));
    }

    #[test]
    #[should_panic(expected = "project id")]
    fn test_parse_request_validate_project() {
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/99\"}",
        );
        Envelope::parse_request(bytes, request_meta()).unwrap();
    }

    #[test]
    #[should_panic(expected = "public key")]
    fn test_parse_request_validate_key() {
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:@sentry.io/42\"}",
        );
        Envelope::parse_request(bytes, request_meta()).unwrap();
    }

    #[test]
    #[should_panic(expected = "origin")]
    fn test_parse_request_validate_origin() {
        let bytes = Bytes::from(
            "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\",\"origin\":\"http://localhost/\"}",
        );
        Envelope::parse_request(bytes, request_meta()).unwrap();
    }

    #[test]
    fn test_serialize_envelope_empty() {
        let event_id = EventId("9ec79c33ec9942ab8353589fcb2e04dc".parse().unwrap());
        let envelope = Envelope::from_request(Some(event_id), request_meta());

        let mut buffer = Vec::new();
        envelope.serialize(&mut buffer).unwrap();

        let stringified = String::from_utf8_lossy(&buffer);
        insta::assert_snapshot!(stringified, @r###"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","client":"sentry/client","version":7,"origin":"http://origin/","remote_addr":"192.168.0.1","user_agent":"sentry/agent"}"###);
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
        insta::assert_snapshot!(stringified, @r###"
        {"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","client":"sentry/client","version":7,"origin":"http://origin/","remote_addr":"192.168.0.1","user_agent":"sentry/agent"}
        {"type":"event","length":41,"content_type":"application/json"}
        {"message":"hello world","level":"error"}
        {"type":"attachment","length":7,"content_type":"text/plain","filename":"application.log"}
        Hello
        "###);
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

    #[test]
    fn test_parametrize_root_transaction() {
        let dsc = DynamicSamplingContext {
            trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_owned()),
            user: Default::default(),
            replay_id: None,
            environment: None,
            transaction: Some("/auth/login/test/".into()), // the only important bit for this test
            sample_rate: Some(0.5),
            sampled: Some(true),
            other: BTreeMap::new(),
        };

        let rule: TransactionNameRule = {
            // here you see the pattern that'll transform the transaction name.
            let json = r#"{
                "pattern": "/auth/login/*/**",
                "expiry": "3022-11-30T00:00:00.000000Z",
                "redaction": {
                    "method": "replace",
                    "substitution": "*"
                    }
                }"#;

            serde_json::from_str(json).unwrap()
        };

        // Envelope only created in order to run the parametrize dsc method.
        let mut envelope = {
            let bytes = bytes::Bytes::from(
                "{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n",
            );
            *Envelope::parse_bytes(bytes).unwrap()
        };
        envelope.set_dsc(dsc.clone());

        assert_eq!(
            envelope.dsc().unwrap().transaction.as_ref().unwrap(),
            "/auth/login/test/"
        );
        // parametrize the transaciton name in the dsc.
        envelope.parametrize_dsc_transaction(&[rule]);

        assert_eq!(
            envelope.dsc().unwrap().transaction.as_ref().unwrap(),
            "/auth/login/*/"
        );
    }
}
