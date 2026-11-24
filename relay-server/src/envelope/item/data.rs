//! Shared headers and serialized payload storage.

use std::fmt;
use std::ops::AddAssign;

use bytes::Bytes;
use relay_quotas::ReasonCode;
#[cfg(test)]
use serde::Serialize;
use uuid::Uuid;

use super::headers::{CommonItemHeaders, ItemHeadersRef};
use super::{ContentType, ItemHeaderKey, ItemType};
use crate::utils::DebugBytes;

/// Raw headers and payload shared by all item types.
#[derive(Clone, Default)]
pub struct ItemData {
    headers: CommonItemHeaders,
    payload: Bytes,
}

impl ItemData {
    /// Creates empty storage for a serialized item.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates storage from parsed headers and payload without modifying either.
    pub fn from_parts(headers: CommonItemHeaders, payload: Bytes) -> Self {
        Self { headers, payload }
    }

    /// Returns the shared item headers.
    pub fn headers(&self) -> &CommonItemHeaders {
        &self.headers
    }

    /// Returns mutable access to the shared item headers.
    pub fn headers_mut(&mut self) -> &mut CommonItemHeaders {
        &mut self.headers
    }

    /// Borrows the serialized payload without cloning its shared buffer.
    pub fn payload_ref(&self) -> &[u8] {
        &self.payload
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

    /// Sets the content type of this item's payload without changing the payload.
    ///
    /// Unlike [`Self::set_payload`], this does not update the length header.
    pub fn set_content_type(&mut self, content_type: ContentType) {
        self.headers.set(ItemHeaderKey::ContentType, content_type);
    }

    /// Sets the content type if there isn't already one set.
    pub fn set_default_content_type(&mut self, content_type: ContentType) {
        if !self.headers.contains(ItemHeaderKey::ContentType) {
            self.headers.set(ItemHeaderKey::ContentType, content_type);
        }
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
    pub fn set_payload_with_item_count<B>(
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
        self.headers.rate_limited()
    }

    /// Sets whether this item should be rate limited.
    pub fn set_rate_limited(&mut self, rate_limited: bool) {
        self.headers.set_rate_limited(rate_limited);
    }

    /// Returns the reason code of the rate limit that marked this item as rate limited.
    pub fn rate_limit_reason(&self) -> Option<&ReasonCode> {
        self.headers.rate_limit_reason()
    }

    /// Sets the reason code of the rate limit that marked this item as rate limited.
    pub fn set_rate_limit_reason(&mut self, rate_limit_reason: Option<ReasonCode>) {
        self.headers.set_rate_limit_reason(rate_limit_reason);
    }

    /// Returns the contained source quantities.
    pub fn source_quantities(&self) -> Option<SourceQuantities> {
        self.headers.source_quantities()
    }

    /// Sets new source quantities.
    pub fn set_source_quantities(&mut self, source_quantities: SourceQuantities) {
        self.headers.set_source_quantities(source_quantities);
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

    /// Whether the content-type header names a container format.
    pub fn is_container(&self) -> bool {
        self.content_type().is_some_and(ContentType::is_container)
    }

    /// Sets arbitrary headers for tests, including invalid and misplaced values.
    #[cfg(test)]
    pub fn set_test_header<T: Serialize>(&mut self, key: ItemHeaderKey, value: Option<T>) {
        self.headers.set_or_remove(key, value);
    }

    pub fn debug_with_type(&self, f: &mut fmt::Formatter<'_>, ty: &ItemType) -> fmt::Result {
        f.debug_struct("Item")
            .field("headers", &ItemHeadersRef::new(ty, &self.headers))
            .field("payload", &DebugBytes(&self.payload))
            .finish()
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
