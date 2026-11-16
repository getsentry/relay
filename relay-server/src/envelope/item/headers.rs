//! Wire headers and forward-compatible item metadata.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ItemType, SourceQuantities};

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
    /// This header is required for all items that are transmitted in an envelope [`crate::envelope::ItemContainer`].
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
    /// Only valid in combination with [`ContentType::AttachmentRef`](crate::envelope::ContentType::AttachmentRef).
    /// This untrusted header is used to emit negative outcomes, but must not be used for consistent
    /// rate limiting.
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
    pub(super) stored_key: Option<String>,
    /// The routing_hint may be used to specify how the envelpope should be routed in when
    /// published to Kafka.
    #[serde(default, skip)]
    pub(super) routing_hint: Option<Uuid>,
    /// Indicates that this item is being rate limited.
    ///
    /// By default, rate limited items are immediately removed from Envelopes. For processing,
    /// native crash reports still need to be retained. These attachments are marked with the
    /// `rate_limited` header, which signals to the processing pipeline that the attachment should
    /// not be persisted after processing.
    ///
    /// NOTE: This is internal-only and not exposed into the Envelope.
    #[serde(default, skip)]
    pub(super) rate_limited: bool,
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
    pub(super) source_quantities: Option<SourceQuantities>,

    /// Other attributes for forward compatibility.
    #[serde(flatten)]
    pub(super) inner: BTreeMap<MaybeKnown<ItemHeaderKey>, ItemHeaderValue>,
}

/// Wire headers used while parsing, before the tag selects an owning variant.
#[derive(Clone, Deserialize, Serialize)]
pub struct ItemHeaders {
    #[serde(rename = "type")]
    pub(super) ty: ItemType,
    #[serde(flatten)]
    pub(super) data: CommonItemHeaders,
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
pub(in crate::envelope) struct ItemHeadersRef<'a> {
    #[serde(rename = "type")]
    pub(super) ty: &'a ItemType,
    #[serde(flatten)]
    pub(super) data: &'a CommonItemHeaders,
}
impl fmt::Debug for ItemHeadersRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.data.debug_with_type(f, Some(self.ty))
    }
}
impl CommonItemHeaders {
    pub(super) fn contains(&self, key: ItemHeaderKey) -> bool {
        self.inner.contains_key(&MaybeKnown::Known(key))
    }

    pub(super) fn get<'a, T>(&'a self, key: ItemHeaderKey) -> Option<T>
    where
        T: Deserialize<'a>,
    {
        self.inner
            .get(&MaybeKnown::Known(key))
            .and_then(|v| T::deserialize(&v.0).ok())
    }

    pub(super) fn try_get<'a, T>(
        &'a self,
        key: ItemHeaderKey,
    ) -> Result<Option<T>, serde_json::Error>
    where
        T: Deserialize<'a>,
    {
        self.inner
            .get(&MaybeKnown::Known(key))
            .map(|v| T::deserialize(&v.0))
            .transpose()
    }

    pub(super) fn set<T>(&mut self, key: ItemHeaderKey, value: T) -> &mut Self
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).expect("header value must be json serializable");
        self.inner
            .insert(MaybeKnown::Known(key), ItemHeaderValue(value));
        self
    }

    pub(super) fn set_or_remove<T>(&mut self, key: ItemHeaderKey, value: Option<T>) -> &mut Self
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

impl fmt::Debug for CommonItemHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.debug_with_type(f, None)
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
