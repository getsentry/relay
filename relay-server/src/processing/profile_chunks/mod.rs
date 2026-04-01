use std::sync::Arc;

use relay_profiling::ProfileType;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, ManagedResult as _, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use smallvec::smallvec;

mod filter;
mod process;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error raised in [`relay_profiling`].
    #[error("Profiling Error: {0}")]
    Profiling(#[from] relay_profiling::ProfileError),
    /// The profile chunks are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Profile chunks filtered because of a missing feature flag.
    #[error("profile chunks feature flag missing")]
    FilterFeatureFlag,
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

impl crate::managed::OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::Profiling(relay_profiling::ProfileError::Filtered(f)) => {
                Some(Outcome::Filtered(f.clone()))
            }
            Self::Profiling(err) => Some(Outcome::Invalid(DiscardReason::Profiling(
                relay_profiling::discard_reason(err),
            ))),

            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::FilterFeatureFlag => None,
        };
        (outcome, self)
    }
}

/// A processor for profile chunks.
///
/// It processes items of type: [`ItemType::ProfileChunk`].
#[derive(Debug)]
pub struct ProfileChunksProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl ProfileChunksProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for ProfileChunksProcessor {
    type Input = SerializedProfileChunks;
    type Output = ProfileChunkOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let profile_chunks = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::ProfileChunk))
            .into_vec();

        if profile_chunks.is_empty() {
            return None;
        }

        Some(Managed::with_meta_from(
            envelope,
            SerializedProfileChunks {
                headers: envelope.envelope().headers().clone(),
                profile_chunks,
            },
        ))
    }

    async fn process(
        &self,
        mut profile_chunks: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        filter::feature_flag(ctx).reject(&profile_chunks)?;

        process::process(&mut profile_chunks, ctx);

        let profile_chunks = self.limiter.enforce_quotas(profile_chunks, ctx).await?;

        Ok(Output::just(ProfileChunkOutput(profile_chunks)))
    }
}

/// Output produced by [`ProfileChunksProcessor`].
#[derive(Debug)]
pub struct ProfileChunkOutput(Managed<SerializedProfileChunks>);

impl Forward for ProfileChunkOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let Self(profile_chunks) = self;
        Ok(profile_chunks
            .map(|pc, _| Envelope::from_parts(pc.headers, Items::from_vec(pc.profile_chunks))))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::forward::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        use crate::services::store::StoreProfileChunk;

        let Self(profile_chunks) = self;
        let retention_days = ctx.event_retention().standard;

        for item in profile_chunks.split(|pc| pc.profile_chunks) {
            let (kafka_payload, raw_profile, raw_profile_content_type) = split_item_payload(&item);

            s.send_to_store(item.map(|item, _| StoreProfileChunk {
                retention_days,
                payload: kafka_payload,
                quantities: item.quantities(),
                raw_profile,
                raw_profile_content_type,
            }));
        }

        Ok(())
    }
}

/// Splits a profile chunk item payload into its constituent parts.
///
/// For compound items (those with a `meta_length` header), the payload is
/// `[expanded JSON][raw binary]`. Returns `(kafka_payload, raw_profile, content_type)`.
///
/// For plain items, returns `(full_payload, None, None)`.
#[cfg_attr(not(feature = "processing"), allow(dead_code))]
fn split_item_payload(item: &Item) -> (bytes::Bytes, Option<bytes::Bytes>, Option<String>) {
    let payload = item.payload();

    let Some(meta_length) = item.meta_length() else {
        return (payload, None, None);
    };

    let meta_length = meta_length as usize;
    let Some((meta, body)) = payload.split_at_checked(meta_length) else {
        return (payload, None, None);
    };

    if body.is_empty() {
        return (payload.slice_ref(meta), None, None);
    }

    // Extract content_type from the expanded JSON metadata using a minimal
    // deserializer that only reads this single field, skipping the bulk of the
    // payload (frames, stacks, samples, etc.).
    #[derive(serde::Deserialize)]
    struct ContentTypeProbe {
        content_type: Option<String>,
    }
    let content_type = serde_json::from_slice::<ContentTypeProbe>(meta)
        .ok()
        .and_then(|v| v.content_type);

    (
        payload.slice_ref(meta),
        Some(payload.slice_ref(body)),
        content_type,
    )
}

/// Serialized profile chunks extracted from an envelope.
#[derive(Debug)]
pub struct SerializedProfileChunks {
    /// Original envelope headers.
    pub headers: EnvelopeHeaders,
    /// List of serialized profile chunk items.
    pub profile_chunks: Vec<Item>,
}

impl Counted for SerializedProfileChunks {
    fn quantities(&self) -> Quantities {
        let mut ui = 0;
        let mut backend = 0;

        for pc in &self.profile_chunks {
            match pc.profile_type() {
                Some(ProfileType::Ui) => ui += 1,
                Some(ProfileType::Backend) => backend += 1,
                None => {}
            }
        }

        let mut quantities = smallvec![];
        if ui > 0 {
            quantities.push((DataCategory::ProfileChunkUi, ui));
        }
        if backend > 0 {
            quantities.push((DataCategory::ProfileChunk, backend));
        }

        quantities
    }
}

impl CountRateLimited for Managed<SerializedProfileChunks> {
    type Error = Error;
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use crate::envelope::ContentType;

    use super::*;

    fn make_chunk_item(meta: &[u8]) -> Item {
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::Json, bytes::Bytes::copy_from_slice(meta));
        item
    }

    fn make_compound_item(meta: &[u8], body: &[u8]) -> Item {
        let meta_length = meta.len();
        let mut payload = bytes::BytesMut::with_capacity(meta_length + body.len());
        payload.extend_from_slice(meta);
        payload.extend_from_slice(body);

        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::OctetStream, payload.freeze());
        item.set_meta_length(meta_length as u32);
        item
    }

    #[test]
    fn test_split_plain_chunk() {
        let item = make_chunk_item(b"{}");
        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), b"{}");
        assert!(raw.is_none());
        assert!(ct.is_none());
    }

    #[test]
    fn test_split_compound_chunk() {
        let meta = br#"{"content_type":"perfetto"}"#;
        let body = b"binary-data";
        let item = make_compound_item(meta, body);

        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), meta.as_ref());
        assert_eq!(raw.as_deref(), Some(b"binary-data".as_ref()));
        assert_eq!(ct.as_deref(), Some("perfetto"));
    }

    #[test]
    fn test_split_compound_no_content_type() {
        let meta = b"{}";
        let body = b"binary-data";
        let item = make_compound_item(meta, body);

        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(raw.as_deref(), Some(b"binary-data".as_ref()));
        assert!(ct.is_none());
    }

    #[test]
    fn test_split_compound_empty_body() {
        let meta = br#"{"content_type":"perfetto"}"#;
        let item = make_compound_item(meta, b"");

        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), meta.as_ref());
        assert!(raw.is_none());
        assert!(ct.is_none());
    }

    #[test]
    fn test_split_compound_meta_length_exceeds_payload() {
        // meta_length is set to more bytes than the payload actually contains.
        // split_at_checked returns None, so we fall back to the full payload with no split.
        let body = b"binary-data";
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::OctetStream, bytes::Bytes::from(body.as_ref()));
        item.set_meta_length(body.len() as u32 + 100);

        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), body.as_ref());
        assert!(raw.is_none());
        assert!(ct.is_none());
    }

    #[test]
    fn test_split_compound_invalid_json_meta() {
        // meta portion is not valid JSON; content_type should be None.
        let meta = b"not valid json {{{{";
        let body = b"binary-data";
        let item = make_compound_item(meta, body);

        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), meta.as_ref());
        assert_eq!(raw.as_deref(), Some(b"binary-data".as_ref()));
        assert!(ct.is_none());
    }

    #[test]
    fn test_split_compound_zero_meta_length() {
        // meta_length = 0: meta slice is empty, entire payload is treated as body.
        let body = b"binary-data";
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::OctetStream, bytes::Bytes::from(body.as_ref()));
        item.set_meta_length(0);

        let (payload, raw, ct) = split_item_payload(&item);
        assert_eq!(payload.as_ref(), b"");
        assert_eq!(raw.as_deref(), Some(b"binary-data".as_ref()));
        assert!(ct.is_none());
    }
}
