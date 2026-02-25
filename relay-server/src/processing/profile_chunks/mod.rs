use std::sync::Arc;

use relay_profiling::ProfileType;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
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
    type UnitOfWork = SerializedProfileChunks;
    type Output = ProfileChunkOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let items = envelope
            .envelope_mut()
            .take_items_by(|item| {
                matches!(
                    *item.ty(),
                    ItemType::ProfileChunk | ItemType::ProfileChunkData
                )
            })
            .into_vec();

        if items.is_empty() {
            return None;
        }

        let profile_chunks = pair_profile_chunks(items);

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
        mut profile_chunks: Managed<Self::UnitOfWork>,
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
        Ok(profile_chunks.map(|pc, _| {
            let mut items: Vec<Item> = Vec::new();
            for ppc in pc.profile_chunks {
                items.push(ppc.item);
                if let Some(raw) = ppc.raw_profile {
                    let mut data_item = Item::new(ItemType::ProfileChunkData);
                    data_item.set_payload(ContentType::OctetStream, raw);
                    items.push(data_item);
                }
            }
            Envelope::from_parts(pc.headers, Items::from_vec(items))
        }))
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

        for ppc in profile_chunks.split(|pc| pc.profile_chunks) {
            s.store(ppc.map(|ppc, _| StoreProfileChunk {
                retention_days,
                payload: ppc.item.payload(),
                quantities: ppc.item.quantities(),
                raw_profile: ppc.raw_profile,
            }));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ProcessedProfileChunk {
    pub item: Item,
    /// Raw binary profile blob. The `platform` field describes the format, e.g. Perfetto.
    pub raw_profile: Option<bytes::Bytes>,
}

impl Counted for ProcessedProfileChunk {
    fn quantities(&self) -> Quantities {
        self.item.quantities()
    }
}

/// Serialized profile chunks extracted from an envelope.
#[derive(Debug)]
pub struct SerializedProfileChunks {
    /// Original envelope headers.
    pub headers: EnvelopeHeaders,
    /// List of serialized profile chunk items.
    pub profile_chunks: Vec<ProcessedProfileChunk>,
}

impl Counted for SerializedProfileChunks {
    fn quantities(&self) -> Quantities {
        let mut ui = 0;
        let mut backend = 0;

        for pc in &self.profile_chunks {
            match pc.item.profile_type() {
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

/// Pairs `ProfileChunk` items with their optional `ProfileChunkData` companions.
///
/// Expects items ordered as they appear in the envelope: each `ProfileChunk` may be
/// followed by a `ProfileChunkData` item containing the raw binary profile.
fn pair_profile_chunks(items: Vec<Item>) -> Vec<ProcessedProfileChunk> {
    let mut metadata_item: Option<Item> = None;
    let mut binary_item: Option<Item> = None;
    let mut profile_chunks: Vec<ProcessedProfileChunk> = Vec::new();

    for item in items {
        match item.ty() {
            ItemType::ProfileChunkData => {
                binary_item = Some(item);
            }
            _ => {
                if let Some(meta) = metadata_item.take() {
                    profile_chunks.push(ProcessedProfileChunk {
                        item: meta,
                        raw_profile: binary_item.take().map(|i| i.payload()),
                    });
                }
                metadata_item = Some(item);
            }
        }
    }
    if let Some(meta) = metadata_item.take() {
        profile_chunks.push(ProcessedProfileChunk {
            item: meta,
            raw_profile: binary_item.take().map(|i| i.payload()),
        });
    }

    profile_chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk_item(payload: &[u8]) -> Item {
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::Json, bytes::Bytes::copy_from_slice(payload));
        item
    }

    fn make_data_item(payload: &[u8]) -> Item {
        let mut item = Item::new(ItemType::ProfileChunkData);
        item.set_payload(
            ContentType::OctetStream,
            bytes::Bytes::copy_from_slice(payload),
        );
        item
    }

    #[test]
    fn test_pair_single_chunk_without_data() {
        let items = vec![make_chunk_item(b"meta1")];
        let result = pair_profile_chunks(items);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].item.payload().as_ref(), b"meta1");
        assert!(result[0].raw_profile.is_none());
    }

    #[test]
    fn test_pair_chunk_with_data() {
        let items = vec![make_chunk_item(b"meta1"), make_data_item(b"binary1")];
        let result = pair_profile_chunks(items);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].item.payload().as_ref(), b"meta1");
        assert_eq!(result[0].raw_profile.as_deref(), Some(b"binary1".as_ref()));
    }

    #[test]
    fn test_pair_multiple_chunks_mixed() {
        let items = vec![
            make_chunk_item(b"meta1"),
            make_data_item(b"binary1"),
            make_chunk_item(b"meta2"),
        ];
        let result = pair_profile_chunks(items);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].item.payload().as_ref(), b"meta1");
        assert_eq!(result[0].raw_profile.as_deref(), Some(b"binary1".as_ref()));
        assert_eq!(result[1].item.payload().as_ref(), b"meta2");
        assert!(result[1].raw_profile.is_none());
    }

    #[test]
    fn test_pair_multiple_chunks_each_with_data() {
        let items = vec![
            make_chunk_item(b"meta1"),
            make_data_item(b"binary1"),
            make_chunk_item(b"meta2"),
            make_data_item(b"binary2"),
        ];
        let result = pair_profile_chunks(items);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].raw_profile.as_deref(), Some(b"binary1".as_ref()));
        assert_eq!(result[1].raw_profile.as_deref(), Some(b"binary2".as_ref()));
    }

    #[test]
    fn test_pair_data_before_chunk_is_associated() {
        let items = vec![make_data_item(b"binary1"), make_chunk_item(b"meta1")];
        let result = pair_profile_chunks(items);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].item.payload().as_ref(), b"meta1");
        assert_eq!(result[0].raw_profile.as_deref(), Some(b"binary1".as_ref()));
    }

    #[test]
    fn test_pair_only_data_produces_nothing() {
        let items = vec![make_data_item(b"orphan")];
        let result = pair_profile_chunks(items);

        assert!(result.is_empty());
    }

    #[test]
    fn test_pair_empty_items() {
        let result = pair_profile_chunks(vec![]);
        assert!(result.is_empty());
    }
}
