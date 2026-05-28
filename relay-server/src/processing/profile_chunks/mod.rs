use std::sync::Arc;

use bytes::Bytes;
use smallvec::smallvec;

use relay_profiling::ProfileType;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, ManagedResult as _, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

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

#[derive(Debug)]
#[cfg_attr(all(not(feature = "processing"), not(test)), expect(dead_code))]
pub struct RawProfile {
    pub payload: Bytes,
    pub content_type: ContentType,
}

/// A single profile chunk after expansion.
#[derive(Debug)]
#[cfg_attr(all(not(feature = "processing"), not(test)), expect(dead_code))]
pub struct ExpandedProfileChunk {
    pub payload: Bytes,
    pub raw_profile: Option<RawProfile>,
    pub quantities: Quantities,
}

impl Counted for ExpandedProfileChunk {
    fn quantities(&self) -> Quantities {
        self.quantities.clone()
    }
}

/// Profile chunks after expansion: all items have been parsed, validated, and
/// converted into typed representations.
#[derive(Debug)]
pub struct ExpandedProfileChunks {
    pub chunks: Vec<ExpandedProfileChunk>,
}

impl Counted for ExpandedProfileChunks {
    fn quantities(&self) -> Quantities {
        let mut q = Quantities::new();
        for chunk in &self.chunks {
            q.extend(chunk.quantities());
        }
        q
    }
}

impl CountRateLimited for Managed<ExpandedProfileChunks> {
    type Error = Error;
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
        profile_chunks: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        filter::feature_flag(ctx).reject(&profile_chunks)?;

        if !ctx.is_processing() {
            let profile_chunks = self.limiter.enforce_quotas(profile_chunks, ctx).await?;
            return Ok(Output::just(ProfileChunkOutput::Serialized(profile_chunks)));
        }

        let expanded: Managed<ExpandedProfileChunks> = process::expand(profile_chunks, ctx);
        let expanded = self.limiter.enforce_quotas(expanded, ctx).await?;

        Ok(Output::just(ProfileChunkOutput::Expanded(expanded)))
    }
}

/// Output produced by [`ProfileChunksProcessor`].
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "variants are sized by Managed<T> which wraps different pipeline stages"
)]
pub enum ProfileChunkOutput {
    /// Non-processing relay: items forwarded as-is.
    Serialized(Managed<SerializedProfileChunks>),
    /// Processing relay: items expanded into typed representations.
    Expanded(Managed<ExpandedProfileChunks>),
}

impl Forward for ProfileChunkOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {
            Self::Serialized(profile_chunks) => Ok(profile_chunks
                .map(|pc, _| Envelope::from_parts(pc.headers, Items::from_vec(pc.profile_chunks)))),
            Self::Expanded(m) => {
                Err(m.internal_error("serialize_envelope called with expanded profile chunks"))
            }
        }
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::forward::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        use crate::services::objectstore::StoreRawProfile;
        use crate::services::store::StoreProfileChunk;

        let expanded = match self {
            Self::Expanded(e) => e,
            Self::Serialized(m) => {
                return Err(
                    m.internal_error("forward_store called with non-expanded profile chunks")
                );
            }
        };
        let retention_days = ctx.event_retention().standard;

        for chunk in expanded.split(|e| e.chunks) {
            if chunk.raw_profile.is_some() {
                s.send_to_objectstore(chunk.map(|chunk, _| {
                    let raw_profile = chunk.raw_profile.unwrap();
                    StoreRawProfile {
                        payload: raw_profile.payload,
                        content_type: raw_profile.content_type,
                        store_message: StoreProfileChunk {
                            retention_days,
                            payload: chunk.payload,
                            quantities: chunk.quantities,
                            raw_profile_object_store_key: None,
                            raw_profile_content_type: None,
                        },
                        retention: retention_days,
                    }
                }));
            } else {
                s.send_to_store(chunk.map(|chunk, _| StoreProfileChunk {
                    retention_days,
                    payload: chunk.payload,
                    quantities: chunk.quantities,
                    raw_profile_object_store_key: None,
                    raw_profile_content_type: None,
                }));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::Context;

    fn make_expanded(
        chunks: Vec<ExpandedProfileChunk>,
    ) -> (
        Managed<ExpandedProfileChunks>,
        crate::managed::ManagedTestHandle,
    ) {
        Managed::for_test(ExpandedProfileChunks { chunks }).build()
    }

    #[test]
    #[should_panic(expected = "serialize_envelope called with expanded profile chunks")]
    fn test_serialize_envelope_rejects_expanded() {
        let chunk = ExpandedProfileChunk {
            payload: Bytes::from(b"{\"hello\":\"world\"}".as_ref()),
            raw_profile: None,
            quantities: smallvec![(DataCategory::ProfileChunk, 1)],
        };
        let (managed, _handle) = make_expanded(vec![chunk]);
        let output = ProfileChunkOutput::Expanded(managed);

        let _ = output.serialize_envelope(Context::for_test().to_forward());
    }
}
