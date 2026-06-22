use std::sync::Arc;

use relay_cogs::{AppFeature, FeatureWeights};

use relay_profiling::{ProfileChunk, ProfileType};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, ManagedResult as _, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

mod filter;
mod process;
#[cfg(feature = "processing")]
mod store;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error raised in [`relay_profiling`].
    #[error("Profiling Error: {0}")]
    Profiling(#[from] relay_profiling::ProfileError),
    /// Profile chunks filtered due to a filtering rule.
    #[error("profile chunk filtered")]
    Filtered(relay_filter::FilterStatKey),
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
            Self::Filtered(f) => Some(Outcome::Filtered(f.clone())),
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

    fn cogs() -> FeatureWeights {
        AppFeature::Profiles.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let profile_chunks = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::ProfileChunk))
            .into_vec();

        if profile_chunks.is_empty() {
            return None;
        }

        Some(Managed::with_meta_from_managed_envelope(
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
        filter::feature_flag(&mut profile_chunks, ctx).reject(&profile_chunks)?;

        if !ctx.is_processing() {
            let profile_chunks = self.limiter.enforce_quotas(profile_chunks, ctx).await?;
            return Ok(Output::just(ProfileChunkOutput::Serialized(profile_chunks)));
        }

        let mut profile_chunks = process::expand(profile_chunks);

        filter::filter(&mut profile_chunks, ctx);
        process::normalize(&mut profile_chunks);

        let profile_chunks = self.limiter.enforce_quotas(profile_chunks, ctx).await?;

        Ok(Output::just(ProfileChunkOutput::Expanded(profile_chunks)))
    }
}

/// Output produced by [`ProfileChunksProcessor`].
#[derive(Debug)]
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
        let expanded = match self {
            Self::Expanded(e) => e,
            Self::Serialized(m) => {
                return Err(
                    m.internal_error("forward_store called with non-expanded profile chunks")
                );
            }
        };

        for chunk in expanded.split(|e| e.profile_chunks) {
            if let Ok(pc) = chunk.try_map(|pc, _| store::convert(pc, ctx)) {
                match pc.transpose() {
                    either::Either::Left(message) => s.send_to_store(message),
                    either::Either::Right(message) => s.send_to_objectstore(message),
                }
            }
        }

        Ok(())
    }
}

/// Serialized profile chunks extracted from an envelope.
#[derive(Debug)]
pub struct SerializedProfileChunks {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// List of serialized profile chunk items.
    profile_chunks: Vec<Item>,
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

        let mut quantities = smallvec::smallvec![];
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

/// A single profile chunk after expansion.
#[derive(Debug)]
struct ExpandedProfileChunk(relay_profiling::AnyProfileChunk);

impl Counted for ExpandedProfileChunk {
    fn quantities(&self) -> Quantities {
        let category = match self.0.profile_type() {
            ProfileType::Backend => DataCategory::ProfileChunk,
            ProfileType::Ui => DataCategory::ProfileChunkUi,
        };
        smallvec::smallvec![(category, 1)]
    }
}

/// Profile chunks after expansion: all items have been parsed, validated, and
/// converted into typed representations.
#[derive(Debug)]
pub struct ExpandedProfileChunks {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// List of parsed and expanded profile chunks.
    profile_chunks: Vec<ExpandedProfileChunk>,
}

impl Counted for ExpandedProfileChunks {
    fn quantities(&self) -> Quantities {
        let mut q = Quantities::new();
        for chunk in &self.profile_chunks {
            q.extend(chunk.quantities());
        }
        q
    }
}

impl CountRateLimited for Managed<ExpandedProfileChunks> {
    type Error = Error;
}
