use relay_profiling::AnyProfileChunk;

use crate::envelope::ContentType;
use crate::managed::Counted;
use crate::processing::ForwardContext;
use crate::processing::profile_chunks::{ExpandedProfileChunk, Result};
use crate::services::store::{RawProfile, StoreProfileChunk};

pub fn convert(pc: ExpandedProfileChunk, ctx: ForwardContext<'_>) -> Result<StoreProfileChunk> {
    let retention_days = ctx.event_retention().standard;

    let payload = match &pc.0 {
        AnyProfileChunk::Android(chunk) => chunk.serialize()?,
        AnyProfileChunk::Perfetto(chunk) => chunk.as_v2().serialize()?,
        AnyProfileChunk::V2(chunk) => chunk.serialize()?,
    };

    let quantities = pc.quantities();

    let raw_profile = match &pc.0 {
        AnyProfileChunk::Android(_) => None,
        AnyProfileChunk::Perfetto(chunk) => Some(RawProfile {
            payload: chunk.perfetto().clone(),
            content_type: ContentType::PerfettoTrace,
        }),
        AnyProfileChunk::V2(_) => None,
    };

    // Follows the pre-existing logic for validating the profiling sizes,
    // we may want to consider validating Perfetto (or "raw") profiles separately.
    if payload.len() + raw_profile.as_ref().map_or(0, |r| r.payload.len())
        > ctx.config.max_profile_size()
    {
        return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
    }

    Ok(StoreProfileChunk {
        retention_days,
        payload,
        quantities,
        raw_profile,
    })
}
