use either::Either;
use relay_profiling::AnyProfileChunk;

use crate::envelope::ContentType;
use crate::managed::Counted;
use crate::processing::ForwardContext;
use crate::processing::profile_chunks::{ExpandedProfileChunk, Result};
use crate::services::objectstore::{RawProfile, StoreRawProfile};
use crate::services::store::StoreProfileChunk;

pub fn convert(
    pc: ExpandedProfileChunk,
    ctx: ForwardContext<'_>,
) -> Result<Either<StoreProfileChunk, StoreRawProfile>> {
    let retention_days = ctx.event_retention().standard;

    let payload = match &pc.0 {
        AnyProfileChunk::Android(chunk) => chunk.serialize()?,
        AnyProfileChunk::Perfetto(chunk) => chunk.as_v2().serialize()?,
        AnyProfileChunk::V2(chunk) => chunk.serialize()?,
    };

    let quantities = pc.quantities();

    let raw = match &pc.0 {
        AnyProfileChunk::V2(_) | AnyProfileChunk::Android(_) => None,
        AnyProfileChunk::Perfetto(chunk) => Some(RawProfile {
            name: "profile.perfetto".to_owned(),
            payload: chunk.perfetto().clone(),
            content_type: ContentType::PerfettoTrace,
        }),
    };

    // Follows the pre-existing logic for validating the profiling sizes,
    // we may want to consider validating Perfetto (or "raw") profiles separately.
    if payload.len() + raw.as_ref().map_or(0, |r| r.payload.len()) > ctx.config.max_profile_size() {
        return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
    }

    let profile_chunk = StoreProfileChunk {
        retention_days,
        payload,
        attachments: Vec::new(),
        quantities,
    };

    Ok(match raw {
        Some(raw) => Either::Right(StoreRawProfile { profile_chunk, raw }),
        None => Either::Left(profile_chunk),
    })
}
