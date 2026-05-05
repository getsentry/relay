use crate::processing::profile_chunks::ExpandedProfileChunk;
use crate::services::store::StoreProfileChunk;

pub fn convert(chunk: ExpandedProfileChunk, retention_days: u16) -> StoreProfileChunk {
    StoreProfileChunk {
        retention_days,
        payload: chunk.payload,
        quantities: chunk.quantities,
        raw_profile: chunk.raw_profile,
    }
}
