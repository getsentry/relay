use relay_dynamic_config::Feature;

use crate::envelope::ContentType;
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::profile_chunks::{
    Error, ExpandedProfileChunk, ExpandedProfileChunks, Result, SerializedProfileChunks,
};

/// Checks whether the profile ingestion feature flag is enabled for the current project.
pub fn feature_flag(items: &mut Managed<SerializedProfileChunks>, ctx: Context<'_>) -> Result<()> {
    if ctx.should_filter(Feature::ContinuousProfiling) {
        return Err(Error::FilterFeatureFlag);
    }

    if ctx.should_filter(Feature::ContinuousProfilingPerfetto) {
        items.retain(
            |items| &mut items.profile_chunks,
            |pc, _| match pc.content_type() {
                Some(ContentType::PerfettoTrace) => Err(Error::FilterFeatureFlag),
                _ => Ok(()),
            },
        );

        // All items rejected, no need to go on.
        if items.profile_chunks.is_empty() {
            return Err(Error::FilterFeatureFlag);
        }
    }

    Ok(())
}

/// Applies inbound filters to individual profile chunks.
pub fn filter(profile_chunks: &mut Managed<ExpandedProfileChunks>, ctx: Context<'_>) {
    profile_chunks.retain_with_context(
        |profile_chunks| {
            (
                &mut profile_chunks.profile_chunks,
                profile_chunks.headers.meta(),
            )
        },
        |pc, meta, _| filter_profile_chunk(pc, meta, ctx),
    );
}

fn filter_profile_chunk(
    profile_chunk: &ExpandedProfileChunk,
    meta: &RequestMeta,
    ctx: Context<'_>,
) -> Result<()> {
    relay_filter::should_filter(
        &profile_chunk.0,
        meta.client_addr(),
        &ctx.project_info.config.filter_settings,
        ctx.global_config.filters(),
    )
    .map_err(Error::Filtered)
}
