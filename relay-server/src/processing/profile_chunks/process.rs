use relay_profiling::AndroidOrV2ProfileChunk;
use relay_profiling::ProfileChunk as _;
use relay_profiling::ProfileError;
use relay_quotas::DataCategory;

use relay_profiling::ProfileType;

use crate::envelope::ContentType;
use crate::envelope::Item;
use crate::managed::RecordKeeper;
use crate::processing::Managed;
use crate::processing::profile_chunks::Error;
use crate::processing::profile_chunks::{
    ExpandedProfileChunk, ExpandedProfileChunks, Result, SerializedProfileChunks,
};
use crate::statsd::RelayCounters;
use crate::utils;

/// Expands serialized profile chunk items into typed representations.
///
/// Each item is individually parsed and validated. Items that fail are
/// removed with outcome tracking.
pub fn expand(chunks: Managed<SerializedProfileChunks>) -> Managed<ExpandedProfileChunks> {
    let sdk = utils::client_name_tag(chunks.headers.meta().client_name());

    chunks.map(|serialized, records| {
        let SerializedProfileChunks {
            headers,
            profile_chunks,
        } = serialized;

        let profile_chunks = profile_chunks
            .into_iter()
            .filter_map(|mut item| {
                expand_profile_chunk(&mut item, sdk, records)
                    .map_err(|err| records.reject_err(err, &item))
                    .ok()
            })
            .collect();

        ExpandedProfileChunks {
            headers,
            profile_chunks,
        }
    })
}

/// Normalizes all profile chunks.
pub fn normalize(profile_chunks: &mut Managed<ExpandedProfileChunks>) {
    profile_chunks.retain(
        |pc| &mut pc.profile_chunks,
        |pc, _| pc.0.normalize().map_err(Error::from),
    );
}

fn expand_profile_chunk(
    item: &mut Item,
    sdk: &str,
    records: &mut RecordKeeper<'_>,
) -> Result<ExpandedProfileChunk> {
    let profile_chunk = match item.content_type() {
        Some(ContentType::PerfettoTrace) => expand_perfetto_profile_chunk(item),
        _ => expand_json_item(item, sdk, records),
    }?;

    // Make sure the item platform and profile-type always matches the actual
    // profile platform. We want to avoid cases where we let mismatches through.
    //
    // We want to force all implementing SDKs to include the `platform` item header,
    // in order to allow fast path rate-limiting.
    if Some(profile_chunk.0.profile_type()) != item.profile_type()
        || Some(profile_chunk.0.platform()) != item.platform()
    {
        return Err(Error::Profiling(ProfileError::InvalidProfileType));
    }

    Ok(profile_chunk)
}

fn expand_json_item(
    item: &mut Item,
    sdk: &str,
    records: &mut RecordKeeper<'_>,
) -> Result<ExpandedProfileChunk> {
    if item.meta_length().is_some() {
        return Err(relay_profiling::ProfileError::InvalidSampledProfile.into());
    }

    let pc = AndroidOrV2ProfileChunk::parse(&item.payload())?;

    // Validate the item inferred profile type with the one from the payload.
    //
    // This is currently necessary to ensure profile chunks are emitted in the correct
    // data category, as well as rate limited with the correct data category.
    //
    // In the future we plan to make the profile type on the item header a necessity.
    // For more context see also: <https://github.com/getsentry/relay/pull/4595>.
    if item
        .profile_type()
        .is_some_and(|pt| pt != pc.profile_type())
    {
        return Err(relay_profiling::ProfileError::InvalidProfileType.into());
    }
    // Update the profile type to ensure the following outcomes are emitted in the correct
    // data category.
    //
    // Once the item header on the item is required, this is no longer required.
    if item.profile_type().is_none() {
        relay_statsd::metric!(
            counter(RelayCounters::ProfileChunksWithoutPlatform) += 1,
            sdk = sdk
        );

        item.set_platform(pc.platform().to_owned());
        debug_assert_eq!(item.profile_type(), Some(pc.profile_type()));
        match pc.profile_type() {
            ProfileType::Ui => records.modify_by(DataCategory::ProfileChunkUi, 1),
            ProfileType::Backend => records.modify_by(DataCategory::ProfileChunk, 1),
        }
    }

    Ok(ExpandedProfileChunk(pc.into()))
}

fn expand_perfetto_profile_chunk(item: &Item) -> Result<ExpandedProfileChunk> {
    let meta_length =
        item.meta_length()
            .ok_or(relay_profiling::ProfileError::InvalidSampledProfile)? as usize;

    let (v2, perfetto) = {
        let mut v2 = item.payload();
        // Split off panics on out of bounds -> validate the split off length.
        //
        // Both parts (v2 metadata and Perfetto profile) are required and must be length > 0.
        if meta_length >= v2.len() || meta_length == 0 {
            return Err(relay_profiling::ProfileError::InvalidSampledProfile.into());
        }
        let perfetto = v2.split_off(meta_length);
        (v2, perfetto)
    };

    let chunk = Box::new(relay_profiling::PerfettoProfileChunk::parse(&v2, perfetto)?);
    Ok(ExpandedProfileChunk(chunk.into()))
}
