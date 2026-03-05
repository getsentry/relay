use relay_profiling::ProfileType;
use relay_quotas::DataCategory;

use crate::envelope::{ContentType, Item, ItemType};
use crate::processing::Context;
use crate::processing::Managed;
use crate::processing::profile_chunks::{Result, SerializedProfileChunks};
use crate::statsd::RelayCounters;
use crate::utils;

/// Processes profile chunks.
pub fn process(profile_chunks: &mut Managed<SerializedProfileChunks>, ctx: Context<'_>) {
    // Only run this 'expensive' processing step in processing Relays.
    if !ctx.is_processing() {
        return;
    }

    let sdk = utils::client_name_tag(profile_chunks.headers.meta().client_name());
    let client_ip = profile_chunks.headers.meta().client_addr();
    let filter_settings = &ctx.project_info.config.filter_settings;

    profile_chunks.retain(
        |pc| &mut pc.profile_chunks,
        |item, records| -> Result<()> {
            if let Some(meta_length) = item.meta_length() {
                return process_compound_item(
                    item,
                    meta_length,
                    sdk,
                    client_ip,
                    filter_settings,
                    ctx,
                    records,
                );
            }

            let pc = relay_profiling::ProfileChunk::new(item.payload())?;

            // Validate the item inferred profile type with the one from the payload,
            // or if missing set it.
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

            pc.filter(client_ip, filter_settings, ctx.global_config)?;

            let expanded = pc.expand()?;
            if expanded.len() > ctx.config.max_profile_size() {
                return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
            }

            *item = {
                let mut new_item = Item::new(ItemType::ProfileChunk);
                new_item.set_platform(pc.platform().to_owned());
                new_item.set_payload(ContentType::Json, expanded);
                new_item
            };

            Ok(())
        },
    );
}

/// Processes a compound profile chunk item (JSON metadata + binary blob).
///
/// The item payload is `[JSON metadata bytes][binary blob bytes]`, split at `meta_length`.
/// After expansion, the item is rebuilt with `[expanded JSON][raw binary]` and an updated
/// `meta_length`, so that `forward_store` can still extract the raw profile.
fn process_compound_item(
    item: &mut Item,
    meta_length: u32,
    sdk: &str,
    client_ip: Option<std::net::IpAddr>,
    filter_settings: &relay_filter::ProjectFiltersConfig,
    ctx: Context<'_>,
    records: &mut crate::managed::RecordKeeper,
) -> Result<()> {
    let payload = item.payload();
    let meta_length = meta_length as usize;

    let Some((meta_json, raw_profile)) = payload.split_at_checked(meta_length) else {
        return Err(relay_profiling::ProfileError::InvalidSampledProfile.into());
    };

    let content_type = serde_json::from_slice::<serde_json::Value>(meta_json)
        .ok()
        .and_then(|v| v.get("content_type")?.as_str().map(|s| s.to_owned()));

    match content_type.as_deref() {
        Some("perfetto") => {}
        _ => return Err(relay_profiling::ProfileError::PlatformNotSupported.into()),
    }

    let expanded = relay_profiling::expand_perfetto(raw_profile, meta_json)?;
    if expanded.len() > ctx.config.max_profile_size() {
        return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
    }

    let expanded = bytes::Bytes::from(expanded);
    let pc = relay_profiling::ProfileChunk::new(expanded.clone())?;

    if item
        .profile_type()
        .is_some_and(|pt| pt != pc.profile_type())
    {
        return Err(relay_profiling::ProfileError::InvalidProfileType.into());
    }

    if item.profile_type().is_none() {
        relay_statsd::metric!(
            counter(RelayCounters::ProfileChunksWithoutPlatform) += 1,
            sdk = sdk
        );
        item.set_platform(pc.platform().to_owned());
        match pc.profile_type() {
            ProfileType::Ui => records.modify_by(DataCategory::ProfileChunkUi, 1),
            ProfileType::Backend => records.modify_by(DataCategory::ProfileChunk, 1),
        }
    }

    pc.filter(client_ip, filter_settings, ctx.global_config)?;

    // Rebuild the compound payload: [expanded JSON][raw binary].
    // This preserves the raw profile for downstream extraction in forward_store.
    let mut compound = bytes::BytesMut::with_capacity(expanded.len() + raw_profile.len());
    compound.extend_from_slice(&expanded);
    compound.extend_from_slice(raw_profile);

    *item = {
        let mut new_item = Item::new(ItemType::ProfileChunk);
        new_item.set_platform(pc.platform().to_owned());
        new_item.set_payload(ContentType::Json, compound.freeze());
        new_item.set_meta_length(expanded.len() as u32);
        new_item
    };

    Ok(())
}
