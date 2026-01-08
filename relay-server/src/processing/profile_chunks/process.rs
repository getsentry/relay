use relay_profiling::ProfileType;
use relay_quotas::DataCategory;

use crate::envelope::{ContentType, Item, ItemType};
use crate::processing::Context;
use crate::processing::Managed;
use crate::processing::profile_chunks::{Result, SerializedProfileChunks};

/// Processes profile chunks.
pub fn process(profile_chunks: &mut Managed<SerializedProfileChunks>, ctx: Context<'_>) {
    // Only run this 'expensive' processing step in processing Relays.
    if !ctx.is_processing() {
        return;
    }

    let client_ip = profile_chunks.headers.meta().client_addr();
    let filter_settings = &ctx.project_info.config.filter_settings;

    profile_chunks.retain(
        |pc| &mut pc.profile_chunks,
        |item, records| -> Result<()> {
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
                item.set_profile_type(pc.profile_type());
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
                let mut item = Item::new(ItemType::ProfileChunk);
                item.set_profile_type(pc.profile_type());
                item.set_payload(ContentType::Json, expanded);
                item
            };

            Ok(())
        },
    );
}
