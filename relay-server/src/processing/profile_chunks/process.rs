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
        |item, _| -> Result<()> {
            let pc = relay_profiling::ProfileChunk::new(item.payload())?;

            // Profile chunks must carry the same profile type in the item header as well as the
            // payload.
            if item.profile_type().is_none_or(|pt| pt != pc.profile_type()) {
                relay_log::debug!("dropping profile chunk due to profile type/platform mismatch");
                return Err(relay_profiling::ProfileError::InvalidProfileType.into());
            }

            pc.filter(client_ip, filter_settings, ctx.global_config)?;

            let expanded = pc.expand()?;
            if expanded.len() > ctx.config.max_profile_size() {
                relay_log::debug!("dropping profile chunk exceeding the size limit");
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
