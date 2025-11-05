//! Profiles related processor code.
use relay_dynamic_config::{Feature, GlobalConfig};
use std::net::IpAddr;

use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_event_schema::protocol::{Contexts, Event, ProfileContext};
use relay_filter::ProjectFiltersConfig;
use relay_profiling::{ProfileError, ProfileId};
use relay_protocol::Annotated;
#[cfg(feature = "processing")]
use relay_protocol::{Getter, Remark, RemarkType};

use crate::envelope::{ContentType, Item, ItemType};
use crate::managed::{ItemAction, TypedEnvelope};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::TransactionGroup;
use crate::services::projects::project::ProjectInfo;

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
pub fn process(
    client_ip: Option<IpAddr>,
    event: &mut Annotated<Event>,
    ctx: &Context,
) -> Option<ProfileId> {
    let filter_settings = &ctx.project_info.config.filter_settings;

    let profiling_enabled = ctx.project_info.has_feature(Feature::Profiling);
    let mut profile_id = None;

    managed_envelope.retain_items(|item| match item.ty() {
        ItemType::Profile => {
            if !profiling_enabled {
                return ItemAction::DropSilently;
            }

            // There should always be an event/transaction available at this stage.
            // It is required to expand the profile. If it's missing, drop the item.
            let Some(event) = event.value() else {
                return ItemAction::DropSilently;
            };

            match expand_profile(
                item,
                event,
                config,
                client_ip,
                filter_settings,
                global_config,
            ) {
                Ok(id) => {
                    profile_id = Some(id);
                    ItemAction::Keep
                }
                Err(outcome) => ItemAction::Drop(outcome),
            }
        }
        _ => ItemAction::Keep,
    });

    profile_id
}
