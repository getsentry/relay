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
use crate::managed::{ItemAction, Managed, ManagedResult, Rejected, TypedEnvelope};
use crate::processing::Context;
use crate::processing::transactions::{Error, SerializedTransaction};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::TransactionGroup;
use crate::services::projects::project::ProjectInfo;

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
pub fn process(
    profile: &mut Managed<Item>,
    client_ip: Option<IpAddr>,
    event: Option<&Event>,
    ctx: &Context,
) -> Result<ProfileId, Rejected<()>> {
    debug_assert_eq!(profile.ty(), &ItemType::Profile);
    let filter_settings = &ctx.project_info.config.filter_settings;
    let profiling_enabled = ctx.project_info.has_feature(Feature::Profiling);

    if !profiling_enabled {
        return Err(
            profile.reject_err(Outcome::Invalid(DiscardReason::FeatureDisabled(
                Feature::Profiling,
            ))),
        );
    }

    let Some(event) = event else {
        return Err(profile.reject_err(Outcome::Invalid(DiscardReason::NoEventPayload)));
    };

    let mut profile_id;
    let result = profile.try_modify(|profile, _| {
        profile_id = expand_profile(
            profile,
            event,
            ctx.config,
            client_ip,
            filter_settings,
            ctx.global_config,
        );
    });
    profile_id
}

/// Transfers transaction metadata to profile and check its size.
fn expand_profile(
    item: &mut Item,
    event: &Event,
    config: &Config,
    client_ip: Option<IpAddr>,
    filter_settings: &ProjectFiltersConfig,
    global_config: &GlobalConfig,
) -> Result<ProfileId, Outcome> {
    match relay_profiling::expand_profile(
        &item.payload(),
        event,
        client_ip,
        filter_settings,
        global_config,
    ) {
        Ok((id, payload)) => {
            if payload.len() <= config.max_profile_size() {
                item.set_payload(ContentType::Json, payload);
                Ok(id)
            } else {
                Err(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(relay_profiling::ProfileError::ExceedSizeLimit),
                )))
            }
        }
        Err(relay_profiling::ProfileError::Filtered(filter_stat_key)) => {
            Err(Outcome::Filtered(filter_stat_key))
        }
        Err(err) => Err(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(err),
        ))),
    }
}
