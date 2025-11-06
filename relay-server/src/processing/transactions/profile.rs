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
use crate::managed::{Counted, ItemAction, Managed, ManagedResult, Rejected, TypedEnvelope};
use crate::processing::transactions::{Error, SerializedTransaction};
use crate::processing::{Context, CountRateLimited};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::TransactionGroup;
use crate::services::projects::project::ProjectInfo;

pub struct Profile(pub Item);

impl Counted for Profile {
    fn quantities(&self) -> crate::managed::Quantities {
        self.0.quantities()
    }
}

impl CountRateLimited for Managed<Profile> {
    type Error = super::Error;
}

/// Transfers the profile ID from the profile item to the transaction item.
///
/// The profile id may be `None` when the envelope does not contain a profile,
/// in that case the profile context is removed.
/// Some SDKs send transactions with profile ids but omit the profile in the envelope.
pub fn transfer_id(event: &mut Annotated<Event>, profile_id: Option<ProfileId>) {
    let Some(event) = event.value_mut() else {
        return;
    };

    match profile_id {
        Some(profile_id) => {
            let contexts = event.contexts.get_or_insert_with(Contexts::new);
            contexts.add(ProfileContext {
                profile_id: Annotated::new(profile_id),
                ..ProfileContext::default()
            });
        }
        None => {
            if let Some(contexts) = event.contexts.value_mut()
                && let Some(profile_context) = contexts.get_mut::<ProfileContext>()
            {
                profile_context.profile_id = Annotated::empty();
            }
        }
    }
}

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
#[must_use]
pub fn process(
    profile: &mut Item,
    client_ip: Option<IpAddr>,
    event: Option<&Event>,
    ctx: &Context,
) -> Result<ProfileId, Outcome> {
    debug_assert_eq!(profile.ty(), &ItemType::Profile);
    let filter_settings = &ctx.project_info.config.filter_settings;
    let profiling_enabled = ctx.project_info.has_feature(Feature::Profiling);

    if !profiling_enabled {
        return Err(Outcome::Invalid(DiscardReason::FeatureDisabled(
            Feature::Profiling,
        )));
    }

    let Some(event) = event else {
        return Err(Outcome::Invalid(DiscardReason::NoEventPayload));
    };

    expand_profile(
        profile,
        event,
        ctx.config,
        client_ip,
        filter_settings,
        ctx.global_config,
    )
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
