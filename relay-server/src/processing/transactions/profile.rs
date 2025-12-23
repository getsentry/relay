//! Profiles related processor code.
use relay_base_schema::project::ProjectId;
use relay_dynamic_config::{Feature, GlobalConfig};
use relay_quotas::{DataCategory, Scoping};
use std::net::IpAddr;

use relay_config::Config;
use relay_event_schema::protocol::{Contexts, Event, ProfileContext};
use relay_filter::ProjectFiltersConfig;
use relay_profiling::{ProfileId, ProfileType};
use relay_protocol::{Annotated, Empty};
use relay_protocol::{Getter, Remark, RemarkType};

use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, Quantities, RecordKeeper};
use crate::processing::transactions::{Error, ExpandedTransaction, Transaction};
use crate::processing::{Context, CountRateLimited};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::utils::should_filter;

/// An item wrapper that counts as profile.
#[derive(Debug)]
pub struct Profile(pub Item);

impl Counted for Profile {
    fn quantities(&self) -> Quantities {
        self.0.quantities()
    }
}

/// A profile with metadata required to forward it.
#[derive(Debug)]
pub struct ProfileWithHeaders {
    pub headers: EnvelopeHeaders,
    pub item: Item,
}

impl Counted for ProfileWithHeaders {
    fn quantities(&self) -> Quantities {
        self.item.quantities()
    }
}

impl CountRateLimited for Managed<ProfileWithHeaders> {
    type Error = Error;
}

/// Filters out invalid profiles.
///
/// Returns the profile id of the single remaining profile, if there is one.
pub fn filter(
    work: &mut ExpandedTransaction<Transaction>,
    record_keeper: &mut RecordKeeper,
    ctx: Context,
    project_id: ProjectId,
) -> Option<ProfileId> {
    let profile_item = work.profile.as_ref()?;

    let feature = Feature::Profiling;
    let mut profile_id = None;
    if should_filter(ctx.config, ctx.project_info, feature) {
        record_keeper.reject_err(
            Outcome::Invalid(DiscardReason::FeatureDisabled(feature)),
            work.profile.take(),
        );
    } else if work.transaction.0.value().is_none() && profile_item.sampled() {
        // A profile with `sampled=true` should never be without a transaction
        record_keeper.reject_err(
            Outcome::Invalid(DiscardReason::Profiling("missing_transaction")),
            work.profile.take(),
        );
    } else {
        match relay_profiling::parse_metadata(&profile_item.payload(), project_id) {
            Ok(id) => {
                profile_id = Some(id);
            }
            Err(err) => {
                record_keeper.reject_err(
                    Outcome::Invalid(DiscardReason::Profiling(relay_profiling::discard_reason(
                        &err,
                    ))),
                    work.profile.take(),
                );
            }
        }
    }
    profile_id
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

/// Removes the profile context from the transaction item if there is an active rate limit.
///
/// With continuous profiling profile chunks are ingested separately to transactions,
/// in the case where these profiles are rate limited the link on the associated transaction(s)
/// should also be removed.
///
/// See also: <https://github.com/getsentry/relay/issues/5071>.
pub fn remove_context_if_rate_limited(
    event: &mut Annotated<Event>,
    scoping: Scoping,
    ctx: Context<'_>,
) {
    let Some(event) = event.value_mut() else {
        return;
    };

    // There is always only either a transaction profile or a continuous profile, never both.
    //
    // If the `profiler_id` is set on the context, it is for a continuous profile, the case we want
    // to handle here.
    // If it is empty -> do nothing.
    let profile_ctx = event.context::<ProfileContext>();
    if profile_ctx.is_none_or(|pctx| pctx.profiler_id.is_empty()) {
        return;
    }

    // Continuous profiling has two separate categories based on the platform, infer the correct
    // category to check for rate limits.
    let categories = match event.platform.as_str().map(ProfileType::from_platform) {
        Some(ProfileType::Ui) => &[
            scoping.item(DataCategory::ProfileChunkUi),
            scoping.item(DataCategory::ProfileDurationUi),
        ],
        Some(ProfileType::Backend) => &[
            scoping.item(DataCategory::ProfileChunk),
            scoping.item(DataCategory::ProfileDuration),
        ],
        _ => return,
    };

    // This is a 'best effort' approach, which is why it is enough to check against cached rate
    // limits here.
    let is_limited = ctx
        .rate_limits
        .is_any_limited_with_quotas(ctx.project_info.get_quotas(), categories);

    if is_limited && let Some(contexts) = event.contexts.value_mut() {
        let _ = contexts.remove::<ProfileContext>();
    }
}

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
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

/// Strip out the profiler_id from the transaction's profile context if the transaction lasts less than 20ms.
///
/// This is necessary because if the transaction lasts less than 19.8ms, we know that the respective
/// profile data won't have enough samples to be of any use, hence we "unlink" the profile from the transaction.
pub fn scrub_profiler_id(event: &mut Annotated<Event>) {
    let Some(event) = event.value_mut() else {
        return;
    };
    let transaction_duration = event
        .get_value("event.duration")
        .and_then(|duration| duration.as_f64());

    if !transaction_duration.is_some_and(|duration| duration < 19.8) {
        return;
    }
    if let Some(contexts) = event.contexts.value_mut().as_mut()
        && let Some(profiler_id) = contexts
            .get_mut::<ProfileContext>()
            .map(|ctx| &mut ctx.profiler_id)
    {
        let id = std::mem::take(profiler_id.value_mut());
        let remark = Remark::new(RemarkType::Removed, "transaction_duration");
        profiler_id.meta_mut().add_remark(remark);
        profiler_id.meta_mut().set_original_value(id);
    }
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
                    relay_profiling::discard_reason(
                        &relay_profiling::ProfileError::ExceedSizeLimit,
                    ),
                )))
            }
        }
        Err(relay_profiling::ProfileError::Filtered(filter_stat_key)) => {
            Err(Outcome::Filtered(filter_stat_key))
        }
        Err(err) => Err(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(&err),
        ))),
    }
}

#[cfg(test)]
#[cfg(feature = "processing")]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use relay_base_schema::events::EventType;
    use relay_event_schema::protocol::EventId;
    use relay_protocol::get_value;
    use uuid::Uuid;

    #[test]
    fn test_scrub_profiler_id_should_not_be_stripped() {
        let mut contexts = Contexts::new();
        contexts.add(ProfileContext {
            profiler_id: Annotated::new(EventId(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )),
            ..Default::default()
        });
        let mut event: Annotated<Event> = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
                    .unwrap()
                    .checked_add_signed(Duration::milliseconds(20))
                    .unwrap()
                    .into(),
            ),
            contexts: Annotated::new(contexts),
            ..Default::default()
        });

        scrub_profiler_id(&mut event);

        let profile_context = get_value!(event.contexts)
            .unwrap()
            .get::<ProfileContext>()
            .unwrap();

        assert!(
            !profile_context
                .profiler_id
                .meta()
                .iter_remarks()
                .any(|remark| remark.rule_id == *"transaction_duration"
                    && remark.ty == RemarkType::Removed)
        )
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_scrub_profiler_id_should_be_stripped() {
        let mut contexts = Contexts::new();
        contexts.add(ProfileContext {
            profiler_id: Annotated::new(EventId(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )),
            ..Default::default()
        });
        let mut event: Annotated<Event> = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
                    .unwrap()
                    .checked_add_signed(Duration::milliseconds(15))
                    .unwrap()
                    .into(),
            ),
            contexts: Annotated::new(contexts),
            ..Default::default()
        });

        scrub_profiler_id(&mut event);

        let profile_context = get_value!(event.contexts)
            .unwrap()
            .get::<ProfileContext>()
            .unwrap();

        assert!(
            profile_context
                .profiler_id
                .meta()
                .iter_remarks()
                .any(|remark| remark.rule_id == *"transaction_duration"
                    && remark.ty == RemarkType::Removed)
        )
    }
}
