use relay_dynamic_config::Feature;
use relay_statsd::metric;

use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::replays::{Error, ExpandedReplay, Result, SerializedReplays};
use crate::statsd::RelayCounters;

/// Maximum expected segment ID for a replay session, under normal operation.
const MAX_SEGMENTS_ID: u64 = 720;

/// Filters replays sent for a project which does not allow replay ingestion.
pub fn feature_flag(
    replays: Managed<SerializedReplays>,
    ctx: Context<'_>,
) -> Result<Managed<SerializedReplays>, Rejected<Error>> {
    match ctx.should_filter(Feature::SessionReplay)
        || (ctx
            .project_info
            .has_feature(Feature::SessionReplayVideoDisabled)
            && !replays.videos.is_empty())
    {
        true => Err(replays.reject_err(Error::FilterFeatureFlag)),
        false => Ok(replays),
    }
}

/// Applies inbound filters to a replay.
pub fn filter(replay: &ExpandedReplay, ctx: Context<'_>) -> Result<()> {
    let Some(event) = replay.payload.event() else {
        return Ok(());
    };

    let client_addr = replay.headers.meta().client_addr();
    let event_id = replay.headers.event_id();
    let event = event.value().ok_or(Error::NoEventContent)?;

    relay_filter::should_filter(
        event,
        client_addr,
        &ctx.project_info.config.filter_settings,
        ctx.global_config.filters(),
    )
    .map_err(Error::Filtered)?;

    // Log segments that exceed the hour limit so we can diagnose errant SDKs
    // or exotic customer implementations.
    if let Some(segment_id) = event.segment_id.value()
        && *segment_id > MAX_SEGMENTS_ID
    {
        metric!(counter(RelayCounters::ReplayExceededSegmentLimit) += 1);

        relay_log::debug!(
            event_id = ?event_id,
            project_id = ctx.project_info.project_id.map(|v| v.value()),
            organization_id = ctx.project_info.organization_id.map(|o| o.value()),
            segment_id = segment_id,
            "replay segment-exceeded-limit"
        );
    }

    Ok(())
}
