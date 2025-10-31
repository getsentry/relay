use relay_dynamic_config::Feature;
use relay_event_schema::protocol::OurLog;
use relay_protocol::Annotated;

use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::logs::{Error, ExpandedLogs, Result};

/// Filters logs sent for a project which does not allow logs ingestion.
pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::OurLogsIngestion) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}

/// Applies inbound filters to individual logs.
pub fn filter(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    logs.retain_with_context(
        |logs| (&mut logs.logs, logs.headers.meta()),
        |log, meta, _| filter_log(log, meta, ctx),
    );
}

fn filter_log(log: &Annotated<OurLog>, meta: &RequestMeta, ctx: Context<'_>) -> Result<()> {
    let Some(log) = log.value() else {
        return Ok(());
    };

    relay_filter::should_filter(
        log,
        meta.client_addr(),
        &ctx.project_info.config.filter_settings,
        ctx.global_config.filters(),
    )
    .map_err(Error::Filtered)
}
