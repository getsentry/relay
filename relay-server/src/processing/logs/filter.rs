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
    logs.modify(|logs, records| {
        let meta = logs.headers.meta();
        logs.logs.retain_mut(|log| {
            let r = filter_log(log, meta, ctx);
            records.or_default(r.map(|_| true), &*log)
        })
    });
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
