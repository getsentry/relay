use relay_filter::Filterable;
use relay_protocol::Getter;

use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::sessions::{Error, ExpandedSessions, Result};

/// Applies inbound filters to individual sessions.
pub fn filter(sessions: &mut Managed<ExpandedSessions>, ctx: Context<'_>) {
    sessions.retain_with_context(
        |sessions| (&mut sessions.updates, sessions.headers.meta()),
        |update, meta, _| filter_session(update, meta, ctx),
    );

    sessions.retain_with_context(
        |sessions| (&mut sessions.aggregates, sessions.headers.meta()),
        |aggregate, meta, _| filter_session(aggregate, meta, ctx),
    );
}

fn filter_session<T>(session: &T, meta: &RequestMeta, ctx: Context<'_>) -> Result<()>
where
    T: Filterable + Getter,
{
    relay_filter::should_filter(
        session,
        meta.client_addr(),
        &ctx.project_info.config.filter_settings,
        ctx.global_config.filters(),
    )
    .map_err(Error::Filtered)
}
