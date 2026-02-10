use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::protocol::{SessionAggregates, SessionAttributes, SessionUpdate};
use relay_quotas::DataCategory;

use crate::envelope::Item;
use crate::managed::{Managed, RecordKeeper};
use crate::metrics_extraction;
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::processing::Context;
use crate::processing::sessions::{Error, ExpandedSessions, Result, SerializedSessions};
use crate::services::processor::MINIMUM_CLOCK_DRIFT;
use crate::statsd::RelayTimers;

/// Result of [`expand`].
pub enum Expansion {
    /// Continue processing with the expanded sessions.
    Continue(Managed<ExpandedSessions>),
    /// Do not continue processing, only forward the sessions.
    Forward(Managed<SerializedSessions>),
}

pub fn expand(sessions: Managed<SerializedSessions>, ctx: Context<'_>) -> Expansion {
    let can_extract_metrics = ctx.project_info.config.session_metrics.is_enabled();

    // Always extract metrics in processing Relays, even if the metrics extraction for sessions
    // is erroneous. In the migration phase from specific session payloads to session metrics,
    // it was possible to still forward sessions as payloads instead of items, this no longer
    // exists.
    //
    // Having metrics extraction *not* enabled for processing Relays is a misconfiguration.
    // The alternative to not extracting metrics here is to just drop them, which would be worse
    // than extracting.
    if !can_extract_metrics && !ctx.is_processing() {
        return Expansion::Forward(sessions);
    }

    let expanded = sessions.map(|sessions, records| {
        let mut updates = Vec::with_capacity(sessions.updates.len());
        for item in sessions.updates {
            match expand_session(&item) {
                Ok(value) => updates.push(value),
                Err(err) => drop(records.reject_err(err, item)),
            }
        }

        let mut aggregates = Vec::with_capacity(sessions.aggregates.len());
        for item in sessions.aggregates {
            match expand_session_aggregates(&item) {
                Ok(value) => aggregates.push(value),
                Err(err) => drop(records.reject_err(err, item)),
            }
        }

        ExpandedSessions {
            headers: sessions.headers,
            updates,
            aggregates,
        }
    });

    Expansion::Continue(expanded)
}

fn expand_session(item: &Item) -> Result<SessionUpdate> {
    let payload = item.payload();

    SessionUpdate::parse(&payload).map_err(|err| {
        relay_log::trace!(
            error = &err as &dyn std::error::Error,
            "invalid session aggregate payload"
        );
        Error::Invalid
    })
}

fn expand_session_aggregates(item: &Item) -> Result<SessionAggregates> {
    let payload = item.payload();

    SessionAggregates::parse(&payload).map_err(|err| {
        relay_log::trace!(
            error = &err as &dyn std::error::Error,
            "invalid session aggregate payload"
        );
        Error::Invalid
    })
}

pub fn normalize(sessions: &mut Managed<ExpandedSessions>, ctx: Context<'_>) {
    let received_at = sessions.received_at();
    let ctx = NormalizeContext {
        ctx,
        clock_drift: ClockDriftProcessor::new(sessions.headers.sent_at(), received_at)
            .at_least(MINIMUM_CLOCK_DRIFT),
        received_at,
        client_addr: sessions.headers.meta().client_addr(),
    };

    sessions.retain(
        |sessions| &mut sessions.updates,
        |update, _| normalize_update(update, &ctx),
    );

    sessions.retain(
        |sessions| &mut sessions.aggregates,
        |aggregates, records| normalize_aggregates(aggregates, &ctx, records),
    );
}

struct NormalizeContext<'a> {
    ctx: Context<'a>,
    clock_drift: ClockDriftProcessor,
    received_at: DateTime<Utc>,
    client_addr: Option<IpAddr>,
}

impl NormalizeContext<'_> {
    fn has_delay(&self, ts: DateTime<Utc>) -> Option<std::time::Duration> {
        let delay = self.received_at - ts;
        delay.to_std().ok().filter(|delay| delay.as_secs() >= 60)
    }

    fn validate_session_timestamp(&self, timestamp: DateTime<Utc>) -> Result<()> {
        let max_age = chrono::Duration::seconds(self.ctx.config.max_session_secs_in_past());
        if (self.received_at - timestamp) > max_age {
            relay_log::trace!("skipping session older than {} days", max_age.num_days());
            return Err(Error::Invalid);
        }

        let max_future = chrono::Duration::seconds(self.ctx.config.max_secs_in_future());
        if (timestamp - self.received_at) > max_future {
            relay_log::trace!(
                "skipping session more than {}s in the future",
                max_future.num_seconds()
            );
            return Err(Error::Invalid);
        }

        Ok(())
    }
}

fn normalize_update(session: &mut SessionUpdate, ctx: &NormalizeContext<'_>) -> Result<()> {
    if ctx.clock_drift.is_drifted() {
        relay_log::trace!("applying clock drift correction to session");
        ctx.clock_drift.process_datetime(&mut session.started);
        ctx.clock_drift.process_datetime(&mut session.timestamp);
    }

    if session.timestamp < session.started {
        relay_log::trace!("fixing session timestamp to {}", session.timestamp);
        session.timestamp = session.started;
    }

    // Log the timestamp delay for all sessions after clock drift correction.
    if let Some(delay) = ctx.has_delay(session.timestamp) {
        relay_statsd::metric!(
            timer(RelayTimers::TimestampDelay) = delay,
            category = "session",
        );
    }

    ctx.validate_session_timestamp(session.timestamp)?;
    ctx.validate_session_timestamp(session.started)?;

    normalize_attributes(&mut session.attributes, ctx)?;

    Ok(())
}

fn normalize_aggregates(
    aggregates: &mut SessionAggregates,
    ctx: &NormalizeContext<'_>,
    records: &mut RecordKeeper<'_>,
) -> Result<()> {
    if ctx.clock_drift.is_drifted() {
        relay_log::trace!("applying clock drift correction to session");
        for aggregate in &mut aggregates.aggregates {
            ctx.clock_drift.process_datetime(&mut aggregate.started);
        }
    }

    aggregates.aggregates.retain(|aggregate| {
        match ctx.validate_session_timestamp(aggregate.started) {
            Ok(()) => true,
            Err(err) => {
                records.reject_err(err, aggregate);
                false
            }
        }
    });

    normalize_attributes(&mut aggregates.attributes, ctx)?;

    Ok(())
}

fn normalize_attributes(attrs: &mut SessionAttributes, ctx: &NormalizeContext<'_>) -> Result<()> {
    relay_event_normalization::validate_release(&attrs.release).map_err(|_| Error::Invalid)?;

    attrs.environment = attrs
        .environment
        .take()
        .filter(|env| relay_event_normalization::validate_environment(env).is_ok());

    if attrs.ip_address.as_ref().is_some_and(|ip| ip.is_auto()) {
        attrs.ip_address = ctx.client_addr.map(Into::into);
    }

    Ok(())
}

pub fn extract_metrics(
    sessions: Managed<ExpandedSessions>,
    ctx: Context<'_>,
) -> Managed<ExtractedMetrics> {
    let should_extract_abnormal_mechanism = ctx
        .project_info
        .config
        .session_metrics
        .should_extract_abnormal_mechanism();

    sessions.map(|sessions, records| {
        let mut metrics = Vec::new();
        let meta = sessions.headers.meta();

        for update in sessions.updates {
            records.modify_by(DataCategory::Session, -1);
            metrics_extraction::sessions::extract_session_metrics(
                &update.attributes,
                &update,
                meta.client(),
                &mut metrics,
                should_extract_abnormal_mechanism,
            );
        }

        for aggregates in sessions.aggregates {
            for item in aggregates.aggregates {
                records.modify_by(DataCategory::Session, -1);
                metrics_extraction::sessions::extract_session_metrics(
                    &aggregates.attributes,
                    &item,
                    meta.client(),
                    &mut metrics,
                    should_extract_abnormal_mechanism,
                );
            }
        }

        records.modify_by(DataCategory::MetricBucket, metrics.len() as isize);

        ExtractedMetrics {
            project_metrics: metrics,
            sampling_metrics: Vec::new(),
        }
    })
}
