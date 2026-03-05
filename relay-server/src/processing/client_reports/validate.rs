use chrono::{DateTime, Duration, Utc};
use relay_common::time::UnixTimestamp;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::client_reports::{ClientOutcomes, Error, Result};

/// The max allowed length for a client report outcome reason.
const MAX_OUTCOME_REASON_LENGTH: usize = 200;

/// Validates individual client report outcomes, rejecting invalid ones.
///
/// Checks the outcome reason is not overlong and that the timestamp is within an acceptable range.
pub fn validate(outcomes: &mut Managed<ClientOutcomes>, ctx: Context<'_>) {
    let retention_days = ctx
        .project_info
        .config
        .event_retention
        .unwrap_or(DEFAULT_EVENT_RETENTION);
    let received_at = outcomes.received_at();
    let max_age = Duration::days(retention_days.into());
    let max_future = Duration::seconds(ctx.config.max_secs_in_future());

    outcomes.retain(
        |outcomes| &mut outcomes.outcomes,
        |outcome, _| {
            validate_reason(&outcome.reason)?;
            validate_timestamp(outcome.timestamp, received_at, max_age, max_future)
        },
    );
}

fn validate_reason(reason: &str) -> Result<()> {
    if reason.len() > MAX_OUTCOME_REASON_LENGTH {
        relay_log::trace!("reject client outcome with an overlong reason");
        return Err(Error::OverlongOutcomeReason);
    }
    Ok(())
}

fn validate_timestamp(
    timestamp: UnixTimestamp,
    received_at: DateTime<Utc>,
    max_age: Duration,
    max_future: Duration,
) -> Result<()> {
    let timestamp = timestamp.as_datetime().ok_or(Error::InvalidTimestamp)?;

    if (received_at - timestamp) > max_age {
        relay_log::trace!(
            "reject client outcome older than {} days",
            max_age.num_days()
        );
        return Err(Error::OutcomeTooOld);
    }
    if (timestamp - received_at) > max_future {
        relay_log::trace!(
            "reject client outcome more than {}s in the future",
            max_future.num_seconds()
        );
        return Err(Error::OutcomeInFuture);
    }
    Ok(())
}
