use std::net::IpAddr;

use chrono::Utc;
use relay_common::time::UnixTimestamp;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::protocol::{ClientReport, DiscardedEvent};
use relay_filter::FilterStatKey;
use relay_quotas::{ReasonCode, Scoping};
use relay_sampling::evaluation::MatchedRuleIds;
use relay_system::Addr;

use crate::managed::Managed;
use crate::processing::client_reports::{
    ClientOutcome, ClientOutcomeType, ClientOutcomes, Error, SerializedClientReports,
};
use crate::services::outcome::{Outcome, RuleCategories, TrackOutcome};
use crate::services::processor::MINIMUM_CLOCK_DRIFT;

/// Parses serialized client reports and extracts their outcomes.
///
/// Individual reports that fail to parse are rejected.
pub fn expand(reports: Managed<SerializedClientReports>) -> Managed<ClientOutcomes> {
    reports.map(|reports, records| {
        let SerializedClientReports { headers, reports } = reports;
        let mut outcomes = Vec::new();
        let default_timestamp =
            UnixTimestamp::from_secs(headers.meta().received_at().timestamp() as u64);

        for report in reports {
            match ClientReport::parse(&report.payload()) {
                Ok(client_report) => expand_report(client_report, default_timestamp, &mut outcomes),
                Err(err) => {
                    relay_log::debug!(
                        error = &err as &dyn std::error::Error,
                        "invalid client report received"
                    );
                    records.reject_err(Error::from(err), report);
                }
            }
        }

        ClientOutcomes { headers, outcomes }
    })
}

/// Extracts the outcomes from a single parsed client report.
///
/// If the report does not have a timestamp falls back to the envelope's received-at time.
fn expand_report(
    report: ClientReport,
    default_timestamp: UnixTimestamp,
    outcomes: &mut Vec<ClientOutcome>,
) {
    let timestamp = report.timestamp.unwrap_or(default_timestamp);

    for (outcome_type, events) in [
        (ClientOutcomeType::ClientDiscard, report.discarded_events),
        (ClientOutcomeType::RateLimited, report.rate_limited_events),
        (ClientOutcomeType::Filtered, report.filtered_events),
        (
            ClientOutcomeType::FilteredSampling,
            report.filtered_sampling_events,
        ),
    ] {
        outcomes.extend(events.into_iter().map(
            |DiscardedEvent {
                 reason,
                 category,
                 quantity,
             }| ClientOutcome {
                outcome_type,
                timestamp,
                reason,
                category,
                quantity,
            },
        ));
    }
}

/// Normalizes individual client report outcomes.
///
/// Applies clock drift correction on the client report outcome timestamps.
pub fn normalize(outcomes: &mut Managed<ClientOutcomes>) {
    let clock_drift_processor =
        ClockDriftProcessor::new(outcomes.headers.sent_at(), outcomes.received_at())
            .at_least(MINIMUM_CLOCK_DRIFT);

    if clock_drift_processor.is_drifted() {
        relay_log::trace!("applying clock drift correction to client report outcomes");
        outcomes.modify(|outcomes, _| {
            for outcome in &mut outcomes.outcomes {
                clock_drift_processor.process_timestamp(&mut outcome.timestamp);
            }
        });
    }
}

/// Emits outcomes to the outcome aggregator.
///
/// Client report outcomes that cannot be converted to an [`Outcome`] are rejected.
pub fn emit(outcomes: Managed<ClientOutcomes>, outcome_aggregator: &Addr<TrackOutcome>) {
    let scoping = outcomes.scoping();
    let remote_addr = outcomes.headers.meta().remote_addr();

    for outcome in outcomes.split(|outcomes| outcomes.outcomes) {
        let _ = outcome
            .try_accept(|outcome| emit_outcome(outcome, outcome_aggregator, scoping, remote_addr));
    }
}

fn emit_outcome(
    outcome: ClientOutcome,
    outcome_aggregator: &Addr<TrackOutcome>,
    scoping: Scoping,
    remote_addr: Option<IpAddr>,
) -> Result<(), Error> {
    let ClientOutcome {
        outcome_type,
        timestamp,
        reason,
        category,
        quantity,
    } = outcome;

    let outcome = outcome_from_parts(outcome_type, &reason).map_err(|()| {
        relay_log::trace!(?outcome_type, reason, "invalid outcome");
        Error::InvalidOutcome
    })?;

    outcome_aggregator.send(TrackOutcome {
        // If we get to this point, the unwrap should not be used anymore, since we know by
        // now that the timestamp can be parsed, but just in case we fallback to UTC current
        // `DateTime`.
        timestamp: timestamp.as_datetime().unwrap_or_else(Utc::now),
        scoping,
        outcome,
        event_id: None,
        remote_addr,
        category,
        quantity,
    });
    Ok(())
}

/// Reconstructs an [`Outcome`] from a [`ClientOutcomeType`] and reason string.
fn outcome_from_parts(outcome_type: ClientOutcomeType, reason: &str) -> Result<Outcome, ()> {
    match outcome_type {
        ClientOutcomeType::FilteredSampling => match reason.strip_prefix("Sampled:") {
            Some(rule_ids) => MatchedRuleIds::parse(rule_ids)
                .map(RuleCategories::from)
                .map(Outcome::FilteredSampling)
                .map_err(|_| ()),
            None => Err(()),
        },
        ClientOutcomeType::ClientDiscard => Ok(Outcome::ClientDiscard(reason.into())),
        ClientOutcomeType::Filtered => Ok(Outcome::Filtered(
            FilterStatKey::try_from(reason).map_err(|_| ())?,
        )),
        ClientOutcomeType::RateLimited => Ok(Outcome::RateLimited(match reason {
            "" => None,
            other => Some(ReasonCode::new(other)),
        })),
    }
}

#[cfg(test)]
mod tests {
    use crate::services::outcome::RuleCategory;

    use super::*;

    #[test]
    fn test_from_outcome_type_sampled() {
        assert!(outcome_from_parts(ClientOutcomeType::FilteredSampling, "adsf").is_err());

        assert!(outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:").is_err());

        assert!(outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:foo").is_err());

        assert!(matches!(
            outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:"),
            Err(())
        ));

        assert!(matches!(
            outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:;"),
            Err(())
        ));

        assert!(matches!(
            outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:ab;12"),
            Err(())
        ));

        assert_eq!(
            outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:123,456"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::Other].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:123"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::Other].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(ClientOutcomeType::FilteredSampling, "Sampled:1001"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::BoostEnvironments].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(
                ClientOutcomeType::FilteredSampling,
                "Sampled:1001,1456,1567,3333,4444"
            ),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [
                    RuleCategory::BoostEnvironments,
                    RuleCategory::BoostLowVolumeTransactions,
                    RuleCategory::BoostLatestReleases,
                    RuleCategory::Custom
                ]
                .into()
            )))
        );
    }

    #[test]
    fn test_from_outcome_type_filtered() {
        assert!(matches!(
            outcome_from_parts(ClientOutcomeType::Filtered, "error-message"),
            Ok(Outcome::Filtered(FilterStatKey::ErrorMessage))
        ));

        assert!(matches!(
            outcome_from_parts(ClientOutcomeType::Filtered, "hydration-error"),
            Ok(Outcome::Filtered(FilterStatKey::GenericFilter(_)))
        ));
    }

    #[test]
    fn test_from_outcome_type_client_discard() {
        assert_eq!(
            outcome_from_parts(ClientOutcomeType::ClientDiscard, "foo_reason").unwrap(),
            Outcome::ClientDiscard("foo_reason".into())
        );
    }

    #[test]
    fn test_from_outcome_type_rate_limited() {
        assert!(matches!(
            outcome_from_parts(ClientOutcomeType::RateLimited, ""),
            Ok(Outcome::RateLimited(None))
        ));
        assert_eq!(
            outcome_from_parts(ClientOutcomeType::RateLimited, "foo_reason").unwrap(),
            Outcome::RateLimited(Some(ReasonCode::new("foo_reason")))
        );
    }
}
