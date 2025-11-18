use std::collections::BTreeMap;

use chrono::{Duration as SignedDuration, Utc};
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::protocol::ClientReport;
use relay_filter::FilterStatKey;
use relay_quotas::ReasonCode;
use relay_sampling::evaluation::MatchedRuleIds;
use relay_system::Addr;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::managed::{Managed, Rejected};
use crate::processing::client_reports::{
    ClientReportField, Error, ExpandedClientReports, OutcomeEvent, SerializedClientReports,
    ValidatedClientReports,
};
use crate::services::outcome::{Outcome, RuleCategories, TrackOutcome};
use crate::services::processor::MINIMUM_CLOCK_DRIFT;
use crate::services::projects::project::ProjectInfo;

/// Parses and aggregates all client reports in their [`ExpandedClientReports`] representation.
///
/// Invalid reports will be discarded.
/// Events with overlong reasons will be skipped.
pub fn expand(client_reports: Managed<SerializedClientReports>) -> Managed<ExpandedClientReports> {
    client_reports.map(|client_reports, records| {
        let mut timestamp = None;
        let mut output_events = BTreeMap::new();

        for item in client_reports.client_reports {
            match ClientReport::parse(&item.payload()) {
                Ok(ClientReport {
                    timestamp: report_timestamp,
                    discarded_events,
                    rate_limited_events,
                    filtered_events,
                    filtered_sampling_events,
                }) => {
                    // Glue all discarded events together and give them the appropriate outcome type
                    let input_events = discarded_events
                        .into_iter()
                        .map(|discarded_event| (ClientReportField::ClientDiscard, discarded_event))
                        .chain(
                            filtered_events.into_iter().map(|discarded_event| {
                                (ClientReportField::Filtered, discarded_event)
                            }),
                        )
                        .chain(filtered_sampling_events.into_iter().map(|discarded_event| {
                            (ClientReportField::FilteredSampling, discarded_event)
                        }))
                        .chain(rate_limited_events.into_iter().map(|discarded_event| {
                            (ClientReportField::RateLimited, discarded_event)
                        }));

                    for (outcome_type, discarded_event) in input_events {
                        if discarded_event.reason.len() > 200 {
                            relay_log::trace!("ignored client outcome with an overlong reason");
                            // TODO: Not sure if we want to reject the individual 'discarded_event'
                            // here. Rejecting the entire item also seems wrong.
                            continue;
                        }
                        *output_events
                            .entry((
                                outcome_type,
                                discarded_event.reason,
                                discarded_event.category,
                            ))
                            .or_insert(0) += discarded_event.quantity;
                    }
                    if let Some(ts) = report_timestamp {
                        timestamp.get_or_insert(ts);
                    }
                }
                Err(err) => {
                    relay_log::trace!(
                        error = &err as &dyn std::error::Error,
                        "invalid client report received"
                    );
                    records.reject_err(Error::InvalidClientReport, item);
                }
            }
        }

        ExpandedClientReports {
            headers: client_reports.headers,
            timestamp,
            output_events: output_events
                .into_iter()
                .map(
                    |((outcome_type, reason, category), quantity)| OutcomeEvent {
                        outcome_type,
                        reason,
                        category,
                        quantity,
                    },
                )
                .collect(),
        }
    })
}

/// Validates and extracts client reports to their [`ValidatedClientReports`] representation.
pub fn validate(
    client_reports: Managed<ExpandedClientReports>,
    config: &Config,
    project_info: &ProjectInfo,
) -> Result<Managed<ValidatedClientReports>, Rejected<Error>> {
    let received = client_reports.received_at();
    let clock_drift_processor =
        ClockDriftProcessor::new(client_reports.headers.sent_at(), received)
            .at_least(MINIMUM_CLOCK_DRIFT);

    let mut timestamp = client_reports.timestamp;
    let timestamp =
        timestamp.get_or_insert_with(|| UnixTimestamp::from_secs(received.timestamp() as u64));

    if clock_drift_processor.is_drifted() {
        relay_log::trace!("applying clock drift correction to client report");
        clock_drift_processor.process_timestamp(timestamp);
    }

    let retention_days = project_info
        .config()
        .event_retention
        .unwrap_or(DEFAULT_EVENT_RETENTION);
    let max_age = SignedDuration::days(retention_days.into());
    // also if we unable to parse the timestamp, we assume it's way too old here.
    let in_past = timestamp
        .as_datetime()
        .map(|ts| (received - ts) > max_age)
        .unwrap_or(true);
    if in_past {
        relay_log::trace!(
            "skipping client outcomes older than {} days",
            max_age.num_days()
        );
        return Err(client_reports.reject_err(Error::TimestampTooOld));
    }

    let max_future = SignedDuration::seconds(config.max_secs_in_future());
    // also if we unable to parse the timestamp, we assume it's way far in the future here.
    let in_future = timestamp
        .as_datetime()
        .map(|ts| (ts - received) > max_future)
        .unwrap_or(true);
    if in_future {
        relay_log::trace!(
            "skipping client outcomes more than {}s in the future",
            max_future.num_seconds()
        );
        return Err(client_reports.reject_err(Error::TimestampInFuture));
    }

    let scoping = client_reports.scoping();
    Ok(client_reports.map(|client_reports, records| {
        let mut outcomes = Vec::new();
        for event_outcome in client_reports.output_events {
            let OutcomeEvent {
                outcome_type,
                ref reason,
                category,
                quantity,
            } = event_outcome;

            let Ok(outcome) = outcome_from_parts(outcome_type, reason) else {
                relay_log::trace!(?outcome_type, reason, "invalid outcome combination");
                records.reject_err(Error::InvalidOutcome, event_outcome);
                continue;
            };
            outcomes.push(TrackOutcome {
                //   If we get to this point, the unwrap should not be used anymore, since we know by
                // now that the timestamp can be parsed, but just incase we fallback to UTC current
                // `DateTime`.
                timestamp: timestamp.as_datetime().unwrap_or_else(Utc::now),
                scoping,
                outcome,
                event_id: None,
                remote_addr: None, // omitting the client address allows for better aggregation
                category,
                quantity,
            });
        }

        ValidatedClientReports { outcomes }
    }))
}

/// Emits the validated client report outcomes to the outcome aggregator.
pub fn emit(
    client_reports: Managed<ValidatedClientReports>,
    outcome_aggregator: &Addr<TrackOutcome>,
) {
    client_reports.accept(|client_reports| {
        for outcome in client_reports.outcomes {
            outcome_aggregator.send(outcome)
        }
    })
}

/// Parse an outcome from an outcome ID and a reason string.
///
/// Currently only used to reconstruct outcomes encoded in client reports.
fn outcome_from_parts(field: ClientReportField, reason: &str) -> Result<Outcome, ()> {
    match field {
        ClientReportField::FilteredSampling => match reason.strip_prefix("Sampled:") {
            Some(rule_ids) => MatchedRuleIds::parse(rule_ids)
                .map(RuleCategories::from)
                .map(Outcome::FilteredSampling)
                .map_err(|_| ()),
            None => Err(()),
        },
        ClientReportField::ClientDiscard => Ok(Outcome::ClientDiscard(reason.into())),
        ClientReportField::Filtered => Ok(Outcome::Filtered(
            FilterStatKey::try_from(reason).map_err(|_| ())?,
        )),
        ClientReportField::RateLimited => Ok(Outcome::RateLimited(match reason {
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
        assert!(outcome_from_parts(ClientReportField::FilteredSampling, "adsf").is_err());

        assert!(outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:").is_err());

        assert!(outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:foo").is_err());

        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:"),
            Err(())
        ));

        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:;"),
            Err(())
        ));

        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:ab;12"),
            Err(())
        ));

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123,456"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::Other].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::Other].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::Other].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:1001"),
            Ok(Outcome::FilteredSampling(RuleCategories(
                [RuleCategory::BoostEnvironments].into()
            )))
        );

        assert_eq!(
            outcome_from_parts(
                ClientReportField::FilteredSampling,
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
            outcome_from_parts(ClientReportField::Filtered, "error-message"),
            Ok(Outcome::Filtered(FilterStatKey::ErrorMessage))
        ));

        assert!(matches!(
            outcome_from_parts(ClientReportField::Filtered, "hydration-error"),
            Ok(Outcome::Filtered(FilterStatKey::GenericFilter(_)))
        ));
    }

    #[test]
    fn test_from_outcome_type_client_discard() {
        assert_eq!(
            outcome_from_parts(ClientReportField::ClientDiscard, "foo_reason").unwrap(),
            Outcome::ClientDiscard("foo_reason".into())
        );
    }

    #[test]
    fn test_from_outcome_type_rate_limited() {
        assert!(matches!(
            outcome_from_parts(ClientReportField::RateLimited, ""),
            Ok(Outcome::RateLimited(None))
        ));
        assert_eq!(
            outcome_from_parts(ClientReportField::RateLimited, "foo_reason").unwrap(),
            Outcome::RateLimited(Some(ReasonCode::new("foo_reason")))
        );
    }
}
