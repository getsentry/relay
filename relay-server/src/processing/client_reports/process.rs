use std::collections::BTreeMap;
use std::error::Error;

use chrono::{Duration as SignedDuration, Utc};
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::protocol::ClientReport;
use relay_filter::FilterStatKey;
use relay_quotas::ReasonCode;
use relay_sampling::evaluation::MatchedRuleIds;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::managed::Managed;
use crate::processing::client_reports::SerializedClientReport;
use crate::services::outcome::{Outcome, RuleCategories, TrackOutcome};
use crate::services::processor::MINIMUM_CLOCK_DRIFT;
use crate::services::projects::project::ProjectInfo;

use crate::processing::client_reports;

/// Fields of client reports that map to specific [`Outcome`]s without content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClientReportField {
    /// The event has been filtered by an inbound data filter.
    Filtered,

    /// The event has been filtered by a sampling rule.
    FilteredSampling,

    /// The event has been rate limited.
    RateLimited,

    /// The event has already been discarded on the client side.
    ClientDiscard,
}

/// Validates and extracts client reports.
///
/// At the moment client reports are primarily used to transfer outcomes from
/// client SDKs.  The outcomes are removed here and sent directly to the outcomes
/// system.
pub fn process_client_reports(
    client_reports: &mut Managed<SerializedClientReport>,
    config: &Config,
    project_info: &ProjectInfo,
) -> Vec<TrackOutcome> {
    // if client outcomes are disabled we leave the client reports unprocessed
    // and pass them on.
    if !config.emit_outcomes().any() || !config.emit_client_outcomes() {
        // if a processing relay has client outcomes disabled we drop them.
        if config.processing_enabled() {
            // FIXME: Understand how to best drop them here silently
            todo!("Drop all the items silently");
        }
        return vec![];
    }

    let mut timestamp = None;
    let mut output_events = BTreeMap::new();
    let received = client_reports.received_at();

    let clock_drift_processor =
        ClockDriftProcessor::new(client_reports.headers.sent_at(), received)
            .at_least(MINIMUM_CLOCK_DRIFT);

    // we're going through all client reports but we're effectively just merging
    // them into the first one.
    client_reports.retain(
        |client_reports| &mut client_reports.client_reports,
        |item, _| {
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
                    relay_log::trace!(error = &err as &dyn Error, "invalid client report received")
                }
            }
            // FIXME: Understand what the equivalent of just dropping them here silently would be.
            Ok::<_, client_reports::Error>(())
        },
    );

    if output_events.is_empty() {
        return vec![];
    }

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
        return vec![];
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
        return vec![];
    }

    // FIXME: This can be done more elegantly
    let mut outcome_collection: Vec<TrackOutcome> = vec![];
    for ((outcome_type, reason, category), quantity) in output_events.into_iter() {
        let outcome = match outcome_from_parts(outcome_type, &reason) {
            Ok(outcome) => outcome,
            Err(_) => {
                relay_log::trace!(?outcome_type, reason, "invalid outcome combination");
                continue;
            }
        };

        outcome_collection.push(TrackOutcome {
            // If we get to this point, the unwrap should not be used anymore, since we know by
            // now that the timestamp can be parsed, but just incase we fallback to UTC current
            // `DateTime`.
            timestamp: timestamp.as_datetime().unwrap_or_else(Utc::now),
            scoping: client_reports.scoping(),
            outcome,
            event_id: None,
            remote_addr: None, // omitting the client address allows for better aggregation
            category,
            quantity,
        });
    }

    outcome_collection
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

//  FIXME: Move the test over and adapt them.
