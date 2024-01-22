//! Contains code related to validation and normalization of the user and client reports.

use std::collections::BTreeMap;
use std::error::Error;

use chrono::{Duration as SignedDuration, Utc};
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::protocol::{ClientReport, UserReport};
use relay_filter::FilterStatKey;
use relay_quotas::ReasonCode;
use relay_sampling::evaluation::MatchedRuleIds;
use relay_system::Addr;

use crate::envelope::{ContentType, ItemType};
use crate::services::outcome::{Outcome, TrackOutcome};
use crate::services::processor::{ProcessEnvelopeState, MINIMUM_CLOCK_DRIFT};
use crate::utils::ItemAction;

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

/// Validates and normalizes:
/// - all user reports items in the envelopes
/// - all client reports
///
/// User feedback items are removed from the envelope if they contain invalid JSON or if the
/// JSON violates the schema (basic type validation). Otherwise, their normalized representation
/// is written back into the item.
///
/// At the moment client reports are primarily used to transfer outcomes from
/// client SDKs.  The outcomes are removed here and sent directly to the outcomes
/// system.
pub fn process(
    state: &mut ProcessEnvelopeState,
    config: &Config,
    outcome_aggregator: Addr<TrackOutcome>,
) {
    process_client_reports(state, config, outcome_aggregator);
    process_user_reports(state);
}

/// Validates and normalizes all user report items in the envelope.
///
/// User feedback items are removed from the envelope if they contain invalid JSON or if the
/// JSON violates the schema (basic type validation). Otherwise, their normalized representation
/// is written back into the item.
fn process_user_reports(state: &mut ProcessEnvelopeState) {
    state.managed_envelope.retain_items(|item| {
        if item.ty() != &ItemType::UserReport {
            return ItemAction::Keep;
        };

        let payload = item.payload();
        // There is a customer SDK which sends invalid reports with a trailing `\n`,
        // strip it here, even if they update/fix their SDK there will still be many old
        // versions with the broken SDK out there.
        let payload = trim_whitespaces(&payload);
        let report = match serde_json::from_slice::<UserReport>(payload) {
            Ok(session) => session,
            Err(error) => {
                relay_log::error!(error = &error as &dyn Error, "failed to store user report");
                return ItemAction::DropSilently;
            }
        };

        let json_string = match serde_json::to_string(&report) {
            Ok(json) => json,
            Err(err) => {
                relay_log::error!(
                    error = &err as &dyn Error,
                    "failed to serialize user report"
                );
                return ItemAction::DropSilently;
            }
        };

        item.set_payload(ContentType::Json, json_string);
        ItemAction::Keep
    });
}

fn trim_whitespaces(data: &[u8]) -> &[u8] {
    let Some(from) = data.iter().position(|x| !x.is_ascii_whitespace()) else {
        return &[];
    };
    let Some(to) = data.iter().rposition(|x| !x.is_ascii_whitespace()) else {
        return &[];
    };
    &data[from..to + 1]
}

/// Parse an outcome from an outcome ID and a reason string.
///
/// Currently only used to reconstruct outcomes encoded in client reports.
fn outcome_from_parts(field: ClientReportField, reason: &str) -> Result<Outcome, ()> {
    match field {
        ClientReportField::FilteredSampling => match reason.strip_prefix("Sampled:") {
            Some(rule_ids) => MatchedRuleIds::parse(rule_ids)
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

/// Validates and extracts client reports.
///
/// At the moment client reports are primarily used to transfer outcomes from
/// client SDKs.  The outcomes are removed here and sent directly to the outcomes
/// system.
fn process_client_reports(
    state: &mut ProcessEnvelopeState,
    config: &Config,
    outcome_aggregator: Addr<TrackOutcome>,
) {
    // if client outcomes are disabled we leave the the client reports unprocessed
    // and pass them on.
    if !config.emit_outcomes().any() || !config.emit_client_outcomes() {
        // if a processing relay has client outcomes disabled we drop them.
        if config.processing_enabled() {
            state.managed_envelope.retain_items(|item| match item.ty() {
                ItemType::ClientReport => ItemAction::DropSilently,
                _ => ItemAction::Keep,
            });
        }
        return;
    }

    let mut timestamp = None;
    let mut output_events = BTreeMap::new();
    let received = state.managed_envelope.received_at();

    let clock_drift_processor = ClockDriftProcessor::new(state.envelope().sent_at(), received)
        .at_least(MINIMUM_CLOCK_DRIFT);

    // we're going through all client reports but we're effectively just merging
    // them into the first one.
    state.managed_envelope.retain_items(|item| {
        if item.ty() != &ItemType::ClientReport {
            return ItemAction::Keep;
        };
        match ClientReport::parse(&item.payload()) {
            Ok(ClientReport {
                timestamp: report_timestamp,
                discarded_events,
                rate_limited_events,
                filtered_events,
                filtered_sampling_events,
            }) => {
                // Glue all discarded events together and give them the appropriate outcome type
                let input_events =
                    discarded_events
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
        ItemAction::DropSilently
    });

    if output_events.is_empty() {
        return;
    }

    let timestamp =
        timestamp.get_or_insert_with(|| UnixTimestamp::from_secs(received.timestamp() as u64));

    if clock_drift_processor.is_drifted() {
        relay_log::trace!("applying clock drift correction to client report");
        clock_drift_processor.process_timestamp(timestamp);
    }

    let max_age = SignedDuration::seconds(config.max_secs_in_past());
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
        return;
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
        return;
    }

    for ((outcome_type, reason, category), quantity) in output_events.into_iter() {
        let outcome = match outcome_from_parts(outcome_type, &reason) {
            Ok(outcome) => outcome,
            Err(_) => {
                relay_log::trace!(?outcome_type, reason, "invalid outcome combination");
                continue;
            }
        };

        outcome_aggregator.send(TrackOutcome {
            // If we get to this point, the unwrap should not be used anymore, since we know by
            // now that the timestamp can be parsed, but just incase we fallback to UTC current
            // `DateTime`.
            timestamp: timestamp.as_datetime().unwrap_or_else(Utc::now),
            scoping: state.managed_envelope.scoping(),
            outcome,
            event_id: None,
            remote_addr: None, // omitting the client address allows for better aggregation
            category,
            quantity,
        });
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use relay_event_schema::protocol::EventId;
    use relay_sampling::config::RuleId;
    use relay_sampling::evaluation::ReservoirCounters;

    use crate::envelope::{Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::services::processor::{ProcessEnvelope, ProcessingGroup};
    use crate::services::project::ProjectState;
    use crate::testutils::{self, create_test_processor};
    use crate::utils::ManagedEnvelope;

    use super::*;

    #[tokio::test]
    async fn test_client_report_removal() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": true,
                "emit_client_outcomes": true
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        assert!(envelope_response.envelope.is_none());
    }

    #[tokio::test]
    async fn test_client_report_forwarding() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": false,
                // a relay need to emit outcomes at all to not process.
                "emit_client_outcomes": true
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let item = ctx.envelope().items().next().unwrap();
        assert_eq!(item.ty(), &ItemType::ClientReport);

        ctx.accept(); // do not try to capture or emit outcomes
    }

    #[tokio::test]
    #[cfg(feature = "processing")]
    async fn test_client_report_removal_in_processing() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": true,
                "emit_client_outcomes": false,
            },
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        assert!(envelope_response.envelope.is_none());
    }

    #[tokio::test]
    async fn test_user_report_only() {
        relay_log::init_test!();
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = testutils::processor_services();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::UserReport);
            item.set_payload(
                ContentType::Json,
                format!(r#"{{"event_id": "{event_id}"}}"#),
            );
            item
        });

        let mut envelope = ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store);
        envelope.set_processing_group(ProcessingGroup::UserReport);

        let message = ProcessEnvelope {
            envelope,
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(
            new_envelope.items().next().unwrap().ty(),
            &ItemType::UserReport
        );
    }

    #[tokio::test]
    async fn test_user_report_invalid() {
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = testutils::processor_services();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::UserReport);
            item.set_payload(ContentType::Json, r#"{"foo": "bar"}"#);
            item
        });

        envelope.add_item({
            let mut item = Item::new(ItemType::Event);
            item.set_payload(ContentType::Json, "{}");
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(new_envelope.items().next().unwrap().ty(), &ItemType::Event);
    }

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
            Ok(Outcome::FilteredSampling(MatchedRuleIds(vec![
                RuleId(123),
                RuleId(456),
            ])))
        );

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123"),
            Ok(Outcome::FilteredSampling(MatchedRuleIds(vec![RuleId(123)])))
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

    #[test]
    fn test_trim_whitespaces() {
        assert_eq!(trim_whitespaces(b""), b"");
        assert_eq!(trim_whitespaces(b" \n\r "), b"");
        assert_eq!(trim_whitespaces(b" \nx\r "), b"x");
        assert_eq!(trim_whitespaces(b" {foo: bar} "), b"{foo: bar}");
        assert_eq!(trim_whitespaces(b"{ foo: bar}"), b"{ foo: bar}");
    }
}
