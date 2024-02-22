//! Contains the sessions related processor code.

use std::error::Error;
use std::net;

use chrono::{DateTime, Duration as SignedDuration, Utc};
use relay_config::Config;
use relay_dynamic_config::SessionMetricsConfig;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::protocol::{
    IpAddr, SessionAggregates, SessionAttributes, SessionStatus, SessionUpdate,
};
use relay_metrics::Bucket;
use relay_statsd::metric;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::processor::state::EnforceQuotasState;
use crate::services::processor::{ProcessEnvelopeState, SessionGroup, MINIMUM_CLOCK_DRIFT};
use crate::statsd::RelayTimers;
use crate::utils::ItemAction;

/// Validates all sessions and session aggregates in the envelope, if any.
///
/// Both are removed from the envelope if they contain invalid JSON or if their timestamps
/// are out of range after clock drift correction.
pub fn process<'a>(
    mut state: ProcessEnvelopeState<'a, SessionGroup>,
    config: &'_ Config,
) -> EnforceQuotasState<'a, SessionGroup> {
    let received = state.managed_envelope.received_at();
    let extracted_metrics = &mut state.extracted_metrics.project_metrics;
    let metrics_config = state.project_state.config().session_metrics;
    let envelope = state.managed_envelope.envelope_mut();
    let client = envelope.meta().client().map(|x| x.to_owned());
    let client_addr = envelope.meta().client_addr();

    let clock_drift_processor =
        ClockDriftProcessor::new(envelope.sent_at(), received).at_least(MINIMUM_CLOCK_DRIFT);

    state.managed_envelope.retain_items(|item| {
        let should_keep = match item.ty() {
            ItemType::Session => process_session(
                item,
                config,
                received,
                client.as_deref(),
                client_addr,
                metrics_config,
                &clock_drift_processor,
                extracted_metrics,
            ),
            ItemType::Sessions => process_session_aggregates(
                item,
                config,
                received,
                client.as_deref(),
                client_addr,
                metrics_config,
                &clock_drift_processor,
                extracted_metrics,
            ),
            _ => true, // Keep all other item types
        };
        if should_keep {
            ItemAction::Keep
        } else {
            ItemAction::DropSilently // sessions never log outcomes.
        }
    });

    EnforceQuotasState::new(state)
}

/// Returns Ok(true) if attributes were modified.
/// Returns Err if the session should be dropped.
fn validate_attributes(
    client_addr: &Option<net::IpAddr>,
    attributes: &mut SessionAttributes,
) -> Result<bool, ()> {
    let mut changed = false;

    let release = &attributes.release;
    if let Err(e) = relay_event_normalization::validate_release(release) {
        relay_log::trace!(
            error = &e as &dyn Error,
            release,
            "skipping session with invalid release"
        );
        return Err(());
    }

    if let Some(ref env) = attributes.environment {
        if let Err(e) = relay_event_normalization::validate_environment(env) {
            relay_log::trace!(
                error = &e as &dyn Error,
                env,
                "removing invalid environment"
            );
            attributes.environment = None;
            changed = true;
        }
    }

    if let Some(ref ip_address) = attributes.ip_address {
        if ip_address.is_auto() {
            attributes.ip_address = client_addr.map(IpAddr::from);
            changed = true;
        }
    }

    Ok(changed)
}

fn is_valid_session_timestamp(
    received: DateTime<Utc>,
    timestamp: DateTime<Utc>,
    max_secs_in_future: i64,
    max_session_secs_in_past: i64,
) -> bool {
    let max_age = SignedDuration::seconds(max_session_secs_in_past);
    if (received - timestamp) > max_age {
        relay_log::trace!("skipping session older than {} days", max_age.num_days());
        return false;
    }

    let max_future = SignedDuration::seconds(max_secs_in_future);
    if (timestamp - received) > max_future {
        relay_log::trace!(
            "skipping session more than {}s in the future",
            max_future.num_seconds()
        );
        return false;
    }

    true
}

/// Returns true if the item should be kept.
#[allow(clippy::too_many_arguments)]
fn process_session(
    item: &mut Item,
    config: &Config,
    received: DateTime<Utc>,
    client: Option<&str>,
    client_addr: Option<net::IpAddr>,
    metrics_config: SessionMetricsConfig,
    clock_drift_processor: &ClockDriftProcessor,
    extracted_metrics: &mut Vec<Bucket>,
) -> bool {
    let mut changed = false;
    let payload = item.payload();
    let max_secs_in_future = config.max_secs_in_future();
    let max_session_secs_in_past = config.max_session_secs_in_past();

    // sessionupdate::parse is already tested
    let mut session = match SessionUpdate::parse(&payload) {
        Ok(session) => session,
        Err(error) => {
            relay_log::trace!(
                error = &error as &dyn Error,
                "skipping invalid session payload"
            );
            return false;
        }
    };

    if session.sequence == u64::MAX {
        relay_log::trace!("skipping session due to sequence overflow");
        return false;
    };

    if clock_drift_processor.is_drifted() {
        relay_log::trace!("applying clock drift correction to session");
        clock_drift_processor.process_datetime(&mut session.started);
        clock_drift_processor.process_datetime(&mut session.timestamp);
        changed = true;
    }

    if session.timestamp < session.started {
        relay_log::trace!("fixing session timestamp to {}", session.timestamp);
        session.timestamp = session.started;
        changed = true;
    }

    // Log the timestamp delay for all sessions after clock drift correction.
    let session_delay = received - session.timestamp;
    if session_delay > SignedDuration::minutes(1) {
        metric!(
            timer(RelayTimers::TimestampDelay) = session_delay.to_std().unwrap(),
            category = "session",
        );
    }

    // Validate timestamps
    for t in [session.timestamp, session.started] {
        if !is_valid_session_timestamp(received, t, max_secs_in_future, max_session_secs_in_past) {
            return false;
        }
    }

    // Validate attributes
    match validate_attributes(&client_addr, &mut session.attributes) {
        Err(_) => return false,
        Ok(changed_attributes) => {
            changed |= changed_attributes;
        }
    }

    if config.processing_enabled() && matches!(session.status, SessionStatus::Unknown(_)) {
        return false;
    }

    // Extract metrics if they haven't been extracted by a prior Relay
    if metrics_config.is_enabled()
        && !item.metrics_extracted()
        && !matches!(session.status, SessionStatus::Unknown(_))
    {
        crate::metrics_extraction::sessions::extract_session_metrics(
            &session.attributes,
            &session,
            client,
            extracted_metrics,
            metrics_config.should_extract_abnormal_mechanism(),
        );
        item.set_metrics_extracted(true);
    }

    // Drop the session if metrics have been extracted in this or a prior Relay
    if metrics_config.should_drop() && item.metrics_extracted() {
        return false;
    }

    if changed {
        let json_string = match serde_json::to_string(&session) {
            Ok(json) => json,
            Err(err) => {
                relay_log::error!(error = &err as &dyn Error, "failed to serialize session");
                return false;
            }
        };

        item.set_payload(ContentType::Json, json_string);
    }

    true
}

#[allow(clippy::too_many_arguments)]
fn process_session_aggregates(
    item: &mut Item,
    config: &Config,
    received: DateTime<Utc>,
    client: Option<&str>,
    client_addr: Option<net::IpAddr>,
    metrics_config: SessionMetricsConfig,
    clock_drift_processor: &ClockDriftProcessor,
    extracted_metrics: &mut Vec<Bucket>,
) -> bool {
    let mut changed = false;
    let payload = item.payload();
    let max_secs_in_future = config.max_secs_in_future();
    let max_session_secs_in_past = config.max_session_secs_in_past();

    let mut session = match SessionAggregates::parse(&payload) {
        Ok(session) => session,
        Err(error) => {
            relay_log::trace!(
                error = &error as &dyn Error,
                "skipping invalid sessions payload"
            );
            return false;
        }
    };

    if clock_drift_processor.is_drifted() {
        relay_log::trace!("applying clock drift correction to session");
        for aggregate in &mut session.aggregates {
            clock_drift_processor.process_datetime(&mut aggregate.started);
        }
        changed = true;
    }

    // Validate timestamps
    session.aggregates.retain(|aggregate| {
        is_valid_session_timestamp(
            received,
            aggregate.started,
            max_secs_in_future,
            max_session_secs_in_past,
        )
    });

    // Aftter timestamp validation, aggregates could now be empty
    if session.aggregates.is_empty() {
        return false;
    }

    // Validate attributes
    match validate_attributes(&client_addr, &mut session.attributes) {
        Err(_) => return false,
        Ok(changed_attributes) => {
            changed |= changed_attributes;
        }
    }

    // Extract metrics if they haven't been extracted by a prior Relay
    if metrics_config.is_enabled() && !item.metrics_extracted() {
        for aggregate in &session.aggregates {
            crate::metrics_extraction::sessions::extract_session_metrics(
                &session.attributes,
                aggregate,
                client,
                extracted_metrics,
                metrics_config.should_extract_abnormal_mechanism(),
            );
            item.set_metrics_extracted(true);
        }
    }

    // Drop the aggregate if metrics have been extracted in this or a prior Relay
    if metrics_config.should_drop() && item.metrics_extracted() {
        return false;
    }

    if changed {
        let json_string = match serde_json::to_string(&session) {
            Ok(json) => json,
            Err(err) => {
                relay_log::error!(error = &err as &dyn Error, "failed to serialize session");
                return false;
            }
        };

        item.set_payload(ContentType::Json, json_string);
    }

    true
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    struct TestProcessSessionArguments<'a> {
        item: Item,
        received: DateTime<Utc>,
        client: Option<&'a str>,
        client_addr: Option<net::IpAddr>,
        metrics_config: SessionMetricsConfig,
        clock_drift_processor: ClockDriftProcessor,
        extracted_metrics: Vec<Bucket>,
    }

    impl<'a> TestProcessSessionArguments<'a> {
        fn run_session_producer(&mut self) -> bool {
            process_session(
                &mut self.item,
                &Config::default(),
                self.received,
                self.client,
                self.client_addr,
                self.metrics_config,
                &self.clock_drift_processor,
                &mut self.extracted_metrics,
            )
        }

        fn default() -> Self {
            let mut item = Item::new(ItemType::Event);

            let session = r#"{
            "init": false,
            "started": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "attrs": {
                "release": "1.0.0"
            },
            "did": "user123",
            "status": "this is not a valid status!",
            "duration": 123.4
        }"#;

            item.set_payload(ContentType::Json, session);
            let received = DateTime::from_str("2021-04-26T08:00:00+0100").unwrap();

            Self {
                item,
                received,
                client: None,
                client_addr: None,
                metrics_config: serde_json::from_str(
                    "
        {
            \"version\": 0,
            \"drop\": true
        }",
                )
                .unwrap(),
                clock_drift_processor: ClockDriftProcessor::new(None, received),
                extracted_metrics: vec![],
            }
        }
    }

    /// Checks that the default test-arguments leads to the item being kept, which helps ensure the
    /// other tests are valid.
    #[test]
    fn test_process_session_keep_item() {
        let mut args = TestProcessSessionArguments::default();
        assert!(args.run_session_producer());
    }

    #[test]
    fn test_process_session_invalid_json() {
        let mut args = TestProcessSessionArguments::default();
        args.item
            .set_payload(ContentType::Json, "this isnt valid json");
        assert!(!args.run_session_producer());
    }

    #[test]
    fn test_process_session_sequence_overflow() {
        let mut args = TestProcessSessionArguments::default();
        args.item.set_payload(
            ContentType::Json,
            r#"{
            "init": false,
            "started": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "seq": 18446744073709551615,
            "attrs": {
                "release": "1.0.0"
            },
            "did": "user123",
            "status": "this is not a valid status!",
            "duration": 123.4
        }"#,
        );
        assert!(!args.run_session_producer());
    }

    #[test]
    fn test_process_session_invalid_timestamp() {
        let mut args = TestProcessSessionArguments::default();
        args.received = DateTime::from_str("2021-05-26T08:00:00+0100").unwrap();
        assert!(!args.run_session_producer());
    }

    #[test]
    fn test_process_session_metrics_extracted() {
        let mut args = TestProcessSessionArguments::default();
        args.item.set_metrics_extracted(true);
        assert!(!args.run_session_producer());
    }
}
