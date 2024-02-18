use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::EventId;
use relay_sampling::config::RuleType;
use relay_server::envelope::ItemType;
use relay_server::services::outcome::OutcomeId;
use relay_test::mini_sentry::MiniSentry;
use relay_test::relay::Relay;
use serde_json::json;
use uuid::Uuid;

use relay_test::{
    create_error_item, new_sampling_rule, opt_create_transaction_item, x_create_transaction_item,
    RawEnvelope, StateBuilder,
};

/// Tests that when sampling is set to 0% for the trace context project the events are removed.
#[test]
fn test_it_removes_events() {
    let sample_rate = 0.0;
    let project_state = StateBuilder::new()
        // add a sampling rule to project config that removes all transactions (sample_rate=0)
        .add_basic_sampling_rule(RuleType::Transaction, sample_rate)
        .set_transaction_metrics_version(1);

    let public_key = project_state.public_key();
    let sentry = MiniSentry::new().add_project_state(project_state);
    let relay = Relay::builder(&sentry).enable_outcomes().build();

    // create an envelope with a trace context that is initiated by this project (for simplicity)
    let envelope = RawEnvelope::new().add_transaction_and_trace_info(public_key, None);

    // send the event, the transaction should be removed.
    relay.send_envelope(envelope);

    // the event should be removed by Relay sampling
    sentry.captured_envelopes().wait(2).assert_empty();

    sentry
        .captured_outcomes()
        .wait_for_outcome(30)
        .assert_outcome_qty(1)
        .assert_all_outcome_id(OutcomeId::FILTERED)
        .assert_all_outcome_reasons("Sampled:1");
}

///Tests that we keep an event if it is of type error.
#[test]
fn test_it_does_not_sample_error() {
    let project_state = StateBuilder::new()
        // add a sampling rule to project config that removes all traces of release "1.0"
        .add_sampling_rule(new_sampling_rule(
            0.0,
            RuleType::Trace.into(),
            vec![1.0],
            None,
            None,
        ))
        .set_transaction_metrics_version(1);
    let public_key = project_state.public_key();

    let sentry = MiniSentry::new().add_project_state(project_state);
    let relay = Relay::builder(&sentry).enable_outcomes().build();

    let (item, trace_id, event_id) = create_error_item();

    // create an envelope with a trace context that is initiated by this project (for simplicity)
    let envelope = RawEnvelope::new()
        .add_raw_item(item)
        .add_header("event_id", event_id.to_string().as_str())
        .add_trace_info(
            trace_id,
            public_key,
            Some(1.0),
            Some(true),
            Some(1.0),
            Some("/transaction"),
        );

    // send the event, the transaction should be removed.
    relay.send_envelope(envelope);

    // test that error is kept by Relay
    sentry
        .captured_envelopes()
        .wait_for_envelope(5)
        .assert_item_qty(1)
        // double check that we get back our object
        .assert_contains_event_id(EventId(event_id));
}

// Tests that it tags an incoming error if the trace connected to it its sampled or not.
#[test]
fn test_it_tags_error() {
    for (sample_rate, expected_sampled) in [(1.0, true), (0.0, false)] {
        // add a sampling rule to project config that keeps all events (sample_rate=1)
        let project_state =
            StateBuilder::new().add_basic_sampling_rule(RuleType::Trace, sample_rate);
        let public_key = project_state.public_key();

        let sentry = MiniSentry::new().add_project_state(project_state);

        // create an envelope with a trace context that is initiated by this project (for simplicity)
        let envelope = RawEnvelope::new().add_error_event_with_trace_info(public_key);
        let relay = Relay::builder(&sentry).enable_outcomes().build();
        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(3)
            .assert_envelope_qty(1)
            .assert_all_sampled_status(expected_sampled);
    }
}

///Tests that when sampling is set to 100% for the trace context project the events are kept
#[test]
fn test_it_keeps_event() {
    let rule = new_sampling_rule(1.0, RuleType::Transaction.into(), vec![1.0], None, None);

    let project_state = StateBuilder::new().add_sampling_rule(rule);
    let public_key = project_state.public_key();

    let sentry = MiniSentry::new().add_project_state(project_state);

    let relay = Relay::builder(&sentry).enable_outcomes().build();

    let (item, trace_id, event_id) = x_create_transaction_item(None);

    let envelope = RawEnvelope::new()
        .add_raw_item(item)
        .set_event_id(event_id)
        .add_trace_info_simple(trace_id, public_key);

    relay.send_envelope(envelope);

    sentry
        .captured_envelopes()
        .wait_for_envelope(3)
        .assert_item_qty(1)
        .assert_contains_event_id(EventId(event_id));
}

/// Tests that the `public_key` from the trace context is used.
///
/// The project configuration corresponding to the project pointed to
/// by the context `public_key` DSN is used (not the DSN of the request).
///
/// # Scenario
///
/// - Create a trace context for `projectA` and send an event from `projectB`
///   using `projectA`'s trace.
/// - Configure `project1` to sample out all events (`sample_rate=0`).
/// - Configure `project2` to sample in all events (`sample_rate=1`).
///
/// # Steps
///
/// 1. Send an event to `project2` with a trace from `project1`.
///    - It should be removed (sampled out).
/// 2. Send an event to `project1` with a trace from `project2`.
///    - It should pass through.
///
/// This test ensures that the sampling decision respects the trace context's
/// `public_key` rather than the request's `public_key`.
#[test]
fn test_uses_trace_public_key() {
    // create basic project configs
    let project_id1 = ProjectId::new(42);
    let config1 = StateBuilder::new()
        .set_project_id(project_id1)
        .set_transaction_metrics_version(1)
        .set_sampling_rule(0.0, RuleType::Trace);
    let public_key1 = config1.public_key();

    let project_id2 = ProjectId::new(43);
    let config2 = StateBuilder::new()
        .set_project_id(project_id2)
        .set_transaction_metrics_version(1)
        .set_sampling_rule(1.0, RuleType::Trace);
    let public_key2 = config2.public_key();

    let sentry = MiniSentry::new()
        .add_project_state(config1)
        .add_project_state(config2);
    let relay = Relay::builder(&sentry).enable_outcomes().build();

    // First
    // send trace with project_id1 context (should be removed)
    let (transaction, trace_id, _) = x_create_transaction_item(None);
    let envelope = RawEnvelope::new()
        .add_raw_item(transaction)
        .add_trace_info_simple(trace_id, public_key1)
        .set_project_id(project_id2);

    // Send the event, the transaction should be removed.
    relay.send_envelope(envelope);
    // The event should be removed by Relay sampling.
    sentry.captured_envelopes().wait(1).assert_empty();

    // and it should create an outcome
    sentry.captured_outcomes().assert_outcome_qty(1).clear();

    // Second
    // send trace with project_id2 context (shoudl go through)
    let (transaction, trace_id, _) = x_create_transaction_item(None);
    let envelope = RawEnvelope::new()
        .add_raw_item(transaction)
        .add_trace_info_simple(trace_id, public_key2)
        .set_project_id(project_id1);

    // send the event
    relay.send_envelope(envelope);

    // the event should be passed along to usptream (with the transaction unchanged)
    sentry
        .captured_envelopes()
        .wait_for_envelope(10)
        .assert_item_qty(1)
        .assert_all_item_types(ItemType::Transaction);

    // no outcome should be generated (since the event is passed along to the upstream)
    sentry.captured_outcomes().assert_empty();
}

/// Associated items are removed together with event item.
///
/// The event is sent twice to account for both fast and slow paths.
///
/// When sampling decides to remove a transaction it should also remove all
/// dependent items (attachments).
#[test]
fn test_multi_item_envelope() {
    for rule_type in [RuleType::Transaction, RuleType::Trace] {
        let project_id = ProjectId::new(42);
        let project_state = StateBuilder::new()
            .enable_outcomes()
            .set_project_id(project_id)
            .set_transaction_metrics_version(1)
            .add_basic_sampling_rule(rule_type, 0.0);
        let public_key = project_state.public_key();
        let sentry = MiniSentry::new().add_project_state(project_state.clone());
        let relay = Relay::builder(&sentry).enable_outcomes().build();

        for _ in 0..2 {
            let envelope = RawEnvelope::new()
                .add_transaction_and_trace_info(public_key, None)
                .add_item_from_json(json!({"x": "some attachment"}), ItemType::Attachment)
                .add_item_from_json(json!({"y": "some other attachment"}), ItemType::Attachment);

            relay.send_envelope(envelope);

            sentry.captured_envelopes().wait(1).assert_empty().clear();

            sentry.captured_outcomes().wait_for_outcome(2).clear();
        }
    }
}

/// Tests that the client sample rate is honored when applying server-side
/// sampling. Do so by sending an envelope with a very low reported client sample rate
/// and a sampling rule with the same sample rate. The server should adjust
/// itself to 1.0. The chances of this test passing without the adjustment in
/// place are very low (but not 0).
#[test]
fn test_client_sample_rate_adjusted() {
    let sample_rate = 0.001;

    for rule_type in [RuleType::Trace, RuleType::Transaction] {
        let project_state = StateBuilder::new()
            .set_transaction_metrics_version(1)
            .add_basic_sampling_rule(rule_type, sample_rate);
        let public_key = project_state.public_key();
        let sentry = MiniSentry::new().add_project_state(project_state);
        let relay = Relay::builder(&sentry).build();

        let envelope = RawEnvelope::new().add_transaction_and_trace_info_not_simple(
            public_key,
            None,
            Some(sample_rate),
        );

        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(5)
            .debug()
            .assert_item_qty(1)
            .assert_n_item_types(ItemType::ClientReport, 1)
            .clear();

        let envelope = RawEnvelope::new().add_transaction_and_trace_info_not_simple(
            public_key,
            None,
            Some(1.0),
        );

        relay.send_envelope(envelope);
        sentry
            .captured_envelopes()
            .wait(1)
            .assert_item_qty(1)
            .assert_n_item_types(ItemType::ClientReport, 1);
    }
}

///  Tests that nested relays do not end up double-sampling. This is guaranteed
///  by the fact that we never actually use an RNG, but hash either the event-
///  or trace-id.
#[test]
fn test_relay_chain() {
    for rule_type in [RuleType::Transaction, RuleType::Trace] {
        let sample_rate = 0.001;
        let project_state = StateBuilder::new().add_basic_sampling_rule(rule_type, sample_rate);
        let sentry = MiniSentry::new().add_project_state(project_state);
        let inner_relay = Relay::new(&sentry);
        let outer_relay = Relay::new(&inner_relay);

        // A trace ID that gets hashed to a value lower than 0.001
        let magic_uuid = Uuid::parse_str("414e119d37694a32869f9d81b76a0b70").unwrap();

        let (trace_id, event_id) = match rule_type {
            RuleType::Trace => (Some(magic_uuid), None),
            RuleType::Transaction => (None, Some(magic_uuid)),
            RuleType::Unsupported => panic!(),
        };

        let (item, _, _) = opt_create_transaction_item(None, trace_id, event_id);
        let envelope = RawEnvelope::new().add_raw_item(item);
        outer_relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(2)
            .assert_n_item_types(ItemType::Transaction, 1);
    }
}
