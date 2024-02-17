#[cfg(test)]
mod tests {

    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_event_schema::protocol::EventId;
    use relay_sampling::config::RuleType;
    use relay_server::actors::outcome::OutcomeId;
    use relay_server::envelope::ItemType;
    use serde_json::json;
    use uuid::Uuid;

    use crate::mini_sentry::{self, MiniSentry};
    use crate::relay::{Relay, RelayBuilder};
    use crate::test_envelopy::{create_error_item, RawEnvelope, RawItem};
    use crate::{
        create_error_envelope, create_transaction_envelope, create_transaction_item,
        event_id_from_item, is_envelope_sampled, new_sampling_rule, opt_create_transaction_item,
        outcomes_enabled_config, x_create_transaction_envelope, x_create_transaction_item,
        EnvelopeBuilder, StateBuilder,
    };

    #[test]
    fn test_uses_trace_public_key() {
        // create basic project configs
        let project_id1 = ProjectId(42);
        let config1 = StateBuilder::new()
            .set_project_id(project_id1)
            .set_transaction_metrics_version(1)
            .set_sampling_rule(0.0, RuleType::Trace);
        let public_key1 = config1.public_key();

        let project_id2 = ProjectId(43);
        let config2 = StateBuilder::new()
            .set_project_id(project_id1)
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
        let envelope = RawEnvelope::new(sentry.dsn_public_key())
            .add_raw_item(transaction)
            .add_trace_info_simple(trace_id, public_key1)
            .set_project_id(project_id2);

        // Send the event, the transaction should be removed.
        relay.send_envelope(envelope);
        // The event should be removed by Relay sampling.
        sentry.captured_envelopes().wait(2).assert_empty();

        // and it should create an outcome
        sentry.captured_outcomes().assert_outcome_qty(1).clear();

        // Second
        // send trace with project_id2 context (shoudl go through)
        let (transaction, trace_id, _) = x_create_transaction_item(None);
        let envelope = RawEnvelope::new(sentry.dsn_public_key())
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

    /// Tests that when sampling is set to 0% for the trace context project the events are removed.
    #[test]
    fn test_it_removes_events() {
        let sample_rate = 0.0;
        let project_state = StateBuilder::new()
            .add_basic_sampling_rule(RuleType::Transaction, sample_rate)
            .set_transaction_metrics_version(1);

        let public_key = project_state.public_key();
        let sentry = MiniSentry::new().add_project_state(project_state);

        let relay = Relay::builder(&sentry).enable_outcomes().build();

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
            .add_transaction_and_trace_info(public_key, None);

        relay.send_envelope(envelope);

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
        let rule = new_sampling_rule(0.0, RuleType::Trace.into(), vec![1.0], None, None);
        let project_state = StateBuilder::new()
            .add_sampling_rule(rule)
            .set_transaction_metrics_version(1);
        let public_key = project_state.public_key();

        let sentry = MiniSentry::new().add_project_state(project_state);
        let relay = Relay::builder(&sentry).enable_outcomes().build();

        let (item, trace_id, event_id) = create_error_item(public_key);

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
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

        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(5)
            .assert_item_qty(1)
            .assert_contains_event_id(EventId(event_id));
    }

    #[test]
    fn test_it_tags_error() {
        for (sample_rate, expected_sampled) in [(1.0, true), (0.0, false)] {
            let project_state =
                StateBuilder::new().add_basic_sampling_rule(RuleType::Trace, sample_rate);
            let public_key = project_state.public_key();

            let sentry = MiniSentry::new().add_project_state(project_state);

            let envelope = RawEnvelope::new(sentry.dsn_public_key())
                .add_error_event_with_trace_info(public_key);
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

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
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

    #[test]
    fn test_multi_item_envelope() {
        for rule_type in [RuleType::Transaction, RuleType::Trace] {
            let project_id = ProjectId(42);
            let project_state = StateBuilder::new()
                .enable_outcomes()
                .set_project_id(project_id)
                .set_transaction_metrics_version(1)
                .add_basic_sampling_rule(rule_type, 0.0);
            let public_key = project_state.public_key();
            let sentry = MiniSentry::new().add_project_state(project_state.clone());
            let relay = Relay::builder(&sentry).enable_outcomes().build();

            for _ in 0..2 {
                let envelope = RawEnvelope::new(sentry.dsn_public_key())
                    .add_transaction_and_trace_info(public_key, None)
                    .add_item_from_json(json!({"x": "some attachment"}), ItemType::Attachment)
                    .add_item_from_json(
                        json!({"y": "some other attachment"}),
                        ItemType::Attachment,
                    );

                relay.send_envelope(envelope);

                sentry.captured_envelopes().wait(2).assert_empty().clear();

                sentry.captured_outcomes().wait_for_outcome(2).clear();
            }
        }
    }

    #[test]
    fn test_client_sample_rate_adjusted() {
        let sample_rate = 0.001;

        for rule_type in [RuleType::Trace, RuleType::Transaction] {
            let mut sentry = MiniSentry::new();
            let relay = Relay::builder(&sentry).build();
            let project_state = StateBuilder::new()
                .set_transaction_metrics_version(1)
                .add_basic_sampling_rule(rule_type, sample_rate);
            let public_key = project_state.public_key();
            sentry = sentry.add_project_state(project_state);

            let envelope = RawEnvelope::new(sentry.dsn_public_key())
                .add_transaction_and_trace_info_not_simple(public_key, None, Some(sample_rate));

            relay.send_envelope(envelope);

            sentry
                .captured_envelopes()
                .wait_for_envelope(5)
                .debug()
                .assert_item_qty(1)
                .assert_n_item_types(ItemType::ClientReport, 1)
                .clear();

            let envelope = RawEnvelope::new(sentry.dsn_public_key())
                .add_transaction_and_trace_info_not_simple(public_key, None, Some(1.0));

            relay.send_envelope(envelope);
            sentry
                .captured_envelopes()
                .wait(2)
                .assert_item_qty(1)
                .assert_n_item_types(ItemType::ClientReport, 1);
        }
    }

    #[test]
    fn test_relay_chain() {
        for rule_type in [RuleType::Transaction, RuleType::Trace] {
            let sample_rate = 0.001;
            let project_state = StateBuilder::new().add_basic_sampling_rule(rule_type, sample_rate);
            let sentry = MiniSentry::new().add_project_state(project_state);
            let inner_relay = Relay::new(&sentry);
            let outer_relay = Relay::new(&inner_relay);

            let magic_uuid = Uuid::parse_str("414e119d37694a32869f9d81b76a0b70").unwrap();

            let (trace_id, event_id) = match rule_type {
                RuleType::Trace => (Some(magic_uuid), None),
                RuleType::Transaction => (None, Some(magic_uuid)),
                RuleType::Unsupported => panic!(),
            };

            let (item, _, _) = opt_create_transaction_item(None, trace_id, event_id);
            let envelope = RawEnvelope::new(sentry.dsn_public_key()).add_raw_item(item);
            outer_relay.send_envelope(envelope);

            sentry
                .captured_envelopes()
                .wait_for_envelope(2)
                .assert_n_item_types(ItemType::Transaction, 1);
        }
    }
}
