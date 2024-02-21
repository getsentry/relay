use relay_sampling::config::RuleType;
use relay_test::{mini_sentry::MiniSentry, relay::Relay, Envelope, ProjectState};

// Tests that it tags an incoming error if the trace connected to it its sampled or not.
#[test]
fn test_it_tags_error() {
    for (sample_rate, expected_sampled) in [(1.0, true), (0.0, false)] {
        // add a sampling rule to project config that keeps all events (sample_rate=1)
        let project_state =
            ProjectState::new().add_basic_sampling_rule(RuleType::Trace, sample_rate);
        let public_key = project_state.public_key();

        let sentry = MiniSentry::new().add_project_state(project_state);

        // create an envelope with a trace context that is initiated by this project (for simplicity)
        let envelope = Envelope::new().add_error_event_with_trace_info(public_key);
        let relay = Relay::builder(&sentry).enable_outcomes().build();
        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(3)
            .assert_envelope_qty(1)
            .assert_all_sampled_status(expected_sampled);
    }
}
