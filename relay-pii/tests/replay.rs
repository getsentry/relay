use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Replay;
use relay_pii::{DataScrubbingConfig, PiiProcessor};
use relay_protocol::{get_value, Annotated};

fn simple_enabled_config() -> DataScrubbingConfig {
    let mut scrub_config = DataScrubbingConfig::default();
    scrub_config.scrub_data = true;
    scrub_config.scrub_defaults = true;
    scrub_config.scrub_ip_addresses = true;
    scrub_config
}

#[test]
fn test_scrub_pii_from_annotated_replay() {
    let scrub_config = simple_enabled_config();
    let pii_config = scrub_config.pii_config().unwrap().as_ref().unwrap();
    let mut pii_processor = PiiProcessor::new(pii_config.compiled());

    let payload = include_str!("../../tests/fixtures/replay.json");
    let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
    processor::process_value(&mut replay, &mut pii_processor, ProcessingState::root()).unwrap();

    // Ip-address was removed.
    assert_eq!(get_value!(replay.user.ip_address), None);

    let replay_value = replay.value().unwrap();
    let credit_card = replay_value.tags.value().unwrap().get("credit-card");
    assert_eq!(credit_card, Some("[Filtered]"));

    // Assert URLs field scrubs array items.
    let urls = replay_value.urls.value().unwrap();
    assert_eq!(
        urls.first().and_then(|a| a.as_str()),
        Some("sentry.io?ssn=[Filtered]")
    );
    assert_eq!(urls.get(1).and_then(|a| a.as_str()), Some("[Filtered]"));
    assert_eq!(urls.get(2).and_then(|a| a.as_str()), Some("[Filtered]"));
}
