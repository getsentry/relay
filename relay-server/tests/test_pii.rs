use relay_event_normalization::{NormalizationConfig, normalize_event};
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Event;
use relay_pii::{DataScrubbingConfig, PiiProcessor};
use relay_protocol::{FromValue, assert_annotated_snapshot};

#[test]
fn test_reponse_context_pii() {
    let mut data = Event::from_value(
        serde_json::json!({
            "event_id": "7b9e89cf79ee451986112e0425fa9fd4",
            "contexts": {
                "response": {
                    "type": "response",
                    "headers": {
                        "Authorization": "Basic 1122334455",
                        "Set-Cookie": "token=a3fWa; Expires=Wed, 21 Oct 2015 07:28:00 GMT",
                        "Proxy-Authorization": "11234567",
                    },
                    "status_code": 200
                }
            }
        })
        .into(),
    );

    // Run all normalization steps.
    let normalization_config = NormalizationConfig::default();
    normalize_event(&mut data, &normalization_config);

    let mut ds_config = DataScrubbingConfig::default();
    ds_config.scrub_data = true;
    ds_config.scrub_defaults = true;
    ds_config.scrub_ip_addresses = true;

    // And also run the PII processort to check if the sensitive data is scrubbed.
    let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
    let mut pii_processor = PiiProcessor::new(pii_config.compiled());
    processor::process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(data);
}
