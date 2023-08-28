use std::fs;

use relay_event_normalization::{
    light_normalize_event, LightNormalizationConfig, StoreConfig, StoreProcessor,
};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::Event;
use relay_pii::{PiiConfig, PiiProcessor};
use relay_protocol::{Annotated, SerializableAnnotated};

fn pii_config() -> PiiConfig {
    serde_json::from_str(
        r#"{
          "rules": {
            "removeOkDetectToken": {
              "type": "pattern",
              "pattern": ".token.:.([0-9]+).",
              "redaction": {"method": "replace", "text": "[ok detect token]"}
            },
            "removeDotLocal": {
              "type": "pattern",
              "pattern": "(.*)[.]local",
              "redaction": {"method": "replace", "text": "[.local hostname]"}
            }
          },
          "applications": {
            "tags.server_name": ["removeDotLocal"],
            "tags.RequestId": ["@anything:remove"],

            "contexts.device.boot_time": ["@anything:remove"],
            "contexts.device.screen_resolution": ["@anything:remove"],
            "contexts.device.screen_density": ["@anything:remove"],
            "contexts.device.screen_height_pixels": ["@anything:remove"],
            "contexts.device.screen_width_pixels": ["@anything:remove"],
            "contexts.device.screen_dpi": ["@anything:remove"],
            "contexts.device.memory_size": ["@anything:remove"],
            "contexts.device.timezone": ["@anything:remove"],

            "user.ip_address": ["@anything:remove"],
            "user.email": ["@anything:hash"],

            "request.cookies.wcsid": ["@anything:replace"],
            "request.cookies.hblid": ["@anything:replace"],
            "request.cookies._okdetect": ["removeOkDetectToken"],
            "request.headers.MS-ASPNETCORE-TOKEN": ["@anything:replace"],
            "request.headers.User-Agent": ["@anything:replace"],

            "request.env.DOCUMENT_ROOT": ["@userpath:replace"],
            "exception.values.*.stacktrace.frames.*.filename": ["@userpath:replace"],
            "exception.values.*.stacktrace.frames.*.abs_path": ["@userpath:replace"],
            "breadcrumbs.values.*.message": ["@userpath:replace"]
          }
        }"#,
    )
    .unwrap()
}

macro_rules! event_snapshot {
    ($id:ident) => {
        mod $id {
            use super::*;

            fn load_fixture() -> Annotated<Event> {
                let data = fs::read_to_string(
                    format!("tests/fixtures/payloads/{}.json", stringify!($id))
                ).unwrap();
                Annotated::<Event>::from_json(&data).unwrap()
            }

            #[test]
            fn test_processing() {
                let mut event = load_fixture();

                let config = LightNormalizationConfig::default();
                light_normalize_event(&mut event, config).unwrap();

                let config = StoreConfig::default();
                let mut processor = StoreProcessor::new(config, None);
                process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

                let pii_config = pii_config();
                let compiled = pii_config.compiled();
                let mut processor = PiiProcessor::new(&compiled);

                process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
                insta::assert_yaml_snapshot!("pii_stripping", SerializableAnnotated(&event), {
                    ".received" => "[received]",
                    ".timestamp" => "[timestamp]"
                });
            }
        }
    }
}

event_snapshot!(android);
event_snapshot!(cocoa);
event_snapshot!(cordova);
event_snapshot!(dotnet);
event_snapshot!(legacy_python);
event_snapshot!(legacy_node_exception);
event_snapshot!(unity_macos);
event_snapshot!(unity_windows);
event_snapshot!(unity_ios);
event_snapshot!(unity_linux);
event_snapshot!(unity_android);

#[test]
fn test_event_schema_snapshot() {
    insta::assert_json_snapshot!(
        "event_schema",
        relay_event_schema::protocol::event_json_schema()
    );
}
