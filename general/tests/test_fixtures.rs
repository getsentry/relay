use std::fs;

use relay_general::processor::{process_value, ProcessingState};
use relay_general::protocol::Event;
use relay_general::store::{StoreConfig, StoreProcessor};
use relay_general::types::{Annotated, SerializableAnnotated};

use insta::assert_yaml_snapshot;

macro_rules! event_snapshot {
    ($id:expr) => {
        let data = fs::read_to_string(format!("tests/fixtures/payloads/{}.json", $id)).unwrap();

        let mut event = Annotated::<Event>::from_json(&data).unwrap();
        assert_yaml_snapshot!(SerializableAnnotated(&event));

        let config = StoreConfig::default();
        let mut processor = StoreProcessor::new(config, None);
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_yaml_snapshot!(SerializableAnnotated(&event), {
            ".received" => "[received]",
            ".timestamp" => "[timestamp]"
        });
    }
}

#[test]
fn test_android() {
    event_snapshot!("android");
}

#[test]
fn test_cocoa() {
    event_snapshot!("cocoa");
}

#[test]
fn test_cordova() {
    event_snapshot!("cordova");
}

#[test]
fn test_dotnet() {
    event_snapshot!("dotnet");
}

#[test]
fn test_legacy_python() {
    event_snapshot!("legacy_python");
}

#[test]
fn test_legacy_node_exception() {
    event_snapshot!("legacy_node_exception");
}
