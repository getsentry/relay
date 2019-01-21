use std::fs;

use insta::assert_serialized_snapshot_matches;
use semaphore_general::processor::{process_value, ProcessingState};
use semaphore_general::protocol::Event;
use semaphore_general::store::{StoreConfig, StoreProcessor};
use semaphore_general::types::Annotated;

macro_rules! event_snapshot {
    ($id:expr) => {
        let id: &str = &$id;
        let data = fs::read_to_string(format!("tests/fixtures/payloads/{}.json", id)).unwrap();
        let mut event = Annotated::<Event>::from_json(&data).unwrap();
        assert_serialized_snapshot_matches!($id, event);
        let mut processor = StoreProcessor::new(StoreConfig::default(), None);
        process_value(&mut event, &mut processor, ProcessingState::root());
        assert_serialized_snapshot_matches!(format!("{}_normalized", $id), event, {
            ".received" => "[received]",
            ".timestamp" => "[timestamp]"
        });
    }
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
