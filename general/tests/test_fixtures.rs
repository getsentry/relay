use std::collections::BTreeSet;
use std::fs;

use insta::assert_yaml_snapshot;
use semaphore_general::processor::{process_value, ProcessingState};
use semaphore_general::protocol::Event;
use semaphore_general::store::{StoreConfig, StoreProcessor};
use semaphore_general::types::{Annotated, SerializableAnnotated};

lazy_static::lazy_static! {
    static ref VALID_PLATOFORMS: BTreeSet<String> = {
        let mut platforms = BTreeSet::new();
        platforms.insert("as3".to_string());
        platforms.insert("c".to_string());
        platforms.insert("cfml".to_string());
        platforms.insert("cocoa".to_string());
        platforms.insert("csharp".to_string());
        platforms.insert("go".to_string());
        platforms.insert("java".to_string());
        platforms.insert("javascript".to_string());
        platforms.insert("node".to_string());
        platforms.insert("objc".to_string());
        platforms.insert("other".to_string());
        platforms.insert("perl".to_string());
        platforms.insert("php".to_string());
        platforms.insert("python".to_string());
        platforms.insert("ruby".to_string());
        platforms.insert("elixir".to_string());
        platforms.insert("haskell".to_string());
        platforms.insert("groovy".to_string());
        platforms.insert("native".to_string());
        platforms
    };
}

macro_rules! event_snapshot {
    ($id:expr) => {
        let data = fs::read_to_string(format!("tests/fixtures/payloads/{}.json", $id)).unwrap();

        let mut event = Annotated::<Event>::from_json(&data).unwrap();
        assert_yaml_snapshot!(SerializableAnnotated(&event));

        let mut config = StoreConfig::default();
        config.valid_platforms = VALID_PLATOFORMS.clone();
        let mut processor = StoreProcessor::new(config, None);
        process_value(&mut event, &mut processor, ProcessingState::root());
        assert_yaml_snapshot!(SerializableAnnotated(&event), {
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
