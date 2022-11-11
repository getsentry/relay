// #[cfg(feature = "jsonschema")]
// use schemars::gen::SchemaGenerator;
// #[cfg(feature = "jsonschema")]
// use schemars::schema::Schema;

use crate::protocol::{ClientSdkInfo, Contexts, LenientString, Request, Tags, Timestamp, User};
use crate::types::{Annotated, Array};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_replay", value_type = "Replay")]
pub struct Replay {
    pub event_id: Annotated<String>,
    pub replay_id: Annotated<String>,
    pub segment_id: Annotated<u64>,
    pub timestamp: Annotated<Timestamp>,
    pub replay_start_timestamp: Annotated<Timestamp>,
    pub urls: Annotated<Array<String>>,
    pub error_ids: Annotated<Array<String>>,
    pub trace_ids: Annotated<Array<String>>,
    pub contexts: Annotated<Contexts>,
    pub platform: Annotated<String>,

    #[metastructure(
        max_chars = "tag_value",
        required = "false",
        trim_whitespace = "true",
        nonempty = "true",
        skip_serialization = "empty"
    )]
    pub release: Annotated<LenientString>,

    #[metastructure(
        allow_chars = "a-zA-Z0-9_.-",
        trim_whitespace = "true",
        required = "false",
        nonempty = "true"
    )]
    pub dist: Annotated<String>,

    #[metastructure(
        max_chars = "environment",
        nonempty = "true",
        required = "false",
        trim_whitespace = "true"
    )]
    pub environment: Annotated<String>,

    #[metastructure(skip_serialization = "empty", pii = "maybe")]
    pub tags: Annotated<Tags>,

    #[metastructure(field = "type")]
    pub ty: Annotated<String>,

    #[metastructure(skip_serialization = "empty")]
    pub user: Annotated<User>,

    #[metastructure(skip_serialization = "empty")]
    pub request: Annotated<Request>,

    #[metastructure(field = "sdk")]
    #[metastructure(skip_serialization = "empty")]
    pub sdk: Annotated<ClientSdkInfo>,
}

impl Replay {
    pub fn get_tag_value(&self, tag_key: &str) -> Option<&str> {
        if let Some(tags) = self.tags.value() {
            tags.get(tag_key)
        } else {
            None
        }
    }

    pub fn sdk_name(&self) -> &str {
        if let Some(sdk) = self.sdk.value() {
            if let Some(name) = sdk.name.as_str() {
                return name;
            }
        }

        "unknown"
    }

    pub fn sdk_version(&self) -> &str {
        if let Some(sdk) = self.sdk.value() {
            if let Some(version) = sdk.version.as_str() {
                return version;
            }
        }

        "unknown"
    }
}

mod test {
    use super::*;
    use crate::protocol::TagEntry;
    use crate::types::ErrorKind;
    use crate::types::{Map, Meta};
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_event_roundtrip() {
        // NOTE: Interfaces will be tested separately.
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "replay_id": "52df9022835246eeb317dbd739ccd059",
  "segment_id": 0,
  "timestamp": 946684800.0,
  "replay_start_timestamp": 946684800.0,
  "urls": ["localhost:9000"],
  "error_ids": ["52df9022835246eeb317dbd739ccd059"],
  "trace_ids": ["52df9022835246eeb317dbd739ccd059"],
  "platform": "myplatform",
  "release": "myrelease",
  "dist": "mydist",
  "environment": "myenv",
  "tags": [
    [
      "tag",
      "value"
    ]
  ]
}"#;

        let replay = Annotated::new(Replay {
            event_id: Annotated::new("52df9022835246eeb317dbd739ccd059".to_string()),
            replay_id: Annotated::new("52df9022835246eeb317dbd739ccd059".to_string()),
            segment_id: Annotated::new(0),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            replay_start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            urls: Annotated::new(vec![Annotated::new("localhost:9000".to_string())]),
            error_ids: Annotated::new(vec![Annotated::new(
                "52df9022835246eeb317dbd739ccd059".to_string(),
            )]),
            trace_ids: Annotated::new(vec![Annotated::new(
                "52df9022835246eeb317dbd739ccd059".to_string(),
            )]),
            platform: Annotated::new("myplatform".to_string()),
            release: Annotated::new("myrelease".to_string().into()),
            dist: Annotated::new("mydist".to_string()),
            environment: Annotated::new("myenv".to_string()),
            tags: {
                let items = vec![Annotated::new(TagEntry(
                    Annotated::new("tag".to_string()),
                    Annotated::new("value".to_string()),
                ))];
                Annotated::new(Tags(items.into()))
            },
            ..Default::default()
        });

        assert_eq!(replay, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_lenient_release() {
        let input = r#"{"release":42}"#;
        let output = r#"{"release":"42"}"#;
        let event = Annotated::new(Replay {
            release: Annotated::new("42".to_string().into()),
            ..Default::default()
        });

        assert_eq!(event, Annotated::from_json(input).unwrap());
        assert_eq!(output, event.to_json().unwrap());
    }
}
