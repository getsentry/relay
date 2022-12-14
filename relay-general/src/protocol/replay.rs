//! Replay processing and normalization module.
//!
//! Replays are multi-part values sent from Sentry integrations spanning arbitrary time-periods.
//! They are ingested incrementally.
//!
//! # Protocol
//!
//! Relay is expecting a JSON object with some mandatory metadata.  However, environment and user
//! metadata is usually sent in addition to the minimal payload.
//!
//! ```json
//! {
//!     "type": "replay_event",
//!     "replay_id": "d2132d31b39445f1938d7e21b6bf0ec4",
//!     "event_id": "63c5b0f895441a94340183c5f1e74cd4",
//!     "segment_id": 0,
//!     "timestamp": 1597976392.6542819,
//!     "replay_start_timestamp": 1597976392.6542819,
//!     "urls": ["https://sentry.io"],
//!     "error_ids": ["d2132d31b39445f1938d7e21b6bf0ec4"],
//!     "trace_ids": ["63c5b0f895441a94340183c5f1e74cd4"],
//!     "request": {
//!         "headers": {"User-Agent": "Mozilla/5.0..."}
//!     },
//! }
//! ```
use crate::protocol::{
    ClientSdkInfo, Contexts, IpAddr, LenientString, Request, Tags, Timestamp, User,
};
use crate::store::is_valid_platform;
use crate::store::user_agent::normalize_user_agent_generic;
use crate::types::{Annotated, Array};
use crate::user_agent;
use std::net::IpAddr as RealIPAddr;

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_replay", value_type = "Replay")]
pub struct Replay {
    pub event_id: Annotated<String>,
    pub replay_id: Annotated<String>,
    pub replay_type: Annotated<String>,
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
    pub fn normalize(&mut self, client_ip: Option<RealIPAddr>, user_agent: Option<&str>) {
        self.normalize_platform();
        self.normalize_ip_address(client_ip);
        self.normalize_user_agent(user_agent);
        self.normalize_type();
    }

    fn normalize_ip_address(&mut self, ip_address: Option<RealIPAddr>) {
        if let Some(addr) = ip_address {
            if let Some(user) = self.user.value_mut() {
                if user.ip_address.value().is_none() {
                    user.ip_address.set_value(Some(IpAddr(addr.to_string())));
                }
            }
        }
    }

    fn normalize_user_agent(&mut self, default_user_agent: Option<&str>) {
        let user_agent = match user_agent::get_user_agent_generic(&self.request) {
            Some(ua) => ua,
            None => match default_user_agent {
                Some(dua) => dua,
                None => return,
            },
        };

        if let Some(contexts) = self.contexts.value_mut() {
            // If a contexts object exists we modify in place.
            normalize_user_agent_generic(contexts, &self.platform, user_agent);
        } else {
            // If a contexts object does not exist we create a new one and attempt to populate
            // it.  If we didn't write any data to our new contexts instance we can throw it out
            // and leave the existing contexts value as "None".
            let mut contexts = Contexts::new();
            normalize_user_agent_generic(&mut contexts, &self.platform, user_agent);

            if !contexts.is_empty() {
                self.contexts.set_value(Some(contexts));
            }
        }
    }

    fn normalize_platform(&mut self) {
        // Null platforms are permitted but must be defaulted before continuing.
        let platform = self.platform.get_or_insert_with(|| "other".to_string());

        // Normalize bad platforms to "other" type.
        if !is_valid_platform(platform) {
            self.platform = Annotated::from("other".to_string());
        }
    }

    fn normalize_type(&mut self) {
        self.ty = Annotated::from("replay_event".to_string());
    }

    pub fn scrub_ip_address(&mut self) {
        if let Some(user) = self.user.value_mut() {
            user.ip_address.set_value(None);
        }
    }

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

#[cfg(test)]
mod tests {
    use crate::pii::{DataScrubbingConfig, PiiProcessor};
    use crate::processor::process_value;
    use crate::processor::{FieldAttrs, Pii, ProcessingState, Processor, ValueType};
    use crate::protocol::{
        BrowserContext, Context, ContextInner, Contexts, DeviceContext, OsContext, Replay,
        TagEntry, Tags,
    };
    use crate::types::{Annotated, ErrorKind, Map, Meta, Object, ProcessingAction};
    use chrono::{TimeZone, Utc};
    use std::net::{IpAddr, Ipv4Addr};

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

    #[test]
    fn test_set_user_agent_meta() {
        let os_context = Annotated::new(ContextInner(Context::Os(Box::new(OsContext {
            name: Annotated::new("Mac OS X".to_string()),
            version: Annotated::new("10.15.7".to_string()),
            ..Default::default()
        }))));
        let browser_context =
            Annotated::new(ContextInner(Context::Browser(Box::new(BrowserContext {
                name: Annotated::new("Safari".to_string()),
                version: Annotated::new("15.5".to_string()),
                ..Default::default()
            }))));
        let device_context =
            Annotated::new(ContextInner(Context::Device(Box::new(DeviceContext {
                family: Annotated::new("Mac".to_string()),
                brand: Annotated::new("Apple".to_string()),
                model: Annotated::new("Mac".to_string()),
                ..Default::default()
            }))));

        // Parse user input.
        let payload = include_str!("../../tests/fixtures/replays/replay.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        let replay_value = replay.value_mut().as_mut().unwrap();
        replay_value.normalize(None, None);

        let loaded_browser_context = replay_value
            .contexts
            .value_mut()
            .as_mut()
            .unwrap()
            .get("browser")
            .unwrap()
            .clone();

        let loaded_os_context = replay_value
            .contexts
            .value_mut()
            .as_mut()
            .unwrap()
            .get("client_os")
            .unwrap()
            .clone();

        let loaded_device_context = replay_value
            .contexts
            .value_mut()
            .as_mut()
            .unwrap()
            .get("device")
            .unwrap()
            .clone();

        assert_eq!(loaded_browser_context, browser_context);
        assert_eq!(loaded_os_context, os_context);
        assert_eq!(loaded_device_context, device_context);
    }

    #[test]
    fn test_missing_user() {
        let payload = include_str!("../../tests/fixtures/replays/replay_missing_user.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        let replay_value = replay.value_mut().as_mut().unwrap();
        replay_value.normalize(None, None);

        let user = replay_value.user.value();
        assert!(user.is_none());
    }

    #[test]
    fn test_set_ip_address_missing_user_ip_address() {
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // IP-Address set.
        let payload =
            include_str!("../../tests/fixtures/replays/replay_missing_user_ip_address.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        let replay_value = replay.value_mut().as_mut().unwrap();
        replay_value.normalize(Some(ip_address), None);

        let ipaddr = replay_value
            .user
            .value_mut()
            .as_ref()
            .unwrap()
            .ip_address
            .value()
            .unwrap()
            .as_str();
        assert!("127.0.0.1" == ipaddr);
    }

    #[test]
    fn test_loose_type_requirements() {
        let payload = include_str!("../../tests/fixtures/replays/replay_failure_22_08_31.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        let replay_value = replay.value_mut().as_mut().unwrap();
        replay_value.normalize(None, None);

        let user = replay_value.user.value_mut().as_mut().unwrap();
        assert!(user.ip_address.value_mut().as_mut().unwrap().as_str() == "127.1.1.1");
        assert!(user.username.value_mut().is_none());
        assert!(user.email.value_mut().as_mut().unwrap().as_str() == "email@sentry.io");
        assert!(user.id.value_mut().as_mut().unwrap().as_str() == "1");
    }

    #[test]
    fn test_scrub_pii_from_annotated_replay() {
        let mut scrub_config = DataScrubbingConfig::default();
        scrub_config.scrub_data = true;
        scrub_config.scrub_defaults = true;
        scrub_config.scrub_ip_addresses = true;

        let pii_config = scrub_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        let payload = include_str!("../../tests/fixtures/replays/replay.json");
        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        process_value(&mut replay, &mut pii_processor, ProcessingState::root()).unwrap();

        let maybe_ip_address = replay
            .value()
            .unwrap()
            .user
            .value()
            .unwrap()
            .ip_address
            .value();

        assert!(maybe_ip_address.is_none());
    }
}
